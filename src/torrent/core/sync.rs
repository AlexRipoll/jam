use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Display,
    time::Duration,
};

use protocol::{bitfield::Bitfield, piece::Piece};
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{debug, error, info};

use crate::torrent::events::Event;

#[derive(Debug)]
pub struct Synchronizer {
    pub pieces: HashMap<u32, Piece>,

    // Bitfield of the downloaded pieces
    pub bitfield: Bitfield,

    // FIX: check if necessary
    // Tracking active peer sessions
    active_sessions: HashSet<String>,

    // Peers' bitfield from all peer connections
    peers_bitfield: HashMap<String, Bitfield>,

    // Containing the amount of occurrences of a piece index
    pieces_rarity: Vec<u8>,

    // Sorted set of piece indexes pending to be downloaded from the current
    pending_pieces: Vec<u32>,

    // peer connections (rarest first)
    assigned_pieces: HashSet<u32>, // Indexes of the pieces being downloaded

    // Indexes of the pieces being downloaded by each worker
    workers_pending_pieces: HashMap<String, Vec<u32>>,

    // Workers' queue max capacity
    queue_capacity: usize,

    // Channels for communication
    event_tx: mpsc::Sender<Event>,

    // Keep track of whether dispatching is in progress
    dispatch_in_progress: bool,
}

// Enum defining all possible commands our actor can receive
#[derive(Debug)]
pub enum SynchronizerCommand {
    // Dispatch related commands
    RequestDispatch,
    PieceDispatched {
        session_id: String,
        piece_index: u32,
    },
    DispatchCompleted,

    // Peer management commands
    ProcessBitfield {
        session_id: String,
        bitfield: Vec<u8>,
    },
    ProcessHave {
        session_id: String,
        piece_index: u32,
    },
    CorruptedPiece {
        session_id: String,
        piece_index: u32,
    },
    ClosePeerSession(String),

    // Piece management commands
    MarkPieceComplete(u32),
    UnassignPiece {
        session_id: String,
        piece_index: u32,
    },
    UnassignPieces {
        session_id: String,
        pieces_index: Vec<u32>,
    },

    // Query commands (with response channels)
    QueryProgress(mpsc::Sender<u32>),
    QueryIsCompleted(mpsc::Sender<bool>),
    QueryHasActiveWorkers(mpsc::Sender<bool>),
}

// Data structure to pass data to the dispatch task
#[derive(Debug, Clone)]
pub struct DispatchData {
    pieces: HashMap<u32, Piece>,
    peers_bitfield: HashMap<String, Bitfield>,
    workers_pending_pieces: HashMap<String, Vec<u32>>,
    pending_pieces: Vec<u32>,
    assigned_pieces: HashSet<u32>,
    queue_capacity: usize,
}

impl Synchronizer {
    pub fn new(
        pieces: HashMap<u32, Piece>,
        queue_capacity: usize,
        event_tx: mpsc::Sender<Event>,
    ) -> Self {
        let total_pieces = pieces.len();

        Self {
            pieces,
            bitfield: Bitfield::new(total_pieces),
            active_sessions: HashSet::new(),
            peers_bitfield: HashMap::new(),
            pieces_rarity: vec![0u8; total_pieces],
            pending_pieces: Vec::new(),
            assigned_pieces: HashSet::new(),
            workers_pending_pieces: HashMap::new(),
            queue_capacity,
            event_tx,
            dispatch_in_progress: false,
        }
    }

    pub fn run(mut self) -> (mpsc::Sender<SynchronizerCommand>, JoinHandle<()>) {
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<SynchronizerCommand>(100);
        let (dispatch_tx, mut dispatch_rx) = mpsc::channel::<DispatchData>(1);

        let event_tx_clone = self.event_tx.clone();
        let dispatch_cmd_tx = cmd_tx.clone();
        let actor_cmd_tx = cmd_tx.clone();

        // Spawn a separate task for handling dispatch operations
        let dispatch_handle = tokio::spawn(async move {
            while let Some(data) = dispatch_rx.recv().await {
                if let Err(e) = Self::handle_dispatch(data, &dispatch_cmd_tx, &event_tx_clone).await
                {
                    error!("Error handling dispatch: {}", e);
                }
            }
        });

        // Main actor task
        let actor_handle = tokio::spawn(async move {
            // Store handle to ensure it is dropped properly when actor_handle completes
            let _dispatch_handle = dispatch_handle;

            while let Some(cmd) = cmd_rx.recv().await {
                if let Err(e) = self.handle_command(cmd, &dispatch_tx, &actor_cmd_tx).await {
                    error!("Error handling command: {}", e);
                }
            }
        });

        (cmd_tx, actor_handle)
    }

    async fn handle_dispatch(
        data: DispatchData,
        dispatch_cmd_tx: &mpsc::Sender<SynchronizerCommand>,
        event_tx: &mpsc::Sender<Event>,
    ) -> Result<(), SynchronizerError> {
        // Create workers queue locally
        let workers_queue = Self::assign_pieces_to_workers(
            &data.peers_bitfield,
            &data.workers_pending_pieces,
            &data.pending_pieces,
            &data.assigned_pieces,
        );

        // Process each worker's queue
        for (worker_id, mut queue) in workers_queue {
            let worker_pending = data
                .workers_pending_pieces
                .get(&worker_id)
                .map(|v| v.len())
                .unwrap_or(0);

            let available_slots = data.queue_capacity.saturating_sub(worker_pending);
            let pieces_to_dispatch = queue.drain(..queue.len().min(available_slots));

            for piece_index in pieces_to_dispatch {
                if let Some(piece) = data.pieces.get(&piece_index) {
                    // Dispatch the piece
                    event_tx
                        .send(Event::PieceDispatch {
                            session_id: worker_id.clone(),
                            piece: piece.clone(),
                        })
                        .await?;

                    // Notify synchronizer about dispatched piece
                    dispatch_cmd_tx
                        .send(SynchronizerCommand::PieceDispatched {
                            session_id: worker_id.clone(),
                            piece_index,
                        })
                        .await?;

                    // Small delay to prevent flooding
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }

        // Notify completion
        dispatch_cmd_tx
            .send(SynchronizerCommand::DispatchCompleted)
            .await?;

        Ok(())
    }

    async fn handle_command(
        &mut self,
        command: SynchronizerCommand,
        dispatch_tx: &mpsc::Sender<DispatchData>,
        actor_cmd_tx: &mpsc::Sender<SynchronizerCommand>,
    ) -> Result<(), SynchronizerError> {
        match command {
            SynchronizerCommand::RequestDispatch => {
                if !self.dispatch_in_progress && !self.pending_pieces.is_empty() {
                    self.dispatch_in_progress = true;

                    // Prepare data for the dispatch task
                    let data = DispatchData {
                        pieces: self.pieces.clone(),
                        peers_bitfield: self.peers_bitfield.clone(),
                        workers_pending_pieces: self.workers_pending_pieces.clone(),
                        pending_pieces: self.pending_pieces.clone(),
                        assigned_pieces: self.assigned_pieces.clone(),
                        queue_capacity: self.queue_capacity,
                    };

                    dispatch_tx.send(data).await?;
                }
            }
            SynchronizerCommand::PieceDispatched {
                session_id,
                piece_index,
            } => {
                // Update internal state
                self.workers_pending_pieces
                    .entry(session_id)
                    .or_insert_with(Vec::new)
                    .push(piece_index);
                self.assigned_pieces.insert(piece_index);
            }
            SynchronizerCommand::DispatchCompleted => {
                self.dispatch_in_progress = false;

                // If there are still pending pieces, request another dispatch
                if !self.pending_pieces.is_empty() {
                    actor_cmd_tx
                        .send(SynchronizerCommand::RequestDispatch)
                        .await?;
                }
            }
            SynchronizerCommand::ProcessBitfield {
                session_id,
                bitfield,
            } => {
                debug!(
                    session_id = %session_id,
                    bitfield = ?bitfield,
                    "Peer bitfield received"
                );
                let total_pieces = self.bitfield.total_pieces;
                let bitfield = Bitfield::from(&bitfield, total_pieces);

                if self.has_missing_pieces(&bitfield) {
                    self.event_tx
                        .send(Event::NotifyInterest {
                            session_id: session_id.clone(),
                        })
                        .await?;
                    self.process_bitfield(&session_id, bitfield);

                    // After processing a new bitfield, we might have new pieces to dispatch
                    if !self.pending_pieces.is_empty() && !self.dispatch_in_progress {
                        actor_cmd_tx
                            .send(SynchronizerCommand::RequestDispatch)
                            .await?;
                    }
                } else {
                    self.event_tx
                        .send(Event::DisconnectPeerSession {
                            session_id: session_id.clone(),
                        })
                        .await?;
                }
            }
            SynchronizerCommand::ProcessHave {
                session_id,
                piece_index,
            } => {
                debug!(
                    session_id = %session_id,
                    piece_index = %piece_index,
                    "'Have' message received"
                );
                if let Some(bitfield) = self.peers_bitfield.get_mut(&session_id) {
                    bitfield.set_piece(piece_index as usize);
                }
                // TODO: check if it is a missing piece and it is not in the remaining
                // pieces list, if so add it
            }
            SynchronizerCommand::CorruptedPiece {
                session_id,
                piece_index,
            } => {
                debug!(
                    session_id = %session_id,
                    piece_index = %piece_index,
                    "Corrupted piece"
                );
                self.unassign_piece(&session_id, piece_index);
                // TODO: blacklist worker for this piece index
            }
            SynchronizerCommand::ClosePeerSession(session_id) => {
                debug!(
                    session_id = %session_id,
                    "Closing peer session"
                );
                self.close_session(&session_id);
            }
            SynchronizerCommand::MarkPieceComplete(piece_index) => {
                debug!(
                    piece_index = %piece_index,
                    "Piece completed"
                );
                self.mark_piece_complete(piece_index);
                // check if any of the peer session has completed all the work
                for (session_id, pending_pieces) in self.workers_pending_pieces.clone().iter() {
                    // TODO: check also if has_assignable_pieces
                    if pending_pieces.is_empty() {
                        self.event_tx
                            .send(Event::DisconnectPeerSession {
                                session_id: session_id.clone(),
                            })
                            .await?;
                        self.close_session(&session_id);
                    }
                }

                info!("Download progress: {}%", self.download_progress_percent());

                if self.is_completed() {
                    info!("Download completed");
                    self.event_tx.send(Event::DownloadCompleted).await?;
                }
            }
            SynchronizerCommand::UnassignPiece {
                session_id,
                piece_index,
            } => {
                debug!(
                    session_id = %session_id,
                    piece_index = %piece_index,
                    "Unassign piece"
                );
                self.unassign_piece(&session_id, piece_index);
            }
            SynchronizerCommand::UnassignPieces {
                session_id,
                pieces_index,
            } => {
                debug!(
                    session_id = %session_id,
                    pieces_index = ?pieces_index,
                    "Unassign pieces"
                );
                self.unassign_pieces(&session_id, pieces_index);
            }
            SynchronizerCommand::QueryProgress(response_tx) => {
                let progress = self.download_progress_percent();
                response_tx.send(progress).await?;
            }
            SynchronizerCommand::QueryIsCompleted(response_tx) => {
                let completed = self.is_completed();
                response_tx.send(completed).await?;
            }
            SynchronizerCommand::QueryHasActiveWorkers(response_tx) => {
                let has_active = self.has_active_worker();
                response_tx.send(has_active).await?;
            }
        }

        Ok(())
    }

    // Function that runs in a separate task, doesn't access synchronizer state directly
    fn assign_pieces_to_workers(
        peers_bitfield: &HashMap<String, Bitfield>,
        workers_pending_pieces: &HashMap<String, Vec<u32>>,
        pending_pieces: &Vec<u32>,
        assigned_pieces: &HashSet<u32>,
    ) -> HashMap<String, VecDeque<u32>> {
        let mut assignments: HashMap<String, VecDeque<u32>> = HashMap::new();
        let mut worker_pieces: Vec<(String, Vec<u32>)> = Vec::new();

        // Check how many pieces can be downloaded by each worker
        for (worker_id, peer_bitfield) in peers_bitfield.iter() {
            let available_pieces: Vec<u32> = pending_pieces
                .iter()
                .filter(|&&piece_index| {
                    !assigned_pieces.contains(&piece_index)
                        && peer_bitfield.has_piece(piece_index as usize)
                })
                .cloned()
                .collect();

            if !available_pieces.is_empty() {
                worker_pieces.push((worker_id.clone(), available_pieces));
            }
        }

        // Convert pending_pieces to HashSet for faster lookups
        let pending_set: HashSet<u32> = pending_pieces.iter().cloned().collect();
        let mut remaining_pieces: HashSet<u32> =
            pending_set.difference(assigned_pieces).cloned().collect();

        // Sort workers by how many pending pieces they already have (fewer first)
        worker_pieces.sort_by_key(|(worker_id, _)| {
            workers_pending_pieces.get(worker_id).map_or(0, |v| v.len())
        });

        // Distribute pieces across workers in a fair way
        while !remaining_pieces.is_empty() && !worker_pieces.is_empty() {
            for (worker_id, available_pieces) in &worker_pieces {
                if remaining_pieces.is_empty() {
                    break;
                }

                // Find the first available piece this worker can handle
                if let Some(&piece) = available_pieces
                    .iter()
                    .find(|&&piece| remaining_pieces.contains(&piece))
                {
                    assignments
                        .entry(worker_id.clone())
                        .or_insert_with(VecDeque::new)
                        .push_back(piece);
                    remaining_pieces.remove(&piece);
                }
            }
        }

        assignments
    }

    fn process_bitfield(&mut self, peer_id: &str, peer_bitfield: Bitfield) {
        let missing_pieces_bitfield = self.build_interested_pieces_bitfield(&peer_bitfield);
        self.peers_bitfield
            .insert(peer_id.to_string(), peer_bitfield);
        self.workers_pending_pieces
            .entry(peer_id.to_string())
            .or_insert(Vec::new());

        self.increase_pieces_rarity(&missing_pieces_bitfield);
        self.pending_pieces = self.sort_pieces();
    }

    /// Constructs a bitfield representing the pieces that the peer has but are missing
    /// from the current state. This bitfield indicates the pending pieces that the state
    /// can request for download.
    ///
    /// # Parameters
    /// - `peer_bitfield`: A reference to the peer's bitfield, representing the pieces
    ///   that the peer possesses.
    ///
    /// # Returns
    /// A `Bitfield` where each bit is set if the corresponding piece is available
    /// in the peer's bitfield but not in the current state's bitfield.
    fn build_interested_pieces_bitfield(&self, peer_bitfield: &Bitfield) -> Bitfield {
        let mut missing = Vec::new();

        for (peer_byte, state_byte) in peer_bitfield.bytes.iter().zip(self.bitfield.bytes.iter()) {
            missing.push(peer_byte & !state_byte);
        }

        Bitfield::from(&missing, self.bitfield.total_pieces)
    }

    fn downloaded_pieces_count(&self) -> u32 {
        self.bitfield
            .bytes
            .iter()
            .map(|byte| byte.count_ones())
            .sum::<u32>()
    }

    // Find the next assignable piece for the given peer bitfield and returns the piece index.
    fn assign_piece(&mut self, peer_bitfield: &Bitfield) -> Option<u32> {
        if let Some(pos) = self.pending_pieces.iter().position(|&piece_index| {
            !self.assigned_pieces.contains(&piece_index)
                && peer_bitfield.has_piece(piece_index as usize)
        }) {
            let piece_index = self.pending_pieces[pos];
            self.assigned_pieces.insert(piece_index.clone());
            return Some(piece_index);
        }

        None
    }

    fn unassign_piece(&mut self, session_id: &str, piece_index: u32) {
        self.assigned_pieces.remove(&piece_index);
        if let Some(pending_pieces) = self.workers_pending_pieces.get_mut(session_id) {
            if let Some(pos) = pending_pieces.iter().position(|&p| p == piece_index) {
                pending_pieces.remove(pos);
            }
        }
    }

    fn unassign_pieces(&mut self, session_id: &str, pieces_index: Vec<u32>) {
        for piece_index in pieces_index {
            self.unassign_piece(session_id, piece_index);
        }
    }

    fn remove_pending_piece(&mut self, piece_index: u32) {
        for (pos, &piece_idx) in self.pending_pieces.iter().enumerate() {
            if piece_idx == piece_index {
                self.pending_pieces.remove(pos as usize);
                break;
            }
        }
    }

    fn remove_worker_pending_piece(&mut self, piece_index: u32) {
        for (_, values) in self.workers_pending_pieces.iter_mut() {
            if let Some(pos) = values.iter().position(|&x| x == piece_index) {
                values.remove(pos);
                break;
            }
        }
    }

    // Mark a piece as downloaded, remove from missing and assigned sets
    fn mark_piece_complete(&mut self, piece_index: u32) {
        self.bitfield.set_piece(piece_index as usize);
        self.remove_pending_piece(piece_index);
        self.remove_worker_pending_piece(piece_index);
        self.assigned_pieces.remove(&piece_index);
        // set to 0 so it is not taken in consideration since it is already set in the `bitfield` field.
        self.pieces_rarity[piece_index as usize] = 0;
    }

    fn increase_pieces_rarity(&mut self, peer_bitfield: &Bitfield) {
        for byte_index in 0..peer_bitfield.bytes.len() {
            for bit_index in 0..8 {
                let piece_index = (byte_index * 8 + bit_index) as u32;
                if peer_bitfield.has_piece(piece_index as usize) {
                    self.pieces_rarity[piece_index as usize] =
                        self.pieces_rarity[piece_index as usize].saturating_add(1);
                }
            }
        }
    }

    fn decrease_pieces_rarity(&mut self, peer_bitfield: &Bitfield) {
        for byte_index in 0..peer_bitfield.bytes.len() {
            for bit_index in 0..8 {
                let piece_index = (byte_index * 8 + bit_index) as u32;
                if peer_bitfield.has_piece(piece_index as usize) {
                    self.pieces_rarity[piece_index as usize] =
                        self.pieces_rarity[piece_index as usize].saturating_sub(1);
                }
            }
        }
    }

    fn sort_pieces(&self) -> Vec<u32> {
        // remove already downloaded or unavailable pieces
        let mut indices: Vec<u32> = self
            .pieces_rarity
            .iter()
            .enumerate()
            .filter(|&(_, &count)| count > 0)
            .map(|(piece_index, _)| piece_index as u32)
            .collect();

        // sort pieces by rarity (rarest first)
        indices.sort_by(|&a, &b| {
            let count_a = self.pieces_rarity[a as usize];
            let count_b = self.pieces_rarity[b as usize];
            count_a.cmp(&count_b).then_with(|| a.cmp(&b))
        });

        indices
    }

    fn populate_queue(&mut self, sorted_pieces: Vec<u32>) {
        self.pending_pieces.clear();
        self.pending_pieces.extend(sorted_pieces);
    }

    fn has_missing_pieces(&self, peer_bitfield: &Bitfield) -> bool {
        for (byte_index, _) in peer_bitfield.bytes.iter().enumerate() {
            for bit_index in 0..8 {
                let piece_index = (byte_index * 8 + bit_index) as u32;

                if peer_bitfield.has_piece(piece_index as usize)
                    && !self.bitfield.has_piece(piece_index as usize)
                {
                    return true;
                }
            }
        }

        false
    }

    // checks if the state bitfield already has downloaded all the availabe pieces of a given peer
    // (state_bitfield & peer_bitfield == peer_bitfield)
    fn has_downloaded_all_peer_pieces(&self, peer_bitfield: &Bitfield) -> bool {
        if peer_bitfield.bytes.is_empty() {
            return false;
        }

        peer_bitfield
            .bytes
            .iter()
            .zip(self.bitfield.bytes.iter())
            .all(|(&x, &y)| (x & y) == x)
    }

    // checks if there is any missing (not in progress) piece to be downloaded from a given peer
    fn has_assignable_pieces(&self, peer_bitfield: &Bitfield) -> bool {
        // Iterate over the peer's bitfield to identify missing pieces
        let missing_pieces: Vec<usize> = (0..peer_bitfield.total_pieces)
            .filter(|&index| peer_bitfield.has_piece(index) && !self.bitfield.has_piece(index))
            .collect();

        // Check if any missing piece is already assigned to another task
        missing_pieces
            .into_iter()
            .any(|piece_index| !self.assigned_pieces.contains(&(piece_index as u32)))
    }

    // Iterate over the peer's bitfield to identify missing pieces that are not assigned
    fn missing_unassigned_pieces(&self, peer_bitfield: &Bitfield) -> Vec<usize> {
        (0..peer_bitfield.total_pieces)
            .filter(|&piece_index| {
                peer_bitfield.has_piece(piece_index)
                    && !self.bitfield.has_piece(piece_index)
                    && !self.assigned_pieces.contains(&(piece_index as u32))
            })
            .collect()
    }

    // checks if any of the workers has any piece stil in progress
    fn has_active_worker(&self) -> bool {
        self.workers_pending_pieces
            .iter()
            .any(|(_, pending_pieces)| !pending_pieces.is_empty())
    }

    // checks if all pieces have been downloaded
    fn is_completed(&self) -> bool {
        self.bitfield.has_all_pieces()
    }

    fn download_progress_percent(&self) -> u32 {
        let downloaded_pieces = self.downloaded_pieces_count();

        downloaded_pieces * 100 / self.bitfield.total_pieces as u32
    }

    fn close_session(&mut self, session_id: &str) {
        self.active_sessions.remove(session_id);
        self.peers_bitfield.remove(session_id);
        if let Some(pieces_indexes) = self.workers_pending_pieces.remove(session_id) {
            self.unassign_pieces(session_id, pieces_indexes);
        }
    }
}

#[derive(Debug)]
pub enum SynchronizerError {
    DispatcherDataTxError(mpsc::error::SendError<DispatchData>),
    CommandTxError(mpsc::error::SendError<SynchronizerCommand>),
    EventTxError(mpsc::error::SendError<Event>),
    QueryTxError(String),
}

impl Display for SynchronizerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SynchronizerError::DispatcherDataTxError(err) => {
                write!(f, "Failed to send dispatch data: {}", err)
            }
            SynchronizerError::CommandTxError(err) => {
                write!(f, "Failed to send command: {}", err)
            }
            SynchronizerError::EventTxError(err) => {
                write!(f, "Failed to send event: {}", err)
            }
            SynchronizerError::QueryTxError(msg) => {
                write!(f, "Failed to send response: {}", msg)
            }
        }
    }
}

impl From<mpsc::error::SendError<DispatchData>> for SynchronizerError {
    fn from(err: mpsc::error::SendError<DispatchData>) -> Self {
        SynchronizerError::DispatcherDataTxError(err)
    }
}

impl From<mpsc::error::SendError<SynchronizerCommand>> for SynchronizerError {
    fn from(err: mpsc::error::SendError<SynchronizerCommand>) -> Self {
        SynchronizerError::CommandTxError(err)
    }
}

impl From<mpsc::error::SendError<Event>> for SynchronizerError {
    fn from(err: mpsc::error::SendError<Event>) -> Self {
        SynchronizerError::EventTxError(err)
    }
}

impl From<mpsc::error::SendError<u32>> for SynchronizerError {
    fn from(err: mpsc::error::SendError<u32>) -> Self {
        SynchronizerError::QueryTxError(format!("{}", err))
    }
}

impl From<mpsc::error::SendError<bool>> for SynchronizerError {
    fn from(err: mpsc::error::SendError<bool>) -> Self {
        SynchronizerError::QueryTxError(format!("{}", err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::{bitfield::Bitfield, piece::Piece};
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    };
    use tokio::{sync::mpsc, test as async_test};

    // Helper function to create test pieces map
    fn create_pieces_hashmap(count: u32, size: usize) -> HashMap<u32, Piece> {
        let mut pieces = HashMap::new();
        for i in 0..count {
            pieces.insert(i, Piece::new(i, size, [i as u8; 20]));
        }
        pieces
    }

    // Helper function to create a bitfield with specific pieces set
    fn create_bitfield(total: usize, set_indices: &[usize]) -> Vec<u8> {
        let mut bitfield = Bitfield::new(total);
        for &idx in set_indices {
            bitfield.set_piece(idx);
        }

        bitfield.bytes
    }

    #[async_test]
    async fn test_new_synchronizer() {
        let pieces = create_pieces_hashmap(10, 16834);
        let (event_tx, _) = mpsc::channel(100);

        let sync = Synchronizer::new(pieces, 5, event_tx);

        assert_eq!(sync.pieces.len(), 10);
        assert_eq!(sync.queue_capacity, 5);
        assert_eq!(sync.bitfield.total_pieces, 10);
        assert!(sync.active_sessions.is_empty());
        assert!(sync.peers_bitfield.is_empty());
        assert_eq!(sync.pieces_rarity.len(), 10);
        assert!(sync.pending_pieces.is_empty());
        assert!(sync.assigned_pieces.is_empty());
        assert!(sync.workers_pending_pieces.is_empty());
        assert!(!sync.dispatch_in_progress);
    }

    #[test]
    fn test_process_bitfield() {
        let total_pieces = 14;
        let pieces = create_pieces_hashmap(total_pieces as u32, 16384);
        let (event_tx, _) = mpsc::channel(100);

        let mut sync = Synchronizer::new(pieces.clone(), 5, event_tx);

        // Create a peer that has pieces 0, 1, 2, 5, 8
        let peer_bitfield = create_bitfield(total_pieces, &[0, 1, 2, 5, 8]);

        sync.process_bitfield("peer1", Bitfield::from(&peer_bitfield, total_pieces));

        // Verify worker was added
        assert!(sync.workers_pending_pieces.contains_key("peer1"));
        assert!(sync.peers_bitfield.contains_key("peer1"));

        // Verify the rarity of pieces
        assert_eq!(sync.pieces_rarity[0], 1);
        assert_eq!(sync.pieces_rarity[1], 1);
        assert_eq!(sync.pieces_rarity[2], 1);
        assert_eq!(sync.pieces_rarity[3], 0);
        assert_eq!(sync.pieces_rarity[5], 1);
        assert_eq!(sync.pieces_rarity[8], 1);

        // Verify pending pieces are populated and sorted (rarest first)
        assert_eq!(sync.pending_pieces.len(), 5);
        assert_eq!(sync.pending_pieces, vec![0, 1, 2, 5, 8]);

        // Add another peer with some overlapping pieces
        let peer2_bitfield = create_bitfield(total_pieces, &[0, 3, 5, 7, 9]);
        sync.process_bitfield("peer2", Bitfield::from(&peer2_bitfield, total_pieces));

        // Verify the rarity updated correctly
        assert_eq!(sync.pieces_rarity[0], 2); // Both peers have piece 0
        assert_eq!(sync.pieces_rarity[1], 1);
        assert_eq!(sync.pieces_rarity[2], 1);
        assert_eq!(sync.pieces_rarity[3], 1);
        assert_eq!(sync.pieces_rarity[5], 2); // Both peers have piece 5
        assert_eq!(sync.pieces_rarity[7], 1);
        assert_eq!(sync.pieces_rarity[8], 1);
        assert_eq!(sync.pieces_rarity[9], 1);

        // Verify pending pieces list is updated
        assert_eq!(sync.pending_pieces.len(), 8);
        assert_eq!(sync.pending_pieces, vec![1, 2, 3, 7, 8, 9, 0, 5,]);
    }

    #[test]
    fn test_mark_piece_complete() {
        let pieces = create_pieces_hashmap(8, 16384);
        let (event_tx, _) = mpsc::channel(100);

        let mut sync = Synchronizer::new(pieces, 5, event_tx);

        // Set up initial state with a peer having all pieces
        let peer_bitfield = create_bitfield(8, &[0, 1, 3, 4, 6, 7]);
        sync.process_bitfield("peer1", Bitfield::from(&peer_bitfield, 8));

        // Mark some pieces as assigned
        sync.assigned_pieces.insert(1);
        sync.assigned_pieces.insert(3);

        // Mark worker as having piece 1 pending
        sync.workers_pending_pieces
            .entry("peer1".to_string())
            .or_default()
            .push(1);

        // Verify the piece is still not marked in bitfield
        assert!(!sync.bitfield.has_piece(1));

        // Now mark piece 1 as complete
        sync.mark_piece_complete(1);

        // Verify the piece is marked in bitfield
        assert!(sync.bitfield.has_piece(1));

        // Verify the piece is removed from assigned pieces
        assert!(!sync.assigned_pieces.contains(&1));

        // Verify piece rarity is set to 0
        assert_eq!(sync.pieces_rarity[1], 0);

        // Verify piece is removed from worker's pending list
        assert!(!sync.workers_pending_pieces["peer1"].contains(&1));

        // Verify piece is removed from pending pieces
        assert!(!sync.pending_pieces.contains(&1));
    }

    #[test]
    fn test_has_missing_pieces() {
        let total_pieces = 14;
        let pieces = create_pieces_hashmap(total_pieces as u32, 16384);
        let (event_tx, _) = mpsc::channel(100);

        let mut sync = Synchronizer::new(pieces, 5, event_tx);

        // Create a peer bitfield with some pieces
        let peer_bitfield =
            Bitfield::from(&create_bitfield(total_pieces, &[0, 1, 2, 3]), total_pieces);

        // Initially we have no pieces, so the peer has pieces we're missing
        assert!(sync.has_missing_pieces(&peer_bitfield));

        // Mark all pieces as complete
        for i in 0..4 {
            sync.bitfield.set_piece(i);
        }

        // We should have no missing pieces from this peer
        assert!(!sync.has_missing_pieces(&peer_bitfield));

        // A different peer with a new piece should be detected as having missing pieces
        let peer2_bitfield = Bitfield::from(
            &create_bitfield(total_pieces, &[0, 1, 2, 3, 9]),
            total_pieces,
        );
        assert!(sync.has_missing_pieces(&peer2_bitfield));
    }

    #[test]
    fn test_close_session() {
        let total_pieces = 10;
        let pieces = create_pieces_hashmap(total_pieces as u32, 16384);
        let (event_tx, _) = mpsc::channel(100);

        let mut sync = Synchronizer::new(pieces, 5, event_tx);

        // Set up a peer with some bitfield
        let peer_bitfield =
            Bitfield::from(&create_bitfield(total_pieces, &[0, 1, 2, 9]), total_pieces);
        sync.peers_bitfield
            .insert("peer1".to_string(), peer_bitfield);
        sync.active_sessions.insert("peer1".to_string());

        // Assign some pieces to the peer
        let mut peer_pieces = Vec::new();
        peer_pieces.push(1);
        peer_pieces.push(2);
        sync.workers_pending_pieces
            .insert("peer1".to_string(), peer_pieces);

        // Mark these pieces as assigned
        sync.assigned_pieces.insert(1);
        sync.assigned_pieces.insert(2);

        // Close the session
        sync.close_session("peer1");

        // Verify the session is removed
        assert!(!sync.active_sessions.contains("peer1"));
        assert!(!sync.peers_bitfield.contains_key("peer1"));
        assert!(!sync.workers_pending_pieces.contains_key("peer1"));

        // Verify the pieces are unassigned
        assert!(!sync.assigned_pieces.contains(&1));
        assert!(!sync.assigned_pieces.contains(&2));
        // Verify the pieces are in the pending pieces list
        assert!(!sync.pending_pieces.contains(&1));
        assert!(!sync.pending_pieces.contains(&2));
    }

    #[test]
    fn test_has_concluded() {
        let total_pieces = 8;
        let pieces = create_pieces_hashmap(total_pieces as u32, 16384);
        let (event_tx, _) = mpsc::channel(100);

        let mut sync = Synchronizer::new(pieces.clone(), 5, event_tx);

        // Create a peer bitfield with pieces 0, 1, 2
        let peer_bitfield =
            Bitfield::from(&create_bitfield(total_pieces, &[0, 1, 2]), total_pieces);

        // Initially we have no pieces, so we haven't concluded
        assert!(!sync.has_downloaded_all_peer_pieces(&peer_bitfield));

        // Mark piece 0 and 1 as complete
        sync.bitfield.set_piece(0);
        sync.bitfield.set_piece(1);

        // We still need piece 2, so not concluded
        assert!(!sync.has_downloaded_all_peer_pieces(&peer_bitfield));

        // Mark the last piece as complete
        sync.bitfield.set_piece(2);

        // Now we should have concluded with this peer (got all their pieces)
        assert!(sync.has_downloaded_all_peer_pieces(&peer_bitfield));
    }

    #[test]
    fn test_assign_pieces_to_workers() {
        let total_pieces = 11;
        let mut peers_bitfield = HashMap::new();
        let mut workers_pending_pieces = HashMap::new();
        let pending_pieces = vec![0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11];
        let assigned_pieces = HashSet::from([2]); // Set piece 2 as already assigned

        // Worker1 has pieces 0, 1, 2, 7
        let worker1_bitfield =
            Bitfield::from(&create_bitfield(total_pieces, &[0, 1, 2, 7]), total_pieces);
        peers_bitfield.insert("worker1".to_string(), worker1_bitfield);
        workers_pending_pieces.insert("worker1".to_string(), vec![]);

        // Worker2 has pieces 1, 2, 3, 9, 11
        let worker2_bitfield = Bitfield::from(
            &create_bitfield(total_pieces, &[1, 2, 3, 6, 9, 11]),
            total_pieces,
        );
        peers_bitfield.insert("worker2".to_string(), worker2_bitfield);
        workers_pending_pieces.insert("worker2".to_string(), vec![6]); // Already has one piece assigned

        // Worker3 has pieces 0, 3, 5, 7, 10
        let worker3_bitfield = Bitfield::from(
            &create_bitfield(total_pieces, &[0, 3, 4, 5, 7, 10]),
            total_pieces,
        );
        peers_bitfield.insert("worker3".to_string(), worker3_bitfield);
        workers_pending_pieces.insert("worker1".to_string(), vec![4, 5]);

        // Call the function
        let assignments = Synchronizer::assign_pieces_to_workers(
            &peers_bitfield,
            &workers_pending_pieces,
            &pending_pieces,
            &assigned_pieces,
        );

        // Verify assignments
        assert!(assignments.contains_key("worker1"));
        assert!(assignments.contains_key("worker2"));
        assert!(assignments.contains_key("worker3"));

        // Check distribution is fair - since worker1 has 0 pending pieces, it should get assigned first
        let worker1_assignments = assignments.get("worker1").unwrap();
        let worker2_assignments = assignments.get("worker2").unwrap();
        let worker3_assignments = assignments.get("worker3").unwrap();

        // Worker1 should have pieces 7
        assert!(worker1_assignments.contains(&7));

        // Worker2 should have pieces 1, 6, 9, 11
        assert!(worker2_assignments.contains(&1));
        assert!(worker2_assignments.contains(&6));
        assert!(worker2_assignments.contains(&9));
        assert!(worker2_assignments.contains(&11));

        // Worker3 should have pieces 0, 3, 4, 5, 10
        assert!(worker3_assignments.contains(&0));
        assert!(worker3_assignments.contains(&3));
        assert!(worker3_assignments.contains(&4));
        assert!(worker3_assignments.contains(&5));
        assert!(worker3_assignments.contains(&10));

        // Piece 2 should not be assigned to anyone as it's already assigned
        assert!(!worker1_assignments.contains(&2));
        assert!(!worker2_assignments.contains(&2));
    }

    #[test]
    fn test_build_interested_pieces_bitfield() {
        let total_pieces = 13;
        let pieces = create_pieces_hashmap(total_pieces as u32, 16384);
        let (event_tx, _) = mpsc::channel(100);

        let mut sync = Synchronizer::new(pieces.clone(), 5, event_tx);

        // Set up state bitfield with pieces 0, 2, 4 completed
        sync.bitfield.set_piece(0);
        sync.bitfield.set_piece(2);
        sync.bitfield.set_piece(4);

        // Create peer bitfield with pieces 0, 1, 2, 3, 5, 9, 12
        let peer_bitfield = Bitfield::from(
            &create_bitfield(total_pieces, &[0, 1, 2, 3, 5, 9, 12]),
            total_pieces,
        );

        // Build interested pieces bitfield
        let interested = sync.build_interested_pieces_bitfield(&peer_bitfield);

        // Verify interested pieces are 1, 3, 5 (peer has them, we don't)
        assert!(!interested.has_piece(0)); // We already have piece 0
        assert!(interested.has_piece(1)); // We need piece 1
        assert!(!interested.has_piece(2)); // We already have piece 2
        assert!(interested.has_piece(3)); // We need piece 3
        assert!(!interested.has_piece(4)); // Peer doesn't have piece 4
        assert!(interested.has_piece(5)); // We need piece 5
        assert!(!interested.has_piece(6)); // Neither has piece 6
        assert!(!interested.has_piece(7)); // Neither has piece 7
        assert!(!interested.has_piece(8)); // Neither has piece 8
        assert!(interested.has_piece(9)); // We need piece 9
        assert!(!interested.has_piece(10)); // Neither has piece 10
        assert!(!interested.has_piece(11)); // Neither has piece 11
        assert!(interested.has_piece(12)); // We need piece 12
    }

    #[test]
    fn test_downloaded_pieces_count() {
        let total_pieces = 16;
        let pieces = create_pieces_hashmap(total_pieces as u32, 16384);
        let (event_tx, _) = mpsc::channel(100);

        let mut sync = Synchronizer::new(pieces.clone(), 5, event_tx);

        // Initially no pieces downloaded
        assert_eq!(sync.downloaded_pieces_count(), 0);

        // Set some pieces as downloaded
        sync.bitfield.set_piece(0);
        sync.bitfield.set_piece(5);
        sync.bitfield.set_piece(10);
        sync.bitfield.set_piece(15);

        // Should have 4 pieces downloaded
        assert_eq!(sync.downloaded_pieces_count(), 4);

        // Set more pieces
        sync.bitfield.set_piece(2);
        sync.bitfield.set_piece(3);

        // Should have 6 pieces downloaded
        assert_eq!(sync.downloaded_pieces_count(), 6);
    }

    #[test]
    fn test_assign_piece() {
        let total_pieces = 9;
        let pieces = create_pieces_hashmap(total_pieces as u32, 16384);
        let (event_tx, _) = mpsc::channel(100);

        let mut sync = Synchronizer::new(pieces.clone(), 5, event_tx);

        // Set up pending pieces
        sync.pending_pieces = vec![1, 2, 4, 0, 3];

        // Create peer bitfield with pieces 1, 3, 5, 7
        let peer_bitfield =
            Bitfield::from(&create_bitfield(total_pieces, &[1, 3, 5, 7]), total_pieces);

        // Assign first piece
        let piece = sync.assign_piece(&peer_bitfield);

        // Should assign piece 1 (first matching piece in pending_pieces)
        assert_eq!(piece, Some(1));

        // Verify piece is now assigned
        assert!(sync.assigned_pieces.contains(&1));

        // Try to assign another piece
        let piece = sync.assign_piece(&peer_bitfield);

        // Should assign piece 3 (next matching piece)
        assert_eq!(piece, Some(3));

        // Piece 5 is in peer's bitfield but not in pending_pieces
        // Piece 0, 2, 4 are in pending_pieces but not in peer's bitfield

        // Mark all pieces as assigned
        sync.assigned_pieces.insert(0);
        sync.assigned_pieces.insert(2);
        sync.assigned_pieces.insert(4);

        // Try to assign when all pieces are assigned
        let piece = sync.assign_piece(&peer_bitfield);

        // Should not assign any piece
        assert_eq!(piece, None);
    }

    #[test]
    fn test_unassign_piece() {
        let total_pieces = 8;
        let pieces = create_pieces_hashmap(total_pieces, 16384);
        let (event_tx, _) = mpsc::channel(100);

        let mut sync = Synchronizer::new(pieces, 5, event_tx);

        // Assign some pieces
        sync.assigned_pieces.insert(1);
        sync.assigned_pieces.insert(3);
        sync.assigned_pieces.insert(5);

        // Assign the pieces to the peer sessions
        sync.workers_pending_pieces
            .entry("peer1".to_string())
            .or_insert_with(Vec::new)
            .push(1);
        sync.workers_pending_pieces
            .entry("peer1".to_string())
            .or_insert_with(Vec::new)
            .push(3);
        sync.workers_pending_pieces
            .entry("peer2".to_string())
            .or_insert_with(Vec::new)
            .push(5);

        // Unassign piece 3
        sync.unassign_piece("peer1", 3);

        // Verify piece 3 is unassigned
        assert!(!sync.assigned_pieces.contains(&3));

        // Verify other pieces remain assigned
        assert!(sync.assigned_pieces.contains(&1));
        assert!(sync.assigned_pieces.contains(&5));

        // Verify piece 3 is not assigned to peer1
        assert!(!sync
            .workers_pending_pieces
            .get("peer1")
            .unwrap()
            .contains(&3));

        // Verify piece 1 remain assigned
        assert!(sync
            .workers_pending_pieces
            .get("peer1")
            .unwrap()
            .contains(&1));

        // Unassign a piece that's not assigned
        sync.unassign_piece("peer1", 2);

        // Should not affect assigned pieces
        assert_eq!(sync.assigned_pieces.len(), 2);
    }

    #[test]
    fn test_remove_pending_piece() {
        let pieces = create_pieces_hashmap(8, 16384);
        let (event_tx, _) = mpsc::channel(100);

        let mut sync = Synchronizer::new(pieces, 5, event_tx);

        // Set up pending pieces
        sync.pending_pieces = vec![0, 1, 2, 3, 4];

        // Remove piece 2
        sync.remove_pending_piece(2);

        // Verify piece is removed
        assert_eq!(sync.pending_pieces, vec![0, 1, 3, 4]);

        // Remove piece 0 (first in list)
        sync.remove_pending_piece(0);

        // Verify piece is removed
        assert_eq!(sync.pending_pieces, vec![1, 3, 4]);

        // Remove piece 4 (last in list)
        sync.remove_pending_piece(4);

        // Verify piece is removed
        assert_eq!(sync.pending_pieces, vec![1, 3]);

        // Remove a piece that's not in the list
        sync.remove_pending_piece(5);

        // List should not change
        assert_eq!(sync.pending_pieces, vec![1, 3]);
    }

    #[test]
    fn test_increase_pieces_rarity() {
        let pieces = create_pieces_hashmap(8, 16384);
        let (event_tx, _) = mpsc::channel(100);

        let mut sync = Synchronizer::new(pieces, 5, event_tx);

        // All pieces initially have rarity 0
        assert_eq!(sync.pieces_rarity, vec![0, 0, 0, 0, 0, 0, 0, 0]);

        // Create a bitfield with pieces 0, 2, 4, 6
        let bitfield = Bitfield::from(&create_bitfield(8, &[0, 2, 4, 6]), 8);

        // Increase rarity
        sync.increase_pieces_rarity(&bitfield);

        // Verify rarity is increased for pieces in bitfield
        assert_eq!(sync.pieces_rarity[0], 1);
        assert_eq!(sync.pieces_rarity[1], 0);
        assert_eq!(sync.pieces_rarity[2], 1);
        assert_eq!(sync.pieces_rarity[3], 0);
        assert_eq!(sync.pieces_rarity[4], 1);
        assert_eq!(sync.pieces_rarity[5], 0);
        assert_eq!(sync.pieces_rarity[6], 1);
        assert_eq!(sync.pieces_rarity[7], 0);

        // Create another bitfield with pieces 0, 1, 2, 3
        let bitfield2 = Bitfield::from(&create_bitfield(8, &[0, 1, 2, 3]), 8);

        // Increase rarity again
        sync.increase_pieces_rarity(&bitfield2);

        // Verify rarity is increased correctly
        assert_eq!(sync.pieces_rarity[0], 2); // 1 + 1
        assert_eq!(sync.pieces_rarity[1], 1); // 0 + 1
        assert_eq!(sync.pieces_rarity[2], 2); // 1 + 1
        assert_eq!(sync.pieces_rarity[3], 1); // 0 + 1
        assert_eq!(sync.pieces_rarity[4], 1); // Unchanged
        assert_eq!(sync.pieces_rarity[5], 0); // Unchanged
        assert_eq!(sync.pieces_rarity[6], 1); // Unchanged
        assert_eq!(sync.pieces_rarity[7], 0); // Unchanged
    }

    #[test]
    fn test_sort_pieces() {
        let pieces = create_pieces_hashmap(8, 16384);
        let (event_tx, _) = mpsc::channel(100);

        let mut sync = Synchronizer::new(pieces.clone(), 5, event_tx);

        // Set up pieces rarity
        sync.pieces_rarity = vec![3, 1, 0, 2, 1, 0, 4, 2];

        // Sort pieces
        let sorted = sync.sort_pieces();

        // Pieces should be sorted by rarity (rarest first)
        // 0 count pieces should be excluded
        // For equal rarity, sort by index
        assert_eq!(sorted, vec![1, 4, 3, 7, 0, 6]);
    }

    #[test]
    fn test_populate_queue() {
        let pieces = create_pieces_hashmap(8, 16384);
        let (event_tx, _) = mpsc::channel(100);

        let mut sync = Synchronizer::new(pieces, 5, event_tx);

        // Initial queue is empty
        assert!(sync.pending_pieces.is_empty());

        // Populate queue
        sync.populate_queue(vec![3, 1, 4, 2]);

        // Verify queue has been populated
        assert_eq!(sync.pending_pieces, vec![3, 1, 4, 2]);

        // Populate queue again (should clear previous entries)
        sync.populate_queue(vec![5, 0]);

        // Verify queue has been updated
        assert_eq!(sync.pending_pieces, vec![5, 0]);
    }

    #[test]
    fn test_remove_worker_pending_piece() {
        let pieces = create_pieces_hashmap(8, 16384);
        let (event_tx, _) = mpsc::channel(100);

        let mut sync = Synchronizer::new(pieces, 5, event_tx);

        // Set up worker pending pieces
        sync.workers_pending_pieces
            .insert("worker1".to_string(), vec![1, 3, 5]);
        sync.workers_pending_pieces
            .insert("worker2".to_string(), vec![2, 4, 6]);

        // Remove piece 3 from worker1
        sync.remove_worker_pending_piece(3);

        // Verify piece is removed from worker1
        assert_eq!(sync.workers_pending_pieces["worker1"], vec![1, 5]);

        // Verify worker2 is unaffected
        assert_eq!(sync.workers_pending_pieces["worker2"], vec![2, 4, 6]);

        // Remove piece 6 from worker2
        sync.remove_worker_pending_piece(6);

        // Verify piece is removed from worker2
        assert_eq!(sync.workers_pending_pieces["worker2"], vec![2, 4]);
    }

    #[test]
    fn test_download_progress_percent() {
        let total_pieces = 10;
        let pieces = create_pieces_hashmap(total_pieces as u32, 16384);
        let (event_tx, _) = mpsc::channel(100);

        let mut sync = Synchronizer::new(pieces, 5, event_tx);

        // No pieces downloaded yet
        assert_eq!(sync.download_progress_percent(), 0);

        // Mark 3 pieces as complete
        for i in 0..3 {
            sync.bitfield.set_piece(i);
        }

        // Should be 30% complete
        assert_eq!(sync.download_progress_percent(), 30);

        // Mark 2 more pieces as complete
        for i in 3..5 {
            sync.bitfield.set_piece(i);
        }

        // Should be 50% complete
        assert_eq!(sync.download_progress_percent(), 50);

        // Mark all remaining pieces as complete
        for i in 5..total_pieces {
            sync.bitfield.set_piece(i);
        }

        // Should be 100% complete
        assert_eq!(sync.download_progress_percent(), 100);
    }
}
