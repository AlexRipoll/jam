use std::{
    collections::{HashMap, HashSet, VecDeque},
    error::Error,
    fmt::Display,
    time::Duration,
};

use protocol::{bitfield::Bitfield, piece::Piece};
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{debug, error, info};

use crate::torrent::events::Event;

/// The Synchronizer coordinates piece downloads across multiple peer connections,
/// managing piece assignment, tracking download progress, and handling the overall
/// download state for a torrent.
///
/// It implements a rarest-first piece selection strategy to optimize download efficiency
/// and ensure pieces are distributed fairly among available peers.
#[derive(Debug)]
pub struct Synchronizer {
    /// Metadata about each piece in the torrent
    pub pieces: HashMap<u32, Piece>,

    /// Bitfield of the pieces that have been successfully downloaded
    pub bitfield: Bitfield,

    /// Map of peer IDs to their corresponding bitfields (the pieces they have)
    peers_bitfield: HashMap<String, Bitfield>,

    /// Tracks the rarity of each piece (how many peers have it)
    pieces_rarity: Vec<u8>,

    /// Sorted list of piece indexes pending download, prioritized by rarity
    pending_pieces: Vec<u32>,

    /// Set of piece indexes assigned to a peer for being downloaded
    assigned_pieces: HashSet<u32>,

    /// Map of peer IDs to the pieces they're currently downloading
    peer_assignments: HashMap<String, Vec<u32>>,

    /// Maximum number of pieces a peer can download simultaneously
    max_peer_queue_size: usize,

    /// Channel for sending events to the torrent manager
    event_tx: mpsc::Sender<Event>,

    /// Flag indicating whether a dispatch operation is currently in progress
    dispatch_in_progress: bool,
}

/// Represents the rarity information of a piece
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PieceRarity {
    /// Number of peers that have this piece
    count: u8,
    /// Index of the piece
    index: u32,
}

/// Commands that can be sent to the Synchronizer actor
#[derive(Debug)]
pub enum SynchronizerCommand {
    // Dispatch related commands
    /// Request that pieces be assigned to peers for downloading
    RequestDispatch,
    /// Notification that a piece has been assigned to a peer
    PieceDispatched {
        session_id: String,
        piece_index: u32,
    },
    /// Notification that the dispatch process has completed
    DispatchCompleted,

    // Peer management commands
    /// Process a bitfield received from a peer
    ProcessBitfield {
        session_id: String,
        bitfield: Vec<u8>,
    },
    /// Process a "have" message from a peer indicating they have a specific piece
    ProcessHave {
        session_id: String,
        piece_index: u32,
    },
    /// Handle a corrupted piece received from a peer
    CorruptedPiece {
        session_id: String,
        piece_index: u32,
    },
    /// Close a peer connection and clean up associated state
    ClosePeerSession(String),

    // Piece management commands
    /// Mark a piece as successfully downloaded and verified
    MarkPieceComplete(u32),
    /// Unassign a piece from a peer
    UnassignPiece {
        session_id: String,
        piece_index: u32,
    },
    /// Unassign multiple pieces from a peer
    UnassignPieces {
        session_id: String,
        pieces_index: Vec<u32>,
    },

    // Query commands (with response channels)
    /// Query the current download progress percentage
    QueryProgress(mpsc::Sender<u32>),
    /// Query whether the download is complete
    QueryIsCompleted(mpsc::Sender<bool>),
    /// Query whether there are active workers downloading pieces
    QueryHasActiveWorkers(mpsc::Sender<bool>),
}

/// Data structure to pass to the dispatch task
#[derive(Debug, Clone)]
pub struct DispatchData {
    /// Metadata about each piece in the torrent
    pieces: HashMap<u32, Piece>,
    /// Map of peer IDs to their corresponding bitfields
    peers_bitfield: HashMap<String, Bitfield>,
    /// Map of peer IDs to the pieces they're currently downloading
    peer_assignments: HashMap<String, Vec<u32>>,
    /// Sorted list of piece indexes pending download
    pending_pieces: Vec<u32>,
    /// Set of piece indexes assigned to a peer for being downloaded
    assigned_pieces: HashSet<u32>,
    /// Maximum number of pieces a peer can download simultaneously
    max_peer_queue_size: usize,
}

impl Synchronizer {
    /// Creates a new Synchronizer with the specified pieces and configuration.
    ///
    /// # Arguments
    ///
    /// * `pieces` - Metadata about each piece in the torrent
    /// * `max_peer_queue_size` - Maximum number of pieces that can be assigned to a peer
    /// * `event_tx` - Channel for sending events to the torrent manager
    ///
    /// # Returns
    ///
    /// A new Synchronizer instance
    pub fn new(
        pieces: HashMap<u32, Piece>,
        max_peer_queue_size: usize,
        event_tx: mpsc::Sender<Event>,
    ) -> Self {
        let total_pieces = pieces.len();

        Self {
            pieces,
            bitfield: Bitfield::new(total_pieces),
            peers_bitfield: HashMap::new(),
            pieces_rarity: vec![0u8; total_pieces],
            pending_pieces: Vec::new(),
            assigned_pieces: HashSet::new(),
            peer_assignments: HashMap::new(),
            max_peer_queue_size,
            dispatch_in_progress: false,
            event_tx,
        }
    }

    /// Starts the synchronizer actor and returns a command sender and task handle.
    ///
    /// This method spawns two Tokio tasks:
    /// - The main actor task that processes commands
    /// - A dispatch task that handles piece assignment to peers
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - A sender for sending commands to the synchronizer
    /// - A JoinHandle for the main actor task
    pub fn run(mut self) -> (mpsc::Sender<SynchronizerCommand>, JoinHandle<()>) {
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<SynchronizerCommand>(128);
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

    /// Handles the dispatch of pieces to peers in a separate task.
    ///
    /// # Arguments
    ///
    /// * `data` - The dispatch data containing the current state
    /// * `dispatch_cmd_tx` - A sender for sending commands back to the synchronizer
    /// * `event_tx` - A sender for sending events to the torrent manager
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure
    async fn handle_dispatch(
        data: DispatchData,
        dispatch_cmd_tx: &mpsc::Sender<SynchronizerCommand>,
        event_tx: &mpsc::Sender<Event>,
    ) -> Result<(), SynchronizerError> {
        // Create workers queue locally
        let workers_queue = Self::assign_pieces_to_peer_session(
            &data.peers_bitfield,
            &data.peer_assignments,
            &data.pending_pieces,
            &data.assigned_pieces,
        );

        // Process each worker's queue
        for (worker_id, mut queue) in workers_queue {
            let worker_pending = data
                .peer_assignments
                .get(&worker_id)
                .map(|v| v.len())
                .unwrap_or(0);

            let available_slots = data.max_peer_queue_size.saturating_sub(worker_pending);
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

    /// Handles a command received by the synchronizer actor.
    ///
    /// This method processes various commands to manage the download state,
    /// peer connections, and piece assignments.
    ///
    /// # Arguments
    ///
    /// * `command` - The command to process
    /// * `dispatch_tx` - A sender for sending dispatch data to the dispatch task
    /// * `actor_cmd_tx` - A sender for sending commands back to this actor
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure
    async fn handle_command(
        &mut self,
        command: SynchronizerCommand,
        dispatch_tx: &mpsc::Sender<DispatchData>,
        actor_cmd_tx: &mpsc::Sender<SynchronizerCommand>,
    ) -> Result<(), SynchronizerError> {
        match command {
            SynchronizerCommand::RequestDispatch => {
                self.handle_request_dispatch(dispatch_tx).await?;
            }
            SynchronizerCommand::PieceDispatched {
                session_id,
                piece_index,
            } => {
                self.handle_piece_dispatched(session_id, piece_index);
            }
            SynchronizerCommand::DispatchCompleted => {
                self.handle_dispatch_completed(actor_cmd_tx).await?;
            }
            SynchronizerCommand::ProcessBitfield {
                session_id,
                bitfield,
            } => {
                self.handle_process_bitfield(session_id, bitfield, actor_cmd_tx)
                    .await?;
            }
            SynchronizerCommand::ProcessHave {
                session_id,
                piece_index,
            } => {
                self.handle_process_have(session_id, piece_index);
            }
            SynchronizerCommand::CorruptedPiece {
                session_id,
                piece_index,
            } => {
                self.handle_corrupted_piece(session_id, piece_index);
            }
            SynchronizerCommand::ClosePeerSession(session_id) => {
                self.remove_session(&session_id);
            }
            SynchronizerCommand::MarkPieceComplete(piece_index) => {
                self.handle_mark_piece_complete(piece_index).await?;
            }
            SynchronizerCommand::UnassignPiece {
                session_id,
                piece_index,
            } => {
                self.unassign_piece(&session_id, piece_index);
            }
            SynchronizerCommand::UnassignPieces {
                session_id,
                pieces_index,
            } => {
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
                let has_active = self.has_active_sessions();
                response_tx.send(has_active).await?;
            }
        }

        Ok(())
    }

    /// Handles a request to dispatch pieces to peers.
    ///
    /// Initiates the dispatch process if there are pieces to be assigned
    /// and no dispatch is currently in progress.
    async fn handle_request_dispatch(
        &mut self,
        dispatch_tx: &mpsc::Sender<DispatchData>,
    ) -> Result<(), SynchronizerError> {
        if !self.dispatch_in_progress && (self.pending_pieces.len() > self.assigned_pieces.len()) {
            self.dispatch_in_progress = true;

            // Prepare data for the dispatch task
            let data = DispatchData {
                pieces: self.pieces.clone(),
                peers_bitfield: self.peers_bitfield.clone(),
                peer_assignments: self.peer_assignments.clone(),
                pending_pieces: self.pending_pieces.clone(),
                assigned_pieces: self.assigned_pieces.clone(),
                max_peer_queue_size: self.max_peer_queue_size,
            };

            dispatch_tx.send(data).await?;
        }

        Ok(())
    }

    /// Updates internal state after a piece has been dispatched to a peer.
    fn handle_piece_dispatched(&mut self, session_id: String, piece_index: u32) {
        // Update internal state
        self.peer_assignments
            .entry(session_id)
            .or_insert_with(Vec::new)
            .push(piece_index);
        self.assigned_pieces.insert(piece_index);
    }

    /// Handles the completion of a dispatch operation.
    ///
    /// Resets the dispatch_in_progress flag and potentially
    /// initiates another dispatch if there are more pieces to assign.
    async fn handle_dispatch_completed(
        &mut self,
        actor_cmd_tx: &mpsc::Sender<SynchronizerCommand>,
    ) -> Result<(), SynchronizerError> {
        self.dispatch_in_progress = false;

        // If pending pieces list length is greater than assigned pieces length,
        // it means there are still pieces to be dispatched
        if self.pending_pieces.len() > self.assigned_pieces.len() {
            actor_cmd_tx
                .send(SynchronizerCommand::RequestDispatch)
                .await?;
        }

        Ok(())
    }

    /// Processes a bitfield received from a peer.
    ///
    /// Updates internal state with the peer's available pieces and
    /// potentially initiates a dispatch operation.
    async fn handle_process_bitfield(
        &mut self,
        session_id: String,
        bitfield: Vec<u8>,
        actor_cmd_tx: &mpsc::Sender<SynchronizerCommand>,
    ) -> Result<(), SynchronizerError> {
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

        Ok(())
    }

    /// Processes a "have" message from a peer indicating they have a specific piece.
    fn handle_process_have(&mut self, session_id: String, piece_index: u32) {
        debug!(
            session_id = %session_id,
            piece_index = %piece_index,
            "'Have' message received"
        );
        if let Some(bitfield) = self.peers_bitfield.get_mut(&session_id) {
            bitfield.set_piece(piece_index as usize);
        }
        // Update rarity information for this piece
        if (piece_index as usize) < self.pieces_rarity.len() {
            self.pieces_rarity[piece_index as usize] =
                self.pieces_rarity[piece_index as usize].saturating_add(1);
        }

        // If this is a piece we're missing and it's not already in our pending list,
        // add it and re-sort our pending pieces
        if !self.bitfield.has_piece(piece_index as usize)
            && !self.pending_pieces.contains(&piece_index)
        {
            self.pending_pieces.push(piece_index);
            self.pending_pieces = self.sort_pieces();
        }
    }

    /// Handles a corrupted piece received from a peer.
    fn handle_corrupted_piece(&mut self, session_id: String, piece_index: u32) {
        debug!(
            session_id = %session_id,
            piece_index = %piece_index,
            "Corrupted piece"
        );
        self.unassign_piece(&session_id, piece_index);
        // TODO: Consider blacklisting this peer for this piece
    }

    /// Marks a piece as successfully downloaded and verified.
    ///
    /// Updates internal state and potentially disconnects peers
    /// that have completed their work.
    async fn handle_mark_piece_complete(
        &mut self,
        piece_index: u32,
    ) -> Result<(), SynchronizerError> {
        debug!(
            piece_index = %piece_index,
            "Piece completed"
        );
        self.mark_piece_complete(piece_index);

        // Check if any peer session has completed all their work
        let peers_to_disconnect: Vec<String> = self
            .peer_assignments
            .iter()
            .filter_map(|(session_id, pending_pieces)| {
                if pending_pieces.is_empty() && !self.has_assignable_pieces_for_peer(session_id) {
                    Some(session_id.clone())
                } else {
                    None
                }
            })
            .collect();

        // Disconnect peers with no more work
        for session_id in peers_to_disconnect {
            self.event_tx
                .send(Event::DisconnectPeerSession {
                    session_id: session_id.clone(),
                })
                .await?;
            self.remove_session(&session_id);
        }

        info!("Download progress: {}%", self.download_progress_percent());

        if self.is_completed() {
            info!("Download completed");
            self.event_tx.send(Event::DownloadCompleted).await?;
        }

        Ok(())
    }

    /// Function that runs in a separate task to assign pieces to peer session.
    ///
    /// This method implements the rarest-first piece selection strategy and
    /// distributes pieces fairly among available peers.
    ///
    /// # Arguments
    ///
    /// * `peers_bitfield` - Map of peer IDs to their available pieces
    /// * `peer_assignments` - Map of peer IDs to pieces they're currently downloading
    /// * `pending_pieces` - List of pieces pending download
    /// * `assigned_pieces` - Set of pieces already assigned for download
    ///
    /// # Returns
    ///
    /// A map of peer IDs to queues of piece indexes to download
    fn assign_pieces_to_peer_session(
        peers_bitfield: &HashMap<String, Bitfield>,
        peer_assignments: &HashMap<String, Vec<u32>>,
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
        worker_pieces
            .sort_by_key(|(worker_id, _)| peer_assignments.get(worker_id).map_or(0, |v| v.len()));

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

    /// Processes a bitfield received from a peer.
    ///
    /// Updates internal state with the peer's available pieces and
    /// recalculates piece rarity.
    fn process_bitfield(&mut self, session_id: &str, peer_bitfield: Bitfield) {
        let missing_pieces_bitfield = self.build_interested_pieces_bitfield(&peer_bitfield);
        self.peers_bitfield
            .insert(session_id.to_string(), peer_bitfield);
        self.peer_assignments
            .entry(session_id.to_string())
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
    ///
    /// A bitfield where each bit represents a piece available from the peer but not yet downloaded
    fn build_interested_pieces_bitfield(&self, peer_bitfield: &Bitfield) -> Bitfield {
        let mut missing = Vec::new();

        for (peer_byte, state_byte) in peer_bitfield.bytes.iter().zip(self.bitfield.bytes.iter()) {
            missing.push(peer_byte & !state_byte);
        }

        Bitfield::from(&missing, self.bitfield.total_pieces)
    }

    /// Returns the count of downloaded pieces.
    fn downloaded_pieces_count(&self) -> u32 {
        self.bitfield
            .bytes
            .iter()
            .map(|byte| byte.count_ones())
            .sum::<u32>()
    }

    /// Unassigns a piece from a peer.
    ///
    /// Removes the piece from the peer's assignment list and from the
    /// global assigned pieces set.
    fn unassign_piece(&mut self, session_id: &str, piece_index: u32) {
        self.assigned_pieces.remove(&piece_index);
        if let Some(pending_pieces) = self.peer_assignments.get_mut(session_id) {
            if let Some(pos) = pending_pieces.iter().position(|&p| p == piece_index) {
                pending_pieces.remove(pos);
            }
        }
    }

    /// Unassigns multiple pieces from a peer.
    fn unassign_pieces(&mut self, session_id: &str, pieces_index: Vec<u32>) {
        for piece_index in pieces_index {
            self.unassign_piece(session_id, piece_index);
        }
    }

    /// Removes a pending piece from the list of pieces to download.
    fn remove_pending_piece(&mut self, piece_index: u32) {
        for (pos, &piece_idx) in self.pending_pieces.iter().enumerate() {
            if piece_idx == piece_index {
                self.pending_pieces.remove(pos as usize);
                break;
            }
        }
    }

    /// Removes a piece from all peer assignment lists.
    fn remove_worker_pending_piece(&mut self, piece_index: u32) {
        for (_, values) in self.peer_assignments.iter_mut() {
            if let Some(pos) = values.iter().position(|&x| x == piece_index) {
                values.remove(pos);
                break;
            }
        }
    }

    /// Marks a piece as successfully downloaded and verified.
    ///
    /// Updates the bitfield, removes the piece from pending and assigned lists,
    /// and updates rarity information.
    fn mark_piece_complete(&mut self, piece_index: u32) {
        self.bitfield.set_piece(piece_index as usize);
        self.remove_pending_piece(piece_index);
        self.remove_worker_pending_piece(piece_index);
        self.assigned_pieces.remove(&piece_index);
        // set to 0 so it is not taken in consideration since it is already set in the `bitfield` field.
        self.pieces_rarity[piece_index as usize] = 0;
    }

    /// Increases the rarity count for pieces available in the given bitfield.
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

    /// Decreases the rarity count for pieces available in the given bitfield.
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

    /// Sorts pieces by rarity (rarest first).
    ///
    /// # Returns
    ///
    /// A sorted vector of piece indexes, prioritizing rarest pieces
    fn sort_pieces(&self) -> Vec<u32> {
        // Create a vector of pieces with their rarity information
        let mut piece_rarities: Vec<PieceRarity> = self
            .pieces_rarity
            .iter()
            .enumerate()
            .filter(|&(_, &count)| count > 0)
            .map(|(index, &count)| PieceRarity {
                count,
                index: index as u32,
            })
            .collect();

        // Sort by rarity (ascending - rarest first) and then by piece index
        piece_rarities.sort_by(|a, b| a.count.cmp(&b.count).then_with(|| a.index.cmp(&b.index)));

        // Extract just the piece indexes
        piece_rarities
            .into_iter()
            .map(|rarity| rarity.index)
            .collect()
    }

    /// Updates the queue of pending pieces based on rarity information.
    fn update_pending_pieces(&mut self) {
        self.pending_pieces = self.sort_pieces();
    }

    /// Checks if the peer has pieces that we still need.
    ///
    /// # Arguments
    ///
    /// * `peer_bitfield` - The peer's bitfield of available pieces
    ///
    /// # Returns
    ///
    /// True if the peer has pieces we need, false otherwise
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

    /// Checks if we already have all pieces available from a peer.
    ///
    /// # Arguments
    ///
    /// * `peer_bitfield` - The peer's bitfield of available pieces
    ///
    /// # Returns
    ///
    /// True if we have all pieces from this peer, false otherwise
    fn has_downloaded_all_peer_pieces(&self, peer_bitfield: &Bitfield) -> bool {
        if peer_bitfield.bytes.is_empty() {
            return false;
        }

        peer_bitfield
            .bytes
            .iter()
            .zip(self.bitfield.bytes.iter())
            .all(|(&peer_byte, &self_byte)| (peer_byte & self_byte) == peer_byte)
    }

    /// Checks if there are pieces we can assign to a peer.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - The ID of the peer to check
    ///
    /// # Returns
    ///
    /// True if there are assignable pieces for this peer, false otherwise
    fn has_assignable_pieces_for_peer(&self, session_id: &str) -> bool {
        if let Some(peer_bitfield) = self.peers_bitfield.get(session_id) {
            self.missing_unassigned_pieces(peer_bitfield).len() > 0
        } else {
            false
        }
    }

    /// Identifies pieces that are missing and not assigned that a peer has.
    ///
    /// # Arguments
    ///
    /// * `peer_bitfield` - The peer's bitfield of available pieces
    ///
    /// # Returns
    ///
    /// A vector of piece indexes that can be assigned to this peer
    fn missing_unassigned_pieces(&self, peer_bitfield: &Bitfield) -> Vec<usize> {
        (0..peer_bitfield.total_pieces)
            .filter(|&piece_index| {
                peer_bitfield.has_piece(piece_index)
                    && !self.bitfield.has_piece(piece_index)
                    && !self.assigned_pieces.contains(&(piece_index as u32))
            })
            .collect()
    }

    /// Checks if any peers have pieces still in progress.
    ///
    /// # Returns
    ///
    /// True if there are active downloads, false otherwise
    fn has_active_sessions(&self) -> bool {
        self.peer_assignments
            .iter()
            .any(|(_, pending_pieces)| !pending_pieces.is_empty())
    }

    /// Checks if all pieces have been downloaded.
    ///
    /// # Returns
    ///
    /// True if the download is complete, false otherwise
    fn is_completed(&self) -> bool {
        self.bitfield.has_all_pieces()
    }

    /// Calculates the download progress as a percentage.
    ///
    /// # Returns
    ///
    /// The percentage of pieces downloaded (0-100)
    fn download_progress_percent(&self) -> u32 {
        let downloaded_pieces = self.downloaded_pieces_count();

        downloaded_pieces * 100 / self.bitfield.total_pieces as u32
    }

    /// Removes a peer and cleans up associated state.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - The ID of the peer to remove
    fn remove_session(&mut self, session_id: &str) {
        // Get the peer's bitfield to update rarity counts
        if let Some(peer_bitfield) = self.peers_bitfield.remove(session_id) {
            // Decrease rarity counts for pieces this peer had
            self.decrease_pieces_rarity(&peer_bitfield);
        }

        // Unassign any pieces this peer was downloading
        if let Some(pieces_indexes) = self.peer_assignments.remove(session_id) {
            self.unassign_pieces(session_id, pieces_indexes);
        }

        // Re-sort pieces since rarity information changed
        self.update_pending_pieces();
    }
}

#[derive(Debug)]
pub enum SynchronizerError {
    /// Error sending dispatch data
    DispatcherDataTxError(mpsc::error::SendError<DispatchData>),
    /// Error processing a command
    CommandTxError(mpsc::error::SendError<SynchronizerCommand>),
    /// Error sending an orchestrator event
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

impl std::error::Error for SynchronizerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            SynchronizerError::DispatcherDataTxError(err) => Some(err),
            SynchronizerError::CommandTxError(err) => Some(err),
            SynchronizerError::EventTxError(err) => Some(err),
            _ => None,
        }
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
        assert_eq!(sync.max_peer_queue_size, 5);
        assert_eq!(sync.bitfield.total_pieces, 10);
        assert!(sync.peers_bitfield.is_empty());
        assert_eq!(sync.pieces_rarity.len(), 10);
        assert!(sync.pending_pieces.is_empty());
        assert!(sync.assigned_pieces.is_empty());
        assert!(sync.peer_assignments.is_empty());
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
        assert!(sync.peer_assignments.contains_key("peer1"));
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
        sync.peer_assignments
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
        assert!(!sync.peer_assignments["peer1"].contains(&1));

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

        // Assign some pieces to the peer
        let mut peer_pieces = Vec::new();
        peer_pieces.push(1);
        peer_pieces.push(2);
        sync.peer_assignments
            .insert("peer1".to_string(), peer_pieces);

        // Mark these pieces as assigned
        sync.assigned_pieces.insert(1);
        sync.assigned_pieces.insert(2);

        // Close the session
        sync.remove_session("peer1");

        // Verify the session is removed
        assert!(!sync.peers_bitfield.contains_key("peer1"));
        assert!(!sync.peer_assignments.contains_key("peer1"));

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
        let mut peer_assignments = HashMap::new();
        let pending_pieces = vec![0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11];
        let assigned_pieces = HashSet::from([2]); // Set piece 2 as already assigned

        // Worker1 has pieces 0, 1, 2, 7
        let worker1_bitfield =
            Bitfield::from(&create_bitfield(total_pieces, &[0, 1, 2, 7]), total_pieces);
        peers_bitfield.insert("worker1".to_string(), worker1_bitfield);
        peer_assignments.insert("worker1".to_string(), vec![]);

        // Worker2 has pieces 1, 2, 3, 9, 11
        let worker2_bitfield = Bitfield::from(
            &create_bitfield(total_pieces, &[1, 2, 3, 6, 9, 11]),
            total_pieces,
        );
        peers_bitfield.insert("worker2".to_string(), worker2_bitfield);
        peer_assignments.insert("worker2".to_string(), vec![6]); // Already has one piece assigned

        // Worker3 has pieces 0, 3, 5, 7, 10
        let worker3_bitfield = Bitfield::from(
            &create_bitfield(total_pieces, &[0, 3, 4, 5, 7, 10]),
            total_pieces,
        );
        peers_bitfield.insert("worker3".to_string(), worker3_bitfield);
        peer_assignments.insert("worker1".to_string(), vec![4, 5]);

        // Call the function
        let assignments = Synchronizer::assign_pieces_to_peer_session(
            &peers_bitfield,
            &peer_assignments,
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
        sync.peer_assignments
            .entry("peer1".to_string())
            .or_insert_with(Vec::new)
            .push(1);
        sync.peer_assignments
            .entry("peer1".to_string())
            .or_insert_with(Vec::new)
            .push(3);
        sync.peer_assignments
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
        assert!(!sync.peer_assignments.get("peer1").unwrap().contains(&3));

        // Verify piece 1 remain assigned
        assert!(sync.peer_assignments.get("peer1").unwrap().contains(&1));

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
    fn test_remove_worker_pending_piece() {
        let pieces = create_pieces_hashmap(8, 16384);
        let (event_tx, _) = mpsc::channel(100);

        let mut sync = Synchronizer::new(pieces, 5, event_tx);

        // Set up worker pending pieces
        sync.peer_assignments
            .insert("worker1".to_string(), vec![1, 3, 5]);
        sync.peer_assignments
            .insert("worker2".to_string(), vec![2, 4, 6]);

        // Remove piece 3 from worker1
        sync.remove_worker_pending_piece(3);

        // Verify piece is removed from worker1
        assert_eq!(sync.peer_assignments["worker1"], vec![1, 5]);

        // Verify worker2 is unaffected
        assert_eq!(sync.peer_assignments["worker2"], vec![2, 4, 6]);

        // Remove piece 6 from worker2
        sync.remove_worker_pending_piece(6);

        // Verify piece is removed from worker2
        assert_eq!(sync.peer_assignments["worker2"], vec![2, 4]);
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
