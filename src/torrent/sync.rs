use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use protocol::{bitfield::Bitfield, piece::Piece};
use tokio::{sync::mpsc, task::JoinHandle};

use super::events::Event;

#[derive(Debug)]
pub struct Synchronizer {
    pub pieces: HashMap<u32, Piece>,

    // Bitfield of the downloaded pieces
    pub bitfield: Bitfield,

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
    event_tx: Arc<mpsc::Sender<Event>>,

    // Channel to notify when pieces should be dispatched
    dispatch_notifier: mpsc::Sender<DispatchData>,

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
    UnassignPiece(u32),
    UnassignPieces(Vec<u32>),

    // Query commands (with response channels)
    QueryProgress(mpsc::Sender<u32>),
    QueryIsCompleted(mpsc::Sender<bool>),
    QueryHasActiveWorkers(mpsc::Sender<bool>),
}

// Data structure to pass data to the dispatch task
#[derive(Debug, Clone)]
struct DispatchData {
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
        event_tx: Arc<mpsc::Sender<Event>>,
    ) -> Self {
        let total_pieces = pieces.len();

        // Create a placeholder channel that will be replaced in start()
        let (tx, _) = mpsc::channel(1);

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
            dispatch_notifier: tx,
            dispatch_in_progress: false,
        }
    }

    pub fn run(mut self) -> (mpsc::Sender<SynchronizerCommand>, JoinHandle<()>) {
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<SynchronizerCommand>(100);
        let (dispatch_tx, mut dispatch_rx) = mpsc::channel::<DispatchData>(1);

        self.dispatch_notifier = dispatch_tx;

        let event_tx_clone = self.event_tx.clone();
        let dispatch_cmd_tx = cmd_tx.clone();
        let actor_cmd_tx = cmd_tx.clone();

        // Spawn a separate task for handling dispatch operations
        let dispatch_handle = tokio::spawn(async move {
            while let Some(data) = dispatch_rx.recv().await {
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
                            let _ = event_tx_clone
                                .send(Event::PieceDispatch {
                                    session_id: worker_id.clone(),
                                    piece: piece.clone(),
                                })
                                .await;

                            // Notify synchronizer about dispatched piece
                            let _ = dispatch_cmd_tx
                                .send(SynchronizerCommand::PieceDispatched {
                                    session_id: worker_id.clone(),
                                    piece_index,
                                })
                                .await;

                            // Small delay to prevent flooding
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    }
                }

                // Notify completion
                let _ = dispatch_cmd_tx
                    .send(SynchronizerCommand::DispatchCompleted)
                    .await;
            }
        });

        // Main actor task
        let actor_handle = tokio::spawn(async move {
            // Store handle to ensure it is dropped properly when actor_handle completes
            let _dispatch_handle = dispatch_handle;

            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
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

                            let _ = self.dispatch_notifier.send(data).await;
                        }
                    }
                    SynchronizerCommand::PieceDispatched {
                        session_id: worker_id,
                        piece_index,
                    } => {
                        // Update internal state
                        self.workers_pending_pieces
                            .entry(worker_id)
                            .or_insert_with(Vec::new)
                            .push(piece_index);
                        self.assigned_pieces.insert(piece_index);
                        self.remove_pending_piece(piece_index);
                    }
                    SynchronizerCommand::DispatchCompleted => {
                        self.dispatch_in_progress = false;

                        // If there are still pending pieces, request another dispatch
                        if !self.pending_pieces.is_empty() {
                            let _ = actor_cmd_tx
                                .send(SynchronizerCommand::RequestDispatch)
                                .await;
                        }
                    }
                    SynchronizerCommand::ProcessBitfield {
                        session_id: peer_id,
                        bitfield,
                    } => {
                        let total_pieces = self.bitfield.total_pieces;
                        let bitfield = Bitfield::from(&bitfield, total_pieces);

                        if self.has_missing_pieces(&bitfield) {
                            self.process_bitfield(&peer_id, bitfield);

                            // After processing a new bitfield, we might have new pieces to dispatch
                            if !self.pending_pieces.is_empty() && !self.dispatch_in_progress {
                                let _ = actor_cmd_tx
                                    .send(SynchronizerCommand::RequestDispatch)
                                    .await;
                            }
                        } else {
                            self.close_session(peer_id);
                            // TODO: send shutdown event to peer
                            // self.self
                            //     .event_tx
                            //     .send(Event::State(StateEvent::ClosePeerSession(
                            //         worker_id.clone(),
                            //     )))
                            //     .await;
                        }
                    }
                    SynchronizerCommand::ProcessHave {
                        session_id: peer_id,
                        piece_index,
                    } => {
                        if let Some(bitfield) = self.peers_bitfield.get_mut(&peer_id) {
                            bitfield.set_piece(piece_index as usize);
                        }
                        // TODO: recalculate piece rarity?
                    }
                    SynchronizerCommand::CorruptedPiece {
                        session_id: peer_id,
                        piece_index,
                    } => {
                        self.unassign_piece(piece_index);
                        // TODO: blacklist worker for this piece index
                    }
                    SynchronizerCommand::ClosePeerSession(worker_id) => {
                        self.close_session(worker_id);
                        // TODO: send shutdown event to peer
                        // self.self
                        //     .event_tx
                        //     .send(Event::State(StateEvent::ClosePeerSession(
                        //         worker_id.clone(),
                        //     )))
                        //     .await;
                    }
                    SynchronizerCommand::MarkPieceComplete(piece_index) => {
                        self.mark_piece_complete(piece_index);
                    }
                    SynchronizerCommand::UnassignPiece(piece_index) => {
                        self.unassign_piece(piece_index);
                    }
                    SynchronizerCommand::UnassignPieces(piece_indexes) => {
                        self.unassign_pieces(piece_indexes);
                    }
                    SynchronizerCommand::QueryProgress(response_tx) => {
                        let progress = self.download_progress_percent();
                        let _ = response_tx.send(progress).await;
                    }
                    SynchronizerCommand::QueryIsCompleted(response_tx) => {
                        let completed = self.is_completed();
                        let _ = response_tx.send(completed).await;
                    }
                    SynchronizerCommand::QueryHasActiveWorkers(response_tx) => {
                        let has_active = self.has_active_worker();
                        let _ = response_tx.send(has_active).await;
                    }
                }
            }
        });

        (cmd_tx, actor_handle)
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

    fn unassign_piece(&mut self, piece_index: u32) {
        self.assigned_pieces.remove(&piece_index);
    }

    fn unassign_pieces(&mut self, pieces_index: Vec<u32>) {
        for piece_index in pieces_index {
            self.unassign_piece(piece_index);
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
    fn has_concluded(&self, peer_bitfield: &Bitfield) -> bool {
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

    fn close_session(&mut self, id: String) {
        self.active_sessions.remove(&id);
        self.peers_bitfield.remove(&id);
        if let Some(pieces_indexes) = self.workers_pending_pieces.remove(&id) {
            self.unassign_pieces(pieces_indexes);
        }
    }
}
//
// #[cfg(test)]
// mod test {
//     use super::*;
//
//     pub fn mock_pieces(total_piece: u32) -> HashMap<u32, Piece> {
//         (0..total_piece)
//             .map(|i| (i, Piece::new(i, 1024, [i as u8; 20])))
//             .collect()
//     }
//
//     #[test]
//     fn test_process_bitfield() {
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//
//         let peer1 = (
//             "peer1_id",
//             Bitfield::from(&vec![0b11001000, 0b0], total_pieces as usize),
//         );
//         let peer2 = (
//             "peer2_id",
//             Bitfield::from(&vec![0b01001101, 0b10000000], total_pieces as usize),
//         );
//         let peer3 = (
//             "peer3_id",
//             Bitfield::from(&vec![0b11100010, 0b01010000], total_pieces as usize),
//         );
//
//         // process peer1 bitfield
//         state.process_bitfield(peer1.0, peer1.1.clone());
//         assert_eq!(state.peers_bitfield.get(peer1.0), Some(&peer1.1));
//         assert_eq!(
//             state.pieces_rarity,
//             vec![1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
//         );
//         assert_eq!(state.pending_pieces, vec![0, 1, 4]);
//
//         // process peer2 bitfield
//         state.process_bitfield(peer2.0, peer2.1.clone());
//         assert_eq!(state.peers_bitfield.get(peer2.0), Some(&peer2.1));
//         assert_eq!(
//             state.pieces_rarity,
//             vec![1, 2, 0, 0, 2, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0]
//         );
//         assert_eq!(state.pending_pieces, vec![0, 5, 7, 8, 1, 4]);
//
//         // process peer3 bitfield
//         state.process_bitfield(peer3.0, peer3.1.clone());
//         assert_eq!(state.peers_bitfield.get(peer3.0), Some(&peer3.1));
//         assert_eq!(
//             state.pieces_rarity,
//             vec![2, 3, 1, 0, 2, 1, 1, 1, 1, 1, 0, 1, 0, 0, 0]
//         );
//         assert_eq!(state.pending_pieces, vec![2, 5, 6, 7, 8, 9, 11, 0, 4, 1]);
//     }
//
//     #[test]
//     fn test_build_interested_pieces_bitfield_empty_state_bitfield() {
//         // Test the case where the peer has pieces that have not been downloaded yet.
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let state = State::new(pieces);
//         let peer_bitfield = Bitfield::from(&vec![0b0010111, 0b00110000], total_pieces as usize);
//
//         let interested_pieces_bitfield = state.build_interested_pieces_bitfield(&peer_bitfield);
//
//         // Verify that the generated bitfield correctly indicates interest in all pieces
//         // available from the peer since the current State has no downloaded pieces.
//         assert_eq!(interested_pieces_bitfield, interested_pieces_bitfield);
//     }
//
//     #[test]
//     fn test_build_interested_pieces_bitfield_non_empty_state_bitfield() {
//         // Test the case where the peer has pieces that have not been downloaded yet.
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
//
//         let peer_bitfield = Bitfield::from(&vec![0b0010111, 0b00110000], total_pieces as usize);
//
//         let interested_pieces_bitfield = state.build_interested_pieces_bitfield(&peer_bitfield);
//
//         assert_eq!(
//             interested_pieces_bitfield,
//             Bitfield::from(&vec![0b0000100, 0b00110000], total_pieces as usize)
//         );
//     }
//
//     #[test]
//     fn test_build_interested_pieces_bitfield_no_interesting_pieces() {
//         // Test the case where the peer does not have any interesting piece.
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00111011, 0b11110000], total_pieces as usize);
//
//         let peer_bitfield = Bitfield::from(&vec![0b00011010, 0b01100000], total_pieces as usize);
//
//         let interested_pieces_bitfield = state.build_interested_pieces_bitfield(&peer_bitfield);
//
//         assert_eq!(
//             interested_pieces_bitfield,
//             Bitfield::from(&vec![0b0000000, 0b00000000], total_pieces as usize)
//         );
//     }
//
//     #[test]
//     fn test_count_downloaded_pieces() {
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00111011, 0b11110000], total_pieces as usize);
//
//         assert_eq!(state.downloaded_pieces_count(), 9);
//     }
//
//     #[test]
//     fn test_count_downloaded_pieces_empty_bitfield() {
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00000000, 0b00000000], total_pieces as usize);
//
//         assert_eq!(state.downloaded_pieces_count(), 0);
//     }
//
//     #[test]
//     fn test_assign_piece() {
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
//         state.pending_pieces = vec![0, 1, 12, 9, 5];
//
//         // pieces 5 and 9 available
//         let peer_bitfield = Bitfield::from(&vec![0b00011110, 0b01100000], total_pieces as usize);
//
//         assert_eq!(state.assign_piece(&peer_bitfield), Some(9));
//         assert_eq!(state.assigned_pieces.get(&9), Some(&9));
//     }
//
//     #[test]
//     fn test_assign_piece_non_assignable() {
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
//         state.pending_pieces = vec![0, 1, 12, 9, 5];
//
//         // no pieces available
//         let peer_bitfield = Bitfield::from(&vec![0b00011010, 0b00000000], total_pieces as usize);
//
//         assert_eq!(state.assign_piece(&peer_bitfield), None);
//         assert!(state.assigned_pieces.is_empty());
//     }
//
//     #[test]
//     fn test_unassign_piece() {
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
//         state.pending_pieces = vec![0, 1, 12, 9, 5];
//         state.assigned_pieces = HashSet::from_iter(vec![1, 9]);
//
//         state.unassign_piece(9);
//
//         assert_eq!(state.assigned_pieces.get(&9), None);
//     }
//
//     #[test]
//     fn test_unassign_pieces() {
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
//         state.pending_pieces = vec![0, 1, 12, 9, 5];
//         state.assigned_pieces = HashSet::from_iter(vec![1, 9, 5]);
//
//         state.unassign_pieces(vec![1, 9]);
//
//         assert_eq!(state.assigned_pieces.get(&1), None);
//         assert_eq!(state.assigned_pieces.get(&9), None);
//         assert_eq!(state.assigned_pieces.get(&5), Some(&5));
//     }
//
//     #[test]
//     fn test_mark_piece_as_complete() {
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
//         state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1];
//         state.assigned_pieces = HashSet::from_iter(vec![1, 9, 5]);
//
//         assert!(!state.bitfield.has_piece(9));
//
//         state.mark_piece_complete(9);
//         // check piece 9 has been set in bitfield
//         assert!(state.bitfield.has_piece(9));
//         // check piece 9 has been removed assigned pieces
//         assert_eq!(state.assigned_pieces.get(&1), Some(&1));
//         assert_eq!(state.assigned_pieces.get(&5), Some(&5));
//         assert_eq!(state.assigned_pieces.get(&9), None);
//         // check piece 9 rarity is set to 0
//         assert_eq!(state.pieces_rarity[9], 0);
//     }
//
//     #[test]
//     fn test_increase_pieces_rarity() {
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
//         state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1];
//         state.assigned_pieces = HashSet::from_iter(vec![1, 9, 5]);
//
//         // pieces 0, 1, 9 and 10 count should increase
//         let interested_pieces =
//             Bitfield::from(&vec![0b11000000, 0b01100000], total_pieces as usize);
//
//         state.increase_pieces_rarity(&interested_pieces);
//
//         assert_eq!(
//             state.pieces_rarity,
//             vec![3, 3, 0, 0, 0, 1, 0, 0, 1, 2, 2, 1, 1, 1, 1]
//         );
//     }
//
//     #[test]
//     fn test_decrease_pieces_rarity() {
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
//         state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1];
//         state.assigned_pieces = HashSet::from_iter(vec![1, 9, 5]);
//
//         // pieces 0, 1, 9 and 10 count should increase
//         let interested_pieces =
//             Bitfield::from(&vec![0b11000000, 0b01100000], total_pieces as usize);
//
//         state.decrease_pieces_rarity(&interested_pieces);
//
//         assert_eq!(
//             state.pieces_rarity,
//             vec![1, 1, 0, 0, 0, 1, 0, 0, 1, 0, 0, 1, 1, 1, 1]
//         );
//     }
//
//     #[test]
//     fn test_sort_piece() {
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
//         state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 3, 2, 1, 4, 1, 1, 1];
//         state.assigned_pieces = HashSet::from_iter(vec![1, 9, 5]);
//
//         assert_eq!(state.sort_pieces(), vec![5, 10, 12, 13, 14, 0, 1, 9, 8, 11]);
//     }
//
//     #[test]
//     fn test_populate_queue() {
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
//         state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 3, 2, 1, 4, 1, 1, 1];
//         state.pending_pieces = vec![0, 1, 12, 9, 5];
//         state.assigned_pieces = HashSet::from_iter(vec![1, 9, 5]);
//
//         let sorted_pieces = state.sort_pieces();
//         state.populate_queue(sorted_pieces);
//
//         assert_eq!(
//             state.pending_pieces,
//             vec![5, 10, 12, 13, 14, 0, 1, 9, 8, 11]
//         );
//     }
//
//     #[test]
//     fn test_has_missing_pieces_true() {
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
//         state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 3, 2, 1, 4, 1, 1, 1];
//         state.pending_pieces = vec![0, 1, 12, 9, 5];
//         state.assigned_pieces = HashSet::from_iter(vec![1, 9, 5]);
//
//         // pieces 5 available
//         let peer_bitfield = Bitfield::from(&vec![0b00011110, 0b00000000], total_pieces as usize);
//
//         assert!(state.has_missing_pieces(&peer_bitfield));
//     }
//
//     #[test]
//     fn test_has_missing_pieces_false() {
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
//         state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 3, 2, 1, 4, 1, 1, 1];
//         state.pending_pieces = vec![0, 1, 12, 9, 5];
//         state.assigned_pieces = HashSet::from_iter(vec![1, 9, 5]);
//
//         // pieces 5 available
//         let peer_bitfield = Bitfield::from(&vec![0b00011010, 0b00000000], total_pieces as usize);
//
//         assert!(!state.has_missing_pieces(&peer_bitfield));
//     }
//
//     #[test]
//     fn test_has_concluded() {
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00111011, 0b10111000], total_pieces as usize);
//
//         let peer_bitfield = Bitfield::from(&vec![0b00011010, 0b10100000], total_pieces as usize);
//
//         assert!(state.has_concluded(&peer_bitfield));
//     }
//
//     #[test]
//     fn test_has_not_concluded() {
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00111011, 0b10111000], total_pieces as usize);
//
//         // pieces 0 and 1 stil to be downloaded
//         let peer_bitfield = Bitfield::from(&vec![0b11011010, 0b10100000], total_pieces as usize);
//
//         assert!(!state.has_concluded(&peer_bitfield));
//     }
//
//     #[test]
//     fn test_has_assignable_pieces() {
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
//         state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 3, 2, 1, 4, 1, 1, 1];
//         state.pending_pieces = vec![0, 1, 12, 9, 5];
//         state.assigned_pieces = HashSet::from_iter(vec![9, 5]);
//
//         // piece 14 availabe
//         let peer_bitfield = Bitfield::from(&vec![0b00011010, 0b00000010], total_pieces as usize);
//
//         assert!(state.has_assignable_pieces(&peer_bitfield));
//     }
//
//     #[test]
//     fn test_has_not_assignable_pieces() {
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
//         state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 3, 2, 1, 4, 1, 1, 1];
//         state.pending_pieces = vec![0, 1, 12, 9, 5];
//         state.assigned_pieces = HashSet::from_iter(vec![9, 5]);
//
//         // no more pieces to be downloaded from peer
//         let peer_bitfield = Bitfield::from(&vec![0b00011010, 0b00000000], total_pieces as usize);
//
//         assert!(!state.has_assignable_pieces(&peer_bitfield));
//     }
//
//     #[test]
//     fn test_has_not_assignable_pieces_already_assigned() {
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
//         state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 3, 2, 1, 4, 1, 1, 1];
//         state.pending_pieces = vec![0, 1, 12, 9, 5];
//         state.assigned_pieces = HashSet::from_iter(vec![0, 1, 9, 5]);
//
//         // pieces 0 and 1 available but already assigned
//         let peer_bitfield = Bitfield::from(&vec![0b11011010, 0b00000000], total_pieces as usize);
//
//         assert!(!state.has_assignable_pieces(&peer_bitfield));
//     }
//
//     #[test]
//     fn test_missing_unassigned_pieces() {
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
//         state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 3, 2, 1, 4, 1, 1, 1];
//         state.pending_pieces = vec![0, 1, 12, 9, 5, 10, 14];
//         state.assigned_pieces = HashSet::from_iter(vec![9, 5, 10]);
//
//         // pieces 0, 1, 10 and 14 available but piece 10 is already assigned
//         let peer_bitfield = Bitfield::from(&vec![0b11011010, 0b00100010], total_pieces as usize);
//
//         assert_eq!(
//             state.missing_unassigned_pieces(&peer_bitfield),
//             vec![0, 1, 14]
//         );
//     }
//
//     #[test]
//     fn test_no_missing_unassigned_pieces() {
//         let total_pieces = 12;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
//         state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 3, 2, 1, 4, 1, 1, 1];
//         state.pending_pieces = vec![0, 1, 12, 9, 5, 10, 14];
//         state.assigned_pieces = HashSet::from_iter(vec![0, 1, 9, 5, 10, 14]);
//
//         // pieces 0, 1, 10 and 14 available but piece 10 is already assigned
//         let peer_bitfield = Bitfield::from(&vec![0b11011010, 0b00100010], total_pieces as usize);
//
//         assert!(state.missing_unassigned_pieces(&peer_bitfield).is_empty());
//     }
//
//     #[test]
//     fn test_has_active_workers() {
//         let total_pieces = 12;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state
//             .workers_pending_pieces
//             .insert("worker1".to_string(), vec![]);
//         state
//             .workers_pending_pieces
//             .insert("worker2".to_string(), vec![2, 4, 5]);
//         state
//             .workers_pending_pieces
//             .insert("worker3".to_string(), vec![]);
//
//         assert!(state.has_active_worker());
//     }
//
//     #[test]
//     fn test_has_no_active_workers() {
//         let total_pieces = 12;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state
//             .workers_pending_pieces
//             .insert("worker1".to_string(), vec![]);
//         state
//             .workers_pending_pieces
//             .insert("worker2".to_string(), vec![]);
//         state
//             .workers_pending_pieces
//             .insert("worker3".to_string(), vec![]);
//
//         assert!(!state.has_active_worker());
//     }
//
//     #[test]
//     fn test_bitfield_is_completed() {
//         let total_pieces = 12;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b11111111, 0b11110000], total_pieces as usize);
//
//         assert!(state.is_completed());
//     }
//
//     #[test]
//     fn test_bitfield_is_not_completed() {
//         let total_pieces = 12;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b11110011, 0b11110000], total_pieces as usize);
//
//         assert!(!state.is_completed());
//     }
//
//     #[test]
//     fn test_bitfield_is_not_completed_invalid_pieces_set() {
//         let total_pieces = 12;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b11111111, 0b00001111], total_pieces as usize);
//
//         assert!(!state.is_completed());
//     }
// }
