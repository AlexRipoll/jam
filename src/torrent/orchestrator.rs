use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Display,
    usize,
};

use tokio::sync::mpsc;

use super::{
    events::{Event, StateEvent},
    peer::Peer,
    state::State,
};

#[derive(Debug)]
struct Orchestrator {
    state: State,
    peers: HashSet<Peer>,
    workers: HashSet<String>,
    workers_channel: HashMap<String, mpsc::Sender<Event>>, // worker_id -> worker sender for sending Events to worker
    queue_capacity: usize,
}

impl Orchestrator {
    fn new(state: State, queue_capacity: usize, peers: HashSet<Peer>) -> Orchestrator {
        Orchestrator {
            state,
            peers,
            workers: HashSet::new(),
            workers_channel: HashMap::new(),
            queue_capacity,
        }
    }

    async fn send_event(&self, worker_id: &str, event: Event) -> Result<(), OrchestratorError> {
        match self.workers_channel.get(worker_id) {
            Some(worker_tx) => worker_tx
                .send(event)
                .await
                .map_err(OrchestratorError::ChannelTxError),
            None => Err(OrchestratorError::WorkerChannelNotFound(
                worker_id.to_string(),
            )),
        }
    }

    async fn dispatch_pending_pieces(&mut self) -> Result<(), OrchestratorError> {
        let mut workers_queue = self.assign_pieces_to_workers();

        while !self.state.pending_pieces.is_empty() {
            // iterate over each current worker
            for (worker_id, peer_bitfield) in self.state.peers_bitfield.clone().iter() {
                while self
                    .state
                    .workers_pending_pieces
                    .get(worker_id)
                    .ok_or(OrchestratorError::WorkerNotFound(worker_id.to_string()))?
                    .len()
                    < self.queue_capacity
                    && !workers_queue
                        .get(worker_id)
                        .ok_or(OrchestratorError::WorkerNotFound(worker_id.to_string()))?
                        .is_empty()
                {
                    if let Some(queue) = workers_queue.get_mut(worker_id) {
                        if let Some(piece_index) = queue.pop_front() {
                            let piece = self
                                .state
                                .pieces
                                .get(&piece_index)
                                .ok_or(OrchestratorError::PieceNotFound(piece_index))?;
                            self.send_event(
                                &worker_id,
                                Event::State(StateEvent::RequestPiece(piece.clone())),
                            )
                            .await?;
                            self.state
                                .workers_pending_pieces
                                .get_mut(worker_id)
                                .ok_or(OrchestratorError::PieceNotFound(piece_index))?
                                .push(piece_index);
                            self.state.remove_pending_piece(piece_index);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    // assign the pieces to be downloaded by each worker in a balanced distribution.
    fn assign_pieces_to_workers(&self) -> HashMap<String, VecDeque<u32>> {
        let mut assignments: HashMap<String, VecDeque<u32>> = HashMap::new();
        let mut worker_pieces: Vec<(String, Vec<usize>)> = Vec::new();

        // checks how many pieces can be downloaded by each worker
        for (worker_id, peer_bitfield) in self.state.peers_bitfield.iter() {
            let pieces = self.state.missing_unassigned_pieces(peer_bitfield);
            worker_pieces.push((worker_id.clone(), pieces));
        }

        // sort assignable_pieces by amount per worker (asc)
        worker_pieces.sort_by_key(|(_, pieces)| pieces.len());

        let mut pending_pieces: HashSet<u32> =
            HashSet::from_iter(self.state.pending_pieces.clone());
        while !pending_pieces.is_empty() {
            // Try to assign a piece to each worker in a fair way
            for (worker_id, available_pieces) in &mut worker_pieces {
                if let Some(&piece) = available_pieces
                    .iter()
                    .find(|&&piece| pending_pieces.contains(&(piece as u32)))
                {
                    assignments
                        .entry(worker_id.clone())
                        .or_insert_with(VecDeque::new)
                        .push_back(piece as u32);
                    pending_pieces.remove(&(piece as u32));
                }
            }
        }

        assignments
    }
}

#[derive(Debug)]
pub enum OrchestratorError {
    WorkerNotFound(String),
    PieceNotFound(u32),
    WorkerChannelNotFound(String),
    ChannelTxError(mpsc::error::SendError<Event>),
}

impl Display for OrchestratorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrchestratorError::WorkerNotFound(worker_id) => {
                write!(f, "Worker {} not found", worker_id)
            }
            OrchestratorError::PieceNotFound(piece_index) => {
                write!(f, "Piece with index {} not found", piece_index)
            }
            OrchestratorError::WorkerChannelNotFound(worker_id) => {
                write!(f, "Channel for worker {} not found", worker_id)
            }
            OrchestratorError::ChannelTxError(err) => {
                write!(f, "Failed to send event: {}", err)
            }
        }
    }
}

impl From<mpsc::error::SendError<Event>> for OrchestratorError {
    fn from(err: mpsc::error::SendError<Event>) -> Self {
        OrchestratorError::ChannelTxError(err)
    }
}

#[cfg(test)]
mod test {
    use super::Orchestrator;
    use crate::torrent::{events::Event, state::State};
    use protocol::{bitfield::Bitfield, piece::Piece};
    use std::collections::{HashMap, HashSet, VecDeque};
    use tokio::sync::mpsc;

    pub fn mock_pieces(total_piece: u32) -> HashMap<u32, Piece> {
        (0..total_piece)
            .map(|i| (i, Piece::new(i, 1024, [i as u8; 20])))
            .collect()
    }

    #[test]
    fn test_assign_pieces_to_workers() {
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);

        let peer1 = (
            "worker1",
            Bitfield::from(&vec![0b11001000, 0b0], total_pieces as usize),
        );
        let peer2 = (
            "worker2",
            Bitfield::from(&vec![0b01001101, 0b10000000], total_pieces as usize),
        );
        let peer3 = (
            "worker3",
            Bitfield::from(&vec![0b11100010, 0b01010000], total_pieces as usize),
        );
        let peer4 = (
            "worker4",
            Bitfield::from(&vec![0b11101101, 0b10110000], total_pieces as usize),
        );

        for (id, bitfield) in [&peer1, &peer2, &peer3, &peer4] {
            state.process_bitfield(id, bitfield.clone());
        }

        let orchestrator = Orchestrator::new(state, 5, HashSet::new());

        let assignments = orchestrator.assign_pieces_to_workers();
        assert_eq!(assignments.get(peer1.0), Some(&VecDeque::from([0])));
        assert_eq!(assignments.get(peer2.0), Some(&VecDeque::from([1, 5, 8])));
        assert_eq!(
            assignments.get(peer3.0),
            Some(&VecDeque::from([2, 6, 9, 11]))
        );
        assert_eq!(assignments.get(peer4.0), Some(&VecDeque::from([4, 7, 10])));
    }

    #[tokio::test]
    async fn test_dispatch_pending_pieces() {
        let total_pieces = 10;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b10000001, 0b01101100], total_pieces as usize);

        let peer1 = (
            "worker1",
            Bitfield::from(&vec![0b11001010, 0b0], total_pieces as usize),
        );
        let peer2 = (
            "worker2",
            Bitfield::from(&vec![0b01001101, 0b10000000], total_pieces as usize),
        );

        let peer3 = (
            "worker3",
            Bitfield::from(&vec![0b10001001, 0b00000000], total_pieces as usize),
        );

        for (id, bitfield) in [&peer1, &peer2, &peer3] {
            state.process_bitfield(id, bitfield.clone());
        }

        state.pending_pieces = vec![1, 4, 5, 6, 8];

        let mut orchestrator = Orchestrator::new(state, 5, HashSet::new());

        // Create a test channel (1 slot to ensure async behavior)
        let (worker1_tx, worker1_rx) = mpsc::channel::<Event>(10);
        // Insert mock channel into orchestrator
        orchestrator
            .workers_channel
            .insert(peer1.0.to_string(), worker1_tx);

        let (worker2_tx, worker2_rx) = mpsc::channel::<Event>(10);
        orchestrator
            .workers_channel
            .insert(peer2.0.to_string(), worker2_tx);

        let (worker3_tx, worker3_rx) = mpsc::channel::<Event>(10);
        orchestrator
            .workers_channel
            .insert(peer3.0.to_string(), worker3_tx);

        orchestrator.dispatch_pending_pieces().await.unwrap();

        // Check that workers have been assigned pieces to download
        let worker1_pending_pieces = orchestrator
            .state
            .workers_pending_pieces
            .get(peer1.0)
            .unwrap();
        assert_eq!(worker1_pending_pieces, &vec![1, 6]);
        let worker2_pending_pieces = orchestrator
            .state
            .workers_pending_pieces
            .get(peer2.0)
            .unwrap();
        assert_eq!(worker2_pending_pieces, &vec![5, 8]);
        let worker3_pending_pieces = orchestrator
            .state
            .workers_pending_pieces
            .get(peer3.0)
            .unwrap();
        assert_eq!(worker3_pending_pieces, &vec![4]);
    }

    #[tokio::test]
    async fn test_dispatch_pending_pieces_append_to_non_empty_queues() {
        let total_pieces = 10;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b10000001, 0b01101100], total_pieces as usize);

        let peer1 = (
            "worker1",
            Bitfield::from(&vec![0b11001010, 0b0], total_pieces as usize),
        );
        let peer2 = (
            "worker2",
            Bitfield::from(&vec![0b01001101, 0b10000000], total_pieces as usize),
        );

        let peer3 = (
            "worker3",
            Bitfield::from(&vec![0b10001001, 0b00000000], total_pieces as usize),
        );

        for (id, bitfield) in [&peer1, &peer2, &peer3] {
            state.process_bitfield(id, bitfield.clone());
        }

        state.pending_pieces = vec![1, 4, 5, 6, 8];
        state
            .workers_pending_pieces
            .insert(peer1.0.to_string(), vec![7, 9]);
        state
            .workers_pending_pieces
            .insert(peer3.0.to_string(), vec![2]);

        let mut orchestrator = Orchestrator::new(state, 5, HashSet::new());

        // Create a test channel (1 slot to ensure async behavior)
        let (worker1_tx, worker1_rx) = mpsc::channel::<Event>(10);
        // Insert mock channel into orchestrator
        orchestrator
            .workers_channel
            .insert(peer1.0.to_string(), worker1_tx);

        let (worker2_tx, worker2_rx) = mpsc::channel::<Event>(10);
        orchestrator
            .workers_channel
            .insert(peer2.0.to_string(), worker2_tx);

        let (worker3_tx, worker3_rx) = mpsc::channel::<Event>(10);
        orchestrator
            .workers_channel
            .insert(peer3.0.to_string(), worker3_tx);

        orchestrator.dispatch_pending_pieces().await.unwrap();

        // Check that workers have been assigned pieces to download
        let worker1_pending_pieces = orchestrator
            .state
            .workers_pending_pieces
            .get(peer1.0)
            .unwrap();
        assert_eq!(worker1_pending_pieces, &vec![7, 9, 1, 6]);
        let worker2_pending_pieces = orchestrator
            .state
            .workers_pending_pieces
            .get(peer2.0)
            .unwrap();
        assert_eq!(worker2_pending_pieces, &vec![5, 8]);
        let worker3_pending_pieces = orchestrator
            .state
            .workers_pending_pieces
            .get(peer3.0)
            .unwrap();
        assert_eq!(worker3_pending_pieces, &vec![2, 4]);
    }
}
