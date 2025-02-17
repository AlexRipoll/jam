use std::collections::{HashMap, HashSet, VecDeque};

use tokio::sync::mpsc;

use super::{events::Event, state::State};

#[derive(Debug)]
struct Orchestrator {
    state: State,
    channels: HashMap<String, mpsc::Sender<Event>>, // worker_id -> worker sender for sending Events to worker
}

impl Orchestrator {
    fn new(state: State) -> Orchestrator {
        Orchestrator {
            state,
            channels: HashMap::new(),
            // tx: None,
        }
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

#[cfg(test)]
mod test {
    use super::Orchestrator;
    use crate::torrent::state::State;
    use protocol::{bitfield::Bitfield, piece::Piece};
    use std::collections::{HashMap, VecDeque};

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

        let orchestrator = Orchestrator::new(state);

        let assignments = orchestrator.assign_pieces_to_workers();
        assert_eq!(assignments.get(peer1.0), Some(&VecDeque::from([0])));
        assert_eq!(assignments.get(peer2.0), Some(&VecDeque::from([1, 5, 8])));
        assert_eq!(
            assignments.get(peer3.0),
            Some(&VecDeque::from([2, 6, 9, 11]))
        );
        assert_eq!(assignments.get(peer4.0), Some(&VecDeque::from([4, 7, 10])));
    }
}
