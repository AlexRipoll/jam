use std::collections::{HashMap, HashSet};

use protocol::{bitfield::Bitfield, piece::Piece};

#[derive(Debug)]
pub struct State {
    pub pieces: HashMap<u32, Piece>,
    pub bitfield: Bitfield, // Bitfield of the downloaded pieces
    pub peers_bitfield: HashMap<String, Bitfield>, // Peers' bitfield from all peer connections
    // pieces_map: HashMap<u32, Piece>,         // Piece index - Piece struct Map
    pieces_rarity: Vec<u8>, // Containing the amount of occurences of a piece index within
    // the  current peers' bitfieds
    pub pending_pieces: Vec<u32>, // Sorted set of piece indexes pending to be downloaded from the current
    // peer connections (rarest first)
    pub assigned_pieces: HashSet<u32>, // Indexes of the pieces being downloaded
    pub workers_pending_pieces: HashMap<String, Vec<u32>>, // Indexes of the pieces being downloaded by each worker
}

impl State {
    pub fn new(pieces: HashMap<u32, Piece>) -> State {
        let total_pieces = pieces.len();
        State {
            pieces,
            bitfield: Bitfield::new(total_pieces),
            peers_bitfield: HashMap::new(),
            pieces_rarity: vec![0u8; total_pieces],
            pending_pieces: Vec::new(),
            assigned_pieces: HashSet::new(),
            workers_pending_pieces: HashMap::new(),
        }
    }

    pub fn process_bitfield(&mut self, peer_id: &str, peer_bitfield: Bitfield) {
        let missing_pieces_bitfield = self.build_interested_pieces_bitfield(&peer_bitfield);
        self.peers_bitfield
            .insert(peer_id.to_string(), peer_bitfield);

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
    pub fn build_interested_pieces_bitfield(&self, peer_bitfield: &Bitfield) -> Bitfield {
        let mut missing = Vec::new();

        for (peer_byte, state_byte) in peer_bitfield.bytes.iter().zip(self.bitfield.bytes.iter()) {
            missing.push(peer_byte & !state_byte);
        }

        Bitfield::from(&missing, self.bitfield.total_pieces)
    }

    pub fn downloaded_pieces_count(&self) -> u32 {
        self.bitfield
            .bytes
            .iter()
            .map(|byte| byte.count_ones())
            .sum::<u32>()
    }

    // Find the next assignable piece for the given peer bitfield and returns the piece index.
    pub fn assign_piece(&mut self, peer_bitfield: &Bitfield) -> Option<u32> {
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

    pub fn unassign_piece(&mut self, piece_index: u32) {
        self.assigned_pieces.remove(&piece_index);
    }

    pub fn unassign_pieces(&mut self, pieces_index: Vec<u32>) {
        for piece_index in pieces_index {
            self.unassign_piece(piece_index);
        }
    }

    // Mark a piece as downloaded, remove from missing and assigned sets
    pub fn mark_piece_complete(&mut self, piece_index: u32) {
        self.bitfield.set_piece(piece_index as usize);
        for (i, &piece_idx) in self.pending_pieces.iter().enumerate() {
            if piece_idx == piece_index {
                self.pending_pieces.remove(i as usize);
                break;
            }
        }
        self.assigned_pieces.remove(&piece_index);
        // set to 0 so it is not taken in consideration since it is already set in the `bitfield` field.
        self.pieces_rarity[piece_index as usize] = 0;
    }

    pub fn increase_pieces_rarity(&mut self, peer_bitfield: &Bitfield) {
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

    pub fn decrease_pieces_rarity(&mut self, peer_bitfield: &Bitfield) {
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

    pub fn sort_pieces(&self) -> Vec<u32> {
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

    pub fn populate_queue(&mut self, sorted_pieces: Vec<u32>) {
        self.pending_pieces.clear();
        self.pending_pieces.extend(sorted_pieces);
    }

    pub fn has_missing_pieces(&self, peer_bitfield: &Bitfield) -> bool {
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
    pub fn has_concluded(&self, peer_bitfield: &Bitfield) -> bool {
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
    pub fn has_assignable_pieces(&self, peer_bitfield: &Bitfield) -> bool {
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
    pub fn missing_unassigned_pieces(&self, peer_bitfield: &Bitfield) -> Vec<usize> {
        (0..peer_bitfield.total_pieces)
            .filter(|&piece_index| {
                peer_bitfield.has_piece(piece_index)
                    && !self.bitfield.has_piece(piece_index)
                    && !self.assigned_pieces.contains(&(piece_index as u32))
            })
            .collect()
    }

    pub fn download_progress_percent(&self) -> u32 {
        let downloaded_pieces = self.downloaded_pieces_count();

        downloaded_pieces * 100 / self.bitfield.total_pieces as u32
    }
}

#[cfg(test)]
mod test {
    use super::*;

    pub fn mock_pieces(total_piece: u32) -> HashMap<u32, Piece> {
        (0..total_piece)
            .map(|i| (i, Piece::new(i, 1024, [i as u8; 20])))
            .collect()
    }

    #[test]
    fn test_process_bitfield() {
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);

        let peer1 = (
            "peer1_id",
            Bitfield::from(&vec![0b11001000, 0b0], total_pieces as usize),
        );
        let peer2 = (
            "peer2_id",
            Bitfield::from(&vec![0b01001101, 0b10000000], total_pieces as usize),
        );
        let peer3 = (
            "peer3_id",
            Bitfield::from(&vec![0b11100010, 0b01010000], total_pieces as usize),
        );

        // process peer1 bitfield
        state.process_bitfield(peer1.0, peer1.1.clone());
        assert_eq!(state.peers_bitfield.get(peer1.0), Some(&peer1.1));
        assert_eq!(
            state.pieces_rarity,
            vec![1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(state.pending_pieces, vec![0, 1, 4]);

        // process peer2 bitfield
        state.process_bitfield(peer2.0, peer2.1.clone());
        assert_eq!(state.peers_bitfield.get(peer2.0), Some(&peer2.1));
        assert_eq!(
            state.pieces_rarity,
            vec![1, 2, 0, 0, 2, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(state.pending_pieces, vec![0, 5, 7, 8, 1, 4]);

        // process peer3 bitfield
        state.process_bitfield(peer3.0, peer3.1.clone());
        assert_eq!(state.peers_bitfield.get(peer3.0), Some(&peer3.1));
        assert_eq!(
            state.pieces_rarity,
            vec![2, 3, 1, 0, 2, 1, 1, 1, 1, 1, 0, 1, 0, 0, 0]
        );
        assert_eq!(state.pending_pieces, vec![2, 5, 6, 7, 8, 9, 11, 0, 4, 1]);
    }

    #[test]
    fn test_build_interested_pieces_bitfield_empty_state_bitfield() {
        // Test the case where the peer has pieces that have not been downloaded yet.
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let state = State::new(pieces);
        let peer_bitfield = Bitfield::from(&vec![0b0010111, 0b00110000], total_pieces as usize);

        let interested_pieces_bitfield = state.build_interested_pieces_bitfield(&peer_bitfield);

        // Verify that the generated bitfield correctly indicates interest in all pieces
        // available from the peer since the current State has no downloaded pieces.
        assert_eq!(interested_pieces_bitfield, interested_pieces_bitfield);
    }

    #[test]
    fn test_build_interested_pieces_bitfield_non_empty_state_bitfield() {
        // Test the case where the peer has pieces that have not been downloaded yet.
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);

        let peer_bitfield = Bitfield::from(&vec![0b0010111, 0b00110000], total_pieces as usize);

        let interested_pieces_bitfield = state.build_interested_pieces_bitfield(&peer_bitfield);

        assert_eq!(
            interested_pieces_bitfield,
            Bitfield::from(&vec![0b0000100, 0b00110000], total_pieces as usize)
        );
    }

    #[test]
    fn test_build_interested_pieces_bitfield_no_interesting_pieces() {
        // Test the case where the peer does not have any interesting piece.
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00111011, 0b11110000], total_pieces as usize);

        let peer_bitfield = Bitfield::from(&vec![0b00011010, 0b01100000], total_pieces as usize);

        let interested_pieces_bitfield = state.build_interested_pieces_bitfield(&peer_bitfield);

        assert_eq!(
            interested_pieces_bitfield,
            Bitfield::from(&vec![0b0000000, 0b00000000], total_pieces as usize)
        );
    }

    #[test]
    fn test_count_downloaded_pieces() {
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00111011, 0b11110000], total_pieces as usize);

        assert_eq!(state.downloaded_pieces_count(), 9);
    }

    #[test]
    fn test_count_downloaded_pieces_empty_bitfield() {
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00000000, 0b00000000], total_pieces as usize);

        assert_eq!(state.downloaded_pieces_count(), 0);
    }

    #[test]
    fn test_assign_piece() {
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
        state.pending_pieces = vec![0, 1, 12, 9, 5];

        // pieces 5 and 9 available
        let peer_bitfield = Bitfield::from(&vec![0b00011110, 0b01100000], total_pieces as usize);

        assert_eq!(state.assign_piece(&peer_bitfield), Some(9));
        assert_eq!(state.assigned_pieces.get(&9), Some(&9));
    }

    #[test]
    fn test_assign_piece_non_assignable() {
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
        state.pending_pieces = vec![0, 1, 12, 9, 5];

        // no pieces available
        let peer_bitfield = Bitfield::from(&vec![0b00011010, 0b00000000], total_pieces as usize);

        assert_eq!(state.assign_piece(&peer_bitfield), None);
        assert!(state.assigned_pieces.is_empty());
    }

    #[test]
    fn test_unassign_piece() {
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
        state.pending_pieces = vec![0, 1, 12, 9, 5];
        state.assigned_pieces = HashSet::from_iter(vec![1, 9]);

        state.unassign_piece(9);

        assert_eq!(state.assigned_pieces.get(&9), None);
    }

    #[test]
    fn test_unassign_pieces() {
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
        state.pending_pieces = vec![0, 1, 12, 9, 5];
        state.assigned_pieces = HashSet::from_iter(vec![1, 9, 5]);

        state.unassign_pieces(vec![1, 9]);

        assert_eq!(state.assigned_pieces.get(&1), None);
        assert_eq!(state.assigned_pieces.get(&9), None);
        assert_eq!(state.assigned_pieces.get(&5), Some(&5));
    }

    #[test]
    fn test_mark_piece_as_complete() {
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
        state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1];
        state.pending_pieces = vec![0, 1, 12, 9, 5];
        state.assigned_pieces = HashSet::from_iter(vec![1, 9, 5]);

        assert!(!state.bitfield.has_piece(9));

        state.mark_piece_complete(9);
        // check piece 9 has been set in bitfield
        assert!(state.bitfield.has_piece(9));
        // check piece 9 has been removed from pending pieces
        assert!(!state.pending_pieces.contains(&9));
        // check piece 9 has been removed assigned pieces
        assert_eq!(state.assigned_pieces.get(&1), Some(&1));
        assert_eq!(state.assigned_pieces.get(&5), Some(&5));
        assert_eq!(state.assigned_pieces.get(&9), None);
        // check piece 9 rarity is set to 0
        assert_eq!(state.pieces_rarity[9], 0);
    }

    #[test]
    fn test_increase_pieces_rarity() {
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
        state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1];
        state.assigned_pieces = HashSet::from_iter(vec![1, 9, 5]);

        // pieces 0, 1, 9 and 10 count should increase
        let interested_pieces =
            Bitfield::from(&vec![0b11000000, 0b01100000], total_pieces as usize);

        state.increase_pieces_rarity(&interested_pieces);

        assert_eq!(
            state.pieces_rarity,
            vec![3, 3, 0, 0, 0, 1, 0, 0, 1, 2, 2, 1, 1, 1, 1]
        );
    }

    #[test]
    fn test_decrease_pieces_rarity() {
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
        state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1];
        state.assigned_pieces = HashSet::from_iter(vec![1, 9, 5]);

        // pieces 0, 1, 9 and 10 count should increase
        let interested_pieces =
            Bitfield::from(&vec![0b11000000, 0b01100000], total_pieces as usize);

        state.decrease_pieces_rarity(&interested_pieces);

        assert_eq!(
            state.pieces_rarity,
            vec![1, 1, 0, 0, 0, 1, 0, 0, 1, 0, 0, 1, 1, 1, 1]
        );
    }

    #[test]
    fn test_sort_piece() {
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
        state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 3, 2, 1, 4, 1, 1, 1];
        state.assigned_pieces = HashSet::from_iter(vec![1, 9, 5]);

        assert_eq!(state.sort_pieces(), vec![5, 10, 12, 13, 14, 0, 1, 9, 8, 11]);
    }

    #[test]
    fn test_populate_queue() {
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
        state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 3, 2, 1, 4, 1, 1, 1];
        state.pending_pieces = vec![0, 1, 12, 9, 5];
        state.assigned_pieces = HashSet::from_iter(vec![1, 9, 5]);

        let sorted_pieces = state.sort_pieces();
        state.populate_queue(sorted_pieces);

        assert_eq!(
            state.pending_pieces,
            vec![5, 10, 12, 13, 14, 0, 1, 9, 8, 11]
        );
    }

    #[test]
    fn test_has_missing_pieces_true() {
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
        state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 3, 2, 1, 4, 1, 1, 1];
        state.pending_pieces = vec![0, 1, 12, 9, 5];
        state.assigned_pieces = HashSet::from_iter(vec![1, 9, 5]);

        // pieces 5 available
        let peer_bitfield = Bitfield::from(&vec![0b00011110, 0b00000000], total_pieces as usize);

        assert!(state.has_missing_pieces(&peer_bitfield));
    }

    #[test]
    fn test_has_missing_pieces_false() {
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
        state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 3, 2, 1, 4, 1, 1, 1];
        state.pending_pieces = vec![0, 1, 12, 9, 5];
        state.assigned_pieces = HashSet::from_iter(vec![1, 9, 5]);

        // pieces 5 available
        let peer_bitfield = Bitfield::from(&vec![0b00011010, 0b00000000], total_pieces as usize);

        assert!(!state.has_missing_pieces(&peer_bitfield));
    }

    #[test]
    fn test_has_concluded() {
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00111011, 0b10111000], total_pieces as usize);

        let peer_bitfield = Bitfield::from(&vec![0b00011010, 0b10100000], total_pieces as usize);

        assert!(state.has_concluded(&peer_bitfield));
    }

    #[test]
    fn test_has_not_concluded() {
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00111011, 0b10111000], total_pieces as usize);

        // pieces 0 and 1 stil to be downloaded
        let peer_bitfield = Bitfield::from(&vec![0b11011010, 0b10100000], total_pieces as usize);

        assert!(!state.has_concluded(&peer_bitfield));
    }

    #[test]
    fn test_has_assignable_pieces() {
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
        state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 3, 2, 1, 4, 1, 1, 1];
        state.pending_pieces = vec![0, 1, 12, 9, 5];
        state.assigned_pieces = HashSet::from_iter(vec![9, 5]);

        // piece 14 availabe
        let peer_bitfield = Bitfield::from(&vec![0b00011010, 0b00000010], total_pieces as usize);

        assert!(state.has_assignable_pieces(&peer_bitfield));
    }

    #[test]
    fn test_has_not_assignable_pieces() {
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
        state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 3, 2, 1, 4, 1, 1, 1];
        state.pending_pieces = vec![0, 1, 12, 9, 5];
        state.assigned_pieces = HashSet::from_iter(vec![9, 5]);

        // no more pieces to be downloaded from peer
        let peer_bitfield = Bitfield::from(&vec![0b00011010, 0b00000000], total_pieces as usize);

        assert!(!state.has_assignable_pieces(&peer_bitfield));
    }

    #[test]
    fn test_has_not_assignable_pieces_already_assigned() {
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
        state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 3, 2, 1, 4, 1, 1, 1];
        state.pending_pieces = vec![0, 1, 12, 9, 5];
        state.assigned_pieces = HashSet::from_iter(vec![0, 1, 9, 5]);

        // pieces 0 and 1 available but already assigned
        let peer_bitfield = Bitfield::from(&vec![0b11011010, 0b00000000], total_pieces as usize);

        assert!(!state.has_assignable_pieces(&peer_bitfield));
    }

    #[test]
    fn test_missing_unassigned_pieces() {
        let total_pieces = 15;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
        state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 3, 2, 1, 4, 1, 1, 1];
        state.pending_pieces = vec![0, 1, 12, 9, 5, 10, 14];
        state.assigned_pieces = HashSet::from_iter(vec![9, 5, 10]);

        // pieces 0, 1, 10 and 14 available but piece 10 is already assigned
        let peer_bitfield = Bitfield::from(&vec![0b11011010, 0b00100010], total_pieces as usize);

        assert_eq!(
            state.missing_unassigned_pieces(&peer_bitfield),
            vec![0, 1, 14]
        );
    }

    #[test]
    fn test_no_missing_unassigned_pieces() {
        let total_pieces = 12;
        let pieces = mock_pieces(total_pieces);
        let mut state = State::new(pieces);
        state.bitfield = Bitfield::from(&vec![0b00111011, 0b00000000], total_pieces as usize);
        state.pieces_rarity = vec![2, 2, 0, 0, 0, 1, 0, 0, 3, 2, 1, 4, 1, 1, 1];
        state.pending_pieces = vec![0, 1, 12, 9, 5, 10, 14];
        state.assigned_pieces = HashSet::from_iter(vec![0, 1, 9, 5, 10, 14]);

        // pieces 0, 1, 10 and 14 available but piece 10 is already assigned
        let peer_bitfield = Bitfield::from(&vec![0b11011010, 0b00100010], total_pieces as usize);

        assert!(state.missing_unassigned_pieces(&peer_bitfield).is_empty());
    }
}
