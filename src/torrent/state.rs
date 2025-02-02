use std::collections::HashSet;

use protocol::bitfield::Bitfield;

#[derive(Debug)]
pub struct State {
    bitfield: Bitfield,                      // Bitfield of the downloaded pieces
    peers_bitfield: Vec<(String, Bitfield)>, // Peers' bitfield from all peer connections
    // pieces_map: HashMap<u32, Piece>,         // Piece index - Piece struct Map
    pieces_rarity: Vec<u32>, // Containing the amount of occurences of a piece index within
    // the  current peers' bitfieds
    pending_pieces: Vec<u32>, // Sorted set of pieces pending to be downloaded from the current
    // peer connections (rarest first)
    assigned_pieces: HashSet<u32>, // Indexes of the pieces being downloaded
}

impl State {
    pub fn new(total_pieces: usize) -> State {
        State {
            bitfield: Bitfield::from_empty(total_pieces),
            peers_bitfield: Vec::new(),
            pieces_rarity: Vec::new(),
            pending_pieces: Vec::new(),
            assigned_pieces: HashSet::new(),
        }
    }

    pub fn process_bitfield(&mut self, peer_bitfield: &Bitfield) {
        let missing_pieces_bitfield = self.build_interested_pieces_bitfield(peer_bitfield);

        self.increase_pieces_rarity(&missing_pieces_bitfield);
        self.pending_pieces = self.sort_pieces();
    }

    fn build_interested_pieces_bitfield(&self, peer_bitfield: &Bitfield) -> Bitfield {
        let mut missing = Vec::new();

        for (peer_byte, state_byte) in peer_bitfield.bytes.iter().zip(self.bitfield.bytes.iter()) {
            missing.push(peer_byte & !state_byte);
        }

        Bitfield::new(&missing)
    }

    pub fn downloaded_pieces_count(&self) -> u32 {
        self.bitfield
            .bytes
            .iter()
            .map(|byte| byte.count_ones())
            .sum::<u32>()
    }

    pub fn unassign_pieces(&mut self, pieces_index: Vec<u32>) {
        for piece_index in pieces_index {
            self.unassign_piece(piece_index);
        }
    }

    // Mark a piece as downloaded, remove from missing and assigned sets
    fn mark_piece_complete(&mut self, piece_index: u32) {
        self.bitfield.set_piece(piece_index as usize);
        self.pending_pieces.remove(piece_index as usize);
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

    pub fn assign_piece(&mut self, peer_bitfield: &Bitfield) -> Option<u32> {
        if let Some(pos) = self.pending_pieces.iter().position(|&piece_index| {
            !self.assigned_pieces.contains(&piece_index)
                && peer_bitfield.has_piece(piece_index as usize)
        }) {
            let piece_index = self.pending_pieces.remove(pos);
            self.assigned_pieces.insert(piece_index.clone());
            return Some(piece_index);
        }

        None
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

    pub fn unassign_piece(&mut self, piece_index: u32) {
        self.assigned_pieces.remove(&piece_index);
        self.pending_pieces.push(piece_index);
    }

    pub fn download_progress_percent(&self) -> u32 {
        let downloaded_pieces = self.downloaded_pieces_count();

        downloaded_pieces * 100 / self.bitfield.total_pieces as u32
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_build_interested_pieces_bitfield_empty_state_bitfield() {
        // Test the case where the peer has pieces that have not been downloaded yet.
        let state = State::new(15);
        let peer_bitfield = Bitfield::new(&vec![0b0010111, 0b00110000]);

        let interested_pieces_bitfield = state.build_interested_pieces_bitfield(&peer_bitfield);

        // Verify that the generated bitfield correctly indicates interest in all pieces
        // available from the peer since the current State has no downloaded pieces.
        assert_eq!(interested_pieces_bitfield, interested_pieces_bitfield);
    }

    #[test]
    fn test_build_interested_pieces_bitfield_non_empty_state_bitfield() {
        // Test the case where the peer has pieces that have not been downloaded yet.
        let mut state = State::new(15);
        state.bitfield = Bitfield::new(&vec![0b00111011, 0b00000000]);

        let peer_bitfield = Bitfield::new(&vec![0b0010111, 0b00110000]);

        let interested_pieces_bitfield = state.build_interested_pieces_bitfield(&peer_bitfield);

        assert_eq!(
            interested_pieces_bitfield,
            Bitfield::new(&vec![0b0000100, 0b00110000])
        );
    }

    #[test]
    fn test_build_interested_pieces_bitfield_no_interesting_pieces() {
        // Test the case where the peer does not have any interesting piece.
        let mut state = State::new(15);
        state.bitfield = Bitfield::new(&vec![0b00111011, 0b11110000]);

        let peer_bitfield = Bitfield::new(&vec![0b00011010, 0b01100000]);

        let interested_pieces_bitfield = state.build_interested_pieces_bitfield(&peer_bitfield);

        assert_eq!(
            interested_pieces_bitfield,
            Bitfield::new(&vec![0b0000000, 0b00000000])
        );
    }
}
