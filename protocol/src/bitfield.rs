#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Bitfield {
    pub bytes: Vec<u8>,      // In-memory bitfield
    pub total_pieces: usize, // Total number of pieces
}

impl Bitfield {
    pub fn new(total_pieces: usize) -> Self {
        let bitfield = vec![0; (total_pieces + 7) / 8];
        Self {
            bytes: bitfield,
            total_pieces,
        }
    }

    pub fn from(bytes: &[u8], total_pieces: usize) -> Self {
        Self {
            bytes: bytes.to_vec(),
            total_pieces,
        }
    }

    pub fn set_piece(&mut self, index: usize) {
        let byte_index = index / 8;
        let bit_index = index % 8;

        if byte_index < self.bytes.len() {
            self.bytes[byte_index] |= 1 << (7 - bit_index);
        }
    }

    pub fn has_piece(&self, index: usize) -> bool {
        let byte_index = index / 8;
        let bit_index = index % 8;

        if byte_index >= self.bytes.len() {
            return false;
        }

        (self.bytes[byte_index] & (1 << (7 - bit_index))) != 0
    }

    pub fn has_all_pieces(&self) -> bool {
        if self.bytes.is_empty() {
            return false;
        }

        // Check all bytes except the last one
        if !self.bytes[..self.bytes.len() - 1]
            .iter()
            .all(|&byte| byte == 0xFF)
        {
            return false;
        }

        // Check the last byte
        let last_byte_index = self.bytes.len() - 1;
        let last_byte = self.bytes[last_byte_index];
        let last_byte_mask = if self.total_pieces % 8 == 0 {
            0xFF // All bits must be set if the total pieces are a multiple of 8
        } else {
            0xFFu8 << (8 - (self.total_pieces % 8)) // Mask for the remaining bits in the last byte
        };

        (last_byte & last_byte_mask) == last_byte_mask
    }
}

#[cfg(test)]
mod test {
    use super::Bitfield;

    #[test]
    fn test_has_piece() {
        let bitfield = Bitfield::from(&vec![0b0, 0b0, 0b00001000, 0b0], 30);
        //check that it has the only piece available at index 20
        assert!(bitfield.has_piece(20));
    }

    #[test]
    fn test_has_piece_out_of_range() {
        let bitfield = Bitfield::from(&vec![0b0, 0b0, 0b00001000, 0b0], 28);
        // should return false when checking for a piece index out of range
        assert!(!bitfield.has_piece(50));
    }

    #[test]
    fn test_set_piece() {
        let mut bitfield = Bitfield::from(&vec![0b0, 0b0, 0b0, 0b0], 32);
        bitfield.set_piece(20);

        assert_eq!(bitfield.bytes, vec![0b0, 0b0, 0b00001000, 0b0]);
    }

    #[test]
    fn test_set_piece_out_of_range() {
        let mut bitfield = Bitfield::from(&vec![0b0, 0b0, 0b0, 0b0], 26);
        // out of range
        bitfield.set_piece(50);

        // no change in bitfield
        assert_eq!(bitfield.bytes, vec![0b0, 0b0, 0b0, 0b0]);
    }

    #[test]
    fn test_has_all_pieces() {
        // Case 1: Empty bitfield
        let bitfield = Bitfield::new(8);
        assert!(!bitfield.has_all_pieces());

        // Case 2: Partially completed bitfield
        let mut bitfield = Bitfield::new(8);
        bitfield.set_piece(0);
        bitfield.set_piece(1);
        bitfield.set_piece(2);
        assert!(!bitfield.has_all_pieces());

        // Case 3: Fully completed bitfield for a multiple of 8 pieces
        let mut bitfield = Bitfield::new(8);
        for i in 0..8 {
            bitfield.set_piece(i);
        }
        assert!(bitfield.has_all_pieces());
    }
}
