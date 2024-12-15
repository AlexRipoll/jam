use std::io;

#[derive(Debug, Clone)]
pub struct Bitfield {
    pub bytes: Vec<u8>, // In-memory bitfield
    num_pieces: usize,  // Total number of pieces
}

impl Bitfield {
    pub fn new(bytes: &[u8]) -> Self {
        let num_pieces: u32 = bytes.iter().map(|byte| byte.count_ones()).sum();
        Self {
            bytes: bytes.to_vec(),
            num_pieces: num_pieces as usize,
        }
    }

    pub fn from_empty(num_pieces: usize) -> Self {
        let bitfield = vec![0; (num_pieces + 7) / 8];
        Self {
            bytes: bitfield,
            num_pieces,
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

        (&self.bytes[byte_index] & (1 << (7 - bit_index))) != 0
    }

    pub fn has_all_pieces(&self) -> bool {
        // Check all bytes except the last one
        for &byte in &self.bytes[..self.bytes.len() - 1] {
            if byte != 0xFF {
                return false;
            }
        }

        // Check the last byte
        if self.num_pieces % 8 != 0 {
            let last_byte_mask = 0xFFu8 << (8 - (self.num_pieces % 8));
            let last_byte_index = self.bytes.len() - 1;
            if self.bytes[last_byte_index] & last_byte_mask != last_byte_mask {
                return false;
            }
        }

        true
    }
}

pub fn load_from_disk(file_path: &str) -> io::Result<Bitfield> {
    let saved_bitfield = std::fs::read(file_path)?;

    Ok(Bitfield::new(&saved_bitfield))
}

pub fn save_to_disk(file_path: &str, bitfield: Bitfield) -> io::Result<()> {
    std::fs::write(file_path, bitfield.bytes)
}

#[cfg(test)]
mod test {
    use crate::bitfield::Bitfield;

    #[test]
    fn test_has_piece() {
        let bitfield = Bitfield::new(&vec![0b0, 0b0, 0b00001000, 0b0]);
        //check that it has the only piece available at index 20
        assert!(bitfield.has_piece(20));
    }

    #[test]
    fn test_has_piece_out_of_range() {
        let bitfield = Bitfield::new(&vec![0b0, 0b0, 0b00001000, 0b0]);
        // should return false when checking for a piece index out of range
        assert!(!bitfield.has_piece(50));
    }

    #[test]
    fn test_set_piece() {
        let mut bitfield = Bitfield::new(&vec![0b0, 0b0, 0b0, 0b0]);
        bitfield.set_piece(20);

        assert_eq!(bitfield.bytes, vec![0b0, 0b0, 0b00001000, 0b0]);
    }

    #[test]
    fn test_set_piece_out_of_range() {
        let mut bitfield = Bitfield::new(&vec![0b0, 0b0, 0b0, 0b0]);
        // out of range
        bitfield.set_piece(50);

        // no change in bitfield
        assert_eq!(bitfield.bytes, vec![0b0, 0b0, 0b0, 0b0]);
    }
}
