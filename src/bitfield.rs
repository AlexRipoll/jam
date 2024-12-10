use sha1::{Digest, Sha1};
use std::{fs::File, io};

#[derive(Debug)]
pub struct TorrentBitfield {
    bytes: Vec<u8>,        // In-memory bitfield
    num_pieces: usize,     // Total number of pieces
    bitfield_file: String, // Path to the persisted bitfield file
}

impl TorrentBitfield {
    pub fn new(num_pieces: usize, bitfield_file: &str) -> Self {
        let bitfield = vec![0; (num_pieces + 7) / 8];
        Self {
            bytes: bitfield,
            num_pieces,
            bitfield_file: bitfield_file.to_string(),
        }
    }

    pub fn load_from_disk(&mut self) -> io::Result<()> {
        let saved_bitfield = std::fs::read(&self.bitfield_file)?;
        self.bytes.copy_from_slice(&saved_bitfield);
        Ok(())
    }

    pub fn save_to_disk(&self) -> io::Result<()> {
        std::fs::write(&self.bitfield_file, &self.bytes)
    }

    pub fn set_piece(&mut self, piece_index: usize) {
        self.bytes[piece_index / 8] |= 1 << (7 - (piece_index % 8));
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
