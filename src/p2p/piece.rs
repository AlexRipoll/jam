use std::{error::Error, fmt::Display};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Piece {
    index: u32,
    size: usize,
    hash: [u8; 20],
    pub blocks: Vec<Vec<u8>>,
    is_finalized: bool,
}

impl Piece {
    pub fn new(index: u32, size: usize, hash: [u8; 20]) -> Self {
        let num_blocks = (size + 16383) / 16384;
        Self {
            index,
            size,
            hash,
            blocks: vec![Vec::new(); num_blocks],
            is_finalized: false,
        }
    }

    pub fn add_block(&mut self, offset: u32, block: Vec<u8>) -> Result<(), PieceError> {
        let block_index = (offset / 16384) as usize;

        if block_index >= self.blocks.len() {
            return Err(PieceError::OutOfBounds);
        }
        self.blocks[block_index] = block;

        Ok(())
    }

    pub fn assemble(&self) -> Result<Vec<u8>, PieceError> {
        let mut buffer = vec![0u8; self.size];
        let mut position = 0;

        for block in &self.blocks {
            if !block.is_empty() {
                buffer[position..position + block.len()].copy_from_slice(block);
                position += block.len();
            } else {
                return Err(PieceError::MissingBlocks);
            }
        }

        Ok(buffer)
    }

    pub fn is_ready(&self) -> bool {
        let downloaded: usize = self.blocks.iter().map(|block| block.len()).sum();

        downloaded == self.size
    }

    pub fn index(&self) -> u32 {
        self.index
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn offset(&self, total_size: u64, standard_piece_size: u64) -> Result<u64, PieceError> {
        let base_offset = (self.index as u64).saturating_mul(standard_piece_size);

        // Calculate the index of the last piece
        let last_piece_index = (total_size + standard_piece_size - 1) / standard_piece_size - 1;

        // If this is the last piece, adjust the offset
        if self.index as u64 == last_piece_index {
            let last_piece_start = last_piece_index.saturating_mul(standard_piece_size);
            return Ok(std::cmp::min(base_offset, last_piece_start));
        }

        Ok(base_offset)
    }

    pub fn hash(&self) -> [u8; 20] {
        self.hash
    }

    pub fn is_finalized(&self) -> bool {
        self.is_finalized
    }

    pub fn mark_finalized(&mut self) {
        self.is_finalized = true;
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum PieceError {
    MissingBlocks,
    OutOfBounds,
}

impl Display for PieceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PieceError::MissingBlocks => write!(f, "Unable to assemble piece, missing blocks"),
            PieceError::OutOfBounds => write!(f, "Block index out of bounds"),
        }
    }
}

impl Error for PieceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_piece_creation() {
        let index = 0;
        let size = 32768; // 32 KB
        let hash = [0u8; 20];

        let piece = Piece::new(index, size, hash);

        assert_eq!(piece.index(), index);
        assert_eq!(piece.size(), size);
        assert_eq!(piece.hash(), hash);
        assert_eq!(piece.blocks.len(), 2); // 32 KB -> 2 blocks of 16 KB
        assert!(!piece.is_finalized());
    }

    #[test]
    fn test_add_block_success() {
        let mut piece = Piece::new(0, 32768, [0u8; 20]);

        let block = vec![1u8; 16384]; // 16 KB block
        assert!(piece.add_block(0, block.clone()).is_ok());

        let block2 = vec![2u8; 16384]; // 16 KB block
        assert!(piece.add_block(16384, block2.clone()).is_ok());

        assert_eq!(piece.blocks[0], block);
        assert_eq!(piece.blocks[1], block2);
    }

    #[test]
    fn test_add_block_out_of_bounds() {
        let mut piece = Piece::new(0, 32768, [0u8; 20]);

        let block = vec![1u8; 16384];
        let result = piece.add_block(32768, block);

        assert!(matches!(result, Err(PieceError::OutOfBounds)));
    }

    #[test]
    fn test_assemble_success() {
        let mut piece = Piece::new(0, 32768, [0u8; 20]);

        let block1 = vec![1u8; 16384];
        let block2 = vec![2u8; 16384];

        piece.add_block(0, block1.clone()).unwrap();
        piece.add_block(16384, block2.clone()).unwrap();

        let assembled = piece.assemble().unwrap();

        assert_eq!(&assembled[0..16384], &block1[..]);
        assert_eq!(&assembled[16384..32768], &block2[..]);
    }

    #[test]
    fn test_assemble_missing_blocks() {
        let mut piece = Piece::new(0, 32768, [0u8; 20]);

        let block1 = vec![1u8; 16384];
        piece.add_block(0, block1).unwrap();

        let result = piece.assemble();

        assert!(matches!(result, Err(PieceError::MissingBlocks)));
    }

    #[test]
    fn test_is_ready_true() {
        let mut piece = Piece::new(0, 32768, [0u8; 20]);

        let block1 = vec![1u8; 16384];
        let block2 = vec![2u8; 16384];

        piece.add_block(0, block1).unwrap();
        piece.add_block(16384, block2).unwrap();

        assert!(piece.is_ready());
    }

    #[test]
    fn test_is_ready_false() {
        let mut piece = Piece::new(0, 32768, [0u8; 20]);

        let block1 = vec![1u8; 16384];
        piece.add_block(0, block1).unwrap();

        assert!(!piece.is_ready());
    }

    #[test]
    fn test_offset_middle_piece() {
        let total_size = 5120; // Total size of the torrent (5 pieces of 1024 bytes + 1 piece of 1024 bytes)
        let standard_piece_size = 1024;

        let piece = Piece::new(2, 1024, [0u8; 20]);

        // For index 2 (middle piece), the offset should be 2 * 1024 = 2048
        let offset = piece.offset(total_size, standard_piece_size);
        assert_eq!(offset.unwrap(), 2048);
    }

    #[test]
    fn test_offset_first_piece() {
        let total_size = 5120; // Total size of the torrent
        let standard_piece_size = 1024;

        let piece = Piece::new(0, 1024, [0u8; 20]);

        // The first piece's offset should be 0
        let offset = piece.offset(total_size, standard_piece_size);
        assert_eq!(offset.unwrap(), 0);
    }

    #[test]
    fn test_offset_last_piece() {
        let total_size = 5120; // Total size of the torrent
        let standard_piece_size = 1024;

        let piece = Piece::new(4, 1024, [0u8; 20]);

        // The last piece should start at the correct offset
        let offset = piece.offset(total_size, standard_piece_size);
        assert_eq!(offset.unwrap(), 4096); // 4 * 1024 = 4096
    }

    #[test]
    fn test_offset_last_piece_with_smaller_final_piece() {
        let total_size = 5240; // Total size of the torrent
        let standard_piece_size = 1024;

        let piece = Piece::new(5, 120, [0u8; 20]);

        // The last piece should have an offset calculated correctly even with a smaller size
        let offset = piece.offset(total_size, standard_piece_size);
        assert_eq!(offset.unwrap(), 5120); // Last piece starts at offset 4096
    }

    #[test]
    fn test_finalize_piece() {
        let mut piece = Piece::new(0, 32768, [0u8; 20]);

        assert!(!piece.is_finalized());

        piece.mark_finalized();

        assert!(piece.is_finalized());
    }
}
