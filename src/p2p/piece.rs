use std::{error::Error, fmt::Display};

#[derive(Debug, Clone)]
pub struct Piece {
    index: u32,
    size: usize,
    hash: [u8; 20],
    blocks: Vec<Vec<u8>>,
    is_complete: bool,
}

impl Piece {
    pub fn new(index: u32, size: usize, hash: [u8; 20]) -> Self {
        let num_blocks = (size + 16383) / 16384;
        Self {
            index,
            size,
            hash,
            blocks: vec![Vec::new(); num_blocks],
            is_complete: false,
        }
    }

    pub fn add_block(&mut self, offset: u32, block: Vec<u8>) -> Result<(), PieceError> {
        let block_index = (offset / 16384) as usize;
        if block_index < self.blocks.len() {
            self.blocks[block_index] = block;
        } else {
            return Err(PieceError::OutOfBounds);
        }

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

    pub fn offset(&self) -> u64 {
        (self.size as u64).saturating_mul(self.index() as u64)
    }

    pub fn hash(&self) -> [u8; 20] {
        self.hash
    }

    pub fn is_completed(&self) -> bool {
        self.is_complete
    }

    pub fn set_complete(&mut self) {
        self.is_complete = true;
    }
}

#[derive(Debug)]
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
