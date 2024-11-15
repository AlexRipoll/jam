use std::collections::HashMap;

pub struct Piece {
    index: u32,
    size: usize,
    hash: [u8; 20],
    blocks: HashMap<usize, Vec<u8>>,
    is_complete: bool,
}

impl Piece {
    pub fn new(index: u32, size: usize, hash: [u8; 20]) -> Self {
        Self {
            index,
            size,
            hash,
            blocks: HashMap::new(),
            is_complete: false,
        }
    }

    pub fn add_block(&mut self, offset: u32, block: Vec<u8>) {
        self.blocks.insert(offset as usize, block);
    }

    pub fn assemble(&self) -> Vec<u8> {
        let mut buffer = vec![0u8; self.size as usize];
        for (&offset, block) in &self.blocks {
            buffer[offset..offset + block.len()].copy_from_slice(&block);
        }

        buffer
    }

    pub fn is_ready(&self) -> bool {
        let downloaded: usize = self.blocks.values().map(|block| block.len()).sum();

        downloaded == self.size
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
