use std::collections::HashSet;

#[derive(Debug)]
pub struct Pipeline {
    pub capacity: usize,
    pub pending_requests: HashSet<PieceData>,
    pub last_added: Option<PieceData>,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct PieceData {
    pub piece_index: u32,
    pub block_offset: u32,
}

impl PieceData {
    pub fn new(piece_index: u32, block_offset: u32) -> Self {
        Self {
            piece_index,
            block_offset,
        }
    }
}

impl Pipeline {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            pending_requests: HashSet::new(),
            last_added: None,
        }
    }

    pub fn add_request(&mut self, piece_index: u32, block_offset: u32) {
        let state = PieceData::new(piece_index, block_offset);
        self.pending_requests.insert(state.clone());
        self.last_added = Some(state);
    }

    pub fn remove_request(&mut self, piece_index: u32, block_offset: u32) {
        self.pending_requests
            .remove(&PieceData::new(piece_index, block_offset));
    }

    pub fn is_full(&self) -> bool {
        self.pending_requests.len() >= self.capacity
    }
}
