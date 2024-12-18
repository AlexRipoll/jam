use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use tokio::sync::Mutex;

use crate::{bitfield::Bitfield, p2p::piece::Piece};

#[derive(Debug)]
pub struct DownloadMetadata {
    pieces_map: HashMap<u32, Piece>,
    pub bitfield: Mutex<Bitfield>,
}

impl DownloadMetadata {
    pub fn new(pieces_map: HashMap<u32, Piece>) -> Self {
        let pieces_amount = pieces_map.len();
        Self {
            pieces_map,
            bitfield: Mutex::new(Bitfield::from_empty(pieces_amount)),
        }
    }

    pub async fn has_piece(&self, index: usize) -> bool {
        self.bitfield.lock().await.has_piece(index)
    }

    pub async fn mark_piece_downloaded(&self, index: usize) {
        let mut bitfield = self.bitfield.lock().await;
        bitfield.set_piece(index);
    }

    pub async fn has_missing_pieces(&self, peer_bitfield: &Bitfield) -> bool {
        let client_bitfield = self.bitfield.lock().await;
        for (byte_index, _) in peer_bitfield.bytes.iter().enumerate() {
            for bit_index in 0..8 {
                let piece_index = (byte_index * 8 + bit_index) as u32;

                if peer_bitfield.has_piece(piece_index as usize)
                    && !client_bitfield.has_piece(piece_index as usize)
                {
                    return true;
                }
            }
        }

        false
    }
}

#[derive(Debug)]
pub struct PiecesQueue {
    pub queue: Mutex<Vec<Piece>>, // Remaining pieces to be downloaded
    assigned_pieces: Mutex<HashSet<Piece>>, // Assigned but not yet completed pieces
}

impl PiecesQueue {
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(Vec::new()),
            assigned_pieces: Mutex::new(HashSet::new()),
        }
    }

    pub async fn populate_queue(&self, sorted_pieces: Vec<Piece>) {
        let mut queue = self.queue.lock().await;
        queue.clear();
        queue.extend(sorted_pieces);
    }

    pub async fn assign_piece(&self, peer_bitfield: &Bitfield) -> Option<Piece> {
        let mut queue = self.queue.lock().await;
        let mut assigned = self.assigned_pieces.lock().await;

        if let Some(pos) = queue.iter().position(|piece| {
            !assigned.contains(piece) && peer_bitfield.has_piece(piece.index() as usize)
        }) {
            let piece = queue.remove(pos);
            assigned.insert(piece.clone());
            return Some(piece);
        }

        None
    }

    pub async fn mark_piece_complete(&self, piece: Piece) {
        self.assigned_pieces.lock().await.remove(&piece);
    }
}

#[derive(Debug)]
pub struct PiecesRarity {
    pub rarity_map: Mutex<HashMap<u32, u16>>, // piece index -> piece count
}

impl PiecesRarity {
    pub fn new() -> Self {
        Self {
            rarity_map: Mutex::new(HashMap::new()),
        }
    }

    pub async fn update_rarity_from_bitfield(&self, bitfield: &Bitfield) {
        let mut rarity_map = self.rarity_map.lock().await;
        for byte_index in 0..bitfield.bytes.len() {
            for bit_index in 0..8 {
                let piece_index = (byte_index * 8 + bit_index) as u32;
                if bitfield.has_piece(piece_index as usize) {
                    *rarity_map.entry(piece_index).or_insert(0) += 1;
                }
            }
        }
    }

    pub async fn get_sorted_pieces(&self) -> Vec<(u32, u16)> {
        let rarity_map = self.rarity_map.lock().await;
        let mut sorted_pieces: Vec<_> = rarity_map.iter().map(|(&k, &v)| (k, v)).collect();
        sorted_pieces.sort_by(|a, b| b.1.cmp(&a.1)); // Most rare first
        sorted_pieces
    }
}

#[derive(Debug)]
pub struct DownloadState {
    pub metadata: Arc<DownloadMetadata>,
    pub pieces_rarity: Arc<PiecesRarity>,
    pub pieces_queue: Arc<PiecesQueue>,
}

impl DownloadState {
    pub fn new(pieces_map: HashMap<u32, Piece>) -> Self {
        Self {
            metadata: Arc::new(DownloadMetadata::new(pieces_map)),
            pieces_rarity: Arc::new(PiecesRarity::new()),
            pieces_queue: Arc::new(PiecesQueue::new()),
        }
    }

    pub async fn process_bitfield(&self, bitfield: &Bitfield) {
        self.pieces_rarity
            .update_rarity_from_bitfield(bitfield)
            .await;

        let sorted_pieces = self
            .pieces_rarity
            .get_sorted_pieces()
            .await
            .into_iter()
            .filter_map(|(index, _)| self.metadata.pieces_map.get(&index).cloned())
            .collect();

        self.pieces_queue.populate_queue(sorted_pieces).await;
    }

    pub async fn assign_piece(&self, peer_bitfield: &Bitfield) -> Option<Piece> {
        self.pieces_queue.assign_piece(peer_bitfield).await
    }

    pub async fn complete_piece(&self, piece: Piece) {
        self.metadata
            .mark_piece_downloaded(piece.index() as usize)
            .await;
        self.pieces_queue.mark_piece_complete(piece).await;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;
    use tokio::sync::Mutex;

    // Helper function to create a sample Piece
    fn create_sample_piece(index: u32, size: usize) -> Piece {
        let hash = [0u8; 20]; // Placeholder hash
        Piece::new(index, size, hash)
    }

    #[tokio::test]
    async fn test_download_metadata_has_piece() {
        let mut pieces_map = HashMap::new();
        pieces_map.insert(0, create_sample_piece(0, 16384)); // Piece 0
        pieces_map.insert(1, create_sample_piece(1, 16384)); // Piece 1
        pieces_map.insert(2, create_sample_piece(2, 16384)); // Piece 2
        pieces_map.insert(3, create_sample_piece(3, 16384)); // Piece 3
        let download_metadata = DownloadMetadata::new(pieces_map);

        // Mark piece 0 as downloaded
        download_metadata.mark_piece_downloaded(0).await;

        // Test that `has_piece` returns true for piece 0 (downloaded)
        assert!(download_metadata.has_piece(0).await);

        // Test that `has_piece` returns false for piece 1 (not downloaded)
        assert!(!download_metadata.has_piece(1).await);
    }

    #[tokio::test]
    async fn test_mark_piece_downloaded() {
        let mut pieces_map = HashMap::new();
        pieces_map.insert(0, create_sample_piece(0, 16384)); // Piece 0
        pieces_map.insert(1, create_sample_piece(1, 16384)); // Piece 1
        let download_metadata = DownloadMetadata::new(pieces_map);

        // Test that `has_piece` returns false initially
        assert!(!download_metadata.has_piece(0).await);

        // Mark piece 0 as downloaded
        download_metadata.mark_piece_downloaded(0).await;

        // Test that `has_piece` now returns true for piece 0
        assert!(download_metadata.has_piece(0).await);
    }

    #[tokio::test]
    async fn test_has_missing_pieces() {
        let mut pieces_map = HashMap::new();
        pieces_map.insert(0, create_sample_piece(0, 16384)); // Piece 0
        pieces_map.insert(1, create_sample_piece(1, 16384)); // Piece 1
        pieces_map.insert(2, create_sample_piece(2, 16384)); // Piece 2
        pieces_map.insert(3, create_sample_piece(3, 16384)); // Piece 3
        let download_metadata = DownloadMetadata::new(pieces_map);

        // Peer bitfield where piece 1 is available
        let mut peer_bitfield = Bitfield::from_empty(4);
        peer_bitfield.set_piece(0);
        peer_bitfield.set_piece(1);
        peer_bitfield.set_piece(2);

        // Test that `has_missing_pieces` returns true because piece 0 is missing
        assert!(download_metadata.has_missing_pieces(&peer_bitfield).await);

        // Mark piece 0 as downloaded
        download_metadata.mark_piece_downloaded(0).await;
        download_metadata.mark_piece_downloaded(2).await;

        // Test that `has_missing_pieces` returns true because it has piece 1 for download
        assert!(download_metadata.has_missing_pieces(&peer_bitfield).await);
    }

    #[tokio::test]
    async fn test_does_not_have_missing_pieces() {
        let mut pieces_map = HashMap::new();
        pieces_map.insert(0, create_sample_piece(0, 16384)); // Piece 0
        pieces_map.insert(1, create_sample_piece(1, 16384)); // Piece 1
        pieces_map.insert(2, create_sample_piece(2, 16384)); // Piece 2
        pieces_map.insert(3, create_sample_piece(3, 16384)); // Piece 3
        let download_metadata = DownloadMetadata::new(pieces_map);

        // Peer bitfield where piece 1 is available
        let mut peer_bitfield = Bitfield::from_empty(4);
        peer_bitfield.set_piece(0);
        peer_bitfield.set_piece(2);

        // Test that `has_missing_pieces` returns true because piece 0 is missing
        assert!(download_metadata.has_missing_pieces(&peer_bitfield).await);

        // Mark piece 0 as downloaded
        download_metadata.mark_piece_downloaded(0).await;
        download_metadata.mark_piece_downloaded(1).await;
        download_metadata.mark_piece_downloaded(2).await;

        // Test that `has_missing_pieces` returns false because all pieces are downloaded
        assert!(!download_metadata.has_missing_pieces(&peer_bitfield).await);
    }

    #[tokio::test]
    async fn test_populate_queue() {
        let pieces_queue = PiecesQueue::new();

        // Create sample pieces
        let pieces = vec![
            create_sample_piece(4, 16384),
            create_sample_piece(1, 16384),
            create_sample_piece(2, 16384),
        ];

        // Populate queue
        pieces_queue.populate_queue(pieces.clone()).await;

        // Verify the queue contains the expected pieces
        let queue = pieces_queue.queue.lock().await;
        assert_eq!(queue.len(), 3);
        assert_eq!(*queue, pieces);
    }

    #[tokio::test]
    async fn test_populate_queue_already_populated() {
        let pieces_queue = PiecesQueue::new();

        // Create sample pieces
        let pieces = vec![create_sample_piece(3, 16384), create_sample_piece(2, 16384)];

        // Populate queue
        pieces_queue.populate_queue(pieces.clone()).await;

        // Create new sample pieces
        let pieces = vec![
            create_sample_piece(4, 16384),
            create_sample_piece(1, 16384),
            create_sample_piece(2, 16384),
        ];

        // Populate queue
        pieces_queue.populate_queue(pieces.clone()).await;

        // Verify the queue contains the expected pieces
        let queue = pieces_queue.queue.lock().await;
        assert_eq!(queue.len(), 3);
        assert_eq!(*queue, pieces);
    }

    #[tokio::test]
    async fn test_assign_piece() {
        let pieces_queue = PiecesQueue::new();

        // Create sample pieces
        let pieces = vec![
            create_sample_piece(4, 16384),
            create_sample_piece(1, 16384),
            create_sample_piece(2, 16384),
        ];

        // Populate the queue
        pieces_queue.populate_queue(pieces.clone()).await;

        // Create a bitfield where peer has pieces 0 and 2
        let mut peer_bitfield = Bitfield::from_empty(4);
        peer_bitfield.set_piece(0);
        peer_bitfield.set_piece(2);

        // Assign a piece
        let assigned_piece = pieces_queue.assign_piece(&peer_bitfield).await;

        // Verify the assigned piece is piece 1 (index 1)
        assert!(assigned_piece.is_some());
        assert_eq!(assigned_piece.unwrap().index(), 2);

        // Verify that the piece is removed from the queue and added to assigned pieces
        let queue = pieces_queue.queue.lock().await;
        assert_eq!(queue.len(), 2);
        let assigned = pieces_queue.assigned_pieces.lock().await;
        assert!(assigned.contains(&create_sample_piece(2, 16384)));
    }

    #[tokio::test]
    async fn test_assign_piece_no_match() {
        let pieces_queue = PiecesQueue::new();

        // Create sample pieces
        let pieces = vec![
            create_sample_piece(4, 16384),
            create_sample_piece(1, 16384),
            create_sample_piece(2, 16384),
        ];

        // Populate the queue
        pieces_queue.populate_queue(pieces.clone()).await;

        // Create a bitfield where peer has no matching pieces
        let mut peer_bitfield = Bitfield::from_empty(4);
        peer_bitfield.set_piece(0);

        // Attempt to assign a piece
        let assigned_piece = pieces_queue.assign_piece(&peer_bitfield).await;

        // Verify no piece is assigned
        assert!(assigned_piece.is_none());

        // Verify the queue remains unchanged
        let queue = pieces_queue.queue.lock().await;
        assert_eq!(queue.len(), 3);
    }

    #[tokio::test]
    async fn test_mark_piece_complete() {
        let pieces_queue = PiecesQueue::new();

        // Create a sample piece
        let piece = create_sample_piece(0, 16384);

        // Manually insert the piece into assigned_pieces
        {
            let mut assigned = pieces_queue.assigned_pieces.lock().await;
            assigned.insert(piece.clone());
        }

        // Mark the piece as complete
        pieces_queue.mark_piece_complete(piece.clone()).await;

        // Verify the piece is removed from assigned_pieces
        let assigned = pieces_queue.assigned_pieces.lock().await;
        assert!(!assigned.contains(&piece));
    }

    #[tokio::test]
    async fn test_assign_piece_many() {
        let pieces_queue = PiecesQueue::new();

        // Create sample pieces
        let pieces = vec![
            create_sample_piece(4, 16384),
            create_sample_piece(1, 16384),
            create_sample_piece(2, 16384),
        ];

        // Populate the queue
        pieces_queue.populate_queue(pieces.clone()).await;

        // Create a bitfield where peer has pieces 1 and 2
        let mut peer_bitfield = Bitfield::from_empty(4);
        peer_bitfield.set_piece(1);
        peer_bitfield.set_piece(2);

        // Assign a piece
        let first_assignment = pieces_queue.assign_piece(&peer_bitfield).await;

        // Assign the same piece again
        let second_assignment = pieces_queue.assign_piece(&peer_bitfield).await;

        // Verify the first assigned piece is piece 1
        assert!(first_assignment.is_some());
        assert_eq!(first_assignment.unwrap().index(), 1);

        // Verify the second assigned piece is piece 2 (not already assigned)
        assert!(second_assignment.is_some());
        assert_eq!(second_assignment.unwrap().index(), 2);

        // Verify that both pieces 1 and 2 are in assigned_pieces
        let assigned = pieces_queue.assigned_pieces.lock().await;
        assert!(assigned.contains(&create_sample_piece(1, 16384)));
        assert!(assigned.contains(&create_sample_piece(2, 16384)));
    }

    #[tokio::test]
    async fn test_assign_piece_already_assigned() {
        let pieces_queue = PiecesQueue::new();

        // Create sample pieces
        let pieces = vec![
            create_sample_piece(4, 16384),
            create_sample_piece(1, 16384),
            create_sample_piece(2, 16384),
        ];

        // Populate the queue
        pieces_queue.populate_queue(pieces.clone()).await;

        // Create a bitfield where peer has pieces 1 and 2
        let mut peer_bitfield = Bitfield::from_empty(4);
        peer_bitfield.set_piece(1);
        peer_bitfield.set_piece(3);

        // Assign a piece
        let first_assignment = pieces_queue.assign_piece(&peer_bitfield).await;

        // Assign the same piece again
        let second_assignment = pieces_queue.assign_piece(&peer_bitfield).await;

        // Verify the first assigned piece is piece 1
        assert!(first_assignment.is_some());
        assert_eq!(first_assignment.unwrap().index(), 1);

        // Verify the second assigned piece is piece 2 (not already assigned)
        assert!(second_assignment.is_none());

        // Verify that both pieces 1 and 2 are in assigned_pieces
        let assigned = pieces_queue.assigned_pieces.lock().await;
        assert!(assigned.contains(&create_sample_piece(1, 16384)));
        assert_eq!(assigned.len(), 1);
    }
}
