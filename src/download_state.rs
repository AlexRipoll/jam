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
        let bitfield = self.bitfield.lock().await;
        bitfield.has_piece(index)
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
