use std::{
    collections::{HashMap, HashSet, VecDeque},
    io,
    sync::Arc,
};

use tokio::sync::{broadcast, mpsc, Mutex};
use tracing::{debug, error, info, trace, warn};

use crate::{
    bitfield::Bitfield, p2p::piece::Piece, session::new_peer_session, store::Writer, tracker::Peer,
};

#[derive(Debug)]
pub struct Client {
    download_path: String,
    file_name: String,
    file_size: u64,
    piece_standard_size: u64,
    peer_id: [u8; 20],
    peers: Vec<Peer>,
    max_peer_connections: usize,
    download_state: Arc<DownloadState>,
    // TODO: move to config
    timeout_duration: u64,
    connection_retries: u32,
}

impl Client {
    pub fn new(
        download_path: String,
        bitfield_path: String,
        file_name: String,
        file_size: u64,
        piece_standard_size: u64,
        peer_id: [u8; 20],
        peers: Vec<Peer>,
        peer_total: usize,
        pieces: HashMap<u32, Piece>,
        // TODO: move to config
        timeout_duration: u64,
        connection_retries: u32,
    ) -> Self {
        Self {
            download_path,
            file_name,
            file_size,
            piece_standard_size,
            peer_id,
            peers,
            max_peer_connections: peer_total,
            download_state: Arc::new(DownloadState::new(pieces)),
            timeout_duration,
            connection_retries,
        }
    }

    pub async fn run(self, info_hash: [u8; 20]) -> io::Result<()> {
        let (client_tx, client_rx) = mpsc::channel(50);
        let (disk_tx, disk_rx) = mpsc::channel(50);
        let (shutdown_tx, _) = broadcast::channel::<()>(1); // Shutdown signal

        // Vector to keep track of task handles
        let mut task_handles = Vec::new();

        let client_span = tracing::info_span!("client");
        let _enter = client_span.enter();

        // Bitfield receiver task
        let download_state = Arc::clone(&self.download_state);
        let bitfield_task = tokio::spawn({
            let bitfield_span = tracing::info_span!("bitfield_listener");
            let mut shutdown_rx = shutdown_tx.subscribe();
            async move {
                let _enter = bitfield_span.enter();
                debug!("Bitfield receiver task initialized");

                let mut receiver = client_rx;

                loop {
                    tokio::select! {
                        Some(bitfield) = receiver.recv() => {
                            trace!("Bitfield received: {:?}", bitfield);
                            download_state.add_bitfield_rarity(bitfield).await;

                            if download_state.bitfield_rarity_map.lock().await.len() >= 3 {
                                debug!("Updating remaining pieces state...");
                                download_state.update_queue().await;
                            }
                        }
                        _ = shutdown_rx.recv() => {
                            info!("Bitfield receiver task shutting down");
                            break;
                        }
                    }
                }
            }
        });
        task_handles.push(bitfield_task);

        // let peers = Arc::new(Mutex::new(self.peers.clone()));
        let peers_queue = Arc::new(Mutex::new(VecDeque::from(self.peers.clone())));

        // Spawn tasks for peer connections
        for i in 0..self.max_peer_connections {
            let peers_queue = Arc::clone(&peers_queue);
            let client_tx = client_tx.clone();
            let disk_tx = disk_tx.clone();
            let peer_id = self.peer_id;
            let download_state = Arc::clone(&self.download_state);
            let timeout_duration = self.timeout_duration;
            let connection_retries = self.connection_retries;
            let shutdown_tx = shutdown_tx.clone();
            let mut shutdown_rx = shutdown_tx.subscribe();

            let peer_task = tokio::spawn({
                let peer_span = tracing::info_span!("peer_connection", peer_index = i);
                async move {
                    let _enter = peer_span.enter();
                    debug!(peer_index = i, "Peer connection task started");

                    loop {
                        tokio::select! {
                            _ = async {
                            // check if all pieces are downloaded
                                if download_state.torrent_bitfield.lock().await.has_all_pieces() {
                                    let _ = shutdown_tx.send(()); // Send shutdown signal
                                }
                                let peer = {
                                    let mut queue = peers_queue.lock().await;
                                    queue.pop_front()
                                };

                                match peer {
                                    Some(peer) => {
                                        match new_peer_session(
                                            &peer.address().to_string(),
                                            info_hash,
                                            peer_id,
                                            client_tx.clone(),
                                            disk_tx.clone(),
                                            Arc::clone(&download_state),
                                            timeout_duration,
                                            connection_retries,
                                        ).await {
                                            Ok(_) => {
                                                info!(worker_index = i, peer_address = %peer.address(),  "Peer session completed");
                                            }
                                            Err(e) => {
                                                error!(worker_index = i, peer_address = %peer.address(), error = %e, "Error initializing peer session");
                                            }
                                        }
                                    }
                                    None => {
                                        debug!(worker_index = i, "No more peers available, exiting");
                                    }
                                }
                            } => {}
                            _ = shutdown_rx.recv() => {
                                debug!(worker_index = i, "Peer connection task shutting down");
                                break;
                            }
                        }
                    }
                }
            });
            task_handles.push(peer_task);
        }

        // Disk writer task
        let disk_wirter = Writer::new(&format!("{}/{}", &self.download_path, &self.file_name))?;
        let file_size = self.file_size;
        let piece_standard_size = self.piece_standard_size;
        let mut shutdown_rx = shutdown_tx.subscribe();

        let disk_task = tokio::spawn({
            let disk_span = tracing::info_span!("disk_writer");
            async move {
                let _enter = disk_span.enter();
                debug!("Disk writer task initialized");
                let mut receiver = disk_rx;

                loop {
                    tokio::select! {
                        Some((piece, assembled_piece)) = receiver.recv() => {
                            let piece_index = piece.index();
                            trace!(
                                piece_index = piece.index(),
                                "Received piece for writing to disk"
                            );
                            match disk_wirter.write_piece_to_disk(
                                piece,
                                file_size,
                                piece_standard_size,
                                &assembled_piece,
                            ) {
                                Ok(_) => {
                                    debug!(
                                        piece_index = piece_index,
                                        "Piece written to disk successfully"
                                    );
                                    let mut bitfield = self.download_state.torrent_bitfield.lock().await;
                                    bitfield.set_piece(piece_index as usize);
                                    debug!(piece_index = piece_index, "Bitfield piece index updated");
                                }
                                Err(e) => {
                                    error!(piece_index = piece_index, error = %e, "Error writing piece to disk")
                                }
                            }
                        }
                        _ = shutdown_rx.recv() => {
                            debug!("Disk writer task shutting down");
                            break;
                        }
                    }
                }

                info!("Disk writer task completed");
            }
        });
        task_handles.push(disk_task);

        // Await tasks
        for handle in task_handles {
            if let Err(e) = handle.await {
                error!(error = ?e, "Error in one of the client tasks");
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct DownloadState {
    pub pieces_map: HashMap<u32, Piece>,
    pub torrent_bitfield: Mutex<Bitfield>,
    pub bitfield_rarity_map: Mutex<HashMap<u32, u16>>, // piece index -> piece count in bitfields
    pub pieces_queue: Mutex<Vec<Piece>>,
    pub assigned_pieces: Mutex<HashSet<Piece>>,
}

impl DownloadState {
    pub fn new(pieces_map: HashMap<u32, Piece>) -> Self {
        let pieces_amount = pieces_map.len();
        Self {
            pieces_map,
            torrent_bitfield: Mutex::new(Bitfield::from_empty(pieces_amount)),
            bitfield_rarity_map: Mutex::new(HashMap::new()),
            pieces_queue: Mutex::new(Vec::new()),
            assigned_pieces: Mutex::new(HashSet::new()),
        }
    }

    /// Add a new piece to the remaining queue
    pub async fn add_piece(&self, piece: Piece) {
        let mut queue = self.pieces_queue.lock().await;
        queue.push(piece);
    }

    async fn add_bitfield_rarity(&self, bitfield: Bitfield) {
        let mut rarity_map = self.bitfield_rarity_map.lock().await;
        for byte_index in 0..bitfield.bytes.len() {
            for bit_index in 0..8 {
                let piece_index = (byte_index * 8 + bit_index) as u32;
                if bitfield.has_piece(piece_index as usize) {
                    *rarity_map.entry(piece_index).or_insert(0) += 1;
                }
            }
        }
    }

    pub async fn update_queue(&self) {
        let mut queue = self.pieces_queue.lock().await;
        // remaining.clear(); // Clear previous state

        // Sort pieces based on rarity in `bitfield_rariry_map`
        let rarity_map = self.bitfield_rarity_map.lock().await;
        let mut sorted_pieces: Vec<_> = rarity_map.iter().collect();
        sorted_pieces.sort_by(|a, b| b.1.cmp(&a.1)); // Sort by rarity (most rare first)

        // Populate the `remaining` list with full `Piece` structs
        for (piece_index, _) in sorted_pieces {
            if let Some(piece) = self.pieces_map.get(piece_index) {
                queue.push(piece.clone());
            }
        }
    }

    pub async fn has_missing_pieces(&self, bitfield: &Bitfield) -> bool {
        let client_bitfield = self.torrent_bitfield.lock().await;
        for (byte_index, _) in bitfield.bytes.iter().enumerate() {
            for bit_index in 0..8 {
                let piece_index = (byte_index * 8 + bit_index) as u32;

                if bitfield.has_piece(piece_index as usize)
                    && !client_bitfield.has_piece(piece_index as usize)
                {
                    return true;
                }
            }
        }

        false
    }

    /// Extract the rarest piece available for a peer
    pub async fn assign_piece(&self, peer_bitfield: &Bitfield) -> Option<Piece> {
        let mut queue = self.pieces_queue.lock().await;
        let mut assigned = self.assigned_pieces.lock().await;

        if let Some(pos) = queue.iter().position(|piece| {
            !assigned.contains(piece) && peer_bitfield.has_piece(piece.index() as usize)
        }) {
            let piece = queue.remove(pos);
            assigned.insert(piece.clone());
            return Some(piece);
        }

        None // No suitable piece found
    }

    /// Mark a piece as completed
    pub async fn complete_piece(&self, piece: Piece) {
        let mut assigned = self.assigned_pieces.lock().await;
        assigned.remove(&piece);
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::{bitfield::Bitfield, client::DownloadState, p2p::piece::Piece};

    #[tokio::test]
    async fn test_has_missing_pieces() {
        let mut pieces_map = HashMap::new();
        pieces_map.insert(0, Piece::new(0, 16384, [0u8; 20]));
        pieces_map.insert(1, Piece::new(1, 16384, [0u8; 20]));
        pieces_map.insert(2, Piece::new(2, 16384, [0u8; 20]));
        pieces_map.insert(3, Piece::new(3, 16384, [0u8; 20]));
        pieces_map.insert(4, Piece::new(4, 16384, [0u8; 20]));
        pieces_map.insert(5, Piece::new(5, 16384, [0u8; 20]));
        pieces_map.insert(6, Piece::new(6, 16384, [0u8; 20]));
        pieces_map.insert(7, Piece::new(7, 16384, [0u8; 20]));

        // Create a PiecesState instance
        let download_state = DownloadState::new(pieces_map);

        // Add bitfield with pieces 0, 2 and 5 available
        let bitfield = Bitfield::new(&[0b10100100]); // Bits 0, 2 and 5 are set
        let mut client_bitfield = download_state.torrent_bitfield.lock().await;
        client_bitfield.set_piece(0);
        client_bitfield.set_piece(2);
        client_bitfield.set_piece(5);
        drop(client_bitfield);

        // check if new bitfield has pieces the rarity map does not have (in this case piece 5 is missing)
        let bitfield = Bitfield::new(&[0b10101100]); // Bits 0, 2, 4 and 5 are set
        let result = download_state.has_missing_pieces(&bitfield).await;

        // Assert that piece 4 is missing since it is marked in the bitfield but not in the rarity_map
        assert!(result);
    }

    #[tokio::test]
    async fn test_does_not_have_missing_pieces() {
        let mut pieces_map = HashMap::new();
        pieces_map.insert(0, Piece::new(0, 16384, [0u8; 20]));
        pieces_map.insert(1, Piece::new(1, 16384, [0u8; 20]));
        pieces_map.insert(2, Piece::new(2, 16384, [0u8; 20]));
        pieces_map.insert(3, Piece::new(3, 16384, [0u8; 20]));
        pieces_map.insert(4, Piece::new(4, 16384, [0u8; 20]));
        pieces_map.insert(5, Piece::new(5, 16384, [0u8; 20]));
        pieces_map.insert(6, Piece::new(6, 16384, [0u8; 20]));
        pieces_map.insert(7, Piece::new(7, 16384, [0u8; 20]));

        // Create a PiecesState instance
        let download_state = DownloadState::new(pieces_map);

        // Add bitfield with pieces 0, 2 and 5 available
        let bitfield = Bitfield::new(&[0b10100100]); // Bits 0, 2 and 5 are set
        let mut client_bitfield = download_state.torrent_bitfield.lock().await;
        client_bitfield.set_piece(0);
        client_bitfield.set_piece(2);
        client_bitfield.set_piece(5);
        drop(client_bitfield);

        // check if new bitfield has pieces the rarity map does not have
        let bitfield = Bitfield::new(&[0b10000100]); // Bits 0 and 5 are set
        let result = download_state.has_missing_pieces(&bitfield).await;

        // Assert that no new pieces are found in this bitfield
        assert!(!result);
    }
}
