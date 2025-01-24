use std::{
    collections::{HashMap, VecDeque},
    io,
    sync::Arc,
};

use tokio::sync::{broadcast, mpsc, Mutex};
use tracing::{debug, error, info, warn};

use crate::{
    config::Config,
    disk::disk::Writer,
    download::{message_handler::Handshake, state::DownloadState},
    session::{session::PeerSession, tcp_connection::connect_to_peer},
    torrent::{
        peer::Peer,
        torrent::{Metadata, Torrent},
    },
};
use protocol::bitfield::Bitfield;
use protocol::piece::Piece;

// Configuration constants
const CLIENT_CHANNEL_BUFFER: usize = 128;
const DISK_CHANNEL_BUFFER: usize = 128;
const SHUTDOWN_CHANNEL_BUFFER: usize = 1;

#[derive(Debug)]
pub struct Client {
    download_path: String,
    timeout_duration: u64,
    connection_retries: u32,
    max_peer_connections: u32,
    file_name: String,
    file_size: u64,
    piece_standard_size: u64,
    info_hash: [u8; 20],
    peers: Vec<Peer>,
    peer_id: [u8; 20],
    download_state: Arc<DownloadState>,
}

impl Client {
    pub fn new(
        config: Config,
        torrent: Torrent,
        peers: Vec<Peer>,  // metainfo-tracker
        peer_id: [u8; 20], // fn output
    ) -> Self {
        Self {
            download_path: config.disk.download_path,
            connection_retries: config.p2p.connection_retries,
            timeout_duration: config.p2p.timeout_duration,
            max_peer_connections: config.p2p.max_peer_connections,
            file_name: torrent.metadata.name,
            file_size: torrent.metadata.total_length,
            piece_standard_size: torrent.metadata.piece_length,
            info_hash: torrent.metadata.info_hash,
            peers,
            peer_id,
            download_state: Arc::new(DownloadState::new(torrent.metadata.pieces)),
        }
    }

    pub async fn run(self) -> io::Result<()> {
        let (client_tx, client_rx) = mpsc::channel(CLIENT_CHANNEL_BUFFER);
        let (disk_tx, disk_rx) = mpsc::channel(DISK_CHANNEL_BUFFER);
        let (shutdown_tx, _) = broadcast::channel::<()>(SHUTDOWN_CHANNEL_BUFFER); // Shutdown signal

        // Vector to keep track of task handles
        let mut task_handles = Vec::new();

        let client_tx = Arc::new(client_tx);
        let disk_tx = Arc::new(disk_tx);
        let shutdown_tx = Arc::new(shutdown_tx);
        let download_state = Arc::clone(&self.download_state);

        let bitfield_task = tokio::spawn(Client::bitfield_listener(
            client_rx,
            Arc::clone(&download_state),
            shutdown_tx.subscribe(),
        ));
        task_handles.push(bitfield_task);

        let peers_queue = Arc::new(Mutex::new(VecDeque::from(self.peers.clone())));

        // Spawn tasks for peer connections
        for i in 0..self.max_peer_connections {
            let peers_queue = Arc::clone(&peers_queue);
            let download_state = Arc::clone(&self.download_state);
            let bitfield_tx = Arc::clone(&client_tx);
            let disk_tx = Arc::clone(&disk_tx);
            let shutdown_tx = Arc::clone(&shutdown_tx);

            let peer_task = tokio::spawn(Client::run_peer_connection(
                // TODO: generate peer id
                i,
                self.timeout_duration,
                self.connection_retries,
                self.peer_id,
                self.info_hash,
                peers_queue,
                download_state,
                bitfield_tx,
                disk_tx,
                shutdown_tx,
            ));
            task_handles.push(peer_task);
        }

        // Disk writer task
        let disk_writer = Writer::new(&format!("{}/{}", &self.download_path, &self.file_name))?;
        let file_size = self.file_size;
        let piece_standard_size = self.piece_standard_size;
        let shutdown_rx = shutdown_tx.subscribe();

        let disk_task = tokio::spawn(Client::disk_listener(
            disk_rx,
            disk_writer,
            file_size,
            piece_standard_size,
            Arc::clone(&self.download_state),
            shutdown_rx,
        ));
        task_handles.push(disk_task);

        // Await tasks
        for handle in task_handles {
            if let Err(e) = handle.await {
                error!(error = ?e, "Error in one of the client tasks");
            }
        }

        Ok(())
    }

    async fn bitfield_listener(
        mut receiver: mpsc::Receiver<Bitfield>,
        download_state: Arc<DownloadState>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        debug!("Bitfield receiver task initialized");

        loop {
            tokio::select! {
                Some(bitfield) = receiver.recv() => {
                    debug!("Bitfield received, processing...");
                    download_state.process_bitfield(&bitfield).await;
                }
                _ = shutdown_rx.recv() => {
                    info!("Bitfield receiver task shutting down");
                    break;
                }
            }
        }
    }

    async fn run_peer_connection(
        id: u32,
        timeout_duration: u64,
        connection_retries: u32,
        peer_id: [u8; 20],
        info_hash: [u8; 20],
        peers_queue: Arc<Mutex<VecDeque<Peer>>>,
        download_state: Arc<DownloadState>,
        bitfield_tx: Arc<mpsc::Sender<Bitfield>>,
        disk_tx: Arc<mpsc::Sender<(Piece, Vec<u8>)>>,
        shutdown_tx: Arc<broadcast::Sender<()>>,
    ) {
        debug!(peer_id = id, "Peer connection task started");

        let mut shutdown_rx = shutdown_tx.subscribe();
        loop {
            tokio::select! {
                _ = async {
                    // Check if all pieces are downloaded
                    if download_state.metadata.bitfield.lock().await.has_all_pieces() {
                        let _ = shutdown_tx.send(()); // Send shutdown signal
                    }
                    // Get a peer from the queue
                    let peer = {
                        let mut queue = peers_queue.lock().await;
                        queue.pop_front()
                    };

                    match peer {
                        Some(peer) => {
                            let handshake_metadata = Handshake::new(info_hash, peer_id);
                            let peer_session = PeerSession::new(&peer.address(), handshake_metadata,Arc::clone(&download_state),Arc::clone(&bitfield_tx),Arc::clone(&disk_tx));

                            info!(peer_address = %peer.address(), "TCP connection established");
                            let stream = match connect_to_peer(peer.address().to_string(), timeout_duration, connection_retries).await {
                                Ok(stream) => Some(stream), // Successfully connected, return the stream
                                Err(e) => {
                                    warn!(
                                        peer_id = id,
                                        peer_address = %peer.address(),
                                        error = %e,
                                        "Failed to connect to peer after retries"
                                    );
                                    None // Connection failed, return None
                                }
                            };

                            if let Some(stream) = stream {
                                match peer_session.initialize(stream).await {
                                    Ok(_) => {
                                        info!(

                                            peer_id = id,
                                            peer_address = %peer.address(),
                                            "Peer session completed"
                                        );
                                        // push back the peer if it has availability
                                        peers_queue.lock().await.push_back(peer);
                                    }
                                    Err(e) => {
                                        error!(
                                            peer_id = id,
                                            peer_address = %peer.address(),
                                            error = %e,
                                            "Error initializing peer session"
                                        );
                                    }
                                }
                            }
                        }
                        None => {
                            debug!(peer_id = id, "No more peers available, exiting");
                            let _ = shutdown_tx.send(());
                        }
                    }
                } => {}
                _ = shutdown_rx.recv() => {
                    info!(peer_id = id, "Peer connection task shutting down");
                    break;
                }
            }
        }
    }

    async fn disk_listener(
        mut receiver: mpsc::Receiver<(Piece, Vec<u8>)>,
        disk_writer: Writer,
        file_size: u64,
        piece_standard_size: u64,
        download_state: Arc<DownloadState>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        debug!("Disk writer task initialized");

        loop {
            tokio::select! {
                Some((piece, assembled_piece)) = receiver.recv() => {
                    let piece_index = piece.index();
                    debug!(
                        piece_index = piece_index, bytes = piece.size(), blocks = piece.blocks.len(),
                        "Received piece for writing to disk"
                    );
                    match disk_writer.write_piece_to_disk(
                        &piece,
                        file_size,
                        piece_standard_size,
                        &assembled_piece,
                    ) {
                        Ok(_) => {
                            debug!(
                                piece_index = piece_index,
                                "Piece written to disk successfully"
                            );
                            download_state.mark_piece_downloaded(piece).await;
                            let count: u32 = download_state.downloaded_pieces_count().await;

                            let bitfield = download_state.metadata.bitfield.lock().await;
                            debug!(piece_index = piece_index, "Bitfield: {:?} ({}/{})", bitfield.bytes, count, bitfield.num_pieces);
                            debug!(piece_index = piece_index, "{:?}", bitfield.has_piece(piece_index as usize));
                            drop(bitfield);

                            info!(
                                "Progress {}%",
                                 download_state.download_progress_percent().await,
                            );

                        }
                        Err(e) => {
                            error!(
                                piece_index = piece_index,
                                error = %e,
                                "Error writing piece to disk"
                            );
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Disk writer task shutting down");
                    break;

                }
            }
        }
        info!("Disk writer task completed");
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::client::client::DownloadState;
    use protocol::bitfield::Bitfield;
    use protocol::piece::Piece;

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
        let mut client_bitfield = download_state.metadata.bitfield.lock().await;
        client_bitfield.set_piece(0);
        client_bitfield.set_piece(2);
        client_bitfield.set_piece(5);
        drop(client_bitfield);

        // check if new bitfield has pieces the rarity map does not have (in this case piece 5 is missing)
        let bitfield = Bitfield::new(&[0b10101100]); // Bits 0, 2, 4 and 5 are set
        let result = download_state.metadata.has_missing_pieces(&bitfield).await;

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
        let mut client_bitfield = download_state.metadata.bitfield.lock().await;
        client_bitfield.set_piece(0);
        client_bitfield.set_piece(2);
        client_bitfield.set_piece(5);
        drop(client_bitfield);

        // check if new bitfield has pieces the rarity map does not have
        let bitfield = Bitfield::new(&[0b10000100]); // Bits 0 and 5 are set
        let result = download_state.metadata.has_missing_pieces(&bitfield).await;

        // Assert that no new pieces are found in this bitfield
        assert!(!result);
    }
}
