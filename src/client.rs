use std::{
    collections::{HashMap, HashSet, VecDeque},
    io,
    sync::Arc,
    time::Duration,
};

use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    sync::{broadcast, mpsc, Mutex},
    time::timeout,
};
use tracing::{debug, error, info, trace, warn};

use crate::{
    bitfield::{self, TorrentBitfield},
    p2p::{
        connection::{self, Actor},
        io::{read_message, send_message},
        message::{Bitfield, Message},
        piece::Piece,
    },
    store::Writer,
    tracker::Peer,
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
    pieces_state: Arc<DownloadState>,
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
            pieces_state: Arc::new(DownloadState::new(pieces, &bitfield_path)),
            timeout_duration,
            connection_retries,
        }
    }

    pub async fn run(self, info_hash: [u8; 20]) -> io::Result<()> {
        let (client_tx, client_rx) = mpsc::channel(50);
        let (disk_tx, disk_rx) = mpsc::channel(50);

        // Vector to keep track of task handles
        let mut task_handles = Vec::new();

        let client_span = tracing::info_span!("client");
        let _enter = client_span.enter();

        // Bitfield receiver task
        let pieces_state = Arc::clone(&self.pieces_state);
        let bitfield_task = tokio::spawn({
            let bitfield_span = tracing::info_span!("bitfield_listener");
            async move {
                let _enter = bitfield_span.enter();
                debug!("Bitfield receiver task initialized");

                let mut receiver = client_rx;

                while let Some(bitfield) = receiver.recv().await {
                    trace!("Bitfield received: {:?}", bitfield);
                    pieces_state.add_bitfield_rarity(bitfield).await;

                    if pieces_state.bitfield_rarity_map.lock().await.len() >= 3 {
                        debug!("Updating remaining pieces state...");
                        pieces_state.update_queue().await;
                    }
                }

                info!("Bitfield receiver task shutting down");
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
            let pieces_state = Arc::clone(&self.pieces_state);
            let timeout_duration = self.timeout_duration;
            let connection_retries = self.connection_retries;

            let peer_task = tokio::spawn({
                let peer_span = tracing::info_span!("peer_connection", peer_index = i);
                async move {
                    let _enter = peer_span.enter();
                    debug!(peer_index = i, "Peer connection task started");

                    loop {
                        // check if all pieces are downloaded
                        // if pieces_state.torrent_bitfield.lock().await.has_all_pieces() {
                        //     break;
                        // }

                        let peer = {
                            let mut queue = peers_queue.lock().await;
                            queue.pop_front()
                        };

                        match peer {
                            Some(peer) => {
                                // Try to initialize the peer session
                                match init_peer_session(
                                    &peer.address().to_string(),
                                    info_hash,
                                    peer_id,
                                    client_tx.clone(),
                                    disk_tx.clone(),
                                    Arc::clone(&pieces_state),
                                    timeout_duration,
                                    connection_retries,
                                )
                                .await
                                {
                                    Ok(_) => {
                                        info!(worker_index = i, peer_address = %peer.address(),  "Peer session completed");
                                    }
                                    Err(e) => {
                                        error!(worker_index = i, peer_address = %peer.address(), error = %e, "Error initializing peer session");
                                    }
                                }
                            }
                            None => {
                                // No more peers left in the queue
                                debug!(worker_index = i, "No more peers available, exiting");
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

        let disk_task = tokio::spawn({
            let disk_span = tracing::info_span!("disk_writer");
            async move {
                let _enter = disk_span.enter();
                debug!("Disk writer task initialized");
                let mut receiver = disk_rx;
                while let Some((piece, assembled_piece)) = receiver.recv().await {
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
                            // Update torrent_bitfield
                            {
                                let mut bitfield = self.pieces_state.torrent_bitfield.lock().await;
                                bitfield.set_piece(piece_index as usize);
                            }
                            debug!(piece_index = piece_index, "Bitfield piece index updated");
                        }
                        Err(e) => {
                            error!(piece_index = piece_index, error = %e, "Error writing piece to disk")
                        }
                    }
                }

                info!("Disk writer task shutting down");
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
    pub torrent_bitfield: Mutex<TorrentBitfield>,
    pub bitfield_rarity_map: Mutex<HashMap<u32, u16>>, // piece index -> piece count in bitfields
    pub pieces_queue: Mutex<Vec<Piece>>,
    pub assigned_pieces: Mutex<HashSet<Piece>>,
}

impl DownloadState {
    pub fn new(pieces_map: HashMap<u32, Piece>, bitfield_path: &str) -> Self {
        let pieces_amount = pieces_map.len();
        Self {
            pieces_map,
            torrent_bitfield: Mutex::new(TorrentBitfield::new(
                pieces_amount,
                // FIX: bad design
                bitfield_path,
            )),
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

async fn connect_to_peer_with_retries(
    peer_addr: &str,
    timeout_duration: Duration,
    retries: u32,
) -> io::Result<TcpStream> {
    let mut attempts = 0;
    while attempts < retries {
        match timeout(timeout_duration, TcpStream::connect(peer_addr)).await {
            Ok(Ok(stream)) => return Ok(stream),
            Ok(Err(e)) if e.kind() == io::ErrorKind::ConnectionRefused => {
                warn!(peer_addr, "Connection refused by peer");
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "Peer refused connection",
                ));
            }
            Ok(Err(e)) => {
                warn!(peer_addr, "Failed to connect to peer: {}", e);
                return Err(e); // Propagate other errors
            }
            Err(_) => {
                // Timeout elapsed
                attempts += 1;
                warn!(peer_addr, "Attempt {} timed out, retrying...", attempts);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "All connection attempts timed out",
    ))
}

async fn init_peer_session(
    peer_addr: &str,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    client_tx: mpsc::Sender<Bitfield>,
    disk_tx: mpsc::Sender<(Piece, Vec<u8>)>,
    pieces_state: Arc<DownloadState>,
    // TODO: move to config
    timeout_duration: u64,
    connection_retries: u32,
) -> io::Result<()> {
    let session_span = tracing::info_span!("peer_session", peer_addr = %peer_addr);
    let _enter = session_span.enter();

    info!(peer_addr, "Starting peer connection...");

    let mut stream = connect_to_peer_with_retries(
        peer_addr,
        Duration::from_secs(timeout_duration),
        connection_retries,
    )
    .await?;
    info!(peer_addr, "TCP connection established");

    // Perform handshake
    if let Err(e) = connection::handshake(&mut stream, info_hash, peer_id).await {
        error!(peer_addr, error = %e, "Handshake failed");
        return Err(io::Error::new(io::ErrorKind::Other, "Handshake failed"));
    }
    info!(peer_addr, "Handshake completed");

    let (mut read_half, mut write_half) = tokio::io::split(stream);

    // Create channels for communication
    let (io_rtx, io_rrx) = mpsc::channel(50);
    let (io_wtx, io_wrx) = mpsc::channel(50);
    let (shutdown_tx, _) = broadcast::channel::<()>(1); // Shutdown signal

    // Vector to keep track of task handles
    let mut task_handles = Vec::new();

    let actor = Arc::new(Mutex::new(Actor::new(
        client_tx,
        io_wtx,
        disk_tx,
        shutdown_tx.clone(),
        pieces_state,
    )));

    // Reader task
    let reader_task = tokio::spawn({
        let io_rtx = io_rtx.clone();
        let peer_addr = peer_addr.to_string();
        let reader_span = tracing::info_span!("reader_task", peer_addr = %peer_addr);
        let mut shutdown_rx = shutdown_tx.subscribe();
        async move {
            let _enter = reader_span.enter();
            debug!(peer_addr = %peer_addr, "Initializing peer IO reader...");
            loop {
                tokio::select! {
                    result = read_message(&mut read_half) => {
                        match result {
                            Ok(message) => {
                                if io_rtx.send(message).await.is_err() {
                                    error!(peer_addr = %peer_addr, "Receiver dropped; stopping reader task");
                                    break;
                                }
                            }
                            Err(e) => {
                                error!(peer_addr = %peer_addr, error = %e, "Error reading message");
                                break;
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!(peer_addr = %peer_addr, "Reader task received shutdown signal");
                            drop(read_half);
                        break;
                    }
                }
            }
        }
    });
    task_handles.push(reader_task);

    // Writer task
    let writer_task = tokio::spawn({
        let peer_addr = peer_addr.to_string();
        let writer_span = tracing::info_span!("writer_task", peer_addr = %peer_addr);
        let mut receiver: mpsc::Receiver<Message> = io_wrx;
        let mut shutdown_rx = shutdown_tx.subscribe();
        async move {
            let _enter = writer_span.enter();
            debug!(peer_addr = %peer_addr, "Initializing peer IO writer...");
            loop {
                tokio::select! {
                    Some(message) = receiver.recv() => {
                        if let Err(e) = send_message(&mut write_half, message).await {
                            error!(peer_addr = %peer_addr, error = %e, "Error sending message");
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!(peer_addr = %peer_addr, "Writer task received shutdown signal");
                        let _ = write_half.shutdown().await;
                        drop(write_half);
                        info!(peer_addr = %peer_addr, "TCP stream closed");
                        break;
                    }
                }
            }
        }
    });
    task_handles.push(writer_task);

    // Actor handler task
    let actor_task = tokio::spawn({
        let actor = Arc::clone(&actor);
        let actor_span = tracing::info_span!("actor_task", peer_addr = %peer_addr);
        let mut receiver = io_rrx;
        let peer_addr = peer_addr.to_string();
        let mut shutdown_rx = shutdown_tx.subscribe();
        async move {
            let _enter = actor_span.enter();
            debug!(peer_addr = %peer_addr, "Initializing peer actor handler...");
            loop {
                tokio::select! {
                    Some(message) = receiver.recv() => {
                        let mut actor = actor.lock().await;
                        if let Err(e) = actor.handle_message(message).await {
                            error!(peer_addr = %peer_addr, error = %e, "Actor handler error");
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!(peer_addr = %peer_addr, "Actor task received shutdown signal");
                        break;
                    }
                }
            }
        }
    });
    task_handles.push(actor_task);

    // Piece request task
    let piece_request_handle = tokio::spawn({
        let actor = Arc::clone(&actor);
        let peer_addr = peer_addr.to_string();
        let request_span = tracing::info_span!("piece_request_task", peer_addr = %peer_addr);
        let mut shutdown_rx = shutdown_tx.subscribe();
        async move {
            let _enter = request_span.enter();
            debug!(peer_addr = %peer_addr, "Initializing piece requester...");
            loop {
                tokio::select! {
                    _ = async {
                        let mut actor = actor.lock().await;
                        if actor.ready_to_request().await {
                            if let Err(e) = actor.request().await {
                                error!(peer_addr = %peer_addr, error = %e, "Piece request error");
                            }
                        }
                        // Check if the client has all the peer pieces
                        if actor.is_complete().await {
                            debug!(peer_addr = %peer_addr, "All pieces downloaded. Sending shutdown signal...");
                            let _ = shutdown_tx.send(()); // Send shutdown signal
                        }
                    } => {}
                    _ = shutdown_rx.recv() => {
                        debug!(peer_addr = %peer_addr, "Piece requester received shutdown signal");
                        break;
                    }
                }
            }
        }
    });
    task_handles.push(piece_request_handle);

    // Await tasks
    for handle in task_handles {
        if let Err(e) = handle.await {
            tracing::error!(peer_addr, error = ?e, "Peer connection task error");
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::{
        client::DownloadState,
        p2p::{message::Bitfield, piece::Piece},
    };

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
        let pieces_state = DownloadState::new(pieces_map, "torrent_path");

        // Add bitfield with pieces 0, 2 and 5 available
        let bitfield = Bitfield::new(&[0b10100100]); // Bits 0, 2 and 5 are set
        let mut client_bitfield = pieces_state.torrent_bitfield.lock().await;
        client_bitfield.set_piece(0);
        client_bitfield.set_piece(2);
        client_bitfield.set_piece(5);
        drop(client_bitfield);

        // check if new bitfield has pieces the rarity map does not have (in this case piece 5 is missing)
        let bitfield = Bitfield::new(&[0b10101100]); // Bits 0, 2, 4 and 5 are set
        let result = pieces_state.has_missing_pieces(&bitfield).await;

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
        let pieces_state = DownloadState::new(pieces_map, "torrent_path");

        // Add bitfield with pieces 0, 2 and 5 available
        let bitfield = Bitfield::new(&[0b10100100]); // Bits 0, 2 and 5 are set
        let mut client_bitfield = pieces_state.torrent_bitfield.lock().await;
        client_bitfield.set_piece(0);
        client_bitfield.set_piece(2);
        client_bitfield.set_piece(5);
        drop(client_bitfield);

        // check if new bitfield has pieces the rarity map does not have
        let bitfield = Bitfield::new(&[0b10000100]); // Bits 0 and 5 are set
        let result = pieces_state.has_missing_pieces(&bitfield).await;

        // Assert that no new pieces are found in this bitfield
        assert!(!result);
    }
}
