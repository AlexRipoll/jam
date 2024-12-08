use std::{
    collections::{HashMap, HashSet},
    io,
    sync::Arc,
    time::Duration,
};

use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex, Semaphore},
    time::timeout,
};
use tracing::{debug, error, info, trace, warn};

use crate::{
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
    pieces_state: Arc<PiecesState>,
    // TODO: move to config
    timeout_duration: u64,
    connection_retries: u32,
}

impl Client {
    pub fn new(
        download_path: String,
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
            pieces_state: Arc::new(PiecesState::new(pieces)),
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
                        pieces_state.update_remaining().await;
                    }
                }

                info!("Bitfield receiver task shutting down");
            }
        });
        task_handles.push(bitfield_task);

        // Semaphore to enforce 4 active peer sessions
        let active_sessions = Arc::new(Semaphore::new(self.max_peer_connections));
        let peers = Arc::new(Mutex::new(self.peers.clone()));

        // Spawn tasks for peer connections
        for i in 0..self.max_peer_connections {
            let active_sessions = Arc::clone(&active_sessions);
            let peers = Arc::clone(&peers);
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
                    // Acquire a permit to ensure concurrency limit
                    let _permit = active_sessions.acquire().await;
                    debug!(peer_index = i, "Peer connection task started");

                    loop {
                        // Get a peer from the list
                        let peer_address = {
                            let mut peers = peers.lock().await;
                            if let Some(peer) = peers.pop() {
                                peer.address().to_string()
                            } else {
                                warn!(peer_index = i, "No more peers available");
                                break; // No more peers available
                            }
                        };

                        // Try to initialize the peer session
                        match init_peer_session(
                            &peer_address,
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
                                info!(peer_index = i, peer_address = %peer_address, "Peer session initialized successfully");
                                break;
                            }
                            Err(e) => {
                                error!(peer_index = i, peer_address = %peer_address, error = %e, "Error initializing peer session");
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
pub struct PiecesState {
    pub pieces_map: HashMap<u32, Piece>,
    pub bitfield_rarity_map: Mutex<HashMap<u32, u16>>, // piece index -> piece count in bitfields
    pub remaining: Mutex<Vec<Piece>>,
    pub assigned: Mutex<HashSet<Piece>>,
}

impl PiecesState {
    pub fn new(pieces_map: HashMap<u32, Piece>) -> Self {
        Self {
            pieces_map,
            bitfield_rarity_map: Mutex::new(HashMap::new()),
            remaining: Mutex::new(Vec::new()),
            assigned: Mutex::new(HashSet::new()),
        }
    }

    /// Add a new piece to the remaining queue
    pub async fn add_piece(&self, piece: Piece) {
        let mut remaining = self.remaining.lock().await;
        remaining.push(piece);
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

    pub async fn update_remaining(&self) {
        let mut remaining = self.remaining.lock().await;
        // remaining.clear(); // Clear previous state

        // Sort pieces based on rarity in `bitfield_rariry_map`
        let rarity_map = self.bitfield_rarity_map.lock().await;
        let mut sorted_pieces: Vec<_> = rarity_map.iter().collect();
        sorted_pieces.sort_by(|a, b| b.1.cmp(&a.1)); // Sort by rarity (most rare first)

        // Populate the `remaining` list with full `Piece` structs
        for (piece_index, _) in sorted_pieces {
            if let Some(piece) = self.pieces_map.get(piece_index) {
                remaining.push(piece.clone());
            }
        }
    }

    /// Extract the rarest piece available for a peer
    pub async fn assign_piece(&self, peer_bitfield: &Bitfield) -> Option<Piece> {
        let mut remaining = self.remaining.lock().await;
        let mut assigned = self.assigned.lock().await;

        if let Some(pos) = remaining.iter().position(|piece| {
            !assigned.contains(piece) && peer_bitfield.has_piece(piece.index() as usize)
        }) {
            let piece = remaining.remove(pos);
            assigned.insert(piece.clone());
            return Some(piece);
        }

        None // No suitable piece found
    }

    /// Mark a piece as completed
    pub async fn complete_piece(&self, piece: Piece) {
        let mut assigned = self.assigned.lock().await;
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
    pieces_state: Arc<PiecesState>,
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

    // Vector to keep track of task handles
    let mut task_handles = Vec::new();

    let actor = Arc::new(Mutex::new(Actor::new(
        client_tx,
        io_wtx,
        disk_tx,
        pieces_state,
    )));

    // Reader task
    let reader_task = tokio::spawn({
        let io_rtx = io_rtx.clone();
        let peer_addr = peer_addr.to_string();
        let reader_span = tracing::info_span!("reader_task", peer_addr = %peer_addr);
        async move {
            let _enter = reader_span.enter();
            debug!(peer_addr = %peer_addr, "Initializing peer IO reader...");
            loop {
                match read_message(&mut read_half).await {
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
        }
    });
    task_handles.push(reader_task);

    // Writer task
    let writer_task = tokio::spawn({
        let peer_addr = peer_addr.to_string();
        let writer_span = tracing::info_span!("writer_task", peer_addr = %peer_addr);
        async move {
            let _enter = writer_span.enter();
            debug!(peer_addr = %peer_addr, "Initializing peer IO writer...");
            let mut receiver: mpsc::Receiver<Message> = io_wrx;
            while let Some(message) = receiver.recv().await {
                if let Err(e) = send_message(&mut write_half, message).await {
                    error!(peer_addr = %peer_addr, error = %e, "Error sending message");
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
        async move {
            let _enter = actor_span.enter();
            debug!(peer_addr = %peer_addr, "Initializing peer actor handler...");
            while let Some(message) = receiver.recv().await {
                let mut actor = actor.lock().await;
                if let Err(e) = actor.handle_message(message).await {
                    error!(peer_addr = %peer_addr, error = %e, "Actor handler error");
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
        async move {
            let _enter = request_span.enter();
            debug!(peer_addr = %peer_addr, "Initializing piece requester...");
            loop {
                let mut actor = actor.lock().await;
                if actor.ready_to_request().await {
                    // if queue is empty, fill with new pieces
                    if let Err(e) = actor.request().await {
                        error!(peer_addr = %peer_addr, error = %e, "Piece request error");
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
    tracing::info!(peer_addr, "Peer session completed");

    Ok(())
}
