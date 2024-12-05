use std::{
    collections::{HashMap, HashSet},
    io,
    sync::Arc,
    time::Duration,
};

use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex},
    time::timeout,
};

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
    total_peers: usize,
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
            total_peers: peer_total,
            pieces_state: Arc::new(PiecesState::new(pieces)),
            timeout_duration,
            connection_retries,
        }
    }

    pub async fn run(self, info_hash: [u8; 20]) {
        let (client_tx, client_rx) = mpsc::channel(50);
        let (disk_tx, disk_rx) = mpsc::channel(50);

        // Vector to keep track of task handles
        let mut task_handles = Vec::new();

        // Bitfield receiver task
        let pieces_state = Arc::clone(&self.pieces_state);
        let bitfield_task = tokio::spawn(async move {
            let mut receiver = client_rx;
            println!(">> Bitfield receiver listening...");

            while let Some(bitfield) = receiver.recv().await {
                // let mut pieces_state = pieces_state.lock().await;
                println!(">> Bitfield received");
                pieces_state.add_bitfield_rarity(bitfield).await;

                if pieces_state.bitfield_rariry_map.lock().await.len() >= 3 {
                    pieces_state.update_remaining().await;
                }
            }
        });
        task_handles.push(bitfield_task);

        // initiate 4 peer connections
        for i in 0..self.total_peers {
            let client_tx = client_tx.clone();
            let disk_tx = disk_tx.clone();
            let peer_address = self.peers[i].address().to_string();
            let peer_id = self.peer_id;
            let pieces_state = Arc::clone(&self.pieces_state);
            let timeout_duration = self.timeout_duration;
            let connection_retries = self.connection_retries;

            let peer_task = tokio::spawn(async move {
                if let Err(e) = init_peer_session(
                    &peer_address,
                    info_hash,
                    peer_id,
                    client_tx,
                    disk_tx,
                    pieces_state,
                    timeout_duration,
                    connection_retries,
                )
                .await
                {
                    eprintln!("peer connection error: {e}");
                }
            });
            task_handles.push(peer_task);
        }

        // Disk writer task
        let disk_wirter = Writer::new(&format!("{}/{}", &self.download_path, &self.file_name));
        let file_size = self.file_size;
        let piece_standard_size = self.piece_standard_size;

        let disk_task = tokio::spawn(async move {
            let mut receiver = disk_rx;
            println!(">> Disk receiver listening...");
            while let Some((piece, assembled_piece)) = receiver.recv().await {
                if let Err(e) = disk_wirter.write_piece_to_disk(
                    piece,
                    file_size,
                    piece_standard_size,
                    &assembled_piece,
                ) {
                    eprintln!("Error writing piece to disk: {}", e);
                }
            }
        });
        task_handles.push(disk_task);

        // Await tasks
        for handle in task_handles {
            if let Err(e) = handle.await {
                eprintln!("Client task error: {:?}", e);
            }
        }
    }
}

#[derive(Debug)]
pub struct PiecesState {
    pub pieces_map: HashMap<u32, Piece>,
    pub bitfield_rariry_map: Mutex<HashMap<u32, u16>>, // piece index -> piece count in bitfields
    pub remaining: Mutex<Vec<Piece>>,
    pub assigned: Mutex<HashSet<Piece>>,
}

impl PiecesState {
    pub fn new(pieces_map: HashMap<u32, Piece>) -> Self {
        Self {
            pieces_map,
            bitfield_rariry_map: Mutex::new(HashMap::new()),
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
        let mut rarity_map = self.bitfield_rariry_map.lock().await;
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
        let rarity_map = self.bitfield_rariry_map.lock().await;
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
                eprintln!("Connection refused by peer: {}", peer_addr);
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "Peer refused connection",
                ));
            }
            Ok(Err(e)) => {
                eprintln!("Failed to connect to peer {}: {}", peer_addr, e);
                return Err(e); // Propagate other errors
            }
            Err(_) => {
                // Timeout elapsed
                attempts += 1;
                eprintln!("Attempt {} timed out, retrying...", attempts);
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
    println!(">> Starting peer connection...");
    let mut stream = connect_to_peer_with_retries(
        peer_addr,
        Duration::from_secs(timeout_duration),
        connection_retries,
    )
    .await?;
    println!("TCP connection established with peer at address: {peer_addr}");

    // Perform handshake
    if let Err(e) = connection::handshake(&mut stream, info_hash, peer_id).await {
        eprintln!("Handshake failed: {e}");
        return Err(io::Error::new(io::ErrorKind::Other, "Handshake failed"));
    }
    println!("Handshake completed");

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
        println!(">> Peer IO reader initialized...");
        async move {
            loop {
                match read_message(&mut read_half).await {
                    Ok(message) => {
                        if io_rtx.send(message).await.is_err() {
                            eprintln!("Receiver dropped; stopping reader task.");
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("Error reading message: {e}");
                        break;
                    }
                }
            }
        }
    });
    task_handles.push(reader_task);

    // Writer task
    let writer_task = tokio::spawn(async move {
        // writer
        let mut receiver: mpsc::Receiver<Message> = io_wrx;
        println!(">> Peer IO writer initialized...");
        while let Some(message) = receiver.recv().await {
            if let Err(e) = send_message(&mut write_half, message).await {
                eprintln!("send error: {e}");
            }
        }
    });
    task_handles.push(writer_task);

    // Actor handler task
    let actor_task = tokio::spawn({
        let actor = Arc::clone(&actor);
        let mut receiver = io_rrx;
        println!(">> Peer reader initialized...");
        async move {
            while let Some(message) = receiver.recv().await {
                let mut actor = actor.lock().await;
                if let Err(e) = actor.handle_message(message).await {
                    eprintln!("actor handler error: {e}")
                }
            }
        }
    });
    task_handles.push(actor_task);

    // Piece request task
    let piece_request_handle = tokio::spawn({
        let actor = Arc::clone(&actor);
        println!(">> Peer requester initialized...");
        async move {
            loop {
                let mut actor = actor.lock().await;
                if actor.ready_to_request().await {
                    // if queue is empty, fill with new pieces
                    if let Err(e) = actor.request().await {
                        eprintln!("actor piece request error: {e}")
                    }
                }
            }
        }
    });
    task_handles.push(piece_request_handle);

    // Await tasks
    for handle in task_handles {
        if let Err(e) = handle.await {
            eprintln!("Peer connection task error: {:?}", e);
        }
    }

    Ok(())
}
