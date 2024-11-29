use std::{
    collections::{HashMap, HashSet},
    io,
    sync::Arc,
};

use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex},
};

use crate::{
    p2p::{
        client::{self, Actor},
        io::{read_message, send_message},
        message::{Bitfield, Message},
        piece::Piece,
    },
    store::Writer,
    tracker::Peer,
};

#[derive(Debug)]
struct Client {
    download_path: String,
    file_size: u64,
    piece_standard_size: u64,
    peer_id: [u8; 20],
    peers: Vec<Peer>,
    peer_channels: Vec<mpsc::Receiver<Bitfield>>,
    pieces_state: Arc<PiecesState>,
}

impl Client {
    fn new(
        download_path: String,
        file_size: u64,
        piece_standard_size: u64,
        peer_id: [u8; 20],
        peers: Vec<Peer>,
        pieces: HashMap<u32, Piece>,
    ) -> Self {
        Self {
            download_path,
            peer_id,
            peers,
            peer_channels: Vec::new(),
            pieces_state: Arc::new(PiecesState::new(pieces)),
            file_size,
            piece_standard_size,
        }
    }

    async fn run(self, info_hash: [u8; 20]) {
        let (client_tx, client_rx) = mpsc::channel(50);
        let (disk_tx, disk_rx) = mpsc::channel(50);

        // Bitfield receiver task
        let pieces_state = Arc::clone(&self.pieces_state);
        tokio::spawn(async move {
            let mut receiver = client_rx;
            while let Some(bitfield) = receiver.recv().await {
                // let mut pieces_state = pieces_state.lock().await;
                pieces_state.add_bitfield_rarity(bitfield).await;

                if pieces_state.bitfield_rariry_map.lock().await.len() >= 3 {
                    pieces_state.update_remaining().await;
                }
            }
        });

        // initiate 4 peer connections
        for i in 0..4 {
            let client_tx = client_tx.clone();
            let disk_tx = disk_tx.clone();
            let peer_address = self.peers[i].address().to_string();
            let peer_id = self.peer_id;
            let pieces_state = Arc::clone(&self.pieces_state);

            tokio::spawn(async move {
                if let Err(e) = peer_connection(
                    &peer_address,
                    info_hash,
                    peer_id,
                    client_tx,
                    disk_tx,
                    pieces_state,
                )
                .await
                {
                    eprintln!("peer connection error: {e}");
                }
            });
        }

        // Disk writer task
        let disk_wirter = Writer::new(&self.download_path);
        let file_size = self.file_size;
        let piece_standard_size = self.piece_standard_size;

        tokio::spawn(async move {
            let mut receiver = disk_rx;
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

async fn peer_connection(
    peer_addr: &str,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    client_tx: mpsc::Sender<Bitfield>,
    disk_tx: mpsc::Sender<(Piece, Vec<u8>)>,
    pieces_state: Arc<PiecesState>,
) -> io::Result<()> {
    let mut stream = TcpStream::connect(peer_addr).await?;
    println!("TCP conncetion established with peer at address: {peer_addr}");

    // Perform handshake
    if let Err(e) = client::handshake(&mut stream, info_hash, peer_id).await {
        eprintln!("Handshake failed: {e}");
        return Err(io::Error::new(io::ErrorKind::Other, "Handshake failed"));
    }

    let (mut read_half, mut write_half) = tokio::io::split(stream);

    // Create channels for communication
    let (io_rtx, io_rrx) = mpsc::channel(50);
    let (io_wtx, io_wrx) = mpsc::channel(50);

    let actor = Arc::new(Mutex::new(Actor::new(
        client_tx,
        io_wtx,
        disk_tx,
        pieces_state,
    )));

    // Reader task
    tokio::spawn({
        let io_rtx = io_rtx.clone();
        async move {
            loop {
                match read_message(&mut read_half).await {
                    Ok(message) => {
                        if io_rtx.send(message).await.is_err() {
                            println!("Receiver dropped; stopping reader task.");
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

    // Writer task
    tokio::spawn(async move {
        // writer
        let mut receiver: mpsc::Receiver<Message> = io_wrx;
        while let Some(message) = receiver.recv().await {
            if let Err(e) = send_message(&mut write_half, message).await {
                eprintln!("send error: {e}");
            }
        }
    });

    // Actor handler task
    tokio::spawn({
        let actor = Arc::clone(&actor);
        let mut receiver = io_rrx;
        async move {
            while let Some(message) = receiver.recv().await {
                let mut actor = actor.lock().await;
                if let Err(e) = actor.handle_message(message).await {
                    eprintln!("actor handler error: {e}")
                }
            }
        }
    });

    // Piece request task
    tokio::spawn({
        let actor = Arc::clone(&actor);
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

    Ok(())
}
