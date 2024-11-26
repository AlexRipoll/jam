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
    tracker::Peer,
};

#[derive(Debug)]
struct Client {
    peers: Vec<Peer>,
    peer_channels: Vec<mpsc::Receiver<Bitfield>>,
    bitfield_rariry_map: HashMap<u32, u16>, // piece index -> piece count in bitfields
    pieces_state: Arc<PiecesState>,
}

#[derive(Debug)]
pub struct PiecesState {
    pub remaining: Mutex<Vec<u32>>,
    pub assigned: Mutex<HashSet<u32>>,
}

impl PiecesState {
    pub fn new() -> Self {
        Self {
            remaining: Mutex::new(Vec::new()),
            assigned: Mutex::new(HashSet::new()),
        }
    }

    /// Add a new piece to the remaining queue
    pub async fn add_piece(&self, piece_index: u32) {
        let mut remaining = self.remaining.lock().await;
        remaining.push(piece_index);
    }

    /// Extract the rarest piece available for a peer
    pub async fn assign_piece(&self, peer_bitfield: &Bitfield) -> Option<u32> {
        let mut remaining = self.remaining.lock().await;
        let mut assigned = self.assigned.lock().await;

        for (i, &piece_index) in remaining.iter().enumerate() {
            if !assigned.contains(&piece_index) && peer_bitfield.has_piece(piece_index as usize) {
                assigned.insert(piece_index);
                remaining.remove(i);
                return Some(piece_index);
            }
        }

        None // No suitable piece found
    }

    /// Mark a piece as completed
    pub async fn complete_piece(&self, piece_index: u32) {
        let mut assigned = self.assigned.lock().await;
        assigned.remove(&piece_index);
    }
}

async fn bittorrent_client(
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
                actor.handle_message(message);
            }
        }
    });

    // Piece request task
    tokio::spawn({
        let actor = Arc::clone(&actor);
        async move {
            loop {
                let mut actor = actor.lock().await;
                if actor.ready_to_request() {
                    actor.request();
                }
            }
        }
    });

    Ok(())
}
