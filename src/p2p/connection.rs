use rand::{distributions::Alphanumeric, Rng};
use sha1::{Digest, Sha1};
use std::error::Error;
use std::fmt::Display;
use std::sync::Arc;
use std::{collections::HashMap, usize};
use tokio::io::{self, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info};

use crate::bitfield::Bitfield;
use crate::download_state::DownloadState;
use crate::p2p::message::{Message, MessageId, PiecePayload, TransferPayload};

use super::piece::{Piece, PieceError};

const PSTR: &str = "BitTorrent protocol";

#[derive(Debug)]
pub struct Actor {
    client_tx: Arc<mpsc::Sender<Bitfield>>,
    disk_tx: Arc<mpsc::Sender<(Piece, Vec<u8>)>>,
    io_wtx: mpsc::Sender<Message>,
    shutdown_tx: broadcast::Sender<()>,
    state: State,
    download_state: Arc<DownloadState>,
}

#[derive(Debug)]
struct State {
    is_choked: bool,
    is_interested: bool,
    peer_choked: bool,
    peer_interested: bool,
    peer_bitfield: Bitfield,
    pieces_status: HashMap<usize, Piece>,
    download_queue: Vec<Piece>,
}

impl Default for State {
    fn default() -> Self {
        State {
            is_choked: true,
            is_interested: false,
            peer_choked: true,
            peer_interested: false,
            peer_bitfield: Bitfield::new(&vec![]),
            pieces_status: HashMap::new(),
            download_queue: vec![],
        }
    }
}

impl Actor {
    pub fn new(
        client_tx: Arc<mpsc::Sender<Bitfield>>,
        io_wtx: mpsc::Sender<Message>,
        disk_tx: Arc<mpsc::Sender<(Piece, Vec<u8>)>>,
        shutdown_tx: broadcast::Sender<()>,
        pieces_state: Arc<DownloadState>,
    ) -> Self {
        Self {
            client_tx,
            disk_tx,
            io_wtx,
            shutdown_tx,
            state: State::default(),
            download_state: pieces_state,
        }
    }

    pub async fn ready_to_request(&self) -> bool {
        !self.state.is_choked
            && self.state.is_interested
            && !self
                .download_state
                .pieces_queue
                .queue
                .lock()
                .await
                .is_empty()
    }

    pub async fn handle_message(&mut self, message: Message) -> Result<(), P2pError> {
        // modifies state
        match message.message_id {
            MessageId::KeepAlive => {
                debug!("Received KeepAlive message");
                // TODO: extend stream alive
            }
            MessageId::Choke => {
                self.state.is_choked = true;
                info!("Choked by peer");
                // TODO: logic in case peer chokes but client is still interested
            }
            MessageId::Unchoke => {
                self.state.is_choked = false;
                info!("Unchoked by peer");
            }
            MessageId::Interested => {
                self.state.peer_interested = true;
                info!("Peer expressed insterest");

                // TODO: logic for unchoking peer
                if self.state.peer_choked {
                    self.state.peer_choked = false;
                    info!("Peer unchoked");
                    self.io_wtx
                        .send(Message::new(MessageId::Unchoke, None))
                        .await?;
                }
            }
            MessageId::NotInterested => {
                self.state.peer_interested = false;
                info!("Peer no longer insterested");
            }
            MessageId::Have => {
                debug!("'Have' message received from peer");
                self.process_have(&message);
            }
            MessageId::Bitfield => {
                // Update the peer's bitfield and determine if the client is interested
                info!("Received 'Bitfield' message from peer");
                let bitfield = Bitfield::new(message.payload.as_ref().unwrap());
                debug!(bitfield = ?bitfield, "Updated peer bitfield");

                // check if peer has any piece we are interested in downloading
                if self
                    .download_state
                    .metadata
                    .has_missing_pieces(&bitfield)
                    .await
                {
                    self.state.peer_bitfield = bitfield.clone();
                    self.client_tx.send(bitfield).await?;

                    self.io_wtx
                        .send(Message::new(MessageId::Unchoke, None))
                        .await?;
                    self.io_wtx
                        .send(Message::new(MessageId::Interested, None))
                        .await?;
                    self.state.is_interested = true;
                } else {
                    // close TCP stream
                    self.shutdown_tx.send(())?;
                }
            }
            MessageId::Request => {
                // send piece blocks to peer
                todo!()
            }
            MessageId::Piece => {
                // store piece block
                info!("'Piece' message received from peer");
                self.download(message.payload.as_deref()).await?;
            }
            MessageId::Cancel => {
                todo!()
            }
            MessageId::Port => {
                todo!()
            }
        }

        Ok(())
    }

    fn process_have(&mut self, message: &Message) {
        let mut piece_index_be = [0u8; 4];
        piece_index_be.copy_from_slice(message.payload.as_ref().unwrap());
        let piece_index = u32::from_be_bytes(piece_index_be) as usize;

        if !self.state.peer_bitfield.has_piece(piece_index) {
            self.state.peer_bitfield.set_piece(piece_index);
        }
        // TODO: check if interested
    }

    async fn download(&mut self, payload: Option<&[u8]>) -> Result<(), P2pError> {
        let bytes = payload.ok_or(P2pError::EmptyPayload)?;
        let payload = PiecePayload::deserialize(bytes)?;

        let piece = self
            .state
            .pieces_status
            .get_mut(&(payload.index as usize))
            .ok_or(P2pError::PieceNotFound)?;

        debug!(piece_index= ?payload.index, block_offset= ?payload.begin, "Downloading piece");
        // Add the block to the piece
        piece.add_block(payload.begin, payload.block.clone())?;

        // Check if the piece is ready and not yet completed
        if piece.is_ready() && !piece.is_finalized() {
            let buffer = piece.assemble()?;
            debug!(piece_index= ?payload.index, "Piece ready. Verifying hash...");

            // Validate the piece hash
            if is_valid_piece(piece.hash(), &buffer) {
                debug!(piece_index= ?payload.index, "Piece hash verified. Sending to disk");
                self.disk_tx.send((piece.clone(), buffer)).await?;
                // NOTE: consider removing from pieces hashmap instead
                piece.mark_finalized();

                // remove the finalized piece from the queue
                self.state
                    .download_queue
                    .retain(|piece| piece.index() != piece.index());
            } else {
                debug!(piece_index= ?payload.index, "Piece hash verification failed");
                return Err(P2pError::PieceInvalid);
            }
        }

        Ok(())
    }

    pub async fn fill_download_queue(&mut self) {
        debug!("Filling download queue");
        for _ in 0..3 {
            if self.state.download_queue.len() >= 3 {
                debug!("Download queue max capacity reached");
                break;
            }

            if let Some(piece) = self
                .download_state
                .assign_piece(&self.state.peer_bitfield)
                .await
            {
                debug!(piece_index=?piece.index(),"Assigned piece to queue");
                self.state.download_queue.push(piece);
            }
        }
    }

    pub async fn is_complete(&self) -> bool {
        if self.state.peer_bitfield.bytes.is_empty() {
            return false;
        }

        let current_bitfield = self
            .download_state
            .metadata
            .bitfield
            .lock()
            .await
            .bytes
            .clone();

        self.state
            .peer_bitfield
            .bytes
            .iter()
            .zip(current_bitfield.iter())
            .all(|(x, y)| x == y)
    }

    pub async fn request(&mut self) -> Result<(), P2pError> {
        let (queue_starting_index, block_offset) = (0, 0);

        if self.state.download_queue.is_empty() {
            debug!("Download queue is empty; filling download queue");
            self.fill_download_queue().await;
        }

        let queue_length = self.state.download_queue.len();
        for i in queue_starting_index..queue_length {
            let piece = self.state.download_queue[i].clone();
            self.state
                .pieces_status
                .entry(piece.index() as usize)
                .or_insert_with(|| piece.clone());

            self.request_from_offset(&piece, block_offset).await?;
        }
        debug!("Completed piece requests");

        Ok(())
    }

    /// Core logic to request blocks from a piece starting at a given offset
    async fn request_from_offset(
        &mut self,
        piece: &Piece,
        start_offset: u32,
    ) -> Result<(), P2pError> {
        let total_blocks = (piece.size() + 16384 - 1) / 16384;

        debug!(
            piece = ?piece.index(), total_blocks= ?total_blocks, start_offset= ?start_offset, "Requesting blocks for piece"
        );
        for block_index in ((start_offset / 16384) as usize)..total_blocks {
            let block_offset = block_index as u32 * 16384;
            let block_size = 16384.min(piece.size() as u32 - block_offset);

            let payload = TransferPayload::new(piece.index(), block_offset, block_size).serialize();
            debug!(
                piece_index = ?piece.index(),block_offset= ?block_offset, block_size= ?block_size, "Sending block request"
            );
            self.io_wtx
                .send(Message::new(MessageId::Request, Some(payload)))
                .await?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct Handshake {
    pub pstr: String,
    pub reserved: [u8; 8],
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
}

impl Handshake {
    pub fn new(info_hash: [u8; 20], peer_id: [u8; 20]) -> Handshake {
        Handshake {
            pstr: PSTR.to_string(),
            reserved: [0u8; 8],
            info_hash,
            peer_id,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.pstr.len() + 49);

        buf.push(self.pstr.len() as u8);
        buf.extend_from_slice(self.pstr.as_bytes());
        buf.extend_from_slice(&self.reserved);
        buf.extend_from_slice(&self.info_hash);
        buf.extend_from_slice(&self.peer_id);

        buf
    }

    pub fn deserialize(buffer: Vec<u8>) -> Result<Handshake, P2pError> {
        let mut offset = 0;

        // Parse `pstr_length` (1 byte)
        let pstr_length = buffer[offset];
        offset += 1;

        // Check if `pstr_length` matches expected length for "BitTorrent protocol"
        if pstr_length as usize != PSTR.len() {
            return Err(P2pError::DeserializationError("pstr length mismatch"));
        }

        // Parse `pstr` (19 bytes)
        let pstr = std::str::from_utf8(&buffer[offset..offset + pstr_length as usize])
            .map_err(|_| P2pError::DeserializationError("Invalid UTF-8 in pstr"))?
            .to_string();

        offset += pstr_length as usize;

        // Parse `reserved` (8 bytes)
        let mut reserved = [0u8; 8];
        reserved.copy_from_slice(&buffer[offset..offset + 8]);
        offset += 8;

        // Parse `info_hash` (20 bytes)
        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&buffer[offset..offset + 20]);
        offset += 20;

        // Parse `peer_id` (20 bytes)
        let peer_id_len = std::cmp::min(buffer.len() - offset, 20);
        let mut peer_id = [0u8; 20];
        // peer_id.copy_from_slice(&buffer[offset..]);
        peer_id[..peer_id_len].copy_from_slice(&buffer[offset..offset + peer_id_len]);

        Ok(Handshake {
            pstr,
            reserved,
            info_hash,
            peer_id,
        })
    }
}

pub async fn handshake(
    stream: &mut TcpStream,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
) -> Result<Vec<u8>, P2pError> {
    let handshake = Handshake::new(info_hash, peer_id);
    stream.write_all(&handshake.serialize()).await?;

    let mut buffer = vec![0u8; 68];
    stream.readable().await?;

    let mut bytes_read = 0;
    // Loop until we've read exactly 68 bytes
    while bytes_read < 68 {
        match stream.try_read(&mut buffer[bytes_read..]) {
            Ok(0) => break, // Connection closed
            Ok(n) => bytes_read += n,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                stream.readable().await?;
            }
            Err(e) => return Err(e.into()),
        }
    }

    if bytes_read != 68 {
        return Err(P2pError::InvalidHandshake);
    }

    Ok(buffer)
}

#[derive(Debug)]
pub enum P2pError {
    EmptyPayload,
    InvalidHandshake,
    DeserializationError(&'static str),
    PieceMissingBlocks(PieceError),
    PieceOutOfBounds(PieceError),
    PieceNotFound,
    PieceInvalid,
    DiskTxError(mpsc::error::SendError<(Piece, Vec<u8>)>),
    IoTxError(mpsc::error::SendError<Message>),
    ClientTxError(mpsc::error::SendError<Bitfield>),
    ShutdownError(broadcast::error::SendError<()>),
    IoError(io::Error),
}

impl Display for P2pError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            P2pError::EmptyPayload => write!(f, "Empty payload"),
            P2pError::InvalidHandshake => write!(f, "Handshake response was not 68 bytes"),
            P2pError::DeserializationError(err) => write!(f, "Deserialization error: {}", err),
            P2pError::PieceNotFound => write!(f, "Piece not found in map"),
            P2pError::PieceInvalid => write!(f, "Invalid piece, hash mismatch"),
            P2pError::PieceMissingBlocks(err) => write!(f, "{}", err),
            P2pError::PieceOutOfBounds(err) => write!(f, "{}", err),
            P2pError::DiskTxError(err) => write!(f, "Disk tx error: {}", err),
            P2pError::IoTxError(err) => write!(f, "IO tx error: {}", err),
            P2pError::ClientTxError(err) => write!(f, "Client tx error: {}", err),
            P2pError::IoError(err) => write!(f, "IO error: {}", err),
            P2pError::ShutdownError(err) => write!(f, "Shutdown tx error: {}", err),
        }
    }
}

impl From<&'static str> for P2pError {
    fn from(err: &'static str) -> Self {
        P2pError::DeserializationError(err)
    }
}

impl From<PieceError> for P2pError {
    fn from(err: PieceError) -> Self {
        match err {
            PieceError::MissingBlocks => P2pError::PieceMissingBlocks(err),
            PieceError::OutOfBounds => P2pError::PieceOutOfBounds(err),
        }
    }
}

impl From<mpsc::error::SendError<(Piece, Vec<u8>)>> for P2pError {
    fn from(err: mpsc::error::SendError<(Piece, Vec<u8>)>) -> Self {
        P2pError::DiskTxError(err)
    }
}

impl From<mpsc::error::SendError<Message>> for P2pError {
    fn from(err: mpsc::error::SendError<Message>) -> Self {
        P2pError::IoTxError(err)
    }
}

impl From<mpsc::error::SendError<Bitfield>> for P2pError {
    fn from(err: mpsc::error::SendError<Bitfield>) -> Self {
        P2pError::ClientTxError(err)
    }
}

impl From<broadcast::error::SendError<()>> for P2pError {
    fn from(err: broadcast::error::SendError<()>) -> Self {
        P2pError::ShutdownError(err)
    }
}

impl From<io::Error> for P2pError {
    fn from(err: io::Error) -> Self {
        P2pError::IoError(err)
    }
}

impl Error for P2pError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            P2pError::PieceMissingBlocks(err) => Some(err),
            P2pError::PieceOutOfBounds(err) => Some(err),
            P2pError::DiskTxError(err) => Some(err),
            P2pError::IoTxError(err) => Some(err),
            P2pError::ClientTxError(err) => Some(err),
            P2pError::IoError(err) => Some(err),
            P2pError::ShutdownError(err) => Some(err),
            _ => None,
        }
    }
}

fn client_version() -> String {
    let version_tag = env!("CARGO_PKG_VERSION").replace(".", "");
    let version = version_tag
        .chars()
        .filter(|c| c.is_digit(10))
        .take(4)
        .collect::<String>();

    // Ensure exactly 4 characters, padding with "0" if necessary
    format!("{:0<4}", version)
}

pub fn generate_peer_id() -> [u8; 20] {
    let version = client_version();
    let client_id = "JM";

    // Generate a 12-character random alphanumeric sequence
    let random_seq: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(12)
        .map(char::from)
        .collect();

    let format = format!("-{}{}-{}", client_id, version, random_seq);

    let mut id = [0u8; 20];
    id.copy_from_slice(format.as_bytes());

    id
}

fn is_valid_piece(piece_hash: [u8; 20], piece_bytes: &[u8]) -> bool {
    let mut hasher = Sha1::new();
    hasher.update(&piece_bytes);
    let hash = hasher.finalize();

    piece_hash == hash.as_slice()
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use crate::{
        bitfield::Bitfield,
        download_state::DownloadState,
        p2p::{
            connection::{client_version, generate_peer_id, Actor, P2pError},
            message::{Message, MessageId, PiecePayload},
            piece::Piece,
        },
    };
    use assert_matches::assert_matches;
    use tokio::{
        sync::{broadcast, mpsc},
        time::timeout,
    };

    #[tokio::test]
    async fn test_actor_ready_to_request() {
        let (client_tx, _) = mpsc::channel(10);
        let (io_wtx, _) = mpsc::channel(10);
        let (disk_tx, _) = mpsc::channel(10);
        let (shutdown_tx, _) = broadcast::channel::<()>(1); // Shutdown signal

        let pieces_map = HashMap::new();
        let client_tx = Arc::new(client_tx);
        let disk_tx = Arc::new(disk_tx);
        let pieces_state = Arc::new(DownloadState::new(pieces_map));

        let mut actor = Actor::new(client_tx, io_wtx, disk_tx, shutdown_tx, pieces_state);

        // Test when actor is choked
        assert!(!actor.ready_to_request().await);

        // Modify state to make it ready
        actor.state.is_choked = false;
        actor.state.is_interested = true;
        actor
            .download_state
            .pieces_queue
            .queue
            .lock()
            .await
            .push(Piece::new(1, 512, [0u8; 20]));
        assert!(actor.ready_to_request().await);
    }

    #[tokio::test]
    async fn test_handle_message_choke() {
        let (client_tx, _) = mpsc::channel(10);
        let (io_wtx, _) = mpsc::channel(10);
        let (disk_tx, _) = mpsc::channel(10);
        let (shutdown_tx, _) = broadcast::channel::<()>(1); // Shutdown signal

        let pieces_map = HashMap::new();
        let client_tx = Arc::new(client_tx);
        let disk_tx = Arc::new(disk_tx);
        let pieces_state = Arc::new(DownloadState::new(pieces_map));

        let mut actor = Actor::new(client_tx, io_wtx, disk_tx, shutdown_tx, pieces_state);
        actor
            .handle_message(Message::new(MessageId::Choke, None))
            .await
            .unwrap();
        assert!(actor.state.is_choked);
    }

    #[tokio::test]
    async fn test_handle_message_unchoke() {
        let (client_tx, _) = mpsc::channel(10);
        let (io_wtx, _) = mpsc::channel(10);
        let (disk_tx, _) = mpsc::channel(10);
        let (shutdown_tx, _) = broadcast::channel::<()>(1); // Shutdown signal

        let pieces_map = HashMap::new();
        let client_tx = Arc::new(client_tx);
        let disk_tx = Arc::new(disk_tx);
        let pieces_state = Arc::new(DownloadState::new(pieces_map));

        let mut actor = Actor::new(client_tx, io_wtx, disk_tx, shutdown_tx, pieces_state);
        actor
            .handle_message(Message::new(MessageId::Unchoke, None))
            .await
            .unwrap();
        assert!(!actor.state.is_choked);
    }

    #[tokio::test]
    async fn test_handle_message_not_interested() {
        let (client_tx, _) = mpsc::channel(10);
        let (io_wtx, _) = mpsc::channel(10);
        let (disk_tx, _) = mpsc::channel(10);
        let (shutdown_tx, _) = broadcast::channel::<()>(1); // Shutdown signal

        let pieces_map = HashMap::new();
        let client_tx = Arc::new(client_tx);
        let disk_tx = Arc::new(disk_tx);
        let pieces_state = Arc::new(DownloadState::new(pieces_map));

        let mut actor = Actor::new(client_tx, io_wtx, disk_tx, shutdown_tx, pieces_state);
        actor
            .handle_message(Message::new(MessageId::NotInterested, None))
            .await
            .unwrap();
        assert!(!actor.state.peer_interested);
    }

    #[tokio::test]
    async fn test_handle_message_bitfield() {
        let (client_tx, mut client_rx) = mpsc::channel(10);
        let (io_wtx, mut io_rx) = mpsc::channel(10);
        let (disk_tx, _) = mpsc::channel(10);
        let (shutdown_tx, _) = broadcast::channel::<()>(1); // Shutdown signal

        let pieces_map = HashMap::new();
        let client_tx = Arc::new(client_tx);
        let disk_tx = Arc::new(disk_tx);
        let pieces_state = Arc::new(DownloadState::new(pieces_map));

        let mut actor = Actor::new(client_tx, io_wtx, disk_tx, shutdown_tx, pieces_state);

        let bitfield_payload = vec![0b10101010];
        actor
            .handle_message(Message::new(
                MessageId::Bitfield,
                Some(bitfield_payload.clone()),
            ))
            .await
            .unwrap();

        // Verify the peer bitfield was updated
        assert_eq!(actor.state.peer_bitfield.bytes, bitfield_payload);

        // Verify messages were sent
        assert_matches!(io_rx.recv().await, Some(msg) if msg.message_id == MessageId::Unchoke);
        assert_matches!(io_rx.recv().await, Some(msg) if msg.message_id == MessageId::Interested);

        // Verify client received updated bitfield
        assert_matches!(client_rx.recv().await, Some(bitfield) if bitfield.bytes == bitfield_payload);
    }

    #[tokio::test]
    async fn test_handle_piece_message_valid_piece() {
        let (client_tx, mut client_rx) = mpsc::channel(10);
        let (io_wtx, mut io_rx) = mpsc::channel(10);
        let (disk_tx, mut disk_rx) = mpsc::channel(10);
        let (shutdown_tx, _) = broadcast::channel::<()>(1); // Shutdown signal

        let pieces_map = HashMap::new();
        let client_tx = Arc::new(client_tx);
        let disk_tx = Arc::new(disk_tx);
        let pieces_state = Arc::new(DownloadState::new(pieces_map));

        let mut actor = Actor::new(client_tx, io_wtx, disk_tx, shutdown_tx, pieces_state);

        // Set up a valid piece (index 1, size 32KB, with a dummy hash)
        let piece_index = 1;
        let piece_size = 32 * 1024; // 32 KB
        let piece_hash = [
            169, 175, 32, 2, 79, 197, 5, 67, 22, 59, 107, 230, 111, 228, 102, 11, 226, 23, 15, 108,
        ];
        let piece = Piece::new(piece_index, piece_size, piece_hash);

        // Simulate downloading blocks and adding them to the piece
        let block_size = 16384; // 16 KB blocks
        let block_0 = vec![0u8; block_size];
        let block_1 = vec![1u8; block_size];

        // Add the piece to the actor's state
        actor
            .state
            .pieces_status
            .insert(piece_index as usize, piece);

        // downloads the first block from piece 1 but should not assemble since not all blocks from piece 1 are downloaded
        let payload = PiecePayload::new(1, 0, block_0.clone()).serialize();
        actor
            .handle_message(Message::new(MessageId::Piece, Some(payload)))
            .await
            .unwrap();

        let piece = actor
            .state
            .pieces_status
            .get_mut(&(piece_index as usize))
            .unwrap();
        assert!(!piece.is_ready());
        assert!(!piece.is_finalized());

        // all blocks are downloaded, it should assemble the piece and send it to disk_tx
        let payload = PiecePayload::new(1, 16384, block_1.clone()).serialize();
        actor
            .handle_message(Message::new(MessageId::Piece, Some(payload)))
            .await
            .unwrap();

        // Verify that the disk_tx received the piece
        if let Some((received_piece, assembled_blocks)) = disk_rx.recv().await {
            assert_eq!(received_piece.index(), piece_index);
            assert_eq!(assembled_blocks, vec![block_0, block_1].concat());
        } else {
            panic!("Expected disk_tx to receive the finalized piece, but none was received");
        }

        // Verify that the piece has been marked as finalized
        let piece = actor
            .state
            .pieces_status
            .get_mut(&(piece_index as usize))
            .unwrap();
        assert!(piece.is_ready());
        assert!(piece.is_finalized());
    }

    #[tokio::test]
    async fn test_download_valid_piece() {
        let (client_tx, mut client_rx) = mpsc::channel(10);
        let (io_wtx, mut io_rx) = mpsc::channel(10);
        let (disk_tx, mut disk_rx) = mpsc::channel(10);
        let (shutdown_tx, _) = broadcast::channel::<()>(1); // Shutdown signal

        let pieces_map = HashMap::new();
        let client_tx = Arc::new(client_tx);
        let disk_tx = Arc::new(disk_tx);
        let pieces_state = Arc::new(DownloadState::new(pieces_map));

        let mut actor = Actor::new(client_tx, io_wtx, disk_tx, shutdown_tx, pieces_state);

        // Set up a valid piece (index 1, size 32KB, with a dummy hash)
        let piece_index = 1;
        let piece_size = 32 * 1024; // 32 KB
        let piece_hash = [
            169, 175, 32, 2, 79, 197, 5, 67, 22, 59, 107, 230, 111, 228, 102, 11, 226, 23, 15, 108,
        ];
        let piece = Piece::new(piece_index, piece_size, piece_hash);

        // Simulate downloading blocks and adding them to the piece
        let block_size = 16384; // 16 KB blocks
        let block_0 = vec![0u8; block_size];
        let block_1 = vec![1u8; block_size];

        // Add the piece to the actor's state
        actor
            .state
            .pieces_status
            .insert(piece_index as usize, piece);

        let payload = PiecePayload::new(piece_index, 0, block_0.clone()).serialize();
        actor.download(Some(&payload)).await.unwrap();

        let piece = actor
            .state
            .pieces_status
            .get_mut(&(piece_index as usize))
            .unwrap();

        assert_eq!(piece.blocks[0], block_0.clone());
        assert!(!piece.is_ready());
        assert!(!piece.is_finalized());

        let payload = PiecePayload::new(piece_index, 16384, block_1.clone()).serialize();
        actor.download(Some(&payload)).await.unwrap();

        let piece = actor
            .state
            .pieces_status
            .get_mut(&(piece_index as usize))
            .unwrap();

        assert_eq!(piece.blocks[0], block_0.clone());
        assert_eq!(piece.blocks[1], block_1.clone());
        assert!(piece.is_ready());
        assert!(piece.is_finalized());

        // Verify that the disk_tx received the piece
        if let Some((received_piece, assembled_blocks)) = disk_rx.recv().await {
            assert_eq!(received_piece.index(), piece_index);
            assert_eq!(assembled_blocks, vec![block_0, block_1].concat());
        } else {
            panic!("Expected disk_tx to receive the finalized piece, but none was received");
        }
    }

    #[tokio::test]
    async fn test_handle_piece_message_invalid_piece() {
        let (client_tx, mut client_rx) = mpsc::channel(10);
        let (io_wtx, mut io_rx) = mpsc::channel(10);
        let (disk_tx, mut disk_rx) = mpsc::channel(10);
        let (shutdown_tx, _) = broadcast::channel::<()>(1); // Shutdown signal

        let pieces_map = HashMap::new();
        let client_tx = Arc::new(client_tx);
        let disk_tx = Arc::new(disk_tx);
        let pieces_state = Arc::new(DownloadState::new(pieces_map));

        let mut actor = Actor::new(client_tx, io_wtx, disk_tx, shutdown_tx, pieces_state);

        // Set up a valid piece (index 1, size 32KB, with a dummy hash)
        let piece_index = 1;
        let piece_size = 32 * 1024; // 32 KB
        let piece_hash = [
            169, 175, 32, 2, 79, 197, 5, 67, 22, 59, 107, 230, 111, 228, 102, 11, 226, 23, 15, 108,
        ];
        let piece = Piece::new(piece_index, piece_size, piece_hash);

        // Simulate downloading blocks and adding them to the piece
        let block_size = 16384; // 16 KB blocks
                                // the hash from these blocks does not match with the piece hash
        let block_0 = vec![1u8; block_size];
        let block_1 = vec![2u8; block_size];

        // Add the piece to the actor's state
        actor
            .state
            .pieces_status
            .insert(piece_index as usize, piece);

        // downloads the first block from piece 1 but should not assemble since not all blocks from piece 1 are downloaded
        let payload = PiecePayload::new(1, 0, block_0.clone()).serialize();
        actor
            .handle_message(Message::new(MessageId::Piece, Some(payload)))
            .await
            .unwrap();

        let piece = actor
            .state
            .pieces_status
            .get_mut(&(piece_index as usize))
            .unwrap();
        assert!(!piece.is_ready());
        assert!(!piece.is_finalized());

        // all blocks are downloaded, it should assemble the piece and send it to disk_tx
        let payload = PiecePayload::new(1, 16384, block_1.clone()).serialize();
        let result = actor
            .handle_message(Message::new(MessageId::Piece, Some(payload)))
            .await;

        assert_matches!(result, Err(P2pError::PieceInvalid));
    }

    #[tokio::test]
    async fn test_handle_message_have() {
        // Prepare test data
        let piece_index = 5;
        let mut piece_index_be = [0u8; 4];
        piece_index_be.copy_from_slice(&(piece_index as u32).to_be_bytes());

        let message = Message::new(MessageId::Have, Some(piece_index_be.to_vec()));

        // Create an empty Bitfield
        let initial_bitfield = Bitfield::new(&vec![0; 10]);

        // Create a PiecesState
        let pieces_map = HashMap::new();
        let pieces_state = Arc::new(DownloadState::new(pieces_map));

        // Set up mpsc channels
        let (client_tx, mut client_rx) = mpsc::channel(10);
        let (disk_tx, _) = mpsc::channel(10);
        let (io_wtx, _) = mpsc::channel(10);
        let (shutdown_tx, _) = broadcast::channel::<()>(1); // Shutdown signal

        let client_tx = Arc::new(client_tx);
        let disk_tx = Arc::new(disk_tx);

        // Initialize the Actor
        let mut actor = Actor::new(client_tx, io_wtx, disk_tx, shutdown_tx, pieces_state);
        actor.state.peer_bitfield = initial_bitfield.clone();

        // Handle the MessageId::Have message
        actor.handle_message(message).await.unwrap();

        // Assert the peer_bitfield is updated
        assert!(actor.state.peer_bitfield.has_piece(piece_index));
    }

    #[tokio::test]
    async fn test_request() {
        // Create a mock io_wtx sender and receiver
        let (io_wtx, mut io_wrx) = mpsc::channel::<Message>(10);
        let (shutdown_tx, _) = broadcast::channel::<()>(1); // Shutdown signal

        // Create a dummy Piece object
        let piece_index = 0;
        let piece_size = 32768; // 2 blocks of 16KB each
        let piece_hash = [0u8; 20];
        let piece = Piece::new(piece_index, piece_size, piece_hash);

        // Initialize the Actor's state with a queued piece
        let client_tx = mpsc::channel(10).0;
        let disk_tx = mpsc::channel(10).0;
        let pieces_map = HashMap::new();
        let client_tx = Arc::new(client_tx);
        let disk_tx = Arc::new(disk_tx);
        let pieces_state = Arc::new(DownloadState::new(pieces_map));
        let mut actor = Actor::new(client_tx, io_wtx, disk_tx, shutdown_tx, pieces_state);

        // Pre-fill the pieces queue with the dummy piece
        actor.state.download_queue.push(piece);

        // Call the request method
        actor.request().await.expect("request method failed");

        // Collect messages sent to the io_wtx channel
        let mut sent_messages = Vec::new();
        while let Ok(Some(message)) = timeout(Duration::from_millis(100), io_wrx.recv()).await {
            sent_messages.push(message);
        }

        // Check the number of requests made
        assert_eq!(sent_messages.len(), 2); // 2 blocks requested (16KB each)

        // Validate the contents of each message
        for (i, message) in sent_messages.iter().enumerate() {
            assert_eq!(message.message_id, MessageId::Request);

            // Interpret the payload manually
            let payload = message.payload.as_ref().expect("Payload missing");
            let piece_index_field = u32::from_be_bytes(payload[0..4].try_into().unwrap());
            let block_offset_field = u32::from_be_bytes(payload[4..8].try_into().unwrap());
            let block_length_field = u32::from_be_bytes(payload[8..12].try_into().unwrap());

            assert_eq!(piece_index_field, piece_index);
            assert_eq!(block_offset_field, (i * 16384) as u32); // Block offsets
            assert_eq!(block_length_field, 16384); // Block size (16KB)
        }
    }

    #[tokio::test]
    async fn test_request_from_offset() {
        // Create a mock io_wtx sender and receiver
        let (io_wtx, mut io_wrx) = mpsc::channel::<Message>(10);
        let (shutdown_tx, _) = broadcast::channel::<()>(1); // Shutdown signal

        // Create a dummy Piece object
        let piece_index = 0;
        let piece_size = 32768; // 2 blocks of 16KB each
        let piece_hash = [0u8; 20];
        let piece = Piece::new(piece_index, piece_size, piece_hash);

        // Initialize the Actor's state
        let client_tx = mpsc::channel(10).0;
        let disk_tx = mpsc::channel(10).0;
        let pieces_map = HashMap::new();
        let client_tx = Arc::new(client_tx);
        let disk_tx = Arc::new(disk_tx);
        let pieces_state = Arc::new(DownloadState::new(pieces_map));
        let mut actor = Actor::new(client_tx, io_wtx, disk_tx, shutdown_tx, pieces_state);

        // Request blocks starting from offset 0
        let start_offset = 0;
        actor
            .request_from_offset(&piece, start_offset)
            .await
            .expect("request_from_offset failed");

        // Collect messages sent to the io_wtx channel
        let mut sent_messages = Vec::new();
        while let Ok(Some(message)) = timeout(Duration::from_millis(100), io_wrx.recv()).await {
            sent_messages.push(message);
        }

        // Check the number of requests made
        assert_eq!(sent_messages.len(), 2); // 2 blocks requested

        // Validate the contents of each message
        for (i, message) in sent_messages.iter().enumerate() {
            assert_eq!(message.message_id, MessageId::Request);
            // Interpret the payload manually
            let payload = message.payload.as_ref().expect("Payload missing");
            let piece_index_field = u32::from_be_bytes(payload[0..4].try_into().unwrap());
            let block_offset_field = u32::from_be_bytes(payload[4..8].try_into().unwrap());
            let block_length_field = u32::from_be_bytes(payload[8..12].try_into().unwrap());

            assert_eq!(piece_index_field, piece_index);
            assert_eq!(block_offset_field, (i * 16384) as u32); // Block offsets
            assert_eq!(block_length_field, 16384); // Block size (16KB)
        }
    }

    #[test]
    fn test_generate_peer_id() {
        let version = client_version();
        let expected = format!("-JM{}-", version);

        assert_eq!(&generate_peer_id()[..8], expected.as_bytes());
    }
}
