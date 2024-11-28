use rand::{distributions::Alphanumeric, Rng};
use sha1::{Digest, Sha1};
use std::error::Error;
use std::fmt::Display;
use std::sync::Arc;
use std::{collections::HashMap, usize};
use tokio::io::{self, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};

use crate::client::PiecesState;
use crate::p2p::message::{Bitfield, Message, MessageId, PiecePayload, TransferPayload};

use super::piece::{Piece, PieceError};
use super::pipeline::{self, Pipeline};

const PSTR: &str = "BitTorrent protocol";

#[derive(Debug)]
pub struct Actor {
    client_tx: mpsc::Sender<Bitfield>,
    disk_tx: mpsc::Sender<(Piece, Vec<u8>)>,
    io_wtx: mpsc::Sender<Message>,
    state: State,
    pieces_state: Arc<PiecesState>,
}

#[derive(Debug)]
struct State {
    is_choked: bool,
    is_interested: bool,
    peer_choked: bool,
    peer_interested: bool,
    peer_bitfield: Bitfield,
    pieces_status: HashMap<usize, Piece>,
    pieces_queue: Vec<Piece>,
    pipeline: Pipeline,
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
            pieces_queue: vec![],
            pipeline: Pipeline::new(5),
        }
    }
}

impl Actor {
    pub fn new(
        client_tx: mpsc::Sender<Bitfield>,
        io_wtx: mpsc::Sender<Message>,
        disk_tx: mpsc::Sender<(Piece, Vec<u8>)>,
        pieces_state: Arc<PiecesState>,
    ) -> Self {
        Self {
            client_tx,
            disk_tx,
            io_wtx,
            state: State::default(),
            pieces_state,
        }
    }

    pub async fn ready_to_request(&self) -> bool {
        !self.state.is_choked
            && self.state.is_interested
            && !self.pieces_state.remaining.lock().await.is_empty()
    }

    pub async fn handle_message(&mut self, message: Message) -> Result<(), P2pError> {
        // modifies state
        match message.message_id {
            MessageId::KeepAlive => {} // TODO: extend stream alive
            MessageId::Choke => {
                // TODO:  stop sending msg until unchoked
                self.state.is_choked = true;
                println!("Client choked by the peer.");
            }
            MessageId::Unchoke => {
                self.state.is_choked = false;
                println!("Client unchoked by the peer.");
                // TODO:start requesting
            }
            MessageId::Interested => {
                self.state.peer_interested = true;

                // TODO: logic for unchoking peer
                if self.state.peer_choked {
                    self.state.peer_choked = false;
                    self.io_wtx
                        .send(Message::new(MessageId::Unchoke, None))
                        .await?;
                }
            }
            MessageId::NotInterested => {
                self.state.peer_interested = false;
            }
            MessageId::Have => self.process_have(&message),
            MessageId::Bitfield => {
                // Update the peer's bitfield and determine if the client is interested

                let bitfield = Bitfield::new(message.payload.as_ref().unwrap());
                self.state.peer_bitfield = bitfield.clone();
                self.client_tx.send(bitfield).await?;

                // TODO: send bitfield to clients orchestrator.
                // TODO: check if peer has any piece the client is interested in downloading then
                self.io_wtx
                    .send(Message::new(MessageId::Unchoke, None))
                    .await?;
                self.io_wtx
                    .send(Message::new(MessageId::Interested, None))
                    .await?;
                self.state.is_interested = true;
            }
            MessageId::Request => {
                // send piece blocks to peer
                todo!()
            }
            MessageId::Piece => {
                // store piece block
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

        // Add the block to the piece
        piece.add_block(payload.begin, payload.block.clone())?;
        self.state
            .pipeline
            .remove_request(payload.index, payload.begin);

        // Check if the piece is ready and not yet completed
        if piece.is_ready() && !piece.is_finalized() {
            let buffer = piece.assemble()?;

            // Validate the piece hash
            if is_valid_piece(piece.hash(), &buffer) {
                self.disk_tx.send((piece.clone(), buffer)).await?;
                // NOTE: consider removing from pieces hashmap instead
                piece.mark_finalized();

                // remove the finalized piece from the queue
                self.state
                    .pieces_queue
                    .retain(|piece| piece.index() != piece.index());
            } else {
                return Err(P2pError::PieceInvalid);
            }
        }

        Ok(())
    }

    pub async fn fill_pieces_queue(&mut self) {
        let remaining = self.pieces_state.remaining.lock().await;
        for _ in 0..3 {
            if remaining.is_empty() || self.state.pieces_queue.len() >= 3 {
                break;
            }
            if let Some(piece) = self
                .pieces_state
                .assign_piece(&self.state.peer_bitfield)
                .await
            {
                self.state.pieces_queue.push(piece);
            }
        }
    }

    pub async fn request(&mut self) -> Result<(), P2pError> {
        if !self.state.pipeline.is_full() {
            let (queue_starting_index, block_offset) = {
                match self.state.pipeline.last_added.clone() {
                    Some(piece_data) => {
                        if let Some(index) = self
                            .state
                            .pieces_queue
                            .iter()
                            .position(|piece| piece.index() == piece_data.piece_index)
                        {
                            (index, piece_data.block_offset + 16384)
                        } else {
                            panic!("piece not found in queue");
                        }
                    }
                    None => (0, 0),
                }
            };

            if self.state.pieces_queue.is_empty() {
                self.fill_pieces_queue().await;
            }

            let queue_length = self.state.pieces_queue.len();
            for i in queue_starting_index..queue_length {
                let piece = self.state.pieces_queue[i].clone();
                self.request_from_offset(&piece, block_offset).await?;
            }
        }

        Ok(())
    }

    /// Core logic to request blocks from a piece starting at a given offset
    async fn request_from_offset(
        &mut self,
        piece: &Piece,
        start_offset: u32,
    ) -> Result<(), P2pError> {
        let total_blocks = (piece.size() + 16384 - 1) / 16384;

        for block_index in ((start_offset / 16384) as usize)..total_blocks {
            let block_offset = block_index as u32 * 16384;
            let block_size = 16384.min(piece.size() as u32 - block_offset);

            // Add to pipeline and send the request
            self.state.pipeline.add_request(piece.index(), block_offset);

            let payload = TransferPayload::new(piece.index(), block_offset, block_size).serialize();
            self.io_wtx
                .send(Message::new(MessageId::Request, Some(payload)))
                .await?;

            if self.state.pipeline.is_full() {
                break;
            }
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
        eprintln!("Warning: Incomplete handshake response received");
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
    use crate::p2p::client::{client_version, generate_peer_id};

    #[test]
    fn test_generate_peer_id() {
        let version = client_version();
        let expected = format!("-JM{}-", version);

        assert_eq!(&generate_peer_id()[..8], expected.as_bytes());
    }
}
