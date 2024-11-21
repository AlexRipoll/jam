use core::panic;
use rand::{distributions::Alphanumeric, Rng};
use sha1::{Digest, Sha1};
use std::error::Error;
use std::fmt::Display;
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::{collections::HashMap, usize};
use tokio::{
    io::{self, AsyncWriteExt},
    net::TcpStream,
};

use crate::p2p::message::{Bitfield, Message, MessageId, PiecePayload, TransferPayload};

use super::piece::{Piece, PieceError};
use super::pipeline::{self, Pipeline};

const PSTR: &str = "BitTorrent protocol";

#[derive(Debug)]
pub struct Client {
    stream: TcpStream,
    sender: mpsc::Sender<(Piece, Vec<u8>)>,
    is_choked: RwLock<bool>,
    is_interested: RwLock<bool>,
    peer_choked: RwLock<bool>,
    peer_interested: RwLock<bool>,
    peer_bitfield: RwLock<Bitfield>,
    pieces_status: RwLock<HashMap<usize, Piece>>,
    pieces_queue: RwLock<Vec<Piece>>,
    pipeline: Mutex<Pipeline>,
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

impl Client {
    pub fn new(stream: TcpStream, sender: mpsc::Sender<(Piece, Vec<u8>)>) -> Self {
        Self {
            stream,
            sender,
            is_choked: RwLock::new(true),
            is_interested: RwLock::new(false),
            peer_choked: RwLock::new(true),
            peer_interested: RwLock::new(false),
            peer_bitfield: RwLock::new(Bitfield::new(&vec![])),
            pieces_status: RwLock::new(HashMap::new()),
            pieces_queue: RwLock::new(vec![]),
            pipeline: Mutex::new(Pipeline::new(5)),
        }
    }

    pub async fn handshake(
        &mut self,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
    ) -> Result<Vec<u8>, P2pError> {
        let handshake = Handshake::new(info_hash, peer_id);
        self.stream.write_all(&handshake.serialize()).await?;

        let mut buffer = vec![0u8; 68];
        self.stream.readable().await?;

        let mut bytes_read = 0;
        // Loop until we've read exactly 68 bytes
        while bytes_read < 68 {
            match self.stream.try_read(&mut buffer[bytes_read..]) {
                Ok(0) => break, // Connection closed
                Ok(n) => bytes_read += n,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.stream.readable().await?;
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

    pub async fn send_message(
        &mut self,
        message_id: MessageId,
        payload: Option<Vec<u8>>,
    ) -> Result<(), P2pError> {
        let message = Message::new(message_id, payload);
        self.stream.write_all(&message.serialize()).await?;

        Ok(())
    }

    pub async fn read_message(&mut self) -> Result<Message, P2pError> {
        self.stream.readable().await?;

        //  Read the 4-byte length field
        let mut length_buffer = [0u8; 4];
        let mut bytes_read = 0;
        while bytes_read < 4 {
            match self.stream.try_read(&mut length_buffer[bytes_read..]) {
                Ok(0) => {
                    break;
                } // Connection closed
                Ok(n) => bytes_read += n,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.stream.readable().await?;
                }
                Err(e) => return Err(e.into()),
            }
        }

        // Convert the length bytes from Big-Endian to usize
        let message_length = u32::from_be_bytes(length_buffer) as usize;

        //  Allocate a buffer for the message based on the length
        let mut message_buffer = vec![0u8; 4 + message_length];
        message_buffer[..4].copy_from_slice(&length_buffer);

        bytes_read = 0;

        //  Read the actual message data into the buffer
        while bytes_read < message_length {
            match self.stream.try_read(&mut message_buffer[4 + bytes_read..]) {
                Ok(0) => {
                    break;
                } // Connection closed
                Ok(n) => bytes_read += n,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.stream.readable().await?;
                }
                Err(e) => return Err(e.into()),
            }
        }

        let message = Message::deserialize(&message_buffer)?;

        self.process_message(&message).await?;

        Ok(message)
    }

    pub async fn process_message(&mut self, message: &Message) -> Result<(), P2pError> {
        match message.message_id {
            MessageId::KeepAlive => {} // TODO: extend stream alive
            MessageId::Choke => {
                // TODO:  stop sending msg until unchoked
                let mut is_choked = self.is_choked.write().unwrap();
                *is_choked = true;
                println!("Client is now choked by the peer.");
            }
            MessageId::Unchoke => {
                let mut is_choked = self.is_choked.write().unwrap();
                *is_choked = false; // Update `is_choked`
                println!("Client is now unchoked by the peer.");
                // TODO:start requesting
            }
            MessageId::Interested => {
                {
                    let mut peer_interested = self.peer_interested.write().unwrap();
                    *peer_interested = true;
                }

                // TODO: logic for unchoking peer
                let mut peer_choked = self.peer_choked.write().unwrap();
                if *peer_choked {
                    *peer_choked = false;
                    drop(peer_choked); // Explicitly drop lock to avoid conflict
                    self.send_message(MessageId::Unchoke, None).await?; // Notify unchoked
                }
            }
            MessageId::NotInterested => {
                let mut peer_interested = self.peer_interested.write().unwrap();
                *peer_interested = false;
            }
            MessageId::Have => self.process_have(message),
            MessageId::Bitfield => {
                // Update the peer's bitfield and determine if the client is interested
                {
                    let mut peer_bitfield = self.peer_bitfield.write().unwrap();
                    *peer_bitfield = Bitfield::new(message.payload.as_ref().unwrap());
                }

                // TODO: send bitfield to clients orchestrator.
                // TODO: check if peer has any piece the client is interested in downloading then
                // request to unchoke and notify interest
                self.send_message(MessageId::Unchoke, None).await?;
                self.send_message(MessageId::Interested, None).await?;
                let mut is_interested = self.is_interested.write().unwrap();
                *is_interested = true;
            }
            MessageId::Request => {
                // send piece blocks to peer
                todo!()
            }
            MessageId::Piece => {
                // store piece block
                self.download(message.payload.as_deref())?;
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

    pub async fn request(&mut self) -> Result<(), P2pError> {
        let (queue_index, block_offset) = {
            let pipeline = self.pipeline.lock().unwrap();
            match pipeline.last_added.clone() {
                Some(piece_data) => {
                    let pieces_queue = self.pieces_queue.read().unwrap();
                    if let Some(index) = pieces_queue
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

        let queue_length = self.pieces_queue.read().unwrap().len();
        for i in queue_index..queue_length {
            let pieces_queue = self.pieces_queue.read().unwrap();
            let piece = pieces_queue[i].clone();
            drop(pieces_queue);
            self.request_from_offset(&piece, block_offset).await?;
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
            let mut pipeline = self.pipeline.lock().unwrap();
            pipeline.add_request(piece.index(), block_offset);
            drop(pipeline);

            let payload = TransferPayload::new(piece.index(), block_offset, block_size).serialize();
            self.send_message(MessageId::Request, Some(payload)).await?;

            // FIX: bad design
            let pipeline = self.pipeline.lock().unwrap();
            // Stop if the pipeline is full
            if pipeline.is_full() {
                break;
            }
        }

        Ok(())
    }

    fn download(&mut self, payload: Option<&[u8]>) -> Result<(), P2pError> {
        let bytes = payload.ok_or(P2pError::EmptyPayload)?;
        let payload = PiecePayload::deserialize(bytes)?;

        let mut pieces_status = self.pieces_status.write().unwrap();
        let piece = pieces_status
            .get_mut(&(payload.index as usize))
            .ok_or(P2pError::PieceNotFound)?;

        // Add the block to the piece
        piece.add_block(payload.begin, payload.block.clone())?;
        let mut pipeline = self.pipeline.lock().unwrap();
        pipeline.remove_request(payload.index, payload.begin);

        // Check if the piece is ready and not yet completed
        if piece.is_ready() && !piece.is_finalized() {
            let buffer = piece.assemble()?;

            // Validate the piece hash
            if is_valid_piece(piece.hash(), &buffer) {
                self.sender.send((piece.clone(), buffer))?;
                // NOTE: consider removing from pieces hashmap instead
                piece.mark_finalized();

                // remove the finalized piece from the queue
                let mut pieces_queue = self.pieces_queue.write().unwrap();
                pieces_queue.retain(|piece| piece.index() != piece.index());
            } else {
                return Err(P2pError::PieceInvalid);
            }
        }

        Ok(())
    }

    fn process_have(&mut self, message: &Message) {
        let mut piece_index_be = [0u8; 4];
        piece_index_be.copy_from_slice(message.payload.as_ref().unwrap());
        let piece_index = u32::from_be_bytes(piece_index_be) as usize;
        let mut peer_bitfield = self.peer_bitfield.write().unwrap();
        if !peer_bitfield.has_piece(piece_index) {
            peer_bitfield.set_piece(piece_index);
        }
        // TODO: check if interested
    }
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
    SenderError(mpsc::SendError<(Piece, Vec<u8>)>),
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
            P2pError::SenderError(err) => write!(f, "Sender error: {}", err),
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

impl From<mpsc::SendError<(Piece, Vec<u8>)>> for P2pError {
    fn from(err: mpsc::SendError<(Piece, Vec<u8>)>) -> Self {
        P2pError::SenderError(err)
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
            P2pError::SenderError(err) => Some(err),
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
