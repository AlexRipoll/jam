use rand::{distributions::Alphanumeric, Rng};
use sha1::{Digest, Sha1};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{collections::HashMap, usize};
use tokio::io::{self, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, trace, warn};

use crate::download::state::DownloadState;
use crate::error::error::JamError;

use super::piece::Piece;
use protocol::bitfield::Bitfield;
use protocol::message::{Message, MessageId, PiecePayload, TransferPayload};

const PSTR: &str = "BitTorrent protocol";
const MAX_STRIKES: u8 = 3;

#[derive(Debug)]
pub struct Actor {
    client_tx: Arc<mpsc::Sender<Bitfield>>,
    disk_tx: Arc<mpsc::Sender<(Piece, Vec<u8>)>>,
    io_wtx: mpsc::Sender<Message>,
    shutdown_tx: broadcast::Sender<()>,
    state: State,
    pub download_state: Arc<DownloadState>,
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
    unconfirmed: Vec<Piece>,
    unconfirmed_timestamps: HashMap<u32, Instant>,
    strikes: u8,
}

impl Default for State {
    fn default() -> Self {
        State {
            is_choked: true,
            is_interested: false,
            peer_choked: true,
            peer_interested: false,
            peer_bitfield: Bitfield::new(&[]),
            pieces_status: HashMap::new(),
            download_queue: vec![],
            unconfirmed: vec![],
            unconfirmed_timestamps: HashMap::new(),
            strikes: 0,
        }
    }
}

impl Actor {
    pub fn new(
        client_tx: Arc<mpsc::Sender<Bitfield>>,
        io_wtx: mpsc::Sender<Message>,
        disk_tx: Arc<mpsc::Sender<(Piece, Vec<u8>)>>,
        shutdown_tx: broadcast::Sender<()>,
        download_state: Arc<DownloadState>,
    ) -> Self {
        Self {
            client_tx,
            disk_tx,
            io_wtx,
            shutdown_tx,
            state: State::default(),
            download_state,
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
            && self.has_assignable_pieces().await
            && self.state.unconfirmed.len() < 5
    }

    pub fn peer_bitfield(&self) -> &Bitfield {
        &self.state.peer_bitfield
    }

    pub fn has_peer_bitfield(&self) -> bool {
        !self.state.peer_bitfield.bytes.is_empty()
    }

    pub fn has_pending_pieces(&self) -> bool {
        !self.state.unconfirmed.is_empty()
    }

    pub async fn release_timeout_downloads(&mut self) {
        let now = Instant::now();
        let timed_out_pieces: Vec<Piece> = self
            .state
            .unconfirmed
            .iter()
            .filter(|piece| {
                if let Some(&request_time) = self.state.unconfirmed_timestamps.get(&piece.index()) {
                    now.duration_since(request_time) > Duration::new(15, 0)
                } else {
                    false
                }
            })
            .cloned()
            .collect();

        for timedout_pice in timed_out_pieces.iter() {
            self.release_unconfirmed_piece(timedout_pice.clone()).await;
            self.state.strikes += 1;
        }
    }

    pub fn is_strikeout(&self) -> bool {
        self.state.strikes >= MAX_STRIKES
    }

    pub async fn release_unconfirmed_pieces(&mut self) {
        self.download_state
            .unassign_pieces(self.state.unconfirmed.clone())
            .await;
        self.state.unconfirmed.clear();
        self.state.unconfirmed_timestamps.clear();
    }

    pub async fn release_unconfirmed_piece(&mut self, piece: Piece) {
        self.state
            .unconfirmed
            .retain(|unconfirmed_piece| unconfirmed_piece.index() != piece.index());
        self.state.unconfirmed_timestamps.remove(&piece.index());
        self.download_state.unassign_piece(piece).await;
    }

    pub fn all_finalized(&self) -> bool {
        self.state
            .pieces_status
            .iter()
            .all(|(_, piece)| piece.is_finalized())
    }

    pub async fn handle_message(&mut self, message: Message) -> Result<(), JamError> {
        // modifies state
        match message.message_id {
            MessageId::KeepAlive => {
                trace!("Received KeepAlive message");
                // TODO: extend stream alive
            }
            MessageId::Choke => {
                self.state.is_choked = true;
                debug!("Choked by peer");
                // TODO: logic in case peer chokes but client is still interested
            }
            MessageId::Unchoke => {
                self.state.is_choked = false;
                debug!("Unchoked by peer");
            }
            MessageId::Interested => {
                self.state.peer_interested = true;
                trace!("Peer expressed insterest");

                // TODO: logic for unchoking peer
                if self.state.peer_choked {
                    self.state.peer_choked = false;
                    trace!("Peer unchoked");
                    self.io_wtx
                        .send(Message::new(MessageId::Unchoke, None))
                        .await?;
                }
            }
            MessageId::NotInterested => {
                self.state.peer_interested = false;
                trace!("Peer no longer insterested");
            }
            MessageId::Have => {
                trace!("'Have' message received from peer");
                self.process_have(&message);
            }
            MessageId::Bitfield => {
                // Update the peer's bitfield and determine if the client is interested
                trace!("Received 'Bitfield' message from peer");
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
                trace!("'Piece' message received from peer");
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

    async fn download(&mut self, payload: Option<&[u8]>) -> Result<(), JamError> {
        let bytes = payload.ok_or(JamError::EmptyPayload)?;
        let payload = PiecePayload::deserialize(bytes)?;

        let piece = self
            .state
            .pieces_status
            .get_mut(&(payload.index as usize))
            .ok_or(JamError::PieceNotFound)?;

        debug!(piece_index= ?payload.index, block_offset= ?payload.begin, "Downloading piece");
        // Add the block to the piece
        piece.add_block(payload.begin, payload.block.clone())?;
        self.state
            .unconfirmed_timestamps
            .insert(piece.index(), Instant::now());

        // Check if the piece is ready and not yet completed
        if piece.is_ready() && !piece.is_finalized() {
            let buffer = piece.assemble()?;
            debug!(piece_index= ?payload.index, "Piece ready. Verifying hash...");

            // Validate the piece hash
            if is_valid_piece(piece.hash(), &buffer) {
                debug!(piece_index= ?payload.index, "Piece hash verified. Sending to disk");
                self.disk_tx.send((piece.clone(), buffer)).await?;

                // remove the finalized piece from the unconfirmed list
                self.state
                    .unconfirmed
                    .retain(|p| p.index() != piece.index());
                debug!(
                    "Remove piece with index {} from unconfirmed: {:?}",
                    piece.index(),
                    self.state
                        .unconfirmed
                        .iter()
                        .map(|piece| piece.index())
                        .collect::<Vec<u32>>()
                );
                self.download_state
                    .pieces_rarity
                    .rarity_map
                    .lock()
                    .await
                    .remove(&piece.index());
                piece.mark_finalized();
            } else {
                warn!(piece_index= ?payload.index, "Piece hash verification failed");
                self.download_state.unassign_piece(piece.clone()).await;
                // remove the finalized piece from the unconfirmed list
                self.state
                    .unconfirmed
                    .retain(|p| p.index() != piece.index());
                return Err(JamError::PieceInvalid);
            }
        }

        Ok(())
    }

    pub async fn fill_download_queue(&mut self) {
        trace!("Filling download queue");
        for _ in 0..3 {
            if self.state.download_queue.len() >= 3 {
                trace!("Download queue max capacity reached");
                break;
            }

            if let Some(piece) = self
                .download_state
                .assign_piece(&self.state.peer_bitfield)
                .await
            {
                trace!(piece_index=?piece.index(),"Assigned piece to queue");
                self.state.download_queue.push(piece);
            } else {
                debug!("No more pieces available to assign");
                break;
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
            .all(|(&x, &y)| (x & y) == x)
    }

    pub async fn has_assignable_pieces(&self) -> bool {
        // Lock the current bitfield to get the downloaded pieces
        let client_bitfield = self.download_state.metadata.bitfield.lock().await;

        // Iterate over the peer's bitfield to identify missing pieces
        let missing_pieces: Vec<usize> = (0..self.state.peer_bitfield.num_pieces)
            .filter(|&index| {
                self.state.peer_bitfield.has_piece(index) && !client_bitfield.has_piece(index)
            })
            .collect();

        // Check if any missing piece is already assigned to another task
        let assigned_piece_indexes: Vec<usize> = self
            .download_state
            .pieces_queue
            .assigned_pieces
            .lock()
            .await
            .iter()
            .map(|piece| piece.index() as usize)
            .collect();

        missing_pieces
            .into_iter()
            .any(|piece_index| !assigned_piece_indexes.contains(&piece_index))
    }

    pub async fn request(&mut self) -> Result<(), JamError> {
        if self.state.download_queue.is_empty() {
            trace!("Download queue is empty; filling download queue");
            self.fill_download_queue().await;
            if self.state.download_queue.is_empty() {
                return Err(JamError::EndOfWork);
            }
        }

        {
            let indexes: Vec<u32> = self
                .download_state
                .pieces_queue
                .queue
                .lock()
                .await
                .iter()
                .map(|p| p.index())
                .collect();
            debug!("Remaining pieces in download queue: {:?}", indexes);
        }
        {
            let indexes = self.download_state.metadata.bitfield.lock().await;
            debug!("Bitfield++: {:?}", indexes.bytes);
        }
        let indexes: Vec<u32> = self
            .state
            .download_queue
            .iter()
            .map(|piece| piece.index())
            .collect();
        debug!("Download queue: {:?}", indexes);

        for piece in self.state.download_queue.clone().iter() {
            // let piece = self.state.download_queue[i].clone();
            self.state
                .pieces_status
                .entry(piece.index() as usize)
                .or_insert_with(|| piece.clone());

            self.request_from_offset(piece, 0).await?;
        }
        debug!("Piece requests completed");

        Ok(())
    }

    /// Core logic to request blocks from a piece starting at a given offset
    async fn request_from_offset(
        &mut self,
        piece: &Piece,
        start_offset: u32,
    ) -> Result<(), JamError> {
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
        self.state.unconfirmed.push(piece.clone());
        self.state
            .unconfirmed_timestamps
            .insert(piece.index(), Instant::now());
        debug!(
            "Add piece with index {} to unconfirmed list: {:?}",
            piece.index(),
            self.state
                .unconfirmed
                .iter()
                .map(|piece| piece.index())
                .collect::<Vec<u32>>()
        );

        // remove the piece from the download queue
        self.state
            .download_queue
            .retain(|p| p.index() != piece.index());
        debug!(
            "Remove piece with index {} from download queue: {:?}",
            piece.index(),
            self.state
                .download_queue
                .iter()
                .map(|piece| piece.index())
                .collect::<Vec<u32>>()
        );

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

    pub fn deserialize(buffer: Vec<u8>) -> Result<Handshake, JamError> {
        let mut offset = 0;

        // Parse `pstr_length` (1 byte)
        let pstr_length = buffer[offset];
        offset += 1;

        // Check if `pstr_length` matches expected length for "BitTorrent protocol"
        if pstr_length as usize != PSTR.len() {
            return Err(JamError::DeserializationError("pstr length mismatch"));
        }

        // Parse `pstr` (19 bytes)
        let pstr = std::str::from_utf8(&buffer[offset..offset + pstr_length as usize])
            .map_err(|_| JamError::DeserializationError("Invalid UTF-8 in pstr"))?
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

pub async fn perform_handshake(
    stream: &mut TcpStream,
    handshake_metadata: &Handshake,
) -> Result<Vec<u8>, JamError> {
    stream.write_all(&handshake_metadata.serialize()).await?;

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
        return Err(JamError::InvalidHandshake);
    }

    Ok(buffer)
}

fn client_version() -> String {
    let version_tag = env!("CARGO_PKG_VERSION").replace('.', "");
    let version = version_tag
        .chars()
        .filter(|c| c.is_ascii_digit())
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
    hasher.update(piece_bytes);
    let hash = hasher.finalize();

    piece_hash == hash.as_slice()
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use crate::{
        download::state::DownloadState,
        p2p::{
            message_handler::{client_version, generate_peer_id, Actor, JamError, State},
            piece::Piece,
        },
    };
    use assert_matches::assert_matches;
    use protocol::bitfield::Bitfield;
    use protocol::message::{Message, MessageId, PiecePayload};
    use tokio::{
        sync::{broadcast, mpsc},
        time::timeout,
    };

    #[tokio::test]
    async fn test_actor_ready_to_request() {
        let peer_bitfield = Bitfield::new(&[0b11000010, 0b10000000]);
        let mut pieces_map = HashMap::new();
        for i in 0..10 {
            pieces_map.insert(i, create_sample_piece(i, 16384));
        }

        let download_state = Arc::new(DownloadState::new(pieces_map));
        let mut actor = Actor {
            client_tx: Arc::new(tokio::sync::mpsc::channel(1).0),
            io_wtx: tokio::sync::mpsc::channel(1).0,
            disk_tx: Arc::new(tokio::sync::mpsc::channel(1).0),
            shutdown_tx: tokio::sync::broadcast::channel(1).0,
            state: State {
                is_choked: false,
                is_interested: false,
                peer_choked: false,
                peer_interested: false,
                peer_bitfield,
                pieces_status: HashMap::new(),
                download_queue: vec![],
                unconfirmed: vec![],
                unconfirmed_timestamps: HashMap::new(),
                strikes: 0,
            },
            download_state,
        };

        {
            // let download_state = actor.download_state;
            let mut download_state_bitfield = actor.download_state.metadata.bitfield.lock().await;
            // piece 1 is still not downloaded
            download_state_bitfield.set_piece(0); // in peer's bitfield
            download_state_bitfield.set_piece(2);
            download_state_bitfield.set_piece(6); // in peer's bitfield
            download_state_bitfield.set_piece(7);
            download_state_bitfield.set_piece(8); // in peer's bitfield
            download_state_bitfield.set_piece(9);
        }

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
        let (client_tx, _) = mpsc::channel(10);
        let (io_wtx, _) = mpsc::channel(10);
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
        let (client_tx, _) = mpsc::channel(10);
        let (io_wtx, _) = mpsc::channel(10);
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
        let (client_tx, _) = mpsc::channel(10);
        let (io_wtx, _) = mpsc::channel(10);
        let (disk_tx, _) = mpsc::channel(10);
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

        assert_matches!(result, Err(JamError::PieceInvalid));
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
        let (client_tx, _) = mpsc::channel(10);
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

    // Helper function to create a sample Piece
    fn create_sample_piece(index: u32, size: usize) -> Piece {
        let hash = [0u8; 20]; // Placeholder hash
        Piece::new(index, size, hash)
    }

    #[tokio::test]
    async fn test_is_peer_bitfield_complete_exact_pieces() {
        let peer_bitfield = Bitfield::new(&[0b11000010, 0b10000000]);
        let mut pieces_map = HashMap::new();
        for i in 0..10 {
            pieces_map.insert(i, create_sample_piece(i, 16384));
        }

        let download_state = Arc::new(DownloadState::new(pieces_map));
        let actor = Actor {
            client_tx: Arc::new(tokio::sync::mpsc::channel(1).0),
            io_wtx: tokio::sync::mpsc::channel(1).0,
            disk_tx: Arc::new(tokio::sync::mpsc::channel(1).0),
            shutdown_tx: tokio::sync::broadcast::channel(1).0,
            state: State {
                is_choked: false,
                is_interested: false,
                peer_choked: false,
                peer_interested: false,
                peer_bitfield,
                pieces_status: HashMap::new(),
                download_queue: vec![],
                unconfirmed: vec![],
                unconfirmed_timestamps: HashMap::new(),
                strikes: 0,
            },
            download_state,
        };

        {
            let mut download_state_bitfield = actor.download_state.metadata.bitfield.lock().await;
            download_state_bitfield.set_piece(0); // in peer's bitfield
            download_state_bitfield.set_piece(1); // in peer's bitfield
            download_state_bitfield.set_piece(6); // in peer's bitfield
            download_state_bitfield.set_piece(8); // in peer's bitfield
        }

        let completed = actor.is_complete().await;

        // Assert
        assert!(completed);
    }

    #[tokio::test]
    async fn test_is_peer_bitfield_complete_with_more_pieces_set_than_peer() {
        let peer_bitfield = Bitfield::new(&[0b11000010, 0b10000000]);
        let mut pieces_map = HashMap::new();
        for i in 0..10 {
            pieces_map.insert(i, create_sample_piece(i, 16384));
        }

        let download_state = Arc::new(DownloadState::new(pieces_map));
        let actor = Actor {
            client_tx: Arc::new(tokio::sync::mpsc::channel(1).0),
            io_wtx: tokio::sync::mpsc::channel(1).0,
            disk_tx: Arc::new(tokio::sync::mpsc::channel(1).0),
            shutdown_tx: tokio::sync::broadcast::channel(1).0,
            state: State {
                is_choked: false,
                is_interested: false,
                peer_choked: false,
                peer_interested: false,
                peer_bitfield,
                pieces_status: HashMap::new(),
                download_queue: vec![],
                unconfirmed: vec![],
                unconfirmed_timestamps: HashMap::new(),
                strikes: 0,
            },
            download_state,
        };

        {
            let mut download_state_bitfield = actor.download_state.metadata.bitfield.lock().await;
            download_state_bitfield.set_piece(0); // in peer's bitfield
            download_state_bitfield.set_piece(1); // in peer's bitfield
            download_state_bitfield.set_piece(2);
            download_state_bitfield.set_piece(6); // in peer's bitfield
            download_state_bitfield.set_piece(7);
            download_state_bitfield.set_piece(8); // in peer's bitfield
            download_state_bitfield.set_piece(9);
        }

        let completed = actor.is_complete().await;

        // Assert
        assert!(completed);
    }

    #[tokio::test]
    async fn test_peer_bitfield_is_not_completed() {
        let peer_bitfield = Bitfield::new(&[0b11000010, 0b10000000]);
        let mut pieces_map = HashMap::new();
        for i in 0..10 {
            pieces_map.insert(i, create_sample_piece(i, 16384));
        }

        let download_state = Arc::new(DownloadState::new(pieces_map));
        let actor = Actor {
            client_tx: Arc::new(tokio::sync::mpsc::channel(1).0),
            io_wtx: tokio::sync::mpsc::channel(1).0,
            disk_tx: Arc::new(tokio::sync::mpsc::channel(1).0),
            shutdown_tx: tokio::sync::broadcast::channel(1).0,
            state: State {
                is_choked: false,
                is_interested: false,
                peer_choked: false,
                peer_interested: false,
                peer_bitfield,
                pieces_status: HashMap::new(),
                download_queue: vec![],
                unconfirmed: vec![],
                unconfirmed_timestamps: HashMap::new(),
                strikes: 0,
            },
            download_state,
        };

        {
            let mut download_state_bitfield = actor.download_state.metadata.bitfield.lock().await;
            // piece 1 is still not downloaded
            download_state_bitfield.set_piece(0); // in peer's bitfield
            download_state_bitfield.set_piece(2);
            download_state_bitfield.set_piece(6); // in peer's bitfield
            download_state_bitfield.set_piece(7);
            download_state_bitfield.set_piece(8); // in peer's bitfield
            download_state_bitfield.set_piece(9);
        }

        let completed = actor.is_complete().await;

        // Assert
        assert!(!completed);
    }

    #[tokio::test]
    async fn test_does_not_have_pieces() {
        let peer_bitfield = Bitfield::new(&[0b11000010, 0b10000000]);
        let mut pieces_map = HashMap::new();
        for i in 0..10 {
            pieces_map.insert(i, create_sample_piece(i, 16384));
        }

        let download_state = Arc::new(DownloadState::new(pieces_map));
        let actor = Actor {
            client_tx: Arc::new(tokio::sync::mpsc::channel(1).0),
            io_wtx: tokio::sync::mpsc::channel(1).0,
            disk_tx: Arc::new(tokio::sync::mpsc::channel(1).0),
            shutdown_tx: tokio::sync::broadcast::channel(1).0,
            state: State {
                is_choked: false,
                is_interested: false,
                peer_choked: false,
                peer_interested: false,
                peer_bitfield,
                pieces_status: HashMap::new(),
                download_queue: vec![],
                unconfirmed: vec![],
                unconfirmed_timestamps: HashMap::new(),
                strikes: 0,
            },
            download_state,
        };

        {
            // let download_state = actor.download_state;
            let mut download_state_bitfield = actor.download_state.metadata.bitfield.lock().await;
            // piece 1 is still not downloaded
            download_state_bitfield.set_piece(0); // in peer's bitfield
            download_state_bitfield.set_piece(2);
            download_state_bitfield.set_piece(6); // in peer's bitfield
            download_state_bitfield.set_piece(7);
            download_state_bitfield.set_piece(8); // in peer's bitfield
            download_state_bitfield.set_piece(9);

            let mut assigned_pieces = actor
                .download_state
                .pieces_queue
                .assigned_pieces
                .lock()
                .await;
            assigned_pieces.insert(create_sample_piece(9, 16384));
            assigned_pieces.insert(create_sample_piece(7, 16384));
            // piece 1 can be downloaded from peer but it is already assigned
            assigned_pieces.insert(create_sample_piece(1, 16384));
        }

        let unassigned = actor.has_assignable_pieces().await;

        // Assert
        assert!(!unassigned);
    }

    #[tokio::test]
    async fn test_has_assignable_pieces() {
        let peer_bitfield = Bitfield::new(&[0b11000010, 0b10000000]);
        let mut pieces_map = HashMap::new();
        for i in 0..10 {
            pieces_map.insert(i, create_sample_piece(i, 16384));
        }

        let download_state = Arc::new(DownloadState::new(pieces_map));
        let actor = Actor {
            client_tx: Arc::new(tokio::sync::mpsc::channel(1).0),
            io_wtx: tokio::sync::mpsc::channel(1).0,
            disk_tx: Arc::new(tokio::sync::mpsc::channel(1).0),
            shutdown_tx: tokio::sync::broadcast::channel(1).0,
            state: State {
                is_choked: false,
                is_interested: false,
                peer_choked: false,
                peer_interested: false,
                peer_bitfield,
                pieces_status: HashMap::new(),
                download_queue: vec![],
                unconfirmed: vec![],
                unconfirmed_timestamps: HashMap::new(),
                strikes: 0,
            },
            download_state,
        };

        {
            // let download_state = actor.download_state;
            let mut download_state_bitfield = actor.download_state.metadata.bitfield.lock().await;
            // piece 1 is still not downloaded
            download_state_bitfield.set_piece(0); // in peer's bitfield
            download_state_bitfield.set_piece(2);
            download_state_bitfield.set_piece(6); // in peer's bitfield
            download_state_bitfield.set_piece(7);
            download_state_bitfield.set_piece(8); // in peer's bitfield
            download_state_bitfield.set_piece(9);

            let mut assigned_pieces = actor
                .download_state
                .pieces_queue
                .assigned_pieces
                .lock()
                .await;
            // piece one is neither downloaded and nor assigned
            assigned_pieces.insert(create_sample_piece(9, 16384));
        }

        let unassigned = actor.has_assignable_pieces().await;

        // Assert
        assert!(unassigned);
    }

    #[test]
    fn test_generate_peer_id() {
        let version = client_version();
        let expected = format!("-JM{}-", version);

        assert_eq!(&generate_peer_id()[..8], expected.as_bytes());
    }
}
