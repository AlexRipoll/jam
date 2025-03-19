use std::{
    collections::HashMap,
    error::Error,
    fmt::Display,
    sync::Arc,
    time::{Duration, Instant},
};

use protocol::{error::ProtocolError, piece::Piece};
use sha1::{Digest, Sha1};
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{debug, error};

use crate::torrent::peers::{
    message::{MessageError, MessageId, PiecePayload, TransferPayload},
    session::PeerSessionEvent,
};

use super::message::Message;

const MAX_STRIKES: u8 = 3;

#[derive(Debug)]
struct Coordinator {
    is_choked: bool,

    // the status of the pieces that are being downloaded or have been downloaded from this peer
    active_pieces: HashMap<usize, Piece>,

    // vector containing the pieces to be requested to the peer
    queue: Vec<Piece>,

    // vector containing the pieces that are being downloaded but stil not completed
    in_progress: Vec<Piece>,

    // timestamps of the las time a block os a piece
    in_progress_timestamps: HashMap<u32, Instant>,

    // number of times a in_progress piece has been removed from the download in progress pieces vector (in_progress) because the timeout has been reached
    strikes: u8,

    event_tx: Arc<mpsc::Sender<PeerSessionEvent>>,

    // Keep track of whether a piece request is in progress
    request_in_progress: bool,
}

#[derive(Debug)]
pub enum CoordinatorCommand {
    Choked,
    Unchoked,
    AddPiece { piece: Piece },
    DownloadPiece { payload: Option<Vec<u8>> },

    UnassignPiece { piece_index: u32 },
    UnassignPieces,
    RequestDispatched { piece: Piece },
}

impl Coordinator {
    fn new(event_tx: Arc<mpsc::Sender<PeerSessionEvent>>) -> Self {
        Self {
            is_choked: true,
            active_pieces: HashMap::new(),
            queue: vec![],
            in_progress: vec![],
            in_progress_timestamps: HashMap::new(),
            strikes: 0,
            event_tx,
            request_in_progress: false,
        }
    }

    pub fn run(mut self) -> (mpsc::Sender<CoordinatorCommand>, JoinHandle<()>) {
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<CoordinatorCommand>(100);
        let (requester_tx, mut requester_rx) = mpsc::channel::<Piece>(1);

        let event_tx_clone = self.event_tx.clone();
        let cmd_tx_clone = cmd_tx.clone();

        // Spawn a separate task for handling dispatch operations
        let request_handle = tokio::spawn(async move {
            while let Some(piece) = requester_rx.recv().await {
                if let Err(e) = Self::request(&piece, &cmd_tx_clone, &event_tx_clone).await {
                    error!("Error requesting piece with index {}: {}", piece.index(), e);
                }
            }
        });

        let requester_tx_clone = requester_tx.clone();

        // Main actor task
        let actor_handle = tokio::spawn(async move {
            // Store handle to ensure it is dropped properly when actor_handle completes
            let _request_handle = request_handle;

            while let Some(cmd) = cmd_rx.recv().await {
                if let Err(e) = self.handle_command(cmd, &requester_tx_clone).await {
                    error!("Error handling coordinator command: {}", e);
                }
            }

            // TODO: unassign all pieces before terminating
        });

        (cmd_tx, actor_handle)
    }

    async fn handle_command(
        &mut self,
        command: CoordinatorCommand,
        requester_tx: &mpsc::Sender<Piece>,
    ) -> Result<(), CoordinatorError> {
        match command {
            CoordinatorCommand::Choked => {
                self.is_choked = true;
                // trigger piece download if conditions allow it
                self.send_next_piece(requester_tx).await?;
            }
            CoordinatorCommand::Unchoked => self.is_choked = false,
            CoordinatorCommand::AddPiece { piece } => {
                self.queue.push(piece);
                // trigger piece download if conditions allow it
                self.send_next_piece(requester_tx).await?;
            }
            CoordinatorCommand::UnassignPiece { piece_index } => {
                self.release_piece(piece_index).await?
            }
            CoordinatorCommand::UnassignPieces => self.release_pieces().await?,
            CoordinatorCommand::RequestDispatched { piece } => {
                self.active_pieces
                    .entry(piece.index() as usize)
                    .or_insert_with(|| piece.clone());

                self.in_progress.push(piece.clone());
                self.in_progress_timestamps
                    .insert(piece.index(), Instant::now());

                // remove the piece from the download queue
                self.queue.retain(|p| p.index() != piece.index());
                self.request_in_progress = false;

                self.send_next_piece(requester_tx).await?;
            }
            CoordinatorCommand::DownloadPiece { payload } => {
                self.process_piece_message(payload).await?
            }
        }

        Ok(())
    }

    async fn send_next_piece(
        &mut self,
        requester_tx: &mpsc::Sender<Piece>,
    ) -> Result<(), CoordinatorError> {
        if !self.is_choked && !self.queue.is_empty() && !self.request_in_progress {
            let piece = self.queue.first().ok_or(CoordinatorError::EmptyQueue)?;

            requester_tx.send(piece.clone()).await?;
            self.request_in_progress = true;
        }

        Ok(())
    }

    async fn process_piece_message(
        &mut self,
        payload: Option<Vec<u8>>,
    ) -> Result<(), CoordinatorError> {
        let bytes = payload.ok_or(CoordinatorError::EmptyPayload)?;
        let payload = PiecePayload::deserialize(&bytes)?;

        let piece = self
            .active_pieces
            .get_mut(&(payload.index as usize))
            .ok_or(CoordinatorError::PieceNotFound)?;

        debug!(piece_index= ?payload.index, block_offset= ?payload.begin, "Downloading piece");
        // Insert the received block in the piece bytes
        piece.add_block(payload.begin, payload.block.clone())?;
        // update last block insertion
        self.in_progress_timestamps
            .insert(piece.index(), Instant::now());

        // Check if the piece has all the blocks and is not already been processed and stored in disk
        if piece.is_ready() && !piece.is_finalized() {
            let assembled_blocks = piece.assemble()?;
            debug!(piece_index= ?payload.index, "Piece download completed. Verifying hash...");

            // Validate the piece hash
            if is_valid_piece(piece.hash(), &assembled_blocks) {
                debug!(piece_index= ?payload.index, "Piece hash verified. Sending to disk");
                self.event_tx
                    .send(PeerSessionEvent::PieceAssembled {
                        data: assembled_blocks,
                    })
                    .await?;

                // remove the finalized piece from the in_progress list
                self.in_progress.retain(|p| p.index() != piece.index());
            } else {
                debug!(piece_index= ?payload.index, "Piece hash verification failed");
                self.event_tx
                    .send(PeerSessionEvent::PieceCorrupted {
                        piece_index: piece.index(),
                    })
                    .await?;

                return Err(CoordinatorError::CorruptedPiece(piece.index()));
            }
        }

        Ok(())
    }

    pub async fn request(
        piece: &Piece,
        cmd_tx: &mpsc::Sender<CoordinatorCommand>,
        event_tx: &Arc<mpsc::Sender<PeerSessionEvent>>,
    ) -> Result<(), CoordinatorError> {
        Self::request_from_offset(&piece, 0, cmd_tx, event_tx).await?;

        Ok(())
    }

    /// Core logic to request blocks from a piece starting at a given offset
    async fn request_from_offset(
        piece: &Piece,
        start_offset: u32,
        cmd_tx: &mpsc::Sender<CoordinatorCommand>,
        event_tx: &Arc<mpsc::Sender<PeerSessionEvent>>,
    ) -> Result<(), CoordinatorError> {
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
            event_tx
                .send(PeerSessionEvent::PeerMessageOut {
                    message: Message::new(MessageId::Request, Some(payload)),
                })
                .await?;
        }
        cmd_tx
            .send(CoordinatorCommand::RequestDispatched {
                piece: piece.clone(),
            })
            .await?;

        Ok(())
    }

    fn has_pieces_in_progress(&self) -> bool {
        !self.in_progress.is_empty()
    }

    fn is_strikeout(&self) -> bool {
        self.strikes >= MAX_STRIKES
    }

    // release a piece in progress so they are no longuer assigned to the current worker
    async fn release_piece(&mut self, piece_index: u32) -> Result<(), CoordinatorError> {
        self.in_progress
            .retain(|unconfirmed_piece| unconfirmed_piece.index() != piece_index);
        self.in_progress_timestamps.remove(&piece_index);

        self.event_tx
            .send(PeerSessionEvent::UnassignPiece { piece_index })
            .await?;

        Ok(())
    }

    // release pieces in progress so they are no longuer assigned to the current worker
    async fn release_pieces(&mut self) -> Result<(), CoordinatorError> {
        let pieces_index: Vec<u32> = self.in_progress.iter().map(|p| p.index()).collect();
        self.event_tx
            .send(PeerSessionEvent::UnassignPieces { pieces_index })
            .await?;
        self.in_progress.clear();
        self.in_progress_timestamps.clear();

        Ok(())
    }

    async fn release_timed_out_pieces(&mut self) -> Result<(), CoordinatorError> {
        let now = Instant::now();
        let timed_out_pieces: Vec<Piece> = self
            .in_progress
            .iter()
            .filter(|piece| {
                if let Some(&request_time) = self.in_progress_timestamps.get(&piece.index()) {
                    now.duration_since(request_time) > Duration::new(15, 0)
                } else {
                    false
                }
            })
            .cloned()
            .collect();

        for timed_out_piece in timed_out_pieces.iter() {
            self.release_piece(timed_out_piece.index()).await?;
            self.strikes += 1;
        }

        Ok(())
    }

    fn are_active_pieces_finalized(&self) -> bool {
        self.active_pieces
            .iter()
            .all(|(_, piece)| piece.is_finalized())
    }
}

// FIX: move to piece module
fn is_valid_piece(piece_hash: [u8; 20], piece_bytes: &[u8]) -> bool {
    let mut hasher = Sha1::new();
    hasher.update(piece_bytes);
    let hash = hasher.finalize();

    piece_hash == hash.as_slice()
}

#[derive(Debug)]
pub enum CoordinatorError {
    Message(MessageError),
    Protocol(ProtocolError),
    EmptyPayload,
    PieceNotFound,
    EmptyQueue,
    CorruptedPiece(u32),
    CommandTxError(mpsc::error::SendError<CoordinatorCommand>),
    EventTxError(mpsc::error::SendError<PeerSessionEvent>),
    RequesterTxError(mpsc::error::SendError<Piece>),
}

impl Display for CoordinatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CoordinatorError::EmptyPayload => write!(f, "Received message with empty payload"),
            CoordinatorError::PieceNotFound => write!(f, "Piece not found in map"),
            CoordinatorError::EmptyQueue => write!(f, "Empty queue"),
            CoordinatorError::CorruptedPiece(piece_index) => {
                write!(f, "Invalid hash found for piece with index {}", piece_index)
            }
            CoordinatorError::CommandTxError(err) => {
                write!(f, "Failed to send command: {}", err)
            }
            CoordinatorError::EventTxError(err) => {
                write!(f, "Failed to send event: {}", err)
            }
            CoordinatorError::RequesterTxError(err) => {
                write!(f, "Failed to send piece request: {}", err)
            }
            CoordinatorError::Message(err) => write!(f, "Message error: {}", err),
            CoordinatorError::Protocol(err) => write!(f, "Protocol error: {}", err),
        }
    }
}

impl From<ProtocolError> for CoordinatorError {
    fn from(err: ProtocolError) -> Self {
        CoordinatorError::Protocol(err)
    }
}

impl From<MessageError> for CoordinatorError {
    fn from(err: MessageError) -> Self {
        CoordinatorError::Message(err)
    }
}

impl From<mpsc::error::SendError<CoordinatorCommand>> for CoordinatorError {
    fn from(err: mpsc::error::SendError<CoordinatorCommand>) -> Self {
        CoordinatorError::CommandTxError(err)
    }
}

impl From<mpsc::error::SendError<PeerSessionEvent>> for CoordinatorError {
    fn from(err: mpsc::error::SendError<PeerSessionEvent>) -> Self {
        CoordinatorError::EventTxError(err)
    }
}

impl From<mpsc::error::SendError<Piece>> for CoordinatorError {
    fn from(err: mpsc::error::SendError<Piece>) -> Self {
        CoordinatorError::RequesterTxError(err)
    }
}

impl Error for CoordinatorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            CoordinatorError::Protocol(err) => Some(err),
            CoordinatorError::Message(err) => Some(err),
            CoordinatorError::EventTxError(err) => Some(err),
            CoordinatorError::RequesterTxError(err) => Some(err),
            _ => None,
        }
    }
}
