use std::{
    cmp::{max, min},
    collections::HashMap,
    error::Error,
    fmt::{self, Display},
    time::{Duration, Instant},
};

use protocol::{error::ProtocolError, piece::Piece};
use sha1::{Digest, Sha1};
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{debug, error, trace};

use crate::torrent::peers::{
    message::{MessageError, MessageId, PiecePayload, TransferPayload},
    session::PeerSessionEvent,
};

use super::message::Message;

const CHAN_BUFF_MARGIN_COEF: f64 = 0.9;

const MAX_STRIKES: u8 = 3;

// Timeout checker intervals
const MIN_CHECK_INTERVAL: u64 = 1;
const MAX_CHECK_INTERVAL: u64 = 10;

/// Tracks, coordinates and manages the download process for pieces from a peer
///
/// The Coordinator is responsible for:
/// - Managing the queue of pieces to download
/// - Tracking pieces that are actively being downloaded
/// - Handling timeouts for pieces that take too long
/// - Coordinating with the peer session to request and process pieces
#[derive(Debug)]
pub struct Coordinator {
    /// The session id for logging and identification
    id: String,
    /// Flag indicating if the peer has choked us
    is_choked: bool,
    /// Map of pieces that are being downloaded or have been downloaded from this peer
    active_pieces: HashMap<usize, Piece>,
    /// Pieces waiting to be requested from the peer
    queue: Vec<Piece>,
    /// Pieces that are currently being downloaded but not yet completed
    in_progress: Vec<Piece>,
    /// Maximum number of pieces that can be in progress simultaneously
    max_in_progress: usize,
    /// Timestamps for each in-progress piece's last activity
    in_progress_timestamps: HashMap<u32, Instant>,
    /// Configuration parameters
    config: CoordinatorConfig,
    /// Number of consecutive timeout strikes
    strikes: u8,
    /// Channel for sending commands to the coordinator
    cmd_tx: mpsc::Sender<CoordinatorCommand>,
    /// Channel for receiving commands
    cmd_rx: mpsc::Receiver<CoordinatorCommand>,
    /// Channel for sending events to the peer session
    event_tx: mpsc::Sender<PeerSessionEvent>,
    /// Indicates if a piece request is currently being processed
    request_in_progress: bool,
}

/// Configuration for the Coordinator component
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// Factor to determine how much of the channel buffer can be used
    pub chan_buff_margin_coef: f64,
    /// Maximum number of consecutive timeouts before considering a peer unresponsive
    pub max_strikes: u8,
    /// Minimum interval (in seconds) to check for timed-out pieces
    pub min_check_interval: u64,
    /// Maximum interval (in seconds) to check for timed-out pieces
    pub max_check_interval: u64,
    /// Maximum time (in seconds) to wait for a piece block before timing out
    pub timeout_threshold: u64,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            chan_buff_margin_coef: 0.9,
            max_strikes: 3,
            min_check_interval: 1,
            max_check_interval: 10,
            timeout_threshold: 5,
        }
    }
}

/// Commands that can be sent to the Coordinator to control its behavior
#[derive(Debug)]
pub enum CoordinatorCommand {
    /// Indicates the peer has choked us
    Choked,
    /// Indicates the peer has unchoked us
    Unchoked,
    /// Adds a new piece to the download queue
    AddPiece { piece: Piece },
    /// Processes a downloaded piece block payload
    DownloadPiece { payload: Option<Vec<u8>> },
    /// Releases a specific piece back to the piece manager
    ReleasePiece { piece_index: u32 },
    /// Releases all pieces back to the piece manager
    ReleasePieces,
    /// Indicates a piece request has been dispatched
    RequestDispatched { piece: Piece },
    /// Signals to check for timed-out pieces
    CheckTimeouts,
}

impl Coordinator {
    /// Creates a new Coordinator instance
    ///
    /// # Arguments
    ///
    /// * `id` - The session identifier
    /// * `piece_size` - The size of each piece in bytes
    /// * `block_size` - The size of each block in bytes
    /// * `buffer_size` - The size of the command channel buffer
    /// * `config` - Optional configuration, uses default if not provided
    /// * `event_tx` - Channel to send events to the peer session
    ///
    /// # Returns
    ///
    /// A new Coordinator instance
    pub fn new(
        id: String,
        piece_size: usize,
        block_size: usize,
        buffer_size: usize,
        event_tx: mpsc::Sender<PeerSessionEvent>,
        config: CoordinatorConfig,
    ) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::channel::<CoordinatorCommand>(buffer_size);

        let blocks_per_piece = (piece_size + block_size - 1) / block_size;
        let max_in_progress = ((buffer_size as f64 * config.chan_buff_margin_coef).floor()
            as usize)
            .saturating_div(blocks_per_piece);

        let mut config = config;
        // Ensure timeout threshold is never zero
        if config.timeout_threshold == 0 {
            // Set to a reasonable default if zero
            config.timeout_threshold = 5; // Using 5 seconds as a safe default
        }

        Self {
            id,
            is_choked: true,
            active_pieces: HashMap::new(),
            queue: vec![],
            in_progress: vec![],
            max_in_progress,
            in_progress_timestamps: HashMap::new(),
            request_in_progress: false,
            config,
            strikes: 0,
            cmd_tx,
            cmd_rx,
            event_tx,
        }
    }

    /// Starts the coordinator process and returns handles to control it
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - The command sender channel to control the coordinator
    /// - A JoinHandle for the main coordinator task
    pub fn run(mut self) -> (mpsc::Sender<CoordinatorCommand>, JoinHandle<()>) {
        let (requester_tx, mut requester_rx) = mpsc::channel::<Piece>(1);

        let event_tx_clone = self.event_tx.clone();
        let cmd_tx_clone = self.cmd_tx.clone();
        let req_cmd_tx = cmd_tx_clone.clone();

        let id = self.id.clone();

        // Spawn a separate task for handling piece requests
        let request_handle = tokio::spawn(async move {
            while let Some(piece) = requester_rx.recv().await {
                if let Err(e) =
                    Self::request(id.clone(), &piece, &req_cmd_tx, &event_tx_clone).await
                {
                    error!(
                        session_id = %id,
                        piece_index = %piece.index(),
                        error = %e,
                        "Failed to request piece"
                    );
                }
            }
        });

        // Calculate the timeout check interval based on configuration
        let interval = max(
            self.config.min_check_interval,
            min(
                self.config.timeout_threshold / 5,
                self.config.max_check_interval,
            ),
        );

        // Create a shutdown channel to stop the timeout checker when done
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

        let timeout_cmd_tx = cmd_tx_clone.clone();
        let session_id = self.id.clone();

        // Spawn a task for checking timeouts periodically
        let timeout_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval));
            debug!(
                session_id = %session_id,
                interval_secs = ?interval,
                "Starting timeout checker"
            );

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        trace!(session_id = %session_id, "Checking for piece timeouts");
                        if let Err(e) = timeout_cmd_tx.send(CoordinatorCommand::CheckTimeouts).await {
                            error!(session_id = %session_id, error = %e, "Failed to send timeout check command");
                            break;
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!(session_id = %session_id, "Timeout checker received shutdown signal");
                        break;
                    }
                }
            }
        });

        let requester_tx_clone = requester_tx.clone();
        let actor_id = self.id.clone();

        // Main coordinator task
        let actor_handle = tokio::spawn(async move {
            debug!(session_id = %actor_id, "Starting coordinator actor");

            while let Some(cmd) = self.cmd_rx.recv().await {
                trace!(session_id = %actor_id, cmd = ?cmd, "Received coordinator command");

                if let Err(e) = self.handle_command(cmd, &requester_tx_clone).await {
                    error!(
                        session_id = %actor_id,
                        error = %e,
                        "Error handling coordinator command"
                    );
                }
            }

            // Send shutdown signal to the timeout checker
            debug!(session_id = %actor_id, "Coordinator shutting down");
            let _ = shutdown_tx.send(());

            // If shutdown signal might be missed, abort after a short delay
            tokio::time::sleep(Duration::from_millis(100)).await;
            timeout_handle.abort();
            request_handle.abort();

            // Release all pieces before terminating
            if let Err(e) = self.release_all_pieces().await {
                error!(
                    session_id = %actor_id,
                    error = %e,
                    "Failed to release pieces during shutdown"
                );
            }
        });

        (cmd_tx_clone, actor_handle)
    }

    /// Handles a command received by the coordinator
    ///
    /// # Arguments
    ///
    /// * `command` - The command to handle
    /// * `requester_tx` - Channel for sending piece requests
    ///
    /// # Returns
    ///
    /// Result indicating success or failure
    async fn handle_command(
        &mut self,
        command: CoordinatorCommand,
        requester_tx: &mpsc::Sender<Piece>,
    ) -> Result<(), CoordinatorError> {
        match command {
            CoordinatorCommand::Choked => {
                debug!(session_id = %self.id, "Peer has choked us");
                self.is_choked = true;
            }
            CoordinatorCommand::Unchoked => {
                debug!(session_id = %self.id, "Peer has unchoked us");
                self.is_choked = false;
            }
            CoordinatorCommand::AddPiece { piece } => {
                debug!(
                    session_id = %self.id,
                    piece_index = %piece.index(),
                    "Adding piece to download queue"
                );
                self.queue.push(piece);
            }
            CoordinatorCommand::ReleasePiece { piece_index } => {
                debug!(
                    session_id = %self.id,
                    piece_index = piece_index,
                    "Releasing piece"
                );
                self.release_piece(piece_index).await?
            }
            CoordinatorCommand::ReleasePieces => {
                debug!(session_id = %self.id, "Releasing all pieces");
                self.release_all_pieces().await?
            }
            CoordinatorCommand::RequestDispatched { piece } => {
                self.handle_request_dispatched(piece)?;
            }
            CoordinatorCommand::DownloadPiece { payload } => {
                self.process_piece_message(payload).await?
            }
            CoordinatorCommand::CheckTimeouts => {
                self.check_timeouts().await?;
            }
        }

        // Try to send the next piece if conditions allow
        self.send_next_piece(requester_tx).await?;

        Ok(())
    }

    /// Handles a request dispatched event
    ///
    /// # Arguments
    ///
    /// * `piece` - The piece that was dispatched
    ///
    /// # Returns
    ///
    /// Result indicating success or failure
    fn handle_request_dispatched(&mut self, piece: Piece) -> Result<(), CoordinatorError> {
        debug!(
            session_id = %self.id,
            piece_index = %piece.index(),
            "Request dispatched for piece"
        );

        // Store the piece in active pieces map if not already present
        self.active_pieces
            .entry(piece.index() as usize)
            .or_insert_with(|| piece.clone());

        // Add to in-progress collection and record timestamp
        self.in_progress.push(piece.clone());
        self.in_progress_timestamps
            .insert(piece.index(), Instant::now());

        // Remove the piece from the download queue
        self.queue.retain(|p| p.index() != piece.index());
        self.request_in_progress = false;

        Ok(())
    }

    /// Attempts to send the next piece for download if conditions allow
    ///
    /// # Arguments
    ///
    /// * `requester_tx` - Channel for sending piece requests
    ///
    /// # Returns
    ///
    /// Result indicating success or failure
    async fn send_next_piece(
        &mut self,
        requester_tx: &mpsc::Sender<Piece>,
    ) -> Result<(), CoordinatorError> {
        // Check if we can send a new piece request
        if !self.is_choked
            && !self.queue.is_empty()
            && self.in_progress.len() < self.max_in_progress
            && !self.request_in_progress
        {
            let piece = self.queue.first().ok_or(CoordinatorError::EmptyQueue)?;

            debug!(
                session_id = %self.id,
                piece_index = %piece.index(),
                "Sending piece to requester"
            );

            requester_tx.send(piece.clone()).await?;
            self.request_in_progress = true;
        }

        Ok(())
    }

    /// Processes a piece message received from the peer
    ///
    /// # Arguments
    ///
    /// * `payload` - The binary payload containing the piece data
    ///
    /// # Returns
    ///
    /// Result indicating success or failure
    async fn process_piece_message(
        &mut self,
        payload: Option<Vec<u8>>,
    ) -> Result<(), CoordinatorError> {
        // Ensure we have a payload
        let bytes = payload.ok_or(CoordinatorError::EmptyPayload)?;
        let payload = PiecePayload::try_from(bytes.as_slice())?;

        // Find the piece in our active pieces map
        let piece = self
            .active_pieces
            .get_mut(&(payload.index as usize))
            .ok_or(CoordinatorError::PieceNotFound(payload.index))?;

        debug!(
            session_id = %self.id,
            piece_index = %payload.index,
            block_offset = %payload.begin,
            block_size = %payload.block.len(),
            "Received block for piece"
        );

        // Add the block to the piece
        piece.add_block(payload.begin, payload.block.clone())?;

        // Update timestamp for this piece
        self.in_progress_timestamps
            .insert(piece.index(), Instant::now());

        // Check if the piece is complete
        if piece.is_ready() && !piece.is_finalized() {
            let piece_index = piece.index();
            let piece_hash = piece.hash();
            let assembled_blocks = piece.assemble()?;

            self.handle_complete_piece(piece_index, piece_hash, assembled_blocks)
                .await?;
        }

        Ok(())
    }

    /// Handles a piece that has received all its blocks
    ///
    /// # Arguments
    ///
    /// * `piece` - The completed piece
    ///
    /// # Returns
    ///
    /// Result indicating success or failure
    async fn handle_complete_piece(
        &mut self,
        piece_index: u32,
        piece_hash: [u8; 20],
        assembled_blocks: Vec<u8>,
    ) -> Result<(), CoordinatorError> {
        debug!(
            session_id = %self.id,
            piece_index = %piece_index,
            "Piece download completed, verifying hash"
        );

        // Validate the piece hash
        if is_valid_piece(piece_hash, &assembled_blocks) {
            debug!(
                session_id = %self.id,
                piece_index = %piece_index,
                "Piece hash verified successfully"
            );

            // Notify the session that the piece is complete
            self.event_tx
                .send(PeerSessionEvent::PieceAssembled {
                    piece_index,
                    data: assembled_blocks,
                })
                .await?;

            // Remove from in-progress collections
            self.in_progress.retain(|p| p.index() != piece_index);
            self.in_progress_timestamps.remove(&piece_index);
        } else {
            debug!(
                session_id = %self.id,
                piece_index = %piece_index,
                "Piece hash verification failed"
            );

            // Notify the session about the corrupted piece
            self.event_tx
                .send(PeerSessionEvent::PieceCorrupted { piece_index })
                .await?;

            // Remove the corrupted piece from all collections
            self.active_pieces.remove(&(piece_index as usize));
            self.in_progress.retain(|p| p.index() != piece_index);
            self.in_progress_timestamps.remove(&piece_index);

            return Err(CoordinatorError::CorruptedPiece(piece_index));
        }

        Ok(())
    }

    /// Requests all blocks for a piece from a peer
    ///
    /// # Arguments
    ///
    /// * `id` - Session ID for logging
    /// * `piece` - The piece to request
    /// * `cmd_tx` - Channel for sending commands back to the coordinator
    /// * `event_tx` - Channel for sending events to the peer session
    ///
    /// # Returns
    ///
    /// Result indicating success or failure
    pub async fn request(
        id: String,
        piece: &Piece,
        cmd_tx: &mpsc::Sender<CoordinatorCommand>,
        event_tx: &mpsc::Sender<PeerSessionEvent>,
    ) -> Result<(), CoordinatorError> {
        Self::request_from_offset(id, piece, 0, cmd_tx, event_tx).await?;
        Ok(())
    }

    /// Requests blocks for a piece starting from a specified offset
    ///
    /// # Arguments
    ///
    /// * `id` - Session ID for logging
    /// * `piece` - The piece to request
    /// * `start_offset` - Offset to start requesting blocks from
    /// * `cmd_tx` - Channel for sending commands back to the coordinator
    /// * `event_tx` - Channel for sending events to the peer session
    ///
    /// # Returns
    ///
    /// Result indicating success or failure
    async fn request_from_offset(
        id: String,
        piece: &Piece,
        start_offset: u32,
        cmd_tx: &mpsc::Sender<CoordinatorCommand>,
        event_tx: &mpsc::Sender<PeerSessionEvent>,
    ) -> Result<(), CoordinatorError> {
        const BLOCK_SIZE: u32 = 16384; // 16 KiB
        let total_blocks = (piece.size() + BLOCK_SIZE as usize - 1) / BLOCK_SIZE as usize;

        debug!(
            session_id = %id,
            piece_index = %piece.index(),
            total_blocks = total_blocks,
            start_offset = start_offset,
            "Requesting blocks for piece"
        );

        for block_index in ((start_offset / BLOCK_SIZE) as usize)..total_blocks {
            let block_offset = block_index as u32 * BLOCK_SIZE;
            let block_size = BLOCK_SIZE.min(piece.size() as u32 - block_offset);

            let payload = TransferPayload::new(piece.index(), block_offset, block_size).serialize();

            debug!(
                session_id = %id,
                piece_index = %piece.index(),
                block_offset = block_offset,
                block_size = block_size,
                "Sending block request"
            );

            event_tx
                .send(PeerSessionEvent::PeerMessageOut {
                    message: Message::new(MessageId::Request, Some(payload)),
                })
                .await?;
        }

        // Notify the coordinator that the request was dispatched
        cmd_tx
            .send(CoordinatorCommand::RequestDispatched {
                piece: piece.clone(),
            })
            .await?;

        Ok(())
    }

    /// Checks if the coordinator has pieces in progress
    ///
    /// # Returns
    ///
    /// `true` if there are pieces being downloaded, `false` otherwise
    fn has_pieces_in_progress(&self) -> bool {
        !self.in_progress.is_empty()
    }

    /// Checks if the maximum number of strikes has been reached
    ///
    /// # Returns
    ///
    /// `true` if the number of strikes equals or exceeds the maximum, `false` otherwise
    fn is_strikeout(&self) -> bool {
        self.strikes >= self.config.max_strikes
    }

    /// Releases a specific piece back to the piece manager
    ///
    /// # Arguments
    ///
    /// * `piece_index` - Index of the piece to release
    ///
    /// # Returns
    ///
    /// Result indicating success or failure
    async fn release_piece(&mut self, piece_index: u32) -> Result<(), CoordinatorError> {
        // Remove from collections
        self.active_pieces.remove(&(piece_index as usize));
        self.in_progress.retain(|p| p.index() != piece_index);
        self.in_progress_timestamps.remove(&piece_index);

        // Notify session about unassignment
        self.event_tx
            .send(PeerSessionEvent::UnassignPiece { piece_index })
            .await?;

        Ok(())
    }

    /// Releases all pieces back to the piece manager
    ///
    /// # Returns
    ///
    /// Result indicating success or failure
    async fn release_all_pieces(&mut self) -> Result<(), CoordinatorError> {
        let pieces_index: Vec<u32> = self.in_progress.iter().map(|p| p.index()).collect();

        if !pieces_index.is_empty() {
            debug!(
                session_id = %self.id,
                piece_count = pieces_index.len(),
                "Releasing all pieces"
            );

            self.event_tx
                .send(PeerSessionEvent::UnassignPieces { pieces_index })
                .await?;
        }

        // Clear all collections
        self.active_pieces.clear();
        self.in_progress.clear();
        self.in_progress_timestamps.clear();

        Ok(())
    }

    /// Checks for pieces that have timed out and handles them appropriately
    ///
    /// # Returns
    ///
    /// Result indicating success or failure
    async fn check_timeouts(&mut self) -> Result<(), CoordinatorError> {
        if !self.has_pieces_in_progress() {
            return Ok(());
        }

        self.release_timed_out_pieces().await?;

        // Check if we've reached max strikes
        if self.is_strikeout() {
            debug!(
                session_id = %self.id,
                strikes = self.strikes,
                max_strikes = self.config.max_strikes,
                "Maximum strikes reached, shutting down session"
            );

            self.release_all_pieces().await?;

            // Signal session to shut down
            self.event_tx.send(PeerSessionEvent::Shutdown).await?;
        }

        Ok(())
    }

    /// Finds and releases pieces that have timed out
    ///
    /// # Returns
    ///
    /// Result indicating success or failure
    async fn release_timed_out_pieces(&mut self) -> Result<(), CoordinatorError> {
        let now = Instant::now();
        let timeout_duration = Duration::from_secs(self.config.timeout_threshold);

        // Find pieces that have timed out
        let timed_out_pieces: Vec<Piece> = self
            .in_progress
            .iter()
            .filter(|piece| {
                if let Some(&request_time) = self.in_progress_timestamps.get(&piece.index()) {
                    let elapsed = now.duration_since(request_time);
                    if elapsed > timeout_duration {
                        debug!(
                            session_id = %self.id,
                            piece_index = %piece.index(),
                            elapsed_secs = %elapsed.as_secs(),
                            timeout_secs = %self.config.timeout_threshold,
                            "Piece timed out"
                        );
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            })
            .cloned()
            .collect();

        // Release each timed out piece
        for timed_out_piece in timed_out_pieces.iter() {
            debug!(
                session_id = %self.id,
                piece_index = %timed_out_piece.index(),
                strikes = self.strikes + 1,
                "Releasing timed out piece"
            );

            self.release_piece(timed_out_piece.index()).await?;
            self.strikes += 1;
        }

        Ok(())
    }
}

/// Validates the hash of a piece
///
/// # Arguments
///
/// * `piece_hash` - Expected hash for the piece
/// * `piece_bytes` - Actual bytes of the piece
///
/// # Returns
///
/// `true` if the hash matches, `false` otherwise
// FIX: move to piece module
fn is_valid_piece(piece_hash: [u8; 20], piece_bytes: &[u8]) -> bool {
    let mut hasher = Sha1::new();
    hasher.update(piece_bytes);
    let hash = hasher.finalize();

    piece_hash == hash.as_slice()
}

#[derive(Debug)]
pub enum CoordinatorError {
    /// Error in message handling
    Message(MessageError),
    /// Error in protocol
    Protocol(ProtocolError),
    /// Empty payload received
    EmptyPayload,
    /// Piece not found in active pieces
    PieceNotFound(u32),
    /// Queue is empty when trying to get next piece
    EmptyQueue,
    /// Piece hash validation failed
    CorruptedPiece(u32),
    /// Failed to send coordinator command
    CommandTxError(mpsc::error::SendError<CoordinatorCommand>),
    /// Failed to send session event
    EventTxError(mpsc::error::SendError<PeerSessionEvent>),
    /// Failed to send piece to requester
    RequesterTxError(mpsc::error::SendError<Piece>),
}

// Implement Display trait for better error messages
impl fmt::Display for CoordinatorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CoordinatorError::Message(err) => write!(f, "Message error: {}", err),
            CoordinatorError::Protocol(err) => write!(f, "Protocol error: {}", err),
            CoordinatorError::EmptyPayload => write!(f, "Received message with empty payload"),
            CoordinatorError::PieceNotFound(index) => {
                write!(f, "Piece with index {} not found in map", index)
            }
            CoordinatorError::EmptyQueue => write!(f, "Empty queue"),
            CoordinatorError::CorruptedPiece(index) => {
                write!(f, "Invalid hash found for piece with index {}", index)
            }
            CoordinatorError::CommandTxError(err) => write!(f, "Failed to send command: {}", err),
            CoordinatorError::EventTxError(err) => write!(f, "Failed to send event: {}", err),
            CoordinatorError::RequesterTxError(err) => {
                write!(f, "Failed to send piece request: {}", err)
            }
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

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        time::{Duration, Instant},
    };

    use protocol::{error::ProtocolError, piece::Piece};
    use sha1::{Digest, Sha1};
    use tokio::{sync::mpsc, task::JoinHandle};

    use crate::torrent::peers::{
        coordinator::{is_valid_piece, Coordinator, CoordinatorCommand, CoordinatorConfig},
        message::{MessageError, MessageId, PiecePayload, TransferPayload},
        session::PeerSessionEvent,
    };

    // Mock function for creating a test piece
    fn create_test_piece(index: u32, size: usize) -> Piece {
        // Create a simple test piece with the given index and size
        // This is a simplified version just for testing
        Piece::new(index, size, [0; 20])
    }

    // Helper for creating a piece payload for testing
    fn create_piece_payload(index: u32, begin: u32, data: Vec<u8>) -> Vec<u8> {
        let payload = PiecePayload {
            index,
            begin,
            block: data,
        };
        payload.serialize()
    }

    #[tokio::test]
    async fn test_new_coordinator() {
        // Test that a new coordinator is created with the expected properties
        let id = "test_session".to_string();
        let piece_size = 32768;
        let block_size = 16384;
        let buffer_size = 100;
        let (event_tx, _event_rx) = mpsc::channel::<PeerSessionEvent>(10);
        let config = CoordinatorConfig::default();

        let coordinator = Coordinator::new(
            id.clone(),
            piece_size,
            block_size,
            buffer_size,
            event_tx,
            config.clone(),
        );

        // Assert initial state
        assert_eq!(coordinator.id, id);
        assert!(coordinator.is_choked);
        assert!(coordinator.active_pieces.is_empty());
        assert!(coordinator.queue.is_empty());
        assert!(coordinator.in_progress.is_empty());
        assert!(coordinator.in_progress_timestamps.is_empty());
        assert!(!coordinator.request_in_progress);
        assert_eq!(coordinator.strikes, 0);

        // Test max_in_progress calculation
        let blocks_per_piece = (piece_size + block_size - 1) / block_size;
        let expected_max_in_progress = ((buffer_size as f64 * config.chan_buff_margin_coef).floor()
            as usize)
            .saturating_div(blocks_per_piece);
        assert_eq!(coordinator.max_in_progress, expected_max_in_progress);
    }

    #[tokio::test]
    async fn test_handle_request_dispatched() {
        // Test that handling a request dispatched event properly updates the coordinator state
        let id = "test_session".to_string();
        let piece_size = 32768;
        let block_size = 16384;
        let buffer_size = 100;
        let (event_tx, _event_rx) = mpsc::channel::<PeerSessionEvent>(10);
        let config = CoordinatorConfig::default();

        let mut coordinator = Coordinator::new(
            id.clone(),
            piece_size,
            block_size,
            buffer_size,
            event_tx,
            config,
        );

        // Add a piece to the queue
        let piece = create_test_piece(1, piece_size);
        coordinator.queue.push(piece.clone());

        // Handle request dispatched
        let result = coordinator.handle_request_dispatched(piece.clone());
        assert!(result.is_ok());

        // Assert state changes
        assert!(coordinator
            .active_pieces
            .contains_key(&(piece.index() as usize)));
        assert_eq!(coordinator.in_progress.len(), 1);
        assert_eq!(coordinator.in_progress[0].index(), piece.index());
        assert!(coordinator
            .in_progress_timestamps
            .contains_key(&piece.index()));
        assert!(!coordinator.request_in_progress);
        assert!(coordinator.queue.is_empty());
    }

    #[tokio::test]
    async fn test_send_next_piece() {
        // Test sending the next piece for download
        let id = "test_session".to_string();
        let piece_size = 32768;
        let block_size = 16384;
        let buffer_size = 100;
        let (event_tx, _event_rx) = mpsc::channel::<PeerSessionEvent>(10);
        let config = CoordinatorConfig::default();

        let mut coordinator = Coordinator::new(
            id.clone(),
            piece_size,
            block_size,
            buffer_size,
            event_tx,
            config,
        );

        // Add a piece to the queue
        let piece = create_test_piece(1, piece_size);
        coordinator.queue.push(piece.clone());

        // Create a channel for the test
        let (requester_tx, mut requester_rx) = mpsc::channel::<Piece>(1);

        // Setup conditions for sending
        coordinator.is_choked = false;

        // Send the next piece
        let handle = tokio::spawn(async move {
            // Receive the piece in a separate task
            requester_rx.recv().await
        });

        let result = coordinator.send_next_piece(&requester_tx).await;
        assert!(result.is_ok());

        // Assert that request_in_progress is set
        assert!(coordinator.request_in_progress);

        // Verify that the piece was sent
        let received_piece = tokio::time::timeout(Duration::from_millis(100), handle).await;
        assert!(received_piece.is_ok());
        let received_piece = received_piece.unwrap().unwrap();
        assert!(received_piece.is_some());
        let received_piece = received_piece.unwrap();
        assert_eq!(received_piece.index(), piece.index());
    }

    #[tokio::test]
    async fn test_process_piece_message_completed_with_valid_hash() {
        // Test processing a piece message
        let id = "test_session".to_string();
        let piece_size = 32768;
        let block_size = 16384;
        let buffer_size = 100;
        let (event_tx, mut event_rx) = mpsc::channel::<PeerSessionEvent>(10);
        let config = CoordinatorConfig::default();

        let mut coordinator = Coordinator::new(
            id.clone(),
            piece_size,
            block_size,
            buffer_size,
            event_tx,
            config,
        );

        // Add a piece to active pieces
        let piece_index = 1;
        let piece_hash: [u8; 20] = [
            25, 229, 220, 52, 237, 167, 156, 163, 238, 115, 134, 11, 97, 182, 97, 133, 20, 155,
            111, 180,
        ];
        let piece = Piece::new(piece_index, block_size, piece_hash);
        coordinator
            .active_pieces
            .insert(piece_index as usize, piece.clone());
        coordinator.in_progress.push(piece.clone());
        coordinator
            .in_progress_timestamps
            .insert(piece_index, Instant::now());

        // Create a valid piece payload
        let block_offset = 0;
        let data = vec![1; block_size]; // Create a block with all 1s
        let payload = create_piece_payload(piece_index, block_offset as u32, data.clone());

        // Process the piece message
        let result = coordinator.process_piece_message(Some(payload)).await;

        // This should fail because our test piece doesn't have correct hash validation
        assert!(result.is_ok());

        // Verify the event was sent
        let event = tokio::time::timeout(Duration::from_millis(100), event_rx.recv()).await;
        assert!(event.is_ok());
        let event = event.unwrap();
        assert!(
            matches!(event, Some(PeerSessionEvent::PieceAssembled { piece_index: p , data }) if p == piece_index)
        );

        //  we can at least verify the piece was removed after corruption detection
        assert!(!coordinator
            .in_progress
            .iter()
            .any(|p| p.index() == piece_index));
        assert!(!coordinator
            .in_progress_timestamps
            .contains_key(&piece_index));
    }

    #[tokio::test]
    async fn test_process_piece_message_completed_with_invalid_hash() {
        // Test processing a piece message
        let id = "test_session".to_string();
        let piece_size = 32768;
        let block_size = 16384;
        let buffer_size = 100;
        let (event_tx, mut event_rx) = mpsc::channel::<PeerSessionEvent>(10);
        let config = CoordinatorConfig::default();

        let mut coordinator = Coordinator::new(
            id.clone(),
            piece_size,
            block_size,
            buffer_size,
            event_tx,
            config,
        );

        // Add a piece to active pieces
        let piece_index = 1;
        let piece = create_test_piece(piece_index, block_size); // Use a single block piece for simplicity
        coordinator
            .active_pieces
            .insert(piece_index as usize, piece.clone());
        coordinator.in_progress.push(piece.clone());
        coordinator
            .in_progress_timestamps
            .insert(piece_index, Instant::now());

        // Create a valid piece payload
        let block_offset = 0;
        let data = vec![1; block_size]; // Create a block with all 1s
        let payload = create_piece_payload(piece_index, block_offset as u32, data.clone());

        // Process the piece message
        let result = coordinator.process_piece_message(Some(payload)).await;

        // This should fail because our test piece doesn't have correct hash validation
        assert!(result.is_err());

        //  we can at least verify the piece was removed after corruption detection
        assert!(!coordinator
            .in_progress
            .iter()
            .any(|p| p.index() == piece_index));
        assert!(!coordinator
            .in_progress_timestamps
            .contains_key(&piece_index));
    }

    #[tokio::test]
    async fn test_release_piece() {
        // Test releasing a piece
        let id = "test_session".to_string();
        let piece_size = 32768;
        let block_size = 16384;
        let buffer_size = 100;
        let (event_tx, mut event_rx) = mpsc::channel::<PeerSessionEvent>(10);
        let config = CoordinatorConfig::default();

        let mut coordinator = Coordinator::new(
            id.clone(),
            piece_size,
            block_size,
            buffer_size,
            event_tx,
            config,
        );

        // Add a piece to collections
        let piece_index = 1;
        let piece = create_test_piece(piece_index, piece_size);
        coordinator
            .active_pieces
            .insert(piece_index as usize, piece.clone());
        coordinator.in_progress.push(piece.clone());
        coordinator
            .in_progress_timestamps
            .insert(piece_index, Instant::now());

        // Release the piece
        let result = coordinator.release_piece(piece_index).await;
        assert!(result.is_ok());

        // Assert piece was removed from collections
        assert!(!coordinator
            .active_pieces
            .contains_key(&(piece_index as usize)));
        assert!(!coordinator
            .in_progress
            .iter()
            .any(|p| p.index() == piece_index));
        assert!(!coordinator
            .in_progress_timestamps
            .contains_key(&piece_index));

        // Verify the event was sent
        let event = tokio::time::timeout(Duration::from_millis(100), event_rx.recv()).await;
        assert!(event.is_ok());
        let event = event.unwrap();
        assert!(
            matches!(event, Some(PeerSessionEvent::UnassignPiece { piece_index: p }) if p == piece_index)
        );
    }

    #[tokio::test]
    async fn test_release_all_pieces() {
        // Test releasing all pieces
        let id = "test_session".to_string();
        let piece_size = 32768;
        let block_size = 16384;
        let buffer_size = 100;
        let (event_tx, mut event_rx) = mpsc::channel::<PeerSessionEvent>(10);
        let config = CoordinatorConfig::default();

        let mut coordinator = Coordinator::new(
            id.clone(),
            piece_size,
            block_size,
            buffer_size,
            event_tx,
            config,
        );

        // Add multiple pieces to collections
        for i in 1..=3 {
            let piece = create_test_piece(i, piece_size);
            coordinator.active_pieces.insert(i as usize, piece.clone());
            coordinator.in_progress.push(piece.clone());
            coordinator.in_progress_timestamps.insert(i, Instant::now());
        }

        // Release all pieces
        let result = coordinator.release_all_pieces().await;
        assert!(result.is_ok());

        // Assert all collections are empty
        assert!(coordinator.active_pieces.is_empty());
        assert!(coordinator.in_progress.is_empty());
        assert!(coordinator.in_progress_timestamps.is_empty());

        // Verify the event was sent
        let event = tokio::time::timeout(Duration::from_millis(100), event_rx.recv()).await;
        assert!(event.is_ok());
        let event = event.unwrap();
        assert!(
            matches!(event, Some(PeerSessionEvent::UnassignPieces { pieces_index }) if pieces_index.len() == 3)
        );
    }

    #[tokio::test]
    async fn test_check_timeouts() {
        // Test checking for timeouts
        let id = "test_session".to_string();
        let piece_size = 32768;
        let block_size = 16384;
        let buffer_size = 100;
        let (event_tx, mut event_rx) = mpsc::channel::<PeerSessionEvent>(10);

        // Configure a very short timeout for testing
        let mut config = CoordinatorConfig::default();
        config.timeout_threshold = 1; // 1 second timeout

        let mut coordinator = Coordinator::new(
            id.clone(),
            piece_size,
            block_size,
            buffer_size,
            event_tx,
            config.clone(),
        );

        // Add a piece and set its timestamp to be older than the timeout
        let piece_index = 1;
        let piece = create_test_piece(piece_index, piece_size);
        coordinator
            .active_pieces
            .insert(piece_index as usize, piece.clone());
        coordinator.in_progress.push(piece.clone());

        // Set timestamp to be in the past (timeout duration + 1 second)
        let old_timestamp = Instant::now() - Duration::from_secs(config.timeout_threshold + 1);
        coordinator
            .in_progress_timestamps
            .insert(piece_index, old_timestamp);

        // Check timeouts
        let result = coordinator.check_timeouts().await;
        assert!(result.is_ok());

        // Assert the piece was removed
        assert!(!coordinator
            .in_progress
            .iter()
            .any(|p| p.index() == piece_index));
        assert!(!coordinator
            .in_progress_timestamps
            .contains_key(&piece_index));

        // Assert a strike was added
        assert_eq!(coordinator.strikes, 1);

        // Verify the UnassignPiece event was sent
        let event = tokio::time::timeout(Duration::from_millis(100), event_rx.recv()).await;
        assert!(event.is_ok());
        let event = event.unwrap();
        assert!(
            matches!(event, Some(PeerSessionEvent::UnassignPiece { piece_index: p }) if p == piece_index)
        );
    }

    #[tokio::test]
    async fn test_check_timeouts_strikeout() {
        // Test checking for timeouts with strikes exceeding max
        let id = "test_session".to_string();
        let piece_size = 32768;
        let block_size = 16384;
        let buffer_size = 100;
        let (event_tx, mut event_rx) = mpsc::channel::<PeerSessionEvent>(10);

        // Configure a very short timeout and low max strikes
        let mut config = CoordinatorConfig::default();
        config.timeout_threshold = 1; // 1 second timeout
        config.max_strikes = 2;

        let mut coordinator = Coordinator::new(
            id.clone(),
            piece_size,
            block_size,
            buffer_size,
            event_tx,
            config.clone(),
        );

        // Set strikes to be just before the max
        coordinator.strikes = config.max_strikes - 1;

        // Add a piece and set its timestamp to be older than the timeout
        let piece_index = 1;
        let piece = create_test_piece(piece_index, piece_size);
        coordinator
            .active_pieces
            .insert(piece_index as usize, piece.clone());
        coordinator.in_progress.push(piece.clone());

        // Set timestamp to be in the past
        let old_timestamp = Instant::now() - Duration::from_secs(config.timeout_threshold + 1);
        coordinator
            .in_progress_timestamps
            .insert(piece_index, old_timestamp);

        // Check timeouts
        let result = coordinator.check_timeouts().await;
        assert!(result.is_ok());

        // Assert strikes exceeds max
        assert!(coordinator.strikes >= config.max_strikes);

        // Verify the UnassignPiece event was sent first
        let event = tokio::time::timeout(Duration::from_millis(100), event_rx.recv()).await;
        assert!(event.is_ok());
        let event = event.unwrap();
        assert!(matches!(
            event,
            Some(PeerSessionEvent::UnassignPiece { .. })
        ));

        // Then verify the Shutdown event was sent
        let event = tokio::time::timeout(Duration::from_millis(100), event_rx.recv()).await;
        assert!(event.is_ok());
        let event = event.unwrap();
        assert!(matches!(event, Some(PeerSessionEvent::Shutdown)));
    }

    #[tokio::test]
    async fn test_handle_command() {
        // Test handling various commands
        let id = "test_session".to_string();
        let piece_size = 32768;
        let block_size = 16384;
        let buffer_size = 100;
        let (event_tx, _event_rx) = mpsc::channel::<PeerSessionEvent>(10);
        let config = CoordinatorConfig::default();

        let mut coordinator = Coordinator::new(
            id.clone(),
            piece_size,
            block_size,
            buffer_size,
            event_tx,
            config,
        );

        let (requester_tx, _) = mpsc::channel::<Piece>(1);

        // Test handle Unchoked command
        coordinator.is_choked = true;
        let result = coordinator
            .handle_command(CoordinatorCommand::Unchoked, &requester_tx)
            .await;
        assert!(result.is_ok());
        assert!(!coordinator.is_choked);

        // Test handle Choked command
        coordinator.is_choked = false;
        let result = coordinator
            .handle_command(CoordinatorCommand::Choked, &requester_tx)
            .await;
        assert!(result.is_ok());
        assert!(coordinator.is_choked);

        // Test handle AddPiece command
        let piece = create_test_piece(1, piece_size);
        let result = coordinator
            .handle_command(
                CoordinatorCommand::AddPiece {
                    piece: piece.clone(),
                },
                &requester_tx,
            )
            .await;
        assert!(result.is_ok());
        assert_eq!(coordinator.queue.len(), 1);
        assert_eq!(coordinator.queue[0].index(), piece.index());
    }

    #[tokio::test]
    async fn test_is_valid_piece() {
        // Test with matching hash
        let test_data = b"This is test data for SHA1 hashing";
        let mut hasher = Sha1::new();
        hasher.update(test_data);
        let hash = hasher.finalize();

        let mut hash_array = [0u8; 20];
        hash_array.copy_from_slice(&hash);

        assert!(is_valid_piece(hash_array, test_data));

        // Test with non-matching hash
        let wrong_data = b"This is different test data";
        assert!(!is_valid_piece(hash_array, wrong_data));
    }
}
