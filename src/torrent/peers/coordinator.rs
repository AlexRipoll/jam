use std::{
    cmp::{max, min},
    collections::HashMap,
    error::Error,
    fmt::Display,
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

// Timeout checker intervals
const MIN_CHECK_INTERVAL: u64 = 1;
const MAX_CHECK_INTERVAL: u64 = 10;

#[derive(Debug)]
pub struct Coordinator {
    is_choked: bool,

    // the status of the pieces that are being downloaded or have been downloaded from this peer
    active_pieces: HashMap<usize, Piece>,

    // vector containing the pieces to be requested to the peer
    queue: Vec<Piece>,

    // vector containing the pieces that are being downloaded but stil not completed
    in_progress: Vec<Piece>,

    // timestamps of the las time a block os a piece
    in_progress_timestamps: HashMap<u32, Instant>,

    // max seconds for downloading a in progress piece
    timeout_threshold: u64,

    // number of times a in_progress piece has been removed from the download in progress pieces vector (in_progress) because the timeout has been reached
    strikes: u8,

    event_tx: mpsc::Sender<PeerSessionEvent>,

    // Keep track of whether a piece request is in progress
    request_in_progress: bool,
}

#[derive(Debug)]
pub enum CoordinatorCommand {
    Choked,
    Unchoked,
    AddPiece { piece: Piece },
    DownloadPiece { payload: Option<Vec<u8>> },

    ReleasePiece { piece_index: u32 },
    ReleasePieces,
    RequestDispatched { piece: Piece },
    CheckTimeouts,
}

impl Coordinator {
    pub fn new(timeout_threshold: u64, event_tx: mpsc::Sender<PeerSessionEvent>) -> Self {
        let timeout_threshold = if timeout_threshold == 0 {
            5
        } else {
            timeout_threshold
        };

        Self {
            is_choked: true,
            active_pieces: HashMap::new(),
            queue: vec![],
            in_progress: vec![],
            in_progress_timestamps: HashMap::new(),
            timeout_threshold,
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

        // Create a new task for timeout watching
        let interval = max(
            MIN_CHECK_INTERVAL,
            min(self.timeout_threshold / 5, MAX_CHECK_INTERVAL),
        );
        let timeout_cmd_tx = cmd_tx.clone();
        let timeout_handle = tokio::spawn(async move {
            loop {
                // Check for timeouts every X interval of seconds
                tokio::time::sleep(Duration::from_secs(interval)).await;

                // Send a command to check for timed out pieces
                if let Err(e) = timeout_cmd_tx.send(CoordinatorCommand::CheckTimeouts).await {
                    error!("Failed to send timeout check command: {}", e);
                }
            }
        });

        let requester_tx_clone = requester_tx.clone();

        // Main actor task
        let actor_handle = tokio::spawn(async move {
            // Store handle to ensure it is dropped properly when actor_handle completes
            let _request_handle = request_handle;
            let _timeout_handle = timeout_handle;

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
            CoordinatorCommand::ReleasePiece { piece_index } => {
                self.release_piece(piece_index).await?
            }
            CoordinatorCommand::ReleasePieces => self.release_pieces().await?,
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
            CoordinatorCommand::CheckTimeouts => {
                if self.has_pieces_in_progress() {
                    self.release_timed_out_pieces().await?;

                    // Check if we've reached max strikes
                    if self.is_strikeout() {
                        self.release_pieces().await?;
                    }
                }
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
                        piece_index: piece.index(),
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
        event_tx: &mpsc::Sender<PeerSessionEvent>,
    ) -> Result<(), CoordinatorError> {
        Self::request_from_offset(&piece, 0, cmd_tx, event_tx).await?;

        Ok(())
    }

    /// Core logic to request blocks from a piece starting at a given offset
    async fn request_from_offset(
        piece: &Piece,
        start_offset: u32,
        cmd_tx: &mpsc::Sender<CoordinatorCommand>,
        event_tx: &mpsc::Sender<PeerSessionEvent>,
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
                    now.duration_since(request_time) > Duration::new(self.timeout_threshold, 0)
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

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;
    use tokio::sync::mpsc;

    #[test]
    fn test_new_coordinator() {
        let (tx, _) = mpsc::channel::<PeerSessionEvent>(100);
        let coordinator = Coordinator::new(10, tx);

        assert!(coordinator.is_choked);
        assert!(coordinator.active_pieces.is_empty());
        assert!(coordinator.queue.is_empty());
        assert!(coordinator.in_progress.is_empty());
        assert!(coordinator.in_progress_timestamps.is_empty());
        assert_eq!(coordinator.timeout_threshold, 10);
        assert_eq!(coordinator.strikes, 0);
        assert!(!coordinator.request_in_progress);
    }

    #[tokio::test]
    async fn test_new_coordinator_with_zero_timeout() {
        let (tx, _) = mpsc::channel::<PeerSessionEvent>(100);
        let coordinator = Coordinator::new(0, tx);

        // Should use default of 5 seconds when 0 is provided
        assert_eq!(coordinator.timeout_threshold, 5);
    }

    #[tokio::test]
    async fn test_handle_choked_command() {
        let (event_tx, _) = mpsc::channel::<PeerSessionEvent>(100);
        let mut coordinator = Coordinator::new(10, event_tx);
        let (requester_tx, _) = mpsc::channel::<Piece>(1);

        coordinator.is_choked = false; // Start unchoked

        // Handle the choked command
        coordinator
            .handle_command(CoordinatorCommand::Choked, &requester_tx)
            .await
            .unwrap();

        assert!(coordinator.is_choked);
    }

    #[tokio::test]
    async fn test_handle_unchoked_command() {
        let (event_tx, _) = mpsc::channel::<PeerSessionEvent>(100);
        let mut coordinator = Coordinator::new(10, event_tx);
        let (requester_tx, _) = mpsc::channel::<Piece>(1);

        // Start choked
        assert!(coordinator.is_choked);

        // Handle the unchoked command
        coordinator
            .handle_command(CoordinatorCommand::Unchoked, &requester_tx)
            .await
            .unwrap();

        assert!(!coordinator.is_choked);
    }

    #[tokio::test]
    async fn test_add_piece_to_queue() {
        let (event_tx, _) = mpsc::channel::<PeerSessionEvent>(100);
        let mut coordinator = Coordinator::new(10, event_tx);
        let (requester_tx, mut requester_rx) = mpsc::channel::<Piece>(1);

        // assert that the queue is initially empty
        assert!(coordinator.queue.is_empty());

        // Create a test piece
        let piece = Piece::new(1, 16384, [0u8; 20]);

        // Add the piece to the queue
        coordinator
            .handle_command(
                CoordinatorCommand::AddPiece {
                    piece: piece.clone(),
                },
                &requester_tx,
            )
            .await
            .unwrap();

        // Piece should be in queue
        assert_eq!(coordinator.queue.len(), 1);
        assert_eq!(coordinator.queue[0].index(), piece.index());

        // Since we're choked, no piece should be sent to requester
        assert!(requester_rx.try_recv().is_err());

        // Now unchoke and add another piece
        coordinator
            .handle_command(CoordinatorCommand::Unchoked, &requester_tx)
            .await
            .unwrap();

        let piece2 = Piece::new(2, 16384, [0u8; 20]);
        coordinator
            .handle_command(
                CoordinatorCommand::AddPiece {
                    piece: piece2.clone(),
                },
                &requester_tx,
            )
            .await
            .unwrap();

        // Now we should receive a piece request
        let requested_piece = requester_rx.try_recv().unwrap();
        assert_eq!(requested_piece.index(), 1); // First piece should be requested
    }

    #[tokio::test]
    async fn test_request_dispatched() {
        let (event_tx, _) = mpsc::channel::<PeerSessionEvent>(100);
        let mut coordinator = Coordinator::new(10, event_tx);
        let (requester_tx, _) = mpsc::channel::<Piece>(1);

        // Create a test piece
        let piece = Piece::new(1, 16384, [0u8; 20]);

        // Add piece to the queue
        coordinator.queue.push(piece.clone());
        assert_eq!(coordinator.queue.len(), 1);

        // Mark piece as being requested
        coordinator.request_in_progress = true;

        // Handle request dispatched
        coordinator
            .handle_command(
                CoordinatorCommand::RequestDispatched {
                    piece: piece.clone(),
                },
                &requester_tx,
            )
            .await
            .unwrap();

        // Check piece state
        assert!(coordinator
            .active_pieces
            .contains_key(&(piece.index() as usize)));
        assert_eq!(coordinator.in_progress.len(), 1);
        assert_eq!(coordinator.in_progress[0].index(), piece.index());
        assert!(coordinator
            .in_progress_timestamps
            .contains_key(&piece.index()));
        // Check queue state
        assert!(coordinator.queue.is_empty());
        assert!(!coordinator.request_in_progress);

        // since the queue is empty no other piece is sent
    }

    #[tokio::test]
    async fn test_process_piece_message() {
        let (event_tx, mut event_rx) = mpsc::channel::<PeerSessionEvent>(100);
        let mut coordinator = Coordinator::new(10, event_tx);
        let (requester_tx, _) = mpsc::channel::<Piece>(1);

        // Create a piece payload
        let block_data = vec![1; 16384];
        let payload = PiecePayload {
            index: 1,
            begin: 0,
            block: block_data.clone(),
        };

        // Set the piece hash to match what is_valid_piece would compute
        let mut hasher = Sha1::new();
        hasher.update(&block_data);
        let hash = hasher.finalize();

        // Create a test piece and add it to active pieces
        let piece = Piece::new(1, 16384, hash.into());
        coordinator
            .active_pieces
            .insert(piece.index() as usize, piece.clone());
        coordinator.in_progress.push(piece.clone());

        let serialized = payload.serialize();

        // Process the piece message
        coordinator
            .handle_command(
                CoordinatorCommand::DownloadPiece {
                    payload: Some(serialized.clone()),
                },
                &requester_tx,
            )
            .await
            .unwrap();

        // In a real implementation, we would need to properly hash the data
        let active_piece = coordinator
            .active_pieces
            .get_mut(&(piece.index() as usize))
            .unwrap();

        assert!(active_piece.is_ready());
        assert!(!active_piece.is_finalized());

        // Since the piece is now complete, we should get a PieceAssembled event
        if let Some(PeerSessionEvent::PieceAssembled { piece_index, data }) =
            event_rx.try_recv().ok()
        {
            assert_eq!(piece_index, piece.index());
            assert_eq!(data.len(), piece.size());
        } else {
            panic!("Expected PieceAssembled event");
        }

        // The piece should be removed from in_progress
        assert!(coordinator.in_progress.is_empty());
    }
}
