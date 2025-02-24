use protocol::error::ProtocolError;
use sha1::{Digest, Sha1};
use std::error::Error;
use std::fmt::Display;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{collections::HashMap, usize};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::torrent::events::{DiskEvent, Event, StateEvent};

use protocol::message::{Message, MessageId, PiecePayload, TransferPayload};
use protocol::piece::Piece;

const MAX_STRIKES: u8 = 3;

#[derive(Debug)]
pub struct Worker {
    id: String,
    state: State,
    peer_bitfield_received: bool,
    download_status: DownloadStatus,
    io_wtx: mpsc::Sender<Message>,
    disk_tx: Arc<mpsc::Sender<DiskEvent>>,
    orchestrator_tx: Arc<mpsc::Sender<Event>>,
}

#[derive(Debug)]
struct State {
    is_choked: bool,
    is_interested: bool,
    peer_choked: bool,
    peer_interested: bool,
}

#[derive(Debug)]
struct DownloadStatus {
    active_pieces: HashMap<usize, Piece>, // the status of the pieces that are being downloaded or have been downloaded from this peer
    // stores in memeory  the bytes from the corresponding piece
    queue: Vec<Piece>, // vector containing the pieces to be requested to the peer
    in_progress: Vec<Piece>, // vector containing the pieces that are being downloaded but stil not
    // completed
    in_progress_timestamps: HashMap<u32, Instant>, // timestamps of the las time a block os a piece
    // has been received from the peer
    strikes: u8, // number of times a in_progress piece has been removed from the download in progress pieces vector (in_progress) because the timeout has been reached
}

impl Default for State {
    fn default() -> Self {
        State {
            is_choked: true,
            is_interested: false,
            peer_choked: true,
            peer_interested: false,
        }
    }
}

impl Default for DownloadStatus {
    fn default() -> Self {
        DownloadStatus {
            active_pieces: HashMap::new(),
            queue: vec![],
            in_progress: vec![],
            in_progress_timestamps: HashMap::new(),
            strikes: 0,
        }
    }
}

impl Worker {
    pub fn new(
        id: String,
        io_wtx: mpsc::Sender<Message>,
        disk_tx: Arc<mpsc::Sender<DiskEvent>>,
        orchestrator_tx: Arc<mpsc::Sender<Event>>,
    ) -> Self {
        Self {
            id,
            state: State::default(),
            peer_bitfield_received: false,
            download_status: DownloadStatus::default(),
            io_wtx,
            disk_tx,
            orchestrator_tx,
        }
    }

    pub fn ready_to_request(&self) -> bool {
        !self.state.is_choked && self.state.is_interested && !self.download_status.queue.is_empty()
    }

    pub fn push_to_queue(&mut self, piece: Piece) {
        self.download_status.queue.push(piece);
    }

    pub fn has_received_peer_bitfield(&self) -> bool {
        self.peer_bitfield_received
    }

    pub fn has_pieces_in_progress(&self) -> bool {
        !self.download_status.in_progress.is_empty()
    }

    pub fn is_strikeout(&self) -> bool {
        self.download_status.strikes >= MAX_STRIKES
    }

    // release a piece in progress so they are no longuer assigned to the current worker
    pub async fn release_piece(
        &mut self,
        piece_index: u32,
    ) -> Result<(), mpsc::error::SendError<Event>> {
        self.download_status
            .in_progress
            .retain(|unconfirmed_piece| unconfirmed_piece.index() != piece_index);
        self.download_status
            .in_progress_timestamps
            .remove(&piece_index);

        self.orchestrator_tx
            .send(Event::State(StateEvent::UnassignPiece(piece_index)))
            .await
    }

    // release pieces in progress so they are no longuer assigned to the current worker
    pub async fn release_pieces(&mut self) -> Result<(), mpsc::error::SendError<Event>> {
        let piece_indexes: Vec<u32> = self
            .download_status
            .in_progress
            .iter()
            .map(|p| p.index())
            .collect();
        self.orchestrator_tx
            .send(Event::State(StateEvent::UnassignPieces(piece_indexes)))
            .await?;
        self.download_status.in_progress.clear();
        self.download_status.in_progress_timestamps.clear();

        Ok(())
    }

    pub async fn release_timed_out_pieces(&mut self) -> Result<(), WorkerError> {
        let now = Instant::now();
        let timed_out_pieces: Vec<Piece> = self
            .download_status
            .in_progress
            .iter()
            .filter(|piece| {
                if let Some(&request_time) = self
                    .download_status
                    .in_progress_timestamps
                    .get(&piece.index())
                {
                    now.duration_since(request_time) > Duration::new(15, 0)
                } else {
                    false
                }
            })
            .cloned()
            .collect();

        for timed_out_piece in timed_out_pieces.iter() {
            self.release_piece(timed_out_piece.index()).await?;
            self.download_status.strikes += 1;
        }

        Ok(())
    }

    pub fn are_active_pieces_finalized(&self) -> bool {
        self.download_status
            .active_pieces
            .iter()
            .all(|(_, piece)| piece.is_finalized())
    }

    pub async fn request_unchoke(&mut self) -> Result<(), WorkerError> {
        self.io_wtx
            .send(Message::new(MessageId::Unchoke, None))
            .await?;
        self.io_wtx
            .send(Message::new(MessageId::Interested, None))
            .await?;
        self.state.is_interested = true;

        Ok(())
    }

    pub async fn handle_message(&mut self, message: Message) -> Result<(), WorkerError> {
        match message.message_id {
            MessageId::KeepAlive => {
                // TODO: extend stream alive
            }
            MessageId::Choke => {
                self.state.is_choked = true;
                // TODO: logic in case peer chokes but client is still interested
            }
            MessageId::Unchoke => {
                self.state.is_choked = false;
            }
            MessageId::Interested => {
                self.state.peer_interested = true;

                // TODO: logic for unchoking peer
                if self.state.peer_choked {
                    self.state.peer_choked = false;
                    // notify peer it has been unchoked
                    self.io_wtx
                        .send(Message::new(MessageId::Unchoke, None))
                        .await?;
                }
            }
            MessageId::NotInterested => {
                self.state.peer_interested = false;
            }
            MessageId::Have => {
                self.process_have_message(&message).await?;
            }
            MessageId::Bitfield => {
                let bitfield = message.payload.ok_or(WorkerError::EmptyPayload)?;

                self.peer_bitfield_received = true;
                self.orchestrator_tx
                    .send(Event::State(StateEvent::PeerBitfield {
                        worker_id: self.id.clone(),
                        bitfield,
                    }))
                    .await?;
            }
            MessageId::Request => {
                // TODO:send piece blocks to peer
                unimplemented!()
            }
            MessageId::Piece => {
                self.download_piece_block(message.payload.as_deref())
                    .await?;
            }
            MessageId::Cancel => {
                unimplemented!()
            }
            MessageId::Port => {
                unimplemented!()
            }
        }

        Ok(())
    }

    async fn process_have_message(&mut self, message: &Message) -> Result<(), WorkerError> {
        let mut piece_index_be = [0u8; 4];
        piece_index_be.copy_from_slice(message.payload.as_ref().ok_or(WorkerError::EmptyPayload)?);
        let piece_index = u32::from_be_bytes(piece_index_be) as usize;

        self.orchestrator_tx
            .send(Event::State(StateEvent::PeerHave {
                worker_id: self.id.clone(),
                piece_index: piece_index as u32,
            }))
            .await?;

        Ok(())
    }

    async fn download_piece_block(&mut self, payload: Option<&[u8]>) -> Result<(), WorkerError> {
        let bytes = payload.ok_or(WorkerError::EmptyPayload)?;
        let payload = PiecePayload::deserialize(bytes)?;

        let piece = self
            .download_status
            .active_pieces
            .get_mut(&(payload.index as usize))
            .ok_or(WorkerError::PieceNotFound)?;

        debug!(piece_index= ?payload.index, block_offset= ?payload.begin, "Downloading piece");
        // Insert the received block in the piece bytes
        piece.add_block(payload.begin, payload.block.clone())?;
        // update last block insertion
        self.download_status
            .in_progress_timestamps
            .insert(piece.index(), Instant::now());

        // Check if the piece has all the blocks and is not already been processed and stored in disk
        if piece.is_ready() && !piece.is_finalized() {
            let assembled_blocks = piece.assemble()?;
            debug!(piece_index= ?payload.index, "Piece download completed. Verifying hash...");

            // Validate the piece hash
            if is_valid_piece(piece.hash(), &assembled_blocks) {
                debug!(piece_index= ?payload.index, "Piece hash verified. Sending to disk");
                self.disk_tx
                    .send(DiskEvent::Piece {
                        piece: piece.clone(),
                        assembled_blocks,
                    })
                    .await?;

                // remove the finalized piece from the in_progress list
                self.download_status
                    .in_progress
                    .retain(|p| p.index() != piece.index());

                // FIX: piece completion should be sent by the disk service
                self.orchestrator_tx
                    .send(Event::State(StateEvent::CompletedPiece(piece.index())))
                    .await?;
            } else {
                warn!(piece_index= ?payload.index, "Piece hash verification failed");
                self.orchestrator_tx
                    .send(Event::State(StateEvent::CorruptedPiece {
                        piece_index: piece.index(),
                        worker_id: self.id.clone(),
                    }))
                    .await?;

                return Err(WorkerError::CorruptedPiece(piece.index()));
            }
        }

        Ok(())
    }

    pub async fn request(&mut self) -> Result<(), WorkerError> {
        for piece in self.download_status.queue.clone().iter() {
            self.download_status
                .active_pieces
                .entry(piece.index() as usize)
                .or_insert_with(|| piece.clone());

            self.request_from_offset(piece, 0).await?;
        }

        Ok(())
    }

    /// Core logic to request blocks from a piece starting at a given offset
    async fn request_from_offset(
        &mut self,
        piece: &Piece,
        start_offset: u32,
    ) -> Result<(), WorkerError> {
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
        self.download_status.in_progress.push(piece.clone());
        self.download_status
            .in_progress_timestamps
            .insert(piece.index(), Instant::now());

        // remove the piece from the download queue
        self.download_status
            .queue
            .retain(|p| p.index() != piece.index());

        Ok(())
    }
}

fn is_valid_piece(piece_hash: [u8; 20], piece_bytes: &[u8]) -> bool {
    let mut hasher = Sha1::new();
    hasher.update(piece_bytes);
    let hash = hasher.finalize();

    piece_hash == hash.as_slice()
}

#[derive(Debug)]
pub enum WorkerError {
    Protocol(ProtocolError),
    EmptyPayload,
    PieceNotFound,
    CorruptedPiece(u32),
    EventTxError(mpsc::error::SendError<Event>),
    MessageTxError(mpsc::error::SendError<Message>),
    DiskEventTxError(mpsc::error::SendError<DiskEvent>),
}

impl Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkerError::Protocol(err) => write!(f, "Protocol error: {}", err),
            WorkerError::EmptyPayload => write!(f, "Received message with empty payload"),
            WorkerError::PieceNotFound => write!(f, "Piece not found in map"),
            WorkerError::CorruptedPiece(piece_index) => {
                write!(f, "Invalid hash found for piece with index {}", piece_index)
            }
            WorkerError::EventTxError(err) => {
                write!(f, "Failed to send event: {}", err)
            }
            WorkerError::MessageTxError(err) => {
                write!(f, "Failed to send message: {}", err)
            }
            WorkerError::DiskEventTxError(err) => {
                write!(f, "Failed to send event: {}", err)
            }
        }
    }
}

impl From<ProtocolError> for WorkerError {
    fn from(err: ProtocolError) -> Self {
        WorkerError::Protocol(err)
    }
}

impl From<mpsc::error::SendError<Event>> for WorkerError {
    fn from(err: mpsc::error::SendError<Event>) -> Self {
        WorkerError::EventTxError(err)
    }
}

impl From<mpsc::error::SendError<Message>> for WorkerError {
    fn from(err: mpsc::error::SendError<Message>) -> Self {
        WorkerError::MessageTxError(err)
    }
}

impl From<mpsc::error::SendError<DiskEvent>> for WorkerError {
    fn from(err: mpsc::error::SendError<DiskEvent>) -> Self {
        WorkerError::DiskEventTxError(err)
    }
}

impl Error for WorkerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            WorkerError::Protocol(err) => Some(err),
            WorkerError::EventTxError(err) => Some(err),
            WorkerError::MessageTxError(err) => Some(err),
            WorkerError::DiskEventTxError(err) => Some(err),
            _ => None,
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    use protocol::{
        message::{Message, MessageId},
        piece::Piece,
    };
    use tokio::time::timeout;

    use crate::torrent::{
        events::{Event, StateEvent},
        peers::worker::{Worker, MAX_STRIKES},
    };

    #[tokio::test]
    async fn test_worker_ready_to_request() {
        let (io_wtx, _) = tokio::sync::mpsc::channel(1);
        let (disk_tx, _) = tokio::sync::mpsc::channel(1);
        let (orchestrator_tx, _) = tokio::sync::mpsc::channel(1);

        let mut worker = Worker::new(
            "worker-id".to_string(),
            io_wtx,
            Arc::new(disk_tx),
            Arc::new(orchestrator_tx),
        );

        // Modify state to make it ready
        worker.state.is_choked = false;
        worker.state.is_interested = true;
        worker.download_status.queue = vec![Piece::new(0, 1024, [0u8; 20])];
        assert!(worker.ready_to_request());
    }

    #[tokio::test]
    async fn test_worker_not_ready_to_request() {
        let (io_wtx, _) = tokio::sync::mpsc::channel(1);
        let (disk_tx, _) = tokio::sync::mpsc::channel(1);
        let (orchestrator_tx, _) = tokio::sync::mpsc::channel(1);

        let mut worker = Worker::new(
            "worker-id".to_string(),
            io_wtx,
            Arc::new(disk_tx),
            Arc::new(orchestrator_tx),
        );

        // worker is still choked
        worker.state.is_choked = true;
        worker.state.is_interested = true;
        worker.download_status.queue = vec![Piece::new(0, 1024, [0u8; 20])];
        assert!(!worker.ready_to_request());

        // worker has no pieces in download
        worker.state.is_choked = false;
        worker.state.is_interested = true;
        worker.download_status.queue = vec![];
        assert!(!worker.ready_to_request());

        // worker has not expressed interes to peer
        worker.state.is_choked = false;
        worker.state.is_interested = false;
        worker.download_status.queue = vec![Piece::new(0, 1024, [0u8; 20])];
        assert!(!worker.ready_to_request());
    }

    #[tokio::test]
    async fn test_received_peer_bitfield() {
        let (io_wtx, _) = tokio::sync::mpsc::channel(1);
        let (disk_tx, _) = tokio::sync::mpsc::channel(1);
        let (orchestrator_tx, _) = tokio::sync::mpsc::channel(1);

        let mut worker = Worker::new(
            "worker-id".to_string(),
            io_wtx,
            Arc::new(disk_tx),
            Arc::new(orchestrator_tx),
        );

        worker.peer_bitfield_received = true;

        assert!(worker.has_received_peer_bitfield());
    }

    #[tokio::test]
    async fn test_peer_bitfield_not_received() {
        let (io_wtx, _) = tokio::sync::mpsc::channel(1);
        let (disk_tx, _) = tokio::sync::mpsc::channel(1);
        let (orchestrator_tx, _) = tokio::sync::mpsc::channel(1);

        let worker = Worker::new(
            "worker-id".to_string(),
            io_wtx,
            Arc::new(disk_tx),
            Arc::new(orchestrator_tx),
        );

        assert!(!worker.has_received_peer_bitfield());
    }

    #[tokio::test]
    async fn test_has_pieces_in_progress() {
        let (io_wtx, _) = tokio::sync::mpsc::channel(1);
        let (disk_tx, _) = tokio::sync::mpsc::channel(1);
        let (orchestrator_tx, _) = tokio::sync::mpsc::channel(1);

        let mut worker = Worker::new(
            "worker-id".to_string(),
            io_wtx,
            Arc::new(disk_tx),
            Arc::new(orchestrator_tx),
        );

        worker.download_status.in_progress = vec![Piece::new(1, 1024, [0u8; 20])];

        assert!(worker.has_pieces_in_progress());
    }

    #[tokio::test]
    async fn test_has_no_pieces_in_progress() {
        let (io_wtx, _) = tokio::sync::mpsc::channel(1);
        let (disk_tx, _) = tokio::sync::mpsc::channel(1);
        let (orchestrator_tx, _) = tokio::sync::mpsc::channel(1);

        let mut worker = Worker::new(
            "worker-id".to_string(),
            io_wtx,
            Arc::new(disk_tx),
            Arc::new(orchestrator_tx),
        );

        worker.download_status.in_progress = vec![];

        assert!(!worker.has_pieces_in_progress());
    }

    #[tokio::test]
    async fn test_is_strikeout() {
        let (io_wtx, _) = tokio::sync::mpsc::channel(1);
        let (disk_tx, _) = tokio::sync::mpsc::channel(1);
        let (orchestrator_tx, _) = tokio::sync::mpsc::channel(1);

        let mut worker = Worker::new(
            "worker-id".to_string(),
            io_wtx,
            Arc::new(disk_tx),
            Arc::new(orchestrator_tx),
        );

        worker.download_status.strikes = MAX_STRIKES;

        assert!(worker.is_strikeout());
    }

    #[tokio::test]
    async fn test_release_timed_out_pieces() {
        let (io_wtx, _) = tokio::sync::mpsc::channel(1);
        let (disk_tx, _) = tokio::sync::mpsc::channel(1);
        let (orchestrator_tx, mut orchestrator_rx) = tokio::sync::mpsc::channel(2);

        let mut worker = Worker::new(
            "worker-id".to_string(),
            io_wtx,
            Arc::new(disk_tx),
            Arc::new(orchestrator_tx),
        );

        worker.download_status.in_progress = vec![
            Piece::new(1, 1024, [0u8; 20]),
            Piece::new(2, 1024, [0u8; 20]),
            Piece::new(4, 1024, [0u8; 20]),
            Piece::new(8, 1024, [0u8; 20]),
        ];

        worker
            .download_status
            .in_progress_timestamps
            .insert(1, Instant::now() - Duration::from_secs(15));
        worker
            .download_status
            .in_progress_timestamps
            .insert(2, Instant::now() - Duration::from_secs(4));
        worker
            .download_status
            .in_progress_timestamps
            .insert(4, Instant::now() - Duration::from_secs(5));
        worker
            .download_status
            .in_progress_timestamps
            .insert(8, Instant::now() - Duration::from_secs(18));

        assert_eq!(worker.download_status.strikes, 0);

        worker.release_timed_out_pieces().await.unwrap();

        assert_eq!(worker.download_status.strikes, 2);
        assert_eq!(
            worker.download_status.in_progress,
            vec![
                Piece::new(2, 1024, [0u8; 20]),
                Piece::new(4, 1024, [0u8; 20]),
            ]
        );
        let mut remaining_keys = worker
            .download_status
            .in_progress_timestamps
            .keys()
            .collect::<Vec<&u32>>();

        // sorting for consistency
        remaining_keys.sort();
        assert_eq!(remaining_keys, vec![&2, &4]);

        // Ensure the correct event was sent
        match orchestrator_rx.recv().await {
            Some(Event::State(StateEvent::UnassignPiece(index))) => {
                assert_eq!(index, 1);
            }
            _ => {
                panic!("Expected `UnassignPiece` event was not received");
            }
        }
    }

    #[tokio::test]
    async fn test_release_piece() {
        let (io_wtx, _) = tokio::sync::mpsc::channel(1);
        let (disk_tx, _) = tokio::sync::mpsc::channel(1);
        let (orchestrator_tx, mut orchestrator_rx) = tokio::sync::mpsc::channel(1);

        let mut worker = Worker::new(
            "worker-id".to_string(),
            io_wtx,
            Arc::new(disk_tx),
            Arc::new(orchestrator_tx),
        );

        worker.download_status.in_progress = vec![
            Piece::new(1, 1024, [0u8; 20]),
            Piece::new(2, 1024, [0u8; 20]),
        ];

        worker
            .download_status
            .in_progress_timestamps
            .insert(1, Instant::now());
        worker
            .download_status
            .in_progress_timestamps
            .insert(2, Instant::now());

        let released_piece_index = 1;
        worker.release_piece(released_piece_index).await.unwrap();

        // Ensure piece is removed
        assert!(!worker
            .download_status
            .in_progress
            .iter()
            .any(|p| p.index() == released_piece_index));
        assert!(!worker
            .download_status
            .in_progress_timestamps
            .contains_key(&released_piece_index));

        // Ensure the correct event was sent
        match orchestrator_rx.recv().await {
            Some(Event::State(StateEvent::UnassignPiece(index))) => {
                assert_eq!(index, 1);
            }
            _ => {
                panic!("Expected `UnassignPiece` event was not received");
            }
        }
    }

    #[tokio::test]
    async fn test_release_pieces() {
        let (io_wtx, _) = tokio::sync::mpsc::channel(1);
        let (disk_tx, _) = tokio::sync::mpsc::channel(1);
        let (orchestrator_tx, mut orchestrator_rx) = tokio::sync::mpsc::channel(1);

        let mut worker = Worker::new(
            "worker-id".to_string(),
            io_wtx,
            Arc::new(disk_tx),
            Arc::new(orchestrator_tx),
        );

        worker.download_status.in_progress = vec![
            Piece::new(1, 1024, [0u8; 20]),
            Piece::new(2, 1024, [0u8; 20]),
            Piece::new(3, 1024, [0u8; 20]),
        ];

        worker
            .download_status
            .in_progress_timestamps
            .insert(1, Instant::now());
        worker
            .download_status
            .in_progress_timestamps
            .insert(2, Instant::now());
        worker
            .download_status
            .in_progress_timestamps
            .insert(3, Instant::now());

        worker.release_pieces().await.unwrap();

        // Ensure piece is removed
        assert!(worker.download_status.in_progress.is_empty());
        assert!(worker.download_status.in_progress_timestamps.is_empty());

        // Ensure the correct event was sent
        match orchestrator_rx.recv().await {
            Some(Event::State(StateEvent::UnassignPieces(indexes))) => {
                assert_eq!(indexes, vec![1, 2, 3]);
            }
            _ => {
                panic!("Expected `UnassignPieces` event was not received");
            }
        }
    }

    #[tokio::test]
    async fn test_are_active_pieces_finalized() {
        let (io_wtx, _) = tokio::sync::mpsc::channel(1);
        let (disk_tx, _) = tokio::sync::mpsc::channel(1);
        let (orchestrator_tx, _) = tokio::sync::mpsc::channel(1);

        let mut worker = Worker::new(
            "worker-id".to_string(),
            io_wtx,
            Arc::new(disk_tx),
            Arc::new(orchestrator_tx),
        );

        let mut piece1 = Piece::new(1, 1024, [0u8; 20]);
        piece1.mark_finalized();
        let mut piece2 = Piece::new(1, 1024, [0u8; 20]);
        piece2.mark_finalized();
        let mut piece3 = Piece::new(1, 1024, [0u8; 20]);
        piece3.mark_finalized();

        worker.download_status.active_pieces.insert(1, piece1);
        worker.download_status.active_pieces.insert(2, piece2);
        worker.download_status.active_pieces.insert(3, piece3);

        assert!(worker.are_active_pieces_finalized());
    }

    #[tokio::test]
    async fn test_are_active_pieces_not_finalized() {
        let (io_wtx, _) = tokio::sync::mpsc::channel(1);
        let (disk_tx, _) = tokio::sync::mpsc::channel(1);
        let (orchestrator_tx, _) = tokio::sync::mpsc::channel(1);

        let mut worker = Worker::new(
            "worker-id".to_string(),
            io_wtx,
            Arc::new(disk_tx),
            Arc::new(orchestrator_tx),
        );

        let mut piece1 = Piece::new(1, 1024, [0u8; 20]);
        piece1.mark_finalized();
        let piece2 = Piece::new(1, 1024, [0u8; 20]);
        let mut piece3 = Piece::new(1, 1024, [0u8; 20]);
        piece3.mark_finalized();

        worker.download_status.active_pieces.insert(1, piece1);
        worker.download_status.active_pieces.insert(2, piece2);
        worker.download_status.active_pieces.insert(3, piece3);

        assert!(!worker.are_active_pieces_finalized());
    }

    #[tokio::test]
    async fn test_process_have_message() {
        let (io_wtx, _) = tokio::sync::mpsc::channel(1);
        let (disk_tx, _) = tokio::sync::mpsc::channel(1);
        let (orchestrator_tx, mut orchestrator_rx) = tokio::sync::mpsc::channel(1);

        let worker_id = "worker-id".to_string();
        let mut worker = Worker::new(
            worker_id.clone(),
            io_wtx,
            Arc::new(disk_tx),
            Arc::new(orchestrator_tx),
        );

        let piece_index: u32 = 1;
        worker
            .process_have_message(&Message::new(
                MessageId::Have,
                Some(piece_index.to_be_bytes().to_vec()),
            ))
            .await
            .unwrap();

        // Ensure the event is sent
        match orchestrator_rx.recv().await {
            Some(event) => {
                assert_eq!(
                    event,
                    Event::State(StateEvent::PeerHave {
                        worker_id,
                        piece_index
                    })
                );
            }
            _ => {
                panic!("Expected `UnassignPieces` event was not received");
            }
        }
    }

    // TODO: test download_piece_block

    #[tokio::test]
    async fn test_request() {
        let (io_wtx, mut io_wrx) = tokio::sync::mpsc::channel(2);
        let (disk_tx, _) = tokio::sync::mpsc::channel(1);
        let (orchestrator_tx, _) = tokio::sync::mpsc::channel(1);

        let worker_id = "worker-id".to_string();
        let mut worker = Worker::new(
            worker_id.clone(),
            io_wtx,
            Arc::new(disk_tx),
            Arc::new(orchestrator_tx),
        );

        // Create a dummy Piece object
        let piece_index = 0;
        let piece_size = 32768; // 2 blocks of 16KB each
        let piece_hash = [0u8; 20];
        let piece = Piece::new(piece_index, piece_size, piece_hash);

        // Pre-fill the pieces queue with the piece
        worker.download_status.queue.push(piece);

        // Call the request method
        worker.request().await.expect("request method failed");

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
        let (io_wtx, mut io_wrx) = tokio::sync::mpsc::channel(2);
        let (disk_tx, _) = tokio::sync::mpsc::channel(1);
        let (orchestrator_tx, _) = tokio::sync::mpsc::channel(1);

        let worker_id = "worker-id".to_string();
        let mut worker = Worker::new(
            worker_id.clone(),
            io_wtx,
            Arc::new(disk_tx),
            Arc::new(orchestrator_tx),
        );

        // Create a dummy Piece object
        let piece_index = 0;
        let piece_size = 32768; // 2 blocks of 16KB each
        let piece_hash = [0u8; 20];
        let piece = Piece::new(piece_index, piece_size, piece_hash);

        // Request blocks starting from offset 0
        let start_offset = 0;
        worker
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

    #[tokio::test]
    async fn test_handle_message_choke() {
        let (io_wtx, _) = tokio::sync::mpsc::channel(1);
        let (disk_tx, _) = tokio::sync::mpsc::channel(1);
        let (orchestrator_tx, _) = tokio::sync::mpsc::channel(1);

        let mut worker = Worker::new(
            "worker-id".to_string(),
            io_wtx,
            Arc::new(disk_tx),
            Arc::new(orchestrator_tx),
        );

        worker.state.is_choked = false;
        worker
            .handle_message(Message::new(MessageId::Choke, None))
            .await
            .unwrap();

        // the worker has been  choked by the peer
        assert!(worker.state.is_choked);
    }

    #[tokio::test]
    async fn test_handle_message_unchoke() {
        let (io_wtx, _) = tokio::sync::mpsc::channel(1);
        let (disk_tx, _) = tokio::sync::mpsc::channel(1);
        let (orchestrator_tx, _) = tokio::sync::mpsc::channel(1);

        let mut worker = Worker::new(
            "worker-id".to_string(),
            io_wtx,
            Arc::new(disk_tx),
            Arc::new(orchestrator_tx),
        );

        assert!(worker.state.is_choked);

        worker
            .handle_message(Message::new(MessageId::Unchoke, None))
            .await
            .unwrap();

        // the worker has been  unchoked by the peer
        assert!(!worker.state.is_choked);
    }

    #[tokio::test]
    async fn test_handle_message_interested() {
        let (io_wtx, mut io_wrx) = tokio::sync::mpsc::channel(1);
        let (disk_tx, _) = tokio::sync::mpsc::channel(1);
        let (orchestrator_tx, _) = tokio::sync::mpsc::channel(1);

        let mut worker = Worker::new(
            "worker-id".to_string(),
            io_wtx,
            Arc::new(disk_tx),
            Arc::new(orchestrator_tx),
        );

        assert!(!worker.state.peer_interested);
        assert!(worker.state.is_choked);

        worker
            .handle_message(Message::new(MessageId::Interested, None))
            .await
            .unwrap();

        // the peer is interested and unchoked
        assert!(worker.state.peer_interested);
        assert!(!worker.state.peer_choked);

        // Ensure the event is sent
        match io_wrx.recv().await {
            Some(message) => {
                assert_eq!(message, Message::new(MessageId::Unchoke, None));
            }
            _ => {
                panic!("Expected `UnassignPieces` event was not received");
            }
        }
    }

    #[tokio::test]
    async fn test_handle_message_not_interested() {
        let (io_wtx, _) = tokio::sync::mpsc::channel(1);
        let (disk_tx, _) = tokio::sync::mpsc::channel(1);
        let (orchestrator_tx, _) = tokio::sync::mpsc::channel(1);

        let mut worker = Worker::new(
            "worker-id".to_string(),
            io_wtx,
            Arc::new(disk_tx),
            Arc::new(orchestrator_tx),
        );

        worker.state.peer_interested = true;

        worker
            .handle_message(Message::new(MessageId::NotInterested, None))
            .await
            .unwrap();

        // the peer is not interested
        assert!(!worker.state.peer_interested);
    }

    #[tokio::test]
    async fn test_handle_message_bitfield() {
        let (io_wtx, _) = tokio::sync::mpsc::channel(1);
        let (disk_tx, _) = tokio::sync::mpsc::channel(1);
        let (orchestrator_tx, mut orchestrator_rx) = tokio::sync::mpsc::channel(1);

        let worker_id = "worker-id".to_string();
        let mut worker = Worker::new(
            worker_id.clone(),
            io_wtx,
            Arc::new(disk_tx),
            Arc::new(orchestrator_tx),
        );

        assert!(!worker.peer_bitfield_received);

        let bitfield = vec![0b10101010];
        worker
            .handle_message(Message::new(MessageId::Bitfield, Some(bitfield.clone())))
            .await
            .unwrap();

        assert!(worker.peer_bitfield_received);

        // Ensure the event is sent
        match orchestrator_rx.recv().await {
            Some(event) => {
                assert_eq!(
                    event,
                    Event::State(StateEvent::PeerBitfield {
                        worker_id,
                        bitfield
                    })
                );
            }
            _ => {
                panic!("Expected `PeerBitfield` event was not received");
            }
        }
    }
}
