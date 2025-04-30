use std::{error::Error, fmt::Display};

use protocol::{error::ProtocolError, piece::Piece};
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{debug, error, instrument, trace};

use crate::{
    config,
    torrent::{
        events::Event,
        peers::{coordinator::CoordinatorConfig, tcp::ConnectionConfig},
    },
};

use super::{
    coordinator::{Coordinator, CoordinatorCommand},
    handshake::{exchange_handshake, Handshake, HandshakeError},
    io,
    message::{Message, MessageId},
    tcp::{connect, TcpError},
};

/// Default channel capacity for peer session events
const DEFAULT_CHANNEL_CAPACITY: usize = 256;

/// Represents an active BitTorrent peer session
///
/// A PeerSession manages the full lifecycle of communication with a remote peer,
/// including handshaking, message exchanges, and piece downloading/uploading.
#[derive(Debug)]
pub struct PeerSession {
    /// Unique identifier for this session
    id: String,
    /// Peer ID (20-byte identifier)
    peer_id: [u8; 20],
    /// Info hash of the torrent
    info_hash: [u8; 20],
    /// Peer's network address
    peer_addr: String,
    /// Size of pieces in the torrent
    piece_size: usize,
    /// Size of individual blocks within pieces
    block_size: usize,
    /// Timeout threshold for requests in milliseconds
    timeout_threshold: u64,
    /// Whether we've received the peer's bitfield
    peer_bitfield_received: bool,
    /// Current connection state with the peer
    connection_state: ConnectionState,
    /// Channel for sending events to this peer session
    event_tx: mpsc::Sender<PeerSessionEvent>,
    /// Channel for receiving events from other components
    event_rx: mpsc::Receiver<PeerSessionEvent>,
    /// Channel for sending events to the orchestrator
    orchestrator_event_tx: mpsc::Sender<Event>,
}

/// Represents the current state of a BitTorrent peer connection
#[derive(Debug, Clone, Copy)]
struct ConnectionState {
    /// Whether we are choked by the peer (cannot request pieces)
    is_choked: bool,
    /// Whether we are interested in peer's pieces
    is_interested: bool,
    /// Whether we are choking the peer (they cannot request from us)
    peer_choked: bool,
    /// Whether the peer is interested in our pieces
    peer_interested: bool,
}

/// Events that can be sent to or from a peer session
#[derive(Debug)]
pub enum PeerSessionEvent {
    /// Notify the peer that we're interested in their pieces
    NotifyInterest,
    /// Assign a new piece to download from the peer
    AssignPiece { piece: Piece },
    /// Send a message to the peer
    PeerMessageOut { message: Message },
    /// Receive a message from the peer
    PeerMessageIn { message: Message },
    /// A piece has been fully assembled and verified
    PieceAssembled { piece_index: u32, data: Vec<u8> },
    /// Unassign a piece from this peer
    UnassignPiece { piece_index: u32 },
    /// Unassign multiple pieces from this peer
    UnassignPieces { pieces_index: Vec<u32> },
    /// A piece was downloaded but was corrupted
    PieceCorrupted { piece_index: u32 },
    /// The connection to the peer was closed
    ConnectionClosed,
    /// Shutdown this peer session
    Shutdown,
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self {
            is_choked: true,
            is_interested: false,
            peer_choked: true,
            peer_interested: false,
        }
    }
}

impl PeerSession {
    /// Create a new PeerSession with the given parameters
    pub fn new(
        id: String,
        peer_id: [u8; 20],
        info_hash: [u8; 20],
        peer_addr: String,
        piece_size: usize,
        block_size: usize,
        timeout_threshold: u64,
        orchestrator_event_tx: mpsc::Sender<Event>,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::channel::<PeerSessionEvent>(DEFAULT_CHANNEL_CAPACITY);

        PeerSession {
            id,
            peer_id,
            info_hash,
            peer_addr,
            piece_size,
            block_size,
            timeout_threshold,
            peer_bitfield_received: false,
            connection_state: ConnectionState::default(),
            event_tx,
            event_rx,
            orchestrator_event_tx,
        }
    }

    /// Start the peer session
    ///
    /// This method:
    /// 1. Establishes a TCP connection to the peer
    /// 2. Performs the BitTorrent handshake
    /// 3. Starts the IO handling tasks
    /// 4. Starts the coordinator task
    /// 5. Starts the main event loop
    ///
    /// # Returns
    /// - `Ok((event_tx, handle))` on success, where `event_tx` is a channel for sending events
    ///   to this peer session and `handle` is a join handle for the main task
    /// - `Err(PeerSessionError)` if any part of the setup fails
    #[instrument(skip(self), fields(session_id = %self.id, peer_addr = %self.peer_addr))]
    pub async fn run(
        mut self,
    ) -> Result<(mpsc::Sender<PeerSessionEvent>, JoinHandle<()>), PeerSessionError> {
        debug!("Starting peer session");

        let connection_config = ConnectionConfig {
            timeout_ms: 3000,
            max_retries: 1,
            retry_delay_ms: 100,
        };

        // Connect to peer with timeout
        let mut stream = match connect(&self.peer_addr, connection_config).await {
            Ok(stream) => stream,
            Err(err) => {
                return Err(PeerSessionError::Connection(err));
            }
        };

        // Perform handshake with timeout
        let handshake_metadata = Handshake::new(self.info_hash, self.peer_id);
        // Perform handshake
        // TODO: add timeout
        if let Err(err) = exchange_handshake(&mut stream, &handshake_metadata).await {
            return Err(PeerSessionError::Handshake(err));
        }

        let event_tx = self.event_tx.clone();

        // Run the IO handlers
        let (io_out_tx, io_in_handle, io_out_handle) = io::run(stream, event_tx.clone()).await;

        // TODO: make coordinator config congigurable from main config
        let coordinator_config = CoordinatorConfig::default();
        let id = self.id.clone();
        // Initialize and run the coordinator
        let coordinator = Coordinator::new(
            id,
            self.piece_size,
            self.block_size,
            256, // TODO: inject from config
            event_tx.clone(),
            coordinator_config,
        );
        let (coordinator_tx, coordinator_handle) = coordinator.run();

        let actor_handle = tokio::spawn(async move {
            debug!(
                session_id = %self.id,
                peer_addr = %self.peer_addr,
                "Starting event loop"
            );

            // Process events until channel closes or shutdown event
            while let Some(event) = self.event_rx.recv().await {
                if let Err(e) = self.handle_event(event, &io_out_tx, &coordinator_tx).await {
                    error!(
                        session_id = %self.id,
                        peer_addr = %self.peer_addr,
                        error = %e,
                        "Error handling peer session event"
                    );

                    // For critical errors, clean up and break
                    if e.is_critical() {
                        self.shutdown_session(io_in_handle, io_out_handle, coordinator_handle)
                            .await;
                        break;
                    }
                }
            }

            debug!(
                session_id = %self.id,
                peer_addr = %self.peer_addr,
                "Event loop terminated"
            );
        });

        debug!("Peer session started successfully");
        Ok((event_tx, actor_handle))
    }

    async fn handle_event(
        &mut self,
        event: PeerSessionEvent,
        io_out_tx: &mpsc::Sender<Message>,
        coordinator_tx: &mpsc::Sender<CoordinatorCommand>,
    ) -> Result<(), PeerSessionError> {
        match event {
            PeerSessionEvent::NotifyInterest => {
                self.handle_notify_interest(io_out_tx).await?;
            }
            PeerSessionEvent::AssignPiece { piece } => {
                self.handle_assign_piece(piece, coordinator_tx).await?;
            }
            PeerSessionEvent::PeerMessageIn { message } => {
                self.handle_peer_message(message, io_out_tx, coordinator_tx)
                    .await?;
            }
            PeerSessionEvent::PeerMessageOut { message } => {
                self.send_message_to_peer(message, io_out_tx).await?;
            }
            PeerSessionEvent::PieceAssembled { piece_index, data } => {
                self.handle_piece_assembled(piece_index, data).await?;
            }
            PeerSessionEvent::PieceCorrupted { piece_index } => {
                self.handle_piece_corrupted(piece_index).await?;
            }
            PeerSessionEvent::UnassignPiece { piece_index } => {
                self.handle_unassign_piece(piece_index).await?;
            }
            PeerSessionEvent::UnassignPieces { pieces_index } => {
                self.handle_unassign_pieces(pieces_index).await?;
            }
            PeerSessionEvent::Shutdown => {
                return Err(PeerSessionError::ShutdownRequested);
            }
            PeerSessionEvent::ConnectionClosed => {
                self.handle_connection_closed().await?;
            }
        }

        Ok(())
    }

    /// Handle a notify interest event by sending interested and unchoke messages
    async fn handle_notify_interest(
        &mut self,
        io_out_tx: &mpsc::Sender<Message>,
    ) -> Result<(), PeerSessionError> {
        debug!(
            session_id = %self.id,
            peer_addr = %self.peer_addr,
            "Notifying interest to peer"
        );

        // Send interested message
        self.send_message_to_peer(Message::new(MessageId::Interested, None), io_out_tx)
            .await?;

        // Send unchoke message
        // self.send_message_to_peer(Message::new(MessageId::Unchoke, None), io_out_tx)
        //     .await?;

        Ok(())
    }

    /// Send a message to the peer through the IO channel
    async fn send_message_to_peer(
        &self,
        message: Message,
        io_out_tx: &mpsc::Sender<Message>,
    ) -> Result<(), PeerSessionError> {
        debug!(
            session_id = %self.id,
            peer_addr = %self.peer_addr,
            message_id = ?message.message_id,
            "Sending message to peer"
        );

        io_out_tx
            .send(message)
            .await
            .map_err(|_| PeerSessionError::IoChannelClosed)?;

        Ok(())
    }

    /// Handle an assign piece event by forwarding to the coordinator
    async fn handle_assign_piece(
        &mut self,
        piece: Piece,
        coordinator_tx: &mpsc::Sender<CoordinatorCommand>,
    ) -> Result<(), PeerSessionError> {
        debug!(
            session_id = %self.id,
            peer_addr = %self.peer_addr,
            piece_index = piece.index(),
            "Piece assigned"
        );

        coordinator_tx
            .send(CoordinatorCommand::AddPiece { piece })
            .await
            .map_err(|_| PeerSessionError::CoordinatorChannelClosed)?;

        Ok(())
    }

    /// Handle a piece assembled event by forwarding to the orchestrator
    async fn handle_piece_assembled(
        &mut self,
        piece_index: u32,
        data: Vec<u8>,
    ) -> Result<(), PeerSessionError> {
        debug!(
            session_id = %self.id,
            peer_addr = %self.peer_addr,
            piece_index = piece_index,
            data_size = data.len(),
            "Piece assembled"
        );

        self.orchestrator_event_tx
            .send(Event::PieceAssembled { piece_index, data })
            .await
            .map_err(|_| PeerSessionError::OrchestratorChannelClosed)?;

        Ok(())
    }

    /// Handle a piece corrupted event by forwarding to the orchestrator
    async fn handle_piece_corrupted(&mut self, piece_index: u32) -> Result<(), PeerSessionError> {
        debug!(
            session_id = %self.id,
            peer_addr = %self.peer_addr,
            piece_index = piece_index,
            "Piece corrupted"
        );

        self.orchestrator_event_tx
            .send(Event::PieceCorrupted {
                session_id: self.id.clone(),
                piece_index,
            })
            .await
            .map_err(|_| PeerSessionError::OrchestratorChannelClosed)?;

        Ok(())
    }

    /// Handle an unassign piece event by forwarding to the orchestrator
    async fn handle_unassign_piece(&mut self, piece_index: u32) -> Result<(), PeerSessionError> {
        debug!(
            session_id = %self.id,
            peer_addr = %self.peer_addr,
            piece_index = piece_index,
            "Unassigning piece"
        );

        self.orchestrator_event_tx
            .send(Event::PieceUnassign {
                session_id: self.id.clone(),
                piece_index,
            })
            .await
            .map_err(|_| PeerSessionError::OrchestratorChannelClosed)?;

        Ok(())
    }

    /// Handle an unassign pieces event by forwarding to the orchestrator
    async fn handle_unassign_pieces(
        &mut self,
        pieces_index: Vec<u32>,
    ) -> Result<(), PeerSessionError> {
        debug!(
            session_id = %self.id,
            peer_addr = %self.peer_addr,
            num_pieces = pieces_index.len(),
            "Unassigning multiple pieces"
        );

        self.orchestrator_event_tx
            .send(Event::PieceUnassignMany {
                session_id: self.id.clone(),
                pieces_index,
            })
            .await
            .map_err(|_| PeerSessionError::OrchestratorChannelClosed)?;

        Ok(())
    }

    /// Handle a connection closed event by forwarding to the orchestrator
    async fn handle_connection_closed(&mut self) -> Result<(), PeerSessionError> {
        debug!(
            session_id = %self.id,
            peer_addr = %self.peer_addr,
            "Connection closed"
        );

        self.orchestrator_event_tx
            .send(Event::PeerSessionClosed {
                session_id: self.id.clone(),
            })
            .await
            .map_err(|_| PeerSessionError::OrchestratorChannelClosed)?;

        Ok(())
    }

    /// Shutdown the peer session and all associated tasks
    async fn shutdown_session(
        &self,
        io_in_handle: JoinHandle<()>,
        io_out_handle: JoinHandle<()>,
        coordinator_handle: JoinHandle<()>,
    ) {
        debug!(
            session_id = %self.id,
            peer_addr = %self.peer_addr,
            "Shutting down peer session"
        );

        // Attempt to notify orchestrator of disconnection
        let _ = self
            .orchestrator_event_tx
            .send(Event::DisconnectPeerSession {
                session_id: self.id.clone(),
            })
            .await;

        // Abort all session-related tasks
        io_in_handle.abort();
        io_out_handle.abort();
        coordinator_handle.abort();
    }

    /// Handle a message received from the peer
    #[instrument(skip(self, message, io_out_tx, coordinator_tx), fields(session_id = %self.id, peer_addr = %self.peer_addr, message_type = ?message.message_id))]
    pub async fn handle_peer_message(
        &mut self,
        message: Message,
        io_out_tx: &mpsc::Sender<Message>,
        coordinator_tx: &mpsc::Sender<CoordinatorCommand>,
    ) -> Result<(), PeerSessionError> {
        match message.message_id {
            MessageId::KeepAlive => {
                trace!("KeepAlive message received");
                // No action needed
            }
            MessageId::Choke => {
                debug!("Choked by peer");
                coordinator_tx
                    .send(CoordinatorCommand::Choked)
                    .await
                    .map_err(|_| PeerSessionError::CoordinatorChannelClosed)?;
                self.connection_state.is_choked = true;
            }
            MessageId::Unchoke => {
                debug!("Unchoked by peer");
                coordinator_tx
                    .send(CoordinatorCommand::Unchoked)
                    .await
                    .map_err(|_| PeerSessionError::CoordinatorChannelClosed)?;
                self.connection_state.is_choked = false;
            }
            MessageId::Interested => {
                debug!("Peer interested");
                self.connection_state.peer_interested = true;

                // If peer is choked, unchoke them
                if self.connection_state.peer_choked {
                    debug!("Unchoking peer");
                    self.connection_state.peer_choked = false;
                    self.send_message_to_peer(Message::new(MessageId::Unchoke, None), io_out_tx)
                        .await?;
                }
            }
            MessageId::NotInterested => {
                debug!("Peer not interested");
                self.connection_state.peer_interested = false;
            }
            MessageId::Have => {
                self.process_have_message(&message).await?;
            }
            MessageId::Bitfield => {
                let bitfield = message.payload.ok_or(PeerSessionError::EmptyPayload(
                    "Bitfield message without payload".into(),
                ))?;

                debug!(bitfield_size = bitfield.len(), "Received Bitfield message");

                self.peer_bitfield_received = true;
                self.orchestrator_event_tx
                    .send(Event::PeerBitfield {
                        session_id: self.id.clone(),
                        bitfield,
                    })
                    .await
                    .map_err(|_| PeerSessionError::OrchestratorChannelClosed)?;
            }
            MessageId::Request => {
                debug!("Peer requested a piece");
                // TODO: Handle peer requests by sending pieces
                return Err(PeerSessionError::NotImplemented(
                    "Request message handling".into(),
                ));
            }
            MessageId::Piece => {
                debug!("Piece data received");
                coordinator_tx
                    .send(CoordinatorCommand::DownloadPiece {
                        payload: message.payload,
                    })
                    .await
                    .map_err(|_| PeerSessionError::CoordinatorChannelClosed)?;

                debug!("Piece data sent to coordinator");
            }
            MessageId::Cancel => {
                debug!("Received Cancel message");
                return Err(PeerSessionError::NotImplemented(
                    "Cancel message handling".into(),
                ));
            }
            MessageId::Port => {
                debug!("Received Port message");
                return Err(PeerSessionError::NotImplemented(
                    "Port message handling".into(),
                ));
            }
        }

        Ok(())
    }

    /// Process a 'have' message from the peer
    async fn process_have_message(&mut self, message: &Message) -> Result<(), PeerSessionError> {
        let payload = message
            .payload
            .as_ref()
            .ok_or(PeerSessionError::EmptyPayload(
                "Have message without payload".into(),
            ))?;

        if payload.len() != 4 {
            return Err(PeerSessionError::InvalidMessagePayload(
                "Have message payload must be exactly 4 bytes".into(),
            ));
        }

        let mut piece_index_be = [0u8; 4];
        piece_index_be.copy_from_slice(payload);
        let piece_index = u32::from_be_bytes(piece_index_be);

        debug!(piece_index = piece_index, "Peer has piece");

        self.orchestrator_event_tx
            .send(Event::PeerHave {
                session_id: self.id.clone(),
                piece_index,
            })
            .await
            .map_err(|_| PeerSessionError::OrchestratorChannelClosed)?;

        Ok(())
    }
}

/// Errors that can occur in a peer session
#[derive(Debug)]
pub enum PeerSessionError {
    /// Protocol-level error
    Protocol(ProtocolError),

    /// Handshake-related error
    Handshake(HandshakeError),

    /// TCP connection error
    Connection(TcpError),

    /// Operation timed out
    ConnectionTimeout,

    /// Handshake timed out
    HandshakeTimeout,

    /// Received a message with an empty payload
    EmptyPayload(String),

    /// Received a message with invalid payload
    InvalidMessagePayload(String),

    /// Piece not found
    PieceNotFound,

    /// Piece has a corrupted hash
    CorruptedPiece(u32),

    /// IO channel is closed
    IoChannelClosed,

    /// Coordinator channel is closed
    CoordinatorChannelClosed,

    /// Orchestrator channel is closed
    OrchestratorChannelClosed,

    /// Feature not implemented
    NotImplemented(String),

    /// Missing required field in builder
    BuilderMissingField(&'static str),

    /// Session shutdown requested
    ShutdownRequested,
}

impl Display for PeerSessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Protocol(err) => write!(f, "Protocol error: {}", err),
            Self::Handshake(err) => write!(f, "Handshake error: {}", err),
            Self::Connection(err) => write!(f, "Connection error: {}", err),
            Self::ConnectionTimeout => write!(f, "Connection timed out"),
            Self::HandshakeTimeout => write!(f, "Handshake timed out"),
            Self::EmptyPayload(details) => write!(f, "Empty payload: {}", details),
            Self::InvalidMessagePayload(details) => {
                write!(f, "Invalid message payload: {}", details)
            }
            Self::PieceNotFound => write!(f, "Piece not found in map"),
            Self::CorruptedPiece(index) => {
                write!(f, "Invalid hash found for piece with index {}", index)
            }
            Self::IoChannelClosed => write!(f, "IO channel closed"),
            Self::CoordinatorChannelClosed => write!(f, "Coordinator channel closed"),
            Self::OrchestratorChannelClosed => write!(f, "Orchestrator channel closed"),
            Self::NotImplemented(feature) => write!(f, "Feature not implemented: {}", feature),
            Self::BuilderMissingField(field) => {
                write!(f, "Missing required field in builder: {}", field)
            }
            Self::ShutdownRequested => write!(f, "Session shutdown requested"),
        }
    }
}

impl Error for PeerSessionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Protocol(err) => Some(err),
            Self::Handshake(err) => Some(err),
            Self::Connection(err) => Some(err),
            _ => None,
        }
    }
}

impl From<ProtocolError> for PeerSessionError {
    fn from(err: ProtocolError) -> Self {
        Self::Protocol(err)
    }
}

impl From<HandshakeError> for PeerSessionError {
    fn from(err: HandshakeError) -> Self {
        Self::Handshake(err)
    }
}

impl From<TcpError> for PeerSessionError {
    fn from(err: TcpError) -> Self {
        Self::Connection(err)
    }
}

impl From<mpsc::error::SendError<PeerSessionEvent>> for PeerSessionError {
    fn from(_: mpsc::error::SendError<PeerSessionEvent>) -> Self {
        Self::IoChannelClosed
    }
}

impl From<mpsc::error::SendError<Event>> for PeerSessionError {
    fn from(_: mpsc::error::SendError<Event>) -> Self {
        Self::OrchestratorChannelClosed
    }
}

impl From<mpsc::error::SendError<CoordinatorCommand>> for PeerSessionError {
    fn from(_: mpsc::error::SendError<CoordinatorCommand>) -> Self {
        Self::CoordinatorChannelClosed
    }
}

impl PeerSessionError {
    /// Whether this error is critical and should cause the session to terminate
    pub fn is_critical(&self) -> bool {
        matches!(
            self,
            Self::ShutdownRequested
                | Self::ConnectionTimeout
                | Self::HandshakeTimeout
                | Self::IoChannelClosed
                | Self::CoordinatorChannelClosed
                | Self::OrchestratorChannelClosed
        )
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tokio::{sync::mpsc, time::timeout};

    use crate::torrent::{
        events::Event,
        peers::{
            coordinator::CoordinatorCommand,
            message::{Message, MessageId},
            session::{ConnectionState, PeerSession, PeerSessionError},
        },
    };

    // Helper function to create a test peer session
    fn create_test_peer_session() -> (PeerSession, mpsc::Receiver<Event>) {
        let (orchestrator_tx, orchestrator_rx) = mpsc::channel::<Event>(100);

        let peer_session = PeerSession::new(
            "test-peer-id".to_string(),
            [1u8; 20],
            [2u8; 20],
            "127.0.0.1:12345".to_string(),
            65536,
            16384,
            60, // timeout threshold in seconds
            orchestrator_tx,
        );

        (peer_session, orchestrator_rx)
    }

    #[test]
    fn test_peer_session_new() {
        let (orchestrator_tx, _) = mpsc::channel::<Event>(100);

        let session = PeerSession::new(
            "test-peer-id".to_string(),
            [1u8; 20],
            [2u8; 20],
            "127.0.0.1:12345".to_string(),
            65536,
            16384,
            60,
            orchestrator_tx,
        );

        assert_eq!(session.id, "test-peer-id");
        assert_eq!(session.peer_id, [1u8; 20]);
        assert_eq!(session.info_hash, [2u8; 20]);
        assert_eq!(session.peer_addr, "127.0.0.1:12345");
        assert_eq!(session.timeout_threshold, 60);
        assert_eq!(session.peer_bitfield_received, false);
        // choked when initialize
        assert!(session.connection_state.is_choked);
        // not interested when initialize
        assert!(!session.connection_state.is_interested);
        // choked when initialize
        assert!(session.connection_state.peer_choked);
        // not interested when initialize
        assert!(!session.connection_state.peer_interested);
    }

    #[test]
    fn test_connection_state_default() {
        let state = ConnectionState::default();

        assert!(state.is_choked);
        assert!(!state.is_interested);
        assert!(state.peer_choked);
        assert!(!state.peer_interested);
    }

    #[tokio::test]
    async fn test_handle_peer_message_keep_alive() {
        let (mut session, _) = create_test_peer_session();
        let (io_tx, _) = mpsc::channel::<Message>(10);
        let (coordinator_tx, _) = mpsc::channel::<CoordinatorCommand>(10);

        let message = Message::new(MessageId::KeepAlive, None);
        let result = session
            .handle_peer_message(message, &io_tx, &coordinator_tx)
            .await;

        assert!(result.is_ok());
        // No state changes for keep-alive
    }

    #[tokio::test]
    async fn test_handle_peer_message_unchoke() {
        let (mut session, _) = create_test_peer_session();
        let (io_tx, _) = mpsc::channel::<Message>(10);
        let (coordinator_tx, mut coordinator_rx) = mpsc::channel::<CoordinatorCommand>(10);

        // Verify initial state is choked
        assert!(session.connection_state.is_choked);

        let message = Message::new(MessageId::Unchoke, None);
        let result = session
            .handle_peer_message(message, &io_tx, &coordinator_tx)
            .await;

        assert!(result.is_ok());

        if let Some(CoordinatorCommand::Unchoked) = coordinator_rx.try_recv().ok() {
            assert!(true);
        } else {
            panic!("Expected Unchoked event");
        }

        assert!(!session.connection_state.is_choked);
    }

    #[tokio::test]
    async fn test_handle_peer_message_interested() {
        let (mut session, _) = create_test_peer_session();
        let (io_tx, mut io_rx) = mpsc::channel::<Message>(10);
        let (coordinator_tx, _) = mpsc::channel::<CoordinatorCommand>(10);

        // Verify initial state
        assert!(!session.connection_state.peer_interested);
        assert!(session.connection_state.peer_choked);

        let message = Message::new(MessageId::Interested, None);
        let result = session
            .handle_peer_message(message, &io_tx, &coordinator_tx)
            .await;

        assert!(result.is_ok());
        assert!(session.connection_state.peer_interested);
        assert!(!session.connection_state.peer_choked);

        // Verify an unchoke message was sent to peer
        let timeout_result = timeout(Duration::from_millis(100), io_rx.recv()).await;
        assert!(timeout_result.is_ok());
        let sent_message = timeout_result.unwrap().unwrap();
        assert_eq!(sent_message.message_id, MessageId::Unchoke);
    }

    #[tokio::test]
    async fn test_handle_peer_message_not_interested() {
        let (mut session, _) = create_test_peer_session();
        let (io_tx, _) = mpsc::channel::<Message>(10);
        let (coordinator_tx, _) = mpsc::channel::<CoordinatorCommand>(10);

        // First set to interested
        session.connection_state.peer_interested = true;

        let message = Message::new(MessageId::NotInterested, None);
        let result = session
            .handle_peer_message(message, &io_tx, &coordinator_tx)
            .await;

        assert!(result.is_ok());
        assert!(!session.connection_state.peer_interested);
    }

    #[tokio::test]
    async fn test_handle_peer_message_have() {
        let (mut session, mut orchestrator_rx) = create_test_peer_session();
        let (io_tx, _) = mpsc::channel::<Message>(10);
        let (coordinator_tx, _) = mpsc::channel::<CoordinatorCommand>(10);

        // Create a have message for piece index 42
        let piece_index: u32 = 42;
        let payload = Some(piece_index.to_be_bytes().to_vec());
        let message = Message::new(MessageId::Have, payload);

        let result = session
            .handle_peer_message(message, &io_tx, &coordinator_tx)
            .await;

        assert!(result.is_ok());

        // Verify PeerHave event was sent to orchestrator
        let timeout_result = timeout(Duration::from_millis(100), orchestrator_rx.recv()).await;
        assert!(timeout_result.is_ok());

        if let Some(Event::PeerHave {
            session_id,
            piece_index: received_index,
        }) = timeout_result.unwrap()
        {
            assert_eq!(session_id, "test-peer-id");
            assert_eq!(received_index, piece_index);
        } else {
            panic!("Expected PeerHave event");
        }
    }

    #[tokio::test]
    async fn test_handle_peer_message_bitfield() {
        let (mut session, mut orchestrator_rx) = create_test_peer_session();
        let (io_tx, _) = mpsc::channel::<Message>(10);
        let (coordinator_tx, _) = mpsc::channel::<CoordinatorCommand>(10);

        // Create a bitfield message
        let bitfield = vec![0b10101010, 0b11110000];
        let message = Message::new(MessageId::Bitfield, Some(bitfield.clone()));

        let result = session
            .handle_peer_message(message, &io_tx, &coordinator_tx)
            .await;

        assert!(result.is_ok());
        assert!(session.peer_bitfield_received);

        // Verify PeerBitfield event was sent to orchestrator
        let timeout_result = timeout(Duration::from_millis(100), orchestrator_rx.recv()).await;
        assert!(timeout_result.is_ok());

        if let Some(Event::PeerBitfield {
            session_id,
            bitfield: received_bitfield,
        }) = timeout_result.unwrap()
        {
            assert_eq!(session_id, "test-peer-id");
            assert_eq!(received_bitfield, bitfield);
        } else {
            panic!("Expected PeerBitfield event");
        }
    }

    #[tokio::test]
    async fn test_handle_peer_message_piece() {
        let (mut session, _) = create_test_peer_session();
        let (io_tx, _) = mpsc::channel::<Message>(10);
        let (coordinator_tx, mut coordinator_rx) = mpsc::channel::<CoordinatorCommand>(10);

        // Create a piece message
        let piece_data = vec![1, 2, 3, 4, 5];
        let message = Message::new(MessageId::Piece, Some(piece_data.clone()));

        let result = session
            .handle_peer_message(message, &io_tx, &coordinator_tx)
            .await;

        assert!(result.is_ok());

        // Verify DownloadPiece command was sent to coordinator
        let timeout_result = timeout(Duration::from_millis(100), coordinator_rx.recv()).await;
        assert!(timeout_result.is_ok());

        if let Some(CoordinatorCommand::DownloadPiece { payload }) = timeout_result.unwrap() {
            assert_eq!(payload.unwrap(), piece_data);
        } else {
            panic!("Expected DownloadPiece command");
        }
    }

    #[tokio::test]
    async fn test_handle_peer_message_empty_payload() {
        let (mut session, _) = create_test_peer_session();
        let (io_tx, _) = mpsc::channel::<Message>(10);
        let (coordinator_tx, _) = mpsc::channel::<CoordinatorCommand>(10);

        // Create a have message with empty payload (which should cause an error)
        let message = Message::new(MessageId::Have, None);

        let result = session
            .handle_peer_message(message, &io_tx, &coordinator_tx)
            .await;

        assert!(result.is_err());
        if let Err(PeerSessionError::EmptyPayload(_)) = result {
            // Expected error
        } else {
            panic!("Expected EmptyPayload error");
        }
    }
}
