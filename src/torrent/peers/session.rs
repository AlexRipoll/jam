use std::{error::Error, fmt::Display};

use protocol::{error::ProtocolError, piece::Piece};
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::error;

use crate::torrent::events::Event;

use super::{
    coordinator::{Coordinator, CoordinatorCommand},
    handshake::{perform_handshake, Handshake, HandshakeError},
    io,
    message::{Message, MessageId},
    tcp::{connect, TcpError},
};

pub struct PeerSession {
    id: String,
    peer_id: [u8; 20],
    info_hash: [u8; 20],
    peer_addr: String,
    timeout_threshold: u64,
    peer_bitfield_received: bool,
    connection_state: ConnectionState,
    event_tx: mpsc::Sender<PeerSessionEvent>,
    event_rx: mpsc::Receiver<PeerSessionEvent>,
    orchestrator_event_tx: mpsc::Sender<Event>,
}

#[derive(Debug)]
struct ConnectionState {
    is_choked: bool,
    is_interested: bool,
    peer_choked: bool,
    peer_interested: bool,
}

#[derive(Debug)]
pub enum PeerSessionEvent {
    AssignPiece { piece: Piece },
    PeerMessageOut { message: Message },
    PeerMessageIn { message: Message },
    PieceAssembled { piece_index: u32, data: Vec<u8> },

    UnassignPiece { piece_index: u32 },
    UnassignPieces { pieces_index: Vec<u32> },
    PieceCorrupted { piece_index: u32 },
    ConnectionClosed,
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
    pub fn new(
        id: String,
        peer_id: [u8; 20],
        info_hash: [u8; 20],
        peer_addr: String,
        timeout_threshold: u64,
        orchestrator_event_tx: mpsc::Sender<Event>,
    ) -> Self {
        let (event_tx, mut event_rx) = mpsc::channel::<PeerSessionEvent>(100);
        PeerSession {
            id,
            peer_id,
            info_hash,
            peer_addr,
            timeout_threshold,
            peer_bitfield_received: false,
            connection_state: ConnectionState::default(),
            event_tx,
            event_rx,
            orchestrator_event_tx,
        }
    }

    pub async fn run(
        mut self,
    ) -> Result<(mpsc::Sender<PeerSessionEvent>, JoinHandle<()>), PeerSessionError> {
        let id = self.id.clone();

        // Connect to peer
        let mut stream = match connect(
            &self.peer_addr,
            3000, // TODO: inject values from config
            2,    // TODO: inject values from config
        )
        .await
        {
            Ok(stream) => stream,
            Err(err) => {
                return Err(PeerSessionError::Connection(err));
            }
        };

        // Perform handshake
        let handshake_metadata = Handshake::new(self.info_hash, self.peer_id);
        if let Err(err) = perform_handshake(&mut stream, &handshake_metadata).await {
            return Err(PeerSessionError::Handshake(err));
        }

        let event_tx = self.event_tx.clone();

        // Run the io
        let (io_out_tx, io_in_handle, io_out_handle) = io::run(stream, event_tx.clone()).await;

        // Initialize and run the coordinator
        let coordinator = Coordinator::new(self.timeout_threshold, event_tx.clone());
        let (coordinator_tx, coordinator_handle) = coordinator.run();

        let event_tx_clone = self.orchestrator_event_tx.clone();

        // TODO: spawn process that takes pieces from thw download state queue and sends a request to the peer.

        // Main actor task
        let actor_handle = tokio::spawn(async move {
            // Store handle to ensure it is dropped properly when actor_handle completes
            let _coordinator_handle = coordinator_handle;
            let _io_in_handle = io_in_handle;
            let _io_out_handle = io_out_handle;

            let id = self.id.clone();

            while let Some(cmd) = self.event_rx.recv().await {
                match cmd {
                    PeerSessionEvent::AssignPiece { piece } => {
                        if let Err(e) = coordinator_tx
                            .send(CoordinatorCommand::AddPiece { piece })
                            .await
                        {
                            error!("Error forwarding message to Coordinator: {}", e);
                        }
                    }
                    PeerSessionEvent::PeerMessageIn { message } => {
                        if let Err(e) = self
                            .handle_peer_message(message, &io_out_tx, &coordinator_tx)
                            .await
                        {
                            error!("Error processing message: {}", e);
                        }
                    }
                    PeerSessionEvent::PeerMessageOut { message } => {
                        if let Err(e) = io_out_tx.send(message).await {
                            error!("Error forwarding message to IO: {}", e);
                        }
                    }
                    PeerSessionEvent::PieceAssembled { piece_index, data } => {
                        if let Err(e) = self
                            .orchestrator_event_tx
                            .send(Event::PieceAssembled { piece_index, data })
                            .await
                        {
                            error!("Error forwarding event: {}", e);
                        }
                    }
                    PeerSessionEvent::PieceCorrupted { piece_index } => {
                        if let Err(e) = self
                            .orchestrator_event_tx
                            .send(Event::PieceCorrupted {
                                session_id: id.clone(),
                                piece_index,
                            })
                            .await
                        {
                            error!("Error forwarding event: {}", e);
                        }
                    }
                    PeerSessionEvent::UnassignPiece { piece_index } => {
                        if let Err(e) = self
                            .orchestrator_event_tx
                            .send(Event::PieceUnassign {
                                session_id: id.clone(),
                                piece_index,
                            })
                            .await
                        {
                            error!("Error forwarding event: {}", e);
                        }
                    }
                    PeerSessionEvent::UnassignPieces { pieces_index } => {
                        if let Err(e) = self
                            .orchestrator_event_tx
                            .send(Event::PieceUnassignMany {
                                session_id: id.clone(),
                                pieces_index,
                            })
                            .await
                        {
                            error!("Error forwarding event: {}", e);
                        }
                    }
                    PeerSessionEvent::Shutdown => break,
                    PeerSessionEvent::ConnectionClosed => {
                        if let Err(e) = self
                            .orchestrator_event_tx
                            .send(Event::PeerSessionClosed {
                                session_id: id.clone(),
                            })
                            .await
                        {
                            error!("Error forwarding event: {}", e);
                        }
                    }
                }
            }
        });

        Ok((event_tx, actor_handle))
    }

    pub async fn handle_peer_message(
        &mut self,
        message: Message,
        io_out_tx: &mpsc::Sender<Message>,
        coordinator_tx: &mpsc::Sender<CoordinatorCommand>,
    ) -> Result<(), PeerSessionError> {
        match message.message_id {
            MessageId::KeepAlive => {
                // TODO: extend stream alive
            }
            MessageId::Choke => {
                self.connection_state.is_choked = true;
                // TODO: logic in case peer chokes but client is still interested
            }
            MessageId::Unchoke => {
                self.connection_state.is_choked = false;
            }
            MessageId::Interested => {
                self.connection_state.peer_interested = true;

                // TODO: logic for unchoking peer
                if self.connection_state.peer_choked {
                    self.connection_state.peer_choked = false;
                    // notify peer it has been unchoked
                    io_out_tx.send(Message::new(MessageId::Unchoke, None)).await;
                }
            }
            MessageId::NotInterested => {
                self.connection_state.peer_interested = false;
            }
            MessageId::Have => {
                self.process_have_message(&message).await?;
            }
            MessageId::Bitfield => {
                let bitfield = message.payload.ok_or(PeerSessionError::EmptyPayload)?;

                self.peer_bitfield_received = true;
                self.orchestrator_event_tx
                    .send(Event::PeerBitfield {
                        session_id: self.id.clone(),
                        bitfield,
                    })
                    .await?;
            }
            MessageId::Request => {
                // TODO:send piece blocks to peer
                unimplemented!()
            }
            MessageId::Piece => {
                coordinator_tx
                    .send(CoordinatorCommand::DownloadPiece {
                        payload: message.payload,
                    })
                    .await;
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

    async fn process_have_message(&mut self, message: &Message) -> Result<(), PeerSessionError> {
        let mut piece_index_be = [0u8; 4];
        piece_index_be.copy_from_slice(
            message
                .payload
                .as_ref()
                .ok_or(PeerSessionError::EmptyPayload)?,
        );
        let piece_index = u32::from_be_bytes(piece_index_be) as usize;

        self.orchestrator_event_tx
            .send(Event::PeerHave {
                session_id: self.id.clone(),
                piece_index: piece_index as u32,
            })
            .await?;

        Ok(())
    }

    pub fn is_allowed_to_request(&self) -> bool {
        !self.connection_state.is_choked && self.connection_state.is_interested
    }

    pub fn has_received_peer_bitfield(&self) -> bool {
        self.peer_bitfield_received
    }

    pub async fn request_unchoke(&mut self) -> Result<(), PeerSessionError> {
        self.connection_state.is_interested = true;

        Ok(())
    }
}

#[derive(Debug)]
pub enum PeerSessionError {
    Protocol(ProtocolError),
    Handshake(HandshakeError),
    Connection(TcpError),
    EmptyPayload,
    PieceNotFound,
    CorruptedPiece(u32),
    CommandTxError(mpsc::error::SendError<PeerSessionEvent>),
    EventTxError(mpsc::error::SendError<Event>),
}

impl Display for PeerSessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerSessionError::EmptyPayload => write!(f, "Received message with empty payload"),
            PeerSessionError::PieceNotFound => write!(f, "Piece not found in map"),
            PeerSessionError::CorruptedPiece(piece_index) => {
                write!(f, "Invalid hash found for piece with index {}", piece_index)
            }
            PeerSessionError::CommandTxError(err) => {
                write!(f, "Failed to send command: {}", err)
            }
            PeerSessionError::EventTxError(err) => {
                write!(f, "Failed to send event: {}", err)
            }
            PeerSessionError::Connection(err) => write!(f, "Connection error: {}", err),
            PeerSessionError::Handshake(err) => write!(f, "Handshake error: {}", err),
            PeerSessionError::Protocol(err) => write!(f, "Protocol error: {}", err),
        }
    }
}

impl From<ProtocolError> for PeerSessionError {
    fn from(err: ProtocolError) -> Self {
        PeerSessionError::Protocol(err)
    }
}

impl From<HandshakeError> for PeerSessionError {
    fn from(err: HandshakeError) -> Self {
        PeerSessionError::Handshake(err)
    }
}

impl From<TcpError> for PeerSessionError {
    fn from(err: TcpError) -> Self {
        PeerSessionError::Connection(err)
    }
}

impl From<mpsc::error::SendError<PeerSessionEvent>> for PeerSessionError {
    fn from(err: mpsc::error::SendError<PeerSessionEvent>) -> Self {
        PeerSessionError::CommandTxError(err)
    }
}

impl From<mpsc::error::SendError<Event>> for PeerSessionError {
    fn from(err: mpsc::error::SendError<Event>) -> Self {
        PeerSessionError::EventTxError(err)
    }
}

impl Error for PeerSessionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            PeerSessionError::Protocol(err) => Some(err),
            PeerSessionError::Handshake(err) => Some(err),
            PeerSessionError::Connection(err) => Some(err),
            PeerSessionError::EventTxError(err) => Some(err),
            _ => None,
        }
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

    #[test]
    fn test_ready_to_request() {
        let (mut session, _) = create_test_peer_session();

        // Default state - should not be ready
        assert!(!session.is_allowed_to_request());

        // Set interested but still choked - should not be ready
        session.connection_state.is_interested = true;
        assert!(!session.is_allowed_to_request());

        // Set unchoked - should be ready
        session.connection_state.is_choked = false;
        assert!(session.is_allowed_to_request());

        // Set not interested - should not be ready
        session.connection_state.is_interested = false;
        assert!(!session.is_allowed_to_request());
    }

    #[test]
    fn test_has_received_peer_bitfield() {
        let (mut session, _) = create_test_peer_session();

        // Default state
        assert!(!session.has_received_peer_bitfield());

        // Set received
        session.peer_bitfield_received = true;
        assert!(session.has_received_peer_bitfield());
    }

    #[tokio::test]
    async fn test_request_unchoke() {
        let (mut session, _) = create_test_peer_session();

        // Default state
        assert!(!session.connection_state.is_interested);

        // Request unchoke
        let result = session.request_unchoke().await;
        assert!(result.is_ok());
        assert!(session.connection_state.is_interested);
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
        let (coordinator_tx, _) = mpsc::channel::<CoordinatorCommand>(10);

        // Verify initial state is choked
        assert!(session.connection_state.is_choked);

        let message = Message::new(MessageId::Unchoke, None);
        let result = session
            .handle_peer_message(message, &io_tx, &coordinator_tx)
            .await;

        assert!(result.is_ok());
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
        if let Err(PeerSessionError::EmptyPayload) = result {
            // Expected error
        } else {
            panic!("Expected EmptyPayload error");
        }
    }
}
