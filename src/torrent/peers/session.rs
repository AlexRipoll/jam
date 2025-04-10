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

    pub fn ready_to_request(&self) -> bool {
        !self.connection_state.is_choked && self.connection_state.is_interested
        // && !self.download_state.queue.is_empty()
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
