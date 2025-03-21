use std::{
    collections::HashMap,
    error::Error,
    fmt::Display,
    sync::Arc,
    time::{Duration, Instant},
};

use protocol::{error::ProtocolError, message::PiecePayload, piece::Piece};
use sha1::{Digest, Sha1};
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{debug, error};

use crate::torrent::{events::Event, peer::Peer, peers::message::TransferPayload};

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
    AppendPiece { piece: Piece },
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
                // Notify orchestrator and return error
                if let Err(e) = self
                    .orchestrator_event_tx
                    .send(Event::PeerSessionFailed { session_id: id })
                    .await
                {
                    error!("Failed to send PeerSessionFailed event: {}", e);
                }

                return Err(PeerSessionError::Connection(err));
            }
        };

        // Perform handshake
        let handshake_metadata = Handshake::new(self.info_hash, self.peer_id);
        if let Err(err) = perform_handshake(&mut stream, &handshake_metadata).await {
            // Notify orchestrator and return error
            if let Err(e) = self
                .orchestrator_event_tx
                .send(Event::PeerSessionFailed { session_id: id })
                .await
            {
                error!("Failed to send PeerSessionFailed event: {}", e);
            }

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
                    PeerSessionEvent::AppendPiece { piece } => {
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
        // self.io_wtx
        //     .send(Message::new(MessageId::Unchoke, None))
        //     .await?;
        // self.io_wtx
        //     .send(Message::new(MessageId::Interested, None))
        //     .await?;
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

//     pub async fn initialize(&mut self) -> Result<(), SessionError> {
//         let mut stream = connect(
//             &self.peer_addr,
//             3000, // TODO:inject values from config
//             2,    // TODO:inject values from config
//         )
//         .await?;
//
//         let handshake_metadata = Handshake::new(self.info_hash, self.peer_id);
//         // Perform handshake
//         perform_handshake(&mut stream, &handshake_metadata).await?;
//
//         // Create channels for communication
//         // TODO: define channel buffer size in config
//         let (io_rtx, io_rrx) = mpsc::channel(128);
//         let (io_wtx, io_wrx) = mpsc::channel(128);
//         let (shutdown_tx, mut shutdown_rx) = broadcast::channel::<()>(1); // Shutdown signal
//
//         let (read_half, write_half) = tokio::io::split(stream);
//
//         let worker = Arc::new(Mutex::new(Worker::new(
//             self.id.clone(),
//             io_wtx,
//             self.disk_tx.clone(),
//             self.orchestrator_tx.clone(),
//         )));
//
//         // Vector to keep track of task handles
//         let mut task_handles = Vec::new();
//
//         let reader_task = tokio::spawn(message_reader(
//             read_half,
//             self.peer_addr.to_string(),
//             io_rtx.clone(),
//             shutdown_tx.subscribe(),
//             shutdown_tx.clone(),
//         ));
//         task_handles.push(reader_task);
//
//         let writer_task = tokio::spawn(message_writer(
//             write_half,
//             self.peer_addr.to_string(),
//             io_wrx,
//             shutdown_tx.subscribe(),
//         ));
//         task_handles.push(writer_task);
//
//         let message_processor_task = tokio::spawn(process_message(
//             Arc::clone(&worker),
//             self.peer_addr.to_string(),
//             io_rrx,
//             shutdown_tx.subscribe(),
//         ));
//         task_handles.push(message_processor_task);
//
//         let piece_request_task = tokio::spawn(piece_requester(
//             Arc::clone(&worker),
//             self.peer_addr.to_string(),
//             shutdown_tx.subscribe(),
//         ));
//         task_handles.push(piece_request_task);
//
//         let timeout_watcher_task = tokio::spawn(timeout_watcher(
//             Arc::clone(&worker),
//             self.peer_addr.to_string(),
//             shutdown_tx.subscribe(),
//             shutdown_tx.clone(),
//         ));
//         task_handles.push(timeout_watcher_task);
//
//         loop {
//             tokio::select! {
//                 // Receive new piece request events
//                 Some(event) = self.rx.recv() => {
//                     match event {
//                         // FIX: update events
//                         // Event::State(state_event) => match state_event {
//                         //     StateEvent::RequestPiece(piece)=>{
//                         //         let mut worker = worker.lock().await;
//                         //         worker.push_to_queue(piece);
//                         //     },
//                         //     _ => {},
//                         // },
//                         // Event::Shutdown => break,
//                         _ => {},
//                     }
//                 }
//
//                 // Handle shutdown signal
//                 _ = shutdown_rx.recv() => {
//                     break;
//                 }
//             }
//         }
//
//         // release any pieces assigned to this peer
//         let mut worker = worker.lock().await;
//         if let Err(err) = worker.release_pieces().await {
//             error!(peer_addr = %self.peer_addr, error = %err, "timeout watcher error");
//         }
//
//         // Await tasks
//         for handle in task_handles {
//             if let Err(e) = handle.await {
//                 tracing::error!(self.peer_addr, error = ?e, "peer session handle error");
//             }
//         }
//
//         Ok(())
//     }
// }
//
// async fn message_reader(
//     mut read_half: tokio::io::ReadHalf<TcpStream>,
//     peer_addr: String,
//     io_rtx: mpsc::Sender<Message>,
//     mut shutdown_rx: broadcast::Receiver<()>,
//     shutdown_tx: broadcast::Sender<()>,
// ) {
//     loop {
//         tokio::select! {
//             result = read_message(&mut read_half) => {
//                 match result {
//                     Ok(message) => {
//                         if io_rtx.send(message).await.is_err() {
//                             error!(peer_addr = %peer_addr, "Receiver dropped; stopping reader task");
//                             break;
//                         }
//                     }
//                     Err(e) => {
//                         error!(peer_addr = %peer_addr, error = %e, "Error reading message");
//                         match e {
//                             IoError::IncompleteMessage => {
//                                 let _ = shutdown_tx.send(()); // Send shutdown signal
//
//                             }
//                             _ => {}
//                         }
//                     }
//                 }
//             }
//             _ = shutdown_rx.recv() => {
//                 drop(read_half);
//                 break;
//             }
//         }
//     }
// }
//
// async fn message_writer(
//     mut write_half: tokio::io::WriteHalf<tokio::net::TcpStream>,
//     peer_addr: String,
//     mut io_wrx: mpsc::Receiver<Message>,
//     mut shutdown_rx: broadcast::Receiver<()>,
// ) {
//     loop {
//         tokio::select! {
//             // Receive message and send it over the TCP stream
//             Some(message) = io_wrx.recv() => {
//                 if let Err(e) = send_message(&mut write_half, message).await {
//                     error!(peer_addr = %peer_addr, error = %e, "Error sending message");
//                 }
//             }
//
//             // Hhandle_messageandle shutdown signal
//             _ = shutdown_rx.recv() => {
//                 if let Err(e) = write_half.shutdown().await {
//                     error!(peer_addr = %peer_addr, error = %e, "Error shutting down TCP stream");
//                 }
//                 drop(write_half);
//                 debug!(peer_addr = %peer_addr, "TCP stream closed");
//                 break;
//             }
//         }
//     }
// }
//
// async fn process_message(
//     worker: Arc<tokio::sync::Mutex<Worker>>,
//     peer_addr: String,
//     mut io_rrx: mpsc::Receiver<Message>,
//     mut shutdown_rx: broadcast::Receiver<()>,
// ) {
//     loop {
//         tokio::select! {
//             // Process incoming messages
//             Some(message) = io_rrx.recv() => {
//                 let mut worker = worker.lock().await;
//                 if let Err(e) = worker.handle_message(message).await {
//                     error!(peer_addr = %peer_addr, error = %e, "Actor handler error");
//                 }
//             }
//
//             // Handle shutdown signal
//             _ = shutdown_rx.recv() => {
//                 break;
//             }
//         }
//     }
// }
//
// async fn piece_receiver(
//     worker: Arc<tokio::sync::Mutex<Worker>>,
//     mut session_rx: mpsc::Receiver<Event>,
//     mut shutdown_rx: broadcast::Receiver<()>,
// ) {
//     loop {
//         tokio::select! {
//             // Process incoming work event
//             Some(event) = session_rx.recv() => {
//                 match event {
//                         // FIX: update events
//                     // StateEvent::RequestPiece(piece) =>{
//                     //     let mut worker = worker.lock().await;
//                     //     worker.push_to_queue(piece)
//                     // },
//                     _ => {}
//                 }
//             }
//
//             // Handle shutdown signal
//             _ = shutdown_rx.recv() => {
//                 break;
//             }
//         }
//     }
// }
//
// async fn piece_requester(
//     worker: Arc<tokio::sync::Mutex<Worker>>,
//     peer_addr: String,
//     mut shutdown_rx: broadcast::Receiver<()>,
//     // shutdown_tx: broadcast::Sender<()>,
// ) {
//     loop {
//         tokio::select! {
//             // Perform piece request handling
//             _ = async {
//                 let mut worker = worker.lock().await;
//
//                 if worker.ready_to_request() {
//                     if let Err(e) = worker.request().await {
//                         error!(peer_addr = %peer_addr, error = %e, "Piece request error");
//                     }
//                 }
//
//             } => {}
//
//             // Handle shutdown signal
//             _ = shutdown_rx.recv() => {
//                 break;
//             }
//         }
//     }
// }
//
// async fn timeout_watcher(
//     worker: Arc<tokio::sync::Mutex<Worker>>,
//     peer_addr: String,
//     mut shutdown_rx: broadcast::Receiver<()>,
//     shutdown_tx: broadcast::Sender<()>,
// ) {
//     loop {
//         tokio::select! {
//             // Perform piece request handling
//             _ = async {
//                 let mut worker = worker.lock().await;
//
//                 if worker.has_pieces_in_progress() {
//                     if let Err(err) = worker.release_timed_out_pieces().await{
//                         error!(peer_addr = %peer_addr, error = %err, "timeout watcher error");
//                     }                }
//                 if worker.is_strikeout() {
//                     let _ = shutdown_tx.send(()); // Send shutdown signal
//                     debug!(peer_addr = %peer_addr, "max strikes reached. Sending shutdown signal...");
//                 }
//
//             } => {}
//
//             // Handle shutdown signal
//             _ = shutdown_rx.recv() => {
//                 break;
//             }
//         }
//     }
// }
//
// #[derive(Debug)]
// pub enum SessionError {
//     Connection(TcpError),
//     Handshake(HandshakeError),
//     Worker(PeerSessionError),
// }
//
// impl Display for SessionError {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             SessionError::Connection(err) => write!(f, "Connection error: {}", err),
//             SessionError::Handshake(err) => write!(f, "Handshake error: {}", err),
//             SessionError::Worker(err) => write!(f, "Worker error: {}", err),
//         }
//     }
// }
//
// impl From<TcpError> for SessionError {
//     fn from(err: TcpError) -> Self {
//         SessionError::Connection(err)
//     }
// }
//
// impl From<HandshakeError> for SessionError {
//     fn from(err: HandshakeError) -> Self {
//         SessionError::Handshake(err)
//     }
// }
//
// impl From<PeerSessionError> for SessionError {
//     fn from(err: PeerSessionError) -> Self {
//         SessionError::Worker(err)
//     }
// }
//
// impl Error for SessionError {
//     fn source(&self) -> Option<&(dyn Error + 'static)> {
//         match self {
//             SessionError::Connection(err) => Some(err),
//             SessionError::Handshake(err) => Some(err),
//             SessionError::Worker(err) => Some(err),
//             _ => None,
//         }
//     }
// }
