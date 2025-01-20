use std::{io, sync::Arc};

use protocol::bitfield::Bitfield;
use protocol::message::Message;
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    sync::{broadcast, mpsc, Mutex},
};
use tracing::{error, info};

use crate::{
    download::state::DownloadState,
    error::error::JamError,
    p2p::{
        io::{read_message, send_message},
        message_handler::{perform_handshake, Actor, Handshake},
    },
};
use protocol::piece::Piece;

#[derive(Debug)]
pub struct PeerSession {
    peer_addr: String,
    handshake_metadata: Handshake,
    download_state: Arc<DownloadState>,
    bitfield_tx: Arc<mpsc::Sender<Bitfield>>,
    disk_tx: Arc<mpsc::Sender<(Piece, Vec<u8>)>>,
}

impl PeerSession {
    pub fn new(
        peer_addr: &str,
        handshake_metadata: Handshake,
        download_state: Arc<DownloadState>,
        bitfield_tx: Arc<mpsc::Sender<Bitfield>>,
        disk_tx: Arc<mpsc::Sender<(Piece, Vec<u8>)>>,
    ) -> Self {
        Self {
            peer_addr: peer_addr.to_string(),
            handshake_metadata,
            download_state,
            bitfield_tx,
            disk_tx,
        }
    }

    pub async fn initialize(&self, mut stream: TcpStream) -> io::Result<()> {
        // let session_span = tracing::info_span!("peer_session", peer_addr = %self.peer_addr);
        // let _enter = session_span.enter();

        // Perform handshake
        if let Err(e) = perform_handshake(&mut stream, &self.handshake_metadata).await {
            error!(self.peer_addr, error = %e, "Handshake failed");
            return Err(io::Error::new(io::ErrorKind::Other, "Handshake failed"));
        }
        info!(self.peer_addr, "Handshake completed");

        let (read_half, write_half) = tokio::io::split(stream);

        // Create channels for communication
        // TODO: channel buffer size defined in config
        let (io_rtx, io_rrx) = mpsc::channel(128);
        let (io_wtx, io_wrx) = mpsc::channel(128);
        let (shutdown_tx, _) = broadcast::channel::<()>(1); // Shutdown signal

        // Vector to keep track of task handles
        let mut task_handles = Vec::new();

        let actor = Arc::new(Mutex::new(Actor::new(
            self.bitfield_tx.clone(),
            io_wtx,
            self.disk_tx.clone(),
            shutdown_tx.clone(),
            self.download_state.clone(),
        )));

        let reader_task = tokio::spawn(PeerSession::message_reader(
            read_half,
            self.peer_addr.to_string(),
            io_rtx.clone(),
            shutdown_tx.subscribe(),
            shutdown_tx.clone(),
        ));
        task_handles.push(reader_task);

        let writer_task = tokio::spawn(PeerSession::message_writer(
            write_half,
            self.peer_addr.to_string(),
            io_wrx,
            shutdown_tx.subscribe(),
        ));
        task_handles.push(writer_task);

        let message_processor_task = tokio::spawn(PeerSession::process_message(
            Arc::clone(&actor),
            self.peer_addr.to_string(),
            io_rrx,
            shutdown_tx.subscribe(),
        ));
        task_handles.push(message_processor_task);

        let piece_request_task = tokio::spawn(PeerSession::piece_requester(
            Arc::clone(&actor),
            self.peer_addr.to_string(),
            shutdown_tx.subscribe(),
            shutdown_tx.clone(),
        ));
        task_handles.push(piece_request_task);

        let unconfirmed_timeout_task = tokio::spawn(PeerSession::timeout_watcher(
            Arc::clone(&actor),
            self.peer_addr.to_string(),
            shutdown_tx.subscribe(),
            shutdown_tx.clone(),
        ));
        task_handles.push(unconfirmed_timeout_task);

        // Await tasks
        for handle in task_handles {
            if let Err(e) = handle.await {
                tracing::error!(self.peer_addr, error = ?e, "Peer connection task error");
            }
        }

        Ok(())
    }

    async fn message_reader(
        mut read_half: tokio::io::ReadHalf<TcpStream>,
        peer_addr: String,
        io_rtx: mpsc::Sender<Message>,
        mut shutdown_rx: broadcast::Receiver<()>,
        shutdown_tx: broadcast::Sender<()>,
    ) {
        // let reader_span = tracing::info_span!("reader_task", peer_addr = %peer_addr);
        // let _enter = reader_span.enter();
        info!(peer_addr = %peer_addr, "Initializing peer IO reader...");

        loop {
            tokio::select! {
                result = read_message(&mut read_half) => {
                    match result {
                        Ok(message) => {
                            if io_rtx.send(message).await.is_err() {
                                error!(peer_addr = %peer_addr, "Receiver dropped; stopping reader task");
                                break;
                            }
                        }
                        Err(e) => {
                            error!(peer_addr = %peer_addr, error = %e, "Error reading message");
                            if e == JamError::IncompleteMessage {
                                let _ = shutdown_tx.send(()); // Send shutdown signal
                            }
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!(peer_addr = %peer_addr, "Reader task received shutdown signal");
                    drop(read_half);
                    break;
                }
            }
        }
    }

    async fn message_writer(
        mut write_half: tokio::io::WriteHalf<tokio::net::TcpStream>,
        peer_addr: String,
        mut io_wrx: mpsc::Receiver<Message>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        // let writer_span = tracing::info_span!("writer_task", peer_addr = %peer_addr);
        // let _enter = writer_span.enter();
        info!(peer_addr = %peer_addr, "Initializing peer IO writer...");

        loop {
            tokio::select! {
                // Receive message and send it over the TCP stream
                Some(message) = io_wrx.recv() => {
                    if let Err(e) = send_message(&mut write_half, message).await {
                        error!(peer_addr = %peer_addr, error = %e, "Error sending message");
                    }
                }

                // Handle shutdown signal
                _ = shutdown_rx.recv() => {
                    info!(peer_addr = %peer_addr, "Writer task received shutdown signal");
                    if let Err(e) = write_half.shutdown().await {
                        error!(peer_addr = %peer_addr, error = %e, "Error shutting down TCP stream");
                    }
                    drop(write_half);
                    info!(peer_addr = %peer_addr, "TCP stream closed");
                    break;
                }
            }
        }
    }

    async fn process_message(
        actor: Arc<tokio::sync::Mutex<Actor>>,
        peer_addr: String,
        mut io_rrx: mpsc::Receiver<Message>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        // let actor_span = tracing::info_span!("actor_task", peer_addr = %peer_addr);
        // let _enter = actor_span.enter();
        info!(peer_addr = %peer_addr, "Initializing peer actor handler...");

        loop {
            tokio::select! {
                // Process incoming messages
                Some(message) = io_rrx.recv() => {
                    let mut actor = actor.lock().await;
                    if let Err(e) = actor.handle_message(message).await {
                        error!(peer_addr = %peer_addr, error = %e, "Actor handler error");
                    }
                }

                // Handle shutdown signal
                _ = shutdown_rx.recv() => {
                    info!(peer_addr = %peer_addr, "Actor task received shutdown signal");
                    break;
                }
            }
        }
    }

    async fn piece_requester(
        actor: Arc<tokio::sync::Mutex<Actor>>,
        peer_addr: String,
        mut shutdown_rx: broadcast::Receiver<()>,
        shutdown_tx: broadcast::Sender<()>,
    ) {
        // let request_span = tracing::info_span!("piece_request_task", peer_addr = %peer_addr);
        // let _enter = request_span.enter();
        info!(peer_addr = %peer_addr, "Initializing piece requester...");

        loop {
            tokio::select! {
                // Perform piece request handling
                _ = async {
                    let mut actor = actor.lock().await;

                    if actor.ready_to_request().await {
                        if let Err(e) = actor.request().await {
                            error!(peer_addr = %peer_addr, error = %e, "Piece request error");
                        }
                    }

                    // Check if all pieces have been downloaded or if no more pieces can be
                    // downloaded from this peer
                    if actor.is_complete().await  || (actor.has_peer_bitfield() && !actor.download_state.metadata.has_missing_pieces(actor.peer_bitfield()).await) {
                        info!(peer_addr = %peer_addr, "All pieces downloaded. Sending shutdown signal...");
                        let _ = shutdown_tx.send(()); // Send shutdown signal
                    }
                } => {}

                // Handle shutdown signal
                _ = shutdown_rx.recv() => {
                    info!(peer_addr = %peer_addr, "Piece requester received shutdown signal");
                    let mut actor = actor.lock().await;
                    actor.release_unconfirmed_pieces().await;

                    break;
                }
            }
        }
    }

    async fn timeout_watcher(
        actor: Arc<tokio::sync::Mutex<Actor>>,
        peer_addr: String,
        mut shutdown_rx: broadcast::Receiver<()>,
        shutdown_tx: broadcast::Sender<()>,
    ) {
        info!(peer_addr = %peer_addr, "Initializing timout watcher...");

        loop {
            tokio::select! {
                // Perform piece request handling
                _ = async {
                    let mut actor = actor.lock().await;

                    if actor.has_pending_pieces() {
                        actor.release_timeout_downloads().await;
                    }
                    if actor.is_strikeout() {
                        let _ = shutdown_tx.send(()); // Send shutdown signal
                        info!(peer_addr = %peer_addr, "Max strikes reached. Sending shutdown signal...");
                    }

                } => {}

                // Handle shutdown signal
                _ = shutdown_rx.recv() => {
                    info!(peer_addr = %peer_addr, "Piece requester received shutdown signal");
                    let mut actor = actor.lock().await;
                    actor.release_unconfirmed_pieces().await;

                    break;
                }
            }
        }
    }
}
