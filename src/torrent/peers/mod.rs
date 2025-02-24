use core::panic;
use std::sync::Arc;

use handshake::{perform_handshake, Handshake};
use io::{read_message, send_message, IoError};
use protocol::message::Message;
use tcp::connect;
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    sync::{broadcast, mpsc, Mutex},
};
use tracing::{debug, error, warn};
use worker::Worker;

use super::events::{DiskEvent, Event, StateEvent};

mod handshake;
mod io;
mod tcp;
pub mod worker;

pub struct PeerSession {
    // mut stream: TcpStream,
    id: String,
    peer_id: [u8; 20],
    info_hash: [u8; 20],
    peer_addr: String,
    disk_tx: Arc<mpsc::Sender<DiskEvent>>,
    orchestrator_tx: Arc<mpsc::Sender<Event>>,
    rx: mpsc::Receiver<Event>,
}

impl PeerSession {
    pub fn new(
        id: String,
        peer_id: [u8; 20],
        info_hash: [u8; 20],
        peer_addr: String,
        disk_tx: Arc<mpsc::Sender<DiskEvent>>,
        orchestrator_tx: Arc<mpsc::Sender<Event>>,
        rx: mpsc::Receiver<Event>,
    ) -> PeerSession {
        PeerSession {
            id,
            peer_id,
            info_hash,
            peer_addr,
            disk_tx,
            orchestrator_tx,
            rx,
        }
    }

    pub async fn initialize(&mut self) {
        let mut stream = match connect(
            &self.peer_addr,
            3000, // TODO:inject values from config
            2,    // TODO:inject values from config
        )
        .await
        {
            Ok(stream) => stream, // Successfully connected, return the stream
            Err(e) => {
                warn!(
                    peer_id = self.id,
                    peer_address = %self.peer_addr,
                    error = %e,
                    "Failed to connect to peer after retries"
                );
                panic!();
            }
        };

        let handshake_metadata = Handshake::new(self.info_hash, self.peer_id);
        // Perform handshake
        if let Err(e) = perform_handshake(&mut stream, &handshake_metadata).await {
            panic!();
        }

        // Create channels for communication
        // TODO: define channel buffer size in config
        let (io_rtx, io_rrx) = mpsc::channel(128);
        let (io_wtx, io_wrx) = mpsc::channel(128);
        let (shutdown_tx, mut shutdown_rx) = broadcast::channel::<()>(1); // Shutdown signal

        let (read_half, write_half) = tokio::io::split(stream);

        let worker = Arc::new(Mutex::new(Worker::new(
            self.id.clone(),
            io_wtx,
            self.disk_tx.clone(),
            self.orchestrator_tx.clone(),
        )));

        // Vector to keep track of task handles
        let mut task_handles = Vec::new();

        let reader_task = tokio::spawn(message_reader(
            read_half,
            self.peer_addr.to_string(),
            io_rtx.clone(),
            shutdown_tx.subscribe(),
            shutdown_tx.clone(),
        ));
        task_handles.push(reader_task);

        let writer_task = tokio::spawn(message_writer(
            write_half,
            self.peer_addr.to_string(),
            io_wrx,
            shutdown_tx.subscribe(),
        ));
        task_handles.push(writer_task);

        let message_processor_task = tokio::spawn(process_message(
            Arc::clone(&worker),
            self.peer_addr.to_string(),
            io_rrx,
            shutdown_tx.subscribe(),
        ));
        task_handles.push(message_processor_task);

        let piece_request_task = tokio::spawn(piece_requester(
            Arc::clone(&worker),
            self.peer_addr.to_string(),
            shutdown_tx.subscribe(),
        ));
        task_handles.push(piece_request_task);

        let timeout_watcher_task = tokio::spawn(timeout_watcher(
            Arc::clone(&worker),
            self.peer_addr.to_string(),
            shutdown_tx.subscribe(),
            shutdown_tx.clone(),
        ));
        task_handles.push(timeout_watcher_task);

        loop {
            tokio::select! {
                // Receive new piece request events
                Some(event) = self.rx.recv() => {
                    match event {
                        Event::State(state_event) => match state_event {
                            StateEvent::RequestPiece(piece)=>{
                                let mut worker = worker.lock().await;
                                worker.push_to_queue(piece);
                            },
                            _ => {},
                        },
                        Event::Shutdown => break,
                        _ => {},
                    }
                }

                // Handle shutdown signal
                _ = shutdown_rx.recv() => {
                    break;
                }
            }
        }

        // Await tasks
        for handle in task_handles {
            if let Err(e) = handle.await {
                tracing::error!(self.peer_addr, error = ?e, "peer session handle error");
            }
        }
    }
}

async fn message_reader(
    mut read_half: tokio::io::ReadHalf<TcpStream>,
    peer_addr: String,
    io_rtx: mpsc::Sender<Message>,
    mut shutdown_rx: broadcast::Receiver<()>,
    shutdown_tx: broadcast::Sender<()>,
) {
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
                        match e {
                            IoError::IncompleteMessage => {
                                let _ = shutdown_tx.send(()); // Send shutdown signal

                            }
                            _ => {}
                        }
                    }
                }
            }
            _ = shutdown_rx.recv() => {
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
                if let Err(e) = write_half.shutdown().await {
                    error!(peer_addr = %peer_addr, error = %e, "Error shutting down TCP stream");
                }
                drop(write_half);
                debug!(peer_addr = %peer_addr, "TCP stream closed");
                break;
            }
        }
    }
}

async fn process_message(
    worker: Arc<tokio::sync::Mutex<Worker>>,
    peer_addr: String,
    mut io_rrx: mpsc::Receiver<Message>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    loop {
        tokio::select! {
            // Process incoming messages
            Some(message) = io_rrx.recv() => {
                let mut worker = worker.lock().await;
                if let Err(e) = worker.handle_message(message).await {
                    error!(peer_addr = %peer_addr, error = %e, "Actor handler error");
                }
            }

            // Handle shutdown signal
            _ = shutdown_rx.recv() => {
                break;
            }
        }
    }
}

async fn piece_receiver(
    worker: Arc<tokio::sync::Mutex<Worker>>,
    mut session_rx: mpsc::Receiver<StateEvent>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    loop {
        tokio::select! {
            // Process incoming work event
            Some(event) = session_rx.recv() => {
                match event {
                    StateEvent::RequestPiece(piece) =>{
                        let mut worker = worker.lock().await;
                        worker.push_to_queue(piece)
                    },
                    _ => {}
                }
            }

            // Handle shutdown signal
            _ = shutdown_rx.recv() => {
                break;
            }
        }
    }
}

async fn piece_requester(
    worker: Arc<tokio::sync::Mutex<Worker>>,
    peer_addr: String,
    mut shutdown_rx: broadcast::Receiver<()>,
    // shutdown_tx: broadcast::Sender<()>,
) {
    loop {
        tokio::select! {
            // Perform piece request handling
            _ = async {
                let mut worker = worker.lock().await;

                if worker.ready_to_request() {
                    if let Err(e) = worker.request().await {
                        error!(peer_addr = %peer_addr, error = %e, "Piece request error");
                    }
                }

            } => {}

            // Handle shutdown signal
            _ = shutdown_rx.recv() => {
                let mut worker = worker.lock().await;
                // FIX: should be exec by another task
                worker.release_pieces().await;

                break;
            }
        }
    }
}

async fn timeout_watcher(
    worker: Arc<tokio::sync::Mutex<Worker>>,
    peer_addr: String,
    mut shutdown_rx: broadcast::Receiver<()>,
    shutdown_tx: broadcast::Sender<()>,
) {
    loop {
        tokio::select! {
            // Perform piece request handling
            _ = async {
                let mut worker = worker.lock().await;

                if worker.has_pieces_in_progress() {
                    worker.release_timed_out_pieces().await;
                }
                if worker.is_strikeout() {
                    let _ = shutdown_tx.send(()); // Send shutdown signal
                    debug!(peer_addr = %peer_addr, "max strikes reached. Sending shutdown signal...");
                }

            } => {}

            // Handle shutdown signal
            _ = shutdown_rx.recv() => {
            let mut worker = worker.lock().await;
            // FIX: should be exec by another task
                worker.release_pieces().await;

                break;
            }
        }
    }
}
