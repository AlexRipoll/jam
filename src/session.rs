use std::{io, sync::Arc, time::Duration};

use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    sync::{broadcast, mpsc, Mutex},
    time::timeout,
};
use tracing::{debug, error, info, trace, warn};

use crate::{
    bitfield::Bitfield,
    download_state::DownloadState,
    p2p::{
        io::{read_message, send_message},
        message::Message,
        message_handler::{self, handshake, Actor},
        piece::Piece,
    },
};

pub async fn new_peer_session(
    peer_addr: &str,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    client_tx: Arc<mpsc::Sender<Bitfield>>,
    disk_tx: Arc<mpsc::Sender<(Piece, Vec<u8>)>>,
    download_state: Arc<DownloadState>,
    timeout_duration: u64,
    connection_retries: u32,
) -> io::Result<()> {
    let session_span = tracing::info_span!("peer_session", peer_addr = %peer_addr);
    let _enter = session_span.enter();

    info!(peer_addr, "Starting peer connection...");

    let mut stream = connect_to_peer_with_retries(
        peer_addr,
        Duration::from_secs(timeout_duration),
        connection_retries,
    )
    .await?;
    info!(peer_addr, "TCP connection established");

    // Perform handshake
    if let Err(e) = handshake(&mut stream, info_hash, peer_id).await {
        error!(peer_addr, error = %e, "Handshake failed");
        return Err(io::Error::new(io::ErrorKind::Other, "Handshake failed"));
    }
    info!(peer_addr, "Handshake completed");

    let (read_half, write_half) = tokio::io::split(stream);

    // Create channels for communication
    let (io_rtx, io_rrx) = mpsc::channel(50);
    let (io_wtx, io_wrx) = mpsc::channel(50);
    let (shutdown_tx, _) = broadcast::channel::<()>(1); // Shutdown signal

    // Vector to keep track of task handles
    let mut task_handles = Vec::new();

    let actor = Arc::new(Mutex::new(Actor::new(
        client_tx,
        io_wtx,
        disk_tx,
        shutdown_tx.clone(),
        download_state,
    )));

    let reader_task = tokio::spawn(message_reader(
        read_half,
        io_rtx.clone(),
        peer_addr.to_string(),
        shutdown_tx.subscribe(),
    ));
    task_handles.push(reader_task);

    let writer_task = tokio::spawn(message_writer(
        write_half,
        io_wrx,
        shutdown_tx.subscribe(),
        peer_addr.to_string(),
    ));
    task_handles.push(writer_task);

    let message_processor_task = tokio::spawn(process_message(
        Arc::clone(&actor),
        io_rrx,
        shutdown_tx.subscribe(),
        peer_addr.to_string(),
    ));
    task_handles.push(message_processor_task);

    let piece_request_task = tokio::spawn(piece_requester(
        Arc::clone(&actor),
        shutdown_tx.subscribe(),
        shutdown_tx.clone(),
        peer_addr.to_string(),
    ));
    task_handles.push(piece_request_task);

    // Await tasks
    for handle in task_handles {
        if let Err(e) = handle.await {
            tracing::error!(peer_addr, error = ?e, "Peer connection task error");
        }
    }

    Ok(())
}

async fn connect_to_peer_with_retries(
    peer_addr: &str,
    timeout_duration: Duration,
    retries: u32,
) -> io::Result<TcpStream> {
    let mut attempts = 0;
    while attempts < retries {
        match timeout(timeout_duration, TcpStream::connect(peer_addr)).await {
            Ok(Ok(stream)) => return Ok(stream),
            Ok(Err(e)) if e.kind() == io::ErrorKind::ConnectionRefused => {
                warn!(peer_addr, "Connection refused by peer");
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "Peer refused connection",
                ));
            }
            Ok(Err(e)) => {
                warn!(peer_addr, "Failed to connect to peer: {}", e);
                return Err(e); // Propagate other errors
            }
            Err(_) => {
                // Timeout elapsed
                attempts += 1;
                warn!(peer_addr, "Attempt {} timed out, retrying...", attempts);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "All connection attempts timed out",
    ))
}

async fn message_reader(
    mut read_half: tokio::io::ReadHalf<TcpStream>,
    io_rtx: mpsc::Sender<Message>,
    peer_addr: String,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let reader_span = tracing::info_span!("reader_task", peer_addr = %peer_addr);
    let _enter = reader_span.enter();
    debug!(peer_addr = %peer_addr, "Initializing peer IO reader...");

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
                        break;
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                debug!(peer_addr = %peer_addr, "Reader task received shutdown signal");
                drop(read_half);
                break;
            }
        }
    }
}

async fn message_writer(
    mut write_half: tokio::io::WriteHalf<tokio::net::TcpStream>,
    mut io_wrx: mpsc::Receiver<Message>,
    mut shutdown_rx: broadcast::Receiver<()>,
    peer_addr: String,
) {
    let writer_span = tracing::info_span!("writer_task", peer_addr = %peer_addr);
    let _enter = writer_span.enter();
    debug!(peer_addr = %peer_addr, "Initializing peer IO writer...");

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
                debug!(peer_addr = %peer_addr, "Writer task received shutdown signal");
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
    mut io_rrx: mpsc::Receiver<Message>,
    mut shutdown_rx: broadcast::Receiver<()>,
    peer_addr: String,
) {
    let actor_span = tracing::info_span!("actor_task", peer_addr = %peer_addr);
    let _enter = actor_span.enter();
    debug!(peer_addr = %peer_addr, "Initializing peer actor handler...");

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
                debug!(peer_addr = %peer_addr, "Actor task received shutdown signal");
                break;
            }
        }
    }
}

async fn piece_requester(
    actor: Arc<tokio::sync::Mutex<Actor>>,
    mut shutdown_rx: broadcast::Receiver<()>,
    shutdown_tx: broadcast::Sender<()>,
    peer_addr: String,
) {
    let request_span = tracing::info_span!("piece_request_task", peer_addr = %peer_addr);
    let _enter = request_span.enter();
    debug!(peer_addr = %peer_addr, "Initializing piece requester...");

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

                // Check if all pieces have been downloaded
                if actor.is_complete().await {
                    debug!(peer_addr = %peer_addr, "All pieces downloaded. Sending shutdown signal...");
                    let _ = shutdown_tx.send(()); // Send shutdown signal
                }
            } => {}

            // Handle shutdown signal
            _ = shutdown_rx.recv() => {
                debug!(peer_addr = %peer_addr, "Piece requester received shutdown signal");
                break;
            }
        }
    }
}
