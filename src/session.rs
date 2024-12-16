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
    client::DownloadState,
    p2p::{
        connection::{self, Actor},
        io::{read_message, send_message},
        message::Message,
        piece::Piece,
    },
};

pub async fn new_peer_session(
    peer_addr: &str,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    client_tx: mpsc::Sender<Bitfield>,
    disk_tx: mpsc::Sender<(Piece, Vec<u8>)>,
    download_state: Arc<DownloadState>,
    // TODO: move to config
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
    if let Err(e) = connection::handshake(&mut stream, info_hash, peer_id).await {
        error!(peer_addr, error = %e, "Handshake failed");
        return Err(io::Error::new(io::ErrorKind::Other, "Handshake failed"));
    }
    info!(peer_addr, "Handshake completed");

    let (mut read_half, mut write_half) = tokio::io::split(stream);

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

    // Reader task
    let reader_task = tokio::spawn({
        let io_rtx = io_rtx.clone();
        let peer_addr = peer_addr.to_string();
        let reader_span = tracing::info_span!("reader_task", peer_addr = %peer_addr);
        let mut shutdown_rx = shutdown_tx.subscribe();
        async move {
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
    });
    task_handles.push(reader_task);

    // Writer task
    let writer_task = tokio::spawn({
        let peer_addr = peer_addr.to_string();
        let writer_span = tracing::info_span!("writer_task", peer_addr = %peer_addr);
        let mut receiver: mpsc::Receiver<Message> = io_wrx;
        let mut shutdown_rx = shutdown_tx.subscribe();
        async move {
            let _enter = writer_span.enter();
            debug!(peer_addr = %peer_addr, "Initializing peer IO writer...");
            loop {
                tokio::select! {
                    Some(message) = receiver.recv() => {
                        if let Err(e) = send_message(&mut write_half, message).await {
                            error!(peer_addr = %peer_addr, error = %e, "Error sending message");
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!(peer_addr = %peer_addr, "Writer task received shutdown signal");
                        let _ = write_half.shutdown().await;
                        drop(write_half);
                        info!(peer_addr = %peer_addr, "TCP stream closed");
                        break;
                    }
                }
            }
        }
    });
    task_handles.push(writer_task);

    // Actor handler task
    let actor_task = tokio::spawn({
        let actor = Arc::clone(&actor);
        let actor_span = tracing::info_span!("actor_task", peer_addr = %peer_addr);
        let mut receiver = io_rrx;
        let peer_addr = peer_addr.to_string();
        let mut shutdown_rx = shutdown_tx.subscribe();
        async move {
            let _enter = actor_span.enter();
            debug!(peer_addr = %peer_addr, "Initializing peer actor handler...");
            loop {
                tokio::select! {
                    Some(message) = receiver.recv() => {
                        let mut actor = actor.lock().await;
                        if let Err(e) = actor.handle_message(message).await {
                            error!(peer_addr = %peer_addr, error = %e, "Actor handler error");
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!(peer_addr = %peer_addr, "Actor task received shutdown signal");
                        break;
                    }
                }
            }
        }
    });
    task_handles.push(actor_task);

    // Piece request task
    let piece_request_handle = tokio::spawn({
        let actor = Arc::clone(&actor);
        let peer_addr = peer_addr.to_string();
        let request_span = tracing::info_span!("piece_request_task", peer_addr = %peer_addr);
        let mut shutdown_rx = shutdown_tx.subscribe();
        async move {
            let _enter = request_span.enter();
            debug!(peer_addr = %peer_addr, "Initializing piece requester...");
            loop {
                tokio::select! {
                    _ = async {
                        let mut actor = actor.lock().await;
                        if actor.ready_to_request().await {
                            if let Err(e) = actor.request().await {
                                error!(peer_addr = %peer_addr, error = %e, "Piece request error");
                            }
                        }
                        // Check if the client has all the peer pieces
                        if actor.is_complete().await {
                            debug!(peer_addr = %peer_addr, "All pieces downloaded. Sending shutdown signal...");
                            let _ = shutdown_tx.send(()); // Send shutdown signal
                        }
                    } => {}
                    _ = shutdown_rx.recv() => {
                        debug!(peer_addr = %peer_addr, "Piece requester received shutdown signal");
                        break;
                    }
                }
            }
        }
    });
    task_handles.push(piece_request_handle);

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
