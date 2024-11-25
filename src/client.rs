use std::io;

use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex},
};

use crate::p2p::{
    client::{self, Actor},
    io::{read_message, send_message},
    message::Message,
};

async fn bittorrent_client(
    peer_addr: &str,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
) -> io::Result<()> {
    let mut stream = TcpStream::connect(peer_addr).await?;
    println!("TCP conncetion with peer at address: {peer_addr}");

    // Perform handshake
    if let Err(e) = client::handshake(&mut stream, info_hash, peer_id).await {
        eprintln!("Handshake failed: {e}");
        return Err(io::Error::new(io::ErrorKind::Other, "Handshake failed"));
    }

    let (mut read_half, mut write_half) = tokio::io::split(stream);

    // Create channels for communication
    let (io_rtx, io_rrx) = mpsc::channel(50);
    let (io_wtx, io_wrx) = mpsc::channel(50);
    let (disk_tx, disk_rx) = mpsc::channel(50);

    let actor = Mutex::new(Actor::new(io_wtx, disk_tx));

    // Reader task
    tokio::spawn({
        let io_rtx = io_rtx.clone();
        async move {
            loop {
                match read_message(&mut read_half).await {
                    Ok(message) => {
                        if io_rtx.send(message).await.is_err() {
                            println!("Receiver dropped; stopping reader task.");
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("Error reading message: {e}");
                        break;
                    }
                }
            }
        }
    });

    // Writer task
    tokio::spawn(async move {
        // writer
        let mut receiver: mpsc::Receiver<Message> = io_wrx;
        while let Some(message) = receiver.recv().await {
            send_message(&mut write_half, message).await.unwrap();
        }
    });

    // Actor handler task
    tokio::spawn({
        let mut receiver = io_rrx;
        async move {
            while let Some(message) = receiver.recv().await {
                let mut actor = actor.lock().await;
                actor.handle_message(message);
            }
        }
    });

    Ok(())
}
