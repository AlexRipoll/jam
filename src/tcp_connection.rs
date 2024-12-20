use std::{io, time::Duration};

use tokio::{net::TcpStream, time::timeout};
use tracing::warn;

pub async fn connect_to_peer(
    peer_addr: String,
    timeout_duration: u64,
    connection_retries: u32,
) -> io::Result<TcpStream> {
    let mut attempts = 0;
    while attempts < connection_retries {
        match timeout(
            Duration::from_millis(timeout_duration),
            TcpStream::connect(&peer_addr),
        )
        .await
        {
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
