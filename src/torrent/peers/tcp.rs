use std::{error::Error, fmt::Display, io, time::Duration};

use tokio::{net::TcpStream, time::timeout};
use tracing::warn;

pub async fn connect(
    peer_addr: &str,
    timeout_duration: u64,
    connection_retries: u32,
) -> Result<TcpStream, TcpError> {
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
                return Err(TcpError::ConnectionRefused(e));
            }
            Ok(Err(e)) => {
                warn!(peer_addr, "Failed to connect to peer: {}", e);
                return Err(TcpError::ConnectionError(e)); // Propagate other errors
            }
            Err(_) => {
                // Timeout elapsed
                attempts += 1;
                warn!(peer_addr, "Attempt {} timed out, retrying...", attempts);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    Err(TcpError::ConnectionTimeout)
}

#[derive(Debug)]
pub enum TcpError {
    ConnectionRefused(io::Error),
    ConnectionError(io::Error),
    ConnectionTimeout,
    Io(io::Error),
}

impl Display for TcpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TcpError::ConnectionTimeout => write!(f, "All connection attempts timed out"),
            TcpError::ConnectionRefused(err) => write!(f, "Peer refused connection: {}", err),
            TcpError::ConnectionError(err) => write!(f, "Connection error: {}", err),
            TcpError::Io(err) => write!(f, "IO error: {}", err),
        }
    }
}

impl From<io::Error> for TcpError {
    fn from(err: io::Error) -> Self {
        TcpError::Io(err)
    }
}

impl Error for TcpError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            TcpError::ConnectionRefused(err) => Some(err),
            TcpError::Io(err) => Some(err),
            _ => None,
        }
    }
}
