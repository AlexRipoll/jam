use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    io,
    time::Duration,
};

use tokio::{
    net::TcpStream,
    time::{sleep, timeout},
};
use tracing::{debug, instrument, warn};

/// Configuration for TCP connection attempts.
pub struct ConnectionConfig {
    /// Duration in milliseconds for connection timeout
    pub timeout_ms: u64,
    /// Maximum number of connection attempts
    pub max_retries: u32,
    /// Delay between retry attempts in milliseconds
    pub retry_delay_ms: u64,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            timeout_ms: 5000,    // 5 seconds timeout
            max_retries: 3,      // 3 retry attempts
            retry_delay_ms: 500, // 500ms between retries
        }
    }
}

/// Attempts to establish a TCP connection to the specified address with configurable
/// timeout and retry behavior.
///
/// # Arguments
///
/// * `peer_addr` - The address to connect to (e.g. "127.0.0.1:8080")
/// * `config` - Connection configuration parameters
///
/// # Returns
///
/// * `Ok(TcpStream)` - Successfully established TCP connection
/// * `Err(TcpError)` - Connection failed with the specific error
#[instrument(skip(config), level = "debug")]
pub async fn connect(peer_addr: &str, config: ConnectionConfig) -> Result<TcpStream, TcpError> {
    let ConnectionConfig {
        timeout_ms,
        max_retries,
        retry_delay_ms,
    } = config;
    let timeout_duration = Duration::from_millis(timeout_ms);
    let retry_delay = Duration::from_millis(retry_delay_ms);
    let max_retries = if max_retries == 0 { 1 } else { max_retries };

    for attempt in 1..=max_retries {
        debug!(peer_addr, attempt, "Attempting TCP connection");

        match timeout(timeout_duration, TcpStream::connect(peer_addr)).await {
            // Connection succeeded
            Ok(Ok(stream)) => {
                debug!(peer_addr, "Connection established successfully");
                return Ok(stream);
            }

            // Connection refused by peer
            Ok(Err(e)) if e.kind() == io::ErrorKind::ConnectionRefused => {
                warn!(peer_addr, error = %e, "Connection refused by peer");
                return Err(TcpError::ConnectionRefused(e));
            }

            // Other connection error
            Ok(Err(e)) => {
                warn!(peer_addr, error = %e, "Failed to connect to peer");
                return Err(TcpError::ConnectionError(e));
            }

            // Timeout elapsed
            Err(_) => {
                if attempt < max_retries {
                    warn!(
                        peer_addr,
                        attempt,
                        remaining = max_retries - attempt,
                        "Connection attempt timed out, retrying..."
                    );
                    sleep(retry_delay).await;
                } else {
                    warn!(peer_addr, "All connection attempts timed out");
                    return Err(TcpError::ConnectionTimeout);
                }
            }
        }
    }

    // This should be unreachable with the loop structure above
    Err(TcpError::ConnectionTimeout)
}

/// TCP connection errors that may occur when establishing connections.
#[derive(Debug)]
pub enum TcpError {
    /// Connection was explicitly refused by the peer
    ConnectionRefused(io::Error),
    /// General error occurred during connection attempt
    ConnectionError(io::Error),
    /// Connection attempts timed out after all retries
    ConnectionTimeout,
    /// Other I/O errors
    Io(io::Error),
}

impl Display for TcpError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConnectionTimeout => write!(f, "Connection timed out after all retry attempts"),
            Self::ConnectionRefused(err) => write!(f, "Connection refused by peer: {}", err),
            Self::ConnectionError(err) => write!(f, "Failed to establish connection: {}", err),
            Self::Io(err) => write!(f, "I/O error: {}", err),
        }
    }
}

impl Error for TcpError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConnectionRefused(err) | Self::ConnectionError(err) | Self::Io(err) => Some(err),
            Self::ConnectionTimeout => None,
        }
    }
}

impl From<io::Error> for TcpError {
    fn from(err: io::Error) -> Self {
        match err.kind() {
            io::ErrorKind::ConnectionRefused => Self::ConnectionRefused(err),
            _ => Self::Io(err),
        }
    }
}

// #[derive(Debug)]
// pub enum TcpError {
//     ConnectionRefused(io::Error),
//     ConnectionError(io::Error),
//     ConnectionTimeout,
//     Io(io::Error),
// }
//
// impl Display for TcpError {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             TcpError::ConnectionTimeout => write!(f, "All connection attempts timed out"),
//             TcpError::ConnectionRefused(err) => write!(f, "Peer refused connection: {}", err),
//             TcpError::ConnectionError(err) => write!(f, "Connection error: {}", err),
//             TcpError::Io(err) => write!(f, "IO error: {}", err),
//         }
//     }
// }
//
// impl From<io::Error> for TcpError {
//     fn from(err: io::Error) -> Self {
//         TcpError::Io(err)
//     }
// }
//
// impl Error for TcpError {
//     fn source(&self) -> Option<&(dyn Error + 'static)> {
//         match self {
//             TcpError::ConnectionRefused(err) => Some(err),
//             TcpError::Io(err) => Some(err),
//             _ => None,
//         }
//     }
// }
