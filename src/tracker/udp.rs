use std::{
    io::Cursor,
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use rand::Rng;
use tokio::{
    io::AsyncReadExt,
    net::{lookup_host, UdpSocket},
    time::timeout,
};
use url::Url;

use crate::torrent::peer::{Ip, Peer};

use super::tracker::{AnnounceData, Tracker, TrackerError, TrackerResponse};

/// UDP protocol identifier for BitTorrent trackers
const UDP_PROTOCOL_ID: u64 = 0x41727101980;
/// Default timeout for UDP tracker requests in seconds
const DEFAULT_TIMEOUT_SECS: u64 = 10;

/// UDP tracker action codes
#[derive(Debug, Clone, Copy)]
enum Action {
    Connect = 0,
    Announce = 1,
    Scrape = 2,
    Error = 3,
}

impl Action {
    /// Reads the first 4 bytes of a buffer and returns the corresponding Action.
    /// If the value does not match any known action, it returns an `Error`.
    fn from_bytes(msg: &[u8]) -> Result<Action, TrackerError> {
        if msg.len() < 4 {
            return Err(TrackerError::InvalidPacketSize);
        }

        let value = u32::from_be_bytes(msg[..4].try_into().unwrap());

        match value {
            0 => Ok(Action::Connect),
            1 => Ok(Action::Announce),
            2 => Ok(Action::Scrape),
            3 => Ok(Action::Error),
            _ => Err(TrackerError::UnknownAction),
        }
    }
}

/// Implementation of the UDP tracker protocol
#[derive(Debug)]
pub struct UdpTracker {
    announce_url: String,
    socket: Arc<UdpSocket>,
    timeout: u64,
}

impl UdpTracker {
    /// Creates a new UDP tracker client
    pub async fn new(announce_url: &str) -> Result<UdpTracker, TrackerError> {
        // Validate URL
        let url = Url::parse(&announce_url).map_err(|_| {
            TrackerError::InvalidUrl(format!("Invalid UDP tracker URL: {}", announce_url))
        })?;

        if url.scheme() != "udp" {
            return Err(TrackerError::InvalidProtocol(format!(
                "Expected UDP URL, got: {}",
                announce_url
            )));
        }

        // Extract host and port
        let host = url.host_str().ok_or_else(|| {
            TrackerError::InvalidUrl(format!("Missing host in URL: {}", announce_url))
        })?;

        let port = url.port().unwrap_or(80);

        // Create socket
        let socket = UdpSocket::bind("0.0.0.0:0")
            .await
            .map_err(TrackerError::UdpBindingError)?;

        // Resolve target address
        let addr = format!("{}:{}", host, port);
        let addr = lookup_host(addr)
            .await
            .map_err(TrackerError::IoError)?
            .next()
            .ok_or_else(|| TrackerError::UdpError("No addresses found".into()))?;

        // Connect socket
        socket
            .connect(addr)
            .await
            .map_err(|e| TrackerError::UdpError(format!("Failed to connect UDP socket: {}", e)))?;

        Ok(UdpTracker {
            announce_url: announce_url.to_string(),
            socket: Arc::new(socket),
            timeout: DEFAULT_TIMEOUT_SECS,
        })
    }

    /// Sets the timeout for UDP tracker requests
    pub fn with_timeout(mut self, timeout: u64) -> Self {
        self.timeout = timeout;
        self
    }

    /// Creates a connect request packet
    fn build_connect_packet(&self, tx_id: u32) -> Vec<u8> {
        let mut buf = vec![0u8; 16];

        // Connection ID (64 bits)
        buf[0..8].copy_from_slice(&UDP_PROTOCOL_ID.to_be_bytes());
        // Action (32 bits): connect = 0
        buf[8..12].copy_from_slice(&(Action::Connect as u32).to_be_bytes());
        // Transaction ID (32 bits)
        buf[12..16].copy_from_slice(&tx_id.to_be_bytes());

        buf
    }

    /// Creates an announce request packet
    fn build_announce_packet(&self, connection_id: u64, req: &AnnounceData, tx_id: u32) -> Vec<u8> {
        let mut buf = vec![0u8; 98];

        // Connection ID (64 bits)
        buf[0..8].copy_from_slice(&connection_id.to_be_bytes());
        // Action (32 bits): announce = 1
        buf[8..12].copy_from_slice(&(Action::Announce as u32).to_be_bytes());
        // Transaction ID (32 bits)
        buf[12..16].copy_from_slice(&tx_id.to_be_bytes());
        // Info hash (20 bytes)
        buf[16..36].copy_from_slice(&req.info_hash);
        // Peer ID (20 bytes)
        buf[36..56].copy_from_slice(&req.peer_id);
        // Downloaded (64 bits)
        buf[56..64].copy_from_slice(&req.downloaded.to_be_bytes());
        // Left (64 bits)
        buf[64..72].copy_from_slice(&req.left.to_be_bytes());
        // Uploaded (64 bits)
        buf[72..80].copy_from_slice(&req.uploaded.to_be_bytes());
        // Event (32 bits)
        buf[80..84].copy_from_slice(&(req.event as u32).to_be_bytes());

        // IP address (32 bits) - 0 means the tracker uses the sender's IP
        let ip_bytes = match req.ip {
            Some(IpAddr::V4(ipv4)) => ipv4.octets(),
            _ => [0, 0, 0, 0],
        };
        let ip_u32 = u32::from_be_bytes(ip_bytes);
        buf[84..88].copy_from_slice(&ip_u32.to_be_bytes());

        // Key (32 bits) - Used to identify the client
        let key = req.key.unwrap_or_else(|| rand::thread_rng().gen());
        buf[88..92].copy_from_slice(&key.to_be_bytes());

        // Number of peers wanted (32 bits) - -1 for default
        let num_want = req.num_want.unwrap_or(-1);
        buf[92..96].copy_from_slice(&num_want.to_be_bytes());

        // Port (16 bits)
        buf[96..98].copy_from_slice(&req.port.to_be_bytes());

        buf
    }

    /// Reads and processes a tracker response
    async fn read_response(
        &self,
        socket: &UdpSocket,
        expected_tx_id: u32,
    ) -> Result<AnnounceResponse, TrackerError> {
        let mut buf = [0; 1024]; // Large enough for any typical response

        let result = timeout(
            Duration::from_secs(self.timeout),
            socket.recv_from(&mut buf),
        )
        .await;

        match result {
            Ok(Ok((size, _))) => {
                let resp = AnnounceResponse::parse_from_bytes(&buf[..size]).await?;

                // Validate transaction ID
                if let Some(tx_id) = resp.transaction_id() {
                    if tx_id != expected_tx_id {
                        return Err(TrackerError::InvalidTransactionId);
                    }
                }

                // Check for error messages
                if let AnnounceResponse::Error { message, .. } = &resp {
                    return Err(TrackerError::ErrorResponse(message.clone()));
                }

                Ok(resp)
            }
            Ok(Err(e)) => Err(TrackerError::UdpSocketError(e)),
            Err(_) => Err(TrackerError::ResponseTimeout),
        }
    }
}

#[async_trait]
impl Tracker for UdpTracker {
    async fn announce(&self, announce_data: AnnounceData) -> Result<TrackerResponse, TrackerError> {
        // Step 1: Connection handshake

        // Generate a random transaction ID
        let tx_id = rand::thread_rng().gen::<u32>();

        // Send the connect packet
        let connect_packet = self.build_connect_packet(tx_id);
        self.socket
            .send(&connect_packet)
            .await
            .map_err(TrackerError::IoError)?;

        // Receive and parse the connection response
        let connect_resp = self.read_response(&self.socket, tx_id).await?;

        // Extract connection ID for subsequent requests
        let connection_id = match connect_resp {
            AnnounceResponse::Connect { connection_id, .. } => connection_id,
            _ => return Err(TrackerError::UnexpectedAction),
        };

        // Step 2: Announce request

        // Generate a new transaction ID for the announce
        let tx_id = rand::thread_rng().gen::<u32>();

        // Send the announce packet
        let announce_packet = self.build_announce_packet(connection_id, &announce_data, tx_id);
        self.socket
            .send(&announce_packet)
            .await
            .map_err(TrackerError::IoError)?;

        // Receive and parse the announce response
        let announce_resp = self.read_response(&self.socket, tx_id).await?;

        if let AnnounceResponse::Announce {
            transaction_id: _,
            interval,
            leechers,
            seeders,
            peers,
        } = announce_resp
        {
            let peers = decode_peers(peers)?;

            Ok(TrackerResponse {
                interval,
                min_interval: None,
                tracker_id: None,
                seeders,
                leechers,
                peers,
                warning_message: None,
            })
        } else {
            Err(TrackerError::UnexpectedAction)
        }
    }

    fn clone_box(&self) -> Box<dyn Tracker> {
        Box::new(Self {
            announce_url: self.announce_url.clone(),
            socket: Arc::clone(&self.socket),
            timeout: self.timeout.clone(),
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum AnnounceResponse {
    Connect {
        transaction_id: u32,
        connection_id: u64,
    },
    Announce {
        transaction_id: u32,
        interval: u32,
        leechers: u32,
        seeders: u32,
        peers: Vec<(u32, u16)>, // (ip, port) pairs
    },
    Error {
        transaction_id: u32,
        message: String,
    },
}

impl AnnounceResponse {
    /// Returns the transaction ID from the response
    pub fn transaction_id(&self) -> Option<u32> {
        match self {
            AnnounceResponse::Connect { transaction_id, .. } => Some(*transaction_id),
            AnnounceResponse::Announce { transaction_id, .. } => Some(*transaction_id),
            AnnounceResponse::Error { transaction_id, .. } => Some(*transaction_id),
        }
    }

    pub async fn parse_from_bytes(buf: &[u8]) -> Result<AnnounceResponse, TrackerError> {
        if buf.len() < 4 {
            return Err(TrackerError::InvalidPacketSize);
        }

        let action = Action::from_bytes(buf)?;

        match action {
            Action::Connect => Self::parse_connect_packet(buf).await,
            Action::Announce => Self::parse_announce_packet(buf).await,
            Action::Scrape => Err(TrackerError::UnexpectedAction), // Not implemented
            Action::Error => Self::parse_error_packet(buf).await,
        }
    }

    async fn parse_connect_packet(buf: &[u8]) -> Result<AnnounceResponse, TrackerError> {
        if buf.len() < 16 {
            return Err(TrackerError::InvalidPacketSize);
        }

        let mut cursor = Cursor::new(&buf[4..]); // Skip action code
        let transaction_id = read_u32_be(&mut cursor).await?;
        let connection_id = read_u64_be(&mut cursor).await?;

        Ok(AnnounceResponse::Connect {
            transaction_id,
            connection_id,
        })
    }

    async fn parse_announce_packet(buf: &[u8]) -> Result<AnnounceResponse, TrackerError> {
        if buf.len() < 20 {
            return Err(TrackerError::InvalidPacketSize);
        }

        let mut cursor = Cursor::new(&buf[4..]); // Skip action code
        let transaction_id = read_u32_be(&mut cursor).await?;
        let interval = read_u32_be(&mut cursor).await?;
        let leechers = read_u32_be(&mut cursor).await?;
        let seeders = read_u32_be(&mut cursor).await?;

        // Remaining data is peer info
        let mut peers = Vec::new();
        while cursor.position() as usize + 6 <= buf.len() {
            let ip = read_u32_be(&mut cursor).await?;
            let port = read_u16_be(&mut cursor).await?;
            peers.push((ip, port));
        }

        Ok(AnnounceResponse::Announce {
            transaction_id,
            interval,
            leechers,
            seeders,
            peers,
        })
    }

    async fn parse_error_packet(buf: &[u8]) -> Result<AnnounceResponse, TrackerError> {
        if buf.len() < 8 {
            return Err(TrackerError::InvalidPacketSize);
        }

        let mut cursor = Cursor::new(&buf[4..]); // Skip action code
        let transaction_id = read_u32_be(&mut cursor).await?;

        // Rest of the bytes are the error message
        let message_bytes = &buf[8..];
        let message =
            String::from_utf8(message_bytes.to_vec()).map_err(|_| TrackerError::InvalidUTF8)?;

        Ok(AnnounceResponse::Error {
            transaction_id,
            message,
        })
    }
}

/// Convert the response to a list of peers
pub fn decode_peers(peers: Vec<(u32, u16)>) -> Result<Vec<Peer>, TrackerError> {
    if peers.is_empty() {
        return Err(TrackerError::EmptyPeers);
    }

    let peers_list = peers
        .iter()
        .map(|(ip, port)| Peer {
            peer_id: None,
            ip: Ip::IpV4(Ipv4Addr::from(*ip)),
            port: *port,
        })
        .collect();

    Ok(peers_list)
}

// Helper functions to read binary data
async fn read_u32_be(cursor: &mut Cursor<&[u8]>) -> Result<u32, TrackerError> {
    let mut buf = [0; 4];
    cursor
        .read_exact(&mut buf)
        .await
        .map_err(|_| TrackerError::InvalidPacketSize)?;

    Ok(u32::from_be_bytes(buf))
}

async fn read_u64_be(cursor: &mut Cursor<&[u8]>) -> Result<u64, TrackerError> {
    let mut buf = [0; 8];
    cursor
        .read_exact(&mut buf)
        .await
        .map_err(|_| TrackerError::InvalidPacketSize)?;

    Ok(u64::from_be_bytes(buf))
}

async fn read_u16_be(cursor: &mut Cursor<&[u8]>) -> Result<u16, TrackerError> {
    let mut buf = [0; 2];
    cursor
        .read_exact(&mut buf)
        .await
        .map_err(|_| TrackerError::InvalidPacketSize)?;

    Ok(u16::from_be_bytes(buf))
}
