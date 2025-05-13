use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use std::{collections::HashMap, error::Error, fmt::Display, net::IpAddr, time::Duration};
use tokio::io;
use url::Url;

use crate::torrent::peer::Peer;

use super::{http::HttpTracker, udp::UdpTracker};

/// The trait that all tracker implementations must implement
#[async_trait]
pub trait Tracker: Send + Sync {
    /// Announce to the tracker
    async fn announce(&self, data: AnnounceData) -> Result<TrackerResponse, TrackerError>;

    /// Clone the tracker as a boxed trait object
    fn clone_box(&self) -> Box<dyn Tracker>;
}

impl Clone for Box<dyn Tracker> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AnnounceData {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub compact: u8,
    pub no_peer_id: u8,
    pub event: Event,
    pub ip: Option<IpAddr>,
    pub num_want: Option<i32>,
    pub key: Option<u32>,
    pub tracker_id: Option<String>,
}

impl AnnounceData {
    /// Creates a new announce
    pub fn new(
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        port: u16,
        downloaded: u64,
        uploaded: u64,
        left: u64,
        event: Event,
        num_want: Option<i32>,
    ) -> Self {
        Self {
            info_hash,
            peer_id,
            port,
            uploaded,
            downloaded,
            left,
            compact: 1, // Always use compact format
            no_peer_id: 0,
            event,
            ip: None,
            num_want,
            key: None,
            tracker_id: None,
        }
    }
}

/// Factory for creating tracker instances
pub struct TrackerFactory;

impl TrackerFactory {
    /// Create a tracker from an announce URL
    pub async fn create(announce_url: &str) -> Result<Box<dyn Tracker>, TrackerError> {
        // Parse URL to determine protocol
        let url = Url::parse(announce_url).map_err(TrackerError::AnnounceParseError)?;

        match url.scheme() {
            "http" | "https" => {
                let tracker = HttpTracker::new(announce_url)?;
                Ok(Box::new(tracker))
            }
            "udp" => {
                let tracker = UdpTracker::new(announce_url).await?;
                Ok(Box::new(tracker))
            }
            _ => Err(TrackerError::UnsupportedProtocol(format!(
                "Unsupported protocol: {}",
                url.scheme()
            ))),
        }
    }
}

/// The main tracker manager that coordinates with multiple trackers
#[derive(Clone)]
pub struct TrackerManager {
    trackers: Vec<Box<dyn Tracker>>,
    metainfo_hash: [u8; 20],
    peer_id: [u8; 20],
    port: u16,
}

impl TrackerManager {
    /// Create a new tracker manager
    pub fn new(metainfo_hash: [u8; 20], peer_id: [u8; 20], port: u16) -> Self {
        Self {
            trackers: Vec::new(),
            metainfo_hash,
            peer_id,
            port,
        }
    }

    /// Add a tracker to the manager
    pub fn add_tracker(&mut self, tracker: Box<dyn Tracker>) {
        self.trackers.push(tracker);
    }

    /// Add multiple trackers to the manager
    pub async fn add_trackers(
        &mut self,
        announce_urls: &[String],
    ) -> Vec<Result<(), TrackerError>> {
        let mut results = Vec::with_capacity(announce_urls.len());

        for url in announce_urls {
            let result = match TrackerFactory::create(url).await {
                Ok(tracker) => {
                    self.trackers.push(tracker);
                    Ok(())
                }
                Err(e) => Err(e),
            };

            results.push(result);
        }

        results
    }

    pub async fn announce(
        &self,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        port: u16,
        downloaded: u64,
        uploaded: u64,
        left: u64,
        event: Event,
        num_want: Option<i32>,
    ) -> Result<TrackerResponse, TrackerError> {
        let tracker = self.trackers().first().unwrap();
        let announce_data = AnnounceData::new(
            info_hash, peer_id, port, downloaded, uploaded, left, event, num_want,
        );

        tracker.announce(announce_data).await
    }

    /// Get all available trackers
    pub fn trackers(&self) -> &[Box<dyn Tracker>] {
        &self.trackers
    }
}

/// Tracker event types
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u8)]
pub enum Event {
    None = 0,
    Completed = 1,
    Started = 2,
    Stopped = 3,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ScrapeResponse {
    failure_response: Option<String>,
    flags: Option<Flags>,
    files: Option<HashMap<ByteBuf, FileStatus>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct FileStatus {
    name: Option<String>,
    complete: Option<u32>,
    downloaded: Option<u32>,
    incomplete: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Flags {
    min_request_interval: Option<u64>,
}

/// Response from a tracker request across both HTTP and UDP protocols
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackerResponse {
    pub interval: u32,
    pub min_interval: Option<u32>,
    pub tracker_id: Option<String>,
    pub seeders: u32,
    pub leechers: u32,
    pub peers: Vec<Peer>,
    pub warning_message: Option<String>,
}

#[derive(Debug)]
pub enum TrackerError {
    InvalidPeersFormat,
    EmptyPeers,
    InvalidTxId,
    InvalidPacketSize,
    InvalidUTF8,
    UnknownAction,
    HttpRequestError(reqwest::Error), // Error during the HTTP request
    ResponseMissingField(String),
    ParseError(serde_bencode::Error),
    InvalidResponse(reqwest::Error),
    DecodeError(serde_bencode::Error),
    AnnounceParseError(url::ParseError),
    EmptyAnnounceQueue,
    UnsupportedProtocol(String),
    UdpBindingError(io::Error),
    UdpSocketError(io::Error),
    InvalidTransactionId,
    UnexpectedAction,
    ErrorResponse(String),
    IoError(tokio::io::Error),
    ResponseTimeout,
    ConnectionFailed,
    InvalidUrl(String),
    InvalidProtocol(String),
    TrackerResponse(String),
    UdpError(String),
}

impl Display for TrackerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrackerError::InvalidPeersFormat => write!(
                f,
                "Invalid peers format: must be consist of multiples of 6 bytes."
            ),
            TrackerError::EmptyPeers => write!(f, "No peers bytes found in tracker's response"),
            TrackerError::InvalidTxId => write!(f, "Invalid tracker response"),
            TrackerError::InvalidPacketSize => write!(f, "Invalid packet size"),
            TrackerError::InvalidUTF8 => write!(f, "Invalid UTF-8 format"),
            TrackerError::UnknownAction => write!(f, "Unknown action"),
            TrackerError::HttpRequestError(e) => write!(f, "HTTP request error: {}", e),
            TrackerError::DecodeError(e) => write!(f, "Error decoding response: {}", e),
            TrackerError::InvalidResponse(e) => write!(f, "Tracker invalid response: {}", e),
            TrackerError::EmptyAnnounceQueue => write!(f, "Empty announce queue"),
            TrackerError::AnnounceParseError(e) => write!(f, "Announce url parse error: {}", e),
            TrackerError::UnsupportedProtocol(e) => write!(f, "Protocol not supported: {}", e),
            TrackerError::ResponseMissingField(e) => {
                write!(f, "Missing or invalid {} field", e)
            }
            TrackerError::ParseError(e) => write!(f, "Failed to parse bencoded data: {}", e),
            TrackerError::UdpBindingError(e) => write!(f, "UDP binding error: {}", e),
            TrackerError::InvalidTransactionId => write!(f, "Invalid Tx ID received from tracker"),
            TrackerError::UdpSocketError(e) => write!(f, "Socket error: {}", e),
            TrackerError::UnexpectedAction => write!(f, "Unexpected action in tracker response"),
            TrackerError::ErrorResponse(e) => write!(f, "Tracker responded with error: {}", e),
            TrackerError::IoError(e) => write!(f, "IO error: {}", e),
            TrackerError::ResponseTimeout => write!(f, "Tracker response timeout"),
            TrackerError::ConnectionFailed => write!(f, "Failed to connect to the tracker"),
            TrackerError::InvalidUrl(e) => write!(f, "Invalid URL: {}", e),
            TrackerError::InvalidProtocol(e) => write!(f, "Invalid protocol: {}", e),
            TrackerError::TrackerResponse(e) => write!(f, "Announce response failure: {}", e),
            TrackerError::UdpError(e) => write!(f, "UDP error: {}", e),
        }
    }
}

impl Error for TrackerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            TrackerError::HttpRequestError(err) => Some(err),
            TrackerError::InvalidResponse(err) => Some(err),
            TrackerError::DecodeError(err) => Some(err),
            TrackerError::AnnounceParseError(err) => Some(err),
            TrackerError::ParseError(err) => Some(err),
            TrackerError::UdpBindingError(err) => Some(err),
            TrackerError::UdpSocketError(err) => Some(err),
            TrackerError::IoError(err) => Some(err),
            _ => None,
        }
    }
}

mod test {
    use std::net::Ipv4Addr;

    use super::{Peer, TrackerResponse};

    // #[test]
    // fn test_tracker_response_from_bencoded_success_variant() {
    //     let bencoded_body =
    //         b"d8:completei990e10:incompletei63e8:intervali1800e10:tracker_id10:tracker12315:warning_message26:This is a warning message!5:peers6:\xb9}\xbe;\x1b\x14e";
    //
    //     let response = TrackerResponse::from_bencoded(bencoded_body).unwrap();
    //     let expected_response = TrackerResponse::Success {
    //         interval: 1800,
    //         min_interval: None,
    //         tracker_id: Some(String::from("tracker123")),
    //         // tracker_id: None,
    //         seeders: 990,
    //         leechers: 63,
    //         peers: vec![185, 125, 190, 59, 27, 20].into(),
    //         warning: Some(String::from("This is a warning message!")),
    //     };
    //
    //     assert_eq!(expected_response, response);
    // }
    //
    // #[test]
    // fn test_tracker_response_from_bencoded_failure_variant() {
    //     let bencoded_body = b"d14:failure_reason28:Error: Something went wrong!e";
    //
    //     let response = TrackerResponse::from_bencoded(bencoded_body).unwrap();
    //     let expected_response = TrackerResponse::Failure {
    //         message: "Error: Something went wrong!".to_string(),
    //     };
    //
    //     assert_eq!(expected_response, response);
    // }
    //
    // #[test]
    // fn test_peers_binary_model_decode() {
    //     let response = TrackerResponse::Success {
    //         tracker_id: None,
    //         interval: 100,
    //         min_interval: None,
    //         seeders: 20,
    //         leechers: 5,
    //         peers: vec![185, 125, 190, 59, 26, 247, 187, 125, 192, 48, 26, 233],
    //         warning: None,
    //     };
    //
    //     let expected = vec![
    //         Peer {
    //             peer_id: None,
    //             ip: Ip::IpV4(Ipv4Addr::new(185, 125, 190, 59)),
    //             port: 6903,
    //         },
    //         Peer {
    //             peer_id: None,
    //             ip: Ip::IpV4(Ipv4Addr::new(187, 125, 192, 48)),
    //             port: 6889,
    //         },
    //     ];
    //
    //     assert_eq!(expected, response.decode_peers().unwrap());
    // }
    //
    // #[test]
    // fn test_peers_binary_model_decode_error_invalid_format() {
    //     let response = TrackerResponse::Success {
    //         tracker_id: None,
    //         interval: 100,
    //         min_interval: None,
    //         seeders: 20,
    //         leechers: 5,
    //         peers: vec![185, 125, 190, 59, 26, 247, 187, 125, 192, 48],
    //         warning: None,
    //     };
    //
    //     assert!(matches!(
    //         response
    //             .decode_peers()
    //             .expect_err("Expected InvalidPeersFormat error"),
    //         TrackerError::InvalidPeersFormat,
    //     ));
    // }
    // #[test]
    // fn test_peers_binary_model_decode_error_empty_peers() {
    //     let response = TrackerResponse::Success {
    //         tracker_id: None,
    //         interval: 100,
    //         min_interval: None,
    //         seeders: 20,
    //         leechers: 5,
    //         peers: vec![],
    //         warning: None,
    //     };
    //
    //     assert!(matches!(
    //         response
    //             .decode_peers()
    //             .expect_err("Expected EmptyPeers error"),
    //         TrackerError::EmptyPeers,
    //     ));
    // }
}
