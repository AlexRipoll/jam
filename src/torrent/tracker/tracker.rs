use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use std::{collections::HashMap, error::Error, fmt::Display, net::IpAddr};
use tokio::io;
use url::Url;

use crate::torrent::peer::Peer;

use super::{
    http::{self, HttpTracker},
    udp::{self, UdpTracker},
};

pub struct Tracker {
    announce: String,
    seeders: u64,
    leechers: u64,
    http: HttpTracker,
    udp: UdpTracker,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Announce {
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

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
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

impl Announce {
    // TODO: improve with builder pattern
    pub fn new(
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        port: u16,
        left: u64,
        num_want: i32,
    ) -> Announce {
        Announce {
            info_hash,
            peer_id,
            port,
            uploaded: 0,
            downloaded: 0,
            left,
            compact: 1,
            no_peer_id: 0,
            event: Event::Started,
            ip: None,
            num_want: Some(num_want),
            key: None,
            tracker_id: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Flags {
    min_request_interval: Option<u64>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TrackerResponse {
    Http(http::TrackerResponse),
    Udp(udp::TrackerResponse),
}

impl TrackerResponse {
    pub fn get_peers(&self) -> Result<Vec<Peer>, TrackerError> {
        match self {
            TrackerResponse::Http(response) => response.decode_peers(),
            TrackerResponse::Udp(response) => response.decode_peers(),
        }
    }
}

pub trait TrackerProtocol {
    type Response;

    async fn request_announce(
        &mut self,
        announce: &str,
        announce_data: &Announce,
    ) -> Result<Self::Response, TrackerError>;
}

impl Tracker {
    pub fn new(annouce_url: &str) -> Tracker {
        Tracker {
            announce: annouce_url.to_string(),
            seeders: 0,
            leechers: 0,
            http: HttpTracker::new(),
            udp: UdpTracker::new(),
        }
    }

    pub async fn request_announce(
        &mut self,
        announce: &str,
        announce_data: &Announce,
    ) -> Result<TrackerResponse, TrackerError> {
        // Parse the URL to determine the protocol
        let url = Url::parse(announce).map_err(|e| TrackerError::AnnounceParseError(e))?;
        match url.scheme() {
            "http" | "https" => {
                let response = self.http.request_announce(announce, announce_data).await?;
                return Ok(TrackerResponse::Http(response));
            }
            "udp" => {
                let response = self.udp.request_announce(announce, announce_data).await?;
                return Ok(TrackerResponse::Udp(response));
            }
            _ => Err(TrackerError::UnsupportedProtocol(url.scheme().to_string())),
        }
    }
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

    use crate::torrent::tracker::tracker::TrackerError;

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
