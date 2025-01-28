use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    fmt::Display,
    net::IpAddr,
};
use url::Url;

use crate::torrent::peer::Peer;

use super::{
    http::{self, HttpTracker},
    udp::{self, UdpTracker},
};

pub struct Tracker {
    port: u16,
    announce_list: VecDeque<String>,
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
    pub event: Option<Event>,
    pub ip: Option<IpAddr>,
    pub num_want: Option<i32>,
    pub key: Option<u32>,
    pub tracker_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, PartialEq, Eq)]
pub enum TrackerResponse {
    Http(http::TrackerResponse),
    Udp(udp::TrackerResponse),
}

pub trait TrackerProtocol {
    async fn get_peers(
        &mut self,
        announce: &str,
        announce_data: &Announce,
    ) -> Result<Vec<Peer>, TrackerError>;
}

impl Tracker {
    pub fn new(port: u16, annouce_urls: Vec<String>) -> Tracker {
        Tracker {
            port,
            announce_list: VecDeque::from(annouce_urls),
            seeders: 0,
            leechers: 0,
            http: HttpTracker::new(),
            udp: UdpTracker::new(),
        }
    }

    pub async fn request_peers(
        &mut self,
        announce_data: &Announce,
    ) -> Result<Vec<Peer>, TrackerError> {
        // Pop the next announce URL from the queue
        let announce_url = self
            .announce_list
            .pop_front()
            .ok_or(TrackerError::EmptyAnnounceQueue)?;

        // Parse the URL to determine the protocol
        let url = Url::parse(&announce_url).map_err(|e| TrackerError::AnnounceParseError(e))?;
        match url.scheme() {
            "http" | "https" => self.http.get_peers(&announce_url, announce_data).await,
            "udp" => self.udp.get_peers(&announce_url, announce_data).await,
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
            _ => None,
        }
    }
}
