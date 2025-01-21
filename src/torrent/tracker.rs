use serde::{Deserialize, Serialize};
use serde_bencode::de;
use serde_bytes::ByteBuf;
use std::{
    collections::HashMap,
    error::Error,
    fmt::Display,
    io,
    net::{IpAddr, Ipv4Addr},
};

use super::peer::{Ip, Peer};

#[derive(Serialize, Deserialize, Debug)]
pub struct Request {
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Response {
    pub failure_response: Option<String>,
    warning_message: Option<String>,
    pub interval: Option<u32>,
    min_interval: Option<u32>,
    tracker_id: Option<String>,
    pub complete: Option<u32>,   // number of seeders
    pub incomplete: Option<u32>, // number of leechers
    // TODO: deocde both Peers variants
    // peers: Option<Peers>,
    pub peers: Option<ByteBuf>,
}

impl Response {
    pub fn empty() -> Self {
        Response {
            failure_response: None,
            warning_message: None,
            interval: None,
            min_interval: None,
            tracker_id: None,
            complete: None,
            incomplete: None,
            peers: None,
        }
    }
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

impl Response {
    pub fn decode_peers(&self) -> Result<Vec<Peer>, TrackerError> {
        if let Some(peers_bytes) = &self.peers {
            if peers_bytes.len() % 6 != 0 {
                return Err(TrackerError::InvalidPeersFormat);
            }
            let peers: Vec<Peer> = peers_bytes
                .chunks(6)
                .map(|peer_bytes| Peer {
                    peer_id: None,
                    ip: Ip::IpV4(Ipv4Addr::new(
                        peer_bytes[0],
                        peer_bytes[1],
                        peer_bytes[2],
                        peer_bytes[3],
                    )),
                    port: u16::from_be_bytes([peer_bytes[4], peer_bytes[5]]),
                })
                .collect();
            return Ok(peers);
        }

        Err(TrackerError::EmptyPeers)
    }
}

impl Peer {
    pub fn address(&self) -> String {
        format!("{}:{}", &self.ip, &self.port)
    }
}

pub async fn get(url: &str) -> Result<Response, Box<dyn std::error::Error>> {
    let response = reqwest::get(url).await?;
    let bencoded_body = response.bytes().await?;
    let response_parsed = de::from_bytes::<Response>(bencoded_body.as_ref())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    Ok(response_parsed)
}

#[derive(Debug)]
pub enum TrackerError {
    InvalidPeersFormat,
    EmptyPeers,
    InvalidTrackerResponse,
    InvalidPacketSize,
    InvalidUTF8,
}

impl Display for TrackerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrackerError::InvalidPeersFormat => write!(
                f,
                "Invalid peers format: must be consist of multiples of 6 bytes."
            ),
            TrackerError::EmptyPeers => write!(f, "No peers bytes found in tracker's response"),
            TrackerError::InvalidTrackerResponse => write!(f, "Invalid tracker response"),
            TrackerError::InvalidPacketSize => write!(f, "Invalid packet size"),
            TrackerError::InvalidUTF8 => write!(f, "Invalid UTF-8 format"),
        }
    }
}

impl Error for TrackerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

#[cfg(test)]
mod test {
    use std::net::Ipv4Addr;

    use serde_bencode::de;
    use serde_bytes::ByteBuf;

    use crate::torrent::tracker::TrackerError;

    use super::{Ip, Peer, Response};

    #[test]
    fn test_bencode_reponse_decode() {
        let bencoded_body =
            b"d8:completei990e10:incompletei63e8:intervali1800e5:peers6:\xb9}\xbe;\x1b\x14e";

        let response = de::from_bytes::<Response>(bencoded_body.as_ref()).unwrap();

        let expected_response = Response {
            failure_response: None,
            warning_message: None,
            interval: Some(1800),
            min_interval: None,
            tracker_id: None,
            complete: Some(990),
            incomplete: Some(63),
            // peers: Some(Peers::Binary(vec![0xb9, 0xbe, 0x1b, 0x14])),
            peers: Some(vec![185, 125, 190, 59, 27, 20].into()),
        };

        assert_eq!(expected_response, response);
    }

    #[test]
    fn test_peers_binary_model_decode() {
        let response = Response {
            failure_response: None,
            warning_message: None,
            interval: None,
            min_interval: None,
            tracker_id: None,
            complete: None,
            incomplete: None,
            peers: Some(ByteBuf::from(vec![
                185, 125, 190, 59, 26, 247, 187, 125, 192, 48, 26, 233,
            ])),
        };

        let expected = vec![
            Peer {
                peer_id: None,
                ip: Ip::IpV4(Ipv4Addr::new(185, 125, 190, 59)),
                port: 6903,
            },
            Peer {
                peer_id: None,
                ip: Ip::IpV4(Ipv4Addr::new(187, 125, 192, 48)),
                port: 6889,
            },
        ];

        assert_eq!(expected, response.decode_peers().unwrap());
    }

    #[test]
    fn test_peers_binary_model_decode_error_invalid_format() {
        let response = Response {
            failure_response: None,
            warning_message: None,
            interval: None,
            min_interval: None,
            tracker_id: None,
            complete: None,
            incomplete: None,
            peers: Some(ByteBuf::from(vec![
                185, 125, 190, 59, 26, 247, 187, 125, 192, 48,
            ])),
        };

        assert!(matches!(
            response
                .decode_peers()
                .expect_err("Expected InvalidPeersFormat error"),
            TrackerError::InvalidPeersFormat,
        ));
    }
    #[test]
    fn test_peers_binary_model_decode_error_empty_peers() {
        let response = Response {
            failure_response: None,
            warning_message: None,
            interval: None,
            min_interval: None,
            tracker_id: None,
            complete: None,
            incomplete: None,
            peers: None,
        };

        assert!(matches!(
            response
                .decode_peers()
                .expect_err("Expected EmptyPeers error"),
            TrackerError::EmptyPeers,
        ));
    }
}
