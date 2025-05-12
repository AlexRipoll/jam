use reqwest::Client;
use serde_bencode::de;
use std::net::Ipv4Addr;
use std::{collections::HashMap, time::Duration};

use percent_encoding::{percent_encode, NON_ALPHANUMERIC};
use url::Url;

use crate::torrent::peer::{Ip, Peer};

use super::tracker::{Announce, TrackerError, TrackerProtocol};

/// Default timeout for HTTP tracker requests in seconds
const DEFAULT_TIMEOUT_SECS: u64 = 10;

/// HTTP tracker implementation
pub struct HttpTracker {
    client: Client,
}

impl HttpTracker {
    /// Creates a new HTTP tracker client
    pub fn new() -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .build()
            .unwrap_or_default();

        Self { client }
    }

    /// Creates a new HTTP tracker client with a custom timeout
    pub fn new_with_timeout(timeout: Duration) -> Self {
        let client = Client::builder()
            .timeout(timeout)
            .build()
            .unwrap_or_default();

        Self { client }
    }

    /// Builds the announce URL with query parameters
    fn build_announce_url(&self, announce: &str, data: &Announce) -> Result<Url, TrackerError> {
        // Parse the base URL
        let mut url = Url::parse(announce).map_err(TrackerError::AnnounceParseError)?;

        // Start with a clean query string
        url.set_query(None);

        // Manually build query string to avoid double encoding of binary data
        let mut query = String::new();

        // Add the binary fields first (info_hash and peer_id) with direct hex encoding
        // URL-encode the binary info_hash and peer_id
        let info_hash = percent_encode(&data.info_hash, NON_ALPHANUMERIC).to_string();
        let peer_id = percent_encode(&data.peer_id, NON_ALPHANUMERIC).to_string();
        query.push_str(&format!("info_hash={}", info_hash));
        query.push_str(&format!("&peer_id={}", peer_id));

        // Add the remaining parameters with normal URL encoding
        query.push_str(&format!("&port={}", data.port));
        query.push_str(&format!("&uploaded={}", data.uploaded));
        query.push_str(&format!("&downloaded={}", data.downloaded));
        query.push_str(&format!("&left={}", data.left));
        query.push_str(&format!("&compact={}", data.compact));

        // Add optional parameters
        if let Some(ip) = &data.ip {
            query.push_str(&format!("&ip={}", ip));
        }

        if let Some(num_want) = data.num_want {
            query.push_str(&format!("&numwant={}", num_want));
        }

        if let Some(key) = data.key {
            query.push_str(&format!("&key={}", key));
        }

        // Add event if not "None"
        if data.event as u8 != 0 {
            let event_str = match data.event as u8 {
                1 => "completed",
                2 => "started",
                3 => "stopped",
                _ => "started", // Default to started for unknown values
            };
            query.push_str(&format!("&event={}", event_str));
        }

        if let Some(tracker_id) = &data.tracker_id {
            query.push_str(&format!("&trackerid={}", tracker_id));
        }

        // Set the manually constructed query string
        url.set_query(Some(&query));

        Ok(url)
    }
}

impl TrackerProtocol for HttpTracker {
    type Response = TrackerResponse;

    async fn request_announce(
        &mut self,
        announce: &str,
        announce_data: &Announce,
    ) -> Result<Self::Response, TrackerError> {
        // Build the announcement URL with query parameters
        let url = self.build_announce_url(announce, announce_data)?;

        // Send the HTTP request
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(TrackerError::HttpRequestError)?;

        // Get the raw bytes from the response
        let response_bytes = response
            .bytes()
            .await
            .map_err(TrackerError::InvalidResponse)?;

        // Parse the bencoded response
        TrackerResponse::from_bencoded(&response_bytes)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum TrackerResponse {
    Success {
        tracker_id: Option<String>,
        interval: u32,
        min_interval: Option<u32>,
        seeders: u32,
        leechers: u32,
        peers: Vec<u8>,
        warning: Option<String>,
    },
    Failure {
        message: String,
    },
}

impl TrackerResponse {
    /// Parse a bencoded tracker response
    pub fn from_bencoded(response: impl AsRef<[u8]>) -> Result<TrackerResponse, TrackerError> {
        let bytes = response.as_ref();

        // Deserialize the bencoded data into a temporary intermediate structure
        let raw: HashMap<String, serde_bencode::value::Value> =
            de::from_bytes(bytes).map_err(TrackerError::ParseError)?;

        // Check for failure response
        if let Some(serde_bencode::value::Value::Bytes(msg)) = raw.get("failure_reason") {
            return Ok(TrackerResponse::Failure {
                message: String::from_utf8_lossy(msg).to_string(),
            });
        }

        // Extract fields for a successful response
        let interval = match raw.get("interval") {
            Some(serde_bencode::value::Value::Int(i)) if *i >= 0 => *i as u32,
            _ => return Err(TrackerError::ResponseMissingField("interval".to_string())),
        };

        let min_interval = raw.get("min interval").and_then(|v| match v {
            serde_bencode::value::Value::Int(i) if *i >= 0 => Some(*i as u32),
            _ => None,
        });

        let tracker_id = raw.get("tracker_id").and_then(|v| match v {
            serde_bencode::value::Value::Bytes(b) => Some(String::from_utf8_lossy(b).to_string()),
            _ => None,
        });

        let seeders = match raw.get("complete") {
            Some(serde_bencode::value::Value::Int(i)) if *i >= 0 => *i as u32,
            _ => 0, // Default to 0 if missing
        };

        let leechers = match raw.get("incomplete") {
            Some(serde_bencode::value::Value::Int(i)) if *i >= 0 => *i as u32,
            _ => 0, // Default to 0 if missing
        };

        let warning = raw.get("warning_message").and_then(|v| match v {
            serde_bencode::value::Value::Bytes(b) => Some(String::from_utf8_lossy(b).to_string()),
            _ => None,
        });

        let peers = match raw.get("peers") {
            Some(serde_bencode::value::Value::Bytes(bytes)) => bytes.clone(),
            Some(serde_bencode::value::Value::List(_)) => {
                // We don't support dictionary model peers currently, only compact model
                return Err(TrackerError::InvalidPeersFormat);
            }
            _ => return Err(TrackerError::ResponseMissingField("peers".to_string())),
        };

        // Build the success response
        Ok(TrackerResponse::Success {
            tracker_id,
            interval,
            min_interval,
            seeders,
            leechers,
            peers,
            warning,
        })
    }

    /// Decode the binary peers format into a list of Peer objects
    pub fn decode_peers(&self) -> Result<Vec<Peer>, TrackerError> {
        match self {
            TrackerResponse::Success { peers, .. } => {
                if peers.is_empty() {
                    return Err(TrackerError::EmptyPeers);
                }

                // Compact format: each peer is 6 bytes (4 for IPv4, 2 for port)
                if peers.len() % 6 != 0 {
                    return Err(TrackerError::InvalidPeersFormat);
                }

                let peers_list = peers
                    .chunks_exact(6)
                    .map(|chunk| {
                        let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
                        let port = u16::from_be_bytes([chunk[4], chunk[5]]);

                        Peer {
                            peer_id: None,
                            ip: Ip::IpV4(ip),
                            port,
                        }
                    })
                    .collect();

                Ok(peers_list)
            }
            TrackerResponse::Failure { .. } => Err(TrackerError::EmptyPeers),
        }
    }

    /// Get the announce interval from the response
    pub fn interval(&self) -> Result<u32, TrackerError> {
        match self {
            TrackerResponse::Success { interval, .. } => Ok(*interval),
            TrackerResponse::Failure { .. } => Err(TrackerError::ErrorResponse(
                "Cannot get interval from failure response".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod test {

    use std::net::Ipv4Addr;

    use crate::{
        torrent::peer::{Ip, Peer},
        tracker::{
            http::{HttpTracker, TrackerResponse},
            tracker::{Announce, Event, TrackerError},
        },
    };

    #[test]
    fn test_info_hash_url_encoding() {
        let peer_id_str = "-JM0100-XPGcHeKEmI45";
        let mut peer_id = [0u8; 20];
        peer_id.copy_from_slice(peer_id_str.as_bytes());

        let info_hash: [u8; 20] = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf1, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd,
            0xef, 0x12, 0x34, 0x56, 0x78, 0x9a,
        ];

        let announce_data = Announce {
            info_hash,
            peer_id,
            port: 6889,
            uploaded: 0,
            downloaded: 0,
            left: 5665497088,
            compact: 1,
            no_peer_id: 0,
            event: Event::Started,
            ip: None,
            num_want: None,
            key: None,
            tracker_id: None,
        };

        let mut tracker = HttpTracker::new();
        let announce = "https://torrent.ubuntu.com/announce";
        let url = tracker
            .build_announce_url(announce, &announce_data)
            .unwrap();

        assert_eq!(
            "https://torrent.ubuntu.com/announce?info_hash=%124Vx%9A%BC%DE%F1%23Eg%89%AB%CD%EF%124Vx%9A&peer_id=%2DJM0100%2DXPGcHeKEmI45&port=6889&uploaded=0&downloaded=0&left=5665497088&compact=1&event=started",
            url.as_str()
        );
    }

    #[test]
    fn test_tracker_response_from_bencoded_success_variant() {
        let bencoded_body =
            b"d8:completei990e10:incompletei63e8:intervali1800e10:tracker_id10:tracker12315:warning_message26:This is a warning message!5:peers6:\xb9}\xbe;\x1b\x14e";

        let response = TrackerResponse::from_bencoded(bencoded_body).unwrap();
        let expected_response = TrackerResponse::Success {
            interval: 1800,
            min_interval: None,
            tracker_id: Some(String::from("tracker123")),
            // tracker_id: None,
            seeders: 990,
            leechers: 63,
            peers: vec![185, 125, 190, 59, 27, 20].into(),
            warning: Some(String::from("This is a warning message!")),
        };

        assert_eq!(expected_response, response);
    }

    #[test]
    fn test_tracker_response_from_bencoded_failure_variant() {
        let bencoded_body = b"d14:failure_reason28:Error: Something went wrong!e";

        let response = TrackerResponse::from_bencoded(bencoded_body).unwrap();
        let expected_response = TrackerResponse::Failure {
            message: "Error: Something went wrong!".to_string(),
        };

        assert_eq!(expected_response, response);
    }

    #[test]
    fn test_peers_binary_model_decode() {
        let response = TrackerResponse::Success {
            tracker_id: None,
            interval: 100,
            min_interval: None,
            seeders: 20,
            leechers: 5,
            peers: vec![185, 125, 190, 59, 26, 247, 187, 125, 192, 48, 26, 233],
            warning: None,
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
        let response = TrackerResponse::Success {
            tracker_id: None,
            interval: 100,
            min_interval: None,
            seeders: 20,
            leechers: 5,
            peers: vec![185, 125, 190, 59, 26, 247, 187, 125, 192, 48],
            warning: None,
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
        let response = TrackerResponse::Success {
            tracker_id: None,
            interval: 100,
            min_interval: None,
            seeders: 20,
            leechers: 5,
            peers: vec![],
            warning: None,
        };

        assert!(matches!(
            response
                .decode_peers()
                .expect_err("Expected EmptyPeers error"),
            TrackerError::EmptyPeers,
        ));
    }
}
