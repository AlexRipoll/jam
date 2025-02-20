use serde_bencode::de;
use std::collections::HashMap;
use std::net::Ipv4Addr;

use percent_encoding::{percent_encode, NON_ALPHANUMERIC};
use url::Url;

use crate::torrent::peer::{Ip, Peer};

use super::tracker::{Announce, TrackerError, TrackerProtocol};

pub struct HttpTracker {}

impl HttpTracker {
    pub fn new() -> HttpTracker {
        HttpTracker {}
    }

    fn build_announce_request(
        &mut self,
        announce: &str,
        announce_data: &Announce,
    ) -> Result<String, TrackerError> {
        // TODO: if both announce and announce_list are None, it means it is intended to be
        // distributed over a DHT or PEX

        // let announce = self
        //     .annouces
        //     .pop_front()
        //     .ok_or(TrackerError::EmptyAnnounceQueue)?;

        let mut url = Url::parse(&announce).map_err(|e| TrackerError::AnnounceParseError(e))?;

        // Encode info_hash and peer_id
        let info_hash =
            percent_encode(&announce_data.info_hash, NON_ALPHANUMERIC).collect::<String>();
        let encoded_peer_id = percent_encode(&announce_data.peer_id, NON_ALPHANUMERIC).to_string();

        url.set_query(Some(&format!("info_hash={info_hash}")));
        url.set_query(Some(&format!(
            "{}&{}",
            url.query().unwrap_or(""),
            &format!("peer_id={encoded_peer_id}")
        )));

        url.query_pairs_mut()
            .append_pair("port", &announce_data.port.to_string())
            .append_pair("uploaded", &announce_data.uploaded.to_string())
            .append_pair("downloaded", &announce_data.downloaded.to_string())
            .append_pair("left", &announce_data.left.to_string())
            .append_pair("compact", &announce_data.compact.to_string());

        Ok(url.to_string())
    }
}

impl TrackerProtocol for HttpTracker {
    type Response = TrackerResponse;

    async fn request_announce(
        &mut self,
        announce: &str,
        announce_data: &Announce,
    ) -> Result<Self::Response, TrackerError> {
        let req_url = self.build_announce_request(announce, announce_data)?;
        let response = reqwest::get(&req_url)
            .await
            .map_err(|e| TrackerError::HttpRequestError(e))?;
        let bencoded_body = response
            .bytes()
            .await
            .map_err(|e| TrackerError::InvalidResponse(e))?;

        TrackerResponse::from_bencoded(bencoded_body.as_ref())
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
    pub fn from_bencoded(response: &[u8]) -> Result<TrackerResponse, TrackerError> {
        // Deserialize the bencoded data into a temporary intermediate structure
        let raw: HashMap<String, serde_bencode::value::Value> = match de::from_bytes(response) {
            Ok(val) => val,
            Err(err) => return Err(TrackerError::ParseError(err)),
        };

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
            _ => 0,
        };

        let leechers = match raw.get("incomplete") {
            Some(serde_bencode::value::Value::Int(i)) if *i >= 0 => *i as u32,
            _ => 0,
        };

        let warning = raw.get("warning_message").and_then(|v| match v {
            serde_bencode::value::Value::Bytes(b) => Some(String::from_utf8_lossy(b).to_string()),
            _ => None,
        });

        let peers = match raw.get("peers") {
            Some(serde_bencode::value::Value::Bytes(bytes)) => bytes.clone(),
            _ => vec![], // No peers found
        };

        // Build the Ok variant
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

    pub fn decode_peers(&self) -> Result<Vec<Peer>, TrackerError> {
        match &self {
            TrackerResponse::Success { peers, .. } => {
                if !peers.is_empty() {
                    if peers.len() % 6 != 0 {
                        return Err(TrackerError::InvalidPeersFormat);
                    }
                    let peers: Vec<Peer> = peers
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
            }
            TrackerResponse::Failure { .. } => {}
        }

        Err(TrackerError::EmptyPeers)
    }
}

#[cfg(test)]
mod test {

    use std::net::Ipv4Addr;

    use crate::torrent::{
        peer::{Ip, Peer},
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
            .build_announce_request(announce, &announce_data)
            .unwrap();

        assert_eq!(
            "https://torrent.ubuntu.com/announce?info_hash=%124Vx%9A%BC%DE%F1%23Eg%89%AB%CD%EF%124Vx%9A&peer_id=%2DJM0100%2DXPGcHeKEmI45&port=6889&uploaded=0&downloaded=0&left=5665497088&compact=1",
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
