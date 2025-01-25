use percent_encoding::{percent_encode, NON_ALPHANUMERIC};
use serde::{Deserialize, Serialize};
use serde_bencode::de;
use serde_bytes::ByteBuf;
use std::net::Ipv4Addr;
use url::Url;

use crate::torrent::peer::{Ip, Peer};

use super::tracker::{Announce, Response, TrackerError, TrackerProtocol};

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
    async fn get_peers(
        &mut self,
        announce: &str,
        announce_data: &Announce,
    ) -> Result<Vec<Peer>, TrackerError> {
        let req_url = self.build_announce_request(announce, announce_data)?;
        let response = reqwest::get(&req_url)
            .await
            .map_err(|e| TrackerError::HttpRequestError(e))?;
        let bencoded_body = response
            .bytes()
            .await
            .map_err(|e| TrackerError::InvalidResponse(e))?;
        let response_parsed = de::from_bytes::<Response>(bencoded_body.as_ref())
            .map_err(|e| TrackerError::DecodeError(e))?;

        response_parsed.decode_peers()
    }
}

#[cfg(test)]
mod test {

    use crate::torrent::tracker::{
        http::HttpTracker,
        tracker::{Announce, Event},
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
            event: Some(Event::Started),
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
}
