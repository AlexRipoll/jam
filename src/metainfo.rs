use percent_encoding::{percent_encode, NON_ALPHANUMERIC};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_bencode::{de, ser};
use serde_bytes::ByteBuf;
use sha1::{Digest, Sha1};
use std::{collections::BTreeMap, io};

#[derive(Serialize, Deserialize, Debug)]
pub struct Metainfo {
    info: Info,
    #[serde(rename = "announce", skip_serializing_if = "Option::is_none")]
    announce: Option<String>,
    #[serde(rename = "announce-list", skip_serializing_if = "Option::is_none")]
    announce_list: Option<Vec<Vec<String>>>,
    #[serde(rename = "creation date", skip_serializing_if = "Option::is_none")]
    creation_date: Option<u64>,
    comment: Option<String>,
    #[serde(rename = "created by", skip_serializing_if = "Option::is_none")]
    created_by: Option<String>,
    encoding: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Files {
    length: u64,
    md5sum: Option<String>,
    path: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Info {
    name: String,
    length: Option<u64>,
    #[serde(rename = "piece length")]
    piece_length: u64,
    pieces: ByteBuf,
    private: Option<u8>,
    md5sum: Option<String>,
    files: Option<Vec<Files>>,
}

impl Metainfo {
    pub fn deserialize(data: &[u8]) -> io::Result<Metainfo> {
        de::from_bytes::<Metainfo>(data).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    pub fn serialize(metainfo: &Metainfo) -> io::Result<Vec<u8>> {
        ser::to_bytes(metainfo).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    pub fn build_tracker_url(&self, info_hash: Vec<u8>, port: u32) -> String {
        let mut announce = String::new();
        if let Some(n) = self.announce.as_ref() {
            announce.clone_from(n);
        } else if let Some(n) = self.announce_list.as_ref() {
            announce.clone_from(&n[0][0]);
        }

        let mut url = Url::parse(&announce).unwrap();

        let info_hash = percent_encode(&info_hash, NON_ALPHANUMERIC).collect::<String>();
        let peer_id = "-GT0001-123456789012".to_string();
        // TODO: generate peer_id
        let encoded_peer_id = percent_encode(peer_id.as_bytes(), NON_ALPHANUMERIC).to_string();

        url.set_query(Some(&format!("info_hash={info_hash}")));
        url.set_query(Some(&format!(
            "{}&{}",
            url.query().unwrap_or(""),
            &format!("peer_id={encoded_peer_id}")
        )));

        url.query_pairs_mut()
            .append_pair("port", &port.to_string())
            .append_pair("uploaded", "0")
            .append_pair("downloaded", "0")
            .append_pair("left", &self.info.length.unwrap_or(0).to_string())
            // TODO: parameterize
            .append_pair("compact", "1");

        url.to_string()
    }

    pub fn compute_info_hash(torrent_bytes: &[u8]) -> Vec<u8> {
        let decoded: BTreeMap<String, serde_bencode::value::Value> =
            de::from_bytes(torrent_bytes).unwrap();

        //  Extract the `info` dictionary
        let info_value = decoded
            .get("info")
            .ok_or("No `info` field found in the torrent file")
            .unwrap();

        // Bencode the `info` dictionary
        let bencoded_info = ser::to_bytes(info_value).unwrap();

        let mut hasher = Sha1::new();
        hasher.update(bencoded_info);

        hasher.finalize().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use io::Read;

    use super::*;

    #[test]
    fn test_info_hash_url_encoding() {
        // Sha1 sample (no the real one from the torrent specified in Metainfo struct bellow)
        let sha1: [u8; 20] = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf1, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd,
            0xef, 0x12, 0x34, 0x56, 0x78, 0x9a,
        ];

        let metainfo = Metainfo {
            info: Info {
                name: "ubuntu-24.10-desktop-amd64.iso".to_string(),
                length: Some(5665497088),
                piece_length: 262144,
                pieces: ByteBuf::default(),
                private: None,
                md5sum: None,
                files: None,
            },
            announce: Some("https://torrent.ubuntu.com/announce".to_string()),
            announce_list: Some(vec![
                vec!["https://torrent.ubuntu.com/announce".to_string()],
                vec!["https://ipv6.torrent.ubuntu.com/announce".to_string()],
            ]),
            creation_date: Some(1728557557),
            comment: Some("Ubuntu CD releases.ubuntu.com".to_string()),
            created_by: Some("mktorrent 1.1".to_string()),
            encoding: None,
        };

        let url = metainfo.build_tracker_url(sha1.to_vec(), 6889);

        assert_eq!(
            "https://torrent.ubuntu.com/announce?info_hash=%124Vx%9A%BC%DE%F1%23Eg%89%AB%CD%EF%124Vx%9A&peer_id=%2DGT0001%2D123456789012&port=6889&uploaded=0&downloaded=0&left=5665497088&compact=1",
            url.as_str()
        );
    }

    #[test]
    fn test_deserialize() {
        let mut file = File::open("sintel.torrent").unwrap();
        let mut buffer = Vec::new();

        // Read the entire file into buffer
        file.read_to_end(&mut buffer).unwrap();

        // Attempt to deserialize the hardcoded bencoded data
        let result = Metainfo::deserialize(&buffer);

        // Validate the result
        assert!(result.is_ok(), "Deserialization should succeed");
        let metainfo = result.unwrap();

        // Validate the contents of the deserialized Metainfo
        assert_eq!(
            metainfo.announce,
            Some("udp://tracker.leechers-paradise.org:6969".to_string())
        );
        assert_eq!(
            metainfo.announce_list,
            Some(vec![
                vec!["udp://tracker.leechers-paradise.org:6969".to_string()],
                vec!["udp://tracker.coppersurfer.tk:6969".to_string()],
                vec!["udp://tracker.opentrackr.org:1337".to_string()],
                vec!["udp://explodie.org:6969".to_string()],
                vec!["udp://tracker.empire-js.us:1337".to_string()],
                vec!["wss://tracker.btorrent.xyz".to_string()],
                vec!["wss://tracker.openwebtorrent.com".to_string()],
                vec!["wss://tracker.fastcast.nz".to_string()]
            ])
        );
        assert_eq!(metainfo.creation_date, Some(1490916637));
        assert_eq!(
            metainfo.comment,
            Some("WebTorrent <https://webtorrent.io>".to_string())
        );
        assert_eq!(
            metainfo.created_by,
            Some("WebTorrent <https://webtorrent.io>".to_string())
        );
        assert_eq!(metainfo.encoding, Some("UTF-8".to_string()));
        assert_eq!(metainfo.info.name, "Sintel");
        assert_eq!(metainfo.info.piece_length, 131072);
        assert_eq!(metainfo.info.length, None);
        assert_eq!(metainfo.info.pieces.len() % 20, 0);
        assert!(metainfo.info.files.is_some()); // Adjust based on your sample data
    }

    #[test]
    fn test_serialize() {
        // Create a sample Metainfo object
        let metainfo = Metainfo {
            info: Info {
                name: "example_file".to_string(),
                length: Some(123456789),
                piece_length: 16384,
                pieces: ByteBuf::from(vec![0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0]), // Example SHA1 hashes
                private: Some(0),
                md5sum: None,
                files: None,
            },
            announce: Some("http://tracker.com/announce".to_string()),
            announce_list: None,
            creation_date: Some(1730374859),
            comment: Some("Metainfo sample".to_string()),
            created_by: Some("Jam v0.0.1".to_string()),
            encoding: Some("UTF-8".to_string()),
        };

        // Attempt to serialize the Metainfo object
        let result = Metainfo::serialize(&metainfo);

        // Validate the result
        assert!(result.is_ok(), "Serialization should succeed");
        let serialized_data = result.unwrap();

        // Here you can check if serialized_data matches your expected bencoded output
        // For simplicity, we are not doing a specific check on the output
        assert!(
            !serialized_data.is_empty(),
            "Serialized data should not be empty"
        );
    }
}
