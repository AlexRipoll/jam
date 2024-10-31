use serde::{Deserialize, Serialize};
use serde_bencode::{de, ser};
use serde_bytes::ByteBuf;
use std::io;

#[derive(Serialize, Deserialize, Debug)]
pub struct Metainfo {
    info: Info,
    announce: String,
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
    fn deserialize(data: &[u8]) -> io::Result<Metainfo> {
        de::from_bytes::<Metainfo>(data).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    fn serialize(metainfo: &Metainfo) -> io::Result<Vec<u8>> {
        ser::to_bytes(metainfo).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use io::Read;

    use super::*;

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
            "udp://tracker.leechers-paradise.org:6969".to_string()
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
            announce: "http://tracker.com/announce".to_string(),
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

        println!("{:?}", serialized_data);
        // Here you can check if serialized_data matches your expected bencoded output
        // For simplicity, we are not doing a specific check on the output
        assert!(
            !serialized_data.is_empty(),
            "Serialized data should not be empty"
        );
    }
}
