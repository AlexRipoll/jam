use std::collections::{HashMap, HashSet};

use protocol::piece::Piece;

use super::{metainfo::Metainfo, peer::Peer};

pub struct Torrent {
    peer_id: [u8; 20],
    pub metadata: Metadata,
    pub peers: HashSet<Peer>,
    status: Status,
}

impl Torrent {
    pub fn new(peer_id: [u8; 20], torrent_bytes: &[u8]) -> Torrent {
        let info_hash = Metainfo::compute_info_hash(&torrent_bytes).unwrap();
        let metainfo = Metainfo::deserialize(&torrent_bytes).unwrap();

        Torrent {
            peer_id,
            metadata: Metadata::new(info_hash, metainfo),
            peers: HashSet::new(),
            status: Status::Starting,
        }
    }
}

enum Status {
    Starting,
    Downloading,
    Paused,
}

#[derive(Debug)]
pub struct Metadata {
    pub info_hash: [u8; 20],
    pub name: String,
    comment: Option<String>,
    created_by: Option<String>,
    creation_date: Option<u64>,
    announce: Option<String>,
    announce_list: Option<Vec<Vec<String>>>,
    pub private: Option<u8>,
    pub piece_length: u64,
    pub total_length: u64,
    pub pieces: HashMap<u32, Piece>,
}

impl Metadata {
    pub fn new(info_hash: [u8; 20], metainfo: Metainfo) -> Self {
        let pieces = metainfo.parse_pieces().unwrap();
        Self {
            info_hash,
            name: metainfo.info.name,
            comment: metainfo.comment,
            created_by: metainfo.created_by,
            creation_date: metainfo.creation_date,
            announce: metainfo.announce,
            announce_list: metainfo.announce_list,
            private: metainfo.info.private,
            piece_length: metainfo.info.piece_length,
            total_length: (metainfo.info.pieces.chunks(20).count() as u64)
                .saturating_mul(metainfo.info.piece_length),
            pieces,
        }
    }
}
