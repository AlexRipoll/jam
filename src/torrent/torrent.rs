use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    path::Path,
};

use protocol::piece::Piece;
use rand::{distributions::Alphanumeric, Rng};
use tokio::sync::mpsc;
use tracing::debug;

use crate::config::Config;

use super::{
    core::orchestrator::Orchestrator,
    metainfo::Metainfo,
    peer::Peer,
    tracker::tracker::{Announce, Tracker},
};

#[derive(Debug)]
pub struct Torrent {
    peer_id: [u8; 20],
    pub metadata: Metadata,
    pub peers: HashSet<Peer>,
    status: Status,
    config: Config,
}

#[derive(Debug)]
pub enum TorrentCommand {
    DownloadState { percent: usize },
    DownloadCompleted,
}

impl Torrent {
    pub fn new(peer_id: [u8; 20], torrent_bytes: &[u8], config: Config) -> Torrent {
        let info_hash = Metainfo::compute_info_hash(&torrent_bytes).unwrap();
        let metainfo = Metainfo::deserialize(&torrent_bytes).unwrap();
        let total_pieces = metainfo.total_pieces();

        Torrent {
            peer_id,
            metadata: Metadata::new(info_hash, metainfo),
            peers: HashSet::new(),
            status: Status::Starting,
            config,
        }
    }

    pub async fn run(&mut self) {
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<TorrentCommand>(100);

        let mut announces_queue: VecDeque<Vec<String>> = VecDeque::new();
        // extract either the announce_list or announce
        if let Some(announce_list) = self.metadata.announce_list.clone() {
            announces_queue = VecDeque::from(announce_list);
        } else {
            announces_queue.push_front(vec![self.metadata.announce.clone().unwrap()]);
        }

        let announces = announces_queue.pop_front().unwrap();
        let mut tracker = Tracker::new(&announces[0]);
        let port = 6889;
        let numwant = 200;
        let announce_data = Announce::new(
            self.metadata.info_hash,
            self.peer_id,
            port,
            self.metadata.total_length,
            numwant,
        );
        let response = tracker
            .request_announce(&announces[0], &announce_data)
            .await
            .unwrap();
        let peers = response.get_peers().unwrap();

        let new_peers: Vec<Peer> = peers
            .clone()
            .into_iter()
            .filter(|item| !self.peers.contains(item))
            .collect();

        self.peers.extend(new_peers.clone());

        let absolute_download_path =
            Path::new(&self.config.disk.download_path).join(&self.metadata.name);

        let orchestrator = Orchestrator::new(
            self.peer_id.clone(),
            self.metadata.info_hash,
            self.config.network.max_peer_connections as usize,
            self.config.network.queue_capacity as usize,
            self.metadata.pieces.clone(),
            // VecDeque::from_iter(self.peers.clone()),
            VecDeque::new(),
            absolute_download_path, // should be passed by config
            self.metadata.total_length,
            self.metadata.piece_length,
            self.config.network.block_size,
            self.config.network.timeout_threshold, // should be passed by config
            cmd_tx.clone(),
        );

        // -> send download event orchestrator (start, halt)
        // -> request download state to orchestrator
        // <- receive download completion from orchestrator
        // -> send message to tracker every 30-60 seconds or when start or complete event
        let (orchestrator_tx, orchestrator_handle) = orchestrator.run().await;

        let _ = orchestrator_tx
            .send(super::events::Event::AddPeers { peers: new_peers })
            .await;

        // -> send peers to orchestrator
        // <- wait until receiving completion event from orchestrator
        while let Some(event) = cmd_rx.recv().await {
            match event {
                TorrentCommand::DownloadState { percent } => debug!("{percent}%"),
                TorrentCommand::DownloadCompleted => {
                    orchestrator_handle.abort();
                    break;
                }
            }
        }
    }
}

#[derive(Debug)]
enum Status {
    Starting,
    Downloading,
    Paused,
}

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

impl fmt::Debug for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Torrent Metadata {{
                name: {:?},
                comment: {:?},
                created_by: {:?},
                creation_date: {:?},
                announce: {:?},
                announce list: {:?},
                private: {:?},
                piece size: {:?},
                total size: {:?},
            }}",
            self.name,
            self.comment.clone().unwrap_or("not defined".to_string()),
            self.created_by.clone().unwrap_or("not defined".to_string()),
            self.creation_date.clone().unwrap_or(0),
            self.announce.clone().unwrap_or("not defined".to_string()),
            self.announce_list.clone().unwrap_or(vec![]),
            self.private.clone().unwrap_or(0),
            self.piece_length,
            self.total_length,
        )
    }
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

// TODO: move to client module
fn client_version() -> String {
    let version_tag = env!("CARGO_PKG_VERSION").replace('.', "");
    let version = version_tag
        .chars()
        .filter(|c| c.is_ascii_digit())
        .take(4)
        .collect::<String>();

    // Ensure exactly 4 characters, padding with "0" if necessary
    format!("{:0<4}", version)
}

pub fn generate_peer_id() -> [u8; 20] {
    let version = client_version();
    let client_id = "JM";

    // Generate a 12-character random alphanumeric sequence
    let random_seq: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(12)
        .map(char::from)
        .collect();

    let format = format!("-{}{}-{}", client_id, version, random_seq);

    let mut id = [0u8; 20];
    id.copy_from_slice(format.as_bytes());

    id
}
