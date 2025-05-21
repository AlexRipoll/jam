use std::{
    collections::{HashMap, HashSet, VecDeque},
    error::Error,
    fmt::{self, Display},
    fs::File,
    io::{self, Read},
    path::Path,
    time::{Duration, Instant},
};

use hex::encode;
use protocol::{bitfield::Bitfield, piece::Piece};
use rand::{distributions::Alphanumeric, Rng};
use sha1::{Digest, Sha1};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tracing::{debug, error, info, warn};

use crate::{
    config::Config,
    core::orchestrator::{Orchestrator, OrchestratorConfig},
    events::Event,
    torrent::status::{create_progress_bar, format_bytes, format_eta, format_speed, format_status},
    tracker::tracker::{Event as AnnounceEvent, TrackerManager},
};

use super::{
    metainfo::{Metainfo, MetainfoError},
    peer::Peer,
};

// Define TorrentManager to handle multiple downloads
#[derive(Debug)]
pub struct TorrentManager<'a> {
    torrents: HashMap<[u8; 20], TorrentHandle>,
    config: &'a Config,
    peer_id: [u8; 20],
}

impl<'a> TorrentManager<'a> {
    pub fn new(config: &'a Config) -> Self {
        TorrentManager {
            peer_id: generate_peer_id(),
            torrents: HashMap::new(),
            config,
        }
    }

    pub async fn start_torrent(&mut self, file_path: &str) -> Result<String, TorrentError> {
        debug!("Opening torrent file: {}", file_path);
        let mut file = File::open(file_path)?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        debug!("Computing info hash...");
        let info_hash = Metainfo::compute_info_hash(&buffer)?;
        debug!(info_hash = ?hex::encode(info_hash), "Info hash computed");

        debug!("Deserializing metainfo file");
        let metainfo = Metainfo::deserialize(&buffer)?;

        let info_name = metainfo.info.name.clone();
        debug!(torrent_name = ?info_name, "Successfully deserialized metainfo");

        let torrent = Torrent::new(file_path, self.peer_id, &buffer, &self.config);
        let torrent_id = torrent.metadata.info_hash;

        let (torrent_tx, torrent_handle) = torrent.run().await;

        // encode to hexadecimal for human representation
        let torrent_id_hex = encode(torrent_id);

        // Store the handle
        self.torrents.insert(
            torrent_id,
            TorrentHandle {
                tx: torrent_tx,
                _handle: torrent_handle,
            },
        );

        Ok(torrent_id_hex)
    }

    async fn torrents_states(&self) -> Result<Vec<TorrentState>, TorrentError> {
        let mut statuses = Vec::new();
        let timeout_duration = Duration::from_secs(10);

        for (id, handle) in &self.torrents {
            let (response_tx, response_rx) = oneshot::channel();

            // Send status query to the torrent
            if let Err(e) = handle
                .tx
                .send(TorrentCommand::QueryStatus {
                    response_channel: response_tx,
                })
                .await
            {
                println!(
                    "Failed to send status query to torrent {}: {}",
                    hex::encode(id),
                    e
                );
                continue;
            }

            // Wait for response with timeout
            match timeout(timeout_duration, response_rx).await {
                Ok(Ok(status)) => statuses.push(status),
                Ok(Err(e)) => warn!(
                    "Failed to receive status from torrent {}: {}",
                    hex::encode(id),
                    e
                ),
                Err(_) => {
                    return Err(TorrentError::Timeout(format!(
                        "Timeout waiting for status from torrent {}",
                        hex::encode(id)
                    )));
                }
            }
        }

        Ok(statuses)
    }

    pub async fn display_torrents_state(&self) {
        let states = match self.torrents_states().await {
            Ok(states) => states,
            Err(err) => {
                println!("Failed to retrieve torrent states: {}", err);
                return;
            }
        };

        if states.is_empty() {
            println!("No active torrents");
            return;
        }

        // Print header
        println!();
        println!("┌──────────────────────────────────────────────────────────────────────────────────────────────────┐");
        println!("│                                 TORRENT STATUS                                                   │");
        println!("├──────────────────────────────────────────────────────────────────────────────────────────────────┤");
        println!("│ Name                   │ Progress        │ Size                │ Speed  │ ETA   │ Peers │ Status │");
        println!("├──────────────────────────────────────────────────────────────────────────────────────────────────┤");

        for status in states {
            let truncated_name = if status.name.len() > 22 {
                format!("{}...", &status.name[..19])
            } else {
                format!("{:<22}", status.name)
            };

            let progress_bar = create_progress_bar(status.download_state.progress_percentage);
            let downloaded_display = format_bytes(status.download_state.downloaded_bytes);
            let size_display = format_bytes(
                status.download_state.downloaded_bytes + status.download_state.left_bytes,
            );
            let speed_display = format_speed(status.download_speed);
            let eta_display = format_eta(status.eta);
            let status_display = format_status(&status.status);

            println!(
                "│ {} │ {} │ {:>8} of {:>8}│ {:>6} │ {:>5} │ {:>5} │ {:>6} │",
                truncated_name,
                progress_bar,
                downloaded_display,
                size_display,
                speed_display,
                eta_display,
                status.peers_count,
                status_display
            );
        }

        println!("└──────────────────────────────────────────────────────────────────────────────────────────────────┘");
        println!();
    }

    pub async fn display_compact_state(&self) {
        let states = match self.torrents_states().await {
            Ok(states) => states,
            Err(err) => {
                println!("Failed to retrieve torrent states: {}", err);
                return;
            }
        };

        if states.is_empty() {
            println!("No active torrents");
            return;
        }

        for status in states {
            let progress_bar = create_progress_bar(status.download_state.progress_percentage);
            let speed = format_speed(status.download_speed);
            let eta = format_eta(status.eta);

            println!(
                "{} {} {}% ↓{} ETA:{} [{}]",
                status.name,
                progress_bar,
                status.download_state.progress_percentage,
                speed,
                eta,
                format_status(&status.status)
            );
        }
    }

    pub async fn display_pieces_maps(&self) {
        let states = match self.torrents_states().await {
            Ok(states) => states,
            Err(err) => {
                println!("Failed to retrieve torrent states: {}", err);
                return;
            }
        };

        if states.is_empty() {
            println!("No active torrents");
            return;
        }

        for state in &states {
            let total_pieces = state.bitfield.total_pieces;
            let filled_pieces = state
                .bitfield
                .bytes
                .iter()
                .fold(0, |acc, &byte| acc + byte.count_ones() as usize);

            println!("┌{}┐", "─".repeat(102));
            println!("│ {:<100} │", format!("Piece Map: {}", state.name));
            println!(
                "│ {:<100} │",
                format!(
                    "Progress: {}/{} pieces ({:.1}%)",
                    filled_pieces,
                    total_pieces,
                    (filled_pieces as f64 / total_pieces as f64) * 100.0
                )
            );
            println!("├{}┤", "─".repeat(102));

            // Display the piece map with a nice border
            let width = 100;
            let map = display_bitfield(&state.bitfield, width);
            for line in map.lines() {
                println!("│ {:<102} │", line);
            }

            println!("└{}┘", "─".repeat(102));
            println!("Legend: ■ = Downloaded piece  ▪ = Missing piece");
            println!();
        }
    }

    // async fn stop_torrent(&self, id: usize) -> bool {
    //     if let Some(handle) = self.torrents.get(&id) {
    //         let _ = handle.tx.send(TorrentCommand::Pause).await;
    //         return true;
    //     }
    //     false
    // }
    //
    // async fn resume_torrent(&self, id: usize) -> bool {
    //     if let Some(handle) = self.torrents.get(&id) {
    //         let _ = handle.tx.send(TorrentCommand::Resume).await;
    //         return true;
    //     }
    //     false
    // }
    //
    // async fn cancel_torrent(&mut self, id: usize) -> bool {
    //     if let Some(handle) = self.torrents.get(&id) {
    //         let _ = handle.tx.send(TorrentCommand::Cancel).await;
    //         return true;
    //     }
    //     false
    // }
    //
    // fn get_status(&self) -> Vec<(usize, String, TorrentState)> {
    //     let mut result = Vec::new();
    //     for (&id, handle) in &self.torrents {
    //         let handle = handle.lock().unwrap();
    //         result.push((id, handle.info_name.clone(), handle.state));
    //     }
    //     result
    // }
    //
    // fn get_stats(&self, id: usize) -> Option<TorrentStats> {
    //     if let Some(handle) = self.torrents.get(&id) {
    //         return Some(handle.stats.clone());
    //     }
    //     None
    // }
}

#[derive(Debug, Clone)]
struct Torrent<'a> {
    file_path: String,
    peer_id: [u8; 20],
    pub metadata: Metadata,
    pub peers: HashSet<Peer>,
    status: Status,
    download_state: DownloadState,
    config: &'a Config,
}

#[derive(Debug, Clone)]
pub struct DownloadState {
    pub downloaded_bytes: u64,
    pub uploaded_bytes: u64,
    pub left_bytes: u64,
    pub progress_percentage: u64,
    pub bitfield: Bitfield,
}

impl DownloadState {
    fn new(total_size: u64, total_pieces: usize) -> Self {
        Self {
            downloaded_bytes: 0,
            uploaded_bytes: 0,
            left_bytes: 0,
            progress_percentage: 0,
            bitfield: Bitfield::new(total_pieces),
        }
    }
}

impl fmt::Display for DownloadState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Format byte counts to be more readable
        let format_bytes = |bytes: u64| -> String {
            const KB: u64 = 1024;
            const MB: u64 = KB * 1024;
            const GB: u64 = MB * 1024;

            if bytes >= GB {
                format!("{:.2} GB", bytes as f64 / GB as f64)
            } else if bytes >= MB {
                format!("{:.2} MB", bytes as f64 / MB as f64)
            } else if bytes >= KB {
                format!("{:.2} KB", bytes as f64 / KB as f64)
            } else {
                format!("{} B", bytes)
            }
        };

        // Create a simple progress bar
        let bar_width = 20;
        let filled = (self.progress_percentage as usize * bar_width) / 100;
        let empty = bar_width - filled;
        let progress_bar = format!("[{}{}]", "#".repeat(filled), "-".repeat(empty));

        // Format the complete output
        write!(
            f,
            "{} {}% | ↓ {} | ↑ {} | left: {}",
            progress_bar,
            self.progress_percentage,
            format_bytes(self.downloaded_bytes),
            format_bytes(self.uploaded_bytes),
            format_bytes(self.left_bytes)
        )
    }
}

#[derive(Debug)]
struct TorrentHandle {
    /// Channel to send events to the torrent
    tx: mpsc::Sender<TorrentCommand>,
    /// Handle to the torrent task
    _handle: JoinHandle<()>,
}

#[derive(Debug)]
pub enum TorrentCommand {
    /// Download state statistics
    DownloadState {
        downloaded_pieces: u64,
        uploaded_pieces: u64,
        left_pieces: u64,
        progress_percentage: u64,
        bitfield: Bitfield,
    },
    DownloadCompleted,
    /// Query current download state
    QueryStatus {
        response_channel: oneshot::Sender<TorrentState>,
    },
}

#[derive(Debug)]
pub struct TorrentState {
    pub id: String,
    pub name: String,
    pub status: Status,
    pub download_state: DownloadState,
    pub peers_count: usize,
    pub bitfield: Bitfield,
    pub eta: Option<Duration>, // TODO: tbi
    pub download_speed: f64,   // bytes per second TODO:tbi
    pub upload_speed: f64,     // bytes per second TODO:tbi
}

impl<'a> Torrent<'a> {
    pub fn new(
        file_path: &'a str,
        peer_id: [u8; 20],
        torrent_bytes: &'a [u8],
        config: &'a Config,
    ) -> Torrent<'a> {
        let info_hash = Metainfo::compute_info_hash(&torrent_bytes).unwrap();
        let metainfo = Metainfo::deserialize(&torrent_bytes).unwrap();
        let total_pieces = metainfo.total_pieces();
        let total_size = metainfo.info.length.unwrap_or(0);

        Torrent {
            file_path: file_path.to_string(),
            peer_id,
            metadata: Metadata::new(info_hash, metainfo),
            peers: HashSet::new(),
            status: Status::Starting,
            download_state: DownloadState::new(total_size, total_pieces),
            config,
        }
    }

    pub async fn run(mut self) -> (mpsc::Sender<TorrentCommand>, JoinHandle<()>) {
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<TorrentCommand>(128);
        let start = Instant::now();

        // Collect all announce URLs
        let mut announce_urls = Vec::new();

        // Handle announce_list (tiers of trackers) if available
        if let Some(announce_list) = self.metadata.announce_list.clone() {
            // Flatten all tiers into a single list of URLs
            for tier in announce_list {
                announce_urls.extend(tier);
            }
        }
        // Add the main announce URL if it exists
        else if let Some(announce) = self.metadata.announce.clone() {
            announce_urls.push(announce);
        }

        // FIX:
        let port = 6889;
        let num_want = 200;

        let mut tracker_manager = TrackerManager::new(self.metadata.info_hash, self.peer_id, port);

        // Add all trackers to the manager
        if !announce_urls.is_empty() {
            let results = tracker_manager.add_trackers(&announce_urls).await;

            // Optional: Log which trackers were successfully added
            for (i, result) in results.iter().enumerate() {
                if result.is_err() {
                    // Log error or handle failed tracker
                    warn!("Failed to add tracker {}: {:?}", announce_urls[i], result);
                }
            }
        }

        // Now you can make an announce request if at least one tracker was added
        // if !tracker_manager.trackers().is_empty() {
        let tracker_resonse = tracker_manager
            .announce(
                self.metadata.info_hash,
                self.peer_id,
                6889,                       // port
                0,                          // downloaded
                0,                          // uploaded
                self.metadata.total_length, // left
                AnnounceEvent::Started,
                Some(num_want), // num_want
            )
            .await
            .unwrap();
        // }

        let peers = tracker_resonse.peers;
        let announce_interval = tracker_resonse.interval;

        let state_interval: u64 = 5;

        // TODO: Inform the tracker that the client is gracefully stopping its participation in this torrent.
        // TODO: Handling Multiple Trackers (Tracker Tiers) -> Improve reliability by using backup trackers if the primary one fails.

        let new_peers: Vec<Peer> = peers
            .clone()
            .into_iter()
            .filter(|item| !self.peers.contains(item))
            .collect();

        self.peers.extend(new_peers.clone());

        let absolute_download_path =
            Path::new(&self.config.disk.download_path).join(&self.metadata.name);

        let orchestrator_config = OrchestratorConfig {
            max_connections: self.config.network.max_peer_connections as usize,
            queue_capacity: self.config.network.queue_capacity as usize,
            download_path: absolute_download_path,
            file_size: self.metadata.total_length,
            pieces_size: self.metadata.piece_length,
            block_size: self.config.network.block_size,
            timeout_threshold: self.config.network.timeout_threshold,
        };

        let orchestrator = Orchestrator::new(
            self.peer_id.clone(),
            self.metadata.info_hash,
            self.metadata.pieces.clone(),
            // VecDeque::from_iter(self.peers.clone()),
            // should be passed by config
            orchestrator_config,
            cmd_tx.clone(),
        );

        let (orchestrator_tx, orchestrator_handle) = orchestrator.run().await;

        let _ = orchestrator_tx
            .send(Event::AddPeers { peers: new_peers })
            .await;

        // Create an interval for periodic tracker updates
        // The interval value comes from the tracker's response
        let mut update_interval =
            tokio::time::interval(tokio::time::Duration::from_secs(announce_interval as u64));
        let mut state_check_interval =
            tokio::time::interval(tokio::time::Duration::from_secs(state_interval as u64));

        // update status to `downloading``
        self.status = Status::Downloading;

        // Create a clone of cmd_tx to return
        let return_cmd_tx = cmd_tx.clone();

        // Spawn the main loop as a task and get its handle
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Handle commands from the orchestrator
                    Some(event) = cmd_rx.recv() => {
                        match event {
                            TorrentCommand::DownloadState { downloaded_pieces, uploaded_pieces, left_pieces, progress_percentage, bitfield } => {
                                debug!("Download progress: {progress_percentage}%");
                                self.download_state.downloaded_bytes = downloaded_pieces.saturating_mul(self.metadata.piece_length);
                                self.download_state.uploaded_bytes = uploaded_pieces.saturating_mul(self.metadata.piece_length);
                                self.download_state.left_bytes = left_pieces.saturating_mul(self.metadata.piece_length);
                                self.download_state.progress_percentage = progress_percentage;
                                self.download_state.bitfield = bitfield;
                                info!("Download state: {}", self.download_state);
                            },
                            TorrentCommand::QueryStatus { response_channel } => {
                                // Create a channel to receive the connected peers count from orchestrator
                                let (peers_tx, peers_rx) = oneshot::channel();

                                // Send query to orchestrator
                                if let Err(_) = orchestrator_tx.send(Event::QueryConnectedPeers {
                                    response_channel: peers_tx
                                }).await {
                                    // If we can't communicate with orchestrator, fall back to 0
                                    warn!("Failed to query connected peers from orchestrator");
                                    let response = TorrentState {id:hex::encode(self.metadata.info_hash),name:self.metadata.name.clone(),status:self.status.clone(),download_state:self.download_state.clone(), bitfield: self.download_state.bitfield.clone(), peers_count:0,eta:None,download_speed:0.0,upload_speed:0.0};
                                    let _ = response_channel.send(response);
                                    continue;
                                }

                                // Wait for response from orchestrator with a timeout
                                match timeout(Duration::from_secs(5), peers_rx).await {
                                    Ok(Ok(peers_count)) => {
                                        let response = TorrentState {
                                            id: hex::encode(self.metadata.info_hash),
                                            name: self.metadata.name.clone(),
                                            status: self.status.clone(),
                                            download_state: self.download_state.clone(),
                                            bitfield: self.download_state.bitfield.clone(),
                                            peers_count,
                                            eta: None,
                                            download_speed: 0.0,
                                            upload_speed: 0.0,
                                        };
                                        let _ = response_channel.send(response);
                                    },
                                    Ok(Err(_)) => {
                                        warn!("Orchestrator failed to respond with connected peers count");
                                        let response = TorrentState {
                                            id: hex::encode(self.metadata.info_hash),
                                            name: self.metadata.name.clone(),
                                            status: self.status.clone(),
                                            download_state: self.download_state.clone(),
                                            bitfield: self.download_state.bitfield.clone(),
                                            peers_count: 0,
                                            eta: None,
                                            download_speed: 0.0,
                                            upload_speed: 0.0,
                                        };
                                        let _ = response_channel.send(response);
                                    },
                                    Err(_) => {
                                        warn!("Timeout waiting for connected peers count from orchestrator");
                                        let response = TorrentState {
                                            id: hex::encode(self.metadata.info_hash),
                                            name: self.metadata.name.clone(),
                                            status: self.status.clone(),
                                            download_state: self.download_state.clone(),
                                            bitfield: self.download_state.bitfield.clone(),
                                            peers_count: 0,
                                            eta: None,
                                            download_speed: 0.0,
                                            upload_speed: 0.0,
                                        };
                                        let _ = response_channel.send(response);
                                    }
                                }
                            },
                            TorrentCommand::DownloadCompleted => {
                                debug!("Download completed!");
                                self.download_state.downloaded_bytes = self.metadata.total_length;
                                self.download_state.left_bytes = 0;
                                self.download_state.progress_percentage = 100;

                                // Send "completed" event to tracker
                                let info_hash = self.metadata.info_hash;
                                let peer_id = self.peer_id;
                                let event = AnnounceEvent::Completed;
                                let downloaded = self.download_state.downloaded_bytes;
                                let uploaded = 0;
                                let left = self.download_state.left_bytes;

                                info!("Download state: {}", self.download_state);

                                let tracker_manager_clone = tracker_manager.clone();
                                // Fire and forget - we don't need to wait for the response
                                tokio::spawn(async move {
                                    if let Err(e) = tracker_manager_clone.announce(info_hash, peer_id, port, downloaded, uploaded, left, event, None).await {
                                        error!("Failed to send completion event to tracker: {}", e);
                                    }
                                });

                                info!("Download completed successfully");
                                let duration = start.elapsed();
                                debug!("Time elapsed: {:.2?}", duration);

                                self.status = Status::Completed;

                                // Abort the orchestrator task
                                orchestrator_handle.abort();
                            }
                        }
                    },

                    // Periodic tracker updates
                    _ = update_interval.tick() => {
                        // Send update (no specific event type for regular updates)
                        debug!("Sending periodic update to tracker. Progress: {}%, Left: {} bytes", self.download_state.progress_percentage, self.download_state.left_bytes);
                                let event= AnnounceEvent::Completed;
                                let downloaded = self.download_state.downloaded_bytes;
                                let uploaded = 0;
                                let left = self.download_state.left_bytes;


                        match tracker_manager.announce(self.metadata.info_hash,self.peer_id,port, downloaded, uploaded, left, event, None).await {
                            Ok(response) => {
                                // Process any new peers
                                    let filtered_peers: Vec<Peer> = response.peers
                                        .into_iter()
                                        .filter(|item| !self.peers.contains(item))
                                        .collect();

                                    if !filtered_peers.is_empty() {
                                        debug!("Received {} new peers from tracker", filtered_peers.len());
                                        self.peers.extend(filtered_peers.clone());
                                        let _ = orchestrator_tx
                                            .send(Event::AddPeers { peers: filtered_peers })
                                            .await;
                                    }

                                // Update interval if the tracker suggests a new one
                                    if response.interval != announce_interval {
                                        debug!("Tracker suggested new update interval: {} seconds", response.interval );
                                        update_interval = tokio::time::interval(tokio::time::Duration::from_secs(response.interval as u64));
                                    }
                            },
                            Err(e) => {
                                debug!("Failed to send periodic update to tracker: {}", e);
                                // TODO: Maybe try alternate trackers if this one fails
                            }
                        }
                    },
                    // Periodic state check
                    _ = state_check_interval.tick() => {
                        let _ = orchestrator_tx
                            .send(Event::QueryDownloadState { response_channel: cmd_tx.clone() })
                            .await;
                    },
                }
            }
        });

        // Return the command sender and join handle
        (return_cmd_tx, handle)
    }
}

#[derive(Debug, Clone)]
pub enum Status {
    Starting,
    Downloading,
    Paused,
    Completed,
    Error(String),
}

#[derive(Clone)]
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

#[derive(Debug)]
pub enum TorrentError {
    /// Metainfo related error
    Metainfo(MetainfoError),

    /// Standard I/O error
    Io(io::Error),

    Timeout(String),
}

impl Display for TorrentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TorrentError::Metainfo(err) => write!(f, "Metainfo error: {}", err),
            TorrentError::Io(err) => write!(f, "I/O error: {}", err),
            TorrentError::Timeout(msg) => write!(f, "Timeout error: {}", msg),
        }
    }
}

impl Error for TorrentError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            TorrentError::Metainfo(err) => Some(err),
            TorrentError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<MetainfoError> for TorrentError {
    fn from(err: MetainfoError) -> Self {
        TorrentError::Metainfo(err)
    }
}

impl From<io::Error> for TorrentError {
    fn from(err: io::Error) -> Self {
        TorrentError::Io(err)
    }
}

// Bitfield display options
pub enum BitfieldDisplayStyle {
    /// Simple ASCII blocks (# and .)
    Simple,
    /// Unicode blocks with color (if supported)
    Fancy,
    /// Heat map style with gradients
    HeatMap,
}

// Displays bitfield with unicode blocks with pseudo-color indicators
fn display_bitfield(bitfield: &Bitfield, width: usize) -> String {
    // Use different Unicode block characters for better visualization
    // □ ■ ▢ ▣ ▤ ▥ ▦ ▧ ▨ ▩ ▪ ▫ ▬ ▭ ▮ ▯
    let mut result = String::new();
    let mut count = 0;

    // Check if terminal supports ANSI colors
    let use_colors = atty::is(atty::Stream::Stdout);

    for byte in &bitfield.bytes {
        for bit in 0..8 {
            if count >= bitfield.total_pieces {
                break;
            }

            let is_set = (byte & (1 << (7 - bit))) != 0;

            if is_set {
                if use_colors {
                    result.push_str("\x1b[32m"); // Green color
                    result.push('■');
                    result.push_str("\x1b[0m"); // Reset color
                } else {
                    result.push('■');
                }
            } else {
                if use_colors {
                    result.push_str("\x1b[90m"); // Gray color
                    result.push('▪');
                    result.push_str("\x1b[0m"); // Reset color
                } else {
                    result.push('▪');
                }
            }

            count += 1;

            // Add newline for width formatting
            if count % width == 0 && count < bitfield.total_pieces {
                result.push('\n');
            }
        }
    }

    result
}
