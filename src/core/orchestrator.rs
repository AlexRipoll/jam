use std::{collections::HashMap, path::PathBuf};

use protocol::piece::Piece;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::{debug, error, warn};

use crate::{
    core::disk::DiskWriterCommand,
    events::Event,
    peer::session::{PeerSession, PeerSessionEvent},
    torrent::{peer::Peer, torrent::TorrentCommand},
};

use super::{
    disk::DiskWriter,
    monitor::{Monitor, MonitorCommand},
    sync::{Synchronizer, SynchronizerCommand},
};

/// Main component in charge to communicate with all the other components.
///
/// The Orchestrator is responsible for:
/// - Communicate with Peer sessions
/// - Communicate with the monitor
/// - Communicate with the synchronizer
/// - Communicate with the disk writter
#[derive(Debug)]
pub struct Orchestrator {
    /// Peer ID for the local client
    peer_id: [u8; 20],
    /// Info hash of the torrent
    info_hash: [u8; 20],
    /// Channel for sending events to the orchestrator
    event_tx: mpsc::Sender<Event>,
    /// Channel for receiving events in the orchestrator
    event_rx: mpsc::Receiver<Event>,
    /// Map of active peer sessions
    peer_sessions: HashMap<String, PeerSessionData>,
    /// Piece information
    pieces: HashMap<u32, Piece>,
    /// Configuration for the orchestrator
    config: OrchestratorConfig,
    /// Channel to communicate with the parent torrent
    torrent_tx: mpsc::Sender<TorrentCommand>,
}

/// Configuration parameters for the Orchestrator
#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    /// Maximum number of concurrent peer connections
    pub max_connections: usize,
    /// Capacity of the download queue
    pub queue_capacity: usize,
    /// Path where downloaded files will be stored
    pub download_path: PathBuf,
    /// Total size of the file being downloaded
    pub file_size: u64,
    /// Size of each piece
    pub pieces_size: u64,
    /// Size of each block within a piece
    pub block_size: u64,
    /// Threshold for timing out unresponsive peers (in seconds)
    pub timeout_threshold: u64,
}

/// Data associated with an active peer session
#[derive(Debug)]
struct PeerSessionData {
    /// Channel to send events to the peer session
    tx: mpsc::Sender<PeerSessionEvent>,
    /// Handle to the peer session task
    _handle: JoinHandle<()>,
}

/// Event categories for better organization
pub enum PeerManagementEvent {
    /// Add new peers to the swarm
    AddPeers { peers: Vec<Peer> },
    /// Spawn a new peer session
    SpawnPeerSession {
        session_id: String,
        peer_addr: String,
    },
    /// Disconnect a peer session
    DisconnectPeerSession { session_id: String },
    /// Peer session closed notification
    PeerSessionClosed { session_id: String },
    /// Peer session timed out
    PeerSessionTimeout { session_id: String },
}

/// Events related to piece management
pub enum PieceEvent {
    /// Received bitfield from peer
    PeerBitfield {
        session_id: String,
        bitfield: Vec<u8>,
    },
    /// Received have message from peer
    PeerHave {
        session_id: String,
        piece_index: u32,
    },
    /// Notify peer of our interest
    NotifyInterest { session_id: String },
    /// A piece has been fully assembled
    PieceAssembled { piece_index: u32, data: Vec<u8> },
    /// A piece has been verified and completed
    PieceCompleted { piece_index: u32 },
    /// A piece was corrupted
    PieceCorrupted {
        session_id: String,
        piece_index: u32,
    },
    /// Unassign a piece from a peer
    PieceUnassign {
        session_id: String,
        piece_index: u32,
    },
    /// Unassign multiple pieces from a peer
    PieceUnassignMany {
        session_id: String,
        pieces_index: Vec<u32>,
    },
    /// Dispatch a piece to a peer for download
    PieceDispatch { session_id: String, piece: Piece },
}

/// Events related to overall torrent status
pub enum StatusEvent {
    /// Download has completed
    DownloadCompleted,
    /// Request for disk statistics
    QueryDownloadState {
        response_channel: mpsc::Sender<TorrentCommand>,
    },
    QueryConnectedPeers {
        response_channel: oneshot::Sender<usize>,
    },
}

impl Orchestrator {
    pub fn new(
        peer_id: [u8; 20],
        info_hash: [u8; 20],
        pieces: HashMap<u32, Piece>,
        config: OrchestratorConfig,
        torrent_tx: mpsc::Sender<TorrentCommand>,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::channel(512);

        Self {
            peer_id,
            info_hash,
            event_tx,
            event_rx,
            peer_sessions: HashMap::new(),
            pieces,
            config,
            torrent_tx,
        }
    }

    pub async fn run(mut self) -> (mpsc::Sender<Event>, JoinHandle<()>) {
        let event_tx = self.event_tx.clone();

        // Initialize the synchronizer
        let synchronizer = Synchronizer::new(
            self.pieces.clone(),
            self.config.queue_capacity,
            self.event_tx.clone(),
        );

        // Start the synchronizer
        let (sync_tx, sync_handle) = synchronizer.run();

        // Initialize the monitor
        let monitor = Monitor::new(self.config.max_connections, self.event_tx.clone());

        // Start the monitor
        let (monitor_tx, monitor_handle) = monitor.run();

        // Initialize the disk writer
        let disk_writer = DiskWriter::new(
            self.config.download_path.clone(),
            self.config.file_size,
            self.config.pieces_size,
            self.pieces.clone(),
            self.event_tx.clone(),
        );

        let (disk_tx, disk_handle) = disk_writer.run();

        // Orchestrator task
        let orchestrator_handle = tokio::spawn(async move {
            // Store handles to ensure they're dropped properly when orchestrator_handle completes
            let _sync_handle = sync_handle;
            let _monitor_handle = monitor_handle;
            let _disk_handle = disk_handle;

            while let Some(event) = self.event_rx.recv().await {
                // Categorize and handle events
                match event {
                    // Peer Management Events
                    Event::AddPeers { peers } => {
                        self.handle_peer_management_event(
                            PeerManagementEvent::AddPeers { peers },
                            &monitor_tx,
                            &sync_tx,
                        )
                        .await;
                    }
                    Event::SpawnPeerSession {
                        session_id,
                        peer_addr,
                    } => {
                        self.handle_peer_management_event(
                            PeerManagementEvent::SpawnPeerSession {
                                session_id,
                                peer_addr,
                            },
                            &monitor_tx,
                            &sync_tx,
                        )
                        .await;
                    }
                    Event::DisconnectPeerSession { session_id } => {
                        self.handle_peer_management_event(
                            PeerManagementEvent::DisconnectPeerSession { session_id },
                            &monitor_tx,
                            &sync_tx,
                        )
                        .await;
                    }
                    Event::PeerSessionClosed { session_id } => {
                        self.handle_peer_management_event(
                            PeerManagementEvent::PeerSessionClosed { session_id },
                            &monitor_tx,
                            &sync_tx,
                        )
                        .await;
                    }
                    Event::PeerSessionTimeout { session_id } => {
                        self.handle_peer_management_event(
                            PeerManagementEvent::PeerSessionTimeout { session_id },
                            &monitor_tx,
                            &sync_tx,
                        )
                        .await;
                    }

                    // Piece Management Events
                    Event::PeerBitfield {
                        session_id,
                        bitfield,
                    } => {
                        self.handle_piece_event(
                            PieceEvent::PeerBitfield {
                                session_id,
                                bitfield,
                            },
                            &sync_tx,
                            &monitor_tx,
                            &disk_tx,
                        )
                        .await;
                    }
                    Event::PeerHave {
                        session_id,
                        piece_index,
                    } => {
                        self.handle_piece_event(
                            PieceEvent::PeerHave {
                                session_id,
                                piece_index,
                            },
                            &sync_tx,
                            &monitor_tx,
                            &disk_tx,
                        )
                        .await;
                    }
                    Event::NotifyInterest { session_id } => {
                        self.handle_piece_event(
                            PieceEvent::NotifyInterest { session_id },
                            &sync_tx,
                            &monitor_tx,
                            &disk_tx,
                        )
                        .await;
                    }
                    Event::PieceAssembled { piece_index, data } => {
                        self.handle_piece_event(
                            PieceEvent::PieceAssembled { piece_index, data },
                            &sync_tx,
                            &monitor_tx,
                            &disk_tx,
                        )
                        .await;
                    }
                    Event::PieceCompleted { piece_index } => {
                        self.handle_piece_event(
                            PieceEvent::PieceCompleted { piece_index },
                            &sync_tx,
                            &monitor_tx,
                            &disk_tx,
                        )
                        .await;
                    }
                    Event::PieceCorrupted {
                        session_id,
                        piece_index,
                    } => {
                        self.handle_piece_event(
                            PieceEvent::PieceCorrupted {
                                session_id,
                                piece_index,
                            },
                            &sync_tx,
                            &monitor_tx,
                            &disk_tx,
                        )
                        .await;
                    }
                    Event::PieceUnassign {
                        session_id,
                        piece_index,
                    } => {
                        self.handle_piece_event(
                            PieceEvent::PieceUnassign {
                                session_id,
                                piece_index,
                            },
                            &sync_tx,
                            &monitor_tx,
                            &disk_tx,
                        )
                        .await;
                    }
                    Event::PieceUnassignMany {
                        session_id,
                        pieces_index,
                    } => {
                        self.handle_piece_event(
                            PieceEvent::PieceUnassignMany {
                                session_id,
                                pieces_index,
                            },
                            &sync_tx,
                            &monitor_tx,
                            &disk_tx,
                        )
                        .await;
                    }
                    Event::PieceDispatch { session_id, piece } => {
                        self.handle_piece_event(
                            PieceEvent::PieceDispatch { session_id, piece },
                            &sync_tx,
                            &monitor_tx,
                            &disk_tx,
                        )
                        .await;
                    }

                    // Status Events
                    Event::DownloadCompleted => {
                        self.handle_status_event(StatusEvent::DownloadCompleted, &sync_tx)
                            .await;
                        break;
                    }
                    Event::QueryDownloadState { response_channel } => {
                        self.handle_status_event(
                            StatusEvent::QueryDownloadState { response_channel },
                            &sync_tx,
                        )
                        .await;
                    }
                    Event::QueryConnectedPeers { response_channel } => {
                        self.handle_status_event(
                            StatusEvent::QueryConnectedPeers { response_channel },
                            &sync_tx,
                        )
                        .await;
                    }
                }
            }

            debug!("Orchestrator event loop terminated, shutting down");
        });

        (event_tx, orchestrator_handle)
    }

    /// Handle events related to peer management
    async fn handle_peer_management_event(
        &mut self,
        event: PeerManagementEvent,
        monitor_tx: &mpsc::Sender<MonitorCommand>,
        sync_tx: &mpsc::Sender<SynchronizerCommand>,
    ) {
        match event {
            PeerManagementEvent::AddPeers { peers } => {
                debug!(peer_count = peers.len(), "Adding new peers to monitor");
                if let Err(e) = monitor_tx.send(MonitorCommand::AddPeers(peers)).await {
                    error!(error = %e, "Failed to send AddPeers command to monitor");
                }
            }
            PeerManagementEvent::SpawnPeerSession {
                session_id,
                peer_addr,
            } => {
                debug!(
                    session_id = %session_id,
                    peer_addr = %peer_addr,
                    "Spawning new peer session"
                );

                let session = PeerSession::new(
                    session_id.clone(),
                    self.peer_id,
                    self.info_hash,
                    peer_addr.clone(),
                    self.config.pieces_size as usize,
                    self.config.block_size as usize,
                    self.config.timeout_threshold,
                    self.event_tx.clone(),
                );

                match session.run().await {
                    Ok((session_tx, session_handle)) => {
                        self.peer_sessions.insert(
                            session_id.clone(),
                            PeerSessionData {
                                tx: session_tx,
                                _handle: session_handle,
                            },
                        );

                        // Notify the monitor that this session is established
                        if let Err(e) = monitor_tx
                            .send(MonitorCommand::PeerSessionEstablished(session_id.clone()))
                            .await
                        {
                            error!(
                                session_id = %session_id,
                                error = %e,
                                "Failed to notify monitor of established peer session"
                            );
                        }

                        debug!(
                            session_id = %session_id,
                            peer_addr = %peer_addr,
                            "Peer session established successfully"
                        );
                    }
                    Err(e) => {
                        warn!(
                            session_id = %session_id,
                            peer_addr = %peer_addr,
                            error = %e,
                            "Could not establish connection with peer"
                        );

                        // notify monitor to remove peer session
                        if let Err(e) = monitor_tx
                            .send(MonitorCommand::RemovePeerSession(session_id.clone()))
                            .await
                        {
                            error!(
                                session_id = %session_id,
                                error = %e,
                                "Failed to notify monitor to remove peer session"
                            );
                        }
                    }
                }
            }
            PeerManagementEvent::DisconnectPeerSession { session_id } => {
                debug!(session_id = %session_id, "Disconnecting peer session");
                self.handle_peer_session_cleanup(session_id, monitor_tx, sync_tx)
                    .await;
            }
            PeerManagementEvent::PeerSessionClosed { session_id } => {
                debug!(session_id = %session_id, "Peer session closed");

                // Send shutdown signal to peer session if it still exists
                if let Some(peer_session_data) = self.peer_sessions.get(&session_id) {
                    if let Err(e) = peer_session_data.tx.send(PeerSessionEvent::Shutdown).await {
                        debug!(
                            session_id = %session_id,
                            error = %e,
                            "Failed to send shutdown signal to peer session"
                        );
                    }
                }

                self.handle_peer_session_cleanup(session_id, monitor_tx, sync_tx)
                    .await;
            }
            PeerManagementEvent::PeerSessionTimeout { session_id } => {
                debug!(session_id = %session_id, "Peer session timed out");
                self.handle_peer_session_cleanup(session_id, monitor_tx, sync_tx)
                    .await;
            }
        }
    }

    /// Handle events related to piece management
    async fn handle_piece_event(
        &mut self,
        event: PieceEvent,
        sync_tx: &mpsc::Sender<SynchronizerCommand>,
        monitor_tx: &mpsc::Sender<MonitorCommand>,
        disk_tx: &mpsc::Sender<DiskWriterCommand>,
    ) {
        match event {
            PieceEvent::PeerBitfield {
                session_id,
                bitfield,
            } => {
                debug!(
                    session_id = %session_id,
                    bitfield_bytes = bitfield.len(),
                    "Processing peer bitfield"
                );

                if let Err(e) = sync_tx
                    .send(SynchronizerCommand::ProcessBitfield {
                        session_id: session_id.clone(),
                        bitfield,
                    })
                    .await
                {
                    error!(
                        session_id = %session_id,
                        error = %e,
                        "Failed to send bitfield to synchronizer"
                    );
                }
            }
            PieceEvent::PeerHave {
                session_id,
                piece_index,
            } => {
                debug!(
                    session_id = %session_id,
                    piece_index = piece_index,
                    "Processing peer have message"
                );

                if let Err(e) = sync_tx
                    .send(SynchronizerCommand::ProcessHave {
                        session_id: session_id.clone(),
                        piece_index,
                    })
                    .await
                {
                    error!(
                        session_id = %session_id,
                        piece_index = piece_index,
                        error = %e,
                        "Failed to process have message"
                    );
                }
            }
            PieceEvent::NotifyInterest { session_id } => {
                debug!(session_id = %session_id, "Notifying interest to peer");

                if let Some(peer_session_data) = self.peer_sessions.get(&session_id) {
                    // dispatch piece to peer session
                    if let Err(e) = peer_session_data
                        .tx
                        .send(PeerSessionEvent::NotifyInterest)
                        .await
                    {
                        error!(
                            session_id = %session_id,
                            error = %e,
                            "Failed to send interest notification to peer session"
                        );
                    }
                } else {
                    error!(
                        session_id = %session_id,
                        "Failed to notify interest: peer session not found"
                    );
                    self.handle_peer_session_cleanup(session_id, monitor_tx, sync_tx)
                        .await;
                }
            }
            PieceEvent::PieceAssembled { piece_index, data } => {
                debug!(
                    piece_index = piece_index,
                    data_size = data.len(),
                    "Piece assembled, sending to disk writer"
                );

                if let Err(e) = disk_tx
                    .send(DiskWriterCommand::WritePiece { piece_index, data })
                    .await
                {
                    error!(
                        piece_index = piece_index,
                        error = %e,
                        "Failed to send assembled piece to disk writer"
                    );
                }
            }
            PieceEvent::PieceCompleted { piece_index } => {
                debug!(piece_index = piece_index, "Piece completed successfully");

                if let Err(e) = sync_tx
                    .send(SynchronizerCommand::MarkPieceComplete(piece_index))
                    .await
                {
                    error!(
                        piece_index = piece_index,
                        error = %e,
                        "Failed to mark piece as complete in synchronizer"
                    );
                }
            }
            PieceEvent::PieceCorrupted {
                session_id,
                piece_index,
            } => {
                warn!(
                    session_id = %session_id.clone(),
                    piece_index = piece_index,
                    "Corrupted piece detected"
                );

                if let Err(e) = sync_tx
                    .send(SynchronizerCommand::CorruptedPiece {
                        session_id: session_id.clone(),
                        piece_index,
                    })
                    .await
                {
                    error!(
                        session_id = %session_id,
                        piece_index = piece_index,
                        error = %e,
                        "Failed to notify synchronizer of corrupted piece"
                    );
                }
            }
            PieceEvent::PieceUnassign {
                session_id,
                piece_index,
            } => {
                debug!(
                    session_id = %session_id,
                    piece_index = piece_index,
                    "Unassigning piece from peer session"
                );

                if let Err(e) = sync_tx
                    .send(SynchronizerCommand::UnassignPiece {
                        session_id: session_id.clone(),
                        piece_index,
                    })
                    .await
                {
                    error!(
                        session_id = %session_id,
                        piece_index = piece_index,
                        error = %e,
                        "Failed to unassign piece from peer session"
                    );
                }
            }
            PieceEvent::PieceUnassignMany {
                session_id,
                pieces_index,
            } => {
                debug!(
                    session_id = %session_id,
                    piece_count = pieces_index.len(),
                    "Unassigning multiple pieces from peer session"
                );

                if let Err(e) = sync_tx
                    .send(SynchronizerCommand::UnassignPieces {
                        session_id: session_id.clone(),
                        pieces_index,
                    })
                    .await
                {
                    error!(
                        session_id = %session_id,
                        error = %e,
                        "Failed to unassign multiple pieces from peer session"
                    );
                }
            }
            PieceEvent::PieceDispatch { session_id, piece } => {
                if let Some(peer_session_data) = self.peer_sessions.get(&session_id) {
                    if let Err(e) = peer_session_data
                        .tx
                        .send(PeerSessionEvent::AssignPiece {
                            piece: piece.clone(),
                        })
                        .await
                    {
                        error!(
                            session_id = %session_id,
                            error = %e,
                            "Failed to dispatch piece to peer session"
                        );
                    }
                    debug!(
                        session_id = %session_id,
                        piece_index = piece.index(),
                        "Piece dispatched to peer session"
                    );
                } else {
                    error!(
                        session_id = %session_id,
                        "Failed to dispatch piece: peer session not found"
                    );
                    self.handle_peer_session_cleanup(session_id, monitor_tx, sync_tx)
                        .await;
                }
            }
        }
    }

    /// Handle status-related events
    async fn handle_status_event(
        &self,
        event: StatusEvent,
        sync_tx: &mpsc::Sender<SynchronizerCommand>,
    ) {
        match event {
            StatusEvent::DownloadCompleted => {
                debug!("Download completed, notifying torrent");
                if let Err(e) = self
                    .torrent_tx
                    .send(TorrentCommand::DownloadCompleted)
                    .await
                {
                    error!(error = %e, "Failed to notify torrent of download completion");
                }
            }
            StatusEvent::QueryDownloadState { response_channel } => {
                debug!("Querying download statistics");
                if let Err(e) = sync_tx
                    .send(SynchronizerCommand::QueryDownloadState(response_channel))
                    .await
                {
                    error!(error = %e, "Failed to query download statistics");
                }
            }
            StatusEvent::QueryConnectedPeers { response_channel } => {
                if let Err(e) = response_channel.send(self.peer_sessions.iter().count()) {
                    error!(error = %e, "Failed to query current peers connected");
                }
            }
        }
    }

    /// Extract common peer session cleanup logic to avoid repetition
    async fn handle_peer_session_cleanup(
        &mut self,
        session_id: String,
        monitor_tx: &mpsc::Sender<MonitorCommand>,
        sync_tx: &mpsc::Sender<SynchronizerCommand>,
    ) {
        // Remove from peer sessions map
        if let Some(peer_session_data) = self.peer_sessions.remove(&session_id) {
            // Send shutdown signal
            if let Err(e) = peer_session_data.tx.send(PeerSessionEvent::Shutdown).await {
                error!(
                    session_id = %session_id,
                    error = %e,
                    "Failed to send shutdown signal to peer session"
                );
            }
        }

        // Notify monitor
        if let Err(e) = monitor_tx
            .send(MonitorCommand::RemovePeerSession(session_id.clone()))
            .await
        {
            error!(
                session_id = %session_id,
                error = %e,
                "Failed to notify monitor of peer session removal"
            );
        }

        // Notify synchronizer
        if let Err(e) = sync_tx
            .send(SynchronizerCommand::ClosePeerSession(session_id.clone()))
            .await
        {
            error!(
                session_id = %session_id,
                error = %e,
                "Failed to notify synchronizer of peer session closure"
            );
        }

        debug!(session_id = %session_id, "Peer session cleanup completed");
    }

    // // Method to check download progress
    // pub async fn check_progress(&self) -> Option<u32> {
    //     if let Some(sync_tx) = &self.synchronizer_tx {
    //         let (resp_tx, resp_rx) = mpsc::channel(1);
    //         if sync_tx
    //             .send(SynchronizerCommand::QueryProgress(resp_tx))
    //             .await
    //             .is_ok()
    //         {
    //             return resp_rx.recv().await;
    //         }
    //     }
    //     None
    // }
    //
    // // Method to check if download is completed
    // pub async fn is_completed(&self) -> bool {
    //     if let Some(sync_tx) = &self.synchronizer_tx {
    //         let (resp_tx, resp_rx) = mpsc::channel(1);
    //         if sync_tx
    //             .send(SynchronizerCommand::QueryIsCompleted(resp_tx))
    //             .await
    //             .is_ok()
    //         {
    //             return resp_rx.recv().await.unwrap_or(false);
    //         }
    //     }
    //     false
    // }
}
