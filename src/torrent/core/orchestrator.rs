use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
    path::PathBuf,
};

use protocol::piece::Piece;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{debug, error};

use crate::torrent::{
    core::disk::DiskWriterCommand,
    events::Event,
    peer::Peer,
    peers::session::{PeerSession, PeerSessionEvent},
    torrent::TorrentCommand,
};

use super::{
    disk::DiskWriter,
    monitor::{Monitor, MonitorCommand},
    sync::{Synchronizer, SynchronizerCommand},
};

// Main Orchestrator that coordinates the Synchronizer and Monitor
#[derive(Debug)]
pub struct Orchestrator {
    peer_id: [u8; 20],
    info_hash: [u8; 20],

    // FIX: remove unnecessary
    // Channels
    event_tx: mpsc::Sender<Event>,
    event_rx: mpsc::Receiver<Event>,

    peer_sessions: HashMap<String, PeerSessionData>,

    // FIX: remove unnecessary
    // Internal components
    synchronizer: Option<Synchronizer>,
    synchronizer_tx: Option<mpsc::Sender<SynchronizerCommand>>,
    monitor: Option<Monitor>,
    monitor_tx: Option<mpsc::Sender<MonitorCommand>>,

    // Configuration
    max_connections: usize,
    queue_capacity: usize,
    pieces: HashMap<u32, Piece>,
    peers: VecDeque<Peer>, // FIX: remove unnecessary
    // File information needed for DiskWriter
    download_path: PathBuf,
    file_size: u64,
    pieces_size: u64,
    timeout_threshold: u64,
    torrent_tx: mpsc::Sender<TorrentCommand>,
}

// Define a structure to group session-related data
#[derive(Debug)]
struct PeerSessionData {
    tx: mpsc::Sender<PeerSessionEvent>,
    // Using underscore prefix to indicate this field is kept only to maintain the lifetime
    _handle: JoinHandle<()>,
}

impl Orchestrator {
    pub fn new(
        peer_id: [u8; 20],
        info_hash: [u8; 20],
        max_connections: usize,
        queue_capacity: usize,
        pieces: HashMap<u32, Piece>,
        peers: VecDeque<Peer>,
        download_path: PathBuf,
        file_size: u64,
        pieces_size: u64,
        timeout_threshold: u64,
        torrent_tx: mpsc::Sender<TorrentCommand>,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::channel(100);

        Self {
            peer_id,
            info_hash,
            event_tx,
            event_rx,
            synchronizer: None,
            synchronizer_tx: None,
            peer_sessions: HashMap::new(),
            monitor: None,
            monitor_tx: None,
            max_connections,
            queue_capacity,
            pieces,
            peers,
            download_path,
            file_size,
            pieces_size,
            timeout_threshold,
            torrent_tx,
        }
    }

    pub async fn run(mut self) -> (mpsc::Sender<Event>, JoinHandle<()>) {
        let event_tx = self.event_tx.clone();

        // Initialize the synchronizer
        let synchronizer = Synchronizer::new(
            self.pieces.clone(),
            self.queue_capacity,
            self.event_tx.clone(),
        );

        // Start the synchronizer
        let (sync_tx, sync_handle) = synchronizer.run();

        // Store the sender for later use
        self.synchronizer_tx = Some(sync_tx.clone());

        // Initialize the monitor
        let monitor = Monitor::new(
            self.max_connections,
            self.peers.clone(),
            self.event_tx.clone(),
        );

        // Start the monitor
        let (monitor_tx, monitor_handle) = monitor.run();

        // Initialize the disk writer
        let disk_writer = DiskWriter::new(
            self.download_path.clone(),
            self.file_size,
            self.pieces_size,
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
                // Handle events from peer sessions
                match event {
                    Event::AddPeers { peers } => {
                        let _ = monitor_tx.send(MonitorCommand::AddPeers(peers)).await;
                    }
                    Event::SpawnPeerSession {
                        session_id,
                        peer_addr,
                    } => {
                        let session = PeerSession::new(
                            session_id.clone(),
                            self.peer_id,
                            self.info_hash,
                            peer_addr.clone(),
                            self.timeout_threshold,
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
                                let _ = monitor_tx
                                    .send(MonitorCommand::PeerSessionEstablished(
                                        session_id.clone(),
                                    ))
                                    .await;

                                debug!(
                                    session_id = %session_id,
                                    ip = %peer_addr,
                                    "Peer session established"
                                );
                            }
                            Err(e) => {
                                error!(
                                    "Could not establish connection with peer at {peer_addr}: {e}"
                                );
                                // notify monitor to remove peer session
                                let _ = monitor_tx
                                    .send(MonitorCommand::RemovePeerSession(session_id.clone()))
                                    .await;
                            }
                        }
                    }
                    // Event sent by the synchronizer
                    Event::DisconnectPeerSession { session_id } => {
                        if self.peer_sessions.remove(&session_id).is_none() {
                            error!("Peer session with ID {session_id} not found");
                        }

                        // notify monitor to remove peer session
                        let _ = monitor_tx
                            .send(MonitorCommand::RemovePeerSession(session_id))
                            .await;
                    }
                    // Event sent by the peer session
                    Event::PeerSessionClosed { session_id } => {
                        if let Some(peer_session_data) = self.peer_sessions.get(&session_id) {
                            // send shutdown signal to peer session
                            let _ = peer_session_data.tx.send(PeerSessionEvent::Shutdown).await;
                        } else {
                            error!("Peer session with ID {} not found", session_id.clone());
                        }

                        // notify monitor to remove peer session
                        let _ = monitor_tx
                            .send(MonitorCommand::RemovePeerSession(session_id.clone()))
                            .await;

                        // Also notify synchronizer to clean up any assigned pieces
                        let _ = sync_tx
                            .send(SynchronizerCommand::ClosePeerSession(session_id.clone()))
                            .await;
                    }
                    Event::PeerBitfield {
                        session_id,
                        bitfield,
                    } => {
                        // Forward bitfield events to synchronizer
                        let _ = sync_tx
                            .send(SynchronizerCommand::ProcessBitfield {
                                session_id,
                                bitfield,
                            })
                            .await;
                    }
                    Event::PeerHave {
                        session_id,
                        piece_index,
                    } => {
                        let _ = sync_tx
                            .send(SynchronizerCommand::ProcessHave {
                                session_id,
                                piece_index,
                            })
                            .await;
                    }
                    Event::NotifyInterest { session_id } => {
                        if let Some(peer_session_data) = self.peer_sessions.get(&session_id) {
                            // dispatch piece to peer session
                            let _ = peer_session_data
                                .tx
                                .send(PeerSessionEvent::NotifyInterest)
                                .await;
                        } else {
                            error!(
                                "Failed to notify interest: peer session with ID {} not found",
                                session_id
                            );

                            // notify monitor to remove peer session
                            let _ = monitor_tx
                                .send(MonitorCommand::RemovePeerSession(session_id.clone()))
                                .await;

                            // Also notify synchronizer to clean up any assigned pieces
                            let _ = sync_tx
                                .send(SynchronizerCommand::ClosePeerSession(session_id.clone()))
                                .await;
                        }
                    }
                    Event::PieceAssembled { piece_index, data } => {
                        let _ = disk_tx
                            .send(DiskWriterCommand::WritePiece { piece_index, data })
                            .await;
                    }
                    Event::PieceCompleted { piece_index } => {
                        let _ = sync_tx
                            .send(SynchronizerCommand::MarkPieceComplete(piece_index))
                            .await;
                    }
                    Event::PieceCorrupted {
                        session_id,
                        piece_index,
                    } => {
                        let _ = sync_tx
                            .send(SynchronizerCommand::CorruptedPiece {
                                session_id,
                                piece_index,
                            })
                            .await;
                    }
                    Event::PieceUnassign {
                        session_id,
                        piece_index,
                    } => {
                        let _ = sync_tx
                            .send(SynchronizerCommand::UnassignPiece {
                                session_id,
                                piece_index,
                            })
                            .await;
                    }
                    Event::PieceUnassignMany {
                        session_id,
                        pieces_index,
                    } => {
                        let _ = sync_tx
                            .send(SynchronizerCommand::UnassignPieces {
                                session_id,
                                pieces_index,
                            })
                            .await;
                    }
                    Event::PieceDispatch { session_id, piece } => {
                        if let Some(peer_session_data) = self.peer_sessions.get(&session_id) {
                            // dispatch piece to peer session
                            let _ = peer_session_data
                                .tx
                                .send(PeerSessionEvent::AssignPiece { piece })
                                .await;
                        } else {
                            error!(
                                "Failed to dispatch piece: peer session with ID {} not found",
                                session_id
                            );

                            // notify monitor to remove peer session
                            let _ = monitor_tx
                                .send(MonitorCommand::RemovePeerSession(session_id.clone()))
                                .await;

                            // Also notify synchronizer to clean up any assigned pieces
                            let _ = sync_tx
                                .send(SynchronizerCommand::ClosePeerSession(session_id.clone()))
                                .await;
                        }
                    }
                    Event::PeerSessionTimeout { session_id } => {
                        // TODO: remove peer channels and send shutdown event
                    }
                    Event::DiskStats { response_channel } => {
                        // Forward the request to the DiskWriter component
                        let _ = disk_tx
                            .send(DiskWriterCommand::QueryStats(response_channel))
                            .await;
                    }
                }
            }

            // The main loop has exited, clean up
            debug!("Orchestrator event loop terminated, shutting down");
        });

        (event_tx, orchestrator_handle)
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

#[derive(Debug)]
pub enum OrchestratorError {
    WorkerNotFound(String),
    PieceNotFound(u32),
    WorkerChannelNotFound(String),
    ChannelTxError(mpsc::error::SendError<Event>),
}

impl Display for OrchestratorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrchestratorError::WorkerNotFound(worker_id) => {
                write!(f, "Worker {} not found", worker_id)
            }
            OrchestratorError::PieceNotFound(piece_index) => {
                write!(f, "Piece with index {} not found", piece_index)
            }
            OrchestratorError::WorkerChannelNotFound(worker_id) => {
                write!(f, "Channel for worker {} not found", worker_id)
            }
            OrchestratorError::ChannelTxError(err) => {
                write!(f, "Failed to send event: {}", err)
            }
        }
    }
}

impl From<mpsc::error::SendError<Event>> for OrchestratorError {
    fn from(err: mpsc::error::SendError<Event>) -> Self {
        OrchestratorError::ChannelTxError(err)
    }
}
