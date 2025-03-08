use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
    sync::Arc,
};

use protocol::piece::Piece;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::debug;

use crate::torrent::peers::PeerSession;

use super::{
    events::Event,
    monitor::{Monitor, MonitorCommand},
    peer::Peer,
    sync::{Synchronizer, SynchronizerCommand},
};

// Main Orchestrator that coordinates the Synchronizer and Monitor
#[derive(Debug)]
pub struct Orchestrator {
    // Channels
    event_tx: Arc<mpsc::Sender<Event>>,
    event_rx: mpsc::Receiver<Event>,

    // Internal components
    synchronizer: Option<Synchronizer>,
    synchronizer_tx: Option<mpsc::Sender<SynchronizerCommand>>,
    monitor: Option<Monitor>,
    monitor_tx: Option<mpsc::Sender<MonitorCommand>>,

    // Configuration
    max_connections: usize,
    queue_capacity: usize,
    pieces: HashMap<u32, Piece>,
    peers: VecDeque<Peer>,
}

impl Orchestrator {
    pub fn new(
        max_connections: usize,
        queue_capacity: usize,
        pieces: HashMap<u32, Piece>,
        peers: VecDeque<Peer>,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::channel(100);

        Self {
            event_tx: Arc::new(event_tx),
            event_rx,
            synchronizer: None,
            synchronizer_tx: None,
            monitor: None,
            monitor_tx: None,
            max_connections,
            queue_capacity,
            pieces,
            peers,
        }
    }

    pub async fn run(mut self) -> JoinHandle<()> {
        // Initialize the synchronizer
        let synchronizer = Synchronizer::new(
            self.pieces.clone(),
            self.queue_capacity,
            Arc::clone(&self.event_tx),
        );

        // Start the synchronizer
        let (sync_tx, sync_handle) = synchronizer.run();

        // Store the sender for later use
        self.synchronizer_tx = Some(sync_tx.clone());

        // Initialize the monitor
        let monitor = Monitor::new(
            self.max_connections,
            self.peers.clone(),
            Arc::clone(&self.event_tx),
        );

        // Start the monitor
        let (monitor_tx, monitor_handle) = monitor.run();

        // Orchestrator task
        let orchestrator_handle = tokio::spawn(async move {
            // Store handles to ensure they're dropped properly when orchestrator_handle completes
            let _sync_handle = sync_handle;
            let _monitor_handle = monitor_handle;

            while let Some(event) = self.event_rx.recv().await {
                match event {
                    // Handle events from peer sessions
                    Event::PeerSessionEstablished {
                        session_id,
                        peer_addr,
                    } => {
                        let _ = monitor_tx
                            .send(MonitorCommand::PeerSessionEstablished(session_id.clone()))
                            .await;
                        debug!("[{}]:: Peer session {} established", peer_addr, session_id);
                    }

                    Event::SpawnPeerSession {
                        session_id,
                        peer_addr,
                    } => {
                        // TODO: refactor PeerSession to follow the same pattern as the
                        // Synchronizer and the Monitor
                        //
                        //  PeerSession::new(
                        //      session_id,
                        //      self.peer_id,
                        //      self.info_hash,
                        //      peer_addr,
                        //      disk_tx,
                        // Arc::clone(&self.event_tx),
                        //      rx,
                        //  )
                        //  .initialize();
                        //
                        //  -> insert the peer session tx
                    }
                    Event::ShutdownPeerSession { session_id } => {
                        // notify monitor to remove peer session
                        let _ = monitor_tx
                            .send(MonitorCommand::RemovePeerSession(session_id))
                            .await;
                    }
                    Event::PeerSessionClosed { session_id } => {
                        // notify monitor to remove peer session
                        let _ = monitor_tx
                            .send(MonitorCommand::RemovePeerSession(session_id.clone()))
                            .await;

                        // Also notify synchronizer to clean up any assigned pieces
                        let _ = sync_tx
                            .send(SynchronizerCommand::ClosePeerSession(session_id))
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
                    Event::PieceCompleted(piece_index) => {
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
                    Event::PieceUnassign(piece_index) => {
                        let _ = sync_tx
                            .send(SynchronizerCommand::UnassignPiece(piece_index))
                            .await;
                    }
                    Event::PieceUnassignMany(pieces_indexes) => {
                        let _ = sync_tx
                            .send(SynchronizerCommand::UnassignPieces(pieces_indexes))
                            .await;
                    }
                    Event::PieceDispatch { session_id, piece } => {
                        // let _ = session_tx
                        //     .send(PeerSessionCommand::DispatchPieces(piece_index))
                        //     .await;
                    }
                    Event::PeerSessionTimeout { session_id } => {
                        // TODO: remove peer channels and send shutdown event
                    }
                }
            }

            // The main loop has exited, clean up
            println!("Orchestrator shutting down");
        });

        orchestrator_handle
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

// #[derive(Debug)]
// struct Orchestrator {
//     peer_id: [u8; 20],
//     info_hash: [u8; 20],
//     action: Action,
//     // state: State,
//     peers: HashSet<Peer>,
//     workers: HashSet<String>,
//     workers_channel: HashMap<String, mpsc::Sender<Event>>, // worker_id -> worker sender for sending Events to worker
//     queue_capacity: usize,
//     disk_tx: Arc<mpsc::Sender<DiskEvent>>,
// }

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
//
// #[cfg(test)]
// mod test {
//     use super::Orchestrator;
//     use crate::torrent::{events::Event, state::State};
//     use protocol::{bitfield::Bitfield, piece::Piece};
//     use std::collections::{HashMap, HashSet, VecDeque};
//     use tokio::sync::mpsc;
//
//     pub fn mock_pieces(total_piece: u32) -> HashMap<u32, Piece> {
//         (0..total_piece)
//             .map(|i| (i, Piece::new(i, 1024, [i as u8; 20])))
//             .collect()
//     }
//
//     #[test]
//     fn test_assign_pieces_to_workers() {
//         let total_pieces = 15;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//
//         let peer1 = (
//             "worker1",
//             Bitfield::from(&vec![0b11001000, 0b0], total_pieces as usize),
//         );
//         let peer2 = (
//             "worker2",
//             Bitfield::from(&vec![0b01001101, 0b10000000], total_pieces as usize),
//         );
//         let peer3 = (
//             "worker3",
//             Bitfield::from(&vec![0b11100010, 0b01010000], total_pieces as usize),
//         );
//         let peer4 = (
//             "worker4",
//             Bitfield::from(&vec![0b11101101, 0b10110000], total_pieces as usize),
//         );
//
//         for (id, bitfield) in [&peer1, &peer2, &peer3, &peer4] {
//             state.process_bitfield(id, bitfield.clone());
//         }
//
//         let orchestrator = Orchestrator::new(state, 5, HashSet::new());
//
//         let assignments = orchestrator.assign_pieces_to_workers();
//         assert_eq!(assignments.get(peer1.0), Some(&VecDeque::from([0])));
//         assert_eq!(assignments.get(peer2.0), Some(&VecDeque::from([1, 5, 8])));
//         assert_eq!(
//             assignments.get(peer3.0),
//             Some(&VecDeque::from([2, 6, 9, 11]))
//         );
//         assert_eq!(assignments.get(peer4.0), Some(&VecDeque::from([4, 7, 10])));
//     }
//
//     #[tokio::test]
//     async fn test_dispatch_pending_pieces() {
//         let total_pieces = 10;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b10000001, 0b01101100], total_pieces as usize);
//
//         let peer1 = (
//             "worker1",
//             Bitfield::from(&vec![0b11001010, 0b0], total_pieces as usize),
//         );
//         let peer2 = (
//             "worker2",
//             Bitfield::from(&vec![0b01001101, 0b10000000], total_pieces as usize),
//         );
//
//         let peer3 = (
//             "worker3",
//             Bitfield::from(&vec![0b10001001, 0b00000000], total_pieces as usize),
//         );
//
//         for (id, bitfield) in [&peer1, &peer2, &peer3] {
//             state.process_bitfield(id, bitfield.clone());
//         }
//
//         state.pending_pieces = vec![1, 4, 5, 6, 8];
//
//         let mut orchestrator = Orchestrator::new(state, 5, HashSet::new());
//
//         // Create a test channel (1 slot to ensure async behavior)
//         let (worker1_tx, worker1_rx) = mpsc::channel::<Event>(10);
//         // Insert mock channel into orchestrator
//         orchestrator
//             .workers_channel
//             .insert(peer1.0.to_string(), worker1_tx);
//
//         let (worker2_tx, worker2_rx) = mpsc::channel::<Event>(10);
//         orchestrator
//             .workers_channel
//             .insert(peer2.0.to_string(), worker2_tx);
//
//         let (worker3_tx, worker3_rx) = mpsc::channel::<Event>(10);
//         orchestrator
//             .workers_channel
//             .insert(peer3.0.to_string(), worker3_tx);
//
//         orchestrator.dispatch_pending_pieces().await.unwrap();
//
//         // Check that workers have been assigned pieces to download
//         let worker1_pending_pieces = orchestrator
//             .state
//             .workers_pending_pieces
//             .get(peer1.0)
//             .unwrap();
//         assert_eq!(worker1_pending_pieces, &vec![1, 6]);
//         let worker2_pending_pieces = orchestrator
//             .state
//             .workers_pending_pieces
//             .get(peer2.0)
//             .unwrap();
//         assert_eq!(worker2_pending_pieces, &vec![5, 8]);
//         let worker3_pending_pieces = orchestrator
//             .state
//             .workers_pending_pieces
//             .get(peer3.0)
//             .unwrap();
//         assert_eq!(worker3_pending_pieces, &vec![4]);
//     }
//
//     #[tokio::test]
//     async fn test_dispatch_pending_pieces_append_to_non_empty_queues() {
//         let total_pieces = 10;
//         let pieces = mock_pieces(total_pieces);
//         let mut state = State::new(pieces);
//         state.bitfield = Bitfield::from(&vec![0b10000001, 0b01101100], total_pieces as usize);
//
//         let peer1 = (
//             "worker1",
//             Bitfield::from(&vec![0b11001010, 0b0], total_pieces as usize),
//         );
//         let peer2 = (
//             "worker2",
//             Bitfield::from(&vec![0b01001101, 0b10000000], total_pieces as usize),
//         );
//
//         let peer3 = (
//             "worker3",
//             Bitfield::from(&vec![0b10001001, 0b00000000], total_pieces as usize),
//         );
//
//         for (id, bitfield) in [&peer1, &peer2, &peer3] {
//             state.process_bitfield(id, bitfield.clone());
//         }
//
//         state.pending_pieces = vec![1, 4, 5, 6, 8];
//         state
//             .workers_pending_pieces
//             .insert(peer1.0.to_string(), vec![7, 9]);
//         state
//             .workers_pending_pieces
//             .insert(peer3.0.to_string(), vec![2]);
//
//         let mut orchestrator = Orchestrator::new(state, 5, HashSet::new());
//
//         // Create a test channel (1 slot to ensure async behavior)
//         let (worker1_tx, worker1_rx) = mpsc::channel::<Event>(10);
//         // Insert mock channel into orchestrator
//         orchestrator
//             .workers_channel
//             .insert(peer1.0.to_string(), worker1_tx);
//
//         let (worker2_tx, worker2_rx) = mpsc::channel::<Event>(10);
//         orchestrator
//             .workers_channel
//             .insert(peer2.0.to_string(), worker2_tx);
//
//         let (worker3_tx, worker3_rx) = mpsc::channel::<Event>(10);
//         orchestrator
//             .workers_channel
//             .insert(peer3.0.to_string(), worker3_tx);
//
//         orchestrator.dispatch_pending_pieces().await.unwrap();
//
//         // Check that workers have been assigned pieces to download
//         let worker1_pending_pieces = orchestrator
//             .state
//             .workers_pending_pieces
//             .get(peer1.0)
//             .unwrap();
//         assert_eq!(worker1_pending_pieces, &vec![7, 9, 1, 6]);
//         let worker2_pending_pieces = orchestrator
//             .state
//             .workers_pending_pieces
//             .get(peer2.0)
//             .unwrap();
//         assert_eq!(worker2_pending_pieces, &vec![5, 8]);
//         let worker3_pending_pieces = orchestrator
//             .state
//             .workers_pending_pieces
//             .get(peer3.0)
//             .unwrap();
//         assert_eq!(worker3_pending_pieces, &vec![2, 4]);
//     }
// }
