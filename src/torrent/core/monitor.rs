use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use tokio::{sync::mpsc, task::JoinHandle, time::Instant};
use uuid::Uuid;

use crate::torrent::{events::Event, peer::Peer};

#[derive(Debug)]
pub enum MonitorCommand {
    CheckPeerSessions,                      // Trigger connection check cycle
    RemovePeerSession(String),              // Remove a session from active tracking
    AddPeer(Peer),                          // Add a new peer to the queue
    PeerSessionEstablished(String),         // Confirm a peer connection was established
    PeerSessionFailed(String, Peer),        // Notify that a connection attempt failed
    GetStatus(mpsc::Sender<MonitorStatus>), // Request monitor status
    Shutdown,                               // Signal to shutdown the monitor
}

#[derive(Debug, Clone)]
pub struct MonitorStatus {
    pub active_connections: usize,
    pub pending_connections: usize,
    pub queued_peers: usize,
    pub connection_capacity: usize,
}

#[derive(Debug)]
pub struct Monitor {
    // Maximum number of concurrent peer connections
    max_connections: usize,

    // Tracking active peer sessions
    active_sessions: HashSet<String>,

    // Tracking pending connections (requested but not confirmed)
    pending_sessions: HashMap<String, Instant>,

    // Queue of potential peers to connect to
    peer_queue: VecDeque<Peer>,

    // Peers that failed recent connection attempts with retry timeout
    failed_peers: HashMap<String, (Peer, Instant)>,

    // Channels for communication
    event_tx: Arc<mpsc::Sender<Event>>,

    // Configuration
    connection_timeout: Duration,
    peer_backoff_time: Duration,
    check_interval: Duration,
}

impl Monitor {
    pub fn new(
        max_connections: usize,
        peer_queue: VecDeque<Peer>,
        event_tx: Arc<mpsc::Sender<Event>>,
    ) -> Self {
        Self {
            max_connections,
            active_sessions: HashSet::new(),
            pending_sessions: HashMap::new(),
            peer_queue,
            failed_peers: HashMap::new(),
            event_tx,
            connection_timeout: Duration::from_secs(15),
            peer_backoff_time: Duration::from_secs(60), // 1 minutes backoff
            check_interval: Duration::from_secs(5),
        }
    }

    pub fn with_config(
        mut self,
        connection_timeout: Duration,
        peer_backoff_time: Duration,
        check_interval: Duration,
    ) -> Self {
        self.connection_timeout = connection_timeout;
        self.peer_backoff_time = peer_backoff_time;
        self.check_interval = check_interval;

        self
    }

    pub fn run(self) -> (mpsc::Sender<MonitorCommand>, JoinHandle<()>) {
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<MonitorCommand>(100);

        // Create the actor task
        let handle = tokio::spawn(async move {
            let mut monitor = self;
            let mut interval = tokio::time::interval(monitor.check_interval);

            // Initial run to establish connections
            monitor.check_timeouts().await;
            monitor.check_and_request_connections().await;

            loop {
                tokio::select! {
                    // Handle incoming commands
                    Some(cmd) = cmd_rx.recv() => {
                        match cmd {
                            MonitorCommand::CheckPeerSessions => {
                                monitor.check_timeouts().await;
                                monitor.check_and_request_connections().await;
                            },
                            MonitorCommand::RemovePeerSession(session_id) => {
                                monitor.remove_session(&session_id);
                                // After removing a session, check if we need to request new connections
                                monitor.check_and_request_connections().await;
                            },
                            MonitorCommand::AddPeer(peer) => {
                                monitor.add_peer(peer);
                                // After adding a peer, check if we can establish new connections
                                monitor.check_and_request_connections().await;
                            },
                            MonitorCommand::PeerSessionEstablished(session_id) => {
                                monitor.confirm_peer_connection(&session_id);
                            },
                            MonitorCommand::PeerSessionFailed(session_id, peer) => {
                                monitor.handle_connection_failure(session_id, peer);
                                // Try to establish other connections
                                monitor.check_and_request_connections().await;
                            },
                            MonitorCommand::GetStatus(response_tx) => {
                                let status = MonitorStatus {
                                    active_connections: monitor.active_sessions.len(),
                                    pending_connections: monitor.pending_sessions.len(),
                                    queued_peers: monitor.peer_queue.len(),
                                    connection_capacity: monitor.max_connections,
                                };
                                let _ = response_tx.send(status).await;
                            },
                            MonitorCommand::Shutdown => {
                                println!("Monitor shutting down");
                                break;
                            }
                        }
                    },

                    // Periodic connection check
                    _ = interval.tick() => {
                        // Check for timed out connection attempts
                        monitor.check_timeouts().await;

                        // Check if we should initiate new connections
                        monitor.check_and_request_connections().await;

                        // Check if any failed peers can be retried
                        // monitor.recycle_failed_peers().await;
                    }
                }
            }
        });

        (cmd_tx, handle)
    }

    // Check for pending connection timeouts
    async fn check_timeouts(&mut self) {
        let now = Instant::now();
        let timed_out: Vec<String> = self
            .pending_sessions
            .iter()
            .filter(|(_, &timestamp)| now.duration_since(timestamp) > self.connection_timeout)
            .map(|(id, _)| id.clone())
            .collect();

        for session_id in timed_out {
            self.pending_sessions.remove(&session_id);

            // Notify the orchestrator about the timeout
            let _ = self
                .event_tx
                .send(Event::PeerSessionTimeout { session_id })
                .await;
        }
    }

    // Method to check and request new connections
    async fn check_and_request_connections(&mut self) {
        let available_slots = self
            .max_connections
            .saturating_sub(self.active_sessions.len())
            .saturating_sub(self.pending_sessions.len());

        // If no slots available, don't bother continuing
        if available_slots == 0 {
            return;
        }

        // Request connections up to the available slots
        for _ in 0..available_slots {
            if self.peer_queue.is_empty() {
                break;
            }

            // Try to get a new peer from the queue
            if let Some(peer) = self.peer_queue.pop_front() {
                // Generate a unique session ID
                let session_id = Uuid::new_v4().to_string();

                // Prepare to request a new peer session
                let spawn_command = Event::SpawnPeerSession {
                    session_id: session_id.clone(),
                    peer_addr: peer.address(),
                };

                // Send request to orchestrator
                match self.event_tx.send(spawn_command).await {
                    Ok(_) => {
                        // Track as pending connection
                        self.pending_sessions.insert(session_id, Instant::now());
                    }
                    Err(_) => {
                        // If sending fails, put the peer back and break
                        self.peer_queue.push_front(peer);
                        break;
                    }
                }
            } else {
                // No more peers to connect to
                break;
            }
        }
    }

    // Confirm a peer connection was successful
    fn confirm_peer_connection(&mut self, session_id: &str) {
        if self.pending_sessions.remove(session_id).is_some() {
            self.active_sessions.insert(session_id.to_owned());
        }
    }

    // Handle a failed connection attempt
    fn handle_connection_failure(&mut self, session_id: String, peer: Peer) {
        self.pending_sessions.remove(&session_id);

        // Add to failed peers with timestamp
        self.failed_peers
            .insert(peer.address().to_string(), (peer, Instant::now()));
    }

    // Check if any failed peers can be retried and move them back to queue
    async fn recycle_failed_peers(&mut self) {
        let now = Instant::now();
        let retryable: Vec<String> = self
            .failed_peers
            .iter()
            .filter(|(_, (_, timestamp))| now.duration_since(*timestamp) > self.peer_backoff_time)
            .map(|(addr, _)| addr.clone())
            .collect();

        for addr in retryable {
            if let Some((peer, _)) = self.failed_peers.remove(&addr) {
                self.peer_queue.push_back(peer);
            }
        }
    }

    // Method to remove a session when it's closed
    fn remove_session(&mut self, session_id: &str) {
        self.active_sessions.remove(session_id);
        self.pending_sessions.remove(session_id);
    }

    // Method to add a new peer to the queue
    fn add_peer(&mut self, peer: Peer) {
        // Don't add if it's already in the failed list
        let addr_str = peer.address().to_string();
        if self.failed_peers.get(&addr_str).is_some() {
            return;
        }

        // Don't add duplicates
        let is_duplicate = self
            .peer_queue
            .iter()
            .any(|p| p.address() == peer.address());

        if !is_duplicate {
            self.peer_queue.push_back(peer);
        }
    }
}
