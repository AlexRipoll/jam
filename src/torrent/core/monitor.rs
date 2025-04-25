use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    time::Duration,
};

use tokio::{sync::mpsc, task::JoinHandle, time::Instant};
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::torrent::{events::Event, peer::Peer};

/// Commands that can be sent to the Monitor to control its behavior
#[derive(Debug)]
pub enum MonitorCommand {
    /// Trigger a connection check cycle
    CheckPeerSessions,
    /// Remove a session from active tracking
    RemovePeerSession(String),
    /// Add new peers to the connection queue
    AddPeers(Vec<Peer>),
    /// Confirm a peer connection was established
    PeerSessionEstablished(String),
    /// Request current monitor status
    GetStatus(mpsc::Sender<MonitorStatus>),
    /// Signal to shutdown the monitor
    Shutdown,
}

/// Status information about the current state of the Monitor
#[derive(Debug, Clone)]
pub struct MonitorStatus {
    /// Number of currently active peer connections
    pub active_connections: usize,
    /// Number of pending connection attempts
    pub pending_connections: usize,
    /// Number of peers waiting in the connection queue
    pub queued_peers: usize,
    /// Maximum number of connections allowed
    pub connection_capacity: usize,
}

#[derive(Debug, Clone)]
pub struct MonitorConfig {
    /// Maximum number of concurrent peer connections
    pub max_connections: usize,
    /// Time after which a connection attempt is considered failed
    pub connection_timeout: Duration,
    /// Time to wait before retrying a failed peer
    pub peer_backoff_time: Duration,
    /// How often to check for timeouts and new connections
    pub check_interval: Duration,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            max_connections: 50,
            connection_timeout: Duration::from_secs(15),
            peer_backoff_time: Duration::from_secs(60),
            check_interval: Duration::from_secs(5),
        }
    }
}

/// The Monitor manages peer connections for a torrent client.
///
/// It maintains a queue of potential peers to connect to, tracks active connections,
/// handles connection timeouts, and ensures the system stays within connection limits.
#[derive(Debug)]
pub struct Monitor {
    /// Configuration parameters
    config: MonitorConfig,

    /// Tracking active peer sessions
    active_sessions: HashSet<String>,

    /// Tracking pending connections (requested but not confirmed)
    pending_sessions: HashMap<String, Instant>,

    /// Queue of potential peers to connect to
    peer_queue: VecDeque<Peer>,

    /// Peers that failed recent connection attempts with retry timeout
    failed_peers: HashMap<String, (Peer, Instant)>,

    /// Channel for sending events to the orchestrator
    event_tx: mpsc::Sender<Event>,
}

impl Monitor {
    /// Creates a new Monitor with default configuration
    ///
    /// # Arguments
    ///
    /// * `max_connections` - Maximum number of concurrent peer connections
    /// * `event_tx` - Channel for sending events to the orchestrator
    pub fn new(max_connections: usize, event_tx: mpsc::Sender<Event>) -> Self {
        let config = MonitorConfig {
            max_connections,
            ..Default::default()
        };

        Self {
            config,
            active_sessions: HashSet::with_capacity(max_connections),
            pending_sessions: HashMap::with_capacity(max_connections),
            peer_queue: VecDeque::new(),
            failed_peers: HashMap::new(),
            event_tx,
        }
    }

    /// Configures the monitor with custom settings
    ///
    /// # Arguments
    ///
    /// * `config` - Custom configuration parameters
    pub fn with_config(mut self, config: MonitorConfig) -> Self {
        self.config = config;
        self
    }

    /// Starts the monitor actor and returns channels to communicate with it
    ///
    /// Returns a tuple containing:
    /// - A channel for sending commands to the monitor
    /// - A JoinHandle for the monitor task
    pub fn run(self) -> (mpsc::Sender<MonitorCommand>, JoinHandle<()>) {
        let (cmd_tx, cmd_rx) = mpsc::channel::<MonitorCommand>(128);

        // Create the actor task
        let handle = tokio::spawn(async move {
            let mut monitor = self;
            monitor.run_actor(cmd_rx).await;
        });

        (cmd_tx, handle)
    }

    /// Main actor loop processing commands and periodic checks
    async fn run_actor(&mut self, mut cmd_rx: mpsc::Receiver<MonitorCommand>) {
        let mut interval = tokio::time::interval(self.config.check_interval);

        // Initial run to establish connections
        self.check_timeouts().await;
        self.check_and_request_connections().await;

        loop {
            tokio::select! {
                // Handle incoming commands
                Some(cmd) = cmd_rx.recv() => {
                    if !self.handle_command(cmd).await {
                        break;
                    }
                },

                // Periodic connection check
                _ = interval.tick() => {
                    self.perform_periodic_checks().await;
                }
            }
        }

        debug!(task = "Monitor", "Actor shutdown complete");
    }

    /// Processes a single command and returns whether the actor should continue running
    async fn handle_command(&mut self, cmd: MonitorCommand) -> bool {
        match cmd {
            MonitorCommand::CheckPeerSessions => {
                self.check_timeouts().await;
                self.check_and_request_connections().await;
                true
            }
            MonitorCommand::RemovePeerSession(session_id) => {
                debug!(session_id = %session_id, "Removing peer session");
                self.remove_session(&session_id);
                // After removing a session, check if we need to request new connections
                self.check_and_request_connections().await;
                true
            }
            MonitorCommand::AddPeers(peers) => {
                debug!(count = peers.len(), "Adding new peers to set");
                self.add_peers(peers);
                // After adding the peers, check if we can establish new connections
                self.check_and_request_connections().await;
                true
            }
            MonitorCommand::PeerSessionEstablished(session_id) => {
                self.confirm_peer_connection(&session_id);
                true
            }
            MonitorCommand::GetStatus(response_tx) => {
                self.send_status(response_tx).await;
                true
            }
            MonitorCommand::Shutdown => {
                debug!(task = "Monitor", "Shutting down");
                false
            }
        }
    }

    /// Perform periodic maintenance tasks
    async fn perform_periodic_checks(&mut self) {
        // Check for timed out connection attempts
        self.check_timeouts().await;

        // Check if we should initiate new connections
        self.check_and_request_connections().await;

        // Check if any failed peers can be retried
        self.recycle_failed_peers().await;

        // Periodically clean up and optimize collections if they've grown too large
        self.optimize_collections();
    }

    /// Sends the current monitor status through the provided channel
    async fn send_status(&self, response_tx: mpsc::Sender<MonitorStatus>) {
        let status = MonitorStatus {
            active_connections: self.active_sessions.len(),
            pending_connections: self.pending_sessions.len(),
            queued_peers: self.peer_queue.len(),
            connection_capacity: self.config.max_connections,
        };

        if let Err(err) = response_tx.send(status).await {
            warn!("Failed to send monitor status: {}", err);
        }
    }

    /// Check for pending connection timeouts
    async fn check_timeouts(&mut self) {
        let now = Instant::now();
        let timed_out: Vec<String> = self
            .pending_sessions
            .iter()
            .filter(|(_, &timestamp)| {
                now.duration_since(timestamp) > self.config.connection_timeout
            })
            .map(|(id, _)| id.clone())
            .collect();

        for session_id in timed_out {
            self.pending_sessions.remove(&session_id);

            // Notify the orchestrator about the timeout
            match self
                .event_tx
                .send(Event::PeerSessionTimeout {
                    session_id: session_id.clone(),
                })
                .await
            {
                Ok(_) => {
                    debug!(session_id = %session_id, "Sent timeout notification");
                }
                Err(err) => {
                    error!(session_id = %session_id, error = %err, "Failed to send timeout notification");
                }
            }
        }
    }

    /// Check if new connections can be established and request them if possible
    async fn check_and_request_connections(&mut self) {
        // If we're at capacity, don't continue
        if self.is_at_capacity() {
            return;
        }

        let available_slots = self.available_connection_slots();

        // Request connections up to the available slots
        for _ in 0..available_slots {
            if self.peer_queue.is_empty() {
                break;
            }

            // Try to get a new peer from the queue
            if let Some(peer) = self.peer_queue.pop_front() {
                if let Err(err) = self.request_peer_connection(peer.clone()).await {
                    error!(error = %err, "Failed to request peer connection");

                    // If we failed to request the connection, put the peer back in the queue
                    self.peer_queue.push_front(peer);
                    break;
                }
            }
        }
    }

    /// Request a connection to a specific peer
    async fn request_peer_connection(&mut self, peer: Peer) -> Result<(), MonitorError> {
        // Generate a unique session ID
        let session_id = Uuid::new_v4().to_string();
        let peer_addr = peer.address().to_string();

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
                debug!(peer_addr = %peer_addr, "Requested new peer connection");
                Ok(())
            }
            Err(err) => {
                let error_msg = format!("Failed to send spawn command: {}", err);
                Err(MonitorError::SendError(error_msg))
            }
        }
    }

    /// Confirm a peer connection was successful
    fn confirm_peer_connection(&mut self, session_id: &str) {
        if self.pending_sessions.remove(session_id).is_some() {
            self.active_sessions.insert(session_id.to_owned());
            debug!(session_id = %session_id, "Peer connection confirmed");
        } else {
            warn!(session_id = %session_id, "Attempted to confirm unknown session");
        }
    }

    /// Handle a failed connection attempt
    pub fn handle_connection_failure(&mut self, session_id: String, peer: Peer) {
        self.pending_sessions.remove(&session_id);
        let peer_addr = peer.address().to_string();

        debug!(
            session_id = %session_id,
            peer_addr = %peer_addr,
            "Handling connection failure"
        );

        // Add to failed peers with timestamp
        self.failed_peers.insert(peer_addr, (peer, Instant::now()));
    }

    /// Check if any failed peers can be retried and move them back to queue
    async fn recycle_failed_peers(&mut self) {
        let now = Instant::now();
        let retryable: Vec<String> = self
            .failed_peers
            .iter()
            .filter(|(_, (_, timestamp))| {
                now.duration_since(*timestamp) > self.config.peer_backoff_time
            })
            .map(|(addr, _)| addr.clone())
            .collect();

        if !retryable.is_empty() {
            debug!(count = retryable.len(), "Recycling failed peers");
        }

        for addr in retryable {
            if let Some((peer, _)) = self.failed_peers.remove(&addr) {
                self.peer_queue.push_back(peer);
            }
        }
    }

    /// Remove a session when it's closed or disconnected
    fn remove_session(&mut self, session_id: &str) {
        let was_active = self.active_sessions.remove(session_id);
        let was_pending = self.pending_sessions.remove(session_id).is_some();

        if was_active || was_pending {
            debug!(
                session_id = %session_id,
                was_active = was_active,
                was_pending = was_pending,
                "Session removed"
            );
        }
    }

    /// Add multiple peers to the connection queue
    fn add_peers(&mut self, peers: Vec<Peer>) {
        let mut added = 0;
        let peers_len = peers.len();

        for peer in peers {
            if self.add_peer(peer) {
                added += 1;
            }
        }

        debug!(
            added = added,
            skipped = peers_len - added,
            "Added peers to queue"
        );
    }

    /// Add a new peer to the connection queue
    /// Returns true if the peer was added, false if it was a duplicate or in failed list
    fn add_peer(&mut self, peer: Peer) -> bool {
        // Don't add if it's already in the failed list
        let addr_str = peer.address().to_string();
        if self.failed_peers.contains_key(&addr_str) {
            return false;
        }

        // Don't add duplicates
        let is_duplicate = self
            .peer_queue
            .iter()
            .any(|p| p.address() == peer.address());

        if !is_duplicate {
            self.peer_queue.push_back(peer);
            true
        } else {
            false
        }
    }

    /// Check if the monitor is at its connection capacity
    pub fn is_at_capacity(&self) -> bool {
        (self.active_sessions.len() + self.pending_sessions.len()) >= self.config.max_connections
    }

    /// Calculate how many more connections can be established
    pub fn available_connection_slots(&self) -> usize {
        self.config
            .max_connections
            .saturating_sub(self.active_sessions.len())
            .saturating_sub(self.pending_sessions.len())
    }

    /// Periodically optimize collections if they've grown too large
    fn optimize_collections(&mut self) {
        // Only shrink if we're significantly over capacity
        if self.active_sessions.capacity() > self.config.max_connections * 2 {
            self.active_sessions
                .shrink_to(self.config.max_connections * 3 / 2);
        }

        if self.pending_sessions.capacity() > self.config.max_connections * 2 {
            self.pending_sessions
                .shrink_to(self.config.max_connections * 3 / 2);
        }

        // If we have a very large queue, shrink it occasionally
        if self.peer_queue.capacity() > 1000 && self.peer_queue.len() < 500 {
            self.peer_queue.shrink_to(750);
        }

        // If we have a lot of failed peers, clean up old ones
        if self.failed_peers.len() > 200 {
            debug!(
                count = self.failed_peers.len(),
                "Cleaning up old failed peers"
            );
            let now = Instant::now();
            self.failed_peers.retain(|_, (_, timestamp)| {
                now.duration_since(*timestamp) <= self.config.peer_backoff_time * 2
            });
        }
    }
}

/// Error types specific to the Monitor component
#[derive(Debug)]
pub enum MonitorError {
    /// Failed to send an event to the event channel
    SendError(String),
    /// Failed to establish a peer connection
    ConnectionError(String),
}

impl fmt::Display for MonitorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MonitorError::SendError(msg) => write!(f, "Failed to send event: {}", msg),
            MonitorError::ConnectionError(msg) => write!(f, "Connection error: {}", msg),
        }
    }
}

impl std::error::Error for MonitorError {}

#[cfg(test)]
mod test {
    use std::{net::Ipv4Addr, time::Duration};

    use tokio::{sync::mpsc, time::Instant};

    use crate::torrent::{
        core::monitor::{Monitor, MonitorConfig},
        events::Event,
        peer::Peer,
    };

    // Helper function to create a test peer with a specific IP and port
    fn create_test_peer(ip_octets: [u8; 4], port: u16) -> Peer {
        Peer {
            peer_id: None,
            ip: crate::torrent::peer::Ip::IpV4(Ipv4Addr::new(
                ip_octets[0],
                ip_octets[1],
                ip_octets[2],
                ip_octets[3],
            )),
            port,
        }
    }

    // Helper function to create a Monitor instance for testing
    fn create_test_monitor() -> (Monitor, mpsc::Sender<Event>) {
        let (event_tx, _) = mpsc::channel(100);

        let config = MonitorConfig {
            max_connections: 5,
            connection_timeout: Duration::from_millis(50), // Short timeout for testing
            peer_backoff_time: Duration::from_millis(100), // Short backoff for testing
            check_interval: Duration::from_millis(50),     // Short check interval for testing
        };

        let monitor = Monitor::new(5, event_tx.clone()).with_config(config);

        (monitor, event_tx)
    }

    #[test]
    fn test_new_and_with_config() {
        let (event_tx, _) = mpsc::channel(100);

        // Test default constructor
        let monitor = Monitor::new(10, event_tx.clone());
        assert_eq!(monitor.config.max_connections, 10);
        assert!(monitor.active_sessions.is_empty());
        assert!(monitor.pending_sessions.is_empty());
        assert!(monitor.peer_queue.is_empty());
        assert!(monitor.failed_peers.is_empty());
        assert_eq!(monitor.config.connection_timeout, Duration::from_secs(15));
        assert_eq!(monitor.config.peer_backoff_time, Duration::from_secs(60));
        assert_eq!(monitor.config.check_interval, Duration::from_secs(5));

        // Test with_config method
        let custom_config = MonitorConfig {
            max_connections: 10,
            connection_timeout: Duration::from_secs(30),
            peer_backoff_time: Duration::from_secs(120),
            check_interval: Duration::from_secs(10),
        };

        let custom_monitor = Monitor::new(10, event_tx).with_config(custom_config);

        assert_eq!(
            custom_monitor.config.connection_timeout,
            Duration::from_secs(30)
        );
        assert_eq!(
            custom_monitor.config.peer_backoff_time,
            Duration::from_secs(120)
        );
        assert_eq!(
            custom_monitor.config.check_interval,
            Duration::from_secs(10)
        );
    }

    #[test]
    fn test_add_peer() {
        let (mut monitor, _) = create_test_monitor();

        // Add a peer
        let peer1 = create_test_peer([192, 168, 1, 1], 8080);
        monitor.add_peer(peer1.clone());
        assert_eq!(monitor.peer_queue.len(), 1);

        // Add the same peer - should be ignored as duplicate
        monitor.add_peer(peer1);
        assert_eq!(monitor.peer_queue.len(), 1);

        // Add a different peer
        let peer2 = create_test_peer([192, 168, 1, 2], 8080);
        monitor.add_peer(peer2);
        assert_eq!(monitor.peer_queue.len(), 2);

        // Add a peer that's in the failed list - should be ignored
        let peer3 = create_test_peer([192, 168, 1, 3], 8080);
        monitor
            .failed_peers
            .insert(peer3.address().to_string(), (peer3.clone(), Instant::now()));
        monitor.add_peer(peer3);
        assert_eq!(monitor.peer_queue.len(), 2);
    }

    #[test]
    fn test_remove_session() {
        let (mut monitor, _) = create_test_monitor();

        // Add an active session
        let session_id = "session_1".to_string();
        monitor.active_sessions.insert(session_id.clone());
        assert_eq!(monitor.active_sessions.len(), 1);

        // Add a pending session
        let session_id2 = "session_2".to_string();
        monitor
            .pending_sessions
            .insert(session_id2.clone(), Instant::now());
        assert_eq!(monitor.pending_sessions.len(), 1);

        // Remove session_1 from active sessions
        monitor.remove_session(&session_id);
        assert!(monitor.active_sessions.is_empty());

        // Remove session_2 from pending sessions
        monitor.remove_session(&session_id2);
        assert!(monitor.pending_sessions.is_empty());
    }

    #[test]
    fn test_confirm_peer_connection() {
        let (mut monitor, _) = create_test_monitor();

        // Add a pending session
        let session_id = "session_1".to_string();
        monitor
            .pending_sessions
            .insert(session_id.clone(), Instant::now());
        assert_eq!(monitor.pending_sessions.len(), 1);
        assert_eq!(monitor.active_sessions.len(), 0);

        // Confirm the connection
        monitor.confirm_peer_connection(&session_id);
        assert_eq!(monitor.pending_sessions.len(), 0);
        assert_eq!(monitor.active_sessions.len(), 1);
        assert!(monitor.active_sessions.contains(&session_id));

        // Try to confirm a non-existent session (should do nothing)
        monitor.confirm_peer_connection("non_existent_session");
        assert_eq!(monitor.pending_sessions.len(), 0);
        assert_eq!(monitor.active_sessions.len(), 1);
    }

    #[test]
    fn test_handle_connection_failure() {
        let (mut monitor, _) = create_test_monitor();

        // Add a pending session
        let session_id = "session_1".to_string();
        let peer = create_test_peer([192, 168, 1, 10], 8080);
        monitor
            .pending_sessions
            .insert(session_id.clone(), Instant::now());

        // Handle failure
        monitor.handle_connection_failure(session_id.clone(), peer.clone());

        // Verify session removed from pending and added to failed
        assert!(monitor.pending_sessions.is_empty());
        assert_eq!(monitor.failed_peers.len(), 1);
        assert!(monitor
            .failed_peers
            .contains_key(&peer.address().to_string()));
    }

    #[tokio::test]
    async fn test_check_timeouts() {
        let (mut monitor, _) = create_test_monitor();

        // Create a channel to receive events
        let (verify_tx, mut verify_rx) = mpsc::channel::<Event>(10);

        // Replace the event sender with test sender
        monitor.event_tx = verify_tx;

        // Add a pending session that should time out
        let session_id = "session_1".to_string();
        monitor.pending_sessions.insert(
            session_id.clone(),
            Instant::now() - Duration::from_millis(100), // Intentionally in the past
        );

        // Add another session that shouldn't time out yet
        let active_session = "session_2".to_string();
        monitor
            .pending_sessions
            .insert(active_session.clone(), Instant::now());

        // Check timeouts
        monitor.check_timeouts().await;

        // Verify the timed out session was removed
        assert!(!monitor.pending_sessions.contains_key(&session_id));
        assert!(monitor.pending_sessions.contains_key(&active_session));

        // Verify an event was sent
        if let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_millis(100), verify_rx.recv()).await
        {
            match event {
                Event::PeerSessionTimeout { session_id: id } => {
                    assert_eq!(id, session_id);
                }
                _ => panic!("Unexpected event type"),
            }
        } else {
            panic!("Expected timeout event was not received");
        }
    }

    #[tokio::test]
    async fn test_check_and_request_connections() {
        let (mut monitor, _) = create_test_monitor();

        // Create a channel to verify events
        let (verify_tx, mut verify_rx) = mpsc::channel::<Event>(10);
        monitor.event_tx = verify_tx;

        // Add some peers to the queue
        monitor
            .peer_queue
            .push_back(create_test_peer([192, 168, 1, 10], 8080));
        monitor
            .peer_queue
            .push_back(create_test_peer([192, 168, 1, 11], 8081));
        monitor
            .peer_queue
            .push_back(create_test_peer([192, 168, 1, 12], 8082));

        // Set max connections to 2
        monitor.config.max_connections = 2;

        // Request connections
        monitor.check_and_request_connections().await;

        // Verify that 2 connection requests were made
        assert_eq!(monitor.pending_sessions.len(), 2);
        assert_eq!(monitor.peer_queue.len(), 1); // One peer should be left

        // Verify the events were sent
        let mut event_count = 0;
        while let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_millis(100), verify_rx.recv()).await
        {
            match event {
                Event::SpawnPeerSession {
                    session_id,
                    peer_addr,
                } => {
                    assert!(monitor.pending_sessions.contains_key(&session_id));
                    event_count += 1;
                }
                _ => panic!("Unexpected event type"),
            }

            if event_count >= 2 {
                break;
            }
        }

        assert_eq!(event_count, 2);

        // Test with some active connections
        monitor.active_sessions.insert("active1".to_string());
        monitor.active_sessions.insert("active2".to_string());

        // Now we're at capacity (2 active + 2 pending = 4, max is 2)
        // Should not request more connections
        monitor.check_and_request_connections().await;

        // Verify no change in pending or queue
        assert_eq!(monitor.pending_sessions.len(), 2);
        assert_eq!(monitor.peer_queue.len(), 1);
    }

    #[tokio::test]
    async fn test_recycle_failed_peers() {
        let (mut monitor, _) = create_test_monitor();

        // Add some failed peers
        let peer1 = create_test_peer([192, 168, 1, 20], 8080);
        let peer2 = create_test_peer([192, 168, 1, 21], 8081);

        // One old enough to recycle
        monitor.failed_peers.insert(
            peer1.address().to_string(),
            (peer1.clone(), Instant::now() - Duration::from_millis(200)),
        );

        // One too recent to recycle
        monitor
            .failed_peers
            .insert(peer2.address().to_string(), (peer2.clone(), Instant::now()));

        // Recycle failed peers
        monitor.recycle_failed_peers().await;

        // Verify only the old peer was recycled
        assert_eq!(monitor.failed_peers.len(), 1);
        assert!(!monitor
            .failed_peers
            .contains_key(&peer1.address().to_string()));
        assert!(monitor
            .failed_peers
            .contains_key(&peer2.address().to_string()));

        // Verify the recycled peer was added to the queue
        assert_eq!(monitor.peer_queue.len(), 1);
        assert_eq!(monitor.peer_queue[0].address(), peer1.address());
    }
}
