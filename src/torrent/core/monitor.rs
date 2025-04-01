use std::{
    collections::{HashMap, HashSet, VecDeque},
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
    event_tx: mpsc::Sender<Event>,

    // Configuration
    connection_timeout: Duration,
    peer_backoff_time: Duration,
    check_interval: Duration,
}

impl Monitor {
    pub fn new(
        max_connections: usize,
        peer_queue: VecDeque<Peer>,
        event_tx: mpsc::Sender<Event>,
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

#[cfg(test)]
mod test {
    use std::{collections::VecDeque, net::Ipv4Addr, sync::Arc, time::Duration};

    use tokio::{sync::mpsc, time::Instant};

    use crate::torrent::{
        core::monitor::{Monitor, MonitorCommand},
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
        let peer_queue = VecDeque::new();

        let monitor = Monitor::new(5, peer_queue, event_tx.clone()).with_config(
            Duration::from_millis(50),  // Short timeout for testing
            Duration::from_millis(100), // Short backoff for testing
            Duration::from_millis(50),  // Short check interval for testing
        );

        (monitor, event_tx)
    }

    #[test]
    fn test_new_and_with_config() {
        let (event_tx, _) = mpsc::channel(100);
        let peer_queue = VecDeque::new();

        // Test default constructor
        let monitor = Monitor::new(10, peer_queue.clone(), event_tx.clone());
        assert_eq!(monitor.max_connections, 10);
        assert!(monitor.active_sessions.is_empty());
        assert!(monitor.pending_sessions.is_empty());
        assert!(monitor.peer_queue.is_empty());
        assert!(monitor.failed_peers.is_empty());
        assert_eq!(monitor.connection_timeout, Duration::from_secs(15));
        assert_eq!(monitor.peer_backoff_time, Duration::from_secs(60));
        assert_eq!(monitor.check_interval, Duration::from_secs(5));

        // Test with_config method
        let custom_monitor = Monitor::new(10, peer_queue, event_tx).with_config(
            Duration::from_secs(30),
            Duration::from_secs(120),
            Duration::from_secs(10),
        );

        assert_eq!(custom_monitor.connection_timeout, Duration::from_secs(30));
        assert_eq!(custom_monitor.peer_backoff_time, Duration::from_secs(120));
        assert_eq!(custom_monitor.check_interval, Duration::from_secs(10));
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
        monitor.max_connections = 2;

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

    // #[tokio::test]
    // async fn test_add_peer_and_connection_request() {
    //     // Setup
    //     let max_connections = 3;
    //     let (event_tx, mut event_rx) = mpsc::channel(100);
    //     let peer_queue = VecDeque::new();
    //
    //     // Create monitor with short intervals for faster testing
    //     let monitor = Monitor::new(max_connections, peer_queue, event_tx).with_config(
    //         Duration::from_millis(100), // connection timeout
    //         Duration::from_millis(200), // peer backoff time
    //         Duration::from_millis(50),  // check interval
    //     );
    //     let (cmd_tx, handle) = monitor.run();
    //
    //     // Add peers
    //     let peer1 = Peer {
    //         peer_id: None,
    //         ip: crate::torrent::peer::Ip::IpV4(Ipv4Addr::new(192, 168, 1, 1)),
    //         port: 1234,
    //     };
    //     let peer2 = Peer {
    //         peer_id: None,
    //         ip: crate::torrent::peer::Ip::IpV4(Ipv4Addr::new(192, 168, 1, 2)),
    //         port: 1234,
    //     };
    //
    //     cmd_tx
    //         .send(MonitorCommand::AddPeer(peer1.clone()))
    //         .await
    //         .unwrap();
    //     cmd_tx
    //         .send(MonitorCommand::AddPeer(peer2.clone()))
    //         .await
    //         .unwrap();
    //
    //     // Wait for spawn events
    //     let event1 = event_rx.recv().await.unwrap();
    //     let event2 = event_rx.recv().await.unwrap();
    //
    //     // Extract session IDs
    //     let session_id1 = match &event1 {
    //         Event::SpawnPeerSession {
    //             session_id,
    //             peer_addr,
    //         } => {
    //             assert_eq!(peer_addr, &peer1.address());
    //             session_id.clone()
    //         }
    //         _ => panic!("Expected SpawnPeerSession event, got {:?}", event1),
    //     };
    //
    //     let session_id2 = match &event2 {
    //         Event::SpawnPeerSession {
    //             session_id,
    //             peer_addr,
    //         } => {
    //             assert_eq!(peer_addr, &peer2.address());
    //             session_id.clone()
    //         }
    //         _ => panic!("Expected SpawnPeerSession event, got {:?}", event2),
    //     };
    //
    //     // Check status
    //     let (status_tx, mut status_rx) = mpsc::channel(1);
    //     cmd_tx
    //         .send(MonitorCommand::GetStatus(status_tx))
    //         .await
    //         .unwrap();
    //     let status = status_rx.recv().await.unwrap();
    //
    //     assert_eq!(status.active_connections, 0);
    //     assert_eq!(status.pending_connections, 2);
    //     assert_eq!(status.queued_peers, 0);
    //
    //     // Confirm one connection
    //     cmd_tx
    //         .send(MonitorCommand::PeerSessionEstablished(session_id1))
    //         .await
    //         .unwrap();
    //
    //     // Check status again
    //     let (status_tx, mut status_rx) = mpsc::channel(1);
    //     cmd_tx
    //         .send(MonitorCommand::GetStatus(status_tx))
    //         .await
    //         .unwrap();
    //     let status = status_rx.recv().await.unwrap();
    //
    //     assert_eq!(status.active_connections, 1);
    //     assert_eq!(status.pending_connections, 1);
    //
    //     // Cleanup
    //     cmd_tx.send(MonitorCommand::Shutdown).await.unwrap();
    //     handle.await.unwrap();
    // }
}
