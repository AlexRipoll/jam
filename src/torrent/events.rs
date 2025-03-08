use protocol::piece::Piece;

// Enum defining all possible commands the Orchestrator can handle
#[derive(Debug)]
pub enum Event {
    // Peer session related commands
    SpawnPeerSession {
        session_id: String,
        peer_addr: String,
    },
    PeerSessionEstablished {
        session_id: String,
        peer_addr: String,
    },
    // Event sent to a peer session to close the connection
    ShutdownPeerSession {
        session_id: String,
    },
    // Event sent to notify a peer closed the connection
    PeerSessionClosed {
        session_id: String,
    },
    PeerSessionTimeout {
        session_id: String,
    },

    // Peer communication commands
    PeerBitfield {
        session_id: String,
        bitfield: Vec<u8>,
    },
    PeerHave {
        session_id: String,
        piece_index: u32,
    },

    // Piece management commands
    PieceDispatch {
        session_id: String,
        piece: Piece,
    },
    PieceCompleted(u32),
    PieceUnassign(u32),
    PieceUnassignMany(Vec<u32>),
    PieceCorrupted {
        session_id: String,
        piece_index: u32,
    },
}
