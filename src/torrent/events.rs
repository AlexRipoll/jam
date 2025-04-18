use protocol::piece::Piece;
use tokio::sync::mpsc;

use super::{core::disk::DiskWriterStats, peer::Peer};

// Enum defining all possible commands the Orchestrator can handle
#[derive(Debug)]
pub enum Event {
    // Add peers to the queue
    AddPeers {
        peers: Vec<Peer>,
    },

    // Peer session related commands
    SpawnPeerSession {
        session_id: String,
        peer_addr: String,
    },
    // Event sent to a peer session to close the connection
    DisconnectPeerSession {
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
    NotifyInterest {
        session_id: String,
    },

    // Piece management commands
    PieceDispatch {
        session_id: String,
        piece: Piece,
    },
    // Piece data is ready to be written to disk
    PieceAssembled {
        piece_index: u32,
        data: Vec<u8>,
    },
    PieceCompleted {
        piece_index: u32,
    },
    PieceUnassign {
        session_id: String,
        piece_index: u32,
    },
    PieceUnassignMany {
        session_id: String,
        pieces_index: Vec<u32>,
    },
    PieceCorrupted {
        session_id: String,
        piece_index: u32,
    },

    DownloadCompleted,

    // Query
    DiskStats {
        response_channel: mpsc::Sender<DiskWriterStats>,
    },
}
