use protocol::piece::Piece;

#[derive(Debug, PartialEq, Eq)]
pub enum Event {
    Disk(DiskEvent),
    State(StateEvent),
    Shutdown,
}

#[derive(Debug, PartialEq, Eq)]
pub enum DiskEvent {
    Piece {
        piece: Piece,
        assembled_blocks: Vec<u8>,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub enum StateEvent {
    PeerBitfield {
        worker_id: String,
        bitfield: Vec<u8>,
    },
    PeerHave {
        worker_id: String,
        piece_index: u32,
    },
    RequestPiece(Piece),
    UnassignPiece(u32),
    CorruptedPiece {
        piece_index: u32,
        worker_id: String,
    },
    UnassignPieces(Vec<u32>),
    CompletedPiece(u32),
    Shutdown(String),
}
