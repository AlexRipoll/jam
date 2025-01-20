use std::fmt;

use tokio::io;

#[derive(Debug)]
pub enum ProtocolError {
    // Errors from the `Piece` module
    PieceOutOfBounds,
    PieceMissingBlocks,

    // Errors from the `Message` module
    BufferTooShort,
    PayloadTooShort,
    UnknownMessageId,

    // I/O errors
    Io(io::Error),
}

// Implement `std::fmt::Display` for `ProtocolError`
impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::PieceOutOfBounds => write!(f, "Piece out of bounds"),
            ProtocolError::PieceMissingBlocks => write!(f, "Piece has missing blocks"),
            ProtocolError::BufferTooShort => write!(f, "Buffer too short for length prefix"),
            ProtocolError::PayloadTooShort => write!(f, "Payload too short for deserialization"),
            ProtocolError::UnknownMessageId => write!(f, "Unknown MessageId"),
            ProtocolError::Io(err) => write!(f, "IO error: {}", err),
        }
    }
}

// Implement `std::error::Error` for `ProtocolError`
impl std::error::Error for ProtocolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ProtocolError::Io(err) => Some(err),
            _ => None,
        }
    }
}

// Implement `From<io::Error>` for `ProtocolError`
impl From<io::Error> for ProtocolError {
    fn from(err: io::Error) -> Self {
        ProtocolError::Io(err)
    }
}
