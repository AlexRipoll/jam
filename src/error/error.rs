use std::{error::Error, fmt::Display};

use tokio::{
    io,
    sync::{broadcast, mpsc},
};

use crate::p2p::piece::Piece;
use protocol::bitfield::Bitfield;
use protocol::message::Message;

#[derive(Debug, PartialEq, Eq)]
pub enum JamError {
    EmptyPayload,
    InvalidHandshake,
    IncompleteMessage,
    EndOfWork,
    DeserializationError(&'static str),
    PieceMissingBlocks,
    PieceOutOfBounds,
    PieceNotFound,
    PieceInvalid,
    DiskTxError(mpsc::error::SendError<(Piece, Vec<u8>)>),
    IoTxError(mpsc::error::SendError<Message>),
    ClientTxError(mpsc::error::SendError<Bitfield>),
    ShutdownError(BroadcastSendError),
    IoError(IoErrorWrapper),
}

impl Display for JamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JamError::EmptyPayload => write!(f, "Empty payload"),
            JamError::InvalidHandshake => write!(f, "Handshake response was not 68 bytes"),
            JamError::EndOfWork => write!(f, "No available pieces to download from peer"),
            JamError::IncompleteMessage => {
                write!(f, "Connection closed before reading full message")
            }
            JamError::DeserializationError(err) => write!(f, "Deserialization error: {}", err),
            JamError::PieceNotFound => write!(f, "Piece not found in map"),
            JamError::PieceInvalid => write!(f, "Invalid piece, hash mismatch"),
            JamError::PieceMissingBlocks => {
                write!(f, "Unable to assemble piece, missing blocks")
            }
            JamError::PieceOutOfBounds => write!(f, "Block index out of bounds"),
            JamError::DiskTxError(err) => write!(f, "Disk tx error: {}", err),
            JamError::IoTxError(err) => write!(f, "IO tx error: {}", err),
            JamError::ClientTxError(err) => write!(f, "Client tx error: {}", err),
            JamError::IoError(IoErrorWrapper(err)) => write!(f, "IO error: {}", err),
            JamError::ShutdownError(BroadcastSendError(err)) => {
                write!(f, "Shutdown tx error: {}", err)
            }
        }
    }
}

impl From<&'static str> for JamError {
    fn from(err: &'static str) -> Self {
        JamError::DeserializationError(err)
    }
}

impl From<mpsc::error::SendError<(Piece, Vec<u8>)>> for JamError {
    fn from(err: mpsc::error::SendError<(Piece, Vec<u8>)>) -> Self {
        JamError::DiskTxError(err)
    }
}

impl From<mpsc::error::SendError<Message>> for JamError {
    fn from(err: mpsc::error::SendError<Message>) -> Self {
        JamError::IoTxError(err)
    }
}

impl From<mpsc::error::SendError<Bitfield>> for JamError {
    fn from(err: mpsc::error::SendError<Bitfield>) -> Self {
        JamError::ClientTxError(err)
    }
}

impl From<broadcast::error::SendError<()>> for JamError {
    fn from(err: broadcast::error::SendError<()>) -> Self {
        JamError::ShutdownError(BroadcastSendError(err))
    }
}

impl From<io::Error> for JamError {
    fn from(err: io::Error) -> Self {
        JamError::IoError(IoErrorWrapper(err))
    }
}

impl Error for JamError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            JamError::DiskTxError(err) => Some(err),
            JamError::IoTxError(err) => Some(err),
            JamError::ClientTxError(err) => Some(err),
            JamError::IoError(IoErrorWrapper(err)) => Some(err),
            JamError::ShutdownError(BroadcastSendError(err)) => Some(err),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct BroadcastSendError(pub broadcast::error::SendError<()>);

impl PartialEq for BroadcastSendError {
    fn eq(&self, _other: &Self) -> bool {
        // You can define custom equality logic if needed. For simplicity, we treat all errors as equal.
        true
    }
}

impl Eq for BroadcastSendError {}

#[derive(Debug)]
pub struct IoErrorWrapper(pub io::Error);

impl PartialEq for IoErrorWrapper {
    fn eq(&self, other: &Self) -> bool {
        // Compare the `kind` of the error as `io::Error` does not support equality checks.
        self.0.kind() == other.0.kind()
    }
}

impl Eq for IoErrorWrapper {}
