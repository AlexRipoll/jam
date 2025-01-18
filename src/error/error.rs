use std::{error::Error, fmt::Display};

use tokio::{
    io,
    sync::{broadcast, mpsc},
};

use crate::{
    bitfield::bitfield::Bitfield,
    p2p::{message::Message, piece::Piece},
};

#[derive(Debug, PartialEq, Eq)]
pub enum PieceError {
    MissingBlocks,
    OutOfBounds,
}

impl Display for PieceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PieceError::MissingBlocks => write!(f, "Unable to assemble piece, missing blocks"),
            PieceError::OutOfBounds => write!(f, "Block index out of bounds"),
        }
    }
}

impl Error for PieceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum P2pError {
    EmptyPayload,
    InvalidHandshake,
    IncompleteMessage,
    EndOfWork,
    DeserializationError(&'static str),
    PieceMissingBlocks(PieceError),
    PieceOutOfBounds(PieceError),
    PieceNotFound,
    PieceInvalid,
    DiskTxError(mpsc::error::SendError<(Piece, Vec<u8>)>),
    IoTxError(mpsc::error::SendError<Message>),
    ClientTxError(mpsc::error::SendError<Bitfield>),
    ShutdownError(BroadcastSendError),
    IoError(IoErrorWrapper),
}

impl Display for P2pError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            P2pError::EmptyPayload => write!(f, "Empty payload"),
            P2pError::InvalidHandshake => write!(f, "Handshake response was not 68 bytes"),
            P2pError::EndOfWork => write!(f, "No available pieces to download from peer"),
            P2pError::IncompleteMessage => {
                write!(f, "Connection closed before reading full message")
            }
            P2pError::DeserializationError(err) => write!(f, "Deserialization error: {}", err),
            P2pError::PieceNotFound => write!(f, "Piece not found in map"),
            P2pError::PieceInvalid => write!(f, "Invalid piece, hash mismatch"),
            P2pError::PieceMissingBlocks(err) => write!(f, "{}", err),
            P2pError::PieceOutOfBounds(err) => write!(f, "{}", err),
            P2pError::DiskTxError(err) => write!(f, "Disk tx error: {}", err),
            P2pError::IoTxError(err) => write!(f, "IO tx error: {}", err),
            P2pError::ClientTxError(err) => write!(f, "Client tx error: {}", err),
            P2pError::IoError(IoErrorWrapper(err)) => write!(f, "IO error: {}", err),
            P2pError::ShutdownError(BroadcastSendError(err)) => {
                write!(f, "Shutdown tx error: {}", err)
            }
        }
    }
}

impl From<&'static str> for P2pError {
    fn from(err: &'static str) -> Self {
        P2pError::DeserializationError(err)
    }
}

impl From<PieceError> for P2pError {
    fn from(err: PieceError) -> Self {
        match err {
            PieceError::MissingBlocks => P2pError::PieceMissingBlocks(err),
            PieceError::OutOfBounds => P2pError::PieceOutOfBounds(err),
        }
    }
}

impl From<mpsc::error::SendError<(Piece, Vec<u8>)>> for P2pError {
    fn from(err: mpsc::error::SendError<(Piece, Vec<u8>)>) -> Self {
        P2pError::DiskTxError(err)
    }
}

impl From<mpsc::error::SendError<Message>> for P2pError {
    fn from(err: mpsc::error::SendError<Message>) -> Self {
        P2pError::IoTxError(err)
    }
}

impl From<mpsc::error::SendError<Bitfield>> for P2pError {
    fn from(err: mpsc::error::SendError<Bitfield>) -> Self {
        P2pError::ClientTxError(err)
    }
}

impl From<broadcast::error::SendError<()>> for P2pError {
    fn from(err: broadcast::error::SendError<()>) -> Self {
        P2pError::ShutdownError(BroadcastSendError(err))
    }
}

impl From<io::Error> for P2pError {
    fn from(err: io::Error) -> Self {
        P2pError::IoError(IoErrorWrapper(err))
    }
}

impl Error for P2pError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            P2pError::PieceMissingBlocks(err) => Some(err),
            P2pError::PieceOutOfBounds(err) => Some(err),
            P2pError::DiskTxError(err) => Some(err),
            P2pError::IoTxError(err) => Some(err),
            P2pError::ClientTxError(err) => Some(err),
            P2pError::IoError(IoErrorWrapper(err)) => Some(err),
            P2pError::ShutdownError(BroadcastSendError(err)) => Some(err),
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
