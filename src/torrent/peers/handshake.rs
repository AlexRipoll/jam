use std::{error::Error, fmt::Display};

use tokio::{
    io::{self, AsyncWriteExt},
    net::TcpStream,
};

const PSTR: &str = "BitTorrent protocol";

#[derive(Debug)]
pub struct Handshake {
    pub pstr: String,
    pub reserved: [u8; 8],
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
}

impl Handshake {
    pub fn new(info_hash: [u8; 20], peer_id: [u8; 20]) -> Handshake {
        Handshake {
            pstr: PSTR.to_string(),
            reserved: [0u8; 8],
            info_hash,
            peer_id,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.pstr.len() + 49);

        buf.push(self.pstr.len() as u8);
        buf.extend_from_slice(self.pstr.as_bytes());
        buf.extend_from_slice(&self.reserved);
        buf.extend_from_slice(&self.info_hash);
        buf.extend_from_slice(&self.peer_id);

        buf
    }

    pub fn deserialize(buffer: Vec<u8>) -> Result<Handshake, HandshakeError> {
        let mut offset = 0;

        // Parse `pstr_length` (1 byte)
        let pstr_length = buffer[offset];
        offset += 1;

        // Check if `pstr_length` matches expected length for "BitTorrent protocol"
        if pstr_length as usize != PSTR.len() {
            return Err(HandshakeError::InvalidPstrLength);
        }

        // Parse `pstr` (19 bytes)
        let pstr = std::str::from_utf8(&buffer[offset..offset + pstr_length as usize])
            .map_err(|_| HandshakeError::InvalidPstr)?
            .to_string();

        offset += pstr_length as usize;

        // Parse `reserved` (8 bytes)
        let mut reserved = [0u8; 8];
        reserved.copy_from_slice(&buffer[offset..offset + 8]);
        offset += 8;

        // Parse `info_hash` (20 bytes)
        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&buffer[offset..offset + 20]);
        offset += 20;

        // Parse `peer_id` (20 bytes)
        let peer_id_len = std::cmp::min(buffer.len() - offset, 20);
        let mut peer_id = [0u8; 20];
        // peer_id.copy_from_slice(&buffer[offset..]);
        peer_id[..peer_id_len].copy_from_slice(&buffer[offset..offset + peer_id_len]);

        Ok(Handshake {
            pstr,
            reserved,
            info_hash,
            peer_id,
        })
    }
}

pub async fn perform_handshake(
    stream: &mut TcpStream,
    handshake_metadata: &Handshake,
) -> Result<Vec<u8>, HandshakeError> {
    stream.write_all(&handshake_metadata.serialize()).await?;

    let mut buffer = vec![0u8; 68];
    stream.readable().await?;

    let mut bytes_read = 0;
    // Loop until we've read exactly 68 bytes
    while bytes_read < 68 {
        match stream.try_read(&mut buffer[bytes_read..]) {
            Ok(0) => break, // Connection closed
            Ok(n) => bytes_read += n,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                stream.readable().await?;
            }
            Err(e) => return Err(e.into()),
        }
    }

    if bytes_read != 68 {
        return Err(HandshakeError::InvalidLength);
    }

    Ok(buffer)
}

#[derive(Debug)]
pub enum HandshakeError {
    InvalidPstrLength,
    InvalidPstr,
    InvalidLength,
    IoError(tokio::io::Error),
}

impl Display for HandshakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandshakeError::InvalidPstrLength => {
                write!(f, "Peer responded with invalid PSTR length")
            }
            HandshakeError::InvalidPstr => write!(f, "Invalid UTF-8 PSTR format"),
            HandshakeError::InvalidLength => {
                write!(f, "Invalid handshake length, must be 68 bytes long")
            }
            HandshakeError::IoError(e) => write!(f, "I/O error: {}", e),
        }
    }
}

impl Error for HandshakeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            HandshakeError::IoError(err) => Some(err),
            _ => None,
        }
    }
}

impl From<tokio::io::Error> for HandshakeError {
    fn from(err: tokio::io::Error) -> Self {
        HandshakeError::IoError(err)
    }
}
