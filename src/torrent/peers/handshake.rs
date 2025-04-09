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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn test_handshake_new() {
        let info_hash = [1u8; 20];
        let peer_id = [2u8; 20];

        let handshake = Handshake::new(info_hash, peer_id);

        assert_eq!(handshake.pstr, "BitTorrent protocol");
        assert_eq!(handshake.reserved, [0u8; 8]);
        assert_eq!(handshake.info_hash, info_hash);
        assert_eq!(handshake.peer_id, peer_id);
    }

    #[test]
    fn test_handshake_serialize() {
        let info_hash = [1u8; 20];
        let peer_id = [2u8; 20];
        let handshake = Handshake::new(info_hash, peer_id);

        let serialized = handshake.serialize();

        // Expected format:
        // 1 byte for pstr length (19)
        // 19 bytes for "BitTorrent protocol"
        // 8 bytes for reserved
        // 20 bytes for info_hash
        // 20 bytes for peer_id
        assert_eq!(serialized.len(), 68);
        assert_eq!(serialized[0], 19); // pstr length
        assert_eq!(&serialized[1..20], PSTR.as_bytes()); // pstr
        assert_eq!(&serialized[20..28], &[0u8; 8]); // reserved
        assert_eq!(&serialized[28..48], &[1u8; 20]); // info_hash
        assert_eq!(&serialized[48..68], &[2u8; 20]); // peer_id
    }

    #[test]
    fn test_handshake_deserialize_valid() {
        let info_hash = [1u8; 20];
        let peer_id = [2u8; 20];
        let original = Handshake::new(info_hash, peer_id);

        let serialized = original.serialize();
        let deserialized = Handshake::deserialize(serialized).unwrap();

        assert_eq!(deserialized.pstr, original.pstr);
        assert_eq!(deserialized.reserved, original.reserved);
        assert_eq!(deserialized.info_hash, original.info_hash);
        assert_eq!(deserialized.peer_id, original.peer_id);
    }

    #[test]
    fn test_handshake_deserialize_invalid_pstr_length() {
        let mut buffer = vec![0u8; 68];
        buffer[0] = 18; // Wrong pstr length (should be 19)

        let result = Handshake::deserialize(buffer);
        assert!(matches!(result, Err(HandshakeError::InvalidPstrLength)));
    }

    #[test]
    fn test_handshake_deserialize_invalid_pstr() {
        let mut buffer = vec![0u8; 68];
        buffer[0] = 19; // Correct length
                        // Insert invalid UTF-8 bytes
        buffer[10] = 0xFF;
        buffer[11] = 0xFF;

        let result = Handshake::deserialize(buffer);
        assert!(matches!(result, Err(HandshakeError::InvalidPstr)));
    }

    #[test]
    fn test_handshake_deserialize_truncated_message() {
        // Create a truncated message with only peer_id partially filled
        let mut buffer = Vec::with_capacity(65);
        buffer.push(19); // pstr length
        buffer.extend_from_slice(PSTR.as_bytes());
        buffer.extend_from_slice(&[0u8; 8]); // reserved
        buffer.extend_from_slice(&[1u8; 20]); // info_hash
        buffer.extend_from_slice(&[2u8; 17]); // peer_id (truncated)

        // Should still work, with the peer_id padded with zeros
        let result = Handshake::deserialize(buffer).unwrap();
        let mut expected_peer_id = [0u8; 20];
        expected_peer_id[..17].copy_from_slice(&[2u8; 17]);

        assert_eq!(result.peer_id, expected_peer_id);
    }

    #[tokio::test]
    async fn test_perform_handshake() {
        // Start a mock server in a separate task
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Client handshake data
        let client_info_hash = [1u8; 20];
        let client_peer_id = [2u8; 20];
        let client_handshake = Handshake::new(client_info_hash.clone(), client_peer_id.clone());

        // Server handshake data
        let server_info_hash = [3u8; 20];
        let server_peer_id = [4u8; 20];
        let server_handshake = Handshake::new(server_info_hash.clone(), server_peer_id.clone());

        // Start the server
        let server_handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();

            // Read client handshake
            let mut buf = vec![0u8; 68];
            socket.read_exact(&mut buf).await.unwrap();

            let client_hs = Handshake::deserialize(buf).unwrap();
            assert_eq!(client_hs.info_hash, client_info_hash);
            assert_eq!(client_hs.peer_id, client_peer_id);

            // Send server handshake
            socket
                .write_all(&server_handshake.serialize())
                .await
                .unwrap();
        });

        // Client connects and performs handshake
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let response = perform_handshake(&mut stream, &client_handshake)
            .await
            .unwrap();

        // Verify response
        let server_hs = Handshake::deserialize(response).unwrap();
        assert_eq!(server_hs.info_hash, server_info_hash);
        assert_eq!(server_hs.peer_id, server_peer_id);

        // Wait for server to complete
        server_handle.await.unwrap();
    }

    #[test]
    fn test_handshake_error_display() {
        let err1 = HandshakeError::InvalidPstrLength;
        let err2 = HandshakeError::InvalidPstr;
        let err3 = HandshakeError::InvalidLength;
        let err4 = HandshakeError::IoError(io::Error::new(io::ErrorKind::Other, "test error"));

        assert_eq!(err1.to_string(), "Peer responded with invalid PSTR length");
        assert_eq!(err2.to_string(), "Invalid UTF-8 PSTR format");
        assert_eq!(
            err3.to_string(),
            "Invalid handshake length, must be 68 bytes long"
        );
        assert_eq!(err4.to_string(), "I/O error: test error");
    }
}
