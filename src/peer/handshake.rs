use std::{error::Error, fmt::Display};

use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

/// Standard BitTorrent protocol identifier.
const PSTR: &str = "BitTorrent protocol";
/// Length of a complete handshake message in bytes.
const HANDSHAKE_LENGTH: usize = 68;
/// Size of the reserved field in bytes.
const RESERVED_SIZE: usize = 8;
/// Size of the info hash and peer ID fields in bytes.
const HASH_SIZE: usize = 20;

/// Represents a BitTorrent handshake message.
///
/// The BitTorrent handshake is the first message exchanged between peers
/// to establish a connection and verify they are connecting for the same torrent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    /// Protocol string identifier, typically "BitTorrent protocol"
    pub pstr: String,
    /// Reserved bytes for protocol extensions
    pub reserved: [u8; RESERVED_SIZE],
    /// SHA-1 hash of the info dictionary from the torrent metainfo
    pub info_hash: [u8; HASH_SIZE],
    /// Unique identifier for the connecting client
    pub peer_id: [u8; HASH_SIZE],
}

impl Handshake {
    /// Creates a new handshake message with the specified info hash and peer ID.
    ///
    /// # Arguments
    ///
    /// * `info_hash` - The SHA-1 hash of the info dictionary from the torrent metainfo
    /// * `peer_id` - The unique identifier for this client
    ///
    /// # Examples
    ///
    /// ```
    /// use bittorrent::handshake::Handshake;
    ///
    /// let info_hash = [0u8; 20];
    /// let peer_id = [1u8; 20];
    /// let handshake = Handshake::new(info_hash, peer_id);
    /// ```
    pub fn new(info_hash: [u8; HASH_SIZE], peer_id: [u8; HASH_SIZE]) -> Self {
        Self {
            pstr: PSTR.to_string(),
            reserved: [0u8; RESERVED_SIZE],
            info_hash,
            peer_id,
        }
    }

    /// Serializes the handshake message to a byte vector for transmission.
    ///
    /// # Returns
    ///
    /// A vector of bytes representing the handshake message.
    pub fn serialize(&self) -> Vec<u8> {
        let pstr_bytes = self.pstr.as_bytes();
        let mut buf = Vec::with_capacity(pstr_bytes.len() + 49);

        buf.push(pstr_bytes.len() as u8);
        buf.extend_from_slice(pstr_bytes);
        buf.extend_from_slice(&self.reserved);
        buf.extend_from_slice(&self.info_hash);
        buf.extend_from_slice(&self.peer_id);

        buf
    }

    /// Deserializes a byte buffer into a handshake message.
    ///
    /// # Arguments
    ///
    /// * `buffer` - The buffer containing the serialized handshake message
    ///
    /// # Returns
    ///
    /// A Result containing either the deserialized Handshake or a HandshakeError
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The protocol string length is invalid
    /// - The protocol string contains invalid UTF-8
    /// - The buffer doesn't contain enough data
    pub fn deserialize(buffer: &[u8]) -> Result<Self, HandshakeError> {
        if buffer.len() != HANDSHAKE_LENGTH {
            return Err(HandshakeError::InvalidLength(buffer.len()));
        }

        let mut offset = 0;

        // Parse protocol string length (1 byte)
        let pstr_length = buffer[offset];
        offset += 1;

        // Check if protocol string length matches expected length
        if pstr_length as usize != PSTR.len() {
            return Err(HandshakeError::InvalidPstrLength);
        }

        // Parse protocol string
        let pstr_end = offset + pstr_length as usize;
        let pstr = std::str::from_utf8(&buffer[offset..pstr_end])
            .map_err(|_| HandshakeError::InvalidPstr)?
            .to_string();
        offset = pstr_end;

        // Parse reserved bytes (8 bytes)
        let mut reserved = [0u8; RESERVED_SIZE];
        reserved.copy_from_slice(&buffer[offset..offset + RESERVED_SIZE]);
        offset += RESERVED_SIZE;

        // Parse info hash (20 bytes)
        let mut info_hash = [0u8; HASH_SIZE];
        info_hash.copy_from_slice(&buffer[offset..offset + HASH_SIZE]);
        offset += HASH_SIZE;

        // Parse peer ID (20 bytes)
        let mut peer_id = [0u8; HASH_SIZE];
        peer_id.copy_from_slice(&buffer[offset..offset + HASH_SIZE]);

        Ok(Self {
            pstr,
            reserved,
            info_hash,
            peer_id,
        })
    }
}

/// Performs a handshake with a peer using the given TCP stream.
///
/// This function sends a handshake to the peer and waits for their response.
///
/// # Arguments
///
/// * `stream` - A mutable reference to a connected TCP stream
/// * `handshake` - The handshake message to send
///
/// # Returns
///
/// A Result containing the received Handshake on success
///
/// # Errors
///
/// Returns an error if:
/// - There's an I/O error during communication
/// - The handshake times out
/// - The response has an invalid format
pub async fn exchange_handshake(
    stream: &mut TcpStream,
    handshake: &Handshake,
) -> Result<Handshake, HandshakeError> {
    // Send our handshake
    stream.write_all(&handshake.serialize()).await?;

    let buf = receive_handshake_bytes(stream).await?;

    // Deserialize the response
    let received_handshake = Handshake::deserialize(&buf)?;

    Ok(received_handshake)
}

/// Reads a complete handshake response from the stream.
///
/// # Arguments
///
/// * `stream` - The TCP stream to read from
///
/// # Returns
///
/// A Result containing the raw handshake bytes on success
async fn receive_handshake_bytes(stream: &mut TcpStream) -> Result<Vec<u8>, HandshakeError> {
    let mut buffer = vec![0u8; HANDSHAKE_LENGTH];
    let mut bytes_read = 0;

    // Loop until we've read exactly HANDSHAKE_LENGTH bytes
    while bytes_read < HANDSHAKE_LENGTH {
        match stream.read(&mut buffer[bytes_read..]).await {
            Ok(0) => {
                // Connection closed prematurely
                return Err(HandshakeError::InvalidLength(bytes_read));
            }
            Ok(n) => bytes_read += n,
            Err(e) => return Err(HandshakeError::IoError(e)),
        }
    }

    Ok(buffer)
}

/// Errors that can occur during handshake operations.
#[derive(Debug)]
pub enum HandshakeError {
    /// Protocol string length is incorrect
    InvalidPstrLength,
    /// Protocol string contains invalid UTF-8
    InvalidPstr,
    /// Handshake message has incorrect length
    InvalidLength(usize),
    /// Error during network I/O
    IoError(io::Error),
}

impl Display for HandshakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandshakeError::InvalidPstrLength => {
                write!(f, "Peer responded with invalid protocol string length")
            }
            HandshakeError::InvalidPstr => write!(f, "Invalid UTF-8 in protocol string"),
            HandshakeError::InvalidLength(received) => {
                write!(f, "Invalid handshake length, received {received} bytes but expected {HANDSHAKE_LENGTH}")
            }
            HandshakeError::IoError(e) => write!(f, "I/O error: {e}"),
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

impl From<io::Error> for HandshakeError {
    fn from(err: io::Error) -> Self {
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
        let deserialized = Handshake::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.pstr, original.pstr);
        assert_eq!(deserialized.reserved, original.reserved);
        assert_eq!(deserialized.info_hash, original.info_hash);
        assert_eq!(deserialized.peer_id, original.peer_id);
    }

    #[test]
    fn test_handshake_deserialize_invalid_pstr_length() {
        let mut buffer = vec![0u8; 68];
        buffer[0] = 18; // Wrong pstr length (should be 19)

        let result = Handshake::deserialize(&buffer);
        assert!(matches!(result, Err(HandshakeError::InvalidPstrLength)));
    }

    #[test]
    fn test_handshake_deserialize_invalid_pstr() {
        let mut buffer = vec![0u8; 68];
        buffer[0] = 19; // Correct length
                        // Insert invalid UTF-8 bytes
        buffer[10] = 0xFF;
        buffer[11] = 0xFF;

        let result = Handshake::deserialize(&buffer);
        assert!(matches!(result, Err(HandshakeError::InvalidPstr)));
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

            let client_hs = Handshake::deserialize(&buf).unwrap();
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
        let server_hs = exchange_handshake(&mut stream, &client_handshake)
            .await
            .unwrap();

        // Verify response
        assert_eq!(server_hs.info_hash, server_info_hash);
        assert_eq!(server_hs.peer_id, server_peer_id);

        // Wait for server to complete
        server_handle.await.unwrap();
    }
}
