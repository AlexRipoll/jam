use std::{error::Error, fmt::Display};

use tokio::{
    io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
    task::JoinHandle,
};

use tracing::{debug, error};

use super::{
    message::{Message, MessageError},
    session::PeerSessionEvent,
};

// Initial buffer capacity for message reading
const INITIAL_BUFFER_CAPACITY: usize = 4096;
// Maximum allowed message size to prevent DoS attacks
const MAX_MESSAGE_SIZE: usize = 16_777_216; // 16 MB

/// Set up peer communication tasks and return the necessary handles
///
/// # Arguments
/// * `stream` - The TCP connection to the peer
/// * `event_tx` - Channel for forwarding peer events to the session
///
/// # Returns
/// A tuple containing:
/// * A sender channel for outgoing messages
/// * A join handle for the incoming message handler task
/// * A join handle for the outgoing message handler task
pub async fn run(
    stream: TcpStream,
    event_tx: mpsc::Sender<PeerSessionEvent>,
) -> (mpsc::Sender<Message>, JoinHandle<()>, JoinHandle<()>) {
    let (out_tx, mut out_rx) = mpsc::channel::<Message>(128);
    let (read_half, write_half) = io::split(stream);

    // Set up peer reader task
    let reader_event_tx = event_tx.clone();
    let in_handle = tokio::spawn(async move {
        if let Err(e) = handle_incoming_messages(read_half, reader_event_tx).await {
            error!("Peer reader task terminated: {e}");
        }
    });

    // Set up peer writer task
    let writer_event_tx = event_tx.clone();
    let out_handle = tokio::spawn(async move {
        if let Err(e) = handle_outgoing_messages(write_half, &mut out_rx, writer_event_tx).await {
            error!("Peer writer task terminated: {e}");
        }
    });

    (out_tx, in_handle, out_handle)
}

/// Handle incoming messages from the peer
///
/// Reads messages from the stream and forwards them to the session via event_tx
async fn handle_incoming_messages<T>(
    mut read_half: io::ReadHalf<T>,
    event_tx: mpsc::Sender<PeerSessionEvent>,
) -> Result<(), IoError>
where
    T: AsyncRead + Unpin,
{
    let mut buffer = Vec::with_capacity(INITIAL_BUFFER_CAPACITY);

    loop {
        match read_message(&mut read_half, &mut buffer).await {
            Ok(message) => {
                if let Err(e) = event_tx
                    .send(PeerSessionEvent::PeerMessageIn { message })
                    .await
                {
                    error!("Failed to forward peer message: {}", e);
                    break;
                }
            }
            Err(IoError::IncompleteMessage) => {
                debug!("Peer connection closed");
                if let Err(e) = event_tx.send(PeerSessionEvent::ConnectionClosed).await {
                    error!("Failed to notify ConnectionClosed: {e}");
                }
                break;
            }
            Err(e) => {
                error!("Error reading peer message: {e}");
                // if let Err(e) = event_tx
                //     .send(PeerSessionEvent::ConnectionError {
                //         error: format!("{e}"),
                //     })
                //     .await
                // {
                //     error!("Failed to notify ConnectionError: {e}");
                // }
                return Err(e);
            }
        }
    }

    Ok(())
}

/// Handle outgoing messages to the peer
///
/// Receives messages from out_rx and sends them to the peer
async fn handle_outgoing_messages<T>(
    mut write_half: io::WriteHalf<T>,
    out_rx: &mut mpsc::Receiver<Message>,
    event_tx: mpsc::Sender<PeerSessionEvent>,
) -> Result<(), IoError>
where
    T: AsyncWrite + Unpin,
{
    while let Some(message) = out_rx.recv().await {
        if let Err(e) = send_message(&mut write_half, &message).await {
            error!("Failed to send message: {}", e);
            return Err(e);
        }
    }

    debug!("Outgoing message channel closed");
    Ok(())
}

/// Read a complete BitTorrent protocol message from the stream
///
/// # Arguments
/// * `read_half` - The read half of the stream
/// * `buffer` - Buffer to store the message (will be cleared and reused)
///
/// # Returns
/// The deserialized Message on success
async fn read_message<T>(read_half: &mut T, buffer: &mut Vec<u8>) -> Result<Message, IoError>
where
    T: AsyncRead + Unpin,
{
    // Clear the buffer but retain capacity
    buffer.clear();

    // Read the 4-byte length prefix
    let mut length_bytes = [0u8; 4];
    match read_half.read_exact(&mut length_bytes).await {
        Ok(_) => {
            // Length prefix read successfully
        }
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
            return Err(IoError::IncompleteMessage);
        }
        Err(e) => return Err(IoError::Io(e)),
    }

    // Convert the length bytes from Big-Endian to usize
    let message_length = u32::from_be_bytes(length_bytes) as usize;

    // Check if the message size is reasonable
    if message_length > MAX_MESSAGE_SIZE {
        return Err(IoError::MessageTooLarge(message_length, MAX_MESSAGE_SIZE));
    }

    // Add length prefix to buffer
    buffer.extend_from_slice(&length_bytes);

    // If message length is 0 (keep-alive), we don't need to read more
    if message_length == 0 {
        return Message::deserialize(buffer).map_err(IoError::from);
    }

    // Ensure buffer has enough capacity
    buffer.reserve(message_length);
    let original_len = buffer.len();

    // Read the message body directly into the buffer
    buffer.resize(original_len + message_length, 0);
    match read_half.read_exact(&mut buffer[original_len..]).await {
        Ok(_) => {
            // Message body read successfully
        }
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
            return Err(IoError::IncompleteMessage);
        }
        Err(e) => return Err(IoError::Io(e)),
    }

    // Deserialize the complete message
    Message::deserialize(buffer).map_err(IoError::from)
}

/// Send a BitTorrent protocol message to the peer
///
/// # Arguments
/// * `write_half` - The write half of the stream
/// * `message` - The message to send
async fn send_message<T>(write_half: &mut T, message: &Message) -> Result<(), IoError>
where
    T: AsyncWrite + Unpin,
{
    let serialized = message.serialize();
    write_half.write_all(&serialized).await?;
    write_half.flush().await?;

    Ok(())
}

/// Errors that can occur during peer I/O operations
#[derive(Debug)]
pub enum IoError {
    /// Error in message serialization or deserialization
    Message(MessageError),

    /// Connection closed before reading the complete message
    IncompleteMessage,

    /// Message too large (possible DoS attack)
    MessageTooLarge(usize, usize),

    /// Standard I/O error
    Io(io::Error),
}

impl Display for IoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IoError::Message(err) => write!(f, "Message processing error: {}", err),
            IoError::IncompleteMessage => {
                write!(f, "Connection closed before reading the complete message")
            }
            IoError::MessageTooLarge(size, max) => write!(
                f,
                "Message size ({} bytes) exceeds maximum allowed size ({} bytes)",
                size, max
            ),
            IoError::Io(err) => write!(f, "I/O error: {}", err),
        }
    }
}

impl Error for IoError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            IoError::Message(err) => Some(err),
            IoError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<MessageError> for IoError {
    fn from(err: MessageError) -> Self {
        IoError::Message(err)
    }
}

impl From<io::Error> for IoError {
    fn from(err: io::Error) -> Self {
        IoError::Io(err)
    }
}

#[cfg(test)]
mod test {
    use crate::peer::{
        io::{read_message, IoError},
        message::{Message, MessageId},
    };
    use tokio::io::{duplex, AsyncWriteExt};

    #[tokio::test]
    async fn test_read_message() {
        // Create a duplex stream to mock a TcpStream
        let (mut mock_write, mock_read) = duplex(1024);
        let (mut read_half, _) = tokio::io::split(mock_read);

        // Create buffer for read_message
        let mut buffer = Vec::with_capacity(1024);

        // Test data setup
        let message = Message::new(MessageId::Interested, Some(vec![1, 2, 3, 4]));
        let serialized_message = message.serialize();

        // Write the serialized message into the mock stream
        tokio::spawn(async move {
            mock_write.write_all(&serialized_message).await.unwrap();
            mock_write.shutdown().await.unwrap();
        });

        // Read and verify the message using read_message
        let result = read_message(&mut read_half, &mut buffer).await;

        // Assertions
        assert!(result.is_ok(), "Failed to read message");
        let received_message = result.unwrap();
        assert_eq!(received_message, message, "Messages do not match");
    }

    #[tokio::test]
    async fn test_read_message_keep_alive() {
        // Create a duplex stream to mock a TcpStream
        let (mut mock_write, mock_read) = duplex(1024);
        let (mut read_half, _) = tokio::io::split(mock_read);

        // Create buffer for read_message
        let mut buffer = Vec::with_capacity(1024);

        // Test data setup for KeepAlive (length 0)
        let serialized_message = [0, 0, 0, 0]; // KeepAlive has a 0-length prefix

        // Write the serialized message into the mock stream
        tokio::spawn(async move {
            mock_write.write_all(&serialized_message).await.unwrap();
            mock_write.shutdown().await.unwrap();
        });

        // Read and verify the message using read_message
        let result = read_message(&mut read_half, &mut buffer).await;

        // Assertions
        assert!(result.is_ok(), "Failed to read KeepAlive message");
        let received_message = result.unwrap();
        assert_eq!(
            received_message,
            Message::new(MessageId::KeepAlive, None),
            "Messages do not match"
        );
    }

    #[tokio::test]
    async fn test_read_message_incomplete() {
        // Create a duplex stream to mock a TcpStream
        let (mut mock_write, mock_read) = duplex(1024);
        let (mut read_half, _) = tokio::io::split(mock_read);

        // Create buffer for read_message
        let mut buffer = Vec::with_capacity(1024);

        // Write an incomplete message into the mock stream
        let incomplete_message = [0, 0, 0, 5, 6]; // Length 5, Message ID: Request, missing 4 bytes
        tokio::spawn(async move {
            mock_write.write_all(&incomplete_message).await.unwrap();
            mock_write.shutdown().await.unwrap();
        });

        // Attempt to read the message
        let result = read_message(&mut read_half, &mut buffer).await;

        // Assertions
        match result.unwrap_err() {
            IoError::IncompleteMessage => {
                // do nothing: expected result
            }
            _ => panic!("IoError::IncompleteMessage error expected"),
        }
    }

    #[tokio::test]
    async fn test_read_message_connection_closed() {
        // Create a duplex stream to mock a TcpStream
        let (_, mock_read) = duplex(1024);
        let (mut read_half, _) = tokio::io::split(mock_read);

        // Create buffer for read_message
        let mut buffer = Vec::with_capacity(1024);

        // Attempt to read from the stream without writing anything
        let result = read_message(&mut read_half, &mut buffer).await;

        // Assertions
        match result.unwrap_err() {
            IoError::IncompleteMessage => {
                // do nothing: expected result
            }
            _ => panic!("IoError::IncompleteMessage error expected"),
        }
    }
}
