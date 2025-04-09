use std::{error::Error, fmt::Display, sync::Arc};

use tokio::{
    io::{self, AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
    task::JoinHandle,
};

use tracing::error;

use super::{
    message::{Message, MessageError},
    session::PeerSessionEvent,
};

pub async fn run(
    stream: TcpStream,
    event_tx: mpsc::Sender<PeerSessionEvent>,
) -> (mpsc::Sender<Message>, JoinHandle<()>, JoinHandle<()>) {
    let (out_tx, mut out_rx) = mpsc::channel::<Message>(100);
    let cmd_tx_clone = event_tx.clone();

    let (mut read_half, mut write_half) = tokio::io::split(stream);

    // peer listener task
    let in_handle = tokio::spawn(async move {
        loop {
            match read_message(&mut read_half).await {
                Ok(message) => {
                    if let Err(e) = cmd_tx_clone
                        .send(PeerSessionEvent::PeerMessageIn { message })
                        .await
                    {
                        error!("Failed to forward peer message: {}", e);
                    }
                }
                Err(e) => match e {
                    IoError::IncompleteMessage => {
                        if let Err(e) = cmd_tx_clone.send(PeerSessionEvent::ConnectionClosed).await
                        {
                            error!("Failed to notify connection closure: {}", e);
                        }
                    }
                    _ => error!("Error reading peer message: {}", e),
                },
            }
        }
    });

    // peer requester task
    let out_handle = tokio::spawn(async move {
        while let Some(message) = out_rx.recv().await {
            if let Err(e) = send_message(&mut write_half, message).await {
                error!("Failed to send message: {}", e);
            }
        }
    });

    (out_tx, in_handle, out_handle)
}

async fn read_message<T>(read_half: &mut T) -> Result<Message, IoError>
where
    T: AsyncRead + Unpin,
{
    let mut length_buffer = [0u8; 4];
    let mut bytes_read = 0;

    // Read the 4-byte length field
    while bytes_read < 4 {
        match read_half.read(&mut length_buffer[bytes_read..]).await {
            Ok(0) => {
                // Connection closed before reading the full message
                return Err(IoError::IncompleteMessage);
            }
            Ok(n) => bytes_read += n,
            Err(e) => return Err(e.into()),
        }
    }

    // Convert the length bytes from Big-Endian to usize
    let message_length = u32::from_be_bytes(length_buffer) as usize;

    // Allocate a buffer for the message based on the length
    let mut message_buffer = vec![0u8; 4 + message_length];
    message_buffer[..4].copy_from_slice(&length_buffer);

    bytes_read = 0;

    // Read the actual message data into the buffer
    while bytes_read < message_length {
        match read_half.read(&mut message_buffer[4 + bytes_read..]).await {
            Ok(0) => {
                // Connection closed before reading the full message
                return Err(IoError::IncompleteMessage);
            }
            Ok(n) => bytes_read += n,
            Err(e) => return Err(e.into()),
        }
    }

    let message = Message::deserialize(&message_buffer)?;

    Ok(message)
}

async fn send_message(
    write_half: &mut tokio::io::WriteHalf<TcpStream>,
    message: Message,
) -> Result<(), IoError> {
    write_half.write_all(&message.serialize()).await?;

    Ok(())
}

#[derive(Debug)]
pub enum IoError {
    Message(MessageError),
    IncompleteMessage,
    Io(io::Error),
}

impl Display for IoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IoError::Message(err) => write!(f, "Message error: {}", err),
            IoError::IncompleteMessage => {
                write!(f, "Connection closed before reading full message")
            }
            IoError::Io(err) => write!(f, "IO error: {}", err),
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

impl Error for IoError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            IoError::Message(err) => Some(err),
            IoError::Io(err) => Some(err),
            _ => None,
        }
    }
}

#[cfg(test)]
mod test {
    use tokio::io::{duplex, AsyncWriteExt};

    use crate::torrent::peers::{
        io::{read_message, IoError},
        message::{Message, MessageId},
    };

    #[tokio::test]
    async fn test_read_message() {
        // Create a duplex stream to mock a TcpStream
        let (mut mock_write, mock_read) = duplex(1024);
        let (mut read_half, _) = tokio::io::split(mock_read);

        // Test data setup
        let message = Message::new(MessageId::Interested, Some(vec![1, 2, 3, 4]));
        let serialized_message = message.serialize();

        // Write the serialized message into the mock stream
        tokio::spawn(async move {
            mock_write.write_all(&serialized_message).await.unwrap();
            mock_write.shutdown().await.unwrap();
        });

        // Read and verify the message using read_message
        let result = read_message(&mut read_half).await;

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

        // Test data setup for KeepAlive (length 0)
        let serialized_message = [0, 0, 0, 0]; // KeepAlive has a 0-length prefix

        // Write the serialized message into the mock stream
        tokio::spawn(async move {
            mock_write.write_all(&serialized_message).await.unwrap();
            mock_write.shutdown().await.unwrap();
        });

        // Read and verify the message using read_message
        let result = read_message(&mut read_half).await;

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

        // Write an incomplete message into the mock stream
        let incomplete_message = [0, 0, 0, 5, 6]; // Length 5, Message ID: Request, missing 4 bytes
        tokio::spawn(async move {
            mock_write.write_all(&incomplete_message).await.unwrap();
            mock_write.shutdown().await.unwrap();
        });

        // Attempt to read the message
        let result = read_message(&mut read_half).await;

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

        // Attempt to read from the stream without writing anything
        let result = read_message(&mut read_half).await;

        // Assertions
        match result.unwrap_err() {
            IoError::IncompleteMessage => {
                // do nothing: expected result
            }
            _ => panic!("IoError::IncompleteMessage error expected"),
        }
    }
}
