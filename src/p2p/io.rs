use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use super::{connection::P2pError, message::Message};

pub async fn read_message<R>(read_half: &mut R) -> Result<Message, P2pError>
where
    R: AsyncRead + Unpin,
{
    let mut length_buffer = [0u8; 4];
    let mut bytes_read = 0;

    // Read the 4-byte length field
    while bytes_read < 4 {
        match read_half.read(&mut length_buffer[bytes_read..]).await {
            Ok(0) => {
                // Connection closed before reading the full message
                return Err(P2pError::IncompleteMessage);
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
                return Err(P2pError::IncompleteMessage);
            }
            Ok(n) => bytes_read += n,
            Err(e) => return Err(e.into()),
        }
    }

    let message = Message::deserialize(&message_buffer)?;

    Ok(message)
}

pub async fn send_message(
    write_half: &mut tokio::io::WriteHalf<TcpStream>,
    message: Message,
) -> Result<(), P2pError> {
    write_half.write_all(&message.serialize()).await?;

    Ok(())
}

#[cfg(test)]
mod test {
    use tokio::io::{duplex, AsyncWriteExt};

    use crate::p2p::{
        connection::P2pError,
        io::read_message,
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
        assert_eq!(result.unwrap_err(), P2pError::IncompleteMessage);
    }

    #[tokio::test]
    async fn test_read_message_connection_closed() {
        // Create a duplex stream to mock a TcpStream
        let (_, mock_read) = duplex(1024);
        let (mut read_half, _) = tokio::io::split(mock_read);

        // Attempt to read from the stream without writing anything
        let result = read_message(&mut read_half).await;

        // Assertions
        assert_eq!(result.unwrap_err(), P2pError::IncompleteMessage);
    }
}
