use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use super::{connection::P2pError, message::Message};

pub async fn read_message(
    read_half: &mut tokio::io::ReadHalf<TcpStream>,
) -> Result<Message, P2pError> {
    let mut length_buffer = [0u8; 4];
    let mut bytes_read = 0;

    // Read the 4-byte length field
    while bytes_read < 4 {
        match read_half.read(&mut length_buffer[bytes_read..]).await {
            Ok(0) => {
                break;
            } // Connection closed
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
                break;
            } // Connection closed
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
