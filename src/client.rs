use rand::{distributions::Alphanumeric, Rng};
use std::usize;

use tokio::{
    io::{self, AsyncWriteExt},
    net::TcpStream,
};

use crate::message::{self, Bitfield, Message, MessageId, Piece, Transfer};

const PSTR: &str = "BitTorrent protocol";

pub struct Client {
    stream: TcpStream,
    is_choked: bool,
    is_interested: bool,
    peer_chocked: bool,
    peer_interested: bool,
    peer_bitfield: Bitfield,
}

#[derive(Debug, PartialEq, Eq)]
enum State {
    Choked,
    Interested,
}

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

    pub fn deserialize(buffer: Vec<u8>) -> Result<Handshake, &'static str> {
        let mut offset = 0;

        // Parse `pstr_length` (1 byte)
        let pstr_length = buffer[offset];
        offset += 1;

        // Check if `pstr_length` matches expected length for "BitTorrent protocol"
        if pstr_length as usize != PSTR.len() {
            return Err("pstr length mismatch");
        }

        // Parse `pstr` (19 bytes)
        let pstr = match std::str::from_utf8(&buffer[offset..offset + pstr_length as usize]) {
            Ok(s) => s.to_string(),
            Err(_) => return Err("Invalid UTF-8 in pstr"),
        };
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

impl Client {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            is_choked: true,
            is_interested: false,
            peer_chocked: true,
            peer_interested: false,
            peer_bitfield: Bitfield::new(&vec![]),
        }
    }

    pub async fn handshake(
        &mut self,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
    ) -> io::Result<Vec<u8>> {
        let handshake = Handshake::new(info_hash, peer_id);
        self.stream.write_all(&handshake.serialize()).await?;

        let mut buffer = vec![0u8; 68];
        self.stream.readable().await?;

        let mut bytes_read = 0;
        // Loop until we've read exactly 68 bytes
        while bytes_read < 68 {
            match self.stream.try_read(&mut buffer[bytes_read..]) {
                Ok(0) => break, // Connection closed
                Ok(n) => bytes_read += n,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.stream.readable().await?;
                }
                Err(e) => return Err(e.into()),
            }
        }

        if bytes_read != 68 {
            eprintln!("Warning: Incomplete handshake response received");
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Handshake response was not 68 bytes",
            ));
        }

        Ok(buffer)
    }

    pub async fn send_message(
        &mut self,
        message_id: MessageId,
        payload: Option<Vec<u8>>,
    ) -> io::Result<()> {
        let message = Message::new(message_id, payload);
        self.stream.write_all(&message.serialize()).await?;

        Ok(())
    }

    pub async fn read_message(&mut self) -> io::Result<Message> {
        self.stream.readable().await?;

        //  Read the 4-byte length field
        let mut length_buffer = [0u8; 4];
        let mut bytes_read = 0;
        while bytes_read < 4 {
            match self.stream.try_read(&mut length_buffer[bytes_read..]) {
                Ok(0) => {
                    break;
                } // Connection closed
                Ok(n) => bytes_read += n,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.stream.readable().await?;
                }
                Err(e) => return Err(e.into()),
            }
        }

        // Convert the length bytes from Big-Endian to usize
        let message_length = u32::from_be_bytes(length_buffer) as usize;

        //  Allocate a buffer for the message based on the length
        let mut message_buffer = vec![0u8; 4 + message_length];
        message_buffer[..4].copy_from_slice(&length_buffer);

        bytes_read = 0;

        //  Read the actual message data into the buffer
        while bytes_read < message_length {
            match self.stream.try_read(&mut message_buffer[4 + bytes_read..]) {
                Ok(0) => {
                    break;
                } // Connection closed
                Ok(n) => bytes_read += n,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.stream.readable().await?;
                }
                Err(e) => return Err(e.into()),
            }
        }

        let message = Message::deserialize(&message_buffer)?;

        self.process_message(&message).await?;

        Ok(message)
    }

    pub async fn process_message(&mut self, message: &Message) -> io::Result<()> {
        match message.message_id {
            MessageId::KeepAlive => {} // TODO: extend stream alive
            MessageId::Choke => {
                self.is_choked = true;
                // TODO:  stop sending msg until unchoked
            }
            MessageId::Unchoke => {
                self.is_choked = false;
                // TODO:start requesting
            }
            MessageId::Interested => {
                self.peer_interested = true;
                // TODO: logic for unchoking peer
                if self.peer_chocked {
                    self.peer_chocked = false;
                    self.send_message(MessageId::Unchoke, None).await?; // Notifying unchoked
                }
            }
            MessageId::NotInterested => {
                self.peer_interested = false;
            }
            MessageId::Have => self.process_have(message),
            MessageId::Bitfield => {
                self.peer_bitfield = Bitfield::new(message.payload.as_ref().unwrap());

                // TODO: check if peer has any piece the client is interested in downloading then
                // request to unchoke and notify interest
                self.send_message(MessageId::Unchoke, None).await?;
                self.send_message(MessageId::Interested, None).await?;
            }
            MessageId::Request => {
                // send piece block to peer
                todo!()
            }
            MessageId::Piece => {
                // store piece block
                todo!()
            }
            MessageId::Cancel => {
                todo!()
            }
            MessageId::Port => {
                todo!()
            }
        }

        Ok(())
    }

    fn process_have(&mut self, message: &Message) {
        let mut piece_index_be = [0u8; 4];
        piece_index_be.copy_from_slice(message.payload.as_ref().unwrap());
        let piece_index = u32::from_be_bytes(piece_index_be) as usize;
        if !self.peer_bitfield.has_piece(piece_index) {
            self.peer_bitfield.set_piece(piece_index);
        }
        // TODO: check if interested
    }
}

fn client_version() -> String {
    let version_tag = env!("CARGO_PKG_VERSION").replace(".", "");
    let version = version_tag
        .chars()
        .filter(|c| c.is_digit(10))
        .take(4)
        .collect::<String>();

    // Ensure exactly 4 characters, padding with "0" if necessary
    format!("{:0<4}", version)
}

pub fn generate_peer_id() -> [u8; 20] {
    let version = client_version();
    let client_id = "JM";

    // Generate a 12-character random alphanumeric sequence
    let random_seq: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(12)
        .map(char::from)
        .collect();

    let format = format!("-{}{}-{}", client_id, version, random_seq);

    let mut id = [0u8; 20];
    id.copy_from_slice(format.as_bytes());

    id
}

#[cfg(test)]
mod test {
    use crate::client::{client_version, generate_peer_id};

    #[test]
    fn test_generate_peer_id() {
        let version = client_version();
        let expected = format!("-JM{}-", version);

        assert_eq!(&generate_peer_id()[..8], expected.as_bytes());
    }
}
