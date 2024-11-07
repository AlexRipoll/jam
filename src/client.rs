use std::usize;

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

    pub fn deserialize(buffer: Vec<u8>) -> Result<Handshake, &'static str> {
        let mut offset = 0;

        // 1. Parse `pstr_length` (1 byte)
        let pstr_length = buffer[offset];
        offset += 1;

        // Check if `pstr_length` matches expected length for "BitTorrent protocol"
        if pstr_length as usize != PSTR.len() {
            return Err("pstr length mismatch");
        }

        // 2. Parse `pstr` (19 bytes)
        let pstr = match std::str::from_utf8(&buffer[offset..offset + pstr_length as usize]) {
            Ok(s) => s.to_string(),
            Err(_) => return Err("Invalid UTF-8 in pstr"),
        };
        offset += pstr_length as usize;

        // 3. Parse `reserved` (8 bytes)
        let mut reserved = [0u8; 8];
        reserved.copy_from_slice(&buffer[offset..offset + 8]);
        offset += 8;

        // 4. Parse `info_hash` (20 bytes)
        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&buffer[offset..offset + 20]);
        offset += 20;

        // 5. Parse `peer_id` (20 bytes)
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

pub async fn peer_handshake(
    peer_address: &str,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
) -> io::Result<Vec<u8>> {
    let handshake = Handshake::new(info_hash, peer_id);

    let mut stream = TcpStream::connect(peer_address).await?;
    stream.write_all(&handshake.serialize()).await?;

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
        eprintln!("Warning: Incomplete handshake response received");
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Handshake response was not 68 bytes",
        ));
    }

    Ok(buffer)
}
