use std::{
    io::Cursor,
    net::{IpAddr, Ipv4Addr},
    time::Duration,
};

use rand::Rng;
use tokio::{
    io::AsyncReadExt,
    net::{lookup_host, UdpSocket},
    time::timeout,
};

use crate::torrent::peer::{Ip, Peer};

use super::tracker::{Announce, TrackerError, TrackerProtocol};

const UDP_PROTOCOL_ID: u64 = 0x41727101980;

enum Action {
    Connect = 0,
    Announce = 1,
    Scrape = 2,
    Error = 3,
}

impl Action {
    /// Reads the first 4 bytes of a buffer and returns the corresponding Action.
    /// If the value does not match any known action, it returns an `Error`.
    fn from_bytes(msg: &[u8]) -> Result<Action, TrackerError> {
        if msg.len() < 4 {
            return Err(TrackerError::InvalidPacketSize);
        }

        let value = u32::from_be_bytes(msg[..4].try_into().unwrap());

        match value {
            0 => Ok(Action::Connect),
            1 => Ok(Action::Announce),
            2 => Ok(Action::Scrape),
            3 => Ok(Action::Error),
            _ => Err(TrackerError::UnknownAction),
        }
    }
}

#[derive(Debug)]
pub struct UdpTracker {
    connection_id: u64,
}

impl UdpTracker {
    pub fn new() -> UdpTracker {
        UdpTracker {
            connection_id: UDP_PROTOCOL_ID,
        }
    }

    fn build_connect_packet(&self, tx_id: u32) -> [u8; 16] {
        let mut buf = [0u8; 16];
        write_to_buffer(&mut buf, 0, UDP_PROTOCOL_ID);
        write_to_buffer(&mut buf, 8, Action::Connect as u32);
        write_to_buffer(&mut buf, 12, tx_id);

        buf
    }

    fn build_announce_packet(&self, req: &Announce, tx_id: u32) -> [u8; 98] {
        let mut buf = [0u8; 98];
        write_to_buffer(&mut buf, 0, self.connection_id);
        write_to_buffer(&mut buf, 8, Action::Announce as u32);
        write_to_buffer(&mut buf, 12, tx_id);
        write_to_buffer(&mut buf, 16, req.info_hash.as_ref());
        write_to_buffer(&mut buf, 36, req.peer_id.as_ref());
        write_to_buffer(&mut buf, 56, req.downloaded);
        write_to_buffer(&mut buf, 64, req.left);
        write_to_buffer(&mut buf, 72, req.uploaded);
        write_to_buffer(
            &mut buf,
            80,
            req.event.unwrap_or(super::tracker::Event::None) as u32,
        );
        let ip_addr = req
            .ip
            .unwrap_or(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)))
            .to_string();
        write_to_buffer(&mut buf, 84, ip_addr.parse::<u32>().unwrap());
        write_to_buffer(
            &mut buf,
            88,
            req.key.unwrap_or_else(|| rand::thread_rng().gen()),
        );
        write_to_buffer(&mut buf, 92, req.num_want.unwrap_or(-1));
        write_to_buffer(&mut buf, 96, req.port);

        buf
    }

    async fn receive_response(
        &self,
        socket: &UdpSocket,
        expected_tx_id: u32,
    ) -> Result<TrackerResponse, TrackerError> {
        let mut buf = [0; 1024];
        let timeout_duration = Duration::from_secs(5);

        let result = timeout(timeout_duration, socket.recv_from(&mut buf)).await;
        match result {
            Ok(Ok((size, _))) => {
                let resp = TrackerResponse::from_bencoded(&buf[..size]).await?;
                if let TrackerResponse::Error {
                    transaction_id,
                    message,
                } = resp
                {
                    if transaction_id != expected_tx_id {
                        return Err(TrackerError::InvalidTransactionId);
                    }

                    return Err(TrackerError::ErrorResponse(message));
                }

                Ok(resp)
            }
            Ok(Err(e)) => Err(TrackerError::UdpSocketError(e)),
            Err(_) => Err(TrackerError::ResponseTimeout),
        }
    }
}

impl TrackerProtocol for UdpTracker {
    async fn get_peers(
        &mut self,
        announce: &str,
        announce_data: &Announce,
    ) -> Result<Vec<Peer>, TrackerError> {
        // Bind the socket to any available ip address
        let socket = UdpSocket::bind("0.0.0.0:0")
            .await
            .map_err(|e| TrackerError::UdpBindingError(e))?;

        let tracker_addresses = lookup_host(announce)
            .await
            .map_err(|e| TrackerError::IoError(e))?;

        // Connect to one of the tracker's ip addresses
        let mut connected = false;
        for ip_addr in tracker_addresses {
            if socket.connect(ip_addr).await.is_ok() {
                connected = true;
                break;
            }
        }
        if !connected {
            return Err(TrackerError::ConnectionFailed);
        }

        // Generate a random transaction ID for connect request
        let tx_id = rand::thread_rng().gen::<u32>();

        // Send the connect packet to the tracker
        let connect_packet = self.build_connect_packet(tx_id);
        socket
            .send(&connect_packet)
            .await
            .map_err(|e| TrackerError::IoError(e))?;

        // Receive and parse the connection response
        let connect_resp = self.receive_response(&socket, tx_id).await?;
        match connect_resp {
            TrackerResponse::Connect {
                transaction_id,
                connection_id,
            } => {
                if transaction_id != tx_id {
                    return Err(TrackerError::InvalidTransactionId);
                }
                self.connection_id = connection_id;
            }
            _ => return Err(TrackerError::UnexpectedAction),
        }

        // Generate a random transaction ID for announce request
        let tx_id = rand::thread_rng().gen::<u32>();

        // Send the announce packet to the tracker
        let announce_packet = self.build_announce_packet(announce_data, tx_id);
        socket
            .send(&announce_packet)
            .await
            .map_err(|e| TrackerError::IoError(e))?;

        // Receive and parse the announce response
        let announce_resp = self.receive_response(&socket, tx_id).await?;
        match announce_resp {
            TrackerResponse::Announce { transaction_id, .. } => {
                if transaction_id != tx_id {
                    return Err(TrackerError::InvalidTransactionId);
                }

                // Decode and return the list of peers
                return announce_resp.decode_peers();
            }
            _ => {
                return Err(TrackerError::UnexpectedAction);
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum TrackerResponse {
    Connect {
        transaction_id: u32,
        connection_id: u64,
    },
    Announce {
        transaction_id: u32,
        interval: u32,
        leechers: u32,
        seeders: u32,
        ip_address: Vec<u32>,
        tcp_port: Vec<u16>,
    },
    Error {
        transaction_id: u32,
        message: String,
    },
}

impl TrackerResponse {
    pub async fn from_bencoded(buf: &[u8]) -> Result<TrackerResponse, TrackerError> {
        let action = Action::from_bytes(&buf)?;

        match action {
            Action::Connect => TrackerResponse::decode_connect_packet(buf).await,
            Action::Announce => TrackerResponse::decode_announce_packet(buf).await,
            Action::Scrape => {
                unimplemented!()
            }
            Action::Error => TrackerResponse::decode_error_packet(buf).await,
        }
    }

    async fn decode_connect_packet(buf: &[u8]) -> Result<TrackerResponse, TrackerError> {
        if buf.len() < 16 {
            return Err(TrackerError::InvalidPacketSize);
        }
        let mut response = Cursor::new(&buf[4..]);
        let transaction_id = response.read_u32().await.unwrap();
        let connection_id = response.read_u64().await.unwrap();

        Ok(TrackerResponse::Connect {
            transaction_id,
            connection_id,
        })
    }

    async fn decode_announce_packet(buf: &[u8]) -> Result<TrackerResponse, TrackerError> {
        if buf.len() < 20 {
            return Err(TrackerError::InvalidPacketSize);
        }

        let mut cursor = Cursor::new(&buf[4..]);
        let transaction_id = cursor.read_u32().await.unwrap();

        let interval = cursor.read_u32().await.unwrap();
        let leechers = cursor.read_u32().await.unwrap();
        let seeders = cursor.read_u32().await.unwrap();

        let mut ip_address = Vec::new();
        let mut tcp_port = Vec::new();

        while (cursor.position() as usize) + 6 <= buf.len() {
            // Read IPv4 address (4 bytes)
            let ip = cursor.read_u32().await.unwrap();
            ip_address.push(ip);

            // Read TCP port (2 bytes)
            let port = cursor.read_u16().await.unwrap();
            tcp_port.push(port);
        }

        Ok(TrackerResponse::Announce {
            transaction_id,
            interval,
            leechers,
            seeders,
            ip_address,
            tcp_port,
        })
    }

    async fn decode_error_packet(buf: &[u8]) -> Result<TrackerResponse, TrackerError> {
        let mut cursor = Cursor::new(&buf[4..]);
        let transaction_id = cursor.read_u32().await.unwrap();

        let message =
            String::from_utf8(buf[8..].to_vec()).map_err(|_| TrackerError::InvalidUTF8)?;

        Ok(TrackerResponse::Error {
            transaction_id,
            message,
        })
    }

    pub fn decode_peers(&self) -> Result<Vec<Peer>, TrackerError> {
        match &self {
            TrackerResponse::Announce {
                ip_address,
                tcp_port,
                ..
            } => {
                if !ip_address.is_empty() && !tcp_port.is_empty() {
                    if ip_address.len() != tcp_port.len() {
                        return Err(TrackerError::InvalidPeersFormat);
                    }

                    let peers: Vec<Peer> = ip_address
                        .iter()
                        .zip(tcp_port.iter())
                        .map(|(&ip_addr, &port)| Peer {
                            peer_id: None,
                            ip: Ip::IpV4(Ipv4Addr::from(ip_addr)),
                            port,
                        })
                        .collect();

                    return Ok(peers);
                }
            }
            _ => {}
        }

        Err(TrackerError::EmptyPeers)
    }
}

fn write_to_buffer<'a, T>(buf: &mut [u8], offset: usize, value: T)
where
    T: Into<Writeable<'a>>,
{
    match value.into() {
        Writeable::U64(v) => buf[offset..offset + 8].copy_from_slice(&v.to_be_bytes()),
        Writeable::U32(v) => buf[offset..offset + 4].copy_from_slice(&v.to_be_bytes()),
        Writeable::U16(v) => buf[offset..offset + 2].copy_from_slice(&v.to_be_bytes()),
        Writeable::I32(v) => buf[offset..offset + 4].copy_from_slice(&v.to_be_bytes()),
        Writeable::Bytes(b) => {
            let end = offset + b.len();
            if end > buf.len() {
                panic!("Byte slice too large for buffer at offset {offset}");
            }
            buf[offset..end].copy_from_slice(b);
        }
        Writeable::Str(s) => {
            let bytes = s.as_bytes();
            let end = offset + bytes.len();
            if end > buf.len() {
                panic!("String too large for buffer at offset {offset}");
            }
            buf[offset..end].copy_from_slice(bytes);
        }
    }
}

enum Writeable<'a> {
    U64(u64),
    U32(u32),
    U16(u16),
    I32(i32),
    Bytes(&'a [u8]),
    Str(&'a str),
}

impl From<u64> for Writeable<'_> {
    fn from(value: u64) -> Self {
        Writeable::U64(value)
    }
}

impl From<u32> for Writeable<'_> {
    fn from(value: u32) -> Self {
        Writeable::U32(value)
    }
}

impl From<u16> for Writeable<'_> {
    fn from(value: u16) -> Self {
        Writeable::U16(value)
    }
}

impl From<i32> for Writeable<'_> {
    fn from(value: i32) -> Self {
        Writeable::I32(value)
    }
}

impl<'a> From<&'a [u8]> for Writeable<'a> {
    fn from(value: &'a [u8]) -> Self {
        Writeable::Bytes(value)
    }
}

impl<'a> From<&'a str> for Writeable<'a> {
    fn from(value: &'a str) -> Self {
        Writeable::Str(value)
    }
}
