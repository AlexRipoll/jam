use std::{
    io::Cursor,
    net::{IpAddr, Ipv4Addr},
};

use rand::Rng;
use tokio::io::AsyncReadExt;

use super::tracker::{Request, Response, TrackerError};

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
struct Udp {
    connection_id: u64,
}

impl Udp {
    fn connect(tx_id: u32) -> [u8; 16] {
        let mut buf = [0u8; 16];
        write_to_buffer(&mut buf, 0, UDP_PROTOCOL_ID);
        write_to_buffer(&mut buf, 8, Action::Connect as u32);
        write_to_buffer(&mut buf, 12, tx_id);

        buf
    }

    async fn process_connect(buf: &[u8], tx_id: u32) -> Result<u64, TrackerError> {
        if buf.len() < 16 {
            return Err(TrackerError::InvalidPacketSize);
        }
        let mut response = Cursor::new(&buf[4..]);
        let received_tx_id = response.read_u32().await.unwrap();
        let connection_id = response.read_u64().await.unwrap();

        if received_tx_id != tx_id {
            return Err(TrackerError::InvalidTxId);
        }

        Ok(connection_id)
    }

    fn announce(&self, req: Request, tx_id: u32) -> [u8; 98] {
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

    async fn process_announce(buf: &[u8], tx_id: u32) -> Result<Response, TrackerError> {
        if buf.len() < 16 {
            return Err(TrackerError::InvalidPacketSize);
        }

        let mut tracker_response = Response::empty();
        let mut response = Cursor::new(&buf[4..]);
        let received_tx_id = response.read_u32().await.unwrap();

        if received_tx_id != tx_id {
            return Err(TrackerError::InvalidTxId);
        }

        tracker_response.interval = Some(response.read_u32().await.unwrap());
        tracker_response.incomplete = Some(response.read_u32().await.unwrap());
        tracker_response.complete = Some(response.read_u32().await.unwrap());

        Ok(tracker_response)
    }

    async fn process_error(buf: &[u8], tx_id: u32) -> Result<Response, TrackerError> {
        let mut tracker_response = Response::empty();
        let mut response = Cursor::new(buf);
        let received_tx_id = response.read_u32().await.unwrap();

        if received_tx_id != tx_id {
            return Err(TrackerError::InvalidTxId);
        }

        let message =
            String::from_utf8(buf[8..].to_vec()).map_err(|_| TrackerError::InvalidUTF8)?;
        tracker_response.failure_response = Some(message);

        Ok(tracker_response)
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
