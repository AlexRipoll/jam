use serde::{Deserialize, Serialize};
use serde_bencode::de;
use serde_bytes::ByteBuf;
use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
};

#[derive(Serialize, Deserialize, Debug)]
pub struct Request {
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    port: u16,
    uploaded: u64,
    downloaded: u64,
    left: u64,
    compact: u8,
    no_peer_id: u8,
    event: Option<Event>,
    ip: Option<IpAddr>,
    numwant: Option<u32>,
    key: Option<String>,
    trackerid: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Event {
    Started,
    Stopped,
    Completed,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct Response {
    failure_response: Option<String>,
    warning_message: Option<String>,
    interval: Option<u16>,
    min_interval: Option<u16>,
    tracker_id: Option<String>,
    complete: Option<u32>,
    incomplete: Option<u32>,
    // TODO: deocde both Peers variants
    // peers: Option<Peers>,
    peers: Option<ByteBuf>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum Peers {
    Binary(Vec<u8>),
    Dictionary(Vec<Peer>),
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct Peer {
    peer_id: String,
    ip: Ip,
    port: u32,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum Ip {
    IpV4(Ipv4Addr),
    IpV6(Ipv6Addr),
    Dns(String),
}

pub async fn get(url: &str) -> Result<Response, Box<dyn std::error::Error>> {
    let response = reqwest::get(url).await?;
    let bencoded_body = response.bytes().await?;
    let response_parsed = de::from_bytes::<Response>(bencoded_body.as_ref())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    Ok(response_parsed)
}

#[cfg(test)]
mod test {
    use serde_bencode::de;

    use super::Response;

    #[test]
    fn test_bencode_reponse_decode() {
        let bencoded_body =
            b"d8:completei990e10:incompletei63e8:intervali1800e5:peers6:\xb9}\xbe;\x1b\x14e";

        let response = de::from_bytes::<Response>(bencoded_body.as_ref()).unwrap();

        let expected_response = Response {
            failure_response: None,
            warning_message: None,
            interval: Some(1800),
            min_interval: None,
            tracker_id: None,
            complete: Some(990),
            incomplete: Some(63),
            // peers: Some(Peers::Binary(vec![0xb9, 0xbe, 0x1b, 0x14])),
            peers: Some(vec![185, 125, 190, 59, 27, 20].into()),
        };

        assert_eq!(expected_response, response);
    }
}
