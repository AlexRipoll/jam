use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

#[derive(Debug)]
struct Request {
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

#[derive(Debug)]
enum Event {
    Started,
    Stopped,
    Completed,
}

#[derive(Debug)]
struct Response {
    failure_response: Option<String>,
    warning_message: Option<String>,
    interval: Option<u16>,
    min_interval: Option<u16>,
    tracker_id: Option<String>,
    complete: Option<u32>,
    incomplete: Option<u32>,
    peers: Option<Peers>,
}

#[derive(Debug)]
enum Peers {
    Dictionary(Vec<Peer>),
    Binary(Vec<u8>),
}

#[derive(Debug)]
struct Peer {
    peer_id: String,
    ip: Ip,
    port: u32,
}

#[derive(Debug)]
enum Ip {
    IpV4(Ipv4Addr),
    IpV6(Ipv6Addr),
    Dns(String),
}
