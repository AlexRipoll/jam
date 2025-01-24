use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Display},
    net::{Ipv4Addr, Ipv6Addr},
};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Peers {
    Binary(Vec<u8>),
    Dictionary(Vec<Peer>),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Peer {
    pub peer_id: Option<String>,
    pub ip: Ip,
    pub port: u16,
}

impl Peer {
    pub fn address(&self) -> String {
        format!("{}:{}", &self.ip, &self.port)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum Ip {
    IpV4(Ipv4Addr),
    IpV6(Ipv6Addr),
    Dns(String),
}

impl Display for Ip {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Ip::IpV4(ipv4) => write!(f, "{}", ipv4),
            Ip::IpV6(ipv6) => write!(f, "{}", ipv6),
            Ip::Dns(dns) => write!(f, "{}", dns),
        }
    }
}
