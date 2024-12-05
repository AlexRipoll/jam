use std::{
    collections::HashMap,
    fs::File,
    io::{self, Read},
};

use client::Client;
use metainfo::Metainfo;
use p2p::{connection::generate_peer_id, piece::Piece};
use tracker::get;

pub mod client;
pub mod metainfo;
mod p2p;
pub mod store;
pub mod tracker;

#[tokio::main]
async fn main() -> io::Result<()> {
    // Open the torrent file
    // Torrent file options:
    // With announce:
    // - ubuntu-24.10-desktop-amd64.iso.torrent (1 peer)
    // - debian-12.7.0-amd64-netinst.iso.torrent (namy  peers)
    // (udp)
    // - linuxmint-22-cinnamon-64bit.iso.torrent
    // - sintel.torrent
    // - cosmos-laundromat.torrent
    //
    // With DHT or PEX:
    // - archlinux-2024.09.01-x86_64.iso.torrent
    let mut file = File::open("debian-12.7.0-amd64-netinst.iso.torrent")?;
    let mut buffer = Vec::new();

    // Read the entire file into buffer
    file.read_to_end(&mut buffer)?;

    let info_hash = Metainfo::compute_info_hash(&buffer)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    // Deserialize the buffer into a Metainfo struct
    let metainfo =
        Metainfo::deserialize(&buffer).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    println!("->> Metainfo: {:#?}", metainfo);

    let peer_id = generate_peer_id();

    let url = metainfo
        .build_tracker_url(info_hash, peer_id, 6889)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    println!("->> URL: {:?}", url);

    let response = get(&url).await.unwrap();
    println!("->> Tracker Response: {:#?}", response);

    let peers = response.decode_peers().unwrap();

    println!("->> Peers: {:#?}", peers);

    // config data
    let download_path = "./downloads".to_string();
    let piece_standard_size = 16384;
    let total_peers: usize = 2;
    let timeout_duration = 3;
    let connection_retries = 3;

    let mut pieces = HashMap::new();

    for (index, sha1) in metainfo.info.pieces.chunks(20).enumerate() {
        let sha1: [u8; 20] = sha1.try_into().expect("Invalid piece length");
        let piece = Piece::new(index as u32, metainfo.info.piece_length as usize, sha1);
        pieces.insert(index as u32, piece);
    }

    let client = Client::new(
        download_path,
        metainfo.info.name,
        metainfo.info.length.unwrap(),
        piece_standard_size,
        peer_id,
        peers,
        total_peers,
        pieces,
        timeout_duration,
        connection_retries,
    );

    client.run(info_hash).await;

    Ok(())
}
