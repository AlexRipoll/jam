use client::{generate_peer_id, peer_handshake, Handshake};
use metainfo::Metainfo;
use std::{
    fs::File,
    io::{self, Read},
};
use tracker::get;

pub mod client;
pub mod message;
pub mod metainfo;
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
    let mut file = File::open("ubuntu-24.10-desktop-amd64.iso.torrent")?;
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
    // let bytes = peer_id.as_bytes();
    // let mut peer_id_bytes = [0u8; 20];
    // peer_id_bytes.copy_from_slice(bytes);

    println!("->> Peers: {:#?}", peers);

    let res = peer_handshake(&peers[0].address(), info_hash, peer_id).await?;
    println!("RES BYTES: {:?} :: {}", &res, res.len());
    println!("RES: {:?}", String::from_utf8_lossy(&res),);
    let handshake_res = Handshake::deserialize(res).unwrap();
    println!("{:#?}", handshake_res);
    println!("{:#?}", handshake_res.pstr);
    println!("{:#?}", handshake_res.reserved);
    println!("{:#?}", handshake_res.info_hash);
    println!("{:#?}", String::from_utf8(handshake_res.peer_id.to_vec()));

    Ok(())
}
