use std::{
    collections::HashMap,
    fs::File,
    io::{self, Read},
};

use client::Client;
use config::Config;
use metainfo::Metainfo;
use p2p::{message_handler::generate_peer_id, piece::Piece};
use tracing::{debug, error, info, trace, warn, Level};
use tracing_subscriber::FmtSubscriber;
use tracker::get;

pub mod bitfield;
pub mod client;
pub mod config;
pub mod download_state;
pub mod metainfo;
mod p2p;
pub mod session;
pub mod store;
pub mod tracker;

#[tokio::main]
async fn main() -> io::Result<()> {
    // Initialize tracing
    let builder = FmtSubscriber::builder();
    let subscriber = builder
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(Level::INFO)
        .with_thread_names(true)
        .with_target(true)
        // completes the builder.
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    info!(" Starting BitTorrent client...");

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
    let file_path = "debian-12.7.0-amd64-netinst.iso.torrent";
    info!("Opening torrent file: {}", file_path);
    let mut file = File::open(file_path).map_err(|e| {
        warn!("Failed to open torrent file: {}", e);
        e
    })?;
    let mut buffer = Vec::new();

    trace!("Reading torrent file into buffer...");
    file.read_to_end(&mut buffer)?;

    debug!("Computing info hash...");
    let info_hash = Metainfo::compute_info_hash(&buffer)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    debug!(info_hash = ?hex::encode(info_hash), "Info hash computed");

    trace!("Deserializing matainfo file");
    let metainfo = Metainfo::deserialize(&buffer).map_err(|e| {
        warn!("Failed to deserialize metainfo: {}", e);
        io::Error::new(io::ErrorKind::Other, e)
    })?;
    debug!(torrent_name = ?metainfo.info.name, "Successfully deserialized metainfo");

    let peer_id = generate_peer_id();
    debug!(peer_id = ?String::from_utf8_lossy(&peer_id), "Generated session peer ID");

    trace!("Building tracker URL...");
    let url = metainfo
        .build_tracker_url(info_hash, peer_id, 6889)
        .map_err(|e| {
            warn!("Failed to build tracker URL: {}", e);
            io::Error::new(io::ErrorKind::Other, e)
        })?;
    debug!(tracker_url = ?url, "Tracker URL built");

    info!("Querying tracker...");
    let response = get(&url).await.map_err(|e| {
        warn!("Failed to query tracker: {}", e);
        io::Error::new(io::ErrorKind::Other, format!("Tracker query failed: {e}"))
    })?;

    debug!("Decoding peer list...");
    let peers = response.decode_peers().map_err(|e| {
        warn!("Failed to decode peer list: {}", e);
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Failed to decode peers: {e}"),
        )
    })?;
    info!(peer_count = peers.len(), "Successfully decoded peer list");

    trace!("Creating piece map...");
    let mut pieces = HashMap::new();

    for (index, sha1) in metainfo.info.pieces.chunks(20).enumerate() {
        let sha1: [u8; 20] = sha1.try_into().expect("Invalid piece length");
        let piece = Piece::new(index as u32, metainfo.info.piece_length as usize, sha1);
        pieces.insert(index as u32, piece);
    }

    trace!("Loading config...");
    let config = Config::load().unwrap();
    let download_path = config.disk.download_path;
    let timeout_duration = config.p2p.timeout_duration;
    let max_peer_connections = config.p2p.max_peer_connections;
    let piece_standard_size = config.p2p.piece_standard_size;
    let connection_retries = config.p2p.connection_retries;

    info!("Initializing client...");
    let client = Client::new(
        download_path,
        metainfo.info.name,
        metainfo.info.length.unwrap(),
        piece_standard_size,
        peer_id,
        peers,
        max_peer_connections,
        pieces,
        timeout_duration,
        connection_retries,
    );
    debug!("Client initialized");

    info!("Starting download...");
    if let Err(e) = client.run(info_hash).await {
        error!("Error during client run: {}", e);
        return Err(e);
    }

    info!("Download completed successfully");

    Ok(())
}
