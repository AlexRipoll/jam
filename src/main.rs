use std::{
    fs::File,
    io::{self, Read},
    time::Instant,
};

use config::Config;
use torrent::{
    metainfo::Metainfo,
    torrent::{generate_peer_id, Metadata, Torrent},
    tracker,
};
use tracing::{debug, error, info, trace, warn, Level};
use tracing_appender::rolling;
use tracing_subscriber::{
    fmt::{self, format::FmtSpan},
    layer::SubscriberExt,
    EnvFilter, Registry,
};

pub mod config;
pub mod torrent;

#[tokio::main]
async fn main() -> io::Result<()> {
    // File appender: rolling logs daily to "logs/app.log".
    let file_appender = rolling::daily("logs", "app.log");

    // Logger for terminal output (with colors).
    let terminal_layer = fmt::layer()
        .with_thread_names(true)
        .with_target(true)
        .with_span_events(FmtSpan::NONE)
        .with_ansi(true); // Enable ANSI for terminal

    // Logger for file output (no colors or ANSI escape codes).
    let file_layer = fmt::layer()
        .with_writer(file_appender)
        .with_thread_names(true)
        .with_target(true)
        .with_span_events(FmtSpan::NONE)
        .with_ansi(false); // Disable ANSI for file output

    // Combine the layers and apply the subscriber.
    let subscriber = Registry::default()
        .with(EnvFilter::from_default_env().add_directive(Level::DEBUG.into()))
        .with(terminal_layer)
        .with(file_layer);
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    info!(" Starting BitTorrent client...");
    let start = Instant::now();

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
    debug!("Opening torrent file: {}", file_path);
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

    // trace!("Building tracker URL...");
    // let url = metainfo
    //     .build_tracker_url(info_hash, peer_id, 6889)
    //     .map_err(|e| {
    //         warn!("Failed to build tracker URL: {}", e);
    //         io::Error::new(io::ErrorKind::Other, e)
    //     })?;
    // debug!(tracker_url = ?url, "Tracker URL built");
    //
    // debug!("Querying tracker...");
    // let response = tracker::get(&url).await.map_err(|e| {
    //     warn!("Failed to query tracker: {}", e);
    //     io::Error::new(io::ErrorKind::Other, format!("Tracker query failed: {e}"))
    // })?;

    // debug!("Decoding peer list...");
    // let peers = response.decode_peers().map_err(|e| {
    //     warn!("Failed to decode peer list: {}", e);
    //     io::Error::new(
    //         io::ErrorKind::InvalidData,
    //         format!("Failed to decode peers: {e}"),
    //     )
    // })?;
    // debug!(peer_count = peers.len(), "Successfully decoded peer list");
    //
    // trace!("Creating piece map...");
    // let pieces = metainfo.parse_pieces().unwrap();
    //
    // trace!("Loading config...");
    // let config = Config::load().unwrap();
    //
    // let torrent = Torrent::new(&buffer);
    //
    // info!("Initializing client...");
    // let client = Client::new(config, torrent, peers, peer_id);
    //
    // info!("Starting download...");
    // if let Err(e) = client.run().await {
    //     error!("Error during client run: {}", e);
    //     return Err(e);
    // }
    //
    // info!("Download completed successfully");
    //
    let duration = start.elapsed();
    println!("Time elapsed: {:.2?}", duration);

    Ok(())
}
