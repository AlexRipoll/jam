use std::io::{self};

use config::Config;
use repl::run_repl;
use torrent::torrent::TorrentManager;
use tracing::Level;
use tracing_appender::rolling;
use tracing_subscriber::{
    fmt::{self, format::FmtSpan},
    layer::SubscriberExt,
    EnvFilter, Registry,
};

mod config;
mod core;
mod events;
mod peer;
mod repl;
mod torrent;
mod tracker;
mod ui;

#[tokio::main]
async fn main() -> io::Result<()> {
    // File appender: rolling logs daily to "logs/app.log".
    let file_appender = rolling::daily("logs", "app.log");

    // Logger for terminal output (with colors).
    // let terminal_layer = fmt::layer()
    //     .with_thread_names(true)
    //     .with_target(true)
    //     .with_span_events(FmtSpan::NONE)
    //     .with_ansi(true); // Enable ANSI for terminal

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
        // .with(terminal_layer)
        .with(file_layer);
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

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

    let config = Config::load().unwrap();

    // Create torrent manager
    let manager = TorrentManager::new(&config);

    // Start the REPL
    run_repl(manager).await?;

    Ok(())
}
