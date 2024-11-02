use metainfo::Metainfo;
use std::{
    fs::File,
    io::{self, Read},
};
use tracker::get;

pub mod metainfo;
pub mod tracker;

#[tokio::main]
async fn main() -> io::Result<()> {
    // Open the torrent file
    // Torrent file options:
    // With announce:
    // - ubuntu-24.10-desktop-amd64.iso.torrent
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
    let metainfo = Metainfo::deserialize(&buffer)?;

    let url = metainfo
        .build_tracker_url(info_hash, 6889)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    println!("=>> URL: {:?}", url);

    let response = get(&url).await.unwrap();
    println!("{:#?}", response);

    Ok(())
}
