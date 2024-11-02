use metainfo::Metainfo;
use serde_bencode::de;
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
    // archlinux-2024.09.01-x86_64.iso.torrent
    let mut file = File::open("ubuntu-24.10-desktop-amd64.iso.torrent")?;
    let mut buffer = Vec::new();

    // Read the entire file into buffer
    file.read_to_end(&mut buffer)?;

    let info_hash = Metainfo::compute_info_hash(&buffer);

    // Deserialize the buffer into a Metainfo struct
    let metainfo =
        de::from_bytes::<Metainfo>(&buffer).expect("Failed to deserialize torrent file: {}");

    let url = metainfo.build_tracker_url(info_hash, 6889);
    println!("=>> URL: {:?}", url);

    get(&url).await.unwrap();

    Ok(())
}
