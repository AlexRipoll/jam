use std::{
    fs::File,
    io::{self, Read},
};

use metainfo::Metainfo;
use serde_bencode::de;

pub mod metainfo;

fn main() -> io::Result<()> {
    // Open the torrent file
    let mut file = File::open("sintel.torrent")?;
    let mut buffer = Vec::new();

    // Read the entire file into buffer
    file.read_to_end(&mut buffer)?;

    // Deserialize the buffer into a Metainfo struct
    match de::from_bytes::<Metainfo>(&buffer) {
        Ok(metainfo) => {
            println!("{:#?}", metainfo);
        }
        Err(e) => {
            eprintln!("Failed to deserialize torrent file: {}", e);
        }
    }

    Ok(())
}
