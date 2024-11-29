use std::{
    fs::{File, OpenOptions},
    io::{self, Seek, SeekFrom, Write},
};

use crate::p2p::piece::Piece;

pub struct Writer {
    file: File,
}

impl Writer {
    pub fn new(file_path: &str) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)
            .expect("Failed to open file");

        Self { file }
    }

    pub fn write_piece_to_disk(
        &self,
        piece: Piece,
        file_size: u64,
        piece_standard_size: u64,
        assembled_piece: &[u8],
    ) -> std::io::Result<()> {
        let mut file = &self.file;
        let offset = piece
            .offset(file_size, piece_standard_size)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(assembled_piece)?;
        println!(
            "Successfully wrote piece {} at offset {}",
            piece.index(),
            offset
        );

        Ok(())
    }
}
