use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    sync::mpsc,
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

        Self { file}
    }


    pub fn write_piece_to_disk(&self, piece: Piece, assembled_piece: &[u8]) -> std::io::Result<()> {
        let mut file = &self.file;
        let offset = piece.offset();
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
