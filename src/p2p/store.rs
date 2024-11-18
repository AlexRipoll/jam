use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    sync::mpsc,
};

use super::piece::Piece;

pub struct Writer {
    file: File,
    receiver: mpsc::Receiver<(Piece, Vec<u8>)>,
}

impl Writer {
    pub fn new(file_path: &str, receiver: mpsc::Receiver<(Piece, Vec<u8>)>) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)
            .expect("Failed to open file");

        Self { file, receiver }
    }

    pub fn run(&mut self) {
        while let Ok((piece, assembled_piece)) = self.receiver.recv() {
            if let Err(e) = self.write_piece_to_disk(piece, &assembled_piece) {
                eprintln!("Error writing piece to disk: {}", e);
            }
        }
    }

    fn write_piece_to_disk(&self, piece: Piece, assembled_piece: &[u8]) -> std::io::Result<()> {
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
