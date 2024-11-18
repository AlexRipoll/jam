use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    sync::mpsc,
};

use super::piece::Piece;

pub struct Writer {
    file: File,
    receiver: mpsc::Receiver<Piece>,
}

impl Writer {
    pub fn new(file_path: &str, receiver: mpsc::Receiver<Piece>) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)
            .expect("Failed to open file");

        Self { file, receiver }
    }

    pub fn run(&mut self) {
        while let Ok(piece) = self.receiver.recv() {
            if let Err(e) = self.write_piece_to_disk(piece) {
                eprintln!("Error writing piece to disk: {}", e);
            }
        }
    }

    fn write_piece_to_disk(&self, piece: Piece) -> std::io::Result<()> {
        let mut file = &self.file;
        let offset = u64::from(piece.index()).saturating_mul(piece.size() as u64);
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&piece.assemble().unwrap())?;
        println!(
            "Successfully wrote piece {} at offset {}",
            piece.index(),
            offset
        );

        Ok(())
    }
}
