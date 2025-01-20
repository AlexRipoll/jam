use std::{
    fs::{self, File, OpenOptions},
    io::{self, Seek, SeekFrom, Write},
    path::Path,
};

use tracing::debug;

use protocol::bitfield::Bitfield;
use protocol::piece::Piece;

pub struct Writer {
    download_file: File,
}

impl Writer {
    pub fn new(download_path: &str) -> Result<Self, io::Error> {
        // Get the parent directory of the file path
        if let Some(parent_dir) = Path::new(download_path).parent() {
            // Create all directories if they do not exist
            fs::create_dir_all(parent_dir)?;
        }

        let download_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(download_path)
            .expect("Failed to open file");

        Ok(Self { download_file })
    }

    pub fn write_piece_to_disk(
        &self,
        piece: &Piece,
        file_size: u64,
        piece_standard_size: u64,
        assembled_piece: &[u8],
    ) -> std::io::Result<()> {
        let mut file = &self.download_file;
        let offset = piece.offset(file_size, piece_standard_size);

        // Check if the assembled data surpasses the file size
        let total_written = offset + assembled_piece.len() as u64;
        if total_written > file_size {
            return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Data length exceeds file size: writing to offset {} would surpass the file size of {}",
                offset, file_size
            ),
        ));
        }

        file.seek(SeekFrom::Start(offset))?;
        file.write_all(assembled_piece)?;
        debug!(piece_index=?piece.index(),piece_offset=?offset, "Successfully wrote piece to disk");

        Ok(())
    }

    pub fn write_bitfield_to_disk(&self, bitfield: &Bitfield) -> io::Result<()> {
        let mut file = &self.download_file;
        file.write_all(&bitfield.bytes)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{
        fs::{remove_file, File},
        io::{self, Read},
    };

    use tempfile::tempdir;

    use crate::disk::disk::Writer;
    use protocol::piece::Piece;

    #[test]
    fn test_writer_creation_and_file_creation() {
        let dir = tempdir().expect("Failed to create temp directory");
        let download_path = dir.path().join("download_test_file.txt");

        // Create a Writer
        let writer = Writer::new(download_path.to_str().unwrap());

        // Check if the writer was created without errors
        assert!(writer.is_ok());

        // Check if the file is actually created
        let file_exists = download_path.exists();
        assert!(file_exists);

        // Clean up
        remove_file(download_path).expect("Failed to remove test file");
    }

    #[test]
    fn test_write_piece_to_disk() {
        let dir = tempdir().expect("Failed to create temp directory");
        let download_path = dir.path().join("download_test_file.txt");

        // Create a Writer
        let writer = Writer::new(download_path.to_str().unwrap()).expect("Failed to create writer");

        // Create a dummy piece to write
        let piece = Piece::new(0, 100, [0u8; 20]);
        let piece_data = vec![1u8; 100];
        let file_size = 200;
        let piece_standard_size = 100;

        // Write the piece to the disk
        let result =
            writer.write_piece_to_disk(&piece, file_size, piece_standard_size, &piece_data);

        // Ensure no error occurred
        assert!(result.is_ok());

        // Read back the written data and ensure it's correct
        let mut file = File::open(download_path.clone()).expect("Failed to open test file");
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .expect("Failed to read test file");

        // Check that the data we wrote is present in the file
        assert_eq!(buffer, piece_data);

        // Clean up
        remove_file(download_path).expect("Failed to remove test file");
    }

    #[test]
    fn test_write_pieces_to_disk() {
        let dir = tempdir().expect("Failed to create temp directory");
        let download_path = dir.path().join("download_test_file.txt");

        // Create a Writer
        let writer = Writer::new(download_path.to_str().unwrap()).expect("Failed to create writer");

        // Create a dummy piece to write
        let piece = Piece::new(1, 10, [0u8; 20]);
        let piece1_data = vec![1u8; 10];
        let file_size = 40;
        let piece_standard_size = 10;

        // Write the piece to the disk
        let result =
            writer.write_piece_to_disk(&piece, file_size, piece_standard_size, &piece1_data);

        // Ensure no error occurred
        assert!(result.is_ok());

        // Read back the written data and ensure it's correct
        let mut file = File::open(download_path.clone()).expect("Failed to open test file");
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .expect("Failed to read test file");

        // Check that the data we wrote is present in the file
        assert_eq!(buffer, [vec![0u8; 10], piece1_data.clone()].concat());

        // Create a dummy piece to write
        let piece = Piece::new(3, 10, [0u8; 20]);
        let piece2_data = vec![2u8; 10];

        // Write the piece to the disk
        let result =
            writer.write_piece_to_disk(&piece, file_size, piece_standard_size, &piece2_data);

        // Ensure no error occurred
        assert!(result.is_ok());

        // Read back the written data and ensure it's correct
        let mut file = File::open(download_path.clone()).expect("Failed to open test file");
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .expect("Failed to read test file");

        // Check that the data we wrote is present in the file
        assert_eq!(
            buffer,
            [vec![0u8; 10], piece1_data, vec![0u8; 10], piece2_data].concat()
        );

        // Clean up
        remove_file(download_path).expect("Failed to remove test file");
    }

    #[test]
    fn test_write_piece_data_surpasses_file_size() {
        let dir = tempdir().expect("Failed to create temp directory");
        let download_path = dir.path().join("download_test_file.txt");

        // Create a Writer
        let writer = Writer::new(download_path.to_str().unwrap()).expect("Failed to create writer");

        // Create a dummy piece to write
        let piece = Piece::new(1, 105, [0u8; 20]);
        let piece_data = vec![1u8; 105];
        let file_size = 200;
        let piece_standard_size = 100;

        // Write the piece to the disk
        let result =
            writer.write_piece_to_disk(&piece, file_size, piece_standard_size, &piece_data);

        // Ensure no error occurred
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("Data length exceeds file size"));

        // Clean up
        remove_file(download_path).expect("Failed to remove test file");
    }
}
