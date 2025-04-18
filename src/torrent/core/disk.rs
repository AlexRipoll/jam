use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use protocol::piece::Piece;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{debug, error, warn};

use crate::torrent::events::Event;

// Commands that the DiskWriter can process
#[derive(Debug)]
pub enum DiskWriterCommand {
    // Write a piece to disk
    WritePiece { piece_index: u32, data: Vec<u8> },

    // Flush all pending writes to disk
    Flush,

    // Query the current write statistics
    QueryStats(mpsc::Sender<DiskWriterStats>),

    // Shutdown the disk writer
    Shutdown,
}

// Statistics about the disk writer operations
#[derive(Debug, Clone)]
pub struct DiskWriterStats {
    pub bytes_written: u64,
    pub pieces_written: u32,
    pub write_errors: u32,
}

// Main DiskWriter actor that handles writing pieces to disk
#[derive(Debug)]
pub struct DiskWriter {
    // File information
    absolute_file_path: PathBuf,
    file_size: u64,
    piece_size: u64,

    // Piece metadata
    pieces: HashMap<u32, Piece>,

    // Statistics
    stats: DiskWriterStats,

    // Communication channels
    event_tx: mpsc::Sender<Event>,
}

impl DiskWriter {
    pub fn new(
        absolute_file_path: PathBuf,
        file_size: u64,
        piece_size: u64,
        pieces: HashMap<u32, Piece>,
        event_tx: mpsc::Sender<Event>,
    ) -> Self {
        // Ensure the download directory exists
        if let Some(parent_dir) = Path::new(&absolute_file_path).parent() {
            if let Err(e) = fs::create_dir_all(parent_dir) {
                warn!("Failed to create directory structure: {}", e);
            }
        }

        Self {
            absolute_file_path,
            file_size,
            piece_size,
            pieces,
            event_tx,
            stats: DiskWriterStats {
                bytes_written: 0,
                pieces_written: 0,
                write_errors: 0,
            },
        }
    }

    pub fn run(mut self) -> (mpsc::Sender<DiskWriterCommand>, JoinHandle<()>) {
        let (command_tx, mut command_rx) = mpsc::channel(100);

        let handle = tokio::spawn(async move {
            // Create the file once at startup
            let file = match self.open_file() {
                Ok(file) => file,
                Err(e) => {
                    error!("Failed to open download file: {}", e);
                    return;
                }
            };

            // Use a BufWriter for more efficient I/O
            let mut writer = BufWriter::new(file);

            // Process commands until channel closes or shutdown received
            while let Some(command) = command_rx.recv().await {
                match command {
                    // FIX: send piece instead of piece_index so there is no need to have the pieces hashmap field
                    DiskWriterCommand::WritePiece { piece_index, data } => {
                        debug!(
                            piece_index = piece_index,
                            size = %data.len(),
                            "Writing to disk"
                        );
                        if let Err(e) = self.write_piece(&mut writer, piece_index, data).await {
                            error!("Failed to write piece {}: {}", piece_index, e);
                            self.stats.write_errors += 1;
                        } else {
                            // Send event notification
                            if let Err(e) = self
                                .event_tx
                                .send(Event::PieceCompleted { piece_index })
                                .await
                            {
                                // TODO: add piece index to queue and retry notifying the
                                // orchestrator
                                error!("Failed to send PieceCompleted event: {}", e);
                            }
                        }
                    }
                    DiskWriterCommand::Flush => {
                        if let Err(e) = writer.flush() {
                            error!("Failed to flush data to disk: {}", e);
                        }
                    }
                    DiskWriterCommand::QueryStats(response_tx) => {
                        if let Err(e) = response_tx.send(self.stats.clone()).await {
                            error!("Failed to send stats response: {}", e);
                        }
                    }
                    DiskWriterCommand::Shutdown => {
                        debug!(task = "DiskWriter", "Shutting down");
                        break;
                    }
                }
            }

            // Always flush at the end, regardless of how we exited the loop
            if let Err(e) = writer.flush() {
                error!("Failed to flush data to disk: {}", e);
            }
        });

        (command_tx, handle)
    }

    fn open_file(&self) -> Result<File, DiskWriterError> {
        // Pre-allocate the file to its full size for better performance
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.absolute_file_path.with_extension("part"))?;

        // Allocate the full file size if it's a new file
        let metadata = file.metadata()?;
        if metadata.len() == 0 {
            file.set_len(self.file_size)?;
        }

        Ok(file)
    }

    async fn write_piece(
        &mut self,
        writer: &mut BufWriter<File>,
        piece_index: u32,
        data: Vec<u8>,
    ) -> Result<(), DiskWriterError> {
        // Look up the piece metadata
        let piece = match self.pieces.get(&piece_index) {
            Some(piece) => piece,
            None => {
                return Err(DiskWriterError::PieceNotFound(piece_index));
            }
        };

        // Calculate the offset
        let offset = piece.offset(self.file_size, self.piece_size);

        // Validate data size
        let ending_pos = offset + data.len() as u64;
        if ending_pos > self.file_size {
            return Err(DiskWriterError::IoError(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Data length exceeds file size: writing to offset {} would surpass the file size of {}",
                    offset, self.file_size
                ),
            )));
        }

        // Seek to the correct position
        writer.seek(SeekFrom::Start(offset))?;

        // Write data
        writer.write_all(&data)?;

        // Update stats
        self.stats.bytes_written += data.len() as u64;
        self.stats.pieces_written += 1;

        debug!(
            piece_index = ?piece_index,
            piece_offset = ?offset,
            bytes = ?data.len(),
            "Successfully wrote piece to disk"
        );

        Ok(())
    }
}

// Error type for DiskWriter operations
#[derive(Debug)]
pub enum DiskWriterError {
    IoError(io::Error),
    PieceNotFound(u32),
    ChannelError(String),
}

impl From<io::Error> for DiskWriterError {
    fn from(err: io::Error) -> Self {
        DiskWriterError::IoError(err)
    }
}

impl From<mpsc::error::SendError<Event>> for DiskWriterError {
    fn from(err: mpsc::error::SendError<Event>) -> Self {
        DiskWriterError::ChannelError(err.to_string())
    }
}

impl std::fmt::Display for DiskWriterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiskWriterError::IoError(err) => write!(f, "I/O error: {}", err),
            DiskWriterError::PieceNotFound(index) => {
                write!(f, "Piece with index {} not found", index)
            }
            DiskWriterError::ChannelError(msg) => write!(f, "Channel error: {}", msg),
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        fs,
        io::{Read, Seek, SeekFrom, Write},
        path::Path,
    };

    use protocol::piece::Piece;
    use tempfile::tempdir;
    use tokio::sync::mpsc;

    use crate::torrent::{
        core::disk::{DiskWriterCommand, DiskWriterError},
        events::Event,
    };

    use super::DiskWriter;

    #[test]
    fn test_disk_writer_new() {
        // Create a temporary directory that will be deleted after the test
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let absolute_file_path = temp_dir.path().join("test_file.dat");

        // Create a piece
        let piece = Piece::new(0, 1024, [0; 20]);
        let mut pieces = HashMap::new();
        pieces.insert(0, piece);

        // Create channel for events
        let (event_tx, _event_rx) = mpsc::channel(10);

        // Create a new DiskWriter
        let disk_writer = DiskWriter::new(absolute_file_path.clone(), 2048, 1024, pieces, event_tx);

        // Check if the directory is created
        assert!(Path::new(&absolute_file_path).parent().unwrap().exists());

        // Verify the initial state
        assert_eq!(disk_writer.file_size, 2048);
        assert_eq!(disk_writer.piece_size, 1024);
        assert_eq!(disk_writer.stats.bytes_written, 0);
        assert_eq!(disk_writer.stats.pieces_written, 0);
        assert_eq!(disk_writer.stats.write_errors, 0);
    }

    #[test]
    fn test_open_file() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let absolute_file_path = temp_dir.path().join("test_file.dat");

        let piece = Piece::new(0, 1024, [0; 20]);
        let mut pieces = HashMap::new();
        pieces.insert(0, piece);

        let (event_tx, _event_rx) = mpsc::channel(10);

        let disk_writer = DiskWriter::new(absolute_file_path.clone(), 2048, 1024, pieces, event_tx);

        // Test opening the file
        let file = disk_writer.open_file().expect("Failed to open file");

        // Verify file size
        assert_eq!(file.metadata().unwrap().len(), 2048);

        // Verify file exists
        assert!(Path::new(&absolute_file_path.with_extension("part")).exists());
    }

    #[tokio::test]
    async fn test_write_piece() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let absolute_file_path = temp_dir.path().join("test_file.dat");

        // Create a piece
        let piece = Piece::new(1, 1024, [0; 20]);
        let mut pieces = HashMap::new();
        pieces.insert(1, piece);

        // Create channel for events
        let (event_tx, _) = mpsc::channel(10);

        // Create a new DiskWriter
        let mut disk_writer =
            DiskWriter::new(absolute_file_path.clone(), 2048, 1024, pieces, event_tx);

        // Open the file
        let file = disk_writer.open_file().expect("Failed to open file");
        let mut writer = std::io::BufWriter::new(file);

        // Create test data
        let test_data = vec![42u8; 1024];

        // Write the piece
        disk_writer
            .write_piece(&mut writer, 1, test_data.clone())
            .await
            .expect("Failed to write piece");

        // Flush the writer
        writer.flush().expect("Failed to flush writer");

        // Verify stats
        assert_eq!(disk_writer.stats.bytes_written, 1024);
        assert_eq!(disk_writer.stats.pieces_written, 1);
        assert_eq!(disk_writer.stats.write_errors, 0);

        // Verify file contents
        let mut file =
            fs::File::open(absolute_file_path.with_extension("part")).expect("Failed to open file");
        // Skip the first piece's worth of bytes since we're verifying the second piece
        file.seek(SeekFrom::Start(1024)).unwrap();

        let mut contents = vec![0u8; 1024];
        file.read_exact(&mut contents).expect("Failed to read file");
        assert_eq!(contents, test_data);
    }

    #[tokio::test]
    async fn test_write_piece_error_piece_not_found() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let absolute_file_path = temp_dir.path().join("test_file.dat");

        // Create a piece
        let piece = Piece::new(0, 1024, [0; 20]);
        let mut pieces = HashMap::new();
        pieces.insert(0, piece);

        // Create channel for events
        let (event_tx, _event_rx) = mpsc::channel(10);

        // Create a new DiskWriter
        let mut disk_writer =
            DiskWriter::new(absolute_file_path.clone(), 1024, 1024, pieces, event_tx);

        // Open the file
        let file = disk_writer.open_file().expect("Failed to open file");
        let mut writer = std::io::BufWriter::new(file);

        // Create test data
        let test_data = vec![42u8; 1024];

        // Try to write a piece that doesn't exist
        let result = disk_writer
            .write_piece(&mut writer, 1, test_data.clone())
            .await;

        // Verify error
        match result {
            Err(DiskWriterError::PieceNotFound(index)) => {
                assert_eq!(index, 1);
            }
            _ => panic!("Expected PieceNotFound error"),
        }
    }

    #[tokio::test]
    async fn test_write_piece_error_exceeds_file_size() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let absolute_file_path = temp_dir.path().join("test_file.dat");

        // Create a piece
        let piece = Piece::new(0, 1024, [0; 20]);
        let mut pieces = HashMap::new();
        pieces.insert(0, piece);

        // Create channel for events
        let (event_tx, _event_rx) = mpsc::channel(10);

        // Create a new DiskWriter with a smaller file size
        let mut disk_writer = DiskWriter::new(
            absolute_file_path.clone(),
            512, // Smaller file size
            1024,
            pieces,
            event_tx,
        );

        // Open the file
        let file = disk_writer.open_file().expect("Failed to open file");
        let mut writer = std::io::BufWriter::new(file);

        // Create test data that exceeds file size
        let test_data = vec![42u8; 1024];

        // Try to write a piece that exceeds file size
        let result = disk_writer
            .write_piece(&mut writer, 0, test_data.clone())
            .await;

        // Verify error
        match result {
            Err(DiskWriterError::IoError(_)) => {
                // Expected error
            }
            _ => panic!("Expected IoError"),
        }
    }

    #[tokio::test]
    async fn test_full_disk_writer_workflow() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let absolute_file_path = temp_dir.path().join("test_file.dat");

        // Create pieces
        let piece1 = Piece::new(0, 1024, [0; 20]);
        let piece2 = Piece::new(1, 1024, [1; 20]);
        let mut pieces = HashMap::new();
        pieces.insert(0, piece1);
        pieces.insert(1, piece2);

        // Create channel for events
        let (event_tx, mut event_rx) = mpsc::channel(10);

        // Create a new DiskWriter
        let disk_writer = DiskWriter::new(absolute_file_path.clone(), 2048, 1024, pieces, event_tx);

        // Start the disk writer
        let (command_tx, join_handle) = disk_writer.run();

        // Create test data
        let test_data1 = vec![42u8; 1024];
        let test_data2 = vec![84u8; 1024];

        // Write first piece
        command_tx
            .send(DiskWriterCommand::WritePiece {
                piece_index: 0,
                data: test_data1.clone(),
            })
            .await
            .expect("Failed to send command");

        // Write second piece
        command_tx
            .send(DiskWriterCommand::WritePiece {
                piece_index: 1,
                data: test_data2.clone(),
            })
            .await
            .expect("Failed to send command");

        // Flush the data
        command_tx
            .send(DiskWriterCommand::Flush)
            .await
            .expect("Failed to send flush command");

        // Wait for events
        let mut received_events = 0;
        while let Some(event) = event_rx.recv().await {
            match event {
                Event::PieceCompleted { piece_index } => {
                    assert!(piece_index == 0 || piece_index == 1);
                    received_events += 1;
                    if received_events == 2 {
                        break;
                    }
                }
                _ => panic!("Expected PieceCompleted event"),
            }
        }

        // Query stats
        let (stats_tx, mut stats_rx) = mpsc::channel(1);
        command_tx
            .send(DiskWriterCommand::QueryStats(stats_tx))
            .await
            .expect("Failed to send query stats command");

        let stats = stats_rx.recv().await.expect("Failed to receive stats");
        assert_eq!(stats.bytes_written, 2048);
        assert_eq!(stats.pieces_written, 2);
        assert_eq!(stats.write_errors, 0);

        // Shutdown the disk writer
        command_tx
            .send(DiskWriterCommand::Shutdown)
            .await
            .expect("Failed to send shutdown command");

        // Wait for the disk writer to finish
        join_handle.await.expect("Failed to join handle");

        // Verify file contents
        let mut file =
            fs::File::open(absolute_file_path.with_extension("part")).expect("Failed to open file");

        // Read and verify first piece
        let mut contents1 = vec![0u8; 1024];
        file.read_exact(&mut contents1)
            .expect("Failed to read file");
        assert_eq!(contents1, test_data1);

        // Read and verify second piece
        let mut contents2 = vec![0u8; 1024];
        file.read_exact(&mut contents2)
            .expect("Failed to read file");
        assert_eq!(contents2, test_data2);
    }

    #[tokio::test]
    async fn test_error_handling() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let absolute_file_path = temp_dir.path().join("test_file.dat");

        // Create pieces
        let piece = Piece::new(0, 1024, [0; 20]);
        let mut pieces = HashMap::new();
        pieces.insert(0, piece);

        // Create channel for events
        let (event_tx, _event_rx) = mpsc::channel(10);

        // Create a new DiskWriter
        let disk_writer = DiskWriter::new(absolute_file_path.clone(), 1024, 1024, pieces, event_tx);

        // Start the disk writer
        let (command_tx, join_handle) = disk_writer.run();

        // Send a command for a non-existent piece
        command_tx
            .send(DiskWriterCommand::WritePiece {
                piece_index: 999,
                data: vec![42u8; 1024],
            })
            .await
            .expect("Failed to send command");

        // Query stats
        let (stats_tx, mut stats_rx) = mpsc::channel(1);
        command_tx
            .send(DiskWriterCommand::QueryStats(stats_tx))
            .await
            .expect("Failed to send query stats command");

        let stats = stats_rx.recv().await.expect("Failed to receive stats");
        assert_eq!(stats.write_errors, 1);

        // Shutdown the disk writer
        command_tx
            .send(DiskWriterCommand::Shutdown)
            .await
            .expect("Failed to send shutdown command");

        // Wait for the disk writer to finish
        join_handle.await.expect("Failed to join handle");
    }
}
