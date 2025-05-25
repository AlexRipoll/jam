use core::error;
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use protocol::piece::Piece;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::{debug, error, warn};

use crate::events::Event;

/// Commands that the DiskWriter can process
///
/// These commands represent the complete API for interacting with the DiskWriter actor.
#[derive(Debug)]
pub enum DiskWriterCommand {
    /// Write a piece to disk at the appropriate offset
    WritePiece { piece_index: u32, data: Vec<u8> },
    /// Flush all pending writes to disk
    Flush,
    /// Query the current write statistics - response will be sent on the provided channel
    QueryStats(mpsc::Sender<DiskWriterStats>),
    /// Remove the download file from disk
    RemoveFile {
        confirmation_channel: oneshot::Sender<()>,
    },
    /// Shutdown the disk writer gracefully
    Shutdown,
}

/// Statistics about the disk writer operations
///
/// These stats can be queried using the `QueryStats` command.
#[derive(Debug, Clone, Default)]
pub struct DiskWriterStats {
    /// Total number of bytes written to disk
    pub downloaded_bytes: u64,

    /// Number of bytes sent to peers
    pub uploaded_bytes: u64,

    /// Number of write errors encountered
    pub write_errors: u32,
}

/// Main DiskWriter actor that handles writing pieces to disk
///
/// Provides an asynchronous API for writing file pieces to disk in a non-blocking way.
/// The DiskWriter maintains its own state including statistics and manages direct file I/O.
#[derive(Debug)]
pub struct DiskWriter {
    /// Path where the file will be written
    absolute_file_path: PathBuf,
    /// Total size of the target file in bytes
    file_size: u64,
    /// Size of each piece in bytes
    piece_size: u64,
    /// Map of all pieces (index -> metadata)
    pieces: HashMap<u32, Piece>,
    /// Statistics about disk operations
    stats: DiskWriterStats,
    /// Channel for sending events back to the orchestrator
    event_tx: mpsc::Sender<Event>,
}

impl DiskWriter {
    /// Create a new DiskWriter with the given parameters
    ///
    /// This initializes a new DiskWriter but doesn't start its processing loop yet.
    /// Call `run()` to start processing commands.
    ///
    /// # Arguments
    ///
    /// * `absolute_file_path` - The path where the file will be written
    /// * `file_size` - Total size of the target file in bytes
    /// * `piece_size` - Size of each piece in bytes
    /// * `pieces` - Map of all pieces (index -> metadata)
    /// * `event_tx` - Channel for sending events back to the orchestrator
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
            stats: DiskWriterStats::default(),
        }
    }

    /// Start the DiskWriter processing loop
    ///
    /// This runs the DiskWriter in a separate Tokio task and returns a channel
    /// that can be used to send commands to it, along with the task handle.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// * `mpsc::Sender<DiskWriterCommand>` - Channel for sending commands to the DiskWriter
    /// * `JoinHandle<()>` - Handle for the task running the DiskWriter
    pub fn run(mut self) -> (mpsc::Sender<DiskWriterCommand>, JoinHandle<()>) {
        let (command_tx, mut command_rx) = mpsc::channel(128);

        let handle = tokio::spawn(async move {
            // Create the file once at startup
            let file = match self.create_and_open_file() {
                Ok(file) => file,
                Err(e) => {
                    error!("Failed to open download file: {}", e);
                    return;
                }
            };

            // Use a BufWriter for more efficient I/O
            let mut writer = BufWriter::with_capacity(1024 * 64, file);

            // Process commands until channel closes or shutdown received
            while let Some(command) = command_rx.recv().await {
                match command {
                    DiskWriterCommand::WritePiece { piece_index, data } => {
                        debug!(
                            piece_index = piece_index,
                            size = %data.len(),
                            "Writing piece to disk"
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
                                error!("Failed to send PieceCompleted event: {}", e);
                            }
                        }
                    }
                    DiskWriterCommand::Flush => {
                        debug!("Flushing writes to disk");
                        if let Err(e) = writer.flush() {
                            error!("Failed to flush data to disk: {}", e);
                        }
                    }
                    DiskWriterCommand::QueryStats(response_tx) => {
                        if let Err(e) = response_tx.send(self.stats.clone()).await {
                            error!("Failed to send stats response: {}", e);
                        }
                    }
                    DiskWriterCommand::RemoveFile {
                        confirmation_channel: response_tx,
                    } => {
                        debug!(
                            task = "RemoveFile",
                            "Removing file {:?}", self.absolute_file_path
                        );
                        if let Err(e) = self.remove_file().await {
                            error!("Failed to remove file: {}", e);
                        }
                        if let Err(_) = response_tx.send(()) {
                            error!("Failed to send cancellation confirmation");
                        }
                        break;
                    }
                    DiskWriterCommand::Shutdown => {
                        debug!(task = "DiskWriter", "Shutting down gracefully");
                        // Final flush before exit
                        if let Err(e) = writer.flush() {
                            error!("Failed to perform final flush during shutdown: {}", e);
                        }
                        break;
                    }
                }
            }

            // Always flush at the end, regardless of how we exited the loop
            if let Err(e) = writer.flush() {
                error!("Failed to flush data to disk during shutdown: {}", e);
            }
        });

        (command_tx, handle)
    }

    /// Creates and opens the target file, pre-allocating space if necessary
    ///
    /// The file is created with a ".part" extension until it's complete.
    fn create_and_open_file(&self) -> Result<File, DiskWriterError> {
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

    /// Writes a piece to the appropriate offset in the file
    ///
    /// # Arguments
    ///
    /// * `writer` - The BufWriter wrapping the file
    /// * `piece_index` - Index of the piece to write
    /// * `data` - The piece data to write
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if the write failed
    async fn write_piece(
        &mut self,
        writer: &mut BufWriter<File>,
        piece_index: u32,
        data: Vec<u8>,
    ) -> Result<(), DiskWriterError> {
        // Look up the piece metadata
        let piece = self
            .pieces
            .get(&piece_index)
            .ok_or_else(|| DiskWriterError::PieceNotFound(piece_index))?;

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
        self.stats.downloaded_bytes += data.len() as u64;

        debug!(
            piece_index = ?piece_index,
            piece_offset = ?offset,
            bytes = ?data.len(),
            "Successfully wrote piece to disk"
        );

        Ok(())
    }

    /// Remove the download file
    ///
    /// This is the async version that can be used within the DiskWriter's
    /// processing loop without blocking.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the file was successfully removed or didn't exist,
    /// `Err(DiskWriterError)` if removal failed
    pub async fn remove_file(&self) -> Result<(), DiskWriterError> {
        let file_path = self.absolute_file_path.with_extension("part");

        match tokio::fs::remove_file(&file_path).await {
            Ok(()) => {
                debug!("Successfully removed file: {:?}", file_path);
                Ok(())
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // File doesn't exist, which is fine
                debug!("File already doesn't exist: {:?}", file_path);
                Err(DiskWriterError::FileNotFound)
            }
            Err(e) => {
                error!("Failed to remove file {:?}: {}", file_path, e);
                Err(DiskWriterError::IoError(e))
            }
        }
    }
}

/// Error type for DiskWriter operations
///
/// Represents all possible error conditions that can occur during disk operations.
#[derive(Debug)]
pub enum DiskWriterError {
    /// An I/O error occurred
    IoError(io::Error),
    /// Attempted to write a piece that wasn't found in the piece map
    PieceNotFound(u32),
    /// An error occurred when sending messages on a channel
    ChannelError(String),
    FileNotFound,
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
            DiskWriterError::FileNotFound => write!(f, "File not found"),
        }
    }
}

impl std::error::Error for DiskWriterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DiskWriterError::IoError(err) => Some(err),
            _ => None,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use protocol::piece::Piece;
    use std::{
        collections::HashMap,
        error::Error,
        fs,
        io::{Read, Seek, SeekFrom, Write},
        path::Path,
    };
    use tempfile::tempdir;
    use tokio::sync::mpsc;

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
        assert_eq!(disk_writer.stats.downloaded_bytes, 0);
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
        let file = disk_writer
            .create_and_open_file()
            .expect("Failed to open file");

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
        let file = disk_writer
            .create_and_open_file()
            .expect("Failed to open file");
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
        assert_eq!(disk_writer.stats.downloaded_bytes, 1024);
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
        let file = disk_writer
            .create_and_open_file()
            .expect("Failed to open file");
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
        let file = disk_writer
            .create_and_open_file()
            .expect("Failed to open file");
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
        assert_eq!(stats.downloaded_bytes, 2048);
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

    #[test]
    fn test_error_display_and_source() {
        // Test IoError
        let io_err = io::Error::new(io::ErrorKind::Other, "test error");
        let disk_err = DiskWriterError::IoError(io_err);
        assert!(disk_err.to_string().contains("I/O error"));
        assert!(disk_err.source().is_some());

        // Test PieceNotFound
        let piece_err = DiskWriterError::PieceNotFound(42);
        assert!(piece_err.to_string().contains("42"));
        assert!(piece_err.source().is_none());

        // Test ChannelError
        let channel_err = DiskWriterError::ChannelError("test error".to_string());
        assert!(channel_err.to_string().contains("Channel error"));
        assert!(channel_err.source().is_none());
    }
}
