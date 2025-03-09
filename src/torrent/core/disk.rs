use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Seek, SeekFrom, Write},
    path::Path,
    sync::Arc,
};

use protocol::piece::Piece;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{debug, error, info, warn};

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
    download_path: String,
    file_size: u64,
    piece_size: u64,

    // Piece metadata
    pieces: HashMap<u32, Piece>,

    // Statistics
    stats: DiskWriterStats,

    // Communication channels
    event_tx: Arc<mpsc::Sender<Event>>,
}

impl DiskWriter {
    pub fn new(
        download_path: String,
        file_size: u64,
        piece_size: u64,
        pieces: HashMap<u32, Piece>,
        event_tx: Arc<mpsc::Sender<Event>>,
    ) -> Self {
        // Ensure the download directory exists
        if let Some(parent_dir) = Path::new(&download_path).parent() {
            if let Err(e) = fs::create_dir_all(parent_dir) {
                warn!("Failed to create directory structure: {}", e);
            }
        }

        Self {
            download_path,
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
                    DiskWriterCommand::WritePiece { piece_index, data } => {
                        if let Err(e) = self.write_piece(&mut writer, piece_index, data).await {
                            error!("Failed to write piece {}: {}", piece_index, e);
                            self.stats.write_errors += 1;
                        }
                    }
                    DiskWriterCommand::Flush => {
                        if let Err(e) = writer.flush() {
                            error!("Failed to flush data to disk: {}", e);
                        } else {
                            debug!("Successfully flushed data to disk");
                        }
                    }
                    DiskWriterCommand::QueryStats(response_tx) => {
                        let _ = response_tx.send(self.stats.clone()).await;
                    }
                    DiskWriterCommand::Shutdown => {
                        info!("DiskWriter shutting down");
                        // Flush any pending writes
                        let _ = writer.flush();
                        break;
                    }
                }
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
            .open(&self.download_path)?;

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

        // Send event notification
        self.event_tx
            .send(Event::PieceCompleted { piece_index })
            .await?;

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
