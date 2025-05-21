use std::io::{self, BufRead, Write};

use tracing::{debug, Level};

use crate::torrent::torrent::TorrentManager;

// Define commands and control messages
#[derive(Debug)]
enum Command {
    Start(String),   // Start/resume download with torrent file path
    Stop(usize),     // Pause a download by ID
    Resume(usize),   // Resume a paused download by ID
    Cancel(usize),   // Cancel and remove download by ID
    State,           // Show state of all downloads
    Inspect(usize),  // Show detailed stats for a specific download
    Log(Level),      // Change log level
    Help,            // Show help
    Exit,            // Exit the application
    Invalid(String), // Invalid command
}

// Parse a command string into a Command enum
fn parse_command(input: &str) -> Command {
    let parts: Vec<&str> = input.trim().split_whitespace().collect();
    if parts.is_empty() {
        return Command::Invalid("Empty command".to_string());
    }

    match parts[0].to_lowercase().as_str() {
        "start" | "add" => {
            if parts.len() < 2 {
                Command::Invalid("Missing torrent file path".to_string())
            } else {
                Command::Start(parts[1].to_string())
            }
        }
        "stop" | "pause" => {
            if parts.len() < 2 {
                Command::Invalid("Missing torrent ID".to_string())
            } else {
                match parts[1].parse::<usize>() {
                    Ok(id) => Command::Stop(id),
                    Err(_) => Command::Invalid("Invalid torrent ID".to_string()),
                }
            }
        }
        "resume" => {
            if parts.len() < 2 {
                Command::Invalid("Missing torrent ID".to_string())
            } else {
                match parts[1].parse::<usize>() {
                    Ok(id) => Command::Resume(id),
                    Err(_) => Command::Invalid("Invalid torrent ID".to_string()),
                }
            }
        }
        "cancel" | "remove" => {
            if parts.len() < 2 {
                Command::Invalid("Missing torrent ID".to_string())
            } else {
                match parts[1].parse::<usize>() {
                    Ok(id) => Command::Cancel(id),
                    Err(_) => Command::Invalid("Invalid torrent ID".to_string()),
                }
            }
        }
        "state" => Command::State,
        "inspect" => {
            if parts.len() < 2 {
                Command::Invalid("Missing torrent ID".to_string())
            } else {
                match parts[1].parse::<usize>() {
                    Ok(id) => Command::Inspect(id),
                    Err(_) => Command::Invalid("Invalid torrent ID".to_string()),
                }
            }
        }
        "log" => {
            if parts.len() < 2 {
                Command::Invalid("Missing log level".to_string())
            } else {
                match parts[1].to_uppercase().as_str() {
                    "ERROR" => Command::Log(Level::ERROR),
                    "WARN" => Command::Log(Level::WARN),
                    "INFO" => Command::Log(Level::INFO),
                    "DEBUG" => Command::Log(Level::DEBUG),
                    "TRACE" => Command::Log(Level::TRACE),
                    _ => Command::Invalid("Invalid log level".to_string()),
                }
            }
        }
        "help" => Command::Help,
        "exit" | "quit" => Command::Exit,
        _ => Command::Invalid(format!("Unknown command: {}", parts[0])),
    }
}

// Display help information
fn display_help() {
    println!("\nJam BitTorrent Client Help");
    println!("=====================");
    println!("Commands:");
    println!("  start <file>       - Start downloading a torrent file");
    println!("  stop <id>          - Pause a download");
    println!("  resume <id>        - Resume a paused download");
    println!("  cancel <id>        - Cancel and remove a download");
    println!("  state              - Show state of all downloads");
    println!("  stats <id>         - Show detailed stats for a download");
    println!("  peers <id>         - Show connected peers for a download");
    println!("  log <level>        - Set log level (ERROR, WARN, INFO, DEBUG, TRACE)");
    println!("  help               - Show this help");
    println!("  exit               - Exit the application");
}

// The main REPL loop
pub async fn run_repl<'a>(mut manager: TorrentManager<'a>) -> io::Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    println!("Welcome to Jam BitTorrent Client");
    println!("Type 'help' for a list of commands");

    loop {
        print!("> ");
        stdout.flush()?;

        let mut input = String::new();
        stdin.lock().read_line(&mut input)?;

        let command = parse_command(&input);
        debug!("Parsed command: {:?}", command);

        match command {
            Command::Start(path) => match manager.start_torrent(&path).await {
                Ok(torrent_name) => println!("Initiating download for torrent: {}", torrent_name),
                Err(e) => println!("Failed to start download: {}", e),
            },
            Command::Stop(id) => {
                unimplemented!();
            }
            Command::Resume(id) => {
                unimplemented!();
            }
            Command::Cancel(id) => {
                unimplemented!();
            }
            Command::State => manager.display_torrents_state().await,
            Command::Inspect(id) => {
                manager.display_pieces_maps().await;
            }
            Command::Log(level) => {
                println!("Changing log level to {:?}", level);
                unimplemented!();
            }
            Command::Help => {
                display_help();
            }
            Command::Exit => {
                println!("Exiting...");
                break;
            }
            Command::Invalid(reason) => {
                println!("Invalid command: {}", reason);
                println!("Type 'help' for a list of commands");
            }
        }
    }

    Ok(())
}
