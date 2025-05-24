use std::{
    env,
    io::{self, Write},
    time::Duration,
};

use protocol::bitfield::Bitfield;

use crate::{
    torrent::torrent::TorrentState,
    ui::format::{create_progress_bar, format_bytes, format_eta, format_speed, format_status},
};

use super::format::format_duration;

#[derive(Debug, Clone)]
pub struct RarityStats {
    pub counts: [usize; 10],   // 0-9 peer counts
    pub overflow_count: usize, // 10+ peers
    pub total_pieces: usize,
}

impl RarityStats {
    pub fn new(pieces_rarity: &[u8]) -> Self {
        let mut stats = Self {
            counts: [0; 10],
            overflow_count: 0,
            total_pieces: pieces_rarity.len(),
        };

        for &rarity in pieces_rarity {
            if rarity < 10 {
                stats.counts[rarity as usize] += 1;
            } else {
                stats.overflow_count += 1;
            }
        }

        stats
    }

    pub fn unavailable_pieces(&self) -> usize {
        self.counts[0]
    }

    pub fn rare_pieces(&self) -> usize {
        self.counts[1] + self.counts[2] + self.counts[3]
    }
}

fn rarity_to_color(occurrences: u8, colors: &ColorScheme) -> &'static str {
    match occurrences {
        0..=1 => colors.red,    // Very rare - critical pieces
        2..=3 => colors.orange, // Rare - important to download
        4..=5 => colors.yellow, // Uncommon - moderate priority
        6..=8 => colors.green,  // Common - lower priority
        _ => colors.blue,       // Very common - lowest priority
    }
}

// Configuration structures
#[derive(Debug, Clone)]
pub struct BoxChars {
    pub horizontal: char,
    pub vertical: char,
    pub top_left: char,
    pub top_right: char,
    pub bottom_left: char,
    pub bottom_right: char,
    pub cross: char,
    pub tee_down: char,
    pub tee_up: char,
}

impl BoxChars {
    pub fn unicode() -> Self {
        Self {
            horizontal: '─',
            vertical: '│',
            top_left: '┌',
            top_right: '┐',
            bottom_left: '└',
            bottom_right: '┘',
            cross: '┼',
            tee_down: '┬',
            tee_up: '┴',
        }
    }

    pub fn ascii() -> Self {
        Self {
            horizontal: '-',
            vertical: '|',
            top_left: '+',
            top_right: '+',
            bottom_left: '+',
            bottom_right: '+',
            cross: '+',
            tee_down: '+',
            tee_up: '+',
        }
    }
}

#[derive(Debug, Clone)]
pub struct PieceSymbols {
    pub filled: char,
    pub empty: char,
    pub rarity_symbol: char,
}

impl PieceSymbols {
    // □ ■ ▢ ▣ ▤ ▥ ▦ ▧ ▨ ▩ ▪ ▫ ▬ ▭ ▮ ▯
    pub fn blocks() -> Self {
        Self {
            filled: '▣',
            empty: '▪',
            rarity_symbol: '▣',
        }
    }

    pub fn ascii() -> Self {
        Self {
            filled: '#',
            empty: '.',
            rarity_symbol: '#',
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColorScheme {
    pub green: &'static str,
    pub red: &'static str,
    pub orange: &'static str,
    pub yellow: &'static str,
    pub blue: &'static str,
    pub gray: &'static str,
    pub reset: &'static str,
}

impl ColorScheme {
    pub fn ansi() -> Self {
        Self {
            green: "\x1b[32m",
            red: "\x1b[31m",
            orange: "\x1b[33m", // Using yellow for orange
            yellow: "\x1b[93m", // Bright yellow
            blue: "\x1b[34m",
            gray: "\x1b[90m",
            reset: "\x1b[0m",
        }
    }

    pub fn none() -> Self {
        Self {
            green: "",
            red: "",
            orange: "",
            yellow: "",
            blue: "",
            gray: "",
            reset: "",
        }
    }
}

#[derive(Debug, Clone)]
pub struct DisplayTheme {
    pub box_chars: BoxChars,
    pub piece_symbols: PieceSymbols,
    pub colors: ColorScheme,
}

// Terminal capabilities detection
#[derive(Debug, Clone)]
pub struct TerminalCapabilities {
    pub supports_color: bool,
    pub supports_unicode: bool,
    pub width: usize,
}

impl TerminalCapabilities {
    pub fn detect() -> Self {
        Self {
            supports_color: atty::is(atty::Stream::Stdout) && env::var("NO_COLOR").is_err(),
            supports_unicode: env::var("LANG").unwrap_or_default().contains("UTF-8"),
            width: terminal_size::terminal_size()
                .map(|(w, _)| w.0 as usize)
                .unwrap_or(80),
        }
    }
}

impl DisplayTheme {
    pub fn new(capabilities: &TerminalCapabilities) -> Self {
        Self {
            box_chars: if capabilities.supports_unicode {
                BoxChars::unicode()
            } else {
                BoxChars::ascii()
            },
            piece_symbols: if capabilities.supports_unicode {
                PieceSymbols::blocks()
            } else {
                PieceSymbols::ascii()
            },
            colors: if capabilities.supports_color {
                ColorScheme::ansi()
            } else {
                ColorScheme::none()
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct VisualizationConfig {
    pub width: usize,
    pub theme: DisplayTheme,
    pub capabilities: TerminalCapabilities,
}

impl VisualizationConfig {
    pub fn new() -> Self {
        let capabilities = TerminalCapabilities::detect();
        let content_width = std::cmp::min(100, capabilities.width.saturating_sub(6));

        Self {
            width: content_width,
            theme: DisplayTheme::new(&capabilities),
            capabilities,
        }
    }

    pub fn with_width(mut self, width: usize) -> Self {
        self.width = width;
        self
    }
}

#[derive(Debug)]
pub struct ProgressBox {
    pub width: usize,
    pub title: String,
    pub content: Vec<String>,
    pub theme: DisplayTheme,
}

impl ProgressBox {
    pub fn new(title: String, width: usize, theme: DisplayTheme) -> Self {
        Self {
            width,
            title,
            content: Vec::new(),
            theme,
        }
    }

    pub fn add_line(&mut self, line: String) {
        self.content.push(line);
    }

    pub fn render<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let chars = &self.theme.box_chars;

        // Top border
        writeln!(
            writer,
            "{}{}{}",
            chars.top_left,
            chars.horizontal.to_string().repeat(self.width + 2),
            chars.top_right
        )?;

        // Title
        writeln!(
            writer,
            "{} {:<width$} {}",
            chars.vertical,
            self.title,
            chars.vertical,
            width = self.width
        )?;

        // Content separator
        if !self.content.is_empty() {
            writeln!(
                writer,
                "{}{}{}",
                chars.tee_down,
                chars.horizontal.to_string().repeat(self.width + 2),
                chars.tee_down
            )?;
        }

        // Content lines
        for line in &self.content {
            writeln!(
                writer,
                "{} {:<width$} {}",
                chars.vertical,
                line,
                chars.vertical,
                width = self.width
            )?;
        }

        // Bottom border
        writeln!(
            writer,
            "{}{}{}",
            chars.bottom_left,
            chars.horizontal.to_string().repeat(self.width + 2),
            chars.bottom_right
        )?;

        Ok(())
    }
}

// Streaming bitfield formatter for large torrents
pub fn format_bitfield_pieces_streaming<W: Write>(
    bitfield: &Bitfield,
    writer: &mut W,
    config: &VisualizationConfig,
) -> io::Result<()> {
    let chunk_size = config.width * 10; // Process 10 rows at a time

    for chunk_start in (0..bitfield.total_pieces).step_by(chunk_size) {
        let chunk_end = std::cmp::min(chunk_start + chunk_size, bitfield.total_pieces);
        let chunk = format_piece_chunk(bitfield, chunk_start, chunk_end, config)?;
        write!(writer, "{}", chunk)?;
    }

    Ok(())
}

fn format_piece_chunk(
    bitfield: &Bitfield,
    start: usize,
    end: usize,
    config: &VisualizationConfig,
) -> io::Result<String> {
    let mut result = String::with_capacity((end - start) + (end - start) / config.width);
    let colors = &config.theme.colors;
    let symbols = &config.theme.piece_symbols;

    for piece_index in start..end {
        // Add newline at the beginning of each row (except the first piece of first chunk)
        if piece_index > 0 && piece_index % config.width == 0 {
            result.push('\n');
        }

        let byte_index = piece_index / 8;
        let bit_index = 7 - (piece_index % 8);

        if byte_index < bitfield.bytes.len() {
            let is_set = (bitfield.bytes[byte_index] & (1 << bit_index)) != 0;

            if is_set {
                result.push_str(&format!(
                    "{}{}{}",
                    colors.green, symbols.filled, colors.reset
                ));
            } else {
                result.push_str(&format!("{}{}{}", colors.gray, symbols.empty, colors.reset));
            }
        } else {
            // Handle edge case where piece_index exceeds bitfield
            result.push_str(&format!("{}{}{}", colors.gray, symbols.empty, colors.reset));
        }
    }

    Ok(result)
}

pub async fn render_bitfield_progress(
    state: &TorrentState,
    config: Option<VisualizationConfig>,
) -> Result<(), RenderError> {
    let config = config.unwrap_or_else(VisualizationConfig::new);

    let total_pieces = state.bitfield.total_pieces;

    if total_pieces == 0 {
        return Err(RenderError::EmptyBitfield);
    }

    // Validate bitfield integrity
    let expected_bytes = (total_pieces + 7) / 8;
    if state.bitfield.bytes.len() != expected_bytes {
        return Err(RenderError::CorruptedBitfield);
    }

    // Optimized bit counting - only process bytes with actual piece data
    let filled_pieces = state
        .bitfield
        .bytes
        .iter()
        .take(expected_bytes)
        .enumerate()
        .map(|(i, &byte)| {
            let bits_in_byte = std::cmp::min(8, total_pieces - i * 8);
            let mask = if bits_in_byte == 8 {
                0xFF
            } else {
                (1u8 << bits_in_byte) - 1
            };
            (byte & mask).count_ones() as usize
        })
        .sum::<usize>();

    let completion_percentage = (filled_pieces as f64 / total_pieces as f64) * 100.0;

    let mut progress_box = ProgressBox::new(
        format!("Torrent: {}", state.name),
        config.width,
        config.theme.clone(),
    );

    progress_box.add_line(format!(
        "Progress: {}/{} pieces ({:.1}%)",
        filled_pieces, total_pieces, completion_percentage
    ));

    progress_box.add_line(format!(
        "Download time: {}",
        format_duration(state.download_state.time_elasped)
    ));

    // Render to stdout
    let stdout = io::stdout();
    let mut handle = stdout.lock();

    progress_box.render(&mut handle)?;

    // Display the piece map
    writeln!(handle, "{} Piece Map:", config.theme.box_chars.vertical)?;
    format_bitfield_pieces_streaming(&state.bitfield, &mut handle, &config)?;
    writeln!(handle)?;

    // Legend
    let colors = &config.theme.colors;
    let symbols = &config.theme.piece_symbols;
    writeln!(
        handle,
        "Legend: {}{}{} = Downloaded piece  {}{}{} = Missing piece",
        colors.green, symbols.filled, colors.reset, colors.gray, symbols.empty, colors.reset
    )?;
    writeln!(handle)?;

    Ok(())
}

pub async fn render_pieces_rarity(
    state: &TorrentState,
    config: Option<VisualizationConfig>,
) -> Result<(), RenderError> {
    let config = config.unwrap_or_else(VisualizationConfig::new);

    let pieces_rarity = state.download_state.pieces_rarity.as_ref();

    // Calculate rarity statistics directly
    let rarity_stats = RarityStats::new(pieces_rarity);

    let mut rarity_box = ProgressBox::new(
        "Piece rarity map".to_string(),
        config.width,
        config.theme.clone(),
    );

    rarity_box.add_line(format!(
        "Pieces: {} total, {} unavailable, {} rare (≤3 peers)",
        rarity_stats.total_pieces,
        rarity_stats.unavailable_pieces(),
        rarity_stats.rare_pieces()
    ));

    let stdout = io::stdout();
    let mut handle = stdout.lock();

    rarity_box.render(&mut handle)?;

    // Display rarity map
    format_pieces_rarity_streaming(pieces_rarity, &mut handle, &config)?;

    // Enhanced legend with color coding
    let colors = &config.theme.colors;
    let symbol = config.theme.piece_symbols.rarity_symbol;

    writeln!(handle, "Legend:")?;
    writeln!(
        handle,
        "  {}{}{} Unavailable (0 peers)     {}{}{} Very rare (1 peer)",
        colors.red, symbol, colors.reset, colors.red, symbol, colors.reset
    )?;
    writeln!(
        handle,
        "  {}{}{} Rare (2-3 peers)          {}{}{} Uncommon (4-5 peers)",
        colors.orange, symbol, colors.reset, colors.yellow, symbol, colors.reset
    )?;
    writeln!(
        handle,
        "  {}{}{} Common (6-8 peers)        {}{}{} Very common (9+ peers)",
        colors.green, symbol, colors.reset, colors.blue, symbol, colors.reset
    )?;
    writeln!(handle)?;

    Ok(())
}

fn format_pieces_rarity_streaming<W: Write>(
    pieces_rarity: &[u8],
    writer: &mut W,
    config: &VisualizationConfig,
) -> io::Result<()> {
    let chunk_size = config.width * 10;

    for chunk_start in (0..pieces_rarity.len()).step_by(chunk_size) {
        let chunk_end = std::cmp::min(chunk_start + chunk_size, pieces_rarity.len());
        let chunk =
            format_rarity_chunk(&pieces_rarity[chunk_start..chunk_end], chunk_start, config)?;
        write!(writer, "{}", chunk)?;
    }

    Ok(())
}

fn format_rarity_chunk(
    chunk: &[u8],
    start_index: usize,
    config: &VisualizationConfig,
) -> io::Result<String> {
    let mut result = String::with_capacity(chunk.len() * 10); // Account for ANSI codes
    let symbol = config.theme.piece_symbols.rarity_symbol;

    for (relative_index, &rarity) in chunk.iter().enumerate() {
        let absolute_index = start_index + relative_index;

        // Add newline at the beginning of each row (except the first)
        if absolute_index > 0 && absolute_index % config.width == 0 {
            result.push('\n');
        }

        let color = rarity_to_color(rarity, &config.theme.colors);
        result.push_str(&format!("{}{}{}", color, symbol, config.theme.colors.reset));
    }

    Ok(result)
}

// Error handling
#[derive(Debug)]
pub enum RenderError {
    EmptyBitfield,
    CorruptedBitfield,
    IoError(io::Error),
    InvalidConfiguration,
}

impl From<io::Error> for RenderError {
    fn from(err: io::Error) -> Self {
        RenderError::IoError(err)
    }
}

pub async fn display_inspect(torrent_state: &TorrentState) {
    let visualization_config = VisualizationConfig {
        width: 100,
        theme: DisplayTheme {
            box_chars: BoxChars::unicode(),
            piece_symbols: PieceSymbols::blocks(),
            colors: ColorScheme::ansi(),
        },
        capabilities: TerminalCapabilities::detect(),
    };
    render_bitfield_progress(torrent_state, Some(visualization_config.clone())).await;
    render_pieces_rarity(torrent_state, Some(visualization_config.clone())).await;
}

pub async fn display_torrents_state(torrents_state: Vec<TorrentState>) {
    // Print header
    println!();
    println!("┌──────────────────────────────────────────────────────────────────────────────────────────────────┐");
    println!("│                                 TORRENT STATUS                                                   │");
    println!("├──────────────────────────────────────────────────────────────────────────────────────────────────┤");
    println!("│ Name                   │ Progress        │ Size                │ Speed  │ ETA   │ Peers │ Status │");
    println!("├──────────────────────────────────────────────────────────────────────────────────────────────────┤");

    for torrent_state in torrents_state {
        let truncated_name = if torrent_state.name.len() > 22 {
            format!("{}...", &torrent_state.name[..19])
        } else {
            format!("{:<22}", torrent_state.name)
        };

        let progress_bar = create_progress_bar(torrent_state.download_state.progress_percentage);
        let downloaded_display = format_bytes(torrent_state.download_state.downloaded_bytes);
        let size_display = format_bytes(
            torrent_state.download_state.downloaded_bytes + torrent_state.download_state.left_bytes,
        );
        let download_speed = if torrent_state.peers_count > 0 {
            torrent_state.download_state.downloaded_bytes as f64
                / torrent_state.download_state.time_elasped.as_secs() as f64
        } else {
            0.0
        };
        let speed_display = format_speed(download_speed);
        let eta_display = if download_speed > 0.0 && torrent_state.peers_count > 0 {
            let remaining = Duration::from_secs(
                (torrent_state.download_state.left_bytes as f64 / download_speed) as u64,
            );
            format_eta(Some(remaining))
        } else {
            format_eta(None)
        };
        let status_display = format_status(&torrent_state.status);

        println!(
            "│ {} │ {} │ {:>8} of {:>8}│ {:>6} │ {:>5} │ {:>5} │ {:>6} │",
            truncated_name,
            progress_bar,
            downloaded_display,
            size_display,
            speed_display,
            eta_display,
            torrent_state.peers_count,
            status_display
        );
    }

    println!("└──────────────────────────────────────────────────────────────────────────────────────────────────┘");
    println!();
}

pub async fn display_compact_state(torrents_state: Vec<TorrentState>) {
    for state in torrents_state {
        let progress_bar = create_progress_bar(state.download_state.progress_percentage);
        let speed = format_speed(state.download_speed);
        let eta = format_eta(state.eta);

        println!(
            "{} {} {}% ↓{} ETA:{} [{}]",
            state.name,
            progress_bar,
            state.download_state.progress_percentage,
            speed,
            eta,
            format_status(&state.status)
        );
    }
}
