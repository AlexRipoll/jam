use std::time::Duration;

use super::torrent::Status;

pub fn create_progress_bar(percentage: u64) -> String {
    let bar_width = 8;
    let filled = (percentage as usize * bar_width) / 100;
    let empty = bar_width - filled;
    format!(
        "[{}{}] {:>3}%",
        "█".repeat(filled),
        "░".repeat(empty),
        percentage
    )
}

pub fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    const THRESHOLD: f64 = 1024.0;

    if bytes == 0 {
        return "0 B".to_string();
    }

    let size = bytes as f64;
    let base = size.log10() / THRESHOLD.log10();
    let unit_index = (base as usize).min(UNITS.len() - 1);
    let value = size / THRESHOLD.powi(unit_index as i32);

    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.1} {}", value, UNITS[unit_index])
    }
}

pub fn format_speed(bytes_per_second: f64) -> String {
    if bytes_per_second == 0.0 {
        return "0 B/s".to_string();
    }

    const UNITS: &[&str] = &["B/s", "KB/s", "MB/s", "GB/s"];
    const THRESHOLD: f64 = 1024.0;

    let mut size = bytes_per_second;
    let mut unit_index = 0;

    while size >= THRESHOLD && unit_index < UNITS.len() - 1 {
        size /= THRESHOLD;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{:.0} {}", size, UNITS[unit_index])
    } else {
        format!("{:.1} {}", size, UNITS[unit_index])
    }
}

pub fn format_eta(eta: Option<Duration>) -> String {
    match eta {
        Some(duration) => {
            let total_seconds = duration.as_secs();
            if total_seconds < 60 {
                format!("{}s", total_seconds)
            } else if total_seconds < 3600 {
                format!("{}m", total_seconds / 60)
            } else if total_seconds < 86400 {
                format!("{}h", total_seconds / 3600)
            } else {
                format!("{}d", total_seconds / 86400)
            }
        }
        None => "∞".to_string(),
    }
}

pub fn format_status(status: &Status) -> String {
    match status {
        Status::Starting => "START".to_string(),
        Status::Downloading => "DOWN".to_string(),
        Status::Paused => "PAUSE".to_string(),
        Status::Completed => "DONE".to_string(),
        Status::Error(_) => "ERROR".to_string(),
    }
}
