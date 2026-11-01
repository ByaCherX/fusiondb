//! Formatting utilities for bytes, durations, and directory calculations.

use std::path::Path;
use std::time::Duration;

/// Formats a size in bytes into a human-readable string (e.g. "1.23 MB").
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

/// Formats a `Duration` into a human-readable string (e.g. "12.34ms", "1.52s").
pub fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs_f64();
    if secs >= 1.0 {
        format!("{:.2}s", secs)
    } else if secs >= 0.001 {
        format!("{:.2}ms", secs * 1000.0)
    } else if secs >= 0.000_001 {
        format!("{:.2}µs", secs * 1_000_000.0)
    } else {
        format!("{:.2}ns", secs * 1_000_000_000.0)
    }
}

/// Recursively calculates the size of a directory in bytes.
pub fn dir_size(path: impl AsRef<Path>) -> std::io::Result<u64> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(0);
    }
    if path.is_file() {
        return Ok(path.metadata()?.len());
    }

    let mut size = 0;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            size += dir_size(entry.path())?;
        } else {
            size += metadata.len();
        }
    }
    Ok(size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 bytes");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024 * 2 + 500 * 1024 * 1024), "2.49 GB");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(Duration::from_secs(2)), "2.00s");
        assert_eq!(format_duration(Duration::from_millis(150)), "150.00ms");
        assert_eq!(format_duration(Duration::from_micros(350)), "350.00µs");
    }

    #[test]
    fn test_dir_size() {
        let dir = tempdir().unwrap();
        let file1_path = dir.path().join("file1.txt");
        let sub_dir = dir.path().join("subdir");
        std::fs::create_dir(&sub_dir).unwrap();
        let file2_path = sub_dir.join("file2.txt");

        // Close the handles before measuring: on Windows, directory entries
        // report stale sizes for files that are still open for writing.
        {
            let mut file1 = File::create(&file1_path).unwrap();
            file1.write_all(b"hello").unwrap();
            let mut file2 = File::create(&file2_path).unwrap();
            file2.write_all(b"world!").unwrap();
        }

        let total_size = dir_size(dir.path()).unwrap();
        assert_eq!(total_size, 11); // "hello" (5) + "world!" (6)
    }
}
