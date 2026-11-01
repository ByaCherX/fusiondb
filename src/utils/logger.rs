//! Logger configuration and initialization utilities.
//!
//! Exposes configurable options to control FusionDB logging levels,
//! formatting, and target destinations.

use log::LevelFilter;

/// Configuration options for the FusionDB logger.
#[derive(Debug, Clone)]
pub struct LoggerConfig {
    /// The default logging level filter.
    pub default_level: LevelFilter,
    /// Whether to print the timestamp in the log output.
    pub show_timestamp: bool,
    /// Whether to log to stderr (default is stdout).
    pub log_to_stderr: bool,
}

impl Default for LoggerConfig {
    fn default() -> Self {
        Self {
            default_level: LevelFilter::Info,
            show_timestamp: true,
            log_to_stderr: false,
        }
    }
}

impl LoggerConfig {
    /// Create a new LoggerConfig with the specified log level filter.
    pub fn new(default_level: LevelFilter) -> Self {
        Self {
            default_level,
            ..Default::default()
        }
    }

    /// Set whether timestamps should be displayed in the logs.
    pub fn show_timestamp(mut self, show: bool) -> Self {
        self.show_timestamp = show;
        self
    }

    /// Set whether to route logs to stderr instead of stdout.
    pub fn log_to_stderr(mut self, to_stderr: bool) -> Self {
        self.log_to_stderr = to_stderr;
        self
    }
}

/// Initializes the global logging system with the specified configuration.
///
/// This should only be called once during application execution.
/// Subsequent calls will return an error or be ignored.
pub fn init_logger(config: LoggerConfig) -> Result<(), log::SetLoggerError> {
    let mut builder = env_logger::Builder::new();
    
    // Set default log level filter
    builder.filter(None, config.default_level);
    
    // Override log levels for noisy external crates if RUST_LOG is not explicitly set
    if std::env::var("RUST_LOG").is_err() {
        builder.filter_module("datafusion", LevelFilter::Warn);
        builder.filter_module("arrow", LevelFilter::Warn);
        builder.filter_module("tokio", LevelFilter::Warn);
    } else {
        // If RUST_LOG is set, use it to configure levels
        builder.parse_default_env();
    }

    // Configure timestamp formatting
    if !config.show_timestamp {
        builder.format_timestamp(None);
    }

    // Configure target output stream
    if config.log_to_stderr {
        builder.target(env_logger::Target::Stderr);
    } else {
        builder.target(env_logger::Target::Stdout);
    }

    // Try to initialize the global logger instance
    builder.try_init()
}
