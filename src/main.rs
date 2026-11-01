//! FusionDB CLI — binary entry point.
//!
//! Parses command-line arguments and dispatches to the appropriate handler.

mod cli;
mod repl;

use clap::Parser;
use cli::Cli;

fn main() {
    // Initialize logger (suppress noisy crates by default)
    let _ = fusiondb::init_logger(
        fusiondb::LoggerConfig::new(log::LevelFilter::Warn)
            .show_timestamp(false)
            .log_to_stderr(true),
    );

    let cli = Cli::parse();
    cli.execute();
}
