//! Storage layer modules.
//!
//! Provides RocksDB abstraction, Arrow IPC codec, metadata management,
//! and configurable storage options.

pub mod codec;
pub mod config;
pub mod engine;
pub mod metadata;

pub use codec::ArrowCodec;
pub use config::StorageConfig;
pub use engine::StorageEngine;
pub use metadata::{TableMetadata, MetadataStore};
