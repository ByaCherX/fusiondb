//! # FusionDB
//!
//! An embedded, serverless, and columnar database engine written in Rust.
//! FusionDB integrates:
//! - **Apache DataFusion** as its SQL parsing, planning, and execution engine.
//! - **RocksDB** as its persistent, low-latency, ordered key-value storage engine.
//! - **Apache Arrow IPC** (Stream format) as its binary serialization format for record storage.
//!
//! ## System Architecture & Data Flow
//!
//! ```text
//!  +-------------------------------------------------------------+
//!  |                      Client SQL Query                       |
//!  +-------------------------------------------------------------+
//!                                 |
//!                                 v
//!  +-------------------------------------------------------------+
//!  |              Apache DataFusion SessionContext               |
//!  |     - Parses SQL to Logical Plan                            |
//!  |     - Optimizes plan (projection/limit pushdown)            |
//!  +-------------------------------------------------------------+
//!                                 | (Logical Plan)
//!                                 v
//!  +-------------------------------------------------------------+
//!  |                     RocksDBTableProvider                    |
//!  |     - Implements TableProvider                              |
//!  |     - Logical Plan -> Physical Plan (RocksDBExec)           |
//!  +-------------------------------------------------------------+
//!                                 | (Scan / Execution)
//!                                 v
//!  +-------------------------------------------------------------+
//!  |                 RocksDBExec (Physical Plan)                 |
//!  |     - execute() returns SendableRecordBatchStream           |
//!  +-------------------------------------------------------------+
//!                                 | (Pulls from Stream)
//!                                 v
//!  +-------------------------------------------------------------+
//!  |                        RocksDBStream                        |
//!  |     - Spawns multi-threaded RocksDB reads on Column Family  |
//!  |     - Decodes Arrow IPC stream bytes to RecordBatches       |
//!  |     - Projects columns, slices rows, groups into 1024-row   |
//!  |       batches, and streams to DataFusion operators          |
//!  +-------------------------------------------------------------+
//!                                 |
//!                                 v
//!  +-------------------------------------------------------------+
//!  |        RocksDB Storage Engine (Multi-Column Family)         |
//!  |   +--------------------+     +--------------------------+   |
//!  |   |    __metadata__    |     |   data__<table_name>     |   |
//!  |   |   (Table Schemas)  |     |   (PK -> Arrow IPC row)  |   |
//!  |   +--------------------+     +--------------------------+   |
//!  +-------------------------------------------------------------+
//! ```
//!
//! ## Storage Layout
//!
//! FusionDB uses a **hybrid columnar-row layout**:
//! 1. **Key**: The table's primary key is encoded into bytes (using big-endian encoding for numeric types to preserve natural lexical sorting).
//! 2. **Value**: The corresponding row is stored as an Apache Arrow `RecordBatch` containing exactly one row, serialized using the Arrow IPC Stream format.
//!
//! This layout combines the benefits of fast point lookups (provided by RocksDB's LSM-Tree index) with efficient schema validation and vectorization compatibility (provided by Apache Arrow).
//!
//! ## Key System Integrations & Internals
//!
//! ### DataFusion Integration
//! Since DataFusion is in a fast-evolving state, FusionDB carefully wraps its components:
//! - [`RocksDBTableProvider`] implements `TableProvider` to register RocksDB tables in DataFusion's `SessionContext`.
//! - [`RocksDBExec`] implements `ExecutionPlan` to represent physical scans over RocksDB data.
//! - [`RocksDBStream`] bridges async stream processing with synchronous RocksDB scans.
//!
//! ### RocksDB Read & Write Path
//! - **Write Path**: Inserting a batch validates the schema, checks for primary key duplicates (point query via RocksDB CF), converts each row to IPC bytes, buffers mutations into a `WriteBatch`, and writes atomically to the WAL (Write-Ahead Log) and MemTable.
//! - **Read Path**: Point queries read from the active MemTable, immutable MemTables, and SST files using Bloom Filters to skip non-existent keys. DataFusion scans iterate over RocksDB database ranges.
//!
//! ## Usage Example
//!
//! ```rust,no_run
//! use fusiondb::{Database, SchemaBuilder, DataType};
//! use arrow_array::{Int64Array, StringArray};
//! use std::sync::Arc;
//!
//! fn main() -> fusiondb::Result<()> {
//!     // Open or create the database
//!     let db = Database::open("./my_data")?;
//!
//!     // Define a schema with "id" as the primary key
//!     let schema = SchemaBuilder::new()
//!         .field("id", DataType::Int64, false).primary_key()
//!         .field("name", DataType::Utf8, false)
//!         .field("age", DataType::Int64, true)
//!         .build()?;
//!
//!     // Create the table (creates a column family internally)
//!     db.create_table("users", schema)?;
//!
//!     // Insert a batch of data
//!     let table = db.table("users")?;
//!     let batch = arrow_array::RecordBatch::try_new(
//!         table.arrow_schema(),
//!         vec![
//!             Arc::new(Int64Array::from(vec![1, 2, 3])),
//!             Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
//!             Arc::new(Int64Array::from(vec![30, 25, 35])),
//!         ],
//!     ).unwrap();
//!     table.insert(batch)?;
//!
//!     // Query the data via SQL
//!     let results = db.query("SELECT name, age FROM users WHERE age > 28")?;
//!     for batch in results {
//!         println!("{:?}", batch);
//!     }
//!
//!     Ok(())
//! }
//! ```

// Module declarations
pub mod core;
pub mod query;
pub mod storage;
pub mod utils;

// --- Backward-compatible public re-exports ---
// These ensure that existing code like `fusiondb::Database` still works
// after the modular reorganization.

// Core types
pub use core::database::Database;
pub use core::error::{FusionDBError, Result};
pub use core::schema::{FusionSchema, SchemaBuilder};
pub use core::table::Table;

// Utilities
pub use utils::logger::{init_logger, LoggerConfig};

// Arrow re-exports (so users don't need to add arrow crates separately)
pub use arrow::datatypes::DataType;
pub use arrow_array::RecordBatch;
pub use arrow_schema::Schema as ArrowSchema;
