//! Core module — contains the primary database and table abstractions.
//!
//! This module provides the main entry points for interacting with FusionDB:
//! - [`Database`] — database lifecycle, table management, and SQL query execution
//! - [`Table`] — CRUD operations on individual tables
//! - [`FusionSchema`] / [`SchemaBuilder`] — schema definition with primary key semantics
//! - [`FusionDBError`] / [`Result`] — error types

pub mod database;
pub mod error;
pub mod schema;
pub mod table;

pub use database::Database;
pub use error::{FusionDBError, Result};
pub use schema::{FusionSchema, SchemaBuilder};
pub use table::Table;
