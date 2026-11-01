//! FusionDB error types.
//!
//! Type-safe error handling covering all possible error conditions.

use thiserror::Error;

/// Primary error type for FusionDB.
#[derive(Debug, Error)]
pub enum FusionDBError {
    /// RocksDB storage layer errors
    #[error("Storage error: {0}")]
    Storage(#[from] rocksdb::Error),

    /// Arrow IPC codec errors
    #[error("Serialization error: {0}")]
    Serialization(#[from] arrow::error::ArrowError),

    /// DataFusion query errors
    #[error("Query error: {0}")]
    Query(#[from] datafusion::error::DataFusionError),

    /// JSON serde errors (metadata)
    #[error("Metadata serialization error: {0}")]
    MetadataSerde(#[from] serde_json::Error),

    /// Invalid schema definition
    #[error("Schema error: {message}")]
    Schema { message: String },

    /// Table not found
    #[error("Table not found: '{name}'")]
    TableNotFound { name: String },

    /// Table already exists
    #[error("Table already exists: '{name}'")]
    TableAlreadyExists { name: String },

    /// Primary key violation (duplicate)
    #[error("Primary key violation: key '{key}' already exists in table '{table}'")]
    PrimaryKeyViolation { key: String, table: String },

    /// Primary key column not found in schema
    #[error("Primary key column not found in schema")]
    PrimaryKeyNotFound,

    /// Row not found for a given primary key
    #[error("Row not found for primary key: {key}")]
    RowNotFound { key: String },

    /// Invalid operation
    #[error("Invalid operation: {message}")]
    InvalidOperation { message: String },

    /// IO errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// FusionDB result type alias.
pub type Result<T> = std::result::Result<T, FusionDBError>;

impl FusionDBError {
    /// Create a schema error.
    pub fn schema(message: impl Into<String>) -> Self {
        FusionDBError::Schema {
            message: message.into(),
        }
    }

    /// Create a table-not-found error.
    pub fn table_not_found(name: impl Into<String>) -> Self {
        FusionDBError::TableNotFound { name: name.into() }
    }

    /// Create a table-already-exists error.
    pub fn table_already_exists(name: impl Into<String>) -> Self {
        FusionDBError::TableAlreadyExists { name: name.into() }
    }

    /// Create a primary key violation error.
    pub fn pk_violation(key: impl Into<String>, table: impl Into<String>) -> Self {
        FusionDBError::PrimaryKeyViolation {
            key: key.into(),
            table: table.into(),
        }
    }

    /// Create a row-not-found error.
    pub fn row_not_found(key: impl Into<String>) -> Self {
        FusionDBError::RowNotFound { key: key.into() }
    }

    /// Create an invalid-operation error.
    pub fn invalid_op(message: impl Into<String>) -> Self {
        FusionDBError::InvalidOperation {
            message: message.into(),
        }
    }
}
