//! Schema definitions and builder utilities.
//!
//! FusionDB uses Arrow's metadata mechanism to store primary key information
//! on top of an Arrow `Schema`.

use crate::core::error::{FusionDBError, Result};
use arrow::datatypes::{DataType, Field, Schema};
use std::collections::HashMap;
use std::sync::Arc;

/// Arrow schema metadata key for the primary key column name.
pub const PRIMARY_KEY_META: &str = "fusiondb.primary_key";
/// Arrow field metadata value indicating a primary key field.
pub const PK_FIELD_META: &str = "fusiondb.is_primary_key";

/// FusionDB schema wrapper.
///
/// Wraps an Arrow `Schema` and adds primary key semantics via metadata.
#[derive(Debug, Clone)]
pub struct FusionSchema {
    /// Inner Arrow schema
    inner: Arc<Schema>,
    /// Primary key column name
    primary_key_column: String,
    /// Primary key column index
    primary_key_index: usize,
}

impl FusionSchema {
    /// Construct a `FusionSchema` from an existing Arrow schema.
    ///
    /// The schema must contain `fusiondb.primary_key` in its metadata.
    pub fn from_arrow_schema(schema: Arc<Schema>) -> Result<Self> {
        let pk_col = schema
            .metadata()
            .get(PRIMARY_KEY_META)
            .ok_or(FusionDBError::PrimaryKeyNotFound)?
            .clone();

        let pk_index = schema
            .fields()
            .iter()
            .position(|f| f.name() == &pk_col)
            .ok_or(FusionDBError::PrimaryKeyNotFound)?;

        Ok(Self {
            inner: schema,
            primary_key_column: pk_col,
            primary_key_index: pk_index,
        })
    }

    /// Returns a reference to the inner Arrow schema.
    pub fn arrow_schema(&self) -> &Arc<Schema> {
        &self.inner
    }

    /// Returns the primary key column name.
    pub fn primary_key_column(&self) -> &str {
        &self.primary_key_column
    }

    /// Returns the primary key column index.
    pub fn primary_key_index(&self) -> usize {
        self.primary_key_index
    }

    /// Returns the data type of the primary key column.
    pub fn primary_key_type(&self) -> &DataType {
        self.inner.field(self.primary_key_index).data_type()
    }
}

/// Builder for constructing a [`FusionSchema`].
///
/// # Example
/// ```rust
/// use fusiondb::{SchemaBuilder, DataType};
///
/// let schema = SchemaBuilder::new()
///     .field("id", DataType::Int64, false).primary_key()
///     .field("name", DataType::Utf8, false)
///     .field("age", DataType::Int64, true)
///     .build()
///     .unwrap();
/// ```
pub struct SchemaBuilder {
    fields: Vec<Field>,
    primary_key: Option<String>,
    pending_field: Option<PendingField>,
}

struct PendingField {
    name: String,
    data_type: DataType,
    nullable: bool,
}

impl SchemaBuilder {
    /// Create a new empty `SchemaBuilder`.
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
            primary_key: None,
            pending_field: None,
        }
    }

    /// Flush the pending field into the fields list.
    fn flush_pending(&mut self) {
        if let Some(pf) = self.pending_field.take() {
            self.fields.push(Field::new(pf.name, pf.data_type, pf.nullable));
        }
    }

    /// Add a new field to the schema.
    pub fn field(mut self, name: &str, data_type: DataType, nullable: bool) -> Self {
        self.flush_pending();
        self.pending_field = Some(PendingField {
            name: name.to_string(),
            data_type,
            nullable,
        });
        self
    }

    /// Mark the most recently added field as the primary key.
    pub fn primary_key(mut self) -> Self {
        if let Some(ref pf) = self.pending_field {
            self.primary_key = Some(pf.name.clone());
        }
        self
    }

    /// Build and validate the schema.
    pub fn build(mut self) -> Result<FusionSchema> {
        // Flush any pending field
        self.flush_pending();

        // Primary key validation
        let pk_col = self.primary_key.ok_or_else(|| {
            FusionDBError::schema("Schema must have exactly one primary key column. Use .primary_key() after .field()")
        })?;

        if self.fields.is_empty() {
            return Err(FusionDBError::schema("Schema must have at least one field"));
        }

        // Verify the primary key column exists
        let pk_index = self
            .fields
            .iter()
            .position(|f| f.name() == &pk_col)
            .ok_or_else(|| FusionDBError::schema(format!("Primary key column '{}' not found in fields", pk_col)))?;

        // Primary key must not be nullable
        if self.fields[pk_index].is_nullable() {
            return Err(FusionDBError::schema(format!(
                "Primary key column '{}' cannot be nullable",
                pk_col
            )));
        }

        // Validate primary key data type is supported
        match self.fields[pk_index].data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::LargeUtf8 => {}
            dt => {
                return Err(FusionDBError::schema(format!(
                    "Primary key type '{}' is not supported. Supported types: Int8-64, UInt8-64, Utf8, LargeUtf8",
                    dt
                )));
            }
        }

        // Store PK info in Arrow schema metadata
        let mut metadata = HashMap::new();
        metadata.insert(PRIMARY_KEY_META.to_string(), pk_col.clone());

        let schema = Arc::new(Schema::new_with_metadata(self.fields, metadata));

        Ok(FusionSchema {
            inner: schema,
            primary_key_column: pk_col,
            primary_key_index: pk_index,
        })
    }
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}
