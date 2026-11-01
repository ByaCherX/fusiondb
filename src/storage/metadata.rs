//! Table metadata management.
//!
//! Each table's schema, primary key, row count, and other metadata
//! are stored as JSON in the `__metadata__` column family.

use crate::core::error::{FusionDBError, Result};
use crate::core::schema::FusionSchema;
use arrow_schema::Schema;
use rocksdb::{ColumnFamily, DB};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Metadata column family name.
pub const METADATA_CF: &str = "__metadata__";

/// Metadata key prefix.
pub const META_KEY_PREFIX: &str = "table:";

/// Table metadata.
///
/// Contains all metadata for a single table, stored as JSON in RocksDB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    /// Table name
    pub name: String,
    /// Arrow schema serialized as JSON
    pub schema_json: String,
    /// Primary key column name
    pub primary_key_column: String,
    /// Total row count
    pub row_count: u64,
    /// Creation timestamp (Unix milliseconds)
    pub created_at: u64,
    /// Data column family name
    pub data_cf: String,
}

impl TableMetadata {
    /// Create new table metadata.
    pub fn new(name: &str, schema: &FusionSchema) -> Result<Self> {
        let schema_json = schema_to_json(schema.arrow_schema())?;
        let data_cf = data_cf_name(name);
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Ok(Self {
            name: name.to_string(),
            schema_json,
            primary_key_column: schema.primary_key_column().to_string(),
            row_count: 0,
            created_at,
            data_cf,
        })
    }

    /// Reconstruct a FusionSchema from stored JSON.
    pub fn to_fusion_schema(&self) -> Result<FusionSchema> {
        let schema = schema_from_json(&self.schema_json)?;
        FusionSchema::from_arrow_schema(schema)
    }

    /// Reconstruct the Arrow schema from stored JSON.
    pub fn arrow_schema(&self) -> Result<Arc<Schema>> {
        schema_from_json(&self.schema_json)
    }
}

/// Convert an Arrow Schema to JSON.
fn schema_to_json(schema: &Arc<Schema>) -> Result<String> {
    let fields: Vec<serde_json::Value> = schema
        .fields()
        .iter()
        .map(|f| {
            serde_json::json!({
                "name": f.name(),
                "data_type": format!("{:?}", f.data_type()),
                "nullable": f.is_nullable(),
            })
        })
        .collect();

    let metadata: HashMap<&str, &str> = schema
        .metadata()
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    let json = serde_json::json!({
        "fields": fields,
        "metadata": metadata,
    });

    Ok(serde_json::to_string(&json)?)
}

/// Reconstruct an Arrow Schema from JSON.
fn schema_from_json(json_str: &str) -> Result<Arc<Schema>> {
    let json: serde_json::Value = serde_json::from_str(json_str)?;

    let fields_json = json["fields"].as_array().ok_or_else(|| {
        FusionDBError::schema("Invalid schema JSON: missing 'fields' array")
    })?;

    let mut fields = Vec::new();
    for fj in fields_json {
        let name = fj["name"].as_str().ok_or_else(|| {
            FusionDBError::schema("Invalid field JSON: missing 'name'")
        })?;
        let dt_str = fj["data_type"].as_str().ok_or_else(|| {
            FusionDBError::schema("Invalid field JSON: missing 'data_type'")
        })?;
        let nullable = fj["nullable"].as_bool().unwrap_or(true);

        let data_type = parse_data_type(dt_str)?;
        fields.push(arrow_schema::Field::new(name, data_type, nullable));
    }

    let mut metadata = HashMap::new();
    if let Some(meta_obj) = json["metadata"].as_object() {
        for (k, v) in meta_obj {
            if let Some(vs) = v.as_str() {
                metadata.insert(k.clone(), vs.to_string());
            }
        }
    }

    Ok(Arc::new(Schema::new_with_metadata(fields, metadata)))
}

/// Parse a DataType from its Debug string representation.
fn parse_data_type(s: &str) -> Result<arrow_schema::DataType> {
    use arrow_schema::DataType::*;
    let dt = match s {
        "Int8" => Int8,
        "Int16" => Int16,
        "Int32" => Int32,
        "Int64" => Int64,
        "UInt8" => UInt8,
        "UInt16" => UInt16,
        "UInt32" => UInt32,
        "UInt64" => UInt64,
        "Float32" => Float32,
        "Float64" => Float64,
        "Boolean" => Boolean,
        "Utf8" => Utf8,
        "LargeUtf8" => LargeUtf8,
        "Binary" => Binary,
        "LargeBinary" => LargeBinary,
        "Date32" => Date32,
        "Date64" => Date64,
        other => {
            return Err(FusionDBError::schema(format!(
                "Unsupported data type in schema JSON: '{}'",
                other
            )))
        }
    };
    Ok(dt)
}

/// Build the data column family name for a given table.
pub fn data_cf_name(table_name: &str) -> String {
    format!("data__{}", table_name)
}

/// Build the metadata key for a given table.
pub fn meta_key(table_name: &str) -> String {
    format!("{}{}", META_KEY_PREFIX, table_name)
}

/// MetadataStore — manages table metadata via borrowed DB/CF references.
pub struct MetadataStore<'a> {
    db: &'a DB,
    cf: &'a ColumnFamily,
}

impl<'a> MetadataStore<'a> {
    pub fn new(db: &'a DB, cf: &'a ColumnFamily) -> Self {
        Self { db, cf }
    }

    /// Save table metadata.
    pub fn save(&self, metadata: &TableMetadata) -> Result<()> {
        let key = meta_key(&metadata.name);
        let json = serde_json::to_string(metadata)?;
        self.db.put_cf(self.cf, key.as_bytes(), json.as_bytes())?;
        Ok(())
    }

    /// Load table metadata.
    pub fn load(&self, table_name: &str) -> Result<Option<TableMetadata>> {
        let key = meta_key(table_name);
        match self.db.get_cf(self.cf, key.as_bytes())? {
            Some(bytes) => {
                let json = String::from_utf8(bytes.to_vec()).map_err(|e| {
                    FusionDBError::schema(format!("Invalid UTF-8 in metadata: {}", e))
                })?;
                let metadata: TableMetadata = serde_json::from_str(&json)?;
                Ok(Some(metadata))
            }
            None => Ok(None),
        }
    }

    /// Delete table metadata.
    pub fn delete(&self, table_name: &str) -> Result<()> {
        let key = meta_key(table_name);
        self.db.delete_cf(self.cf, key.as_bytes())?;
        Ok(())
    }

    /// List all table names.
    pub fn list_tables(&self) -> Result<Vec<String>> {
        let prefix = META_KEY_PREFIX.as_bytes();
        let iter = self.db.prefix_iterator_cf(self.cf, prefix);
        let mut names = Vec::new();
        for item in iter {
            let (key, _) = item?;
            let key_str = String::from_utf8(key.to_vec()).unwrap_or_default();
            if let Some(name) = key_str.strip_prefix(META_KEY_PREFIX) {
                names.push(name.to_string());
            }
        }
        Ok(names)
    }

    /// Update row count by a delta.
    pub fn update_row_count(&self, table_name: &str, delta: i64) -> Result<()> {
        if let Some(mut meta) = self.load(table_name)? {
            if delta >= 0 {
                meta.row_count = meta.row_count.saturating_add(delta as u64);
            } else {
                meta.row_count = meta.row_count.saturating_sub((-delta) as u64);
            }
            self.save(&meta)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::schema::{SchemaBuilder, PRIMARY_KEY_META};
    use arrow::datatypes::DataType;

    fn create_test_schema() -> FusionSchema {
        SchemaBuilder::new()
            .field("id", DataType::Int64, false)
            .primary_key()
            .field("name", DataType::Utf8, true)
            .build()
            .unwrap()
    }

    #[test]
    fn test_schema_json_roundtrip() {
        let schema = create_test_schema();
        let json = schema_to_json(schema.arrow_schema()).unwrap();
        let recovered = schema_from_json(&json).unwrap();
        assert_eq!(schema.arrow_schema().fields().len(), recovered.fields().len());
        assert_eq!(
            recovered.metadata().get(PRIMARY_KEY_META).unwrap(),
            "id"
        );
    }

    #[test]
    fn test_table_metadata_creation() {
        let schema = create_test_schema();
        let meta = TableMetadata::new("users", &schema).unwrap();
        assert_eq!(meta.name, "users");
        assert_eq!(meta.primary_key_column, "id");
        assert_eq!(meta.row_count, 0);
        assert_eq!(meta.data_cf, "data__users");
    }
}
