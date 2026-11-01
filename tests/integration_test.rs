//! Integration Tests
//!
//! This module contains end-to-end integration tests for FusionDB.
//! It verifies the core operations of the database including:
//! - Table creation and deletion (lifecycle management)
//! - Single and multi-batch row insertion
//! - Schema validation and constraint checks (primary keys, nullability)
//! - Single-row lookups (GET queries) and bulk scans (limit, projection)
//! - Data persistence across database reopens
//! - SQL query execution using the DataFusion integration

use arrow::datatypes::DataType;
use arrow_array::{Int64Array, StringArray};
use fusiondb::{Database, SchemaBuilder};
use std::sync::Arc;
use tempfile::TempDir;

/// Creates a temporary database instance for integration tests.
///
/// This helper function generates a unique temporary directory using `TempDir`
/// and initializes a `Database` instance in that directory. The temporary directory is returned
/// along with the database handle so that it remains alive for the duration of the test and
/// is automatically cleaned up when the test finishes.
fn create_test_db() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let db = Database::open(dir.path().to_str().unwrap()).unwrap();
    (dir, db)
}

/// Returns the Arrow schema definition for the "users" table used in tests.
///
/// The schema contains three fields:
/// - `id` (Int64, non-nullable, designated as the primary key)
/// - `name` (Utf8, non-nullable)
/// - `age` (Int64, nullable)
fn users_schema() -> fusiondb::FusionSchema {
    SchemaBuilder::new()
        .field("id", DataType::Int64, false)
        .primary_key()
        .field("name", DataType::Utf8, false)
        .field("age", DataType::Int64, true)
        .build()
        .unwrap()
}

/// Helper to construct a `RecordBatch` containing test data for the "users" table.
///
/// Converts vectors of IDs, names, and ages into their respective Arrow arrays
/// and packages them into a `RecordBatch` matching the provided schema.
fn users_batch(
    schema: arrow_schema::SchemaRef,
    ids: Vec<i64>,
    names: Vec<&str>,
    ages: Vec<Option<i64>>,
) -> arrow_array::RecordBatch {
    arrow_array::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(ages)),
        ],
    )
    .unwrap()
}

// ============================================================
// TABLE MANAGEMENT TESTS
// ============================================================

#[test]
fn test_create_table() {
    let (_dir, db) = create_test_db();
    let schema = users_schema();
    let table = db.create_table("users", schema).unwrap();

    assert_eq!(table.name(), "users");
    assert!(db.table_exists("users"));
}

#[test]
fn test_create_duplicate_table_fails() {
    let (_dir, db) = create_test_db();
    db.create_table("users", users_schema()).unwrap();
    let result = db.create_table("users", users_schema());
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, fusiondb::FusionDBError::TableAlreadyExists { .. }));
}

#[test]
fn test_get_nonexistent_table_fails() {
    let (_dir, db) = create_test_db();
    let result = db.table("nonexistent");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), fusiondb::FusionDBError::TableNotFound { .. }));
}

#[test]
fn test_list_tables() {
    let (_dir, db) = create_test_db();
    assert!(db.list_tables().unwrap().is_empty());

    db.create_table("alpha", users_schema()).unwrap();
    db.create_table("beta", users_schema()).unwrap();

    let tables = db.list_tables().unwrap();
    assert_eq!(tables.len(), 2);
    assert!(tables.contains(&"alpha".to_string()));
    assert!(tables.contains(&"beta".to_string()));
}

#[test]
fn test_drop_table() {
    let (_dir, db) = create_test_db();
    db.create_table("users", users_schema()).unwrap();
    assert!(db.table_exists("users"));

    db.drop_table("users").unwrap();
    assert!(!db.table_exists("users"));
}

#[test]
fn test_drop_nonexistent_table_fails() {
    let (_dir, db) = create_test_db();
    let result = db.drop_table("ghost");
    assert!(result.is_err());
}

// ============================================================
// INSERT TESTS
// ============================================================

#[test]
fn test_insert_single_batch() {
    let (_dir, db) = create_test_db();
    let table = db.create_table("users", users_schema()).unwrap();
    let schema = table.arrow_schema();

    let batch = users_batch(
        schema,
        vec![1, 2, 3],
        vec!["Alice", "Bob", "Charlie"],
        vec![Some(30), Some(25), None],
    );

    table.insert(batch).unwrap();
    assert_eq!(table.count().unwrap(), 3);
}

#[test]
fn test_insert_multiple_batches() {
    let (_dir, db) = create_test_db();
    let table = db.create_table("users", users_schema()).unwrap();
    let schema = table.arrow_schema();

    let batch1 = users_batch(
        schema.clone(),
        vec![1, 2],
        vec!["Alice", "Bob"],
        vec![Some(30), Some(25)],
    );
    let batch2 = users_batch(
        schema,
        vec![3, 4],
        vec!["Charlie", "Dave"],
        vec![None, Some(40)],
    );

    table.insert(batch1).unwrap();
    table.insert(batch2).unwrap();
    assert_eq!(table.count().unwrap(), 4);
}

#[test]
fn test_insert_duplicate_pk_fails() {
    let (_dir, db) = create_test_db();
    let table = db.create_table("users", users_schema()).unwrap();
    let schema = table.arrow_schema();

    let batch1 = users_batch(schema.clone(), vec![1], vec!["Alice"], vec![Some(30)]);
    let batch2 = users_batch(schema, vec![1], vec!["Bob"], vec![Some(25)]); // Same ID (primary key conflict)

    table.insert(batch1).unwrap();
    let result = table.insert(batch2);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        fusiondb::FusionDBError::PrimaryKeyViolation { .. }
    ));
}

// ============================================================
// GET TESTS
// ============================================================

#[test]
fn test_get_by_pk() {
    let (_dir, db) = create_test_db();
    let table = db.create_table("users", users_schema()).unwrap();
    let schema = table.arrow_schema();

    let batch = users_batch(
        schema,
        vec![1, 2, 3],
        vec!["Alice", "Bob", "Charlie"],
        vec![Some(30), Some(25), None],
    );
    table.insert(batch).unwrap();

    // Retrieve the record with ID = 2
    let result = table.get(2i64).unwrap();
    assert_eq!(result.num_rows(), 1);

    let id_col = result
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id_col.value(0), 2);

    let name_col = result
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name_col.value(0), "Bob");
}

#[test]
fn test_get_nonexistent_pk_fails() {
    let (_dir, db) = create_test_db();
    let table = db.create_table("users", users_schema()).unwrap();
    let schema = table.arrow_schema();

    let batch = users_batch(schema, vec![1], vec!["Alice"], vec![Some(30)]);
    table.insert(batch).unwrap();

    let result = table.get(999i64);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        fusiondb::FusionDBError::RowNotFound { .. }
    ));
}

// ============================================================
// DELETE TESTS
// ============================================================

#[test]
fn test_delete() {
    let (_dir, db) = create_test_db();
    let table = db.create_table("users", users_schema()).unwrap();
    let schema = table.arrow_schema();

    let batch = users_batch(
        schema,
        vec![1, 2, 3],
        vec!["Alice", "Bob", "Charlie"],
        vec![Some(30), Some(25), None],
    );
    table.insert(batch).unwrap();

    table.delete(2i64).unwrap();
    assert_eq!(table.count().unwrap(), 2);

    // Verify that the deleted row no longer exists in the table
    assert!(table.get(2i64).is_err());
}

#[test]
fn test_delete_nonexistent_fails() {
    let (_dir, db) = create_test_db();
    let table = db.create_table("users", users_schema()).unwrap();

    let result = table.delete(999i64);
    assert!(result.is_err());
}

// ============================================================
// UPDATE TESTS
// ============================================================

#[test]
fn test_update() {
    let (_dir, db) = create_test_db();
    let table = db.create_table("users", users_schema()).unwrap();
    let schema = table.arrow_schema();

    // Insert initial test data
    let batch = users_batch(schema.clone(), vec![1], vec!["Alice"], vec![Some(30)]);
    table.insert(batch).unwrap();

    // Update the existing record
    let updated = users_batch(schema, vec![1], vec!["Alice Smith"], vec![Some(31)]);
    table.update(1i64, updated).unwrap();

    // Verify the update took effect
    let result = table.get(1i64).unwrap();
    let name_col = result
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name_col.value(0), "Alice Smith");

    let age_col = result
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(age_col.value(0), 31);
}

// ============================================================
// SCAN TESTS
// ============================================================

#[test]
fn test_scan_all() {
    let (_dir, db) = create_test_db();
    let table = db.create_table("users", users_schema()).unwrap();
    let schema = table.arrow_schema();

    let batch = users_batch(
        schema,
        vec![1, 2, 3],
        vec!["Alice", "Bob", "Charlie"],
        vec![Some(30), Some(25), None],
    );
    table.insert(batch).unwrap();

    let batches = table.scan(None, None).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[test]
fn test_scan_with_limit() {
    let (_dir, db) = create_test_db();
    let table = db.create_table("users", users_schema()).unwrap();
    let schema = table.arrow_schema();

    let batch = users_batch(
        schema,
        vec![1, 2, 3, 4, 5],
        vec!["A", "B", "C", "D", "E"],
        vec![None, None, None, None, None],
    );
    table.insert(batch).unwrap();

    let batches = table.scan(None, Some(3)).unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[test]
fn test_scan_with_projection() {
    let (_dir, db) = create_test_db();
    let table = db.create_table("users", users_schema()).unwrap();
    let schema = table.arrow_schema();

    let batch = users_batch(
        schema,
        vec![1, 2],
        vec!["Alice", "Bob"],
        vec![Some(30), Some(25)],
    );
    table.insert(batch).unwrap();

    // Retrieve only the "name" column (index 1)
    let batches = table.scan(Some(&[1]), None).unwrap();
    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(batches[0].schema().field(0).name(), "name");
}

#[test]
fn test_scan_empty_table() {
    let (_dir, db) = create_test_db();
    let table = db.create_table("users", users_schema()).unwrap();

    let batches = table.scan(None, None).unwrap();
    assert!(batches.is_empty());
}

// ============================================================
// PERSISTENCE TESTS
// ============================================================

#[test]
fn test_data_persists_across_reopens() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    // Open the database, create the table, and insert test data
    {
        let db = Database::open(&path).unwrap();
        let table = db.create_table("users", users_schema()).unwrap();
        let schema = table.arrow_schema();

        let batch = users_batch(
            schema,
            vec![1, 2, 3],
            vec!["Alice", "Bob", "Charlie"],
            vec![Some(30), Some(25), None],
        );
        table.insert(batch).unwrap();
    } // db burada drop edilir

    // Reopen the database and verify that the data persists
    {
        let db = Database::open(&path).unwrap();
        assert!(db.table_exists("users"));

        let table = db.table("users").unwrap();
        assert_eq!(table.count().unwrap(), 3);

        let row = table.get(2i64).unwrap();
        let name_col = row
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "Bob");
    }
}

// ============================================================
// SQL QUERY TESTS (DataFusion)
// ============================================================

#[test]
fn test_sql_select_all() {
    let (_dir, db) = create_test_db();
    let table = db.create_table("users", users_schema()).unwrap();
    let schema = table.arrow_schema();

    let batch = users_batch(
        schema,
        vec![1, 2, 3],
        vec!["Alice", "Bob", "Charlie"],
        vec![Some(30), Some(25), Some(35)],
    );
    table.insert(batch).unwrap();

    let results = db.query("SELECT * FROM users").unwrap();
    let total: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3);
}

#[test]
fn test_sql_select_with_filter() {
    let (_dir, db) = create_test_db();
    let table = db.create_table("users", users_schema()).unwrap();
    let schema = table.arrow_schema();

    let batch = users_batch(
        schema,
        vec![1, 2, 3],
        vec!["Alice", "Bob", "Charlie"],
        vec![Some(30), Some(25), Some(35)],
    );
    table.insert(batch).unwrap();

    let results = db
        .query("SELECT name FROM users WHERE age > 28")
        .unwrap();
    let total: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2); // Alice (30) and Charlie (35)
}

#[test]
fn test_sql_count() {
    let (_dir, db) = create_test_db();
    let table = db.create_table("users", users_schema()).unwrap();
    let schema = table.arrow_schema();

    let batch = users_batch(
        schema,
        vec![1, 2, 3],
        vec!["Alice", "Bob", "Charlie"],
        vec![Some(30), Some(25), Some(35)],
    );
    table.insert(batch).unwrap();

    let results = db.query("SELECT COUNT(*) FROM users").unwrap();
    assert!(!results.is_empty());
}

// ============================================================
// SCHEMA TESTS
// ============================================================

#[test]
fn test_schema_missing_pk_fails() {
    let result = SchemaBuilder::new()
        .field("id", DataType::Int64, false)
        .field("name", DataType::Utf8, false)
        .build(); // primary_key() was not called (should error)

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), fusiondb::FusionDBError::Schema { .. }));
}

#[test]
fn test_schema_nullable_pk_fails() {
    let result = SchemaBuilder::new()
        .field("id", DataType::Int64, true) // nullable PK
        .primary_key()
        .build();

    assert!(result.is_err());
}

#[test]
fn test_string_primary_key() {
    let (_dir, db) = create_test_db();
    let schema = SchemaBuilder::new()
        .field("username", DataType::Utf8, false)
        .primary_key()
        .field("email", DataType::Utf8, true)
        .build()
        .unwrap();

    let table = db.create_table("accounts", schema).unwrap();
    let arrow_schema = table.arrow_schema();

    let batch = arrow_array::RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob"])),
            Arc::new(StringArray::from(vec![
                Some("alice@example.com"),
                Some("bob@example.com"),
            ])),
        ],
    )
    .unwrap();

    table.insert(batch).unwrap();
    assert_eq!(table.count().unwrap(), 2);

    let row = table.get("alice").unwrap();
    assert_eq!(row.num_rows(), 1);
}

#[test]
fn test_open_with_config() {
    use fusiondb::storage::StorageConfig;

    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();

    let config = StorageConfig::default()
        .enable_compression(true);

    let db = Database::open_with_config(path, config).unwrap();

    // Verify database functions normally
    let schema = users_schema();
    let table = db.create_table("users", schema).unwrap();
    assert_eq!(table.name(), "users");
}

#[test]
fn test_insert_duplicate_pk_within_batch_fails() {
    let (_dir, db) = create_test_db();
    let table = db.create_table("users", users_schema()).unwrap();
    let schema = table.arrow_schema();

    // Both rows share id 1 — the second must not silently overwrite the first
    let batch = users_batch(schema, vec![1, 1], vec!["Alice", "Bob"], vec![Some(30), Some(25)]);

    let err = table.insert(batch).unwrap_err();
    assert!(matches!(err, fusiondb::FusionDBError::PrimaryKeyViolation { .. }));
    assert_eq!(table.count().unwrap(), 0);
    assert!(table.get(1i64).is_err());
}

#[test]
fn test_concurrent_inserts_keep_row_count_consistent() {
    let (_dir, db) = create_test_db();
    let table = db.create_table("users", users_schema()).unwrap();
    let schema = table.arrow_schema();

    let handles: Vec<_> = (0..8i64)
        .map(|t| {
            let table = table.clone();
            let schema = schema.clone();
            std::thread::spawn(move || {
                for i in 0..100i64 {
                    let id = t * 1000 + i;
                    let batch = users_batch(schema.clone(), vec![id], vec!["x"], vec![Some(1)]);
                    table.insert(batch).unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(table.count().unwrap(), 800);
}

#[test]
fn test_concurrent_insert_same_pk_only_one_wins() {
    let (_dir, db) = create_test_db();
    let table = db.create_table("users", users_schema()).unwrap();
    let schema = table.arrow_schema();

    let handles: Vec<_> = (0..8)
        .map(|_| {
            let table = table.clone();
            let schema = schema.clone();
            std::thread::spawn(move || {
                let batch = users_batch(schema, vec![42], vec!["x"], vec![Some(1)]);
                table.insert(batch).is_ok()
            })
        })
        .collect();
    let wins = handles.into_iter().filter_map(|h| h.join().ok()).filter(|ok| *ok).count();

    assert_eq!(wins, 1);
    assert_eq!(table.count().unwrap(), 1);
}
