use arrow_array::{Int64Array, StringArray, UInt32Array};
use fusiondb::{DataType, Database, Result, SchemaBuilder};
use std::sync::Arc;

fn main() -> Result<()> {
    // 1. Open the database
    let db_path = "./db_examples_schema_evolution";
    let db = Database::open(db_path)?;

    // 2. Build schema with mixed, nullable and non-nullable data types
    let schema = SchemaBuilder::new()
        .field("id", DataType::Int64, false) // non-nullable PK
        .primary_key()
        .field("name", DataType::Utf8, false) // non-nullable String
        .field("score", DataType::UInt32, true) // nullable unsigned 32-bit int
        .field("notes", DataType::LargeUtf8, true) // nullable large string
        .build()?;

    let table_name = "evolved_table";
    if db.table_exists(table_name) {
        db.drop_table(table_name)?;
    }
    let table = db.create_table(table_name, schema)?;

    // 3. Insert records containing null values
    let batch = arrow_array::RecordBatch::try_new(
        table.arrow_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(UInt32Array::from(vec![Some(100), None, Some(95)])),
            Arc::new(StringArray::from(vec![Some("Notes for Alice"), Some("Notes for Bob"), None])),
        ],
    ).unwrap();

    table.insert(batch)?;
    println!("Inserted records with optional (nullable) fields successfully.");

    // 4. Retrieve and verify null value handles
    let row_bob = table.get(2i64)?;
    println!("Bob's record (contains null score): {:?}", row_bob);

    let row_charlie = table.get(3i64)?;
    println!("Charlie's record (contains null notes): {:?}", row_charlie);

    // Clean up
    db.drop_table(table_name)?;
    let _ = std::fs::remove_dir_all(db_path);

    Ok(())
}
