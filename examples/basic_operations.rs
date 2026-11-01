use arrow_array::{Int64Array, StringArray};
use fusiondb::{DataType, Database, Result, SchemaBuilder};
use std::sync::Arc;

fn main() -> Result<()> {
    // 1. Open or create the database at the specified path
    let db_path = "./db_examples_basic";
    let db = Database::open(db_path)?;
    println!("Database opened successfully at: {}", db_path);

    // 2. Define the schema for the table
    // A table must have exactly one non-nullable primary key column.
    let schema = SchemaBuilder::new()
        .field("id", DataType::Int64, false)
        .primary_key()
        .field("name", DataType::Utf8, false)
        .field("age", DataType::Int64, true)
        .build()?;

    // 3. Create the table (drops existing if it already exists in cache/metadata)
    let table_name = "users";
    if db.table_exists(table_name) {
        db.drop_table(table_name)?;
    }
    let table = db.create_table(table_name, schema)?;
    println!("Table '{}' created successfully.", table_name);

    // 4. Prepare data and insert into the table
    let batch = arrow_array::RecordBatch::try_new(
        table.arrow_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Int64Array::from(vec![Some(30), Some(25), None])),
        ],
    ).unwrap();

    table.insert(batch)?;
    println!("Inserted 3 records.");

    // 5. Get a specific record by primary key
    let row = table.get(1i64)?;
    println!("Retrieved record (ID = 1): {:?}", row);

    // 6. Scan the entire table
    let scanned_batches = table.scan(None, None)?;
    println!("Scanned table contents:");
    for batch in scanned_batches {
        println!("{:?}", batch);
    }

    // Clean up: drop the table and delete the directory if needed
    db.drop_table(table_name)?;
    let _ = std::fs::remove_dir_all(db_path);

    Ok(())
}
