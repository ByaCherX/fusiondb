use arrow_array::{Int64Array, StringArray};
use fusiondb::{DataType, Database, Result, SchemaBuilder};
use std::sync::Arc;

fn main() -> Result<()> {
    // 1. Open the database
    let db_path = "./db_examples_update_delete";
    let db = Database::open(db_path)?;

    // 2. Define schema & create table
    let schema = SchemaBuilder::new()
        .field("id", DataType::Int64, false)
        .primary_key()
        .field("name", DataType::Utf8, false)
        .field("status", DataType::Utf8, false)
        .build()?;

    let table_name = "tasks";
    if db.table_exists(table_name) {
        db.drop_table(table_name)?;
    }
    let table = db.create_table(table_name, schema)?;

    // 3. Insert initial tasks
    let batch = arrow_array::RecordBatch::try_new(
        table.arrow_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Task 1", "Task 2"])),
            Arc::new(StringArray::from(vec!["Pending", "Pending"])),
        ],
    ).unwrap();
    table.insert(batch)?;
    println!("Initial tasks inserted.");

    // 4. Update a task (ID = 1) status to 'Completed'
    let update_batch = arrow_array::RecordBatch::try_new(
        table.arrow_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["Task 1"])),
            Arc::new(StringArray::from(vec!["Completed"])),
        ],
    ).unwrap();
    table.update(1i64, update_batch)?;
    
    let updated_row = table.get(1i64)?;
    println!("Updated record (ID = 1): {:?}", updated_row);

    // 5. Delete a task (ID = 2)
    table.delete(2i64)?;
    println!("Deleted record with ID = 2.");

    // Verify row count is now 1
    println!("Final task count: {}", table.count()?);

    // Clean up
    db.drop_table(table_name)?;
    let _ = std::fs::remove_dir_all(db_path);

    Ok(())
}
