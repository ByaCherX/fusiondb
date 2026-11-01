use arrow_array::{Int64Array, StringArray};
use fusiondb::{DataType, Database, Result, SchemaBuilder};
use std::sync::Arc;
use std::thread;

fn main() -> Result<()> {
    // 1. Open the database
    let db_path = "./db_examples_concurrency";
    // We wrap Database in Arc to share it safely across multiple threads
    let db = Arc::new(Database::open(db_path)?);

    // 2. Create schema & table
    let schema = SchemaBuilder::new()
        .field("id", DataType::Int64, false)
        .primary_key()
        .field("message", DataType::Utf8, false)
        .build()?;

    let table_name = "shared_table";
    if db.table_exists(table_name) {
        db.drop_table(table_name)?;
    }
    db.create_table(table_name, schema)?;

    // 3. Spawn writer thread
    let db_writer = Arc::clone(&db);
    let writer_handle = thread::spawn(move || -> Result<()> {
        let table = db_writer.table("shared_table")?;
        let batch = arrow_array::RecordBatch::try_new(
            table.arrow_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Msg 1", "Msg 2", "Msg 3"])),
            ],
        ).unwrap();
        table.insert(batch)?;
        println!("[Writer] Inserted 3 rows.");
        Ok(())
    });

    // Wait for the writer thread to finish writing
    writer_handle.join().unwrap()?;

    // 4. Spawn multiple concurrent reader threads
    let mut reader_handles = vec![];
    for i in 1..=3 {
        let db_reader = Arc::clone(&db);
        let handle = thread::spawn(move || -> Result<()> {
            let table = db_reader.table("shared_table")?;
            // Read record matching the thread index
            let row = table.get(i as i64)?;
            println!("[Reader {}] Read row: {:?}", i, row);
            Ok(())
        });
        reader_handles.push(handle);
    }

    // Wait for all reader threads to complete
    for handle in reader_handles {
        handle.join().unwrap()?;
    }

    // Clean up
    db.drop_table(table_name)?;
    let _ = std::fs::remove_dir_all(db_path);

    Ok(())
}
