use arrow_array::{Int64Array, StringArray};
use fusiondb::{DataType, Database, Result, SchemaBuilder};
use std::sync::Arc;
use std::time::Instant;

fn main() -> Result<()> {
    // 1. Open the database
    let db_path = "./db_examples_large";
    let db = Database::open(db_path)?;

    // 2. Define schema & create table
    let schema = SchemaBuilder::new()
        .field("id", DataType::Int64, false)
        .primary_key()
        .field("payload", DataType::Utf8, false)
        .build()?;

    let table_name = "large_table";
    if db.table_exists(table_name) {
        db.drop_table(table_name)?;
    }
    let table = db.create_table(table_name, schema)?;

    // 3. Insert a large amount of records in batches
    let batch_size = 5000;
    let total_records = 20000;
    let num_batches = total_records / batch_size;
    let schema_ref = table.arrow_schema();

    println!("Inserting {} records in {} batches...", total_records, num_batches);
    let start_write = Instant::now();
    for b in 0..num_batches {
        let start_id = (b * batch_size) as i64;
        let ids: Vec<i64> = (start_id..(start_id + batch_size as i64)).collect();
        let payload = "X".repeat(128); // 128 bytes payload
        let payloads: Vec<String> = (0..batch_size).map(|_| payload.clone()).collect();

        let batch = arrow_array::RecordBatch::try_new(
            schema_ref.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(payloads)),
            ],
        ).unwrap();

        table.insert(batch)?;
    }
    println!("Inserted all records in {:?}", start_write.elapsed());

    // 4. Scan with projection and limit
    println!("\nScanning first 5 records with projection (only id)...");
    let start_scan = Instant::now();
    let scanned = table.scan(Some(&[0]), Some(5))?;
    println!("Scan finished in {:?}", start_scan.elapsed());
    
    for batch in scanned {
        println!("{:?}", batch);
    }

    // Clean up
    db.drop_table(table_name)?;
    let _ = std::fs::remove_dir_all(db_path);

    Ok(())
}
