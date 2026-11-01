//! Demonstration of FusionDB logging configuration and output.
//!
//! This example shows how to configure the logger, initialize it,
//! and see the log output for various database operations.

use arrow_array::{Int64Array, StringArray};
use fusiondb::{init_logger, DataType, Database, LoggerConfig, Result, SchemaBuilder};
use log::LevelFilter;
use std::sync::Arc;

fn main() -> Result<()> {
    // 1. Initialize the FusionDB logger with DEBUG level to see everything,
    // including RocksDB callback logs and latency details.
    let config = LoggerConfig::new(LevelFilter::Debug)
        .show_timestamp(true)
        .log_to_stderr(false);
    
    if let Err(e) = init_logger(config) {
        eprintln!("Failed to initialize logger: {:?}", e);
    }

    log::info!("Starting logging demonstration...");

    // 2. Open a temporary database
    let db_path = "./db_logging_demo";
    let db = Database::open(db_path)?;

    // 3. Define schema & create a table
    let schema = SchemaBuilder::new()
        .field("id", DataType::Int64, false)
        .primary_key()
        .field("name", DataType::Utf8, false)
        .build()?;

    let table_name = "demo_table";
    if db.table_exists(table_name) {
        db.drop_table(table_name)?;
    }
    let table = db.create_table(table_name, schema)?;

    // 4. Insert data in batch
    log::info!("Inserting batch records...");
    let batch = arrow_array::RecordBatch::try_new(
        table.arrow_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    ).unwrap();
    table.insert(batch)?;

    // 5. Query using SQL
    log::info!("Running SQL query...");
    let query_results = db.query("SELECT * FROM demo_table WHERE id >= 2")?;
    for batch in query_results {
        log::debug!("Query Row count: {}", batch.num_rows());
    }

    // 6. Update and delete
    log::info!("Updating and deleting individual rows...");
    let update_batch = arrow_array::RecordBatch::try_new(
        table.arrow_schema(),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["Bobby"])),
        ],
    ).unwrap();
    table.update(2i64, update_batch)?;
    
    table.delete(1i64)?;

    // 7. Get stats string from RocksDB if enabled
    if let Some(stats) = db.get_statistics() {
        log::info!("RocksDB Internal Statistics:\n{}", stats);
    } else {
        log::warn!("RocksDB statistics are not enabled or collected.");
    }

    // 8. Clean up
    log::info!("Cleaning up database files...");
    db.drop_table(table_name)?;
    let _ = std::fs::remove_dir_all(db_path);

    log::info!("Logging demonstration finished successfully.");
    Ok(())
}
