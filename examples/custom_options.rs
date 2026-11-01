use arrow_array::{Int64Array, StringArray};
use fusiondb::{DataType, Database, Result, SchemaBuilder};
use std::sync::Arc;

fn main() -> Result<()> {
    // 1. Define custom RocksDB options
    let mut db_opts = rocksdb::Options::default();
    db_opts.create_if_missing(true);
    db_opts.create_missing_column_families(true);

    // Set custom memtable & block cache parameters
    db_opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB memtable
    db_opts.set_max_write_buffer_number(3);

    let mut block_opts = rocksdb::BlockBasedOptions::default();
    let cache = rocksdb::Cache::new_lru_cache(128 * 1024 * 1024); // 128MB block cache
    block_opts.set_block_cache(&cache);
    block_opts.set_bloom_filter(10.0, false); // Enable bloom filter (10 bits/key)
    block_opts.set_cache_index_and_filter_blocks(true); // Cache indices and filters
    db_opts.set_block_based_table_factory(&block_opts);

    // 2. Open the Database with custom options
    let db_path = "./db_examples_custom_opts";
    let db = Database::open_with_opts(db_path, db_opts)?;
    println!("Database opened with custom RocksDB options.");

    // 3. Create a schema and table
    let schema = SchemaBuilder::new()
        .field("id", DataType::Int64, false)
        .primary_key()
        .field("value", DataType::Utf8, false)
        .build()?;

    let table_name = "custom_table";
    if db.table_exists(table_name) {
        db.drop_table(table_name)?;
    }
    let table = db.create_table(table_name, schema)?;

    // 4. Insert data
    let batch = arrow_array::RecordBatch::try_new(
        table.arrow_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["A", "B", "C"])),
        ],
    ).unwrap();
    table.insert(batch)?;

    // 5. Query and verify
    let row = table.get(2i64)?;
    println!("Retrieved row: {:?}", row);

    // Clean up
    db.drop_table(table_name)?;
    let _ = std::fs::remove_dir_all(db_path);

    Ok(())
}
