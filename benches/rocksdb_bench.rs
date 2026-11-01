use arrow_array::{Int64Array, StringArray};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use fusiondb::{Database, DataType, FusionSchema, RecordBatch, SchemaBuilder};
use std::sync::Arc;

/// Create a test schema with id (primary key), name, and value fields
fn create_test_schema() -> FusionSchema {
    SchemaBuilder::new()
        .field("id", DataType::Int64, false)
        .primary_key()
        .field("name", DataType::Utf8, false)
        .field("value", DataType::Utf8, false)
        .build()
        .unwrap()
}

/// Generate a RecordBatch with a given range of IDs and string value size
fn generate_batch(
    schema: Arc<arrow_schema::Schema>,
    start_id: i64,
    count: usize,
    value_size: usize,
) -> RecordBatch {
    let ids: Vec<i64> = (start_id..(start_id + count as i64)).collect();
    let names: Vec<String> = (0..count)
        .map(|i| format!("name_{}", start_id + i as i64))
        .collect();
    let value_str = "x".repeat(value_size);
    let values: Vec<String> = (0..count).map(|_| value_str.clone()).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(values)),
        ],
    )
    .unwrap()
}

// =========================================================================
// 1) Write-Heavy (Insert-Intensive) Workload
// =========================================================================
fn bench_write_heavy(c: &mut Criterion) {
    let mut group = c.benchmark_group("RocksDB - Write Heavy");
    group.sample_size(10); // Database setup is heavy, use smaller sample size

    group.bench_function("insert_10k_rows_batch_1000", |b| {
        b.iter_batched(
            || {
                let temp_dir = tempfile::TempDir::new().unwrap();
                let db = Database::open(temp_dir.path().to_str().unwrap()).unwrap();
                let schema = create_test_schema();
                let table = db.create_table("bench_table", schema).unwrap();
                (temp_dir, db, table)
            },
            |(_temp_dir, _db, table)| {
                let schema = table.arrow_schema();
                for i in 0..10 {
                    let batch = generate_batch(schema.clone(), (i * 1000) as i64, 1000, 100);
                    table.insert(batch).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

// =========================================================================
// 2) Read-Heavy (Intensive Reads, Rare Inserts) Workload
// =========================================================================
fn bench_read_heavy(c: &mut Criterion) {
    let mut group = c.benchmark_group("RocksDB - Read Heavy");
    group.sample_size(10);

    group.bench_function("1000_reads_with_10_inserts", |b| {
        b.iter_batched_ref(
            || {
                let temp_dir = tempfile::TempDir::new().unwrap();
                let db = Database::open(temp_dir.path().to_str().unwrap()).unwrap();
                let schema = create_test_schema();
                let table = db.create_table("bench_table", schema).unwrap();

                // Pre-populate with 10,000 rows
                let arrow_schema = table.arrow_schema();
                let batch = generate_batch(arrow_schema, 0, 10000, 100);
                table.insert(batch).unwrap();

                (temp_dir, db, table, 10000_i64)
            },
            |(_temp_dir, _db, table, next_insert_id)| {
                use rand::Rng;
                let mut rng = rand::thread_rng();
                let arrow_schema = table.arrow_schema();

                for step in 0..1000 {
                    if step % 100 == 0 {
                        // Rare insert (1% of operations)
                        let batch = generate_batch(arrow_schema.clone(), *next_insert_id, 1, 100);
                        table.insert(batch).unwrap();
                        *next_insert_id += 1;
                    } else {
                        // Intensive read (99% of operations)
                        let pk = rng.gen_range(0..10000) as i64;
                        let _row = table.get(pk).unwrap();
                    }
                }
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

// =========================================================================
// 3) Latency Benchmarks (Memtable, Disk Cache Miss, Large Value)
// =========================================================================
fn bench_latencies(c: &mut Criterion) {
    let mut group = c.benchmark_group("RocksDB - Latency Analysis");
    group.sample_size(10);

    // Memtable Read: key is just inserted, read immediately before flushing
    group.bench_function("memtable_read_latency", |b| {
        b.iter_batched_ref(
            || {
                let temp_dir = tempfile::TempDir::new().unwrap();
                let db = Database::open(temp_dir.path().to_str().unwrap()).unwrap();
                let table = db.create_table("bench_table", create_test_schema()).unwrap();
                let batch = generate_batch(table.arrow_schema(), 1, 1, 100);
                table.insert(batch).unwrap();
                (temp_dir, db, table)
            },
            |(_temp_dir, _db, table)| {
                let _row = table.get(1i64).unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    // Disk Read (Cache Miss): memtable flushed to disk, opened with minimal block cache
    group.bench_function("disk_read_cache_miss_latency", |b| {
        b.iter_batched_ref(
            || {
                let temp_dir = tempfile::TempDir::new().unwrap();

                // Open DB with minimal cache (1KB LRU block cache) to trigger cache misses
                let mut db_opts = rocksdb::Options::default();
                db_opts.create_if_missing(true);
                db_opts.create_missing_column_families(true);

                let mut block_opts = rocksdb::BlockBasedOptions::default();
                let cache = rocksdb::Cache::new_lru_cache(1024); // 1KB
                block_opts.set_block_cache(&cache);
                db_opts.set_block_based_table_factory(&block_opts);

                let db = Database::open_with_opts(temp_dir.path().to_str().unwrap(), db_opts).unwrap();
                let table = db.create_table("bench_table", create_test_schema()).unwrap();

                // Insert one row
                let batch = generate_batch(table.arrow_schema(), 1, 1, 100);
                table.insert(batch).unwrap();

                // Force flush memtable to SST files on disk
                let cf_name = format!("data_{}", table.name());
                let storage = db.storage();
                let cf = storage.get_cf(&cf_name).unwrap();
                storage.db().flush_cf(&cf).unwrap();

                (temp_dir, db, table)
            },
            |(_temp_dir, _db, table)| {
                let _row = table.get(1i64).unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    // Large Value Read: read 1MB values
    group.bench_function("large_value_read_1mb_latency", |b| {
        b.iter_batched_ref(
            || {
                let temp_dir = tempfile::TempDir::new().unwrap();
                let db = Database::open(temp_dir.path().to_str().unwrap()).unwrap();
                let table = db.create_table("bench_table", create_test_schema()).unwrap();

                // Insert a 1MB string value
                let batch = generate_batch(table.arrow_schema(), 1, 1, 1024 * 1024);
                table.insert(batch).unwrap();

                (temp_dir, db, table)
            },
            |(_temp_dir, _db, table)| {
                let _row = table.get(1i64).unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// =========================================================================
// 4) RocksDB Configuration Comparisons
// =========================================================================

/// Minimal resources configuration (small write buffers, minimal cache)
fn get_minimal_opts() -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_write_buffer_size(2 * 1024 * 1024); // 2MB memtable

    let mut block_opts = rocksdb::BlockBasedOptions::default();
    let cache = rocksdb::Cache::new_lru_cache(1024); // 1KB block cache
    block_opts.set_block_cache(&cache);
    opts.set_block_based_table_factory(&block_opts);
    opts
}

/// Optimized configuration (larger write buffers, block cache, bloom filters)
fn get_optimized_opts() -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB memtable
    opts.set_max_write_buffer_number(3);

    let mut block_opts = rocksdb::BlockBasedOptions::default();
    let cache = rocksdb::Cache::new_lru_cache(128 * 1024 * 1024); // 128MB block cache
    block_opts.set_block_cache(&cache);
    block_opts.set_bloom_filter(10.0, false); // Enable bloom filter
    opts.set_block_based_table_factory(&block_opts);
    opts
}

/// Read-Optimized configuration (large cache, bloom filters, cached indexes/filters)
fn get_high_read_opts() -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_write_buffer_size(128 * 1024 * 1024); // 128MB memtable

    let mut block_opts = rocksdb::BlockBasedOptions::default();
    let cache = rocksdb::Cache::new_lru_cache(256 * 1024 * 1024); // 256MB block cache
    block_opts.set_block_cache(&cache);
    block_opts.set_bloom_filter(10.0, false);
    block_opts.set_cache_index_and_filter_blocks(true); // Cache index/filters in block cache
    opts.set_block_based_table_factory(&block_opts);
    opts
}

fn bench_config_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("RocksDB - Config Comparison");
    group.sample_size(10);

    // Compare Write Performance under different configurations
    for (config_name, opts_fn) in [
        ("minimal", get_minimal_opts as fn() -> rocksdb::Options),
        ("optimized", get_optimized_opts),
        ("high_read", get_high_read_opts),
    ] {
        group.bench_function(format!("write_10k_{}", config_name), |b| {
            b.iter_batched(
                || {
                    let temp_dir = tempfile::TempDir::new().unwrap();
                    let opts = opts_fn();
                    let db = Database::open_with_opts(temp_dir.path().to_str().unwrap(), opts).unwrap();
                    let table = db.create_table("bench_table", create_test_schema()).unwrap();
                    (temp_dir, db, table)
                },
                |(_temp_dir, _db, table)| {
                    let schema = table.arrow_schema();
                    for i in 0..10 {
                        let batch = generate_batch(schema.clone(), (i * 1000) as i64, 1000, 100);
                        table.insert(batch).unwrap();
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }

    // Compare Read Performance under different configurations
    for (config_name, opts_fn) in [
        ("minimal", get_minimal_opts as fn() -> rocksdb::Options),
        ("optimized", get_optimized_opts),
        ("high_read", get_high_read_opts),
    ] {
        group.bench_function(format!("read_1000_{}", config_name), |b| {
            b.iter_batched_ref(
                || {
                    let temp_dir = tempfile::TempDir::new().unwrap();
                    let opts = opts_fn();
                    let db = Database::open_with_opts(temp_dir.path().to_str().unwrap(), opts).unwrap();
                    let table = db.create_table("bench_table", create_test_schema()).unwrap();

                    // Pre-populate with 5,000 rows
                    let batch = generate_batch(table.arrow_schema(), 0, 5000, 100);
                    table.insert(batch).unwrap();

                    (temp_dir, db, table)
                },
                |(_temp_dir, _db, table)| {
                    use rand::Rng;
                    let mut rng = rand::thread_rng();
                    for _ in 0..1000 {
                        let pk = rng.gen_range(0..5000) as i64;
                        let _row = table.get(pk).unwrap();
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_write_heavy,
    bench_read_heavy,
    bench_latencies,
    bench_config_comparison
);
criterion_main!(benches);
