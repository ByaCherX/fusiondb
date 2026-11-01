use arrow_array::{Int64Array, StringArray};
use fusiondb::{DataType, Database, Result, SchemaBuilder};
use std::sync::Arc;
use std::time::Instant;

struct SizeConfig {
    name: &'static str,
    size_bytes: usize,
    num_records: usize,
}

#[allow(dead_code)]
struct PerfResult {
    size_name: &'static str,
    value_size_bytes: usize,
    num_records: usize,
    
    // Write stats
    write_duration_ms: f64,
    write_latency_ms: f64,
    write_throughput_mb_s: f64,
    write_ops_s: f64,

    // Memtable read stats
    mem_read_duration_ms: f64,
    mem_read_latency_ms: f64,
    mem_read_ops_s: f64,
    mem_read_throughput_mb_s: f64,
    mem_hits_diff: u64,

    // Disk read stats
    disk_read_duration_ms: f64,
    disk_read_latency_ms: f64,
    disk_read_ops_s: f64,
    disk_read_throughput_mb_s: f64,
    disk_misses_diff: u64,
}

/// Helper to parse statistics from the RocksDB statistics report
fn parse_stat_value(stats: &str, ticker_key: &str) -> u64 {
    for line in stats.lines() {
        if line.contains(ticker_key) {
            if let Some(pos) = line.rfind(':') {
                if let Ok(val) = line[pos + 1..].trim().parse::<u64>() {
                    return val;
                }
            }
        }
    }
    0
}

fn run_perf_test(config: SizeConfig) -> Result<PerfResult> {
    let db_path = format!("./db_perf_{}", config.name);
    
    // 1. Configure RocksDB options with statistics enabled & minimal block cache to force disk reads
    let mut db_opts = rocksdb::Options::default();
    db_opts.create_if_missing(true);
    db_opts.create_missing_column_families(true);
    db_opts.enable_statistics(); // Enable RocksDB internal statistics tracking
    
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    // 1KB block cache ensures that reading flushed SST files triggers disk I/O (cache miss)
    let cache = rocksdb::Cache::new_lru_cache(8<<20);
    block_opts.set_block_cache(&cache);
    db_opts.set_allow_mmap_reads(true);
    db_opts.set_allow_mmap_writes(true);
    db_opts.set_block_based_table_factory(&block_opts);

    let db = Database::open_with_opts(&db_path, db_opts)?;
    
    let schema = SchemaBuilder::new()
        .field("id", DataType::Int64, false)
        .primary_key()
        .field("payload", DataType::Utf8, false)
        .build()?;
    
    let table_name = "perf_table";
    if db.table_exists(table_name) {
        db.drop_table(table_name)?;
    }
    let table = db.create_table(table_name, schema)?;

    // 2. Prepare test data
    let ids: Vec<i64> = (0..config.num_records as i64).collect();
    let payload = "x".repeat(config.size_bytes);
    let payloads: Vec<String> = (0..config.num_records).map(|_| payload.clone()).collect();
    
    let batch = arrow_array::RecordBatch::try_new(
        table.arrow_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(payloads)),
        ],
    ).unwrap();

    // 3. Measure Write Performance
    let write_start = Instant::now();
    table.insert(batch)?;
    let write_elapsed = write_start.elapsed();
    let write_duration_ms = write_elapsed.as_secs_f64() * 1000.0;
    let write_latency_ms = write_duration_ms / config.num_records as f64;
    let total_bytes_written = (config.num_records * config.size_bytes) as f64;
    let write_throughput_mb_s = (total_bytes_written / (1024.0 * 1024.0)) / write_elapsed.as_secs_f64();
    let write_ops_s = config.num_records as f64 / write_elapsed.as_secs_f64();

    // Capture stats after write
    let stats_after_write = db.get_statistics().unwrap_or_default();
    let mem_hits_before = parse_stat_value(&stats_after_write, "memtable.hit");

    // 4. Measure Memtable Read Performance (data is not flushed yet)
    let mem_read_start = Instant::now();
    for i in 0..config.num_records {
        let _row = table.get(i as i64)?;
    }
    let mem_read_elapsed = mem_read_start.elapsed();
    let mem_read_duration_ms = mem_read_elapsed.as_secs_f64() * 1000.0;
    let mem_read_latency_ms = mem_read_duration_ms / config.num_records as f64;
    let mem_read_ops_s = config.num_records as f64 / mem_read_elapsed.as_secs_f64();
    let mem_read_throughput_mb_s = (total_bytes_written / (1024.0 * 1024.0)) / mem_read_elapsed.as_secs_f64();

    // Capture stats after memtable read to calculate hits
    let stats_after_mem_read = db.get_statistics().unwrap_or_default();
    let mem_hits_after = parse_stat_value(&stats_after_mem_read, "memtable.hit");
    let mem_hits_diff = mem_hits_after.saturating_sub(mem_hits_before);
    let cache_misses_before = parse_stat_value(&stats_after_mem_read, "block.cache.miss");

    // 5. Force Flush Memtable to Disk (creating SST files)
    let cf_name = format!("data__{}", table.name());
    let storage = db.storage();
    let cf = storage.get_cf(&cf_name).unwrap();
    storage.db().flush_cf(&cf).unwrap();

    // 6. Measure Disk Read Performance (Cache Misses)
    let disk_read_start = Instant::now();
    for i in 0..config.num_records {
        let _row = table.get(i as i64)?;
    }
    let disk_read_elapsed = disk_read_start.elapsed();
    let disk_read_duration_ms = disk_read_elapsed.as_secs_f64() * 1000.0;
    let disk_read_latency_ms = disk_read_duration_ms / config.num_records as f64;
    let disk_read_ops_s = config.num_records as f64 / disk_read_elapsed.as_secs_f64();
    let disk_read_throughput_mb_s = (total_bytes_written / (1024.0 * 1024.0)) / disk_read_elapsed.as_secs_f64();

    // Capture stats after disk read to calculate cache misses (disk accesses)
    let stats_after_disk_read = db.get_statistics().unwrap_or_default();
    let cache_misses_after = parse_stat_value(&stats_after_disk_read, "block.cache.miss");
    let disk_misses_diff = cache_misses_after.saturating_sub(cache_misses_before);

    // Clean up
    db.drop_table(table_name)?;
    drop(db);
    let _ = std::fs::remove_dir_all(db_path);

    Ok(PerfResult {
        size_name: config.name,
        value_size_bytes: config.size_bytes,
        num_records: config.num_records,
        
        write_duration_ms,
        write_latency_ms,
        write_throughput_mb_s,
        write_ops_s,

        mem_read_duration_ms,
        mem_read_latency_ms,
        mem_read_ops_s,
        mem_read_throughput_mb_s,
        mem_hits_diff,

        disk_read_duration_ms,
        disk_read_latency_ms,
        disk_read_ops_s,
        disk_read_throughput_mb_s,
        disk_misses_diff,
    })
}

fn main() -> Result<()> {
    println!("=================================================================================");
    println!("               RocksDB Read/Write Performance Insights (FusionDB)");
    println!("=================================================================================");
    println!("Note: Memtable Reads occur before flush. Disk Reads occur after flushing");
    println!("      and are measured using a tiny 1KB cache to trigger physical disk I/O.");
    println!("=================================================================================\n");

    let configs = vec![
        SizeConfig { name: "1KB", size_bytes: 1024, num_records: 1000 },
        SizeConfig { name: "4KB", size_bytes: 4 * 1024, num_records: 500 },
        SizeConfig { name: "16KB", size_bytes: 16 * 1024, num_records: 200 },
        SizeConfig { name: "32KB", size_bytes: 32 * 1024, num_records: 100 },
        SizeConfig { name: "128KB", size_bytes: 128 * 1024, num_records: 50 },
        SizeConfig { name: "512KB", size_bytes: 512 * 1024, num_records: 20 },
        SizeConfig { name: "2MB", size_bytes: 2 * 1024 * 1024, num_records: 5 },
        SizeConfig { name: "8MB", size_bytes: 8 * 1024 * 1024, num_records: 2 },
        SizeConfig { name: "64MB", size_bytes: 64 * 1024 * 1024, num_records: 1 },
    ];

    let mut results = vec![];
    for config in configs {
        println!("Running benchmark for payload size: {} ({} records)...", config.name, config.num_records);
        let result = run_perf_test(config)?;
        results.push(result);
    }

    println!("\n=================================================================================");
    println!("                                   TEST SUMMARY");
    println!("=================================================================================");
    println!(
        "{:<8} | {:<12} | {:<12} | {:<12} | {:<12} | {:<12}",
        "Size", "Op Type", "Time (ms)", "Latency (ms)", "Throughput", "Source / Hits"
    );
    println!("---------------------------------------------------------------------------------");

    for r in results {
        // Write stats
        println!(
            "{:<8} | {:<12} | {:<12.2} | {:<12.4} | {:<9.2} MB/s | -",
            r.size_name, "Write", r.write_duration_ms, r.write_latency_ms, r.write_throughput_mb_s
        );
        // Memtable read stats
        println!(
            "{:<8} | {:<12} | {:<12.2} | {:<12.4} | {:<9.2} MB/s | Memtable ({} hits)",
            r.size_name, "Read (Mem)", r.mem_read_duration_ms, r.mem_read_latency_ms, r.mem_read_throughput_mb_s, r.mem_hits_diff
        );
        // Disk read stats
        println!(
            "{:<8} | {:<12} | {:<12.2} | {:<12.4} | {:<9.2} MB/s | Disk ({} misses)",
            r.size_name, "Read (Disk)", r.disk_read_duration_ms, r.disk_read_latency_ms, r.disk_read_throughput_mb_s, r.disk_misses_diff
        );
        println!("---------------------------------------------------------------------------------");
    }

    Ok(())
}
