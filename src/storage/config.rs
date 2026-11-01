//! Storage configuration for tuning RocksDB performance.
//!
//! Provides a `StorageConfig` struct that lets users customize RocksDB
//! settings like cache size, compression, write buffers, and more.

/// Configurable storage engine options.
///
/// All fields have sensible defaults optimized for embedded database workloads.
///
/// # Example
/// ```rust,no_run
/// use fusiondb::storage::StorageConfig;
///
/// let config = StorageConfig::default()
///     .block_cache_size(256 * 1024 * 1024)  // 256 MB cache
///     .enable_compression(true)
///     .max_background_jobs(8);
/// ```
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Block cache size in bytes (default: 128 MB).
    ///
    /// The block cache stores uncompressed data blocks in memory.
    /// Point queries and sequential iterator scans query this LRU cache first.
    /// Larger sizes speed up read operations for hot datasets.
    pub block_cache_size_bytes: usize,

    /// Write buffer (memtable) size in bytes (default: 64 MB).
    ///
    /// Dictates the capacity of the concurrent SkipList in memory.
    /// When the active MemTable size exceeds this threshold, it is frozen (marked read-only)
    /// and scheduled for a background flush.
    pub write_buffer_size_bytes: usize,

    /// Maximum number of memtables before flushing (default: 4).
    ///
    /// Determines how many read-only MemTables can wait in queue before RocksDB blocks
    /// incoming client writes (write stall). High values absorb spikes in write volume.
    pub max_write_buffer_number: i32,

    /// Minimum number of memtables to merge before flush (default: 2).
    ///
    /// Groups multiple immutable MemTables together during a flush to output larger,
    /// more sequential SST files to Level 0, reducing future compaction workload.
    pub min_write_buffer_number_to_merge: i32,

    /// Bloom filter bits per key (default: 10).
    ///
    /// Sets the sizing for the probabilistic Bloom filter blocks inside SST files.
    /// A setting of 10 bits-per-key provides a false positive rate of ~1%, allowing
    /// point lookups (`get`) to verify if a key exists without reading blocks from disk.
    /// *Tip: Bloom filters are not used during writes; they only optimize read lookups.*
    pub bloom_filter_bits: i32,

    /// Enable LZ4/Zstd compression (default: true).
    ///
    /// Uses LZ4 for fast intermediate level compression and Zstd (high compression ratio)
    /// for bottommost storage levels to minimize total disk utilization.
    pub compression_enabled: bool,

    /// Maximum number of background compaction/flush jobs (default: 4).
    ///
    /// Configures the thread pool size allocated for background operations.
    pub max_background_jobs: i32,

    /// Compaction I/O rate limit in bytes/sec (default: None = unlimited).
    ///
    /// Throttles background write bandwidth to prevent compaction processes from
    /// starving front-end database reads/writes of SSD/HDD throughput.
    pub rate_limit_bytes_per_sec: Option<i64>,

    /// Enable pipelined writes for WAL + memtable (default: true).
    ///
    /// When enabled, allows one thread to write to the WAL while another thread
    /// concurrently writes to the MemTable, increasing write throughput.
    pub pipelined_write: bool,

    /// Compaction readahead size in bytes (default: 2 MB).
    ///
    /// Instructs the OS to prefetch sequential data blocks from disk during compaction,
    /// reducing the time threads wait for disk I/O.
    pub compaction_readahead_bytes: usize,

    /// Bytes between fsync calls during writes (default: 1 MB).
    ///
    /// Limits OS page cache buffer build-up to avoid disk write stalls
    /// by periodically executing raw fsyncs.
    pub bytes_per_sync: u64,

    /// WAL bytes between fsync calls (default: 1 MB).
    ///
    /// Forces write-ahead log logs to flush sequentially to disk every N bytes,
    /// ensuring write durability and limiting data loss windows.
    pub wal_bytes_per_sync: u64,

    /// Maximum total WAL size before forcing flush (default: 256 MB).
    ///
    /// Limits WAL directory growth by forcing a MemTable flush if WAL log files accumulate
    /// past this limit, reducing recovery times after database crashes.
    pub max_total_wal_size: u64,

    /// Maximum number of open files (default: 512).
    ///
    /// Controls file descriptor resource limits for open SST files.
    pub max_open_files: i32,

    /// Target SST file size in bytes (default: 64 MB).
    ///
    /// The baseline size for generated SST files. Large sizes reduce the total number
    /// of files but increase compaction job duration and temporary disk space requirements.
    pub target_file_size_base: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            block_cache_size_bytes: 128 * 1024 * 1024,      // 128 MB
            write_buffer_size_bytes: 64 * 1024 * 1024,       // 64 MB
            max_write_buffer_number: 4,
            min_write_buffer_number_to_merge: 2,
            bloom_filter_bits: 10,
            compression_enabled: true,
            max_background_jobs: 4,
            rate_limit_bytes_per_sec: None,
            pipelined_write: true,
            compaction_readahead_bytes: 2 * 1024 * 1024,     // 2 MB
            bytes_per_sync: 1024 * 1024,                     // 1 MB
            wal_bytes_per_sync: 1024 * 1024,                 // 1 MB
            max_total_wal_size: 256 * 1024 * 1024,           // 256 MB
            max_open_files: 512,
            target_file_size_base: 64 * 1024 * 1024,         // 64 MB
        }
    }
}

impl StorageConfig {
    /// Set the block cache size in bytes.
    pub fn block_cache_size(mut self, bytes: usize) -> Self {
        self.block_cache_size_bytes = bytes;
        self
    }

    /// Set the write buffer (memtable) size in bytes.
    pub fn write_buffer_size(mut self, bytes: usize) -> Self {
        self.write_buffer_size_bytes = bytes;
        self
    }

    /// Set the maximum number of write buffers.
    pub fn max_write_buffers(mut self, n: i32) -> Self {
        self.max_write_buffer_number = n;
        self
    }

    /// Enable or disable LZ4/Zstd compression.
    pub fn enable_compression(mut self, enabled: bool) -> Self {
        self.compression_enabled = enabled;
        self
    }

    /// Set the maximum number of background compaction/flush jobs.
    pub fn max_background_jobs(mut self, n: i32) -> Self {
        self.max_background_jobs = n;
        self
    }

    /// Set the compaction I/O rate limit in bytes per second.
    pub fn rate_limit(mut self, bytes_per_sec: i64) -> Self {
        self.rate_limit_bytes_per_sec = Some(bytes_per_sec);
        self
    }

    /// Convert this config into a `rocksdb::Options` instance.
    pub fn to_rocksdb_options(&self) -> rocksdb::Options {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Parallelism
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4) as i32;
        opts.increase_parallelism(cpus);
        opts.set_max_background_jobs(self.max_background_jobs);

        // Block-based table options
        let mut block_opts = rocksdb::BlockBasedOptions::default();
        block_opts.set_block_cache(&rocksdb::Cache::new_lru_cache(self.block_cache_size_bytes));
        block_opts.set_bloom_filter(self.bloom_filter_bits as f64, false);
        opts.set_block_based_table_factory(&block_opts);

        // MemTable / Write buffer configuration
        opts.set_write_buffer_size(self.write_buffer_size_bytes);
        opts.set_max_write_buffer_number(self.max_write_buffer_number);
        opts.set_min_write_buffer_number_to_merge(self.min_write_buffer_number_to_merge);

        // File limits and sizes
        opts.set_max_open_files(self.max_open_files);
        opts.set_target_file_size_base(self.target_file_size_base);

        // Memory-mapped I/O
        opts.set_allow_mmap_reads(true);
        opts.set_allow_mmap_writes(false);

        // Disable paranoid checks and fsync for throughput
        opts.set_paranoid_checks(false);
        opts.set_use_fsync(false);

        // Compaction: Recommended Level based
        opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
        opts.set_level_zero_file_num_compaction_trigger(10);

        // Compression: LZ4 for upper levels, Zstd for bottommost level
        if self.compression_enabled {
            opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
            opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
        }

        // Pipelined writes: overlap WAL and memtable writes
        opts.set_enable_pipelined_write(self.pipelined_write);

        // Compaction readahead for sequential I/O during compaction
        opts.set_compaction_readahead_size(self.compaction_readahead_bytes);

        // Periodic sync for sequential write durability / reduced fragmentation
        opts.set_bytes_per_sync(self.bytes_per_sync);
        opts.set_wal_bytes_per_sync(self.wal_bytes_per_sync);

        // WAL size limits to prevent unbounded WAL growth
        opts.set_max_total_wal_size(self.max_total_wal_size);

        // Optimize bloom filters: skip the bottommost level (saves space)
        opts.set_optimize_filters_for_hits(true);

        // Enable internal statistics
        opts.enable_statistics();
        opts.set_log_level(rocksdb::LogLevel::Info);

        // Rate limiter for compaction I/O throttling
        if let Some(rate) = self.rate_limit_bytes_per_sec {
            opts.set_ratelimiter(rate, 100_000, 10);
        }

        opts
    }

    /// Convert this config into a `rocksdb::Options` instance using a shared block cache.
    pub fn to_rocksdb_options_with_cache(&self, cache: &rocksdb::Cache) -> rocksdb::Options {
        let mut opts = self.to_rocksdb_options();
        let mut block_opts = rocksdb::BlockBasedOptions::default();
        block_opts.set_block_cache(cache);
        block_opts.set_bloom_filter(self.bloom_filter_bits as f64, false);
        opts.set_block_based_table_factory(&block_opts);
        opts
    }
}
