//! StorageEngine — thin abstraction layer over RocksDB.
//!
//! Manages column families, key-value operations, metadata persistence,
//! and table scanning with configurable performance settings.
//!
//! ### RocksDB Memory & Storage Architecture
//!
//! ```text
//!                              +---------------------------------------+
//!                              |               Write Path              |
//!                              +---------------------------------------+
//!                                   |                     |
//!                                   | (Write WAL)         | (Write MemTable)
//!                                   v                     v
//!                             +-----------+         +------------------+
//!                             | WAL File  |         | Active MemTable  | (In Memory SkipList)
//!                             | (On Disk) |         | (Lock-free write)|
//!                             +-----------+         +------------------+
//!                                                         |
//!                                                         | (Buffer Full -> Freeze)
//!                                                         v
//!                                                   +------------------+
//!                                                   | Immutable        |
//!                                                   | MemTables        | (Waiting for flush)
//!                                                   +------------------+
//!                                                            |
//!                                                            | (Background Flush Thread)
//!                                                            v
//!  +--------------------------------------------------+ +-------------+
//!  |                    Read Path                     | | Level 0 SST | (Sequential Flush output)
//!  +--------------------------------------------------+ +-------------+
//!       |            |              |            |             |
//!       |            |              |            |             v
//!       v            v              v            v      +-------------+
//!   +-------+   +-----------+   +---------+  +-------+  | Compaction  | (Background Merge Thread)
//!   | Active|   | Immutable |   | Bloom   |  | Block |  | Thread      |
//!   | Mem   |   | MemTable  |   | Filters |  | Cache |  +-------------+
//!   | Table |   |           |   | (SSTs)  |  | (LRU) |         |
//!   +-------+   +-----------+   +---------+  +-------+         v
//!       |            |              |            |      +-------------+
//!   [Hit! ]      [Hit! ]        [Hit! ]      [Hit! ]    | Levels 1..N | (Merged sorted files)
//!                                   |                   | SSTs        |
//!                                   v                   +-------------+
//!                               [Disk Read] (pread system call)
//! ```
//!
//! ### Storage isolation via Column Families (CF)
//! RocksDB stores data in a single log directory but supports logical partition namespaces called Column Families:
//! 1. **`__metadata__`**: Contains table schemas serialized as JSON, row count statistics, and creation metadata.
//! 2. **`data__<table_name>`**: Created for each user table. Isolates key-value spaces per table, allowing independent
//!    tuning of block sizes, compaction styles, and bloom filters.

use crate::core::error::Result;
use crate::storage::config::StorageConfig;
use crate::storage::metadata::{data_cf_name, TableMetadata, METADATA_CF};
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options, WriteBatch};
use std::sync::{Arc, Mutex, MutexGuard};

/// Thread-safe, multi-CF RocksDB instance type alias.
pub type RocksDB = DBWithThreadMode<MultiThreaded>;

/// StorageEngine — manages table data and metadata on RocksDB.
///
/// Uses a column-family-per-table approach for data isolation.
pub struct StorageEngine {
    /// RocksDB instance (shared via Arc)
    db: Arc<RocksDB>,
    /// RocksDB options (cloned for new column families)
    db_opts: Options,
    /// Path to the database directory
    path: std::path::PathBuf,
    /// Per-table write lock so writes to different tables proceed concurrently
    table_locks: Mutex<std::collections::HashMap<String, Arc<Mutex<()>>>>,
    /// Fallback global write lock for backwards compatibility
    #[allow(dead_code)]
    write_lock: Mutex<()>,
}

impl std::fmt::Debug for StorageEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageEngine")
            .field("path", &self.path)
            .finish()
    }
}

impl StorageEngine {
    /// Open or create the database with the default configuration.
    pub fn open(path: &str) -> Result<Self> {
        Self::open_with_config(path, StorageConfig::default())
    }

    /// Open or create the database with a custom [`StorageConfig`].
    pub fn open_with_config(path: &str, config: StorageConfig) -> Result<Self> {
        let db_opts = config.to_rocksdb_options();
        Self::open_with_opts(path, db_opts)
    }

    /// Open or create the database with raw RocksDB options.
    ///
    /// # Arguments
    /// * `path` - Database directory path
    /// * `db_opts` - Custom `rocksdb::Options`
    pub fn open_with_opts(path: &str, db_opts: Options) -> Result<Self> {
        log::info!("Opening RocksDB storage engine at path: '{}'", path);
        
        // Find existing column families
        let existing_cfs = match RocksDB::list_cf(&db_opts, path) {
            Ok(cfs) => {
                log::debug!("Found existing column families: {:?}", cfs);
                cfs
            }
            Err(e) => {
                log::debug!("Failed listing column families (possibly new database): {:?}", e);
                vec!["default".to_string()]
            }
        };

        // Create CF descriptors by cloning the DB options
        let cf_descriptors: Vec<ColumnFamilyDescriptor> = existing_cfs
            .iter()
            .map(|name| {
                let cf_opts = db_opts.clone();
                ColumnFamilyDescriptor::new(name, cf_opts)
            })
            .collect();

        log::debug!("Opening database with {} column family descriptors", cf_descriptors.len());
        let db = match RocksDB::open_cf_descriptors(&db_opts, path, cf_descriptors) {
            Ok(db) => {
                log::info!("Successfully opened RocksDB instance at '{}'", path);
                db
            }
            Err(e) => {
                log::error!("Failed to open RocksDB instance: {:?}", e);
                return Err(e.into());
            }
        };

        let engine = StorageEngine { 
            db: Arc::new(db),
            db_opts,
            path: std::path::PathBuf::from(path),
            table_locks: Mutex::new(std::collections::HashMap::new()),
            write_lock: Mutex::new(()),
        };
        engine.ensure_metadata_cf()?;

        Ok(engine)
    }

    /// Acquire the global write lock. Hold it for the whole check-then-write sequence.
    #[allow(dead_code)]
    pub(crate) fn write_guard(&self) -> MutexGuard<'_, ()> {
        self.write_lock.lock().unwrap()
    }

    /// Acquire a table-specific write lock Arc so writes to different tables can proceed concurrently.
    pub(crate) fn table_write_guard(&self, table_name: &str) -> Arc<Mutex<()>> {
        let mut locks = self.table_locks.lock().unwrap();
        locks
            .entry(table_name.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Get the path to the database directory.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Ensure that the metadata column family exists.
    fn ensure_metadata_cf(&self) -> Result<()> {
        if self.db.cf_handle(METADATA_CF).is_none() {
            let opts = self.db_opts.clone();
            self.db.create_cf(METADATA_CF, &opts)?;
        }
        Ok(())
    }

    /// Create a new column family for a table.
    pub fn create_table_cf(&self, table_name: &str) -> Result<()> {
        let cf_name = data_cf_name(table_name);
        log::info!("Creating Column Family '{}' for table '{}'", cf_name, table_name);
        let opts = self.db_opts.clone();
        if let Err(e) = self.db.create_cf(&cf_name, &opts) {
            log::error!("Failed to create Column Family '{}': {:?}", cf_name, e);
            return Err(e.into());
        }
        Ok(())
    }

    /// Drop a table's column family.
    pub fn drop_table_cf(&self, table_name: &str) -> Result<()> {
        let cf_name = data_cf_name(table_name);
        log::info!("Dropping Column Family '{}' for table '{}'", cf_name, table_name);
        if let Err(e) = self.db.drop_cf(&cf_name) {
            log::error!("Failed to drop Column Family '{}': {:?}", cf_name, e);
            return Err(e.into());
        }
        Ok(())
    }

    /// Write a single key-value pair.
    pub fn put(&self, cf_name: &str, key: &[u8], value: &[u8]) -> Result<()> {
        log::debug!("Put operation on CF '{}' (key_len: {}, val_len: {})", cf_name, key.len(), value.len());
        let cf = self.get_cf(cf_name)?;
        if let Err(e) = self.db.put_cf(&cf, key, value) {
            log::error!("Failed put on CF '{}': {:?}", cf_name, e);
            return Err(e.into());
        }
        Ok(())
    }

    /// Read a value by key.
    pub fn get(&self, cf_name: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        log::debug!("Get operation on CF '{}' (key_len: {})", cf_name, key.len());
        let cf = self.get_cf(cf_name)?;
        match self.db.get_cf(&cf, key) {
            Ok(res) => {
                log::debug!("Get result on CF '{}' - found: {}", cf_name, res.is_some());
                Ok(res.map(|v| v.to_vec()))
            }
            Err(e) => {
                log::error!("Failed get on CF '{}': {:?}", cf_name, e);
                Err(e.into())
            }
        }
    }

    /// Read multiple values by keys for a column family in a single batched call.
    pub fn multi_get<K: AsRef<[u8]>>(&self, cf_name: &str, keys: &[K]) -> Result<Vec<Option<Vec<u8>>>> {
        log::debug!("Multi-get operation on CF '{}' (keys_count: {})", cf_name, keys.len());
        let cf = self.get_cf(cf_name)?;
        let items: Vec<(&Arc<BoundColumnFamily<'_>>, &K)> = keys.iter().map(|k| (&cf, k)).collect();
        let results = self.db.multi_get_cf(items);
        let mut values = Vec::with_capacity(results.len());
        for res in results {
            match res {
                Ok(Some(v)) => values.push(Some(v)),
                Ok(None) => values.push(None),
                Err(e) => {
                    log::error!("Failed multi-get on CF '{}': {:?}", cf_name, e);
                    return Err(e.into());
                }
            }
        }
        Ok(values)
    }

    /// Delete a key.
    pub fn delete(&self, cf_name: &str, key: &[u8]) -> Result<()> {
        log::debug!("Delete operation on CF '{}' (key_len: {})", cf_name, key.len());
        let cf = self.get_cf(cf_name)?;
        if let Err(e) = self.db.delete_cf(&cf, key) {
            log::error!("Failed delete on CF '{}': {:?}", cf_name, e);
            return Err(e.into());
        }
        Ok(())
    }

    /// Atomic batch write.
    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        log::debug!("Writing batch to RocksDB (batch_len: {})", batch.len());
        let start = std::time::Instant::now();
        if let Err(e) = self.db.write(batch) {
            log::error!("Failed batch write to RocksDB: {:?}", e);
            return Err(e.into());
        }
        log::debug!("Batch write completed in {:?}", start.elapsed());
        Ok(())
    }

    /// Scan all key-value pairs in a column family.
    pub fn scan_all(&self, cf_name: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        log::debug!("Scan all operation on CF '{}'", cf_name);
        let start = std::time::Instant::now();
        let cf = self.get_cf(cf_name)?;
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        let mut results = Vec::new();
        for item in iter {
            let (k, v) = item?;
            results.push((k.to_vec(), v.to_vec()));
        }
        log::debug!("Scan all on CF '{}' returned {} items in {:?}", cf_name, results.len(), start.elapsed());
        Ok(results)
    }

    /// Scan a limited number of key-value pairs.
    pub fn scan_limit(&self, cf_name: &str, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        log::debug!("Scan limit operation on CF '{}' with limit {}", cf_name, limit);
        let start = std::time::Instant::now();
        let cf = self.get_cf(cf_name)?;
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        let mut results = Vec::new();
        for item in iter.take(limit) {
            let (k, v) = item?;
            results.push((k.to_vec(), v.to_vec()));
        }
        log::debug!("Scan limit on CF '{}' returned {} items in {:?}", cf_name, results.len(), start.elapsed());
        Ok(results)
    }

    /// List all table names from metadata.
    pub fn list_tables(&self) -> Result<Vec<String>> {
        let cf = self.get_cf(METADATA_CF)?;
        let prefix = crate::storage::metadata::META_KEY_PREFIX.as_bytes();
        let iter = self.db.prefix_iterator_cf(&cf, prefix);
        let mut names = Vec::new();
        for item in iter {
            let (key, _) = item?;
            let key_str = String::from_utf8(key.to_vec()).unwrap_or_default();
            if let Some(name) = key_str.strip_prefix(crate::storage::metadata::META_KEY_PREFIX) {
                names.push(name.to_string());
            }
        }
        Ok(names)
    }

    /// Save table metadata.
    pub fn save_metadata(&self, metadata: &TableMetadata) -> Result<()> {
        let cf = self.get_cf(METADATA_CF)?;
        let key = crate::storage::metadata::meta_key(&metadata.name);
        let json = serde_json::to_string(metadata)?;
        self.db.put_cf(&cf, key.as_bytes(), json.as_bytes())?;
        Ok(())
    }

    /// Load table metadata.
    pub fn load_metadata(&self, table_name: &str) -> Result<Option<TableMetadata>> {
        let cf = self.get_cf(METADATA_CF)?;
        let key = crate::storage::metadata::meta_key(table_name);
        match self.db.get_cf(&cf, key.as_bytes())? {
            Some(bytes) => {
                let json = String::from_utf8(bytes.to_vec()).map_err(|e| {
                    crate::core::error::FusionDBError::schema(format!(
                        "Invalid UTF-8 in metadata: {}",
                        e
                    ))
                })?;
                let metadata: TableMetadata = serde_json::from_str(&json)?;
                Ok(Some(metadata))
            }
            None => Ok(None),
        }
    }

    /// Delete table metadata.
    pub fn delete_metadata(&self, table_name: &str) -> Result<()> {
        let cf = self.get_cf(METADATA_CF)?;
        let key = crate::storage::metadata::meta_key(table_name);
        self.db.delete_cf(&cf, key.as_bytes())?;
        Ok(())
    }

    /// Update row count by a delta value (positive for inserts, negative for deletes).
    pub fn increment_row_count(&self, table_name: &str, delta: i64) -> Result<()> {
        if let Some(mut meta) = self.load_metadata(table_name)? {
            if delta >= 0 {
                meta.row_count = meta.row_count.saturating_add(delta as u64);
            } else {
                meta.row_count = meta.row_count.saturating_sub((-delta) as u64);
            }
            self.save_metadata(&meta)?;
        }
        Ok(())
    }

    /// Return an Arc reference to the underlying RocksDB instance.
    pub fn db(&self) -> Arc<RocksDB> {
        self.db.clone()
    }

    /// Get a column family handle by name.
    pub fn get_cf(&self, cf_name: &str) -> Result<Arc<BoundColumnFamily<'_>>> {
        self.db.cf_handle(cf_name).ok_or_else(|| {
            crate::core::error::FusionDBError::invalid_op(format!(
                "Column family '{}' not found",
                cf_name
            ))
        })
    }

    /// Retrieve the formatted statistics string from RocksDB.
    pub fn get_statistics(&self) -> Option<String> {
        self.db_opts.get_statistics()
    }

    /// Trigger a manual compaction on the specified column family.
    pub fn compact_cf(&self, cf_name: &str) -> Result<()> {
        let cf = self.get_cf(cf_name)?;
        self.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
        Ok(())
    }

    /// Trigger a manual compaction on all column families.
    pub fn compact_all(&self) -> Result<()> {
        let table_names = self.list_tables()?;
        for name in &table_names {
            let cf_name = data_cf_name(name);
            self.compact_cf(&cf_name)?;
        }
        // Also compact metadata CF
        self.compact_cf(METADATA_CF)?;
        Ok(())
    }
}
