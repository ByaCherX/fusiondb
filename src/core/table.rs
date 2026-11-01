//! Table — CRUD operations on individual tables.
//!
//! Manages read, write, update, and delete operations for a single table.
//! Data is stored in RocksDB as Arrow IPC encoded bytes.

use crate::core::error::{FusionDBError, Result};
use crate::core::schema::FusionSchema;
use crate::storage::codec::{ArrowCodec, PrimaryKeyValue};
use crate::storage::engine::StorageEngine;
use crate::storage::metadata::TableMetadata;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use rocksdb::WriteBatch;
use std::collections::HashSet;
use std::sync::Arc;

/// Table — manages operations for a single database table.
///
/// Each `Table` instance corresponds to a RocksDB column family.
#[derive(Clone, Debug)]
pub struct Table {
    /// Table name
    name: String,
    /// FusionDB schema (includes primary key information)
    schema: FusionSchema,
    /// Arrow schema (for DataFusion compatibility)
    arrow_schema: Arc<Schema>,
    /// Storage engine reference
    storage: Arc<StorageEngine>,
    /// Data column family name
    data_cf: String,
}

impl Table {
    /// Create a new Table instance (crate-internal only).
    pub(crate) fn new(
        metadata: &TableMetadata,
        schema: FusionSchema,
        storage: Arc<StorageEngine>,
    ) -> Self {
        let arrow_schema = schema.arrow_schema().clone();
        Self {
            name: metadata.name.clone(),
            schema,
            arrow_schema,
            storage,
            data_cf: metadata.data_cf.clone(),
        }
    }

    /// Returns the table name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the FusionDB schema.
    pub fn schema(&self) -> &FusionSchema {
        &self.schema
    }

    /// Returns the Arrow schema (for DataFusion compatibility).
    pub fn arrow_schema(&self) -> Arc<Schema> {
        self.arrow_schema.clone()
    }

    // ========== WRITE OPERATIONS ==========

    /// Insert a `RecordBatch` of rows into the table.
    ///
    /// Each row is written as `primary_key_bytes -> Arrow_IPC_bytes` in RocksDB.
    /// Insertion is performed atomically for the entire batch via a `WriteBatch`.
    ///
    /// ### Write Path Architecture & Data Flow
    ///
    /// ```text
    /// [Client Insert Batch]
    ///        |
    ///        v
    /// 1. Validate Schema
    ///        |
    ///        v
    /// 2. Loop each Row:
    ///    - Encode PK to bytes (lexicographical sorting order)
    ///    - Check for Duplicate PK (Self-Lookup via StorageEngine::get)
    ///      +-------------------------------------------------------+
    ///      |  RocksDB Read Path (for validation):                  |
    ///      |  1. Search Active MemTable (In-memory skiplist)       |
    ///      |  2. Search Immutable MemTables (Read-only buffers)    |
    ///      |  3. Check Bloom Filters (In-memory SST block filter)  |
    ///      |  4. Search Block Cache (LRU uncompressed blocks)      |
    ///      |  5. File Read (SST file data blocks on disk)          |
    ///      +-------------------------------------------------------+
    ///    - If found -> return PrimaryKeyViolation
    ///    - If not found -> Slice row & Serialize using Arrow IPC Stream writer
    ///        |
    ///        v
    /// 3. Add to rocksdb::WriteBatch (wb.put_cf)
    ///        |
    ///        v
    /// 4. Commit WriteBatch (StorageEngine::write_batch)
    ///      +-------------------------------------------------------+
    ///      |  RocksDB Write Path:                                  |
    ///      |  1. Append batch to WAL (Write-Ahead Log) on disk     |
    ///      |  2. Insert values into Active MemTable                |
    ///      |  3. Trigger background Flush if MemTable is full      |
    ///      +-------------------------------------------------------+
    ///        |
    ///        v
    /// 5. Increment Metadata Row Count (Write to METADATA_CF)
    /// ```
    ///
    /// ### RocksDB Write Internals & System Call Behavior
    /// - **WAL Write**: RocksDB appends the data sequentially to the `.log` file on disk. This involves an OS `write` system call. Depending on configuration (`wal_bytes_per_sync` and `use_fsync`), it may trigger `fsync` or `fdatasync` to flush OS buffers to physical disk blocks.
    /// - **MemTable Insertion**: After the WAL write, threads write the keys and values into the MemTable. The MemTable uses a lock-free concurrent SkipList structure, which allows multiple worker threads to write concurrently without lock contention.
    /// - **Flush Thread Initiation**: When the active MemTable becomes full (exceeds `write_buffer_size_bytes`), it is marked as read-only and enters the queue of immutable MemTables. A new active MemTable is allocated. A background thread pool worker is signaled to perform a **Flush** operation:
    ///   - Reads the immutable MemTable contents sequentially.
    ///   - Writes them sorted to a Level 0 SST (Sorted String Table) file on disk.
    ///   - This triggers sequential disk I/O, writing blocks, writing indexes, and appending Bloom filters to the SST.
    ///
    /// ### Rust Code Example
    /// ```rust,no_run
    /// # use std::sync::Arc;
    /// # use arrow_array::{Int64Array, StringArray, RecordBatch};
    /// # use fusiondb::Table;
    /// # fn insert_demo(table: &Table) -> fusiondb::Result<()> {
    /// let batch = RecordBatch::try_new(
    ///     table.arrow_schema(),
    ///     vec![
    ///         Arc::new(Int64Array::from(vec![101, 102])),
    ///         Arc::new(StringArray::from(vec!["Item A", "Item B"])),
    ///     ]
    /// ).unwrap();
    /// table.insert(batch)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// - `Schema`: Schema mismatch between batch and table.
    /// - `PrimaryKeyViolation`: One of the rows contains an already existing primary key.
    /// - `Storage`: Lower-level RocksDB I/O or locking error.
    pub fn insert(&self, batch: RecordBatch) -> Result<()> {
        let num_rows = batch.num_rows();
        log::info!("Inserting {} rows into table '{}'", num_rows, self.name);
        let start = std::time::Instant::now();

        // Schema validation
        if let Err(e) = self.validate_schema(&batch) {
            log::warn!("Schema validation failed for insert in table '{}': {:?}", self.name, e);
            return Err(e);
        }

        let pk_index = self.schema.primary_key_index();
        let pk_col = batch.column(pk_index);

        // Per-table lock so writes to different tables proceed concurrently
        let table_lock = self.storage.table_write_guard(&self.name);
        let _write_guard = table_lock.lock().unwrap();

        // Hold the Arc in a local variable so the CF handle lives long enough
        let db = self.storage.db();
        let cf = db.cf_handle(&self.data_cf).ok_or_else(|| {
            let err = FusionDBError::invalid_op(format!("CF '{}' not found", self.data_cf));
            log::error!("{}", err);
            err
        })?;

        // 1. Pre-encode primary keys and verify in-batch uniqueness
        let mut key_bytes_list: Vec<Vec<u8>> = Vec::with_capacity(num_rows);
        let mut staged_keys: HashSet<Vec<u8>> = HashSet::with_capacity(num_rows);

        for row_idx in 0..num_rows {
            let key_bytes = ArrowCodec::encode_primary_key(pk_col.as_ref(), row_idx)?;
            if !staged_keys.insert(key_bytes.clone()) {
                let pk_display = format!("{:?}", &key_bytes);
                let err = FusionDBError::pk_violation(pk_display, &self.name);
                log::warn!("Primary key violation within batch for table '{}': {:?}", self.name, err);
                return Err(err);
            }
            key_bytes_list.push(key_bytes);
        }

        // 2. Batch check for existing keys in RocksDB via multi_get
        let existing = self.storage.multi_get(&self.data_cf, &key_bytes_list)?;
        for (row_idx, opt) in existing.into_iter().enumerate() {
            if opt.is_some() {
                let pk_display = format!("{:?}", &key_bytes_list[row_idx]);
                let err = FusionDBError::pk_violation(pk_display, &self.name);
                log::warn!("Primary key violation during insert in table '{}': {:?}", self.name, err);
                return Err(err);
            }
        }

        // 3. Encode rows and add to atomic WriteBatch
        let mut wb = WriteBatch::default();
        for (row_idx, key_bytes) in key_bytes_list.into_iter().enumerate() {
            let row_batch = ArrowCodec::extract_row(&batch, row_idx)?;
            let value_bytes = ArrowCodec::encode(&row_batch)?;
            wb.put_cf(&cf, &key_bytes, &value_bytes);
        }

        // Commit atomically. This ensures that either all rows are inserted or none.
        self.storage.write_batch(wb)?;

        // Update row count in table metadata CF
        self.storage.increment_row_count(&self.name, num_rows as i64)?;

        log::info!("Successfully inserted {} rows into table '{}' in {:?}", num_rows, self.name, start.elapsed());
        Ok(())
    }

    /// Read a single row by primary key.
    ///
    /// ### Point Query Lookup path:
    /// 1. **MemTable Check**: RocksDB searches the active MemTable (in-memory skiplist). If found, returns immediately.
    /// 2. **Immutable MemTables Check**: If not in active MemTable, searches read-only MemTables in memory.
    /// 3. **Bloom Filter Check**: If not in memory, checks the Block-Based SST filter blocks loaded in the Block Cache.
    ///    - If the Bloom filter returns `false`, it mathematically guarantees the key does NOT exist in that SST file, skipping it.
    ///    - If it returns `true` (or there's a false positive), it proceeds to search that SST file.
    /// 4. **Block Cache Search**: Looks up the target data block of the SST file in the uncompressed Block Cache.
    /// 5. **Disk File Read**: If cache miss, performs a system call (e.g. `pread`) to load the block from the SST file on disk, parses it, caches it, and retrieves the value.
    ///
    /// The returned bytes are then decoded from the Arrow IPC format into a `RecordBatch`.
    ///
    /// ### Rust Code Example
    /// ```rust,no_run
    /// # use fusiondb::Table;
    /// # fn get_demo(table: &Table) -> fusiondb::Result<()> {
    /// let row = table.get(101i64)?;
    /// assert_eq!(row.num_rows(), 1);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// - `RowNotFound`: No row exists with the specified primary key.
    /// - `Storage`: Lower-level RocksDB point lookup error.
    pub fn get(&self, pk: impl Into<PrimaryKeyValue>) -> Result<RecordBatch> {
        let pk_value: PrimaryKeyValue = pk.into();
        log::debug!("Getting row from table '{}' with key: {}", self.name, pk_value.display());
        let key_bytes = pk_value.to_bytes();

        let value_bytes = match self.storage.get(&self.data_cf, &key_bytes)? {
            Some(v) => v,
            None => {
                let err = FusionDBError::row_not_found(pk_value.display());
                log::debug!("Row not found in table '{}' with key: {}", self.name, pk_value.display());
                return Err(err);
            }
        };

        ArrowCodec::decode(&value_bytes)
    }

    /// Delete a row by primary key.
    ///
    /// ### RocksDB Deletion Mechanism (Tombstones)
    /// In LSM-Tree engines like RocksDB, deletes do not immediately erase data from the disk. Instead:
    /// 1. A point lookup is performed to verify the key exists (checking MemTable, Bloom Filters, SSTs).
    /// 2. If the key exists, RocksDB appends a **Tombstone** (deletion marker) to the WAL and inserts it into the active MemTable.
    /// 3. The old record remains on disk in older SST levels.
    /// 4. During **Compaction**, background threads merge SST files, detect the tombstone, and permanently discard both the tombstone and the deleted record, reclaiming disk space.
    ///
    /// # Errors
    /// - `RowNotFound`: Key does not exist, so deletion cannot proceed.
    /// - `Storage`: Lower-level WAL write or write-buffer allocation error.
    pub fn delete(&self, pk: impl Into<PrimaryKeyValue>) -> Result<()> {
        let pk_value: PrimaryKeyValue = pk.into();
        log::info!("Deleting row from table '{}' with key: {}", self.name, pk_value.display());
        let start = std::time::Instant::now();
        let key_bytes = pk_value.to_bytes();

        let table_lock = self.storage.table_write_guard(&self.name);
        let _write_guard = table_lock.lock().unwrap();

        // Verify the row exists
        if self.storage.get(&self.data_cf, &key_bytes)?.is_none() {
            let err = FusionDBError::row_not_found(pk_value.display());
            log::warn!("Failed delete - row not found in table '{}' with key: {}", self.name, pk_value.display());
            return Err(err);
        }

        self.storage.delete(&self.data_cf, &key_bytes)?;
        self.storage.increment_row_count(&self.name, -1)?;

        log::info!("Successfully deleted row from table '{}' in {:?}", self.name, start.elapsed());
        Ok(())
    }

    /// Update a row by primary key.
    ///
    /// Replaces all columns of the existing row with the new values.
    ///
    /// ### Update Mechanism (Key Shadowing)
    /// Like deletions, updates in RocksDB do not modify data in-place on disk:
    /// 1. Verifies the row exists and that the primary key value remains unchanged.
    /// 2. Serializes the new row as Arrow IPC bytes.
    /// 3. Appends a new write to the WAL and active MemTable for the same key.
    /// 4. This new entry **shadows** the older entry. Any subsequent point read or scan will see the new entry first because it has a higher sequence number.
    /// 5. The older version is cleaned up during compaction.
    ///
    /// # Arguments
    /// * `pk` - Primary key of the row to update
    /// * `batch` - New data (must contain exactly 1 row)
    pub fn update(&self, pk: impl Into<PrimaryKeyValue>, batch: RecordBatch) -> Result<()> {
        let pk_value: PrimaryKeyValue = pk.into();
        log::info!("Updating row in table '{}' with key: {}", self.name, pk_value.display());
        let start = std::time::Instant::now();

        // Schema validation
        if let Err(e) = self.validate_schema(&batch) {
            log::warn!("Schema validation failed for update in table '{}': {:?}", self.name, e);
            return Err(e);
        }

        if batch.num_rows() != 1 {
            let err = FusionDBError::invalid_op("Update batch must contain exactly 1 row");
            log::warn!("Invalid update on table '{}': {}", self.name, err);
            return Err(err);
        }

        let key_bytes = pk_value.to_bytes();

        let table_lock = self.storage.table_write_guard(&self.name);
        let _write_guard = table_lock.lock().unwrap();

        // Verify the row exists
        if self.storage.get(&self.data_cf, &key_bytes)?.is_none() {
            let err = FusionDBError::row_not_found(pk_value.display());
            log::warn!("Failed update - row not found in table '{}' with key: {}", self.name, pk_value.display());
            return Err(err);
        }

        // Verify the primary key value hasn't changed
        let new_pk_col = batch.column(self.schema.primary_key_index());
        let new_pk_bytes = ArrowCodec::encode_primary_key(new_pk_col.as_ref(), 0)?;
        if new_pk_bytes != key_bytes {
            let err = FusionDBError::invalid_op("Cannot update primary key value");
            log::warn!("Failed update on table '{}': {}", self.name, err);
            return Err(err);
        }

        // Encode and write the new value
        let value_bytes = ArrowCodec::encode(&batch)?;
        self.storage.put(&self.data_cf, &key_bytes, &value_bytes)?;

        log::info!("Successfully updated row in table '{}' in {:?}", self.name, start.elapsed());
        Ok(())
    }

    // ========== READ OPERATIONS ==========

    /// Scan the table with optional projection and limits.
    ///
    /// ### Scan Architecture & RocksDB Iterator Internals
    /// 1. **RocksDB Iterator Allocation**: Allocates a RocksDB Iterator over the data column family starting from the minimum key.
    /// 2. **Iterator Scanning**: RocksDB reads keys/values sequentially:
    ///    - Iterates over active and immutable MemTables (merging them on-the-fly).
    ///    - Iterates over SST data blocks in different levels, using block indexes and bloom filters to speed up reads.
    ///    - If `limit` is specified, it stops early, reducing disk I/O.
    /// 3. **Batch Chunking & Concatenation**:
    ///    - Each RocksDB value is parsed as a single-row `RecordBatch`.
    ///    - Multiple single-row batches are concatenated using `arrow::compute::concat_batches` into larger batches 
    ///      (default 1024 rows) to align with standard columnar processing engines and maximize query execution performance.
    /// 4. **Projection Pushdown**: Applies column filter list, discarding unused columns during parsing to reduce CPU cache churn.
    ///
    /// > [!WARNING]
    /// > **Memory Warning**: The current implementation loads all scanned rows from RocksDB into memory at once before returning. 
    /// For large tables, this could lead to Out Of Memory (OOM) errors. In production, utilize the 
    /// DataFusion SQL interface which streams chunks lazily via the `RocksDBStream`.
    ///
    /// # Arguments
    /// * `projection` - Column indices to return (None = all columns)
    /// * `limit` - Maximum number of rows to return (None = all rows)
    ///
    /// # Returns
    /// A list of concatenated `RecordBatch`es (each batch defaults to 1024 rows)
    pub fn scan(
        &self,
        projection: Option<&[usize]>,
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        const BATCH_SIZE: usize = 1024;
        log::info!("Scanning table '{}' with projection: {:?}, limit: {:?}", self.name, projection, limit);
        let start = std::time::Instant::now();

        let target_schema = if let Some(proj) = projection {
            let fields: Vec<_> = proj
                .iter()
                .map(|&i| self.arrow_schema.field(i).clone())
                .collect();
            Arc::new(arrow_schema::Schema::new(fields))
        } else {
            self.arrow_schema.clone()
        };

        let db = self.storage.db();
        let cf = db.cf_handle(&self.data_cf).ok_or_else(|| {
            FusionDBError::invalid_op(format!("CF '{}' not found", self.data_cf))
        })?;

        let iter = db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        let mut result_batches = Vec::new();
        let mut chunk: Vec<RecordBatch> = Vec::with_capacity(BATCH_SIZE);
        let mut total_rows = 0;

        for item in iter {
            let (_, value_bytes) = item?;
            let row_batch = ArrowCodec::decode(&value_bytes)?;
            let projected = if let Some(proj) = projection {
                ArrowCodec::project(&row_batch, proj)?
            } else {
                row_batch
            };

            chunk.push(projected);
            total_rows += 1;

            if chunk.len() >= BATCH_SIZE {
                let merged = merge_record_batches(&chunk, target_schema.clone())?;
                result_batches.push(merged);
                chunk.clear();
            }

            if let Some(lim) = limit {
                if total_rows >= lim {
                    break;
                }
            }
        }

        if !chunk.is_empty() {
            let merged = merge_record_batches(&chunk, target_schema)?;
            result_batches.push(merged);
        }

        log::info!("Successfully scanned {} rows in {} batches from table '{}' in {:?}", total_rows, result_batches.len(), self.name, start.elapsed());
        Ok(result_batches)
    }

    /// Returns the row count from metadata (no RocksDB scan required).
    pub fn count(&self) -> Result<u64> {
        let meta = self
            .storage
            .load_metadata(&self.name)?
            .ok_or_else(|| FusionDBError::table_not_found(&self.name))?;
        Ok(meta.row_count)
    }

    // ========== PRIVATE HELPERS ==========

    /// Validate that a batch's schema matches the table schema.
    fn validate_schema(&self, batch: &RecordBatch) -> Result<()> {
        let batch_schema = batch.schema();
        let table_schema = &self.arrow_schema;

        if batch_schema.fields().len() != table_schema.fields().len() {
            return Err(FusionDBError::schema(format!(
                "Schema mismatch: batch has {} fields, table has {}",
                batch_schema.fields().len(),
                table_schema.fields().len()
            )));
        }

        for (i, (batch_field, table_field)) in batch_schema
            .fields()
            .iter()
            .zip(table_schema.fields().iter())
            .enumerate()
        {
            if batch_field.name() != table_field.name() {
                return Err(FusionDBError::schema(format!(
                    "Field name mismatch at index {}: expected '{}', got '{}'",
                    i,
                    table_field.name(),
                    batch_field.name()
                )));
            }
            if batch_field.data_type() != table_field.data_type() {
                return Err(FusionDBError::schema(format!(
                    "Field type mismatch for '{}': expected {:?}, got {:?}",
                    table_field.name(),
                    table_field.data_type(),
                    batch_field.data_type()
                )));
            }
        }

        Ok(())
    }
}

/// Merge multiple single-row RecordBatches into one.
fn merge_record_batches(batches: &[RecordBatch], schema: Arc<Schema>) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }

    arrow::compute::concat_batches(&schema, batches).map_err(Into::into)
}
