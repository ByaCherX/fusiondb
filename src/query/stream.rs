//! RecordBatch stream implementation.
//!
//! Converts synchronous RocksDB scans into a true streaming DataFusion asynchronous `RecordBatchStream`.
//!
//! ### Streaming Design & Stream Pipeline
//! Unlike naive buffering implementations that load the entire table into memory,
//! `RocksDBStream` lazily iterates over RocksDB on a background thread and streams
//! batches through a bounded channel with backpressure.
//!
//! ```text
//! RocksDB Iterator (Background Worker Thread)
//!        | (Lazy read up to DEFAULT_BATCH_SIZE rows)
//!        v
//! Arrow IPC Decoder (Decode single-row values)
//!        | (Optional column projection pushdown)
//!        v
//! Batch Concatenation (Group into 1024-row blocks)
//!        v
//! Bounded Channel (tokio::sync::mpsc, buffer size = 2)
//!        v
//! DataFusion Operators (Filter, Projection, HashAggregate, etc.)
//! ```

use crate::storage::codec::ArrowCodec;
use crate::storage::engine::StorageEngine;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Default batch size (number of rows per output batch).
pub const DEFAULT_BATCH_SIZE: usize = 1024;

/// Primary key bounds for optimized scanning (filter pushdown).
#[derive(Debug, Clone, Default)]
pub struct KeyBounds {
    /// Exact primary key point lookup (e.g. `id = 5`)
    pub point_key: Option<Vec<u8>>,
    /// Lower bound on primary key (bytes, inclusive)
    pub lower_bound: Option<(Vec<u8>, bool)>,
    /// Upper bound on primary key (bytes, inclusive)
    pub upper_bound: Option<(Vec<u8>, bool)>,
}

/// Converts RocksDB data into a lazy, bounded RecordBatch stream for DataFusion.
///
/// Implements [`Stream`] and [`RecordBatchStream`] traits.
pub struct RocksDBStream {
    /// Stream schema (after projection)
    schema: SchemaRef,
    /// Bounded channel receiver streaming batches from RocksDB worker thread
    receiver: tokio::sync::mpsc::Receiver<datafusion::error::Result<RecordBatch>>,
}

impl RocksDBStream {
    /// Create a new lazy streaming RocksDB reader.
    ///
    /// # Arguments
    /// * `storage` - Storage engine reference
    /// * `data_cf` - Data column family name
    /// * `schema` - Target schema (after projection)
    /// * `projection` - Column indices to include (None = all)
    /// * `limit` - Maximum number of rows to read
    /// * `bounds` - Optional primary key bounds for pushdown optimization
    pub fn new(
        storage: Arc<StorageEngine>,
        data_cf: &str,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        bounds: KeyBounds,
    ) -> datafusion::error::Result<Self> {
        // Bounded channel to enforce backpressure and prevent OOM
        let (tx, receiver) = tokio::sync::mpsc::channel(2);

        let storage_clone = storage;
        let data_cf_owned = data_cf.to_string();
        let schema_clone = schema.clone();

        std::thread::Builder::new()
            .name("rocksdb-stream-worker".to_string())
            .spawn(move || {
                run_stream_worker(
                    storage_clone,
                    data_cf_owned,
                    schema_clone,
                    projection,
                    limit,
                    bounds,
                    tx,
                );
            })
            .map_err(|e| DataFusionError::Execution(format!("Failed to spawn scan thread: {}", e)))?;

        Ok(Self { schema, receiver })
    }
}

/// Background worker function that pulls rows from RocksDB and transmits batches via channel.
fn run_stream_worker(
    storage: Arc<StorageEngine>,
    data_cf: String,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    bounds: KeyBounds,
    tx: tokio::sync::mpsc::Sender<datafusion::error::Result<RecordBatch>>,
) {
    // 1. Fast Path: Point Lookup
    if let Some(pk) = bounds.point_key {
        match storage.get(&data_cf, &pk) {
            Ok(Some(value_bytes)) => {
                match ArrowCodec::decode(&value_bytes) {
                    Ok(row_batch) => {
                        let final_batch = if let Some(ref proj) = projection {
                            match ArrowCodec::project(&row_batch, proj) {
                                Ok(p) => p,
                                Err(e) => {
                                    let _ = tx.blocking_send(Err(DataFusionError::External(Box::new(e))));
                                    return;
                                }
                            }
                        } else {
                            row_batch
                        };
                        let _ = tx.blocking_send(Ok(final_batch));
                    }
                    Err(e) => {
                        let _ = tx.blocking_send(Err(DataFusionError::External(Box::new(e))));
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                let _ = tx.blocking_send(Err(DataFusionError::External(Box::new(e))));
            }
        }
        return;
    }

    // 2. Scan / Range Scan Path
    let db = storage.db();
    let cf = match db.cf_handle(&data_cf) {
        Some(cf) => cf,
        None => {
            let _ = tx.blocking_send(Err(DataFusionError::Execution(format!(
                "Column family '{}' not found",
                data_cf
            ))));
            return;
        }
    };

    let mode = if let Some((ref start_bytes, _)) = bounds.lower_bound {
        rocksdb::IteratorMode::From(start_bytes, rocksdb::Direction::Forward)
    } else {
        rocksdb::IteratorMode::Start
    };

    let iter = db.iterator_cf(&cf, mode);
    let mut chunk: Vec<RecordBatch> = Vec::with_capacity(DEFAULT_BATCH_SIZE);
    let mut total_yielded = 0;

    for item in iter {
        let (k, v) = match item {
            Ok(kv) => kv,
            Err(e) => {
                let _ = tx.blocking_send(Err(DataFusionError::External(Box::new(
                    crate::core::error::FusionDBError::Storage(e),
                ))));
                return;
            }
        };

        // Check lower bound exclusivity
        if let Some((ref start_bytes, inclusive)) = bounds.lower_bound {
            if !inclusive && k.as_ref() == start_bytes.as_slice() {
                continue;
            }
        }

        // Check upper bound
        if let Some((ref end_bytes, inclusive)) = bounds.upper_bound {
            let cmp = k.as_ref().cmp(end_bytes.as_slice());
            if cmp == std::cmp::Ordering::Greater || (!inclusive && cmp == std::cmp::Ordering::Equal) {
                break;
            }
        }

        // Decode single-row RecordBatch
        let row_batch = match ArrowCodec::decode(&v) {
            Ok(rb) => rb,
            Err(e) => {
                let _ = tx.blocking_send(Err(DataFusionError::External(Box::new(e))));
                return;
            }
        };

        // Apply projection if specified
        let projected = if let Some(ref proj) = projection {
            match ArrowCodec::project(&row_batch, proj) {
                Ok(p) => p,
                Err(e) => {
                    let _ = tx.blocking_send(Err(DataFusionError::External(Box::new(e))));
                    return;
                }
            }
        } else {
            row_batch
        };

        chunk.push(projected);
        total_yielded += 1;

        // When chunk is full, merge into a columnar batch and send
        if chunk.len() >= DEFAULT_BATCH_SIZE {
            match arrow::compute::concat_batches(&schema, &chunk) {
                Ok(merged) => {
                    if tx.blocking_send(Ok(merged)).is_err() {
                        // Receiver dropped (e.g. query completed early or was cancelled)
                        return;
                    }
                }
                Err(e) => {
                    let _ = tx.blocking_send(Err(DataFusionError::ArrowError(e, None)));
                    return;
                }
            }
            chunk.clear();
        }

        if let Some(lim) = limit {
            if total_yielded >= lim {
                break;
            }
        }
    }

    // Flush any remaining rows
    if !chunk.is_empty() {
        match arrow::compute::concat_batches(&schema, &chunk) {
            Ok(merged) => {
                let _ = tx.blocking_send(Ok(merged));
            }
            Err(e) => {
                let _ = tx.blocking_send(Err(DataFusionError::ArrowError(e, None)));
            }
        }
    }
}

impl Stream for RocksDBStream {
    type Item = datafusion::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl RecordBatchStream for RocksDBStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
