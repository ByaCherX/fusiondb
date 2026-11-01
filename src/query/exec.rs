//! Custom ExecutionPlan implementation.
//!
//! Integrates RocksDB scan operations into Apache DataFusion's physical plan execution system.
//!
//! ### Physical Scan Execution Lifecycle
//! 1. DataFusion compiles the SQL query and resolves the logical plan to a physical plan tree.
//! 2. The `RocksDBExec` node represents the leaf physical scan.
//! 3. When `SessionContext::collect` is called:
//!    - The physical execution engine traverses the physical plan tree.
//!    - It invokes `execute(partition, context)` on each leaf node.
//!    - `execute()` initiates and returns a `RocksDBStream`, which pulls data from RocksDB.
//!    - Parents of this node (filters, projects, aggregations) pull record batches from this stream.

use crate::query::stream::{KeyBounds, RocksDBStream};
use crate::storage::engine::StorageEngine;
use arrow_schema::SchemaRef;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::execution_plan::EmissionType;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// RocksDB read physical plan node.
///
/// Implements DataFusion's physical [`ExecutionPlan`] trait.
///
/// ### Partitioning & Concurrency Notes
/// We configure `Partitioning::UnknownPartitioning(1)` for the scanner. This means:
/// - The scanning phase runs on a single thread partition.
/// - In more advanced architectures, range scans can be partitioned by splitting RocksDB key-ranges
///   and scanning key-ranges concurrently using separate threads.
#[derive(Debug)]
pub struct RocksDBExec {
    /// Table name (for display)
    table_name: String,
    /// Data column family name
    data_cf: String,
    /// Storage engine reference
    storage: Arc<StorageEngine>,
    /// Output schema (after projection)
    schema: SchemaRef,
    /// Projection indices on the base schema
    projection: Option<Vec<usize>>,
    /// Row limit
    limit: Option<usize>,
    /// Primary key bounds for filter pushdown
    bounds: KeyBounds,
    /// Plan properties
    properties: PlanProperties,
}

impl RocksDBExec {
    /// Create a new RocksDBExec plan node.
    pub fn new(
        table_name: String,
        data_cf: String,
        storage: Arc<StorageEngine>,
        base_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        bounds: KeyBounds,
    ) -> Self {
        // Compute the projected output schema
        let output_schema = if let Some(ref proj) = projection {
            let projected_fields: Vec<_> =
                proj.iter().map(|&i| base_schema.field(i).clone()).collect();
            Arc::new(arrow_schema::Schema::new(projected_fields))
        } else {
            base_schema.clone()
        };

        // Plan properties — DataFusion 48 API
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            table_name,
            data_cf,
            storage,
            schema: output_schema,
            projection,
            limit,
            bounds,
            properties,
        }
    }
}

impl DisplayAs for RocksDBExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RocksDBExec [table={}, limit={:?}, bounds={:?}]",
            self.table_name, self.limit, self.bounds
        )
    }
}

impl ExecutionPlan for RocksDBExec {
    fn name(&self) -> &str {
        "RocksDBExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let stream = RocksDBStream::new(
            self.storage.clone(),
            &self.data_cf,
            self.schema.clone(),
            self.projection.clone(),
            self.limit,
            self.bounds.clone(),
        )?;
        Ok(Box::pin(stream))
    }
}
