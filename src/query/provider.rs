//! DataFusion TableProvider implementation.
//!
//! Exposes RocksDB tables to the DataFusion query engine,
//! enabling SQL queries to run directly on top of RocksDB data with
//! primary key filter pushdown optimization.

use crate::query::exec::RocksDBExec;
use crate::query::stream::KeyBounds;
use crate::storage::engine::StorageEngine;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use std::any::Any;
use std::sync::Arc;

/// RocksDB TableProvider for DataFusion integration.
///
/// Bridges the logical catalog system of DataFusion to RocksDB storage.
#[derive(Debug)]
pub struct RocksDBTableProvider {
    /// Table name
    table_name: String,
    /// Schema exposed to DataFusion (metadata stripped for plan compatibility)
    datafusion_schema: SchemaRef,
    /// Storage engine reference
    storage: Arc<StorageEngine>,
    /// Data column family name
    data_cf: String,
    /// Primary key column name for filter pushdown
    pk_column: Option<String>,
}

impl RocksDBTableProvider {
    /// Create a new provider with primary key column for filter pushdown.
    pub fn new(
        table_name: String,
        schema: SchemaRef,
        storage: Arc<StorageEngine>,
        data_cf: String,
        pk_column: Option<String>,
    ) -> Self {
        // Create a metadata-free schema for DataFusion.
        // DataFusion drops metadata during logical → physical plan conversion,
        // so we strip it upfront to prevent schema mismatches.
        let datafusion_schema = Arc::new(arrow_schema::Schema::new(
            schema.fields().to_vec(),
        ));

        Self {
            table_name,
            datafusion_schema,
            storage,
            data_cf,
            pk_column,
        }
    }
}

/// Convert supported scalar literal into big-endian primary key byte array.
fn scalar_to_key_bytes(scalar: &ScalarValue) -> Option<Vec<u8>> {
    match scalar {
        ScalarValue::Int8(Some(v)) => Some(v.to_be_bytes().to_vec()),
        ScalarValue::Int16(Some(v)) => Some(v.to_be_bytes().to_vec()),
        ScalarValue::Int32(Some(v)) => Some(v.to_be_bytes().to_vec()),
        ScalarValue::Int64(Some(v)) => Some(v.to_be_bytes().to_vec()),
        ScalarValue::UInt8(Some(v)) => Some(v.to_be_bytes().to_vec()),
        ScalarValue::UInt16(Some(v)) => Some(v.to_be_bytes().to_vec()),
        ScalarValue::UInt32(Some(v)) => Some(v.to_be_bytes().to_vec()),
        ScalarValue::UInt64(Some(v)) => Some(v.to_be_bytes().to_vec()),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.as_bytes().to_vec()),
        _ => None,
    }
}

/// Extracts primary key equality or range bounds from an expression tree.
fn extract_bounds_from_expr(expr: &Expr, pk_col: &str, bounds: &mut KeyBounds) -> bool {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let left_is_pk = match left.as_ref() {
                Expr::Column(c) if c.name == pk_col => true,
                _ => false,
            };
            let right_is_pk = match right.as_ref() {
                Expr::Column(c) if c.name == pk_col => true,
                _ => false,
            };

            let left_lit = match left.as_ref() {
                Expr::Literal(s, _) => scalar_to_key_bytes(s),
                _ => None,
            };
            let right_lit = match right.as_ref() {
                Expr::Literal(s, _) => scalar_to_key_bytes(s),
                _ => None,
            };

            if left_is_pk && right_lit.is_some() {
                let bytes = right_lit.unwrap();
                match op {
                    Operator::Eq => {
                        bounds.point_key = Some(bytes);
                        true
                    }
                    Operator::Gt => {
                        bounds.lower_bound = Some((bytes, false));
                        true
                    }
                    Operator::GtEq => {
                        bounds.lower_bound = Some((bytes, true));
                        true
                    }
                    Operator::Lt => {
                        bounds.upper_bound = Some((bytes, false));
                        true
                    }
                    Operator::LtEq => {
                        bounds.upper_bound = Some((bytes, true));
                        true
                    }
                    _ => false,
                }
            } else if right_is_pk && left_lit.is_some() {
                let bytes = left_lit.unwrap();
                match op {
                    Operator::Eq => {
                        bounds.point_key = Some(bytes);
                        true
                    }
                    Operator::Lt => {
                        bounds.lower_bound = Some((bytes, false));
                        true
                    }
                    Operator::LtEq => {
                        bounds.lower_bound = Some((bytes, true));
                        true
                    }
                    Operator::Gt => {
                        bounds.upper_bound = Some((bytes, false));
                        true
                    }
                    Operator::GtEq => {
                        bounds.upper_bound = Some((bytes, true));
                        true
                    }
                    _ => false,
                }
            } else {
                false
            }
        }
        _ => false,
    }
}

#[async_trait]
impl TableProvider for RocksDBTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the metadata-free schema to DataFusion.
    fn schema(&self) -> SchemaRef {
        self.datafusion_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Check which filter expressions can be pushed down to RocksDB key lookups/range scans.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        let mut results = Vec::with_capacity(filters.len());
        for expr in filters {
            if let Some(ref pk) = self.pk_column {
                let mut dummy = KeyBounds::default();
                if extract_bounds_from_expr(expr, pk, &mut dummy) {
                    // Inexact ensures DataFusion will double-check results while RocksDB restricts rows scanned
                    results.push(TableProviderFilterPushDown::Inexact);
                    continue;
                }
            }
            results.push(TableProviderFilterPushDown::Unsupported);
        }
        Ok(results)
    }

    /// Build the DataFusion scan plan with bounds pushdown.
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut bounds = KeyBounds::default();
        if let Some(ref pk) = self.pk_column {
            for filter in filters {
                extract_bounds_from_expr(filter, pk, &mut bounds);
            }
        }

        let exec = RocksDBExec::new(
            self.table_name.clone(),
            self.data_cf.clone(),
            self.storage.clone(),
            self.datafusion_schema.clone(),
            projection.cloned(),
            limit,
            bounds,
        );
        Ok(Arc::new(exec))
    }
}
