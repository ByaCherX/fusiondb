//! CLI command definitions and execution logic.

use clap::{Parser, Subcommand};
use colored::Colorize;
use fusiondb::Database;
use std::time::Instant;
use crate::repl;

/// FusionDB — An embedded database powered by DataFusion and RocksDB.
#[derive(Parser, Debug)]
#[command(
    name = "fusiondb",
    version,
    about = "FusionDB CLI — manage and query your embedded database",
    long_about = "FusionDB is an embedded, serverless database engine.\n\
                  Use this CLI to open an interactive REPL, run SQL queries,\n\
                  manage tables, and import/export data."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Open the database in interactive REPL mode.
    #[command(alias = "shell")]
    Open {
        /// Path to the database directory.
        path: String,
    },

    /// Execute a single SQL query and print results.
    Query {
        /// Path to the database directory.
        path: String,
        /// SQL query string to execute.
        sql: String,
    },

    /// List all tables in the database.
    Tables {
        /// Path to the database directory.
        path: String,
    },

    /// Show the schema of a specific table.
    Schema {
        /// Path to the database directory.
        path: String,
        /// Table name.
        table: String,
    },

    /// Show database statistics (row counts, sizes, RocksDB internals).
    Stats {
        /// Path to the database directory.
        path: String,
    },

    /// Export a table to CSV or JSON format.
    Export {
        /// Path to the database directory.
        path: String,
        /// Table name to export.
        table: String,
        /// Output file path.
        #[arg(short, long)]
        output: String,
        /// Output format: csv or json.
        #[arg(short, long, default_value = "csv")]
        format: String,
    },

    /// Import data from a CSV file into a table.
    Import {
        /// Path to the database directory.
        path: String,
        /// Target table name.
        table: String,
        /// Input CSV file path.
        file: String,
    },

    /// Trigger manual compaction on all tables.
    Compact {
        /// Path to the database directory.
        path: String,
    },

    /// Drop a table and all its data.
    Drop {
        /// Path to the database directory.
        path: String,
        /// Table name to drop.
        table: String,
        /// Skip confirmation prompt.
        #[arg(short = 'y', long)]
        yes: bool,
    },
}

impl Cli {
    pub fn execute(self) {
        match self.command {
            Commands::Open { path } => cmd_open(&path),
            Commands::Query { path, sql } => cmd_query(&path, &sql),
            Commands::Tables { path } => cmd_tables(&path),
            Commands::Schema { path, table } => cmd_schema(&path, &table),
            Commands::Stats { path } => cmd_stats(&path),
            Commands::Export {
                path,
                table,
                output,
                format,
            } => cmd_export(&path, &table, &output, &format),
            Commands::Import { path, table, file } => cmd_import(&path, &table, &file),
            Commands::Compact { path } => cmd_compact(&path),
            Commands::Drop { path, table, yes } => cmd_drop(&path, &table, yes),
        }
    }
}

/// Open the database in interactive REPL mode.
fn cmd_open(path: &str) {
    let db = match open_db(path) {
        Some(db) => db,
        None => return,
    };

    repl::run_repl(&db);
}

/// Execute a single SQL query.
fn cmd_query(path: &str, sql: &str) {
    let db = match open_db(path) {
        Some(db) => db,
        None => return,
    };

    repl::execute_sql(&db, sql);
}

/// List all tables.
fn cmd_tables(path: &str) {
    let db = match open_db(path) {
        Some(db) => db,
        None => return,
    };

    repl::show_tables(&db);
}

/// Show table schema.
fn cmd_schema(path: &str, table: &str) {
    let db = match open_db(path) {
        Some(db) => db,
        None => return,
    };

    repl::show_schema(&db, table);
}

/// Show database statistics.
fn cmd_stats(path: &str) {
    let db = match open_db(path) {
        Some(db) => db,
        None => return,
    };

    repl::show_stats(&db);
}

/// Export a table to CSV or JSON.
fn cmd_export(path: &str, table_name: &str, output: &str, format: &str) {
    let db = match open_db(path) {
        Some(db) => db,
        None => return,
    };

    let tbl = match db.table(table_name) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("{} {}", "error:".red().bold(), e);
            return;
        }
    };

    let batches = match tbl.scan(None, None) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("{} {}", "error:".red().bold(), e);
            return;
        }
    };

    let start = Instant::now();

    match format.to_lowercase().as_str() {
        "csv" => {
            if let Err(e) = export_csv(&batches, output) {
                eprintln!("{} {}", "error:".red().bold(), e);
                return;
            }
        }
        "json" => {
            if let Err(e) = export_json(&batches, output) {
                eprintln!("{} {}", "error:".red().bold(), e);
                return;
            }
        }
        _ => {
            eprintln!(
                "{} Unsupported format '{}'. Use 'csv' or 'json'.",
                "error:".red().bold(),
                format
            );
            return;
        }
    }

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!(
        "{} Exported {} rows to '{}' in {:.1}ms",
        "✓".green().bold(),
        total_rows,
        output,
        start.elapsed().as_secs_f64() * 1000.0
    );
}

/// Import data from a CSV file.
fn cmd_import(path: &str, table_name: &str, file: &str) {
    let db = match open_db(path) {
        Some(db) => db,
        None => return,
    };

    let tbl = match db.table(table_name) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("{} {}", "error:".red().bold(), e);
            return;
        }
    };

    let start = Instant::now();

    // Read CSV file
    let mut reader = match csv::Reader::from_path(file) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("{} Failed to open CSV file: {}", "error:".red().bold(), e);
            return;
        }
    };

    let schema = tbl.arrow_schema();
    let mut total_rows = 0usize;

    // Read all rows into columnar arrays
    let mut columns: Vec<Vec<String>> = vec![Vec::new(); schema.fields().len()];

    for result in reader.records() {
        match result {
            Ok(record) => {
                for (i, value) in record.iter().enumerate() {
                    if i < columns.len() {
                        columns[i].push(value.to_string());
                    }
                }
                total_rows += 1;
            }
            Err(e) => {
                eprintln!("{} CSV read error: {}", "error:".red().bold(), e);
                return;
            }
        }
    }

    if total_rows == 0 {
        println!("{}", "(no rows to import)".dimmed());
        return;
    }

    // Build RecordBatch from CSV data
    let batch = match build_batch_from_csv_columns(&schema, &columns) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("{} Failed to build RecordBatch: {}", "error:".red().bold(), e);
            return;
        }
    };

    if let Err(e) = tbl.insert(batch) {
        eprintln!("{} Insert failed: {}", "error:".red().bold(), e);
        return;
    }

    println!(
        "{} Imported {} rows into '{}' in {:.1}ms",
        "✓".green().bold(),
        total_rows,
        table_name,
        start.elapsed().as_secs_f64() * 1000.0
    );
}

/// Trigger manual compaction.
fn cmd_compact(path: &str) {
    let db = match open_db(path) {
        Some(db) => db,
        None => return,
    };

    println!("{}", "Running manual compaction...".yellow());
    let start = Instant::now();

    if let Err(e) = db.storage().compact_all() {
        eprintln!("{} Compaction failed: {}", "error:".red().bold(), e);
        return;
    }

    println!(
        "{} Compaction completed in {:.1}ms",
        "✓".green().bold(),
        start.elapsed().as_secs_f64() * 1000.0
    );
}

/// Drop a table.
fn cmd_drop(path: &str, table_name: &str, yes: bool) {
    let db = match open_db(path) {
        Some(db) => db,
        None => return,
    };

    if !yes {
        eprint!(
            "{} Drop table '{}' and all its data? [y/N]: ",
            "warning:".yellow().bold(),
            table_name
        );

        let mut input = String::new();
        if std::io::stdin().read_line(&mut input).is_err() {
            eprintln!("{} Failed to read input", "error:".red().bold());
            return;
        }

        if !input.trim().eq_ignore_ascii_case("y") {
            println!("{}", "Cancelled.".dimmed());
            return;
        }
    }

    if let Err(e) = db.drop_table(table_name) {
        eprintln!("{} {}", "error:".red().bold(), e);
        return;
    }

    println!(
        "{} Table '{}' dropped successfully.",
        "✓".green().bold(),
        table_name
    );
}

// ==================== HELPERS ====================

/// Open the database, printing errors if it fails.
fn open_db(path: &str) -> Option<Database> {
    match Database::open(path) {
        Ok(db) => Some(db),
        Err(e) => {
            eprintln!(
                "{} Failed to open database at '{}': {}",
                "error:".red().bold(),
                path,
                e
            );
            None
        }
    }
}

/// Export RecordBatches to a CSV file.
fn export_csv(batches: &[arrow_array::RecordBatch], output: &str) -> Result<(), Box<dyn std::error::Error>> {
    let file = std::fs::File::create(output)?;
    let mut writer = csv::Writer::from_writer(file);

    if batches.is_empty() {
        return Ok(());
    }

    // Write header
    let schema = batches[0].schema();
    let header: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    writer.write_record(&header)?;

    // Write data rows
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                let value = arrow::util::display::array_value_to_string(col, row_idx)
                    .unwrap_or_else(|_| "".to_string());
                row.push(value);
            }
            writer.write_record(&row)?;
        }
    }

    writer.flush()?;
    Ok(())
}

/// Export RecordBatches to a JSON file.
fn export_json(batches: &[arrow_array::RecordBatch], output: &str) -> Result<(), Box<dyn std::error::Error>> {
    let file = std::fs::File::create(output)?;

    if batches.is_empty() {
        serde_json::to_writer_pretty(file, &serde_json::json!([]))?;
        return Ok(());
    }

    let schema = batches[0].schema();
    let mut all_rows: Vec<serde_json::Value> = Vec::new();

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut obj = serde_json::Map::new();
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                let field_name = schema.field(col_idx).name().clone();
                let value = arrow::util::display::array_value_to_string(col, row_idx)
                    .unwrap_or_else(|_| "null".to_string());
                obj.insert(field_name, serde_json::Value::String(value));
            }
            all_rows.push(serde_json::Value::Object(obj));
        }
    }

    serde_json::to_writer_pretty(file, &all_rows)?;
    Ok(())
}

/// Build a RecordBatch from CSV string columns using the table schema.
fn build_batch_from_csv_columns(
    schema: &arrow_schema::SchemaRef,
    columns: &[Vec<String>],
) -> Result<arrow_array::RecordBatch, Box<dyn std::error::Error>> {
    use arrow::datatypes::DataType;
    use arrow_array::*;
    use std::sync::Arc;

    let mut arrays: Vec<Arc<dyn arrow_array::Array>> = Vec::new();

    for (i, field) in schema.fields().iter().enumerate() {
        let col_data = if i < columns.len() { &columns[i] } else { &vec![] };

        let array: Arc<dyn arrow_array::Array> = match field.data_type() {
            DataType::Int8 => {
                let values: Vec<Option<i8>> = col_data
                    .iter()
                    .map(|s| s.parse::<i8>().ok())
                    .collect();
                Arc::new(Int8Array::from(values))
            }
            DataType::Int16 => {
                let values: Vec<Option<i16>> = col_data
                    .iter()
                    .map(|s| s.parse::<i16>().ok())
                    .collect();
                Arc::new(Int16Array::from(values))
            }
            DataType::Int32 => {
                let values: Vec<Option<i32>> = col_data
                    .iter()
                    .map(|s| s.parse::<i32>().ok())
                    .collect();
                Arc::new(Int32Array::from(values))
            }
            DataType::Int64 => {
                let values: Vec<Option<i64>> = col_data
                    .iter()
                    .map(|s| s.parse::<i64>().ok())
                    .collect();
                Arc::new(Int64Array::from(values))
            }
            DataType::UInt8 => {
                let values: Vec<Option<u8>> = col_data
                    .iter()
                    .map(|s| s.parse::<u8>().ok())
                    .collect();
                Arc::new(UInt8Array::from(values))
            }
            DataType::UInt16 => {
                let values: Vec<Option<u16>> = col_data
                    .iter()
                    .map(|s| s.parse::<u16>().ok())
                    .collect();
                Arc::new(UInt16Array::from(values))
            }
            DataType::UInt32 => {
                let values: Vec<Option<u32>> = col_data
                    .iter()
                    .map(|s| s.parse::<u32>().ok())
                    .collect();
                Arc::new(UInt32Array::from(values))
            }
            DataType::UInt64 => {
                let values: Vec<Option<u64>> = col_data
                    .iter()
                    .map(|s| s.parse::<u64>().ok())
                    .collect();
                Arc::new(UInt64Array::from(values))
            }
            DataType::Float32 => {
                let values: Vec<Option<f32>> = col_data
                    .iter()
                    .map(|s| s.parse::<f32>().ok())
                    .collect();
                Arc::new(Float32Array::from(values))
            }
            DataType::Float64 => {
                let values: Vec<Option<f64>> = col_data
                    .iter()
                    .map(|s| s.parse::<f64>().ok())
                    .collect();
                Arc::new(Float64Array::from(values))
            }
            DataType::Boolean => {
                let values: Vec<Option<bool>> = col_data
                    .iter()
                    .map(|s| s.parse::<bool>().ok())
                    .collect();
                Arc::new(BooleanArray::from(values))
            }
            DataType::Utf8 | DataType::LargeUtf8 => {
                Arc::new(StringArray::from(
                    col_data.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                ))
            }
            dt => {
                return Err(format!("Unsupported data type for CSV import: {:?}", dt).into());
            }
        };

        arrays.push(array);
    }

    let batch = arrow_array::RecordBatch::try_new(schema.clone(), arrays)?;
    Ok(batch)
}
