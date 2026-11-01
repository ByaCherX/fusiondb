use arrow_array::{Int64Array, StringArray};
use fusiondb::{DataType, Database, FusionDBError, Result, SchemaBuilder};
use std::sync::Arc;

fn main() -> Result<()> {
    // 1. Open the database
    let db_path = "./db_examples_errors";
    let db = Database::open(db_path)?;

    // 2. Define schema & create table
    let schema = SchemaBuilder::new()
        .field("id", DataType::Int64, false)
        .primary_key()
        .field("name", DataType::Utf8, false)
        .build()?;

    let table_name = "error_table";
    if db.table_exists(table_name) {
        db.drop_table(table_name)?;
    }
    let table = db.create_table(table_name, schema)?;

    // 3. Trigger & handle PrimaryKeyNotFound error during builder
    println!("--- Case 1: Schema without a primary key column ---");
    let invalid_schema_res = SchemaBuilder::new()
        .field("id", DataType::Int64, false)
        .field("name", DataType::Utf8, false)
        .build();
    match invalid_schema_res {
        Err(e) => println!("Caught expected error: {:?}", e),
        Ok(_) => panic!("Expected schema builder to fail!"),
    }

    // 4. Trigger & handle TableAlreadyExists error
    println!("\n--- Case 2: Creating a table that already exists ---");
    let duplicate_table_res = db.create_table(table_name, table.schema().clone());
    match duplicate_table_res {
        Err(e) => {
            assert!(matches!(e, FusionDBError::TableAlreadyExists { .. }));
            println!("Caught expected error: {:?}", e);
        }
        Ok(_) => panic!("Expected duplicate table creation to fail!"),
    }

    // 5. Trigger & handle PrimaryKeyViolation error
    println!("\n--- Case 3: Primary Key duplicate constraint violation ---");
    let batch1 = arrow_array::RecordBatch::try_new(
        table.arrow_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["Alice"])),
        ],
    ).unwrap();
    table.insert(batch1)?;

    let batch2 = arrow_array::RecordBatch::try_new(
        table.arrow_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1])), // duplicate ID
            Arc::new(StringArray::from(vec!["Bob"])),
        ],
    ).unwrap();

    match table.insert(batch2) {
        Err(e) => {
            assert!(matches!(e, FusionDBError::PrimaryKeyViolation { .. }));
            println!("Caught expected error: {:?}", e);
        }
        Ok(_) => panic!("Expected duplicate primary key insert to fail!"),
    }

    // 6. Trigger & handle RowNotFound error
    println!("\n--- Case 4: Requesting a non-existent row ---");
    match table.get(999i64) {
        Err(e) => {
            assert!(matches!(e, FusionDBError::RowNotFound { .. }));
            println!("Caught expected error: {:?}", e);
        }
        Ok(_) => panic!("Expected get for missing row to fail!"),
    }

    // Clean up
    db.drop_table(table_name)?;
    let _ = std::fs::remove_dir_all(db_path);

    Ok(())
}
