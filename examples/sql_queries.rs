use arrow_array::{Int64Array, StringArray};
use fusiondb::{DataType, Database, Result, SchemaBuilder};
use std::sync::Arc;

fn main() -> Result<()> {
    // 1. Open the database
    let db_path = "./db_examples_sql";
    let db = Database::open(db_path)?;

    // 2. Define schema & create table
    let schema = SchemaBuilder::new()
        .field("id", DataType::Int64, false)
        .primary_key()
        .field("name", DataType::Utf8, false)
        .field("role", DataType::Utf8, false)
        .field("salary", DataType::Int64, false)
        .build()?;

    let table_name = "employees";
    if db.table_exists(table_name) {
        db.drop_table(table_name)?;
    }
    let table = db.create_table(table_name, schema)?;

    // 3. Insert employees data
    let batch = arrow_array::RecordBatch::try_new(
        table.arrow_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "David"])),
            Arc::new(StringArray::from(vec!["Engineering", "Sales", "Engineering", "Marketing"])),
            Arc::new(Int64Array::from(vec![90000, 80000, 105000, 75000])),
        ],
    ).unwrap();
    table.insert(batch)?;

    // 4. Run SQL Query: Filter & Projection
    println!("--- SQL Query: Engineers with salary > 85,000 ---");
    let results = db.query("SELECT name, salary FROM employees WHERE role = 'Engineering' AND salary > 85000")?;
    for batch in results {
        println!("{:?}", batch);
    }

    // 5. Run SQL Query: Aggregation (COUNT, AVG, SUM)
    println!("\n--- SQL Query: Average Salary by Role ---");
    let results = db.query("SELECT role, COUNT(*), AVG(salary) FROM employees GROUP BY role")?;
    for batch in results {
        println!("{:?}", batch);
    }

    // Clean up
    db.drop_table(table_name)?;
    let _ = std::fs::remove_dir_all(db_path);

    Ok(())
}
