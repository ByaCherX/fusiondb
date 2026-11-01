use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== DataFusion In-Memory Table & 64 MB Benchmark ===");

    // 1. Arrow Schema Tanımlama
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));

    // 2. ~64 MB Rastgele/Örnek Veri Üretimi
    // Her satır yaklaşık 128 bayt:
    // id (8) + category (~16) + value (8) + payload (96) = 128 bytes
    // 64 MB = 64 * 1024 * 1024 = 67,108,864 bytes -> 524,288 satır
    let batch_size = 8192;
    let num_batches = 64; // 64 * 8192 = 524,288 satır (~64 - 70 MB bellek)
    let total_rows = batch_size * num_batches;

    println!(
        "Veri hazırlanıyor: {} satır ({} batch x {} satır)...",
        total_rows, num_batches, batch_size
    );

    let start_gen = Instant::now();
    let categories = ["electronics", "apparel", "home", "books", "grocery", "automotive", "health", "toys"];
    let dummy_payload = "A".repeat(96);

    let mut batches = Vec::with_capacity(num_batches);
    let mut total_memory_bytes = 0usize;

    for b in 0..num_batches {
        let mut ids = Vec::with_capacity(batch_size);
        let mut cats = Vec::with_capacity(batch_size);
        let mut vals = Vec::with_capacity(batch_size);
        let mut payloads = Vec::with_capacity(batch_size);

        for i in 0..batch_size {
            let row_idx = (b * batch_size + i) as i64;
            ids.push(row_idx);
            cats.push(categories[(row_idx as usize) % categories.len()]);
            // Pseudo-random matematiksel değer üretimi
            vals.push(((row_idx * 17 + 31) % 1000) as f64 + 0.5);
            payloads.push(dummy_payload.as_str());
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(cats)),
                Arc::new(Float64Array::from(vals)),
                Arc::new(StringArray::from(payloads)),
            ],
        )?;

        total_memory_bytes += batch.get_array_memory_size();
        batches.push(batch);
    }

    println!(
        "Veri üretildi! Toplam Arrow bellek boyutu: {:.2} MB (Süre: {:?})",
        total_memory_bytes as f64 / (1024.0 * 1024.0),
        start_gen.elapsed()
    );

    // 3. DataFusion SessionContext ve In-Memory MemTable Kaydı
    let ctx = SessionContext::new();

    // DataFusion'da depolama motoru olarak bellek (MemTable) kullanılır.
    // Partition bazlı paralel okuma için batches tek bir partition veya çoklu partition olarak verilebilir:
    let mem_table = MemTable::try_new(schema.clone(), vec![batches])?;
    ctx.register_table("items_in_memory", Arc::new(mem_table))?;

    println!("\nTablo 'items_in_memory' DataFusion SessionContext'e kaydedildi.\n");

    // 4. SQL Sorgusu 1: Kategori bazlı Aggregation (GROUP BY, COUNT, AVG, SUM)
    let sql_agg = "
        SELECT 
            category,
            COUNT(*) as item_count,
            ROUND(AVG(value)::numeric, 2) as avg_value,
            ROUND(SUM(value)::numeric, 2) as total_value,
            MIN(value) as min_val,
            MAX(value) as max_val
        FROM items_in_memory
        GROUP BY category
        ORDER BY total_value DESC
    ";

    println!("--- Sorgu 1: Group By & Aggregation ---");
    let start_q1 = Instant::now();
    let df1 = ctx.sql(sql_agg).await?;
    let results1 = df1.collect().await?;
    let duration_q1 = start_q1.elapsed();

    arrow::util::pretty::print_batches(&results1)?;
    println!("Sorgu 1 tamamlandı: {:?}\n", duration_q1);

    // 5. SQL Sorgusu 2: Filtreleme, Matematiksel İşlem ve Limit (WHERE, ORDER BY)
    let sql_filter = "
        SELECT id, category, value
        FROM items_in_memory
        WHERE category = 'electronics' AND value > 990.0
        ORDER BY value DESC, id ASC
        LIMIT 5
    ";

    println!("--- Sorgu 2: Filter & Order By & Limit ---");
    let start_q2 = Instant::now();
    let df2 = ctx.sql(sql_filter).await?;
    let results2 = df2.collect().await?;
    let duration_q2 = start_q2.elapsed();

    arrow::util::pretty::print_batches(&results2)?;
    println!("Sorgu 2 tamamlandı: {:?}", duration_q2);

    Ok(())
}

