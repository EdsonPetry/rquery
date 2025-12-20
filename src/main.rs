use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open("userdata.parquet")?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    let schema = builder.schema();

    println!("Schema:\n{}", schema);

    println!("\nFields:");
    for field in schema.fields() {
        println!(
            "\t{} : {:?} (nullable: {})",
            field.name(),
            field.data_type(),
            field.is_nullable()
        );
    }

    Ok(())
}
