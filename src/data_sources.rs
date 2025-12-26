use arrow::{
    csv::{ReaderBuilder, infer_schema_from_files},
    datatypes::Schema,
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchIterator},
};
use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
use std::{fmt::Display, fs::File, sync::Arc};

pub trait DataSource: Display {
    fn schema(&self) -> Arc<Schema>;

    fn scan(
        &self,
        projection: Option<Vec<String>>,
    ) -> Result<
        RecordBatchIterator<Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>>>>,
        Box<dyn std::error::Error>,
    >;
}

pub struct CsvDataSource {
    pub filename: String,
    delimiter: u8,
    header: bool,
    schema: Arc<Schema>,
}

impl CsvDataSource {
    pub fn try_new(
        filename: String,
        delimiter: u8,
        header: bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let schema: Arc<Schema> =
            Arc::new(infer_schema_from_files(&[filename.clone()], ';' as u8, None, true).unwrap());
        Ok(CsvDataSource {
            filename,
            schema,
            delimiter,
            header,
        })
    }
}

impl DataSource for CsvDataSource {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn scan(
        &self,
        projection: Option<Vec<String>>,
    ) -> Result<
        RecordBatchIterator<Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>>>>,
        Box<dyn std::error::Error>,
    > {
        let file = File::open(&self.filename)?;

        let reader = match projection {
            None => ReaderBuilder::new(self.schema.clone())
                .with_delimiter(self.delimiter)
                .with_header(self.header)
                .build(file),
            Some(p) => {
                let p_idx = p
                    .iter()
                    .map(|col| self.schema.index_of(col).unwrap())
                    .collect();
                ReaderBuilder::new(self.schema.clone())
                    .with_delimiter(self.delimiter)
                    .with_header(self.header)
                    .with_projection(p_idx)
                    .build(file)
            }
        }?;
        let schema = reader.schema();
        let batches: Vec<Result<RecordBatch, ArrowError>> = reader.collect();

        Ok(RecordBatchIterator::new(
            Box::new(batches.into_iter()),
            schema,
        ))
    }
}

impl Display for CsvDataSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.filename)
    }
}

impl Clone for CsvDataSource {
    fn clone(&self) -> Self {
        Self {
            filename: self.filename.clone(),
            delimiter: self.delimiter,
            header: self.header,
            schema: self.schema.clone(),
        }
    }
}

#[derive(Clone)]
pub struct ParquetDataSource {
    filename: String,
    schema: Arc<Schema>,
}

impl ParquetDataSource {
    pub fn try_new(filename: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::open(filename).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

        let schema = builder.schema();

        Ok(ParquetDataSource {
            filename: filename.to_string(),
            schema: schema.clone(),
        })
    }
}

impl DataSource for ParquetDataSource {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn scan(
        &self,
        projection: Option<Vec<String>>,
    ) -> Result<
        RecordBatchIterator<Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>>>>,
        Box<dyn std::error::Error>,
    > {
        let file = File::open(&self.filename).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        let reader = match projection {
            Some(names) => {
                let col_indices: Vec<usize> = names
                    .iter()
                    .filter_map(|name| self.schema.index_of(name).ok())
                    .collect();
                let parquet_schema = builder.parquet_schema();
                let mask = ProjectionMask::roots(parquet_schema, col_indices);
                builder.with_projection(mask).build()?
            }
            None => builder.build()?,
        };

        let batches: Vec<Result<RecordBatch, ArrowError>> = reader.collect();

        Ok(RecordBatchIterator::new(
            Box::new(batches.into_iter()),
            self.schema.clone(),
        ))
    }
}

impl Display for ParquetDataSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.filename)
    }
}

#[derive(Clone)]
pub struct InMemoryDataSource {
    schema: Arc<Schema>,
    data: Vec<RecordBatch>,
}

impl InMemoryDataSource {
    pub fn try_new(data: Option<Vec<RecordBatch>>) -> Result<Self, Box<dyn std::error::Error>> {
        match data {
            Some(d) => {
                let schema = d.first().unwrap().schema();
                Ok(InMemoryDataSource { schema, data: d })
            }
            None => {
                let schema = Arc::new(Schema::empty());
                let data: Vec<RecordBatch> = Vec::new();
                Ok(InMemoryDataSource { schema, data })
            }
        }
    }
}

impl DataSource for InMemoryDataSource {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn scan(
        &self,
        projection: Option<Vec<String>>,
    ) -> Result<
        RecordBatchIterator<Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>>>>,
        Box<dyn std::error::Error>,
    > {
        match projection {
            None => {
                let batches: Vec<Result<RecordBatch, ArrowError>> =
                    self.data.iter().cloned().map(Ok).collect();
                Ok(RecordBatchIterator::new(
                    Box::new(batches.into_iter()),
                    self.schema.clone(),
                ))
            }
            Some(names) => {
                let col_indices: Vec<usize> = names
                    .iter()
                    .map(|name| self.schema.index_of(name))
                    .collect::<Result<Vec<_>, _>>()?;
                let projected_schema = Arc::new(self.schema.project(&col_indices)?);

                let batches: Vec<Result<RecordBatch, ArrowError>> = self
                    .data
                    .iter()
                    .map(|batch| batch.project(&col_indices))
                    .collect();

                Ok(RecordBatchIterator::new(
                    Box::new(batches.into_iter()),
                    projected_schema,
                ))
            }
        }
    }
}

impl Display for InMemoryDataSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InMemoryDataSource")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field};

    fn get_string_column(batch: &RecordBatch, col_name: &str) -> Vec<String> {
        let col_idx = batch.schema().index_of(col_name).unwrap();
        let array = batch.column(col_idx);
        let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
        string_array
            .iter()
            .map(|v| v.unwrap().to_string())
            .collect()
    }

    fn get_int_column(batch: &RecordBatch, col_name: &str) -> Vec<i32> {
        let col_idx = batch.schema().index_of(col_name).unwrap();
        let array = batch.column(col_idx);
        let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        int_array.iter().map(|v| v.unwrap()).collect()
    }

    fn get_column_names(batch: &RecordBatch) -> Vec<String> {
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    fn collect_batches(
        iter: RecordBatchIterator<Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>>>>,
    ) -> Vec<RecordBatch> {
        iter.filter_map(|r| r.ok()).collect()
    }

    mod csv_data_source {
        use super::*;
        use std::io::Write;
        use tempfile::NamedTempFile;

        fn create_test_csv(content: &str) -> NamedTempFile {
            let mut file = NamedTempFile::new().expect("Failed to create temp file");
            file.write_all(content.as_bytes())
                .expect("Failed to write test data");
            file.flush().expect("Failed to flush");
            file
        }

        mod schema_inference {
            use super::*;

            #[test]
            fn infers_column_names_from_header() {
                let csv_content = "name;age;city\nAlice;30;NYC\nBob;25;LA";
                let file = create_test_csv(csv_content);

                let csv =
                    CsvDataSource::try_new(file.path().to_string_lossy().to_string(), b';', true)
                        .unwrap();
                let schema = csv.schema();
                let columns: Vec<&str> =
                    schema.fields().iter().map(|f| f.name().as_str()).collect();

                assert_eq!(columns, vec!["name", "age", "city"]);
            }

            #[test]
            fn schema_can_be_called_multiple_times() {
                let csv_content = "name;age;city\nAlice;30;NYC\nBob;25;LA\n";
                let file = create_test_csv(csv_content);

                let csv =
                    CsvDataSource::try_new(file.path().to_string_lossy().to_string(), b';', true)
                        .unwrap();

                let schema1 = csv.schema();
                let schema2 = csv.schema();

                assert!(Arc::ptr_eq(&schema1, &schema2));
            }

            #[test]
            fn schema_without_header() {
                let csv_content = "Alice;30;NYC\nBob;25;LA\n";
                let file = create_test_csv(csv_content);

                let csv =
                    CsvDataSource::try_new(file.path().to_string_lossy().to_string(), b';', false)
                        .unwrap();
                let schema = csv.schema();
                let columns: Vec<&str> =
                    schema.fields().iter().map(|f| f.name().as_str()).collect();

                assert_eq!(columns.len(), 3);
            }
        }

        mod scan_without_projection {
            use super::*;

            #[test]
            fn returns_all_columns() {
                let csv_content = "name;age;city\nAlice;30;NYC\nBob;25;LA";
                let file = create_test_csv(csv_content);

                let source =
                    CsvDataSource::try_new(file.path().to_string_lossy().to_string(), b';', true)
                        .unwrap();
                let batches = collect_batches(source.scan(None).unwrap());

                let total_columns: usize = batches.iter().map(|b| b.num_columns()).sum();
                assert_eq!(total_columns, 3);
            }

            #[test]
            fn returns_all_rows() {
                let csv_content = "name;age\nAlice;30\nBob;25\nCharlie;33";
                let file = create_test_csv(csv_content);

                let source =
                    CsvDataSource::try_new(file.path().to_string_lossy().to_string(), b';', true)
                        .unwrap();
                let batches = collect_batches(source.scan(None).unwrap());

                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(total_rows, 3)
            }

            #[test]
            fn returns_correct_data() {
                let csv_content = "name;city\nAlice;NYC\nBob;LA";
                let file = create_test_csv(csv_content);

                let source =
                    CsvDataSource::try_new(file.path().to_string_lossy().to_string(), b';', true)
                        .unwrap();
                let mut iter = source.scan(None).unwrap();
                let batch = iter.next().unwrap().unwrap();

                assert_eq!(get_string_column(&batch, "name"), vec!["Alice", "Bob"]);
                assert_eq!(get_string_column(&batch, "city"), vec!["NYC", "LA"]);
            }
        }

        mod scan_with_projection {
            use super::*;

            #[test]
            fn returns_projected_columns() {
                let csv_content = "name;age;city\nAlice;30;NYC\nBob;25;LA";
                let file = create_test_csv(csv_content);

                let source =
                    CsvDataSource::try_new(file.path().to_string_lossy().to_string(), b';', true)
                        .unwrap();
                let projection = Some(vec![String::from("name"), String::from("city")]);
                let mut iter = source.scan(projection).unwrap();

                let batch = iter.next().unwrap().unwrap();
                assert_eq!(batch.num_columns(), 2);
                assert_eq!(get_column_names(&batch), vec!["name", "city"]);
            }

            #[test]
            fn projected_columns_have_correct_data() {
                let csv_content = "name;age;city\nAlice;30;NYC\nBob;25;LA";
                let file = create_test_csv(csv_content);

                let source =
                    CsvDataSource::try_new(file.path().to_string_lossy().to_string(), b';', true)
                        .unwrap();
                let projection = Some(vec![String::from("name"), String::from("city")]);
                let mut iter = source.scan(projection).unwrap();

                let batch = iter.next().unwrap().unwrap();
                assert_eq!(get_string_column(&batch, "name"), vec!["Alice", "Bob"]);
                assert_eq!(get_string_column(&batch, "city"), vec!["NYC", "LA"]);
            }
        }
    }

    mod parquet_data_source {
        use super::*;
        use parquet::arrow::ArrowWriter;
        use tempfile::NamedTempFile;

        fn create_test_parquet(batches: &[RecordBatch]) -> NamedTempFile {
            let file = NamedTempFile::new().expect("Failed to create temp file");
            let props = None;
            let mut writer =
                ArrowWriter::try_new(file.reopen().unwrap(), batches[0].schema(), props).unwrap();

            for batch in batches {
                writer.write(batch).unwrap();
            }
            writer.close().unwrap();
            file
        }

        fn create_test_batch() -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("age", DataType::Int32, false),
                Field::new("city", DataType::Utf8, false),
            ]));

            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["Alice", "Bob"])),
                    Arc::new(Int32Array::from(vec![30, 25])),
                    Arc::new(StringArray::from(vec!["NYC", "LA"])),
                ],
            )
            .unwrap()
        }

        mod schema_inference {
            use super::*;

            #[test]
            fn infers_schema_from_parquet() {
                let batch = create_test_batch();
                let file = create_test_parquet(&[batch]);

                let source = ParquetDataSource::try_new(&file.path().to_string_lossy()).unwrap();
                let schema = source.schema();
                let columns: Vec<&str> =
                    schema.fields().iter().map(|f| f.name().as_str()).collect();

                assert_eq!(columns, vec!["name", "age", "city"]);
            }

            #[test]
            fn schema_can_be_called_multiple_times() {
                let batch = create_test_batch();
                let file = create_test_parquet(&[batch]);

                let source = ParquetDataSource::try_new(&file.path().to_string_lossy()).unwrap();

                let schema1 = source.schema();
                let schema2 = source.schema();

                assert!(Arc::ptr_eq(&schema1, &schema2));
            }
        }

        mod scan_without_projection {
            use super::*;

            #[test]
            fn returns_all_columns() {
                let batch = create_test_batch();
                let file = create_test_parquet(&[batch]);

                let source = ParquetDataSource::try_new(&file.path().to_string_lossy()).unwrap();
                let batches = collect_batches(source.scan(None).unwrap());

                let total_columns: usize = batches.iter().map(|b| b.num_columns()).sum();
                assert_eq!(total_columns, 3);
            }

            #[test]
            fn returns_all_rows() {
                let batch = create_test_batch();
                let file = create_test_parquet(&[batch]);

                let source = ParquetDataSource::try_new(&file.path().to_string_lossy()).unwrap();
                let batches = collect_batches(source.scan(None).unwrap());

                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(total_rows, 2);
            }

            #[test]
            fn returns_correct_data() {
                let batch = create_test_batch();
                let file = create_test_parquet(&[batch]);

                let source = ParquetDataSource::try_new(&file.path().to_string_lossy()).unwrap();
                let mut iter = source.scan(None).unwrap();
                let batch = iter.next().unwrap().unwrap();

                assert_eq!(get_string_column(&batch, "name"), vec!["Alice", "Bob"]);
                assert_eq!(get_int_column(&batch, "age"), vec![30, 25]);
                assert_eq!(get_string_column(&batch, "city"), vec!["NYC", "LA"]);
            }
        }

        mod scan_with_projection {
            use super::*;

            #[test]
            fn returns_projected_columns() {
                let batch = create_test_batch();
                let file = create_test_parquet(&[batch]);

                let source = ParquetDataSource::try_new(&file.path().to_string_lossy()).unwrap();
                let projection = Some(vec![String::from("name"), String::from("city")]);
                let mut iter = source.scan(projection).unwrap();

                let batch = iter.next().unwrap().unwrap();
                assert_eq!(batch.num_columns(), 2);
                assert_eq!(get_column_names(&batch), vec!["name", "city"]);
            }

            #[test]
            fn projected_columns_have_correct_data() {
                let batch = create_test_batch();
                let file = create_test_parquet(&[batch]);

                let source = ParquetDataSource::try_new(&file.path().to_string_lossy()).unwrap();
                let projection = Some(vec![String::from("name"), String::from("city")]);
                let mut iter = source.scan(projection).unwrap();

                let batch = iter.next().unwrap().unwrap();
                assert_eq!(get_string_column(&batch, "name"), vec!["Alice", "Bob"]);
                assert_eq!(get_string_column(&batch, "city"), vec!["NYC", "LA"]);
            }
        }
    }

    mod in_memory_data_source {
        use super::*;

        fn create_test_batch() -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("age", DataType::Int32, false),
                Field::new("city", DataType::Utf8, false),
            ]));

            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["Alice", "Bob"])),
                    Arc::new(Int32Array::from(vec![30, 25])),
                    Arc::new(StringArray::from(vec!["NYC", "LA"])),
                ],
            )
            .unwrap()
        }

        fn create_multi_batch_data() -> Vec<RecordBatch> {
            let schema = Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("age", DataType::Int32, false),
            ]));

            vec![
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(StringArray::from(vec!["Alice", "Bob"])),
                        Arc::new(Int32Array::from(vec![30, 25])),
                    ],
                )
                .unwrap(),
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(StringArray::from(vec!["Charlie", "Diana"])),
                        Arc::new(Int32Array::from(vec![35, 28])),
                    ],
                )
                .unwrap(),
            ]
        }

        mod schema {
            use super::*;

            #[test]
            fn returns_schema_from_data() {
                let batch = create_test_batch();
                let source = InMemoryDataSource::try_new(Some(vec![batch])).unwrap();

                let schema = source.schema();
                let columns: Vec<&str> =
                    schema.fields().iter().map(|f| f.name().as_str()).collect();

                assert_eq!(columns, vec!["name", "age", "city"]);
            }

            #[test]
            fn returns_empty_schema_when_no_data() {
                let source = InMemoryDataSource::try_new(None).unwrap();
                let schema = source.schema();

                assert_eq!(schema.fields().len(), 0);
            }

            #[test]
            fn schema_can_be_called_multiple_times() {
                let batch = create_test_batch();
                let source = InMemoryDataSource::try_new(Some(vec![batch])).unwrap();

                let schema1 = source.schema();
                let schema2 = source.schema();

                assert!(Arc::ptr_eq(&schema1, &schema2));
            }
        }

        mod scan_without_projection {
            use super::*;

            #[test]
            fn returns_all_columns() {
                let batch = create_test_batch();
                let source = InMemoryDataSource::try_new(Some(vec![batch])).unwrap();
                let batches = collect_batches(source.scan(None).unwrap());

                let total_columns: usize = batches.iter().map(|b| b.num_columns()).sum();
                assert_eq!(total_columns, 3);
            }

            #[test]
            fn returns_all_rows() {
                let batch = create_test_batch();
                let source = InMemoryDataSource::try_new(Some(vec![batch])).unwrap();
                let batches = collect_batches(source.scan(None).unwrap());

                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(total_rows, 2);
            }

            #[test]
            fn returns_correct_data() {
                let batch = create_test_batch();
                let source = InMemoryDataSource::try_new(Some(vec![batch])).unwrap();
                let mut iter = source.scan(None).unwrap();
                let batch = iter.next().unwrap().unwrap();

                assert_eq!(get_string_column(&batch, "name"), vec!["Alice", "Bob"]);
                assert_eq!(get_int_column(&batch, "age"), vec![30, 25]);
                assert_eq!(get_string_column(&batch, "city"), vec!["NYC", "LA"]);
            }

            #[test]
            fn handles_multiple_batches() {
                let data = create_multi_batch_data();
                let source = InMemoryDataSource::try_new(Some(data)).unwrap();
                let batches = collect_batches(source.scan(None).unwrap());

                assert_eq!(batches.len(), 2);
                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(total_rows, 4);
            }

            #[test]
            fn handles_empty_data() {
                let source = InMemoryDataSource::try_new(None).unwrap();
                let batches = collect_batches(source.scan(None).unwrap());

                assert_eq!(batches.len(), 0);
            }
        }

        mod scan_with_projection {
            use super::*;

            #[test]
            fn returns_projected_columns() {
                let batch = create_test_batch();
                let source = InMemoryDataSource::try_new(Some(vec![batch])).unwrap();
                let projection = Some(vec![String::from("name"), String::from("city")]);
                let mut iter = source.scan(projection).unwrap();

                let batch = iter.next().unwrap().unwrap();
                assert_eq!(batch.num_columns(), 2);
                assert_eq!(get_column_names(&batch), vec!["name", "city"]);
            }

            #[test]
            fn projected_columns_have_correct_data() {
                let batch = create_test_batch();
                let source = InMemoryDataSource::try_new(Some(vec![batch])).unwrap();
                let projection = Some(vec![String::from("name"), String::from("city")]);
                let mut iter = source.scan(projection).unwrap();

                let batch = iter.next().unwrap().unwrap();
                assert_eq!(get_string_column(&batch, "name"), vec!["Alice", "Bob"]);
                assert_eq!(get_string_column(&batch, "city"), vec!["NYC", "LA"]);
            }

            #[test]
            fn projection_works_with_multiple_batches() {
                let data = create_multi_batch_data();
                let source = InMemoryDataSource::try_new(Some(data)).unwrap();
                let projection = Some(vec![String::from("name")]);
                let batches = collect_batches(source.scan(projection).unwrap());

                assert_eq!(batches.len(), 2);
                for batch in &batches {
                    assert_eq!(batch.num_columns(), 1);
                    assert_eq!(get_column_names(batch), vec!["name"]);
                }
            }

            #[test]
            fn errors_on_invalid_column_name() {
                let batch = create_test_batch();
                let source = InMemoryDataSource::try_new(Some(vec![batch])).unwrap();
                let projection = Some(vec![String::from("nonexistent")]);

                let result = source.scan(projection);
                assert!(result.is_err());
            }
        }
    }
}
