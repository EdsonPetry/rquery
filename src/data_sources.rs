use arrow::{
    csv::{ReaderBuilder, infer_schema_from_files},
    datatypes::Schema,
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchIterator},
};
use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
use std::{fs::File, sync::Arc};

pub trait DataSource {
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

pub struct ParquetDataSource {
    filename: String,
    schema: Arc<Schema>,
}

impl ParquetDataSource {
    fn try_new(filename: &str) -> Result<Self, Box<dyn std::error::Error>> {
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

#[cfg(test)]
mod tests {
    use crate::data_sources::DataSource;

    use super::*;
    use arrow::array::StringArray;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_csv(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().expect("Failed to create temp file");
        file.write_all(content.as_bytes())
            .expect("Failed to write test data");
        file.flush().expect("Failed to flush");
        file
    }

    fn get_string_column(batch: &RecordBatch, col_name: &str) -> Vec<String> {
        let col_idx = batch.schema().index_of(col_name).unwrap();
        let array = batch.column(col_idx);
        let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
        string_array
            .iter()
            .map(|v| v.unwrap().to_string())
            .collect()
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

    mod schema_inference {
        use super::*;

        #[test]
        fn infers_column_names_from_header() {
            let csv_content = "name;age;city\nAlice;30;NYC\nBob;25;LA";
            let file = create_test_csv(csv_content);

            let csv = CsvDataSource::try_new(file.path().to_string_lossy().to_string(), b';', true)
                .unwrap();
            let schema = csv.schema();
            let columns: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

            assert_eq!(columns, vec!["name", "age", "city"]);
        }

        #[test]
        fn schema_can_be_called_multiple() {
            let csv_content = "name;age;city\nAlice;30;NYC\nBob;25;LA\n";
            let file = create_test_csv(csv_content);

            let csv = CsvDataSource::try_new(file.path().to_string_lossy().to_string(), b';', true)
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
            let columns: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

            assert_eq!(columns.len(), 3);
        }
    }

    mod scan_without_projection {
        use std::vec;

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

        fn projected_columns_have_correct_data() {
            let csv_content = "name;age;city\nAlice;30;NYC;Bob;25;LA";
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
