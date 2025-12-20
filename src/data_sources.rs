use arrow::{
    csv::{Reader, ReaderBuilder, infer_schema_from_files},
    datatypes::Schema,
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchIterator},
};
use parquet::schema;
use std::{error::Error, fs::File, sync::Arc};

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
    pub fn try_new(filename: String, delimiter: u8, header: bool) -> Self {
        let schema: Arc<Schema> =
            Arc::new(infer_schema_from_files(&[filename.clone()], ';' as u8, None, true).unwrap());
        CsvDataSource {
            filename,
            schema,
            delimiter,
            header,
        }
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

#[cfg(test)]
mod tests {
    use crate::data_sources::DataSource;

    use super::*;

    #[test]
    fn test_csv_schema() {
        let csv = CsvDataSource::try_new(String::from("test_data/csv/username.csv"), b';', true);

        let columns: Vec<String> = csv
            .schema()
            .fields
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(
            columns,
            vec!["Username", " Identifier", "First name", "Last name"]
        );
    }

    #[test]
    fn test_csv_scan_no_projection() {
        let csv = CsvDataSource::try_new(String::from("test_data/csv/username.csv"), b';', true);

        let mut iterator = csv.scan(None).unwrap();

        let total_columns = match iterator.next() {
            Some(Ok(batch)) => batch.num_columns(),
            _ => 0,
        };

        assert_eq!(total_columns, 4);
    }

    #[test]
    fn test_csv_scan_with_projection() {
        let csv = CsvDataSource::try_new(String::from("test_data/csv/username.csv"), b';', true);

        let mut iterator = csv
            .scan(Some(vec![
                String::from("Username"),
                String::from("First name"),
            ]))
            .unwrap();

        let total_columns = match iterator.next() {
            Some(Ok(batch)) => batch.num_columns(),
            _ => 0,
        };

        assert_eq!(total_columns, 2);
    }
}
