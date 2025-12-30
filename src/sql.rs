use std::{collections::HashMap, sync::Arc};

use thiserror::Error;

use crate::data_sources::{CsvDataSource, DataSource};

#[derive(Error, Debug)]
pub enum PlanError {
    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Unsupported feature: {0}")]
    Unsupported(String),

    #[error("Invalid expression: {0}")]
    InvalidExpression(String),

    #[error("Data source error: {0}")]
    DataSourceError(String),
}

impl From<sqlparser::parser::ParserError> for PlanError {
    fn from(e: sqlparser::parser::ParserError) -> Self {
        PlanError::ParseError(e.to_string())
    }
}

pub struct Catalog {
    tables: HashMap<String, Arc<dyn Fn() -> Box<dyn DataSource> + Send + Sync>>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            tables: HashMap::new(),
        }
    }

    pub fn registser_csv(&mut self, name: &str, path: &str, delimiter: u8, header: bool) {
        let path = path.to_string();
        let name = name.to_string();
        self.tables.insert(
            name,
            Arc::new(move || {
                Box::new(
                    CsvDataSource::try_new(path.clone(), delimiter, header)
                        .expect("Failed to create CSV data source"),
                ) as Box<dyn DataSource>
            }),
        );
    }

    pub fn register_table<F>(&mut self, name: &str, factory: F)
    where
        F: Fn() -> Box<dyn DataSource> + Send + Sync + 'static,
    {
        self.tables.insert(name.to_string(), Arc::new(factory));
    }

    pub fn get_table(&self, name: &str) -> Option<Box<dyn DataSource>> {
        self.tables.get(name).map(|factory| factory())
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}
