use std::sync::Arc;

use rquery::data_sources::CsvDataSource;
use rquery::logical_plan::{Alias, BinaryExpr, Column, Literal, Projection, Scan, Selection};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let csv = CsvDataSource::try_new("test_data/csv/username.csv".to_string(), b';', true)?;

    // SELECT first_name, last_name AS full_name
    // FROM username
    // WHERE identifier = '9012'

    let scan = Scan::new(&csv.filename, csv.clone(), None);
    let filter = Selection::new(
        Arc::new(scan),
        BinaryExpr::eq(Column::new("identifier"), Literal::string("9012")),
    );
    let project = Projection::new(
        Arc::new(filter),
        vec![
            Box::new(Column::new("first_name")),
            Box::new(Alias::new(Column::new("last_name"), "full_name")),
        ],
    );

    println!("{}", project);
    // Projection: #first_name, #last_name AS full_name
    //     Filter: #identifier = '9012'
    //         Scan: test_data/csv/username.csv; projection=None

    Ok(())
}
