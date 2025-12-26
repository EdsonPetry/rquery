use rquery::data_frame::ExecutionContext;
use rquery::logical_plan::{BinaryExpr, Column, Literal};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // SELECT first_name, last_name AS full_name
    // FROM username
    // WHERE identifier = '9012'

    // let csv = CsvDataSource::try_new("test_data/csv/username.csv".to_string(), b';', true)?;
    // let scan = Scan::new(&csv.filename, csv.clone(), None);
    // let filter = Selection::new(
    //     Arc::new(scan),
    //     BinaryExpr::eq(Column::new("identifier"), Literal::string("9012")),
    // );
    // let project = Projection::new(
    //     Arc::new(filter),
    //     vec![
    //         Box::new(Column::new("first_name")),
    //         Box::new(Alias::new(Column::new("last_name"), "full_name")),
    //     ],
    // );

    let ctx = ExecutionContext::new(); // gives us a new ExeuctionContext object

    let project = ctx
        .csv("test_data/csv/username.csv", b';', true)
        .filter(BinaryExpr::eq(
            Column::new("Identifier"),
            Literal::string("9012"),
        ))
        .project(vec![
            Box::new(Column::new("first_name")),
            Box::new(Column::new("last_name")),
        ]);

    println!("{}", project);
    // Projection: #first_name, #last_name AS full_name
    //     Filter: #identifier = '9012'
    //         Scan: test_data/csv/username.csv; projection=None

    Ok(())
}
