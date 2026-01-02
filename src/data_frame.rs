use std::{fmt::Display, sync::Arc};

use arrow::datatypes::Schema;

use crate::{
    data_sources::{CsvDataSource, ParquetDataSource},
    logical_plan::{
        Aggregate, AggregateExpr, Expr, LogicalPlan, Plan, Projection, Scan, Selection,
    },
};

pub struct DataFrame {
    plan: Arc<Plan>,
}

impl DataFrame {
    pub fn project(&self, expr: Vec<Expr>) -> Self {
        DataFrame {
            plan: Arc::new(Plan::Projection(Projection::new(self.plan.clone(), expr))),
        }
    }
    pub fn filter(&self, expr: Expr) -> Self {
        DataFrame {
            plan: Arc::new(Plan::Selection(Selection::new(self.plan.clone(), expr))),
        }
    }
    pub fn aggregate(&self, group_by: Vec<Expr>, aggregate_expr: Vec<AggregateExpr>) -> Self {
        DataFrame {
            plan: Arc::new(Plan::Aggregate(Aggregate::new(
                self.plan.clone(),
                group_by,
                aggregate_expr,
            ))),
        }
    }
    pub fn schema(&self) -> Arc<Schema> {
        self.plan.schema()
    }
    pub fn logical_plan(&self) -> Arc<dyn LogicalPlan> {
        self.plan.clone()
    }
}

impl Display for DataFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.plan)
    }
}

// factory that produces data frames for now
pub struct ExecutionContext {}

impl ExecutionContext {
    pub fn new() -> Self {
        ExecutionContext {}
    }

    pub fn csv(&self, filename: &str, delimiter: u8, header: bool) -> DataFrame {
        let plan = Plan::Scan(Scan::new(
            filename,
            Box::new(CsvDataSource::try_new(filename.to_string(), delimiter, header).unwrap()),
            None,
        ));

        DataFrame {
            plan: Arc::new(plan),
        }
    }

    pub fn parquet(&self, filename: &str) -> DataFrame {
        let plan = Plan::Scan(Scan::new(
            filename,
            Box::new(ParquetDataSource::try_new(filename).unwrap()),
            None,
        ));
        DataFrame {
            plan: Arc::new(plan),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::data_sources::InMemoryDataSource;
    use crate::logical_plan::{AggregateExpr, Alias, Binary, Column, Literal};

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, true),
            Field::new("salary", DataType::Float64, true),
            Field::new("active", DataType::Boolean, false),
        ]))
    }

    fn create_test_batch() -> RecordBatch {
        let schema = create_test_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
                Arc::new(Int64Array::from(vec![30, 25, 35])),
                Arc::new(Float64Array::from(vec![50000.0, 60000.0, 70000.0])),
                Arc::new(BooleanArray::from(vec![true, false, true])),
            ],
        )
        .unwrap()
    }

    fn create_test_data_source() -> Box<dyn crate::data_sources::DataSource> {
        Box::new(InMemoryDataSource::try_new(Some(vec![create_test_batch()])).unwrap())
    }

    fn create_test_dataframe() -> DataFrame {
        use std::fs::File;
        use std::io::Write;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_table.csv");
        let mut file = File::create(&file_path).unwrap();

        writeln!(file, "id;name;age;salary;active").unwrap();
        writeln!(file, "1;Alice;30;50000.0;true").unwrap();
        writeln!(file, "2;Bob;25;60000.0;false").unwrap();
        writeln!(file, "3;Charlie;35;70000.0;true").unwrap();

        let ctx = ExecutionContext::new();
        ctx.csv(&file_path.to_string_lossy(), b';', true)
    }

    #[cfg(test)]
    impl DataFrame {
        pub fn from_plan_for_test(plan: Arc<Plan>) -> Self {
            DataFrame { plan }
        }
    }

    mod dataframe {
        use super::*;

        mod project {
            use super::*;

            #[test]
            fn creates_projection_plan() {
                let df = create_test_dataframe();
                let projected = df.project(vec![
                    Expr::Column(Column::new("id")),
                    Expr::Column(Column::new("name")),
                ]);

                let output = format!("{}", projected);
                assert!(output.contains("Projection: #id, #name"));
            }

            #[test]
            fn projection_schema_contains_selected_columns() {
                let df = create_test_dataframe();
                let projected = df.project(vec![
                    Expr::Column(Column::new("id")),
                    Expr::Column(Column::new("name")),
                ]);

                let schema = projected.schema();
                assert_eq!(schema.fields().len(), 2);

                let field_names: Vec<&str> =
                    schema.fields().iter().map(|f| f.name().as_str()).collect();
                assert_eq!(field_names, vec!["id", "name"]);
            }

            #[test]
            fn projection_with_alias() {
                let df = create_test_dataframe();
                let projected = df.project(vec![Expr::Alias(Alias::new(
                    Expr::Column(Column::new("name")),
                    "full_name",
                ))]);

                let schema = projected.schema();
                assert_eq!(schema.fields()[0].name(), "full_name");
            }

            #[test]
            fn projection_with_expressions() {
                let df = create_test_dataframe();
                let projected = df.project(vec![Expr::Binary(Binary::add(
                    Expr::Column(Column::new("salary")),
                    Expr::Literal(Literal::float(1000.0)),
                ))]);

                let schema = projected.schema();
                assert_eq!(schema.fields()[0].data_type(), &DataType::Float64);
            }

            #[test]
            fn projection_preserves_input_plan() {
                let df = create_test_dataframe();
                let projected = df.project(vec![Expr::Column(Column::new("id"))]);

                let output = format!("{}", projected);
                assert!(output.contains("Scan: "));
                assert!(output.contains("test_table"))
            }
        }

        mod filter {
            use super::*;

            #[test]
            fn creates_selection_plan() {
                let df = create_test_dataframe();
                let filtered = df.filter(Expr::Binary(Binary::eq(
                    Expr::Column(Column::new("age")),
                    Expr::Literal(Literal::int(30)),
                )));

                let output = format!("{}", filtered);
                assert!(output.contains("Filter: #age = 30"));
            }

            #[test]
            fn filter_preserves_schema() {
                let df = create_test_dataframe();
                let original_schema = df.schema();

                let filtered = df.filter(Expr::Binary(Binary::gt(
                    Expr::Column(Column::new("age")),
                    Expr::Literal(Literal::int(25)),
                )));

                assert_eq!(filtered.schema(), original_schema);
            }

            #[test]
            fn filter_with_and_condition() {
                let df = create_test_dataframe();
                let filtered = df.filter(Expr::Binary(Binary::and(
                    Expr::Binary(Binary::gt(
                        Expr::Column(Column::new("age")),
                        Expr::Literal(Literal::int(25)),
                    )),
                    Expr::Binary(Binary::eq(
                        Expr::Column(Column::new("active")),
                        Expr::Literal(Literal::bool(true)),
                    )),
                )));

                let output = format!("{}", filtered);
                assert!(output.contains("Filter: #age > 25 AND #active = true"));
            }

            #[test]
            fn filter_with_or_condition() {
                let df = create_test_dataframe();
                let filtered = df.filter(Expr::Binary(Binary::or(
                    Expr::Binary(Binary::eq(
                        Expr::Column(Column::new("name")),
                        Expr::Literal(Literal::string("Alice")),
                    )),
                    Expr::Binary(Binary::eq(
                        Expr::Column(Column::new("name")),
                        Expr::Literal(Literal::string("Bob")),
                    )),
                )));

                let output = format!("{}", filtered);
                assert!(output.contains("OR"));
            }

            #[test]
            fn filter_preserves_input_plan() {
                let df = create_test_dataframe();
                let filtered = df.filter(Expr::Binary(Binary::eq(
                    Expr::Column(Column::new("active")),
                    Expr::Literal(Literal::bool(true)),
                )));

                let output = format!("{}", filtered);
                assert!(output.contains("Scan: "));
                assert!(output.contains("test_table"))
            }
        }

        mod aggregate {
            use super::*;

            #[test]
            fn creates_aggregate_plan() {
                let df = create_test_dataframe();
                let aggregated = df.aggregate(
                    vec![Expr::Column(Column::new("active"))],
                    vec![AggregateExpr::sum(Expr::Column(Column::new("salary")))],
                );

                let output = format!("{}", aggregated);
                assert!(output.contains("Aggregate:"));
                assert!(output.contains("groupBy=[#active]"));
                assert!(output.contains("SUM(#salary)"));
            }

            #[test]
            fn aggregate_schema_has_group_and_agg_columns() {
                let df = create_test_dataframe();
                let aggregated = df.aggregate(
                    vec![Expr::Column(Column::new("active"))],
                    vec![
                        AggregateExpr::sum(Expr::Column(Column::new("salary"))),
                        AggregateExpr::count(Expr::Column(Column::new("id"))),
                    ],
                );

                let schema = aggregated.schema();
                assert_eq!(schema.fields().len(), 3);

                let field_names: Vec<&str> =
                    schema.fields().iter().map(|f| f.name().as_str()).collect();
                assert_eq!(field_names, vec!["active", "SUM(#salary)", "COUNT(#id)"]);
            }

            #[test]
            fn aggregate_without_group_by() {
                let df = create_test_dataframe();
                let aggregated = df.aggregate(
                    vec![],
                    vec![
                        AggregateExpr::count(Expr::Column(Column::new("id"))),
                        AggregateExpr::avg(Expr::Column(Column::new("salary"))),
                    ],
                );

                let schema = aggregated.schema();
                assert_eq!(schema.fields().len(), 2);

                let output = format!("{}", aggregated);
                assert!(output.contains("groupBy=[]"));
            }

            #[test]
            fn aggregate_with_multiple_group_columns() {
                let df = create_test_dataframe();
                let aggregated = df.aggregate(
                    vec![
                        Expr::Column(Column::new("active")),
                        Expr::Column(Column::new("name")),
                    ],
                    vec![AggregateExpr::max(Expr::Column(Column::new("age")))],
                );

                let output = format!("{}", aggregated);
                assert!(output.contains("groupBy=[#active, #name]"));
            }

            #[test]
            fn aggregate_preserves_input_plan() {
                let df = create_test_dataframe();
                let aggregated = df.aggregate(
                    vec![Expr::Column(Column::new("active"))],
                    vec![AggregateExpr::sum(Expr::Column(Column::new("salary")))],
                );

                let output = format!("{}", aggregated);
                println!("{}", output);
                assert!(output.contains("Scan: "));
                assert!(output.contains("test_table"))
            }
        }

        mod schema {
            use super::*;

            #[test]
            fn returns_underlying_plan_schema() {
                let df = create_test_dataframe();
                let schema = df.schema();

                let field_names: Vec<&str> =
                    schema.fields().iter().map(|f| f.name().as_str()).collect();
                assert_eq!(field_names, vec!["id", "name", "age", "salary", "active"]);
            }

            #[test]
            fn schema_reflects_transformations() {
                let df = create_test_dataframe();
                let projected = df.project(vec![
                    Expr::Column(Column::new("name")),
                    Expr::Column(Column::new("age")),
                ]);

                let schema = projected.schema();
                let field_names: Vec<&str> =
                    schema.fields().iter().map(|f| f.name().as_str()).collect();
                assert_eq!(field_names, vec!["name", "age"]);
            }
        }

        mod logical_plan {
            use super::*;

            #[test]
            fn returns_arc_to_plan() {
                let df = create_test_dataframe();
                let plan = df.logical_plan();

                let schema = plan.schema();
                assert_eq!(schema.fields().len(), 5);
            }

            #[test]
            fn plan_can_be_cloned() {
                let df = create_test_dataframe();
                let plan1 = df.logical_plan();
                let plan2 = df.logical_plan();

                assert_eq!(plan1.schema(), plan2.schema());
            }
        }

        mod display {
            use super::*;

            #[test]
            fn displays_underlying_plan() {
                let df = create_test_dataframe();
                let output = format!("{}", df);
                println!("{}", output);

                assert!(output.contains("Scan: "));
                assert!(output.contains("test_table"))
            }

            #[test]
            fn displays_chained_operations() {
                let df = create_test_dataframe();
                let result = df
                    .filter(Expr::Binary(Binary::gt(
                        Expr::Column(Column::new("age")),
                        Expr::Literal(Literal::int(25)),
                    )))
                    .project(vec![
                        Expr::Column(Column::new("name")),
                        Expr::Column(Column::new("salary")),
                    ]);

                let output = format!("{}", result);
                assert!(output.contains("Projection:"));
                assert!(output.contains("Filter:"));
                assert!(output.contains("Scan:"));
            }
        }

        mod chaining {
            use super::*;

            #[test]
            fn filter_then_project() {
                let df = create_test_dataframe();
                let result = df
                    .filter(Expr::Binary(Binary::eq(
                        Expr::Column(Column::new("active")),
                        Expr::Literal(Literal::bool(true)),
                    )))
                    .project(vec![Expr::Column(Column::new("name"))]);

                let output = format!("{}", result);

                let lines: Vec<&str> = output.lines().collect();
                assert!(lines[0].contains("Projection:"));
                assert!(lines[1].contains("Filter:"));
                assert!(lines[2].contains("Scan:"));
            }

            #[test]
            fn project_then_filter() {
                let df = create_test_dataframe();
                let result = df
                    .project(vec![
                        Expr::Column(Column::new("name")),
                        Expr::Column(Column::new("age")),
                    ])
                    .filter(Expr::Binary(Binary::gt(
                        Expr::Column(Column::new("age")),
                        Expr::Literal(Literal::int(25)),
                    )));

                let output = format!("{}", result);

                let lines: Vec<&str> = output.lines().collect();
                assert!(lines[0].contains("Filter:"));
                assert!(lines[1].contains("Projection:"));
                assert!(lines[2].contains("Scan:"));
            }

            #[test]
            fn filter_then_aggregate() {
                let df = create_test_dataframe();
                let result = df
                    .filter(Expr::Binary(Binary::gt(
                        Expr::Column(Column::new("age")),
                        Expr::Literal(Literal::int(25)),
                    )))
                    .aggregate(
                        vec![Expr::Column(Column::new("active"))],
                        vec![AggregateExpr::sum(Expr::Column(Column::new("salary")))],
                    );

                let output = format!("{}", result);

                let lines: Vec<&str> = output.lines().collect();
                assert!(lines[0].contains("Aggregate:"));
                assert!(lines[1].contains("Filter:"));
                assert!(lines[2].contains("Scan:"));
            }

            #[test]
            fn multiple_filters() {
                let df = create_test_dataframe();
                let result = df
                    .filter(Expr::Binary(Binary::gt(
                        Expr::Column(Column::new("age")),
                        Expr::Literal(Literal::int(20)),
                    )))
                    .filter(Expr::Binary(Binary::lt(
                        Expr::Column(Column::new("age")),
                        Expr::Literal(Literal::int(40)),
                    )))
                    .filter(Expr::Binary(Binary::eq(
                        Expr::Column(Column::new("active")),
                        Expr::Literal(Literal::bool(true)),
                    )));

                let output = format!("{}", result);

                let filter_count = output.matches("Filter:").count();
                assert_eq!(filter_count, 3);
            }

            #[test]
            fn complex_query_chain() {
                // SELECT name AS employee, SUM(salary) AS total_pay
                // FROM test_table
                // WHERE age > 25 AND active = true
                // GROUP BY name

                let df = create_test_dataframe();
                let result = df
                    .filter(Expr::Binary(Binary::and(
                        Expr::Binary(Binary::gt(
                            Expr::Column(Column::new("age")),
                            Expr::Literal(Literal::int(25)),
                        )),
                        Expr::Binary(Binary::eq(
                            Expr::Column(Column::new("active")),
                            Expr::Literal(Literal::bool(true)),
                        )),
                    )))
                    .aggregate(
                        vec![Expr::Column(Column::new("name"))],
                        vec![AggregateExpr::sum(Expr::Column(Column::new("salary")))],
                    );

                let schema = result.schema();
                let field_names: Vec<&str> =
                    schema.fields().iter().map(|f| f.name().as_str()).collect();
                assert_eq!(field_names, vec!["name", "SUM(#salary)"]);

                let output = format!("{}", result);
                assert!(output.contains("Aggregate:"));
                assert!(output.contains("Filter:"));
                assert!(output.contains("Scan:"));
            }
        }
    }

    mod integration {
        use super::*;
        use std::fs::File;
        use std::io::Write;
        use tempfile::tempdir;

        #[test]
        fn end_to_end_csv_query() {
            let dir = tempdir().unwrap();
            let file_path = dir.path().join("employees.csv");
            let mut file = File::create(&file_path).unwrap();

            writeln!(file, "id;name;department;salary").unwrap();
            writeln!(file, "1;Alice;Engineering;75000").unwrap();
            writeln!(file, "2;Bob;Sales;65000").unwrap();
            writeln!(file, "3;Charlie;Engineering;80000").unwrap();

            let ctx = ExecutionContext::new();

            // SELECT name, salary
            // FROM employees
            // WHERE department = 'Engineering'
            let df = ctx
                .csv(&file_path.to_string_lossy(), b';', true)
                .filter(Expr::Binary(Binary::eq(
                    Expr::Column(Column::new("department")),
                    Expr::Literal(Literal::string("Engineering")),
                )))
                .project(vec![
                    Expr::Column(Column::new("name")),
                    Expr::Column(Column::new("salary")),
                ]);

            let output = format!("{}", df);
            assert!(output.contains("Projection: #name, #salary"));
            assert!(output.contains("Filter: #department = 'Engineering'"));

            let schema = df.schema();
            let field_names: Vec<&str> =
                schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert_eq!(field_names, vec!["name", "salary"]);
        }

        #[test]
        fn aggregate_query_with_group_by() {
            let dir = tempdir().unwrap();
            let file_path = dir.path().join("sales.csv");
            let mut file = File::create(&file_path).unwrap();

            writeln!(file, "region;product;amount").unwrap();
            writeln!(file, "North;Widget;100").unwrap();
            writeln!(file, "South;Gadget;200").unwrap();
            writeln!(file, "North;Gadget;150").unwrap();

            let ctx = ExecutionContext::new();

            // SELECT region, SUM(amount), COUNT(product)
            // FROM sales
            // GROUP BY region
            let df = ctx.csv(&file_path.to_string_lossy(), b';', true).aggregate(
                vec![Expr::Column(Column::new("region"))],
                vec![
                    AggregateExpr::sum(Expr::Column(Column::new("amount"))),
                    AggregateExpr::count(Expr::Column(Column::new("product"))),
                ],
            );

            let schema = df.schema();
            let field_names: Vec<&str> =
                schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert_eq!(
                field_names,
                vec!["region", "SUM(#amount)", "COUNT(#product)"]
            );
        }

        #[test]
        fn complex_analytical_query() {
            let dir = tempdir().unwrap();
            let file_path = dir.path().join("orders.csv");
            let mut file = File::create(&file_path).unwrap();

            writeln!(file, "order_id;customer;status;total").unwrap();
            writeln!(file, "1;Alice;completed;100").unwrap();
            writeln!(file, "2;Bob;pending;200").unwrap();
            writeln!(file, "3;Alice;completed;150").unwrap();
            writeln!(file, "4;Charlie;completed;300").unwrap();

            let ctx = ExecutionContext::new();

            // SELECT customer, SUM(total) AS total_spent, COUNT(order_id) AS order_count
            // FROM orders
            // WHERE status = 'completed'
            // GROUP BY customer
            let df = ctx
                .csv(&file_path.to_string_lossy(), b';', true)
                .filter(Expr::Binary(Binary::eq(
                    Expr::Column(Column::new("status")),
                    Expr::Literal(Literal::string("completed")),
                )))
                .aggregate(
                    vec![Expr::Column(Column::new("customer"))],
                    vec![
                        AggregateExpr::sum(Expr::Column(Column::new("total"))),
                        AggregateExpr::count(Expr::Column(Column::new("order_id"))),
                    ],
                );

            let output = format!("{}", df);

            assert!(output.contains("Aggregate:"));
            assert!(output.contains("Filter:"));
            assert!(output.contains("Scan:"));

            let schema = df.schema();
            assert_eq!(schema.fields().len(), 3);
        }

        #[test]
        fn dataframe_is_immutable() {
            let dir = tempdir().unwrap();
            let file_path = dir.path().join("test.csv");
            let mut file = File::create(&file_path).unwrap();

            writeln!(file, "a;b").unwrap();
            writeln!(file, "1;2").unwrap();

            let ctx = ExecutionContext::new();
            let df1 = ctx.csv(&file_path.to_string_lossy(), b';', true);

            let df2 = df1.filter(Expr::Binary(Binary::eq(
                Expr::Column(Column::new("a")),
                Expr::Literal(Literal::int(1)),
            )));
            let df3 = df1.project(vec![Expr::Column(Column::new("b"))]);

            let output1 = format!("{}", df1);
            assert!(!output1.contains("Filter:"));
            assert!(!output1.contains("Projection:"));

            let output2 = format!("{}", df2);
            assert!(output2.contains("Filter:"));

            let output3 = format!("{}", df3);
            assert!(output3.contains("Projection:"));
        }
    }
}
