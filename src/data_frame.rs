use std::{fmt::Display, sync::Arc};

use arrow::datatypes::Schema;

use crate::{
    data_sources::{CsvDataSource, ParquetDataSource},
    logical_plan::{
        Aggregate, AggregateExpr, LogicalExpr, LogicalPlan, Projection, Scan, Selection,
    },
};

// pub trait DataFrame {
//     fn project(&self, expr: Vec<&dyn LogicalExpr>) -> Box<dyn DataFrame>;
//     fn filter(&self, expr: &dyn LogicalExpr) -> Box<dyn DataFrame>;
//     fn aggregate(
//         &self,
//         group_by: Vec<&dyn LogicalExpr>,
//         aggregate_expr: Vec<&dyn LogicalExpr>,
//     ) -> Box<dyn DataFrame>;
//     fn schema(&self) -> Arc<Schema>;
//     fn logical_plan(&self) -> Arc<dyn LogicalPlan>;
// }

pub struct DataFrame {
    plan: Arc<dyn LogicalPlan>,
}

impl DataFrame {
    pub fn project(&self, expr: Vec<Box<dyn LogicalExpr>>) -> Self {
        DataFrame {
            plan: Arc::new(Projection::new(self.plan.clone(), expr)),
        }
    }
    pub fn filter(&self, expr: impl LogicalExpr + 'static) -> Self {
        DataFrame {
            plan: Arc::new(Selection::new(self.plan.clone(), expr)),
        }
    }
    pub fn aggregate(
        &self,
        group_by: Vec<Box<dyn LogicalExpr>>,
        aggregate_expr: Vec<AggregateExpr>,
    ) -> Self {
        DataFrame {
            plan: Arc::new(Aggregate::new(self.plan.clone(), group_by, aggregate_expr)),
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
        let plan = Scan::new(
            filename,
            Box::new(CsvDataSource::try_new(filename.to_string(), delimiter, header).unwrap()),
            None,
        );

        DataFrame {
            plan: Arc::new(plan),
        }
    }

    pub fn parquet(&self, filename: &str) -> DataFrame {
        let plan = Scan::new(
            filename,
            Box::new(ParquetDataSource::try_new(filename).unwrap()),
            None,
        );
        DataFrame {
            plan: Arc::new(plan),
        }
    }
}
