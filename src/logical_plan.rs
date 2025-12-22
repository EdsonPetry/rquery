use core::fmt;
use std::{fmt::Display, iter::chain, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema};

use crate::data_sources::DataSource;

pub trait LogicalPlan: Display {
    /// Returns schema that will be produced by logical plan
    fn schema(&self) -> Arc<Schema>;

    /// Returns the children of this logical plan
    fn children(&self) -> Vec<Arc<dyn LogicalPlan>>;

    /// Returns a string representation of just this node (without children)
    /// e.g., "Projection: #name, #salary * 1.1 AS new_salary"
    fn format_node(&self) -> String;
}

fn format_plan(plan: &dyn LogicalPlan, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
    write!(f, "{}", "\t".repeat(indent))?;
    writeln!(f, "{}", plan.format_node())?;
    for child in plan.children() {
        format_plan(child.as_ref(), f, indent + 1)?;
    }

    Ok(())
}

// Expression Type          Example
// ------------------------------------------
// Literal Value            "hello", 12.34, true
// Column Reference         user_id, first_name, salary
// Math Expression          salary * 0.1, price + tax
// Comparison Expression    age >= 21, status != 'inactive'
// Boolean Expression       age >= 21 AND country = 'US'
// Aggregate Expression     MIN(salary), MAX(salary), SUM(amount)
// Scalar Function          UPPER(name), CONCAT(first_name, ' ', last_name)
// Aliased Expression       salary * 1.1 AS new_salary

pub trait LogicalExpr: fmt::Display {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> Arc<Field>;
}

pub struct ColumnExpr {
    name: String,
}

impl ColumnExpr {
    fn new(name: &str) -> Self {
        ColumnExpr {
            name: name.to_string(),
        }
    }
}

impl LogicalExpr for ColumnExpr {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> Arc<Field> {
        let schema = input.schema();
        let fields = schema.fields();
        let (_, field) = fields
            .find(self.name.as_str())
            .expect(format!("No column named {}", self.name).as_str());
        field.clone()
    }
}

impl fmt::Display for ColumnExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.name)
    }
}

struct LiteralStringExpr {
    s: String,
}

impl LiteralStringExpr {
    fn new(input: &str) -> Self {
        LiteralStringExpr {
            s: input.to_string(),
        }
    }
}

impl fmt::Display for LiteralStringExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}", self.s)
    }
}

impl LogicalExpr for LiteralStringExpr {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> Arc<Field> {
        Arc::new(Field::new(&self.s, DataType::Utf8, false))
    }
}

struct LiteralLong {
    n: i64,
}

impl fmt::Display for LiteralLong {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.n)
    }
}

impl LogicalExpr for LiteralLong {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> Arc<Field> {
        Arc::new(Field::new(self.n.to_string(), DataType::Int64, false))
    }
}

struct LiteralDouble {
    n: f64,
}

impl fmt::Display for LiteralDouble {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.n)
    }
}

impl LogicalExpr for LiteralDouble {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> Arc<Field> {
        Arc::new(Field::new(self.n.to_string(), DataType::Float64, false))
    }
}

struct BinaryExpr<T: LogicalExpr> {
    name: String,
    op: String,
    left: T,
    right: T,
}

impl<T: LogicalExpr> BinaryExpr<T> {
    fn new(name: &str, op: &str, left: T, right: T) -> Self {
        BinaryExpr {
            name: name.to_string(),
            op: op.to_string(),
            left,
            right,
        }
    }
}

impl<T: LogicalExpr + fmt::Display> fmt::Display for BinaryExpr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

struct AggregateExpr<T: LogicalExpr> {
    name: String,
    expr: T,
}

impl<T: LogicalExpr> LogicalExpr for AggregateExpr<T> {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> Arc<Field> {
        Arc::new(Field::new(
            &self.name,
            self.expr.to_field(input).data_type().clone(),
            false,
        ))
    }
}

impl<T: LogicalExpr> fmt::Display for AggregateExpr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.name, self.expr)
    }
}

struct Alias<T: LogicalExpr> {
    expr: T,
    alias: String,
}

impl<T: LogicalExpr> LogicalExpr for Alias<T> {
    fn to_field(&self, input: Arc<dyn LogicalPlan>) -> Arc<Field> {
        Arc::new(Field::new(
            &self.alias,
            self.expr.to_field(input).data_type().clone(),
            false,
        ))
    }
}

impl<T: LogicalExpr> fmt::Display for Alias<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} as {}", self.expr, self.alias)
    }
}

struct Scan<D: DataSource> {
    path: String,
    data_source: D,
    projection: Option<Vec<String>>,
    schema: Arc<Schema>,
}

impl<D: DataSource> Scan<D> {
    fn new(path: &str, data_source: D, projection: Option<Vec<String>>) -> Self {
        Scan {
            path: path.to_string(),
            data_source: data_source.clone(),
            projection,
            schema: data_source.schema().clone(),
        }
    }
}

impl<D: DataSource> LogicalPlan for Scan<D> {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        let children: Vec<Arc<dyn LogicalPlan>> = Vec::new();
        children
    }

    fn format_node(&self) -> String {
        match &self.projection {
            None => format!("Scan: {}; projection=None", self.path),
            Some(p) => format!("Scan: {}; projection={:?}", self.path, p),
        }
    }
}

impl<D: DataSource> Display for Scan<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.format_node())
    }
}

struct Selection<E: LogicalExpr> {
    input: Arc<dyn LogicalPlan>,
    expr: E,
}

impl<E: LogicalExpr> LogicalPlan for Selection<E> {
    fn schema(&self) -> Arc<Schema> {
        self.input.schema().clone() // filtering doesn't change schema, only removes rows
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        let children: Vec<Arc<dyn LogicalPlan>> = vec![self.input.clone()];
        children
    }

    fn format_node(&self) -> String {
        format!("Filter: {}", self.expr)
    }
}

impl<E: LogicalExpr> Display for Selection<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.format_node())
    }
}

struct Projection<E: LogicalExpr> {
    input: Arc<dyn LogicalPlan>,
    expr: Vec<E>,
}

impl<E: LogicalExpr> LogicalPlan for Projection<E> {
    fn schema(&self) -> Arc<Schema> {
        let fields: Vec<Arc<Field>> = self
            .expr
            .iter()
            .map(|f| f.to_field(self.input.clone()))
            .collect();
        let schema = Schema::new(fields);
        Arc::new(schema)
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        let children: Vec<Arc<dyn LogicalPlan>> = vec![self.input.clone()];
        children
    }

    fn format_node(&self) -> String {
        format!(
            "Projection: {:?}",
            self.expr.iter().map(|e| e.to_string()).collect::<Vec<_>>()
        )
    }
}

impl<E: LogicalExpr> Display for Projection<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.format_node())
    }
}

struct Aggregate<E: LogicalExpr> {
    input: Arc<dyn LogicalPlan>,
    group_expr: Vec<E>,
    aggregate_expr: Vec<AggregateExpr<E>>,
}

impl<E: LogicalExpr> LogicalPlan for Aggregate<E> {
    fn schema(&self) -> Arc<Schema> {
        let group_iter = self
            .group_expr
            .iter()
            .map(|f| f.to_field(self.input.clone()));
        let aggregate_iter = self
            .aggregate_expr
            .iter()
            .map(|f| f.to_field(self.input.clone()));

        let fields: Vec<Arc<Field>> = chain(group_iter, aggregate_iter).collect();
        Arc::new(Schema::new(fields))
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        let children: Vec<Arc<dyn LogicalPlan>> = vec![self.input.clone()];
        children
    }

    fn format_node(&self) -> String {
        format!(
            "Aggregate: group_expr={:?}, aggregate_expr={:?}",
            self.group_expr
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>(),
            self.aggregate_expr
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
        )
    }
}

impl<E: LogicalExpr> Display for Aggregate<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.format_node())
    }
}
