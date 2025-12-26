use core::fmt;
use std::{fmt::Display, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema};

use crate::data_sources::DataSource;

// Logical Plan

pub trait LogicalPlan: Display {
    fn schema(&self) -> Arc<Schema>;
    fn children(&self) -> Vec<Arc<dyn LogicalPlan>>;
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

// Logical Expressions

pub trait LogicalExpr: fmt::Display {
    fn to_field(&self, input: &dyn LogicalPlan) -> Arc<Field>;
}

pub struct Column {
    pub name: String,
}

impl Column {
    pub fn new(name: &str) -> Self {
        Column {
            name: name.to_string(),
        }
    }
}

impl LogicalExpr for Column {
    fn to_field(&self, input: &dyn LogicalPlan) -> Arc<Field> {
        let schema = input.schema();
        let (_, field) = schema
            .fields()
            .find(&self.name)
            .expect(&format!("No column named {}", self.name));
        field.clone()
    }
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.name)
    }
}

#[derive(Clone, Debug)]
pub enum LiteralValue {
    String(String),
    Int64(i64),
    Float64(f64),
    Bool(bool),
}

pub struct Literal {
    pub value: LiteralValue,
}

impl Literal {
    pub fn string(s: &str) -> Self {
        Literal {
            value: LiteralValue::String(s.to_string()),
        }
    }

    pub fn int(n: i64) -> Self {
        Literal {
            value: LiteralValue::Int64(n),
        }
    }

    pub fn float(n: f64) -> Self {
        Literal {
            value: LiteralValue::Float64(n),
        }
    }

    pub fn bool(b: bool) -> Self {
        Literal {
            value: LiteralValue::Bool(b),
        }
    }
}

impl LogicalExpr for Literal {
    fn to_field(&self, _input: &dyn LogicalPlan) -> Arc<Field> {
        let data_type = match &self.value {
            LiteralValue::String(_) => DataType::Utf8,
            LiteralValue::Int64(_) => DataType::Int64,
            LiteralValue::Float64(_) => DataType::Float64,
            LiteralValue::Bool(_) => DataType::Boolean,
        };
        Arc::new(Field::new(self.to_string(), data_type, false))
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.value {
            LiteralValue::String(s) => write!(f, "'{}'", s),
            LiteralValue::Int64(n) => write!(f, "{}", n),
            LiteralValue::Float64(n) => write!(f, "{}", n),
            LiteralValue::Bool(b) => write!(f, "{}", b),
        }
    }
}

// BINARY OPERATORS
#[derive(Debug, Clone, Copy)]
pub enum BinaryOp {
    // Comparison
    Eq,
    Neq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    // Boolean
    And,
    Or,
    // Math
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

impl BinaryOp {
    pub fn symbol(&self) -> &'static str {
        match self {
            BinaryOp::Eq => "=",
            BinaryOp::Neq => "!=",
            BinaryOp::Lt => "<",
            BinaryOp::LtEq => "<=",
            BinaryOp::Gt => ">",
            BinaryOp::GtEq => ">=",
            BinaryOp::And => "AND",
            BinaryOp::Or => "OR",
            BinaryOp::Add => "+",
            BinaryOp::Sub => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
            BinaryOp::Mod => "%",
        }
    }

    pub fn is_boolean_result(&self) -> bool {
        matches!(
            self,
            BinaryOp::Eq
                | BinaryOp::Neq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq
                | BinaryOp::And
                | BinaryOp::Or
        )
    }
}

pub struct BinaryExpr {
    pub left: Box<dyn LogicalExpr>,
    pub op: BinaryOp,
    pub right: Box<dyn LogicalExpr>,
}

impl BinaryExpr {
    pub fn new(
        left: impl LogicalExpr + 'static,
        op: BinaryOp,
        right: impl LogicalExpr + 'static,
    ) -> Self {
        BinaryExpr {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    pub fn eq(left: impl LogicalExpr + 'static, right: impl LogicalExpr + 'static) -> Self {
        Self::new(left, BinaryOp::Eq, right)
    }

    pub fn neq(left: impl LogicalExpr + 'static, right: impl LogicalExpr + 'static) -> Self {
        Self::new(left, BinaryOp::Neq, right)
    }

    pub fn gt(left: impl LogicalExpr + 'static, right: impl LogicalExpr + 'static) -> Self {
        Self::new(left, BinaryOp::Gt, right)
    }

    pub fn lt(left: impl LogicalExpr + 'static, right: impl LogicalExpr + 'static) -> Self {
        Self::new(left, BinaryOp::Lt, right)
    }

    pub fn and(left: impl LogicalExpr + 'static, right: impl LogicalExpr + 'static) -> Self {
        Self::new(left, BinaryOp::And, right)
    }

    pub fn or(left: impl LogicalExpr + 'static, right: impl LogicalExpr + 'static) -> Self {
        Self::new(left, BinaryOp::Or, right)
    }

    pub fn add(left: impl LogicalExpr + 'static, right: impl LogicalExpr + 'static) -> Self {
        Self::new(left, BinaryOp::Add, right)
    }

    pub fn mul(left: impl LogicalExpr + 'static, right: impl LogicalExpr + 'static) -> Self {
        Self::new(left, BinaryOp::Mul, right)
    }
}

impl LogicalExpr for BinaryExpr {
    fn to_field(&self, input: &dyn LogicalPlan) -> Arc<Field> {
        if self.op.is_boolean_result() {
            Arc::new(Field::new(self.to_string(), DataType::Boolean, false))
        } else {
            // use left operand's type (simplified)
            self.left.to_field(input)
        }
    }
}

impl fmt::Display for BinaryExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.op.symbol(), self.right)
    }
}

// AGGREGATE EXPRESSIONS
#[derive(Debug, Clone, Copy)]
pub enum AggregateOp {
    Sum,
    Min,
    Max,
    Avg,
    Count,
}

impl AggregateOp {
    pub fn symbol(&self) -> &'static str {
        match self {
            AggregateOp::Sum => "SUM",
            AggregateOp::Min => "MIN",
            AggregateOp::Max => "MAX",
            AggregateOp::Avg => "AVG",
            AggregateOp::Count => "COUNT",
        }
    }
}

pub struct AggregateExpr {
    op: AggregateOp,
    expr: Box<dyn LogicalExpr>,
}

impl AggregateExpr {
    pub fn new(op: AggregateOp, expr: impl LogicalExpr + 'static) -> Self {
        AggregateExpr {
            op,
            expr: Box::new(expr),
        }
    }

    pub fn sum(expr: impl LogicalExpr + 'static) -> Self {
        Self::new(AggregateOp::Sum, expr)
    }

    pub fn min(expr: impl LogicalExpr + 'static) -> Self {
        Self::new(AggregateOp::Min, expr)
    }

    pub fn max(expr: impl LogicalExpr + 'static) -> Self {
        Self::new(AggregateOp::Max, expr)
    }

    pub fn avg(expr: impl LogicalExpr + 'static) -> Self {
        Self::new(AggregateOp::Avg, expr)
    }

    pub fn count(expr: impl LogicalExpr + 'static) -> Self {
        Self::new(AggregateOp::Count, expr)
    }
}

impl LogicalExpr for AggregateExpr {
    fn to_field(&self, input: &dyn LogicalPlan) -> Arc<Field> {
        let inner_field = self.expr.to_field(input);

        let data_type = match self.op {
            AggregateOp::Count => DataType::Int64,
            AggregateOp::Avg => DataType::Float64,
            // Sum, Min, Max preserve the input type
            AggregateOp::Sum | AggregateOp::Min | AggregateOp::Max => {
                inner_field.data_type().clone()
            }
        };

        let name = format!("{}({})", self.op.symbol(), self.expr);
        Arc::new(Field::new(&name, data_type, true))
    }
}

impl Display for AggregateExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.op.symbol(), self.expr)
    }
}

pub struct Alias {
    pub expr: Box<dyn LogicalExpr>,
    pub alias: String,
}

impl Alias {
    pub fn new(expr: impl LogicalExpr + 'static, alias: &str) -> Self {
        Alias {
            expr: Box::new(expr),
            alias: alias.to_string(),
        }
    }
}

impl LogicalExpr for Alias {
    fn to_field(&self, input: &dyn LogicalPlan) -> Arc<Field> {
        let field = self.expr.to_field(input);
        Arc::new(Field::new(
            &self.alias,
            field.data_type().clone(),
            field.is_nullable(),
        ))
    }
}

impl fmt::Display for Alias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} AS {}", self.expr, self.alias)
    }
}

// ================== SCAN ==============

pub struct Scan {
    pub path: String,
    pub data_source: Box<dyn DataSource>,
    pub projection: Option<Vec<String>>,
    schema: Arc<Schema>,
}

impl Scan {
    pub fn new(
        path: &str,
        data_source: Box<dyn DataSource>,
        projection: Option<Vec<String>>,
    ) -> Self {
        let schema = data_source.schema().clone();
        Scan {
            path: path.to_string(),
            data_source,
            projection,
            schema,
        }
    }
}

impl LogicalPlan for Scan {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        vec![]
    }

    fn format_node(&self) -> String {
        match &self.projection {
            None => format!("Scan: {}; projection=None", self.path),
            Some(p) => format!("Scan: {}; projection={:?}", self.path, p),
        }
    }
}

impl Display for Scan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_plan(self, f, 0)
    }
}

// ============== SELECTION ===============

pub struct Selection {
    pub input: Arc<dyn LogicalPlan>,
    pub expr: Box<dyn LogicalExpr>,
}

impl Selection {
    pub fn new(input: Arc<dyn LogicalPlan>, expr: impl LogicalExpr + 'static) -> Self {
        Selection {
            input,
            expr: Box::new(expr),
        }
    }
}

impl LogicalPlan for Selection {
    fn schema(&self) -> Arc<Schema> {
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        vec![self.input.clone()]
    }

    fn format_node(&self) -> String {
        format!("Filter: {}", self.expr)
    }
}

impl Display for Selection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_plan(self, f, 0)
    }
}

// ================= PROJECTION ===============

pub struct Projection {
    pub input: Arc<dyn LogicalPlan>,
    pub expr: Vec<Box<dyn LogicalExpr>>,
}

impl Projection {
    pub fn new(input: Arc<dyn LogicalPlan>, expr: Vec<Box<dyn LogicalExpr>>) -> Self {
        Projection { input, expr }
    }
}

impl LogicalPlan for Projection {
    fn schema(&self) -> Arc<Schema> {
        let fields: Vec<Arc<Field>> = self
            .expr
            .iter()
            .map(|e| e.to_field(self.input.as_ref()))
            .collect();
        Arc::new(Schema::new(fields))
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        vec![self.input.clone()]
    }

    fn format_node(&self) -> String {
        let exprs: Vec<String> = self.expr.iter().map(|e| e.to_string()).collect();
        format!("Projection: {}", exprs.join(", "))
    }
}

impl Display for Projection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_plan(self, f, 0)
    }
}

// ================= AGGREGATE ===============

pub struct Aggregate {
    pub input: Arc<dyn LogicalPlan>,
    pub group_expr: Vec<Box<dyn LogicalExpr>>,
    pub aggregate_expr: Vec<AggregateExpr>,
}

impl Aggregate {
    pub fn new(
        input: Arc<dyn LogicalPlan>,
        group_expr: Vec<Box<dyn LogicalExpr>>,
        aggregate_expr: Vec<AggregateExpr>,
    ) -> Self {
        Aggregate {
            input,
            group_expr,
            aggregate_expr,
        }
    }
}

impl LogicalPlan for Aggregate {
    fn schema(&self) -> Arc<Schema> {
        let mut fields: Vec<Arc<Field>> = self
            .group_expr
            .iter()
            .map(|e| e.to_field(self.input.as_ref()))
            .collect();

        let agg_fields: Vec<Arc<Field>> = self
            .aggregate_expr
            .iter()
            .map(|e| e.to_field(self.input.as_ref()))
            .collect();

        fields.extend(agg_fields);
        Arc::new(Schema::new(fields))
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        vec![self.input.clone()]
    }

    fn format_node(&self) -> String {
        let group_exprs: Vec<String> = self.group_expr.iter().map(|e| e.to_string()).collect();
        let agg_exprs: Vec<String> = self.aggregate_expr.iter().map(|e| e.to_string()).collect();

        format!(
            "Aggregate: groupBy=[{}], aggr=[{}]",
            group_exprs.join(", "),
            agg_exprs.join(", ")
        )
    }
}

impl Display for Aggregate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_plan(self, f, 0)
    }
}
