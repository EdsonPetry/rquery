use core::fmt;
use std::{fmt::Display, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema};

// Logical Plan

pub trait LogicalPlan: Display {
    fn schema(&self) -> Arc<Schema>;
    fn children(&self) -> Vec<Arc<Plan>>;
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

// ============= Logical Expressions ================
// RQuery provides support for the following expression types:
// Literal values ("hello", 12.34, true)
// Column references (user_id, first_name, salary)
// Binary expressions:
//      Math expressions (salary * 0.1, price + tax)
//      Comparison expressions (age >= 21, status != 'inactive')
//      Boolean expressions (age >= 21 AND country = 'US')
// Aggregate functions (MIN(salary), MAX(salary), SUM(amount), COUNT(*))
// Scalar functions (UPPER(name), CONCAT(first_name, ' ', last_name))
// Aliased expressions (salary * 1.1 AS new_salary)

pub enum Expr {
    Literal(Literal),
    Column(Column),
    Binary(Binary),
    Aggregate(AggregateExpr),
    Alias(Alias),
}

impl Expr {
    pub fn to_field(&self, input: &dyn LogicalPlan) -> Arc<Field> {
        match self {
            Expr::Literal(lit) => lit.to_field(input),
            Expr::Column(col) => col.to_field(input),
            Expr::Binary(bin) => bin.to_field(input),
            Expr::Aggregate(agg) => agg.to_field(input),
            Expr::Alias(alias) => alias.to_field(input),
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Literal(lit) => write!(f, "{}", lit),
            Expr::Column(col) => write!(f, "{}", col),
            Expr::Binary(bin) => write!(f, "{}", bin),
            Expr::Aggregate(agg) => write!(f, "{}", agg),
            Expr::Alias(alias) => write!(f, "{}", alias),
        }
    }
}

impl LogicalExpr for Expr {
    fn to_field(&self, input: &dyn LogicalPlan) -> Arc<Field> {
        Expr::to_field(self, input)
    }
}

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

pub struct Binary {
    pub left: Box<Expr>,
    pub op: BinaryOp,
    pub right: Box<Expr>,
}

impl Binary {
    pub fn new(left: Expr, op: BinaryOp, right: Expr) -> Self {
        Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    pub fn eq(left: Expr, right: Expr) -> Self {
        Self::new(left, BinaryOp::Eq, right)
    }

    pub fn neq(left: Expr, right: Expr) -> Self {
        Self::new(left, BinaryOp::Neq, right)
    }

    pub fn gt(left: Expr, right: Expr) -> Self {
        Self::new(left, BinaryOp::Gt, right)
    }

    pub fn lt(left: Expr, right: Expr) -> Self {
        Self::new(left, BinaryOp::Lt, right)
    }

    pub fn and(left: Expr, right: Expr) -> Self {
        Self::new(left, BinaryOp::And, right)
    }

    pub fn or(left: Expr, right: Expr) -> Self {
        Self::new(left, BinaryOp::Or, right)
    }

    pub fn add(left: Expr, right: Expr) -> Self {
        Self::new(left, BinaryOp::Add, right)
    }

    pub fn mul(left: Expr, right: Expr) -> Self {
        Self::new(left, BinaryOp::Mul, right)
    }
}

impl LogicalExpr for Binary {
    fn to_field(&self, input: &dyn LogicalPlan) -> Arc<Field> {
        if self.op.is_boolean_result() {
            Arc::new(Field::new(self.to_string(), DataType::Boolean, false))
        } else {
            // use left operand's type (simplified)
            self.left.to_field(input)
        }
    }
}

impl fmt::Display for Binary {
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
    pub op: AggregateOp,
    pub expr: Box<Expr>,
}

impl AggregateExpr {
    pub fn new(op: AggregateOp, expr: Expr) -> Self {
        AggregateExpr {
            op,
            expr: Box::new(expr),
        }
    }

    pub fn sum(expr: Expr) -> Self {
        Self::new(AggregateOp::Sum, expr)
    }

    pub fn min(expr: Expr) -> Self {
        Self::new(AggregateOp::Min, expr)
    }

    pub fn max(expr: Expr) -> Self {
        Self::new(AggregateOp::Max, expr)
    }

    pub fn avg(expr: Expr) -> Self {
        Self::new(AggregateOp::Avg, expr)
    }

    pub fn count(expr: Expr) -> Self {
        Self::new(AggregateOp::Count, expr)
    }

    pub fn to_field(&self, input: &dyn LogicalPlan) -> Arc<Field> {
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

impl LogicalExpr for AggregateExpr {
    fn to_field(&self, input: &dyn LogicalPlan) -> Arc<Field> {
        AggregateExpr::to_field(self, input)
    }
}

impl Display for AggregateExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.op.symbol(), self.expr)
    }
}

pub struct Alias {
    pub expr: Box<Expr>,
    pub alias: String,
}

impl Alias {
    pub fn new(expr: Expr, alias: &str) -> Self {
        Alias {
            expr: Box::new(expr),
            alias: alias.to_string(),
        }
    }

    pub fn to_field(&self, input: &dyn LogicalPlan) -> Arc<Field> {
        let field = self.expr.to_field(input);
        Arc::new(Field::new(
            &self.alias,
            field.data_type().clone(),
            field.is_nullable(),
        ))
    }
}

impl LogicalExpr for Alias {
    fn to_field(&self, input: &dyn LogicalPlan) -> Arc<Field> {
        Alias::to_field(self, input)
    }
}

impl fmt::Display for Alias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} AS {}", self.expr, self.alias)
    }
}

pub enum Plan {
    Scan(Scan),
    Selection(Selection),
    Projection(Projection),
    Aggregate(Aggregate),
}

impl Plan {
    /// Returns the schema that this plan node produces
    pub fn schema(&self) -> Arc<Schema> {
        match self {
            Plan::Scan(scan) => scan.schema(),
            Plan::Selection(sel) => sel.schema(),
            Plan::Projection(proj) => proj.schema(),
            Plan::Aggregate(agg) => agg.schema(),
        }
    }

    /// Returns the child plan nodes
    pub fn children(&self) -> Vec<Arc<Plan>> {
        match self {
            Plan::Scan(scan) => scan.children(),
            Plan::Selection(sel) => sel.children(),
            Plan::Projection(proj) => proj.children(),
            Plan::Aggregate(agg) => agg.children(),
        }
    }

    /// Returns a formatted string for this node (without children)
    pub fn format_node(&self) -> String {
        match self {
            Plan::Scan(scan) => scan.format_node(),
            Plan::Selection(sel) => sel.format_node(),
            Plan::Projection(proj) => proj.format_node(),
            Plan::Aggregate(agg) => agg.format_node(),
        }
    }
}

impl fmt::Display for Plan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Plan::Scan(scan) => write!(f, "{}", scan),
            Plan::Selection(sel) => write!(f, "{}", sel),
            Plan::Projection(proj) => write!(f, "{}", proj),
            Plan::Aggregate(agg) => write!(f, "{}", agg),
        }
    }
}

impl LogicalPlan for Plan {
    fn schema(&self) -> Arc<Schema> {
        Plan::schema(self)
    }

    fn children(&self) -> Vec<Arc<Plan>> {
        Plan::children(self)
    }

    fn format_node(&self) -> String {
        Plan::format_node(self)
    }
}

// ================== SCAN ==============

pub struct Scan {
    pub path: String,
    pub data_source: Arc<crate::data_sources::Source>,
    pub projection: Option<Vec<String>>,
    schema: Arc<Schema>,
}

impl Scan {
    pub fn new(
        path: &str,
        data_source: Arc<crate::data_sources::Source>,
        projection: Option<Vec<String>>,
    ) -> Self {
        let schema = data_source.schema();
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

    fn children(&self) -> Vec<Arc<Plan>> {
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
    pub input: Arc<Plan>,
    pub expr: Expr,
}

impl Selection {
    pub fn new(input: Arc<Plan>, expr: Expr) -> Self {
        Selection { input, expr }
    }
}

impl LogicalPlan for Selection {
    fn schema(&self) -> Arc<Schema> {
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<Plan>> {
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
    pub input: Arc<Plan>,
    pub expr: Vec<Expr>,
}

impl Projection {
    pub fn new(input: Arc<Plan>, expr: Vec<Expr>) -> Self {
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

    fn children(&self) -> Vec<Arc<Plan>> {
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
    pub input: Arc<Plan>,
    pub group_expr: Vec<Expr>,
    pub aggregate_expr: Vec<AggregateExpr>,
}

impl Aggregate {
    pub fn new(
        input: Arc<Plan>,
        group_expr: Vec<Expr>,
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

    fn children(&self) -> Vec<Arc<Plan>> {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::data_sources::{InMemoryDataSource, Source};

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, true),
            Field::new("salary", DataType::Float64, true),
            Field::new("active", DataType::Boolean, false),
        ]))
    }

    fn create_test_data_source() -> Arc<Source> {
        let schema = create_test_schema();
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "Alice", "Bob", "Charlie",
                ])),
                Arc::new(arrow::array::Int64Array::from(vec![30, 25, 35])),
                Arc::new(arrow::array::Float64Array::from(vec![
                    50000.0, 60000.0, 70000.0,
                ])),
                Arc::new(arrow::array::BooleanArray::from(vec![true, false, true])),
            ],
        )
        .unwrap();

        Arc::new(Source::InMemory(
            InMemoryDataSource::try_new(Some(vec![batch])).unwrap(),
        ))
    }

    fn create_test_scan() -> Arc<Plan> {
        Arc::new(Plan::Scan(Scan::new(
            "test_table",
            create_test_data_source(),
            None,
        )))
    }

    mod column {
        use super::*;

        #[test]
        fn new_creates_column_with_name() {
            let col = Column::new("test_column");
            assert_eq!(col.name, "test_column");
        }

        #[test]
        fn display_formats_with_hash_prefix() {
            let col = Column::new("my_col");
            assert_eq!(format!("{}", col), "#my_col");
        }

        #[test]
        fn to_field_returns_correct_field() {
            let scan = create_test_scan();
            let col = Column::new("name");

            let field = col.to_field(scan.as_ref());

            assert_eq!(field.name(), "name");
            assert_eq!(field.data_type(), &DataType::Utf8);
        }

        #[test]
        fn to_field_works_for_different_types() {
            let scan = create_test_scan();

            let int_col = Column::new("id");
            assert_eq!(
                int_col.to_field(scan.as_ref()).data_type(),
                &DataType::Int64
            );

            let float_col = Column::new("salary");
            assert_eq!(
                float_col.to_field(scan.as_ref()).data_type(),
                &DataType::Float64
            );

            let bool_col = Column::new("active");
            assert_eq!(
                bool_col.to_field(scan.as_ref()).data_type(),
                &DataType::Boolean
            );
        }

        #[test]
        #[should_panic(expected = "No column named")]
        fn to_field_panics_on_missing_column() {
            let scan = create_test_scan();
            let col = Column::new("nonexistent");
            col.to_field(scan.as_ref());
        }
    }

    mod literal {
        use super::*;

        mod constructors {
            use super::*;

            #[test]
            fn string_creates_string_literal() {
                let lit = Literal::string("hello");
                assert!(matches!(lit.value, LiteralValue::String(s) if s == "hello"));
            }

            #[test]
            fn int_creates_int64_literal() {
                let lit = Literal::int(42);
                assert!(matches!(lit.value, LiteralValue::Int64(n) if n == 42));
            }

            #[test]
            fn float_creates_float64_literal() {
                let lit = Literal::float(3.13);
                assert!(
                    matches!(lit.value, LiteralValue::Float64(n) if (n - 3.13).abs() < f64::EPSILON)
                );
            }

            #[test]
            fn bool_creates_boolean_literal() {
                let lit_true = Literal::bool(true);
                let lit_false = Literal::bool(false);

                assert!(matches!(lit_true.value, LiteralValue::Bool(true)));
                assert!(matches!(lit_false.value, LiteralValue::Bool(false)));
            }
        }

        mod display {
            use super::*;

            #[test]
            fn string_displays_with_quotes() {
                let lit = Literal::string("test");
                assert_eq!(format!("{}", lit), "'test'");
            }

            #[test]
            fn int_displays_as_number() {
                let lit = Literal::int(123);
                assert_eq!(format!("{}", lit), "123");
            }

            #[test]
            fn negative_int_displays_correctly() {
                let lit = Literal::int(-456);
                assert_eq!(format!("{}", lit), "-456");
            }

            #[test]
            fn float_displays_as_number() {
                let lit = Literal::float(3.13);
                assert_eq!(format!("{}", lit), "3.13");
            }

            #[test]
            fn bool_displays_as_lowercase() {
                assert_eq!(format!("{}", Literal::bool(true)), "true");
                assert_eq!(format!("{}", Literal::bool(false)), "false");
            }
        }

        mod to_field {
            use super::*;

            #[test]
            fn string_returns_utf8_field() {
                let scan = create_test_scan();
                let lit = Literal::string("test");
                let field = lit.to_field(scan.as_ref());

                assert_eq!(field.data_type(), &DataType::Utf8);
            }

            #[test]
            fn int_returns_int64_field() {
                let scan = create_test_scan();
                let lit = Literal::int(42);
                let field = lit.to_field(scan.as_ref());

                assert_eq!(field.data_type(), &DataType::Int64);
            }

            #[test]
            fn float_returns_float64_field() {
                let scan = create_test_scan();
                let lit = Literal::float(3.13);
                let field = lit.to_field(scan.as_ref());

                assert_eq!(field.data_type(), &DataType::Float64);
            }

            #[test]
            fn bool_returns_boolean_field() {
                let scan = create_test_scan();
                let lit = Literal::bool(true);
                let field = lit.to_field(scan.as_ref());

                assert_eq!(field.data_type(), &DataType::Boolean);
            }

            #[test]
            fn field_name_matches_display() {
                let scan = create_test_scan();
                let lit = Literal::string("hello");
                let field = lit.to_field(scan.as_ref());

                assert_eq!(field.name(), "'hello'");
            }
        }
    }

    mod binary_op {
        use super::*;

        #[test]
        fn symbols_are_correct() {
            assert_eq!(BinaryOp::Eq.symbol(), "=");
            assert_eq!(BinaryOp::Neq.symbol(), "!=");
            assert_eq!(BinaryOp::Lt.symbol(), "<");
            assert_eq!(BinaryOp::LtEq.symbol(), "<=");
            assert_eq!(BinaryOp::Gt.symbol(), ">");
            assert_eq!(BinaryOp::GtEq.symbol(), ">=");
            assert_eq!(BinaryOp::And.symbol(), "AND");
            assert_eq!(BinaryOp::Or.symbol(), "OR");
            assert_eq!(BinaryOp::Add.symbol(), "+");
            assert_eq!(BinaryOp::Sub.symbol(), "-");
            assert_eq!(BinaryOp::Mul.symbol(), "*");
            assert_eq!(BinaryOp::Div.symbol(), "/");
            assert_eq!(BinaryOp::Mod.symbol(), "%");
        }

        #[test]
        fn comparison_ops_return_boolean() {
            assert!(BinaryOp::Eq.is_boolean_result());
            assert!(BinaryOp::Neq.is_boolean_result());
            assert!(BinaryOp::Lt.is_boolean_result());
            assert!(BinaryOp::LtEq.is_boolean_result());
            assert!(BinaryOp::Gt.is_boolean_result());
            assert!(BinaryOp::GtEq.is_boolean_result());
        }

        #[test]
        fn logical_ops_return_boolean() {
            assert!(BinaryOp::And.is_boolean_result());
            assert!(BinaryOp::Or.is_boolean_result());
        }

        #[test]
        fn math_ops_do_not_return_boolean() {
            assert!(!BinaryOp::Add.is_boolean_result());
            assert!(!BinaryOp::Sub.is_boolean_result());
            assert!(!BinaryOp::Mul.is_boolean_result());
            assert!(!BinaryOp::Div.is_boolean_result());
            assert!(!BinaryOp::Mod.is_boolean_result());
        }
    }

    mod binary_expr {
        use super::*;

        mod constructors {
            use super::*;

            #[test]
            fn eq_creates_equality_expression() {
                let expr = Binary::eq(
                    Expr::Column(Column::new("a")),
                    Expr::Literal(Literal::int(1)),
                );
                assert!(matches!(expr.op, BinaryOp::Eq));
            }

            #[test]
            fn neq_creates_not_equal_expression() {
                let expr = Binary::neq(
                    Expr::Column(Column::new("a")),
                    Expr::Literal(Literal::int(1)),
                );
                assert!(matches!(expr.op, BinaryOp::Neq));
            }

            #[test]
            fn gt_creates_greater_than_expression() {
                let expr = Binary::gt(
                    Expr::Column(Column::new("a")),
                    Expr::Literal(Literal::int(1)),
                );
                assert!(matches!(expr.op, BinaryOp::Gt));
            }

            #[test]
            fn lt_creates_less_than_expression() {
                let expr = Binary::lt(
                    Expr::Column(Column::new("a")),
                    Expr::Literal(Literal::int(1)),
                );
                assert!(matches!(expr.op, BinaryOp::Lt));
            }

            #[test]
            fn and_creates_and_expression() {
                let expr = Binary::and(
                    Expr::Literal(Literal::bool(true)),
                    Expr::Literal(Literal::bool(false)),
                );
                assert!(matches!(expr.op, BinaryOp::And));
            }

            #[test]
            fn or_creates_or_expression() {
                let expr = Binary::or(
                    Expr::Literal(Literal::bool(true)),
                    Expr::Literal(Literal::bool(false)),
                );
                assert!(matches!(expr.op, BinaryOp::Or));
            }

            #[test]
            fn add_creates_addition_expression() {
                let expr = Binary::add(
                    Expr::Column(Column::new("a")),
                    Expr::Literal(Literal::int(1)),
                );
                assert!(matches!(expr.op, BinaryOp::Add));
            }

            #[test]
            fn mul_creates_multiplication_expression() {
                let expr = Binary::mul(
                    Expr::Column(Column::new("a")),
                    Expr::Literal(Literal::int(2)),
                );
                assert!(matches!(expr.op, BinaryOp::Mul));
            }
        }

        mod display {
            use super::*;

            #[test]
            fn formats_comparison_correctly() {
                let expr = Binary::eq(
                    Expr::Column(Column::new("age")),
                    Expr::Literal(Literal::int(30)),
                );
                assert_eq!(format!("{}", expr), "#age = 30");
            }

            #[test]
            fn formats_math_correctly() {
                let expr = Binary::add(
                    Expr::Column(Column::new("salary")),
                    Expr::Literal(Literal::float(1000.0)),
                );
                assert_eq!(format!("{}", expr), "#salary + 1000");
            }

            #[test]
            fn formats_nested_expressions() {
                let inner = Binary::add(
                    Expr::Column(Column::new("a")),
                    Expr::Literal(Literal::int(1)),
                );
                let outer = Binary::mul(Expr::Binary(inner), Expr::Literal(Literal::int(2)));
                assert_eq!(format!("{}", outer), "#a + 1 * 2");
            }
        }

        mod to_field {
            use super::*;

            #[test]
            fn comparison_returns_boolean_field() {
                let scan = create_test_scan();
                let expr = Binary::eq(
                    Expr::Column(Column::new("age")),
                    Expr::Literal(Literal::int(30)),
                );
                let field = expr.to_field(scan.as_ref());

                assert_eq!(field.data_type(), &DataType::Boolean);
            }

            #[test]
            fn logical_returns_boolean_field() {
                let scan = create_test_scan();
                let expr = Binary::and(
                    Expr::Binary(Binary::eq(
                        Expr::Column(Column::new("age")),
                        Expr::Literal(Literal::int(30)),
                    )),
                    Expr::Binary(Binary::eq(
                        Expr::Column(Column::new("active")),
                        Expr::Literal(Literal::bool(true)),
                    )),
                );
                let field = expr.to_field(scan.as_ref());

                assert_eq!(field.data_type(), &DataType::Boolean);
            }

            #[test]
            fn math_preserves_left_operand_type() {
                let scan = create_test_scan();

                let int_expr = Expr::Binary(Binary::add(
                    Expr::Column(Column::new("age")),
                    Expr::Literal(Literal::int(1)),
                ));
                assert_eq!(
                    int_expr.to_field(scan.as_ref()).data_type(),
                    &DataType::Int64
                );

                let float_expr = Expr::Binary(Binary::mul(
                    Expr::Column(Column::new("salary")),
                    Expr::Literal(Literal::float(1.1)),
                ));
                assert_eq!(
                    float_expr.to_field(scan.as_ref()).data_type(),
                    &DataType::Float64
                );
            }
        }
    }

    mod aggregate_op {
        use super::*;

        #[test]
        fn symbols_are_correct() {
            assert_eq!(AggregateOp::Sum.symbol(), "SUM");
            assert_eq!(AggregateOp::Min.symbol(), "MIN");
            assert_eq!(AggregateOp::Max.symbol(), "MAX");
            assert_eq!(AggregateOp::Avg.symbol(), "AVG");
            assert_eq!(AggregateOp::Count.symbol(), "COUNT");
        }
    }

    mod aggregate_expr {
        use super::*;

        mod constructors {
            use super::*;

            #[test]
            fn sum_creates_sum_expression() {
                let expr = AggregateExpr::sum(Expr::Column(Column::new("salary")));
                assert_eq!(format!("{}", expr), "SUM(#salary)");
            }

            #[test]
            fn min_creates_min_expression() {
                let expr = AggregateExpr::min(Expr::Column(Column::new("age")));
                assert_eq!(format!("{}", expr), "MIN(#age)");
            }

            #[test]
            fn max_creates_max_expression() {
                let expr = AggregateExpr::max(Expr::Column(Column::new("age")));
                assert_eq!(format!("{}", expr), "MAX(#age)");
            }

            #[test]
            fn avg_creates_avg_expression() {
                let expr = AggregateExpr::avg(Expr::Column(Column::new("salary")));
                assert_eq!(format!("{}", expr), "AVG(#salary)");
            }

            #[test]
            fn count_creates_count_expression() {
                let expr = AggregateExpr::count(Expr::Column(Column::new("id")));
                assert_eq!(format!("{}", expr), "COUNT(#id)");
            }
        }

        mod to_field {
            use super::*;

            #[test]
            fn count_returns_int64() {
                let scan = create_test_scan();
                let expr = AggregateExpr::count(Expr::Column(Column::new("name")));
                let field = expr.to_field(scan.as_ref());

                assert_eq!(field.data_type(), &DataType::Int64);
            }

            #[test]
            fn avg_returns_float64() {
                let scan = create_test_scan();
                let expr = AggregateExpr::avg(Expr::Column(Column::new("age")));
                let field = expr.to_field(scan.as_ref());

                assert_eq!(field.data_type(), &DataType::Float64);
            }

            #[test]
            fn sum_preserves_input_type() {
                let scan = create_test_scan();

                let int_sum = AggregateExpr::sum(Expr::Column(Column::new("age")));
                assert_eq!(
                    int_sum.to_field(scan.as_ref()).data_type(),
                    &DataType::Int64
                );

                let float_sum = AggregateExpr::sum(Expr::Column(Column::new("salary")));
                assert_eq!(
                    float_sum.to_field(scan.as_ref()).data_type(),
                    &DataType::Float64
                );
            }

            #[test]
            fn min_max_preserve_input_type() {
                let scan = create_test_scan();

                let min_expr = AggregateExpr::min(Expr::Column(Column::new("age")));
                assert_eq!(
                    min_expr.to_field(scan.as_ref()).data_type(),
                    &DataType::Int64
                );

                let max_expr = AggregateExpr::max(Expr::Column(Column::new("salary")));
                assert_eq!(
                    max_expr.to_field(scan.as_ref()).data_type(),
                    &DataType::Float64
                );
            }

            #[test]
            fn field_name_includes_function_and_column() {
                let scan = create_test_scan();
                let expr = AggregateExpr::sum(Expr::Column(Column::new("salary")));
                let field = expr.to_field(scan.as_ref());

                assert_eq!(field.name(), "SUM(#salary)");
            }

            #[test]
            fn aggregate_fields_are_nullable() {
                let scan = create_test_scan();
                let expr = AggregateExpr::sum(Expr::Column(Column::new("age")));
                let field = expr.to_field(scan.as_ref());

                assert!(field.is_nullable());
            }
        }
    }

    mod alias {
        use super::*;

        #[test]
        fn creates_alias_with_expression_and_name() {
            let alias = Alias::new(Expr::Column(Column::new("first_name")), "fname");
            assert_eq!(alias.alias, "fname");
        }

        #[test]
        fn display_formats_with_as() {
            let alias = Alias::new(Expr::Column(Column::new("first_name")), "fname");
            assert_eq!(format!("{}", alias), "#first_name AS fname");
        }

        #[test]
        fn to_field_uses_alias_name() {
            let scan = create_test_scan();
            let alias = Alias::new(Expr::Column(Column::new("name")), "full_name");
            let field = alias.to_field(scan.as_ref());

            assert_eq!(field.name(), "full_name");
        }

        #[test]
        fn to_field_preserves_data_type() {
            let scan = create_test_scan();
            let alias = Alias::new(Expr::Column(Column::new("salary")), "pay");
            let field = alias.to_field(scan.as_ref());

            assert_eq!(field.data_type(), &DataType::Float64);
        }

        #[test]
        fn to_field_preserves_nullability() {
            let scan = create_test_scan();

            let nullable_alias = Expr::Alias(Alias::new(Expr::Column(Column::new("age")), "years"));
            assert!(nullable_alias.to_field(scan.as_ref()).is_nullable());

            let non_nullable_alias =
                Expr::Alias(Alias::new(Expr::Column(Column::new("id")), "identifier"));
            assert!(!non_nullable_alias.to_field(scan.as_ref()).is_nullable());
        }

        #[test]
        fn works_with_complex_expressions() {
            let scan = create_test_scan();
            let expr = Expr::Binary(Binary::add(
                Expr::Column(Column::new("salary")),
                Expr::Literal(Literal::float(1000.0)),
            ));
            let alias = Alias::new(expr, "adjusted_salary");

            let field = alias.to_field(scan.as_ref());
            assert_eq!(field.name(), "adjusted_salary");
            assert_eq!(field.data_type(), &DataType::Float64);
        }
    }

    mod scan {
        use super::*;

        #[test]
        fn new_creates_scan_with_path() {
            let scan = Scan::new("test/path.csv", create_test_data_source(), None);
            assert_eq!(scan.path, "test/path.csv");
        }

        #[test]
        fn schema_returns_data_source_schema() {
            let scan = Scan::new("test.csv", create_test_data_source(), None);
            let schema = scan.schema();

            let field_names: Vec<&str> =
                schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert_eq!(field_names, vec!["id", "name", "age", "salary", "active"]);
        }

        #[test]
        fn children_returns_empty_vec() {
            let scan = Scan::new("test.csv", create_test_data_source(), None);
            assert!(scan.children().is_empty());
        }

        #[test]
        fn format_node_without_projection() {
            let scan = Scan::new("data/users.csv", create_test_data_source(), None);
            assert_eq!(scan.format_node(), "Scan: data/users.csv; projection=None");
        }

        #[test]
        fn format_node_with_projection() {
            let projection = Some(vec!["id".to_string(), "name".to_string()]);
            let scan = Scan::new("data/users.csv", create_test_data_source(), projection);
            assert_eq!(
                scan.format_node(),
                "Scan: data/users.csv; projection=[\"id\", \"name\"]"
            );
        }

        #[test]
        fn display_formats_correctly() {
            let scan = Scan::new("test.csv", create_test_data_source(), None);
            let output = format!("{}", scan);
            assert!(output.contains("Scan: test.csv"));
        }
    }

    mod selection {
        use super::*;

        #[test]
        fn new_creates_selection_with_input_and_expr() {
            let scan = create_test_scan();
            let expr = Expr::Binary(Binary::eq(
                Expr::Column(Column::new("age")),
                Expr::Literal(Literal::int(30)),
            ));
            let selection = Plan::Selection(Selection::new(scan, expr));

            assert!(!selection.children().is_empty());
        }

        #[test]
        fn schema_passes_through_input_schema() {
            let scan = create_test_scan();
            let expr = Expr::Binary(Binary::eq(
                Expr::Column(Column::new("age")),
                Expr::Literal(Literal::int(30)),
            ));
            let selection = Plan::Selection(Selection::new(scan.clone(), expr));

            assert_eq!(selection.schema(), scan.schema());
        }

        #[test]
        fn children_returns_input() {
            let scan = create_test_scan();
            let expr = Expr::Binary(Binary::eq(
                Expr::Column(Column::new("age")),
                Expr::Literal(Literal::int(30)),
            ));
            let selection = Selection::new(scan, expr);

            assert_eq!(selection.children().len(), 1);
        }

        #[test]
        fn format_node_shows_filter_expression() {
            let scan = create_test_scan();
            let expr = Expr::Binary(Binary::eq(
                Expr::Column(Column::new("age")),
                Expr::Literal(Literal::int(30)),
            ));
            let selection = Selection::new(scan, expr);

            assert_eq!(selection.format_node(), "Filter: #age = 30");
        }

        #[test]
        fn display_shows_hierarchical_plan() {
            let scan = create_test_scan();
            let expr = Expr::Binary(Binary::eq(
                Expr::Column(Column::new("age")),
                Expr::Literal(Literal::int(30)),
            ));
            let selection = Selection::new(scan, expr);

            let output = format!("{}", selection);
            assert!(output.contains("Filter: #age = 30"));
            assert!(output.contains("Scan:"));
        }
    }

    mod projection {
        use super::*;

        #[test]
        fn new_creates_projection() {
            let scan = create_test_scan();
            let exprs: Vec<Expr> = vec![
                Expr::Column(Column::new("id")),
                Expr::Column(Column::new("name")),
            ];
            let projection = Projection::new(scan, exprs);

            assert_eq!(projection.expr.len(), 2);
        }

        #[test]
        fn schema_contains_projected_columns() {
            let scan = create_test_scan();
            let exprs: Vec<Expr> = vec![
                Expr::Column(Column::new("id")),
                Expr::Column(Column::new("name")),
            ];
            let projection = Projection::new(scan, exprs);

            let schema = projection.schema();
            assert_eq!(schema.fields().len(), 2);

            let field_names: Vec<&str> =
                schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert_eq!(field_names, vec!["id", "name"]);
        }

        #[test]
        fn schema_handles_aliases() {
            let scan = create_test_scan();
            let exprs: Vec<Expr> = vec![Expr::Alias(Alias::new(
                Expr::Column(Column::new("name")),
                "full_name",
            ))];
            let projection = Projection::new(scan, exprs);

            let schema = projection.schema();
            assert_eq!(schema.fields()[0].name(), "full_name");
        }

        #[test]
        fn schema_handles_expressions() {
            let scan = create_test_scan();
            let exprs: Vec<Expr> = vec![Expr::Binary(Binary::add(
                Expr::Column(Column::new("salary")),
                Expr::Literal(Literal::float(1000.0)),
            ))];
            let projection = Projection::new(scan, exprs);

            let schema = projection.schema();
            assert_eq!(schema.fields()[0].data_type(), &DataType::Float64);
        }

        #[test]
        fn children_returns_input() {
            let scan = create_test_scan();
            let exprs: Vec<Expr> = vec![Expr::Column(Column::new("id"))];
            let projection = Projection::new(scan, exprs);

            assert_eq!(projection.children().len(), 1);
        }

        #[test]
        fn format_node_shows_projected_columns() {
            let scan = create_test_scan();
            let exprs: Vec<Expr> = vec![
                Expr::Column(Column::new("id")),
                Expr::Column(Column::new("name")),
            ];
            let projection = Projection::new(scan, exprs);

            assert_eq!(projection.format_node(), "Projection: #id, #name");
        }

        #[test]
        fn display_shows_hierarchical_plan() {
            let scan = create_test_scan();
            let exprs: Vec<Expr> = vec![Expr::Column(Column::new("id"))];
            let projection = Projection::new(scan, exprs);

            let output = format!("{}", projection);
            assert!(output.contains("Projection:"));
            assert!(output.contains("Scan:"));
        }
    }

    mod aggregate {
        use super::*;

        #[test]
        fn new_creates_aggregate() {
            let scan = create_test_scan();
            let group_by: Vec<Expr> = vec![Expr::Column(Column::new("active"))];
            let agg_exprs = vec![AggregateExpr::sum(Expr::Column(Column::new("salary")))];

            let aggregate = Aggregate::new(scan, group_by, agg_exprs);

            assert_eq!(aggregate.group_expr.len(), 1);
            assert_eq!(aggregate.aggregate_expr.len(), 1);
        }

        #[test]
        fn schema_includes_group_and_aggregate_columns() {
            let scan = create_test_scan();
            let group_by: Vec<Expr> = vec![Expr::Column(Column::new("active"))];
            let agg_exprs = vec![
                AggregateExpr::sum(Expr::Column(Column::new("salary"))),
                AggregateExpr::count(Expr::Column(Column::new("id"))),
            ];

            let aggregate = Aggregate::new(scan, group_by, agg_exprs);
            let schema = aggregate.schema();

            assert_eq!(schema.fields().len(), 3);

            let field_names: Vec<&str> =
                schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert_eq!(field_names, vec!["active", "SUM(#salary)", "COUNT(#id)"]);
        }

        #[test]
        fn schema_group_columns_come_first() {
            let scan = create_test_scan();
            let group_by: Vec<Expr> = vec![
                Expr::Column(Column::new("active")),
                Expr::Column(Column::new("name")),
            ];
            let agg_exprs = vec![AggregateExpr::avg(Expr::Column(Column::new("age")))];

            let aggregate = Aggregate::new(scan, group_by, agg_exprs);
            let schema = aggregate.schema();

            let field_names: Vec<&str> =
                schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert_eq!(field_names, vec!["active", "name", "AVG(#age)"]);
        }

        #[test]
        fn children_returns_input() {
            let scan = create_test_scan();
            let group_by: Vec<Expr> = vec![Expr::Column(Column::new("active"))];
            let agg_exprs = vec![AggregateExpr::count(Expr::Column(Column::new("id")))];

            let aggregate = Aggregate::new(scan, group_by, agg_exprs);

            assert_eq!(aggregate.children().len(), 1);
        }

        #[test]
        fn format_node_shows_group_and_aggregate() {
            let scan = create_test_scan();
            let group_by: Vec<Expr> = vec![Expr::Column(Column::new("active"))];
            let agg_exprs = vec![
                AggregateExpr::sum(Expr::Column(Column::new("salary"))),
                AggregateExpr::count(Expr::Column(Column::new("id"))),
            ];

            let aggregate = Aggregate::new(scan, group_by, agg_exprs);

            assert_eq!(
                aggregate.format_node(),
                "Aggregate: groupBy=[#active], aggr=[SUM(#salary), COUNT(#id)]"
            );
        }

        #[test]
        fn format_node_handles_empty_group_by() {
            let scan = create_test_scan();
            let group_by: Vec<Expr> = vec![];
            let agg_exprs = vec![AggregateExpr::count(Expr::Column(Column::new("id")))];

            let aggregate = Aggregate::new(scan, group_by, agg_exprs);

            assert_eq!(
                aggregate.format_node(),
                "Aggregate: groupBy=[], aggr=[COUNT(#id)]"
            );
        }

        #[test]
        fn display_shows_hierarchical_plan() {
            let scan = create_test_scan();
            let group_by: Vec<Expr> = vec![Expr::Column(Column::new("active"))];
            let agg_exprs = vec![AggregateExpr::sum(Expr::Column(Column::new("salary")))];

            let aggregate = Aggregate::new(scan, group_by, agg_exprs);

            let output = format!("{}", aggregate);
            assert!(output.contains("Aggregate:"));
            assert!(output.contains("Scan:"));
        }
    }

    mod integration {
        use super::*;

        #[test]
        fn nested_plan_displays_correctly() {
            // SELECT name, salary
            // FROM test_table
            // WHERE age > 25 AND active = true

            let scan = create_test_scan();
            let filter_expr = Expr::Binary(Binary::and(
                Expr::Binary(Binary::gt(
                    Expr::Column(Column::new("age")),
                    Expr::Literal(Literal::int(25)),
                )),
                Expr::Binary(Binary::eq(
                    Expr::Column(Column::new("active")),
                    Expr::Literal(Literal::bool(true)),
                )),
            ));
            let selection = Selection::new(scan, filter_expr);

            let projection = Projection::new(
                Arc::new(Plan::Selection(selection)),
                vec![
                    Expr::Column(Column::new("name")),
                    Expr::Column(Column::new("salary")),
                ],
            );

            let output = format!("{}", projection);

            assert!(output.contains("Projection: #name, #salary"));
            assert!(output.contains("Filter: #age > 25 AND #active = true"));
            assert!(output.contains("Scan: test_table"));
        }

        #[test]
        fn aggregate_with_filter_displays_correctly() {
            // SELECT active, SUM(salary), COUNT(id)
            // FROM test_table
            // WHERE age > 25
            // GROUP BY active

            let scan = create_test_scan();
            let selection = Selection::new(
                scan,
                Expr::Binary(Binary::gt(
                    Expr::Column(Column::new("age")),
                    Expr::Literal(Literal::int(25)),
                )),
            );

            let aggregate = Aggregate::new(
                Arc::new(Plan::Selection(selection)),
                vec![Expr::Column(Column::new("active"))],
                vec![
                    AggregateExpr::sum(Expr::Column(Column::new("salary"))),
                    AggregateExpr::count(Expr::Column(Column::new("id"))),
                ],
            );

            let output = format!("{}", aggregate);

            assert!(
                output.contains("Aggregate: groupBy=[#active], aggr=[SUM(#salary), COUNT(#id)]")
            );
            assert!(output.contains("Filter: #age > 25"));
            assert!(output.contains("Scan: test_table"));
        }

        #[test]
        fn full_query_plan_schema_is_correct() {
            // SELECT name AS employee_name, salary + 1000 AS adjusted_salary
            // FROM test_table
            // WHERE active = true

            let scan = create_test_scan();
            let selection = Selection::new(
                scan,
                Expr::Binary(Binary::eq(
                    Expr::Column(Column::new("active")),
                    Expr::Literal(Literal::bool(true)),
                )),
            );

            let projection = Projection::new(
                Arc::new(Plan::Selection(selection)),
                vec![
                    Expr::Alias(Alias::new(
                        Expr::Column(Column::new("name")),
                        "employee_name",
                    )),
                    Expr::Alias(Alias::new(
                        Expr::Binary(Binary::add(
                            Expr::Column(Column::new("salary")),
                            Expr::Literal(Literal::float(1000.0)),
                        )),
                        "adjusted_salary",
                    )),
                ],
            );

            let schema = projection.schema();
            let field_names: Vec<&str> =
                schema.fields().iter().map(|f| f.name().as_str()).collect();

            assert_eq!(field_names, vec!["employee_name", "adjusted_salary"]);
            assert_eq!(schema.fields()[0].data_type(), &DataType::Utf8);
            assert_eq!(schema.fields()[1].data_type(), &DataType::Float64);
        }

        #[test]
        fn deeply_nested_plan_maintains_correct_indentation() {
            let scan = create_test_scan();
            let filter1 = Selection::new(
                scan,
                Expr::Binary(Binary::gt(
                    Expr::Column(Column::new("age")),
                    Expr::Literal(Literal::int(20)),
                )),
            );
            let filter2 = Selection::new(
                Arc::new(Plan::Selection(filter1)),
                Expr::Binary(Binary::lt(
                    Expr::Column(Column::new("age")),
                    Expr::Literal(Literal::int(40)),
                )),
            );
            let projection = Projection::new(
                Arc::new(Plan::Selection(filter2)),
                vec![Expr::Column(Column::new("name"))],
            );

            let output = format!("{}", projection);
            let lines: Vec<&str> = output.lines().collect();

            // Check indentation increases for each nested level
            assert!(lines[0].starts_with("Projection:"));
            assert!(lines[1].starts_with("\tFilter:"));
            assert!(lines[2].starts_with("\t\tFilter:"));
            assert!(lines[3].starts_with("\t\t\tScan:"));
        }
    }
}
