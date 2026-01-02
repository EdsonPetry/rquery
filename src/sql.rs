use std::collections::HashMap;
use std::sync::Arc;

use sqlparser::{
    ast,
    ast::{
        BinaryOperator, Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
        ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
        UnaryOperator, Value,
    },
    dialect::GenericDialect,
    parser::Parser,
};
use thiserror::Error;

use crate::{
    data_sources::{CsvDataSource, Source},
    logical_plan::{
        Aggregate, AggregateExpr, AggregateOp, Alias, Binary, BinaryOp, Column, Expr, Literal,
        LiteralValue, LogicalPlan, Plan, Projection, Scan, Selection,
    },
};

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
    tables: HashMap<String, Arc<Source>>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            tables: HashMap::new(),
        }
    }

    pub fn register_csv(&mut self, name: &str, path: &str, delimiter: u8, header: bool) {
        let path = path.to_string();
        let name = name.to_string();
        self.tables.insert(
            name,
            Arc::new(Source::Csv(
                CsvDataSource::try_new(path, delimiter, header).unwrap(),
            )),
        );
    }

    pub fn register_table(&mut self, name: &str, source: Source) {
        self.tables.insert(name.to_string(), Arc::new(source));
    }

    pub fn get_table(&self, name: &str) -> Option<Arc<Source>> {
        self.tables.get(name).cloned()
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

// ================== SQL PLANNER =====================

pub struct SqlPlanner {
    catalog: Catalog,
}

impl SqlPlanner {
    pub fn new(catalog: Catalog) -> Self {
        SqlPlanner { catalog }
    }

    pub fn plan(&self, sql: &str) -> Result<Plan, PlanError> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql)?;

        if statements.is_empty() {
            return Err(PlanError::ParseError("Empty SQL".to_string()));
        }
        if statements.len() > 1 {
            return Err(PlanError::Unsupported(
                "Multiple statements not supported".to_string(),
            ));
        }

        self.plan_statement(&statements[0])
    }

    // NOTE: Currently only SELECT (Query) statements are supported.
    fn plan_statement(&self, stmt: &Statement) -> Result<Plan, PlanError> {
        match stmt {
            Statement::Query(query) => self.plan_query(query),

            _ => Err(PlanError::Unsupported(format!(
                "Statement type not supported: {:?}",
                stmt
            ))),
        }
    }

    fn plan_query(&self, query: &Query) -> Result<Plan, PlanError> {
        match query.body.as_ref() {
            SetExpr::Select(select) => self.plan_select(select),

            _ => Err(PlanError::Unsupported(format!(
                "Query type not supported: {:?}",
                query.body
            ))),
        }
    }

    fn plan_select(&self, select: &Select) -> Result<Plan, PlanError> {
        // Start with the FROM clause - this gives us our base plan
        let mut plan = self.plan_from(&select.from)?;

        // Apply WHERE clause if present
        if let Some(where_expr) = &select.selection {
            let filter_expr = self.plan_expr(where_expr, &plan)?;
            plan = Plan::Selection(Selection::new(Arc::new(plan), filter_expr));
        }

        // Check for GROUP BY or aggregates
        let has_group_by =
            !matches!(&select.group_by, GroupByExpr::Expressions(exprs, _) if exprs.is_empty());
        let has_aggregates = self.select_has_aggregates(&select.projection);

        if has_group_by || has_aggregates {
            plan = self.plan_aggregate(select, plan)?;
        }

        // Apply HAVING clause if present
        if let Some(having_expr) = &select.having {
            let having_filter = self.plan_expr(having_expr, &plan)?;
            plan = Plan::Selection(Selection::new(Arc::new(plan), having_filter));
        }

        // Apply projection (SELECT columns)
        let proj_exprs = self.plan_select_items(&select.projection, &plan)?;
        plan = Plan::Projection(Projection::new(Arc::new(plan), proj_exprs));

        Ok(plan)
    }

    fn plan_from(&self, from: &[TableWithJoins]) -> Result<Plan, PlanError> {
        if from.is_empty() {
            return Err(PlanError::InvalidExpression(
                "SELECT requires FROM clause".to_string(),
            ));
        }

        if from.len() > 1 {
            return Err(PlanError::Unsupported(
                "Multiple tables in FROM (implicit join) not yet supported".to_string(),
            ));
        }

        let table_with_joins = &from[0];

        if !table_with_joins.joins.is_empty() {
            return Err(PlanError::Unsupported("JOIN not yet supported".to_string()));
        }

        self.plan_table_factor(&table_with_joins.relation)
    }

    fn plan_table_factor(&self, table: &TableFactor) -> Result<Plan, PlanError> {
        match table {
            TableFactor::Table { name, alias: _, .. } => {
                let table_name = object_name_to_string(name);

                let data_source = self.catalog.get_table(&table_name).ok_or_else(|| {
                    PlanError::TableNotFound(format!(
                        "Table '{}' not found. Available tables: {:?}",
                        table_name,
                        self.catalog.tables.keys().collect::<Vec<_>>()
                    ))
                })?;

                // Create a Scan wrapped in Plan enum
                Ok(Plan::Scan(Scan::new(&table_name, data_source, None)))
            }

            TableFactor::Derived {
                subquery, alias: _, ..
            } => {
                let subquery_plan = self.plan_query(subquery)?;
                Ok(subquery_plan)
            }

            _ => Err(PlanError::Unsupported(format!(
                "Table factor not supported: {:?}",
                table
            ))),
        }
    }

    /// Convert a SQL AST expression to our logical Expr enum
    fn plan_expr(&self, expr: &ast::Expr, input: &Plan) -> Result<Expr, PlanError> {
        match expr {
            ast::Expr::Identifier(ident) => Ok(Expr::Column(Column::new(&ident.value))),

            ast::Expr::CompoundIdentifier(idents) => {
                let column_name = idents
                    .last()
                    .map(|i| i.value.clone())
                    .ok_or_else(|| PlanError::InvalidExpression("Empty identifier".to_string()))?;
                Ok(Expr::Column(Column::new(&column_name)))
            }

            ast::Expr::Value(value) => self.plan_literal(&value.value),

            ast::Expr::BinaryOp { left, op, right } => {
                let left_expr = self.plan_expr(left, input)?;
                let right_expr = self.plan_expr(right, input)?;
                let binary_op = self.plan_binary_operator(op)?;
                Ok(Expr::Binary(Binary::new(left_expr, binary_op, right_expr)))
            }

            ast::Expr::Nested(inner) => self.plan_expr(inner, input),

            ast::Expr::Function(func) => self.plan_function(func, input),

            ast::Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => Err(PlanError::Unsupported(
                    "NOT operator not yet supported".to_string(),
                )),
                UnaryOperator::Minus => {
                    let inner = self.plan_expr(expr, input)?;
                    // Represent -x as 0 - x
                    Ok(Expr::Binary(Binary::new(
                        Expr::Literal(Literal::int(0)),
                        BinaryOp::Sub,
                        inner,
                    )))
                }
                _ => Err(PlanError::Unsupported(format!("Unary op: {:?}", op))),
            },

            ast::Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                if *negated {
                    return Err(PlanError::Unsupported("NOT BETWEEN".to_string()));
                }
                let col = self.plan_expr(expr, input)?;
                let low_expr = self.plan_expr(low, input)?;
                let high_expr = self.plan_expr(high, input)?;

                // Clone the column expression for reuse
                let col2 = self.plan_expr(expr, input)?;

                // expr >= low AND expr <= high
                let gte = Expr::Binary(Binary::new(col, BinaryOp::GtEq, low_expr));
                let lte = Expr::Binary(Binary::new(col2, BinaryOp::LtEq, high_expr));

                Ok(Expr::Binary(Binary::new(gte, BinaryOp::And, lte)))
            }

            ast::Expr::InList {
                expr,
                list,
                negated,
            } => {
                if *negated {
                    return Err(PlanError::Unsupported("NOT IN".to_string()));
                }
                if list.is_empty() {
                    return Err(PlanError::InvalidExpression("Empty IN list".to_string()));
                }

                // Build: expr = list[0] OR expr = list[1] OR ...
                let mut result: Option<Expr> = None;

                for item in list {
                    let col = self.plan_expr(expr, input)?;
                    let val = self.plan_expr(item, input)?;
                    let eq = Expr::Binary(Binary::new(col, BinaryOp::Eq, val));

                    result = Some(match result {
                        None => eq,
                        Some(prev) => Expr::Binary(Binary::new(prev, BinaryOp::Or, eq)),
                    });
                }

                Ok(result.unwrap())
            }

            ast::Expr::IsNull(_inner) => Err(PlanError::Unsupported(
                "IS NULL not yet supported".to_string(),
            )),

            ast::Expr::IsNotNull(_inner) => Err(PlanError::Unsupported(
                "IS NOT NULL not yet supported".to_string(),
            )),

            _ => Err(PlanError::Unsupported(format!(
                "Expression not supported: {:?}",
                expr
            ))),
        }
    }

    fn plan_literal(&self, value: &Value) -> Result<Expr, PlanError> {
        match value {
            Value::Number(n, _long) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Expr::Literal(Literal::int(i)))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Expr::Literal(Literal::float(f)))
                } else {
                    Err(PlanError::InvalidExpression(format!(
                        "Invalid number: {}",
                        n
                    )))
                }
            }

            Value::SingleQuotedString(s) => Ok(Expr::Literal(Literal::string(s))),

            Value::DoubleQuotedString(s) => Ok(Expr::Literal(Literal::string(s))),

            Value::Boolean(b) => Ok(Expr::Literal(Literal::bool(*b))),

            Value::Null => Ok(Expr::Literal(Literal {
                value: LiteralValue::String("NULL".to_string()), // TODO: Add proper null support
            })),

            _ => Err(PlanError::Unsupported(format!(
                "Literal type not supported: {:?}",
                value
            ))),
        }
    }

    fn plan_binary_operator(&self, op: &BinaryOperator) -> Result<BinaryOp, PlanError> {
        match op {
            BinaryOperator::Eq => Ok(BinaryOp::Eq),
            BinaryOperator::NotEq => Ok(BinaryOp::Neq),
            BinaryOperator::Lt => Ok(BinaryOp::Lt),
            BinaryOperator::LtEq => Ok(BinaryOp::LtEq),
            BinaryOperator::Gt => Ok(BinaryOp::Gt),
            BinaryOperator::GtEq => Ok(BinaryOp::GtEq),

            BinaryOperator::And => Ok(BinaryOp::And),
            BinaryOperator::Or => Ok(BinaryOp::Or),

            BinaryOperator::Plus => Ok(BinaryOp::Add),
            BinaryOperator::Minus => Ok(BinaryOp::Sub),
            BinaryOperator::Multiply => Ok(BinaryOp::Mul),
            BinaryOperator::Divide => Ok(BinaryOp::Div),
            BinaryOperator::Modulo => Ok(BinaryOp::Mod),

            _ => Err(PlanError::Unsupported(format!(
                "Binary operator not supported: {:?}",
                op
            ))),
        }
    }

    fn plan_function(&self, func: &Function, input: &Plan) -> Result<Expr, PlanError> {
        let func_name = object_name_to_string(&func.name).to_uppercase();

        let args = match &func.args {
            FunctionArguments::List(arg_list) => &arg_list.args,
            FunctionArguments::None => {
                return Err(PlanError::InvalidExpression(format!(
                    "Function {} requires arguments",
                    func_name
                )));
            }
            _ => {
                return Err(PlanError::Unsupported(format!(
                    "Function argument style not supported: {:?}",
                    func.args
                )));
            }
        };

        match func_name.as_str() {
            "SUM" | "COUNT" | "AVG" | "MIN" | "MAX" => {
                self.plan_aggregate_function(&func_name, args, input)
            }

            _ => Err(PlanError::Unsupported(format!(
                "Function not supported: {}",
                func_name
            ))),
        }
    }

    fn plan_aggregate_function(
        &self,
        name: &str,
        args: &[FunctionArg],
        input: &Plan,
    ) -> Result<Expr, PlanError> {
        let op = match name {
            "SUM" => AggregateOp::Sum,
            "COUNT" => AggregateOp::Count,
            "AVG" => AggregateOp::Avg,
            "MIN" => AggregateOp::Min,
            "MAX" => AggregateOp::Max,
            _ => {
                return Err(PlanError::Unsupported(format!(
                    "Unknown aggregate: {}",
                    name
                )));
            }
        };

        if args.is_empty() {
            return Err(PlanError::InvalidExpression(format!(
                "{} requires an argument",
                name
            )));
        }

        let arg_expr = match &args[0] {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => self.plan_expr(expr, input)?,
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                // COUNT(*) uses literal 1
                Expr::Literal(Literal::int(1))
            }
            _ => {
                return Err(PlanError::Unsupported(format!(
                    "Function argument not supported: {:?}",
                    args[0]
                )));
            }
        };

        Ok(Expr::Aggregate(AggregateExpr::new(op, arg_expr)))
    }

    /// Check if any of the select items contain aggregate functions
    fn select_has_aggregates(&self, items: &[SelectItem]) -> bool {
        items.iter().any(|item| match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                self.ast_expr_has_aggregate(expr)
            }
            _ => false,
        })
    }

    fn ast_expr_has_aggregate(&self, expr: &ast::Expr) -> bool {
        match expr {
            ast::Expr::Function(func) => {
                let name = object_name_to_string(&func.name).to_uppercase();
                matches!(name.as_str(), "SUM" | "COUNT" | "AVG" | "MIN" | "MAX")
            }
            ast::Expr::BinaryOp { left, right, .. } => {
                self.ast_expr_has_aggregate(left) || self.ast_expr_has_aggregate(right)
            }
            ast::Expr::Nested(inner) => self.ast_expr_has_aggregate(inner),
            _ => false,
        }
    }

    fn plan_aggregate(&self, select: &Select, input: Plan) -> Result<Plan, PlanError> {
        let group_exprs = match &select.group_by {
            GroupByExpr::Expressions(exprs, _) => {
                let mut result = Vec::new();
                for expr in exprs {
                    result.push(self.plan_expr(expr, &input)?);
                }
                result
            }
            GroupByExpr::All(_) => {
                return Err(PlanError::Unsupported("GROUP BY ALL".to_string()));
            }
        };

        let agg_exprs = self.collect_aggregates(&select.projection, &input)?;

        Ok(Plan::Aggregate(Aggregate::new(
            Arc::new(input),
            group_exprs,
            agg_exprs,
        )))
    }

    fn collect_aggregates(
        &self,
        items: &[SelectItem],
        input: &Plan,
    ) -> Result<Vec<AggregateExpr>, PlanError> {
        let mut aggregates = Vec::new();

        for item in items {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    self.extract_aggregates_from_ast_expr(expr, input, &mut aggregates)?;
                }
                _ => {}
            }
        }

        Ok(aggregates)
    }

    fn extract_aggregates_from_ast_expr(
        &self,
        expr: &ast::Expr,
        input: &Plan,
        aggregates: &mut Vec<AggregateExpr>,
    ) -> Result<(), PlanError> {
        match expr {
            ast::Expr::Function(func) => {
                let name = object_name_to_string(&func.name).to_uppercase();
                if matches!(name.as_str(), "SUM" | "COUNT" | "AVG" | "MIN" | "MAX") {
                    let args = match &func.args {
                        FunctionArguments::List(list) => &list.args,
                        _ => {
                            return Err(PlanError::InvalidExpression(
                                "Invalid aggregate args".to_string(),
                            ));
                        }
                    };

                    // Build the aggregate expression
                    let op = match name.as_str() {
                        "SUM" => AggregateOp::Sum,
                        "COUNT" => AggregateOp::Count,
                        "AVG" => AggregateOp::Avg,
                        "MIN" => AggregateOp::Min,
                        "MAX" => AggregateOp::Max,
                        _ => unreachable!(),
                    };

                    let arg_expr = if args.is_empty() {
                        Expr::Literal(Literal::int(1))
                    } else {
                        match &args[0] {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                                self.plan_expr(e, input)?
                            }
                            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                                Expr::Literal(Literal::int(1))
                            }
                            _ => {
                                return Err(PlanError::Unsupported(format!(
                                    "Aggregate arg not supported: {:?}",
                                    args[0]
                                )));
                            }
                        }
                    };

                    aggregates.push(AggregateExpr::new(op, arg_expr));
                }
            }
            ast::Expr::BinaryOp { left, right, .. } => {
                self.extract_aggregates_from_ast_expr(left, input, aggregates)?;
                self.extract_aggregates_from_ast_expr(right, input, aggregates)?;
            }
            ast::Expr::Nested(inner) => {
                self.extract_aggregates_from_ast_expr(inner, input, aggregates)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn plan_select_items(
        &self,
        items: &[SelectItem],
        input: &Plan,
    ) -> Result<Vec<Expr>, PlanError> {
        let mut exprs = Vec::new();

        for item in items {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    exprs.push(self.plan_expr(expr, input)?);
                }

                SelectItem::ExprWithAlias { expr, alias } => {
                    let inner = self.plan_expr(expr, input)?;
                    exprs.push(Expr::Alias(Alias::new(inner, &alias.value)));
                }

                SelectItem::Wildcard(_) => {
                    for field in input.schema().fields() {
                        exprs.push(Expr::Column(Column::new(field.name())));
                    }
                }

                SelectItem::QualifiedWildcard(_, _) => {
                    return Err(PlanError::Unsupported(
                        "Qualified wildcard (table.*) not yet supported".to_string(),
                    ));
                }
            }
        }

        Ok(exprs)
    }
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|onp| onp.clone().to_string())
        .collect::<Vec<_>>()
        .join(".")
}
