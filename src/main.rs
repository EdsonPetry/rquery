use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sql = "SELECT a FROM table_1";

    // parse to a Vec<Statement>
    let ast = Parser::parse_sql(&GenericDialect, sql).unwrap();

    println!("AST: {:?}", ast);

    Ok(())
}
