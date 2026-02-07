use sqlparser::ast::{self, Expr, FromTable, ObjectNamePart, SetExpr, Statement, TableFactor, TableObject, Value, ValueWithSpan};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use ulid::Ulid;

use crate::model::*;

/// Parsed command from SQL input.
#[derive(Debug, PartialEq)]
pub enum Command {
    InsertResource {
        id: Ulid,
        parent_id: Option<Ulid>,
        capacity: u32,
        buffer_after: Option<Ms>,
    },
    DeleteResource {
        id: Ulid,
    },
    InsertRule {
        id: Ulid,
        resource_id: Ulid,
        start: Ms,
        end: Ms,
        blocking: bool,
    },
    DeleteRule {
        id: Ulid,
    },
    InsertHold {
        id: Ulid,
        resource_id: Ulid,
        start: Ms,
        end: Ms,
        expires_at: Ms,
    },
    DeleteHold {
        id: Ulid,
    },
    InsertBooking {
        id: Ulid,
        resource_id: Ulid,
        start: Ms,
        end: Ms,
    },
    BatchInsertBookings {
        bookings: Vec<(Ulid, Ulid, Ms, Ms)>, // (id, resource_id, start, end)
    },
    DeleteBooking {
        id: Ulid,
    },
    SelectAvailability {
        resource_id: Ulid,
        start: Ms,
        end: Ms,
        min_duration: Option<Ms>,
    },
    Listen {
        channel: String,
    },
}

pub fn parse_sql(sql: &str) -> Result<Command, SqlError> {
    let trimmed = sql.trim();
    if trimmed.to_uppercase().starts_with("LISTEN ") {
        let channel = trimmed[7..].trim().trim_matches(';').to_string();
        return Ok(Command::Listen { channel });
    }

    let dialect = PostgreSqlDialect {};
    let stmts = Parser::parse_sql(&dialect, sql).map_err(|e| SqlError::Parse(e.to_string()))?;
    if stmts.is_empty() {
        return Err(SqlError::Empty);
    }

    match &stmts[0] {
        Statement::Insert(insert) => parse_insert(insert),
        Statement::Delete(delete) => parse_delete(delete),
        Statement::Query(query) => parse_select(query),
        other => Err(SqlError::Unsupported(format!("{other}"))),
    }
}

fn parse_insert(insert: &ast::Insert) -> Result<Command, SqlError> {
    let table = insert_table_name(insert)?;
    let values = extract_insert_values(insert)?;

    match table.as_str() {
        "resources" => {
            if values.is_empty() {
                return Err(SqlError::WrongArity("resources", 1, 0));
            }
            let id = parse_ulid(&values[0])?;
            let parent_id = if values.len() >= 2 {
                parse_ulid_or_null(&values[1])?
            } else {
                None
            };
            let capacity = if values.len() >= 3 {
                parse_u32(&values[2])?
            } else {
                1
            };
            let buffer_after = if values.len() >= 4 {
                parse_i64_or_null(&values[3])?
            } else {
                None
            };
            Ok(Command::InsertResource { id, parent_id, capacity, buffer_after })
        }
        "rules" => {
            if values.len() < 5 {
                return Err(SqlError::WrongArity("rules", 5, values.len()));
            }
            Ok(Command::InsertRule {
                id: parse_ulid(&values[0])?,
                resource_id: parse_ulid(&values[1])?,
                start: parse_i64(&values[2])?,
                end: parse_i64(&values[3])?,
                blocking: parse_bool(&values[4])?,
            })
        }
        "holds" => {
            if values.len() < 5 {
                return Err(SqlError::WrongArity("holds", 5, values.len()));
            }
            Ok(Command::InsertHold {
                id: parse_ulid(&values[0])?,
                resource_id: parse_ulid(&values[1])?,
                start: parse_i64(&values[2])?,
                end: parse_i64(&values[3])?,
                expires_at: parse_i64(&values[4])?,
            })
        }
        "bookings" => {
            let all_rows = extract_all_insert_rows(insert)?;
            if all_rows.len() == 1 {
                let values = &all_rows[0];
                if values.len() < 4 {
                    return Err(SqlError::WrongArity("bookings", 4, values.len()));
                }
                Ok(Command::InsertBooking {
                    id: parse_ulid(&values[0])?,
                    resource_id: parse_ulid(&values[1])?,
                    start: parse_i64(&values[2])?,
                    end: parse_i64(&values[3])?,
                })
            } else {
                let mut bookings = Vec::with_capacity(all_rows.len());
                for (i, row) in all_rows.iter().enumerate() {
                    if row.len() < 4 {
                        return Err(SqlError::WrongArity("bookings row", 4, row.len()));
                    }
                    bookings.push((
                        parse_ulid(&row[0]).map_err(|e| SqlError::Parse(format!("row {i}: {e}")))?,
                        parse_ulid(&row[1]).map_err(|e| SqlError::Parse(format!("row {i}: {e}")))?,
                        parse_i64(&row[2]).map_err(|e| SqlError::Parse(format!("row {i}: {e}")))?,
                        parse_i64(&row[3]).map_err(|e| SqlError::Parse(format!("row {i}: {e}")))?,
                    ));
                }
                Ok(Command::BatchInsertBookings { bookings })
            }
        }
        _ => Err(SqlError::UnknownTable(table)),
    }
}

fn parse_delete(delete: &ast::Delete) -> Result<Command, SqlError> {
    let table = delete_table_name(delete)?;
    let id = extract_where_id(&delete.selection)?;

    match table.as_str() {
        "resources" => Ok(Command::DeleteResource { id }),
        "rules" => Ok(Command::DeleteRule { id }),
        "holds" => Ok(Command::DeleteHold { id }),
        "bookings" => Ok(Command::DeleteBooking { id }),
        _ => Err(SqlError::UnknownTable(table)),
    }
}

fn parse_select(query: &ast::Query) -> Result<Command, SqlError> {
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return Err(SqlError::Unsupported("non-SELECT query".into())),
    };

    if select.from.is_empty() {
        return Err(SqlError::Parse("SELECT without FROM".into()));
    }
    let table = table_factor_name(&select.from[0].relation)?;

    if table != "availability" {
        return Err(SqlError::UnknownTable(table));
    }

    let (mut resource_id, mut start, mut end, mut min_duration) = (None, None, None, None);
    if let Some(selection) = &select.selection {
        extract_availability_filters(selection, &mut resource_id, &mut start, &mut end, &mut min_duration)?;
    }

    Ok(Command::SelectAvailability {
        resource_id: resource_id.ok_or(SqlError::MissingFilter("resource_id"))?,
        start: start.ok_or(SqlError::MissingFilter("start"))?,
        end: end.ok_or(SqlError::MissingFilter("end"))?,
        min_duration,
    })
}

fn extract_availability_filters(
    expr: &Expr,
    resource_id: &mut Option<Ulid>,
    start: &mut Option<Ms>,
    end: &mut Option<Ms>,
    min_duration: &mut Option<Ms>,
) -> Result<(), SqlError> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            ast::BinaryOperator::And => {
                extract_availability_filters(left, resource_id, start, end, min_duration)?;
                extract_availability_filters(right, resource_id, start, end, min_duration)?;
            }
            ast::BinaryOperator::Eq => {
                let col = expr_column_name(left);
                if col.as_deref() == Some("resource_id") {
                    *resource_id = Some(parse_ulid_expr(right)?);
                } else if col.as_deref() == Some("min_duration") {
                    *min_duration = Some(parse_i64_expr(right)?);
                }
            }
            ast::BinaryOperator::GtEq => {
                if expr_column_name(left).as_deref() == Some("start") {
                    *start = Some(parse_i64_expr(right)?);
                }
            }
            ast::BinaryOperator::LtEq => {
                if expr_column_name(left).as_deref() == Some("end") {
                    *end = Some(parse_i64_expr(right)?);
                }
            }
            _ => {}
        },
        _ => {}
    }
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────

fn object_name_last(name: &ast::ObjectName) -> Option<String> {
    name.0.last().and_then(|part| match part {
        ObjectNamePart::Identifier(ident) => Some(ident.value.to_lowercase()),
        _ => None,
    })
}

fn insert_table_name(insert: &ast::Insert) -> Result<String, SqlError> {
    match &insert.table {
        TableObject::TableName(name) => {
            object_name_last(name).ok_or_else(|| SqlError::Parse("empty table name".into()))
        }
        _ => Err(SqlError::Parse("unsupported table object in INSERT".into())),
    }
}

fn delete_table_name(delete: &ast::Delete) -> Result<String, SqlError> {
    let tables_with_joins = match &delete.from {
        FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
    };
    if let Some(first) = tables_with_joins.first() {
        table_factor_name(&first.relation)
    } else {
        Err(SqlError::Parse("DELETE without table".into()))
    }
}

fn table_factor_name(tf: &TableFactor) -> Result<String, SqlError> {
    match tf {
        TableFactor::Table { name, .. } => {
            object_name_last(name).ok_or_else(|| SqlError::Parse("empty table name".into()))
        }
        _ => Err(SqlError::Parse("complex table expression".into())),
    }
}

fn extract_insert_values(insert: &ast::Insert) -> Result<Vec<Expr>, SqlError> {
    let body = insert
        .source
        .as_ref()
        .ok_or(SqlError::Parse("no VALUES".into()))?;
    match body.body.as_ref() {
        SetExpr::Values(values) => {
            if values.rows.is_empty() {
                return Err(SqlError::Parse("empty VALUES".into()));
            }
            Ok(values.rows[0].clone())
        }
        _ => Err(SqlError::Parse("expected VALUES".into())),
    }
}

fn extract_all_insert_rows(insert: &ast::Insert) -> Result<Vec<Vec<Expr>>, SqlError> {
    let body = insert
        .source
        .as_ref()
        .ok_or(SqlError::Parse("no VALUES".into()))?;
    match body.body.as_ref() {
        SetExpr::Values(values) => {
            if values.rows.is_empty() {
                return Err(SqlError::Parse("empty VALUES".into()));
            }
            Ok(values.rows.clone())
        }
        _ => Err(SqlError::Parse("expected VALUES".into())),
    }
}

fn extract_where_id(selection: &Option<Expr>) -> Result<Ulid, SqlError> {
    let sel = selection.as_ref().ok_or(SqlError::MissingFilter("id"))?;
    match sel {
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } => {
            if expr_column_name(left).as_deref() == Some("id") {
                parse_ulid_expr(right)
            } else {
                Err(SqlError::MissingFilter("id"))
            }
        }
        _ => Err(SqlError::MissingFilter("id")),
    }
}

fn expr_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.to_lowercase()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|i| i.value.to_lowercase()),
        _ => None,
    }
}

fn extract_value(expr: &Expr) -> Option<&Value> {
    match expr {
        Expr::Value(ValueWithSpan { value, .. }) => Some(value),
        _ => None,
    }
}

fn parse_ulid_expr(expr: &Expr) -> Result<Ulid, SqlError> {
    if let Some(value) = extract_value(expr) {
        match value {
            Value::SingleQuotedString(s) | Value::Number(s, _) => {
                Ulid::from_string(s).map_err(|e| SqlError::Parse(format!("bad ULID: {e}")))
            }
            _ => Err(SqlError::Parse(format!("expected string, got {value:?}"))),
        }
    } else {
        Err(SqlError::Parse(format!("expected value, got {expr:?}")))
    }
}

fn parse_i64_expr(expr: &Expr) -> Result<i64, SqlError> {
    if let Some(value) = extract_value(expr) {
        match value {
            Value::Number(s, _) => s
                .parse()
                .map_err(|e| SqlError::Parse(format!("bad i64: {e}"))),
            Value::SingleQuotedString(s) => s
                .parse()
                .map_err(|e| SqlError::Parse(format!("bad i64: {e}"))),
            _ => Err(SqlError::Parse(format!("expected number, got {value:?}"))),
        }
    } else if let Expr::UnaryOp {
        op: ast::UnaryOperator::Minus,
        expr,
    } = expr
    {
        Ok(-parse_i64_expr(expr)?)
    } else {
        Err(SqlError::Parse(format!("expected value, got {expr:?}")))
    }
}

fn parse_ulid(expr: &Expr) -> Result<Ulid, SqlError> {
    parse_ulid_expr(expr)
}

fn parse_ulid_or_null(expr: &Expr) -> Result<Option<Ulid>, SqlError> {
    if let Some(value) = extract_value(expr) {
        match value {
            Value::Null => Ok(None),
            Value::SingleQuotedString(s) | Value::Number(s, _) => Ok(Some(
                Ulid::from_string(s).map_err(|e| SqlError::Parse(format!("bad ULID: {e}")))?,
            )),
            _ => Err(SqlError::Parse(format!(
                "expected string or NULL, got {value:?}"
            ))),
        }
    } else {
        Err(SqlError::Parse(format!("expected value, got {expr:?}")))
    }
}

#[allow(dead_code)]
fn parse_u16(expr: &Expr) -> Result<u16, SqlError> {
    let v = parse_i64_expr(expr)?;
    u16::try_from(v).map_err(|_| SqlError::Parse(format!("{v} out of u16 range")))
}

fn parse_u32(expr: &Expr) -> Result<u32, SqlError> {
    let v = parse_i64_expr(expr)?;
    u32::try_from(v).map_err(|_| SqlError::Parse(format!("{v} out of u32 range")))
}

fn parse_i64_or_null(expr: &Expr) -> Result<Option<i64>, SqlError> {
    if let Some(value) = extract_value(expr) {
        match value {
            Value::Null => Ok(None),
            _ => Ok(Some(parse_i64_expr(expr)?)),
        }
    } else {
        Ok(Some(parse_i64_expr(expr)?))
    }
}

fn parse_i64(expr: &Expr) -> Result<i64, SqlError> {
    parse_i64_expr(expr)
}

fn parse_bool(expr: &Expr) -> Result<bool, SqlError> {
    if let Some(value) = extract_value(expr) {
        match value {
            Value::Boolean(b) => Ok(*b),
            Value::SingleQuotedString(s) => match s.to_lowercase().as_str() {
                "true" | "t" | "1" => Ok(true),
                "false" | "f" | "0" => Ok(false),
                _ => Err(SqlError::Parse(format!("bad bool: {s}"))),
            },
            Value::Number(n, _) => Ok(n != "0"),
            _ => Err(SqlError::Parse(format!("expected bool, got {value:?}"))),
        }
    } else {
        Err(SqlError::Parse(format!("expected value, got {expr:?}")))
    }
}

// ── Errors ────────────────────────────────────────────────────

#[derive(Debug)]
pub enum SqlError {
    Parse(String),
    Empty,
    Unsupported(String),
    UnknownTable(String),
    WrongArity(&'static str, usize, usize),
    MissingFilter(&'static str),
}

impl std::fmt::Display for SqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlError::Parse(s) => write!(f, "parse error: {s}"),
            SqlError::Empty => write!(f, "empty query"),
            SqlError::Unsupported(s) => write!(f, "unsupported: {s}"),
            SqlError::UnknownTable(t) => write!(f, "unknown table: {t}"),
            SqlError::WrongArity(t, expected, got) => {
                write!(f, "{t}: expected {expected} values, got {got}")
            }
            SqlError::MissingFilter(col) => write!(f, "missing filter: {col}"),
        }
    }
}

impl std::error::Error for SqlError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_insert_resource() {
        let sql = "INSERT INTO resources (id) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV')";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertResource { id, parent_id, capacity, buffer_after } => {
                assert_eq!(id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
                assert_eq!(parent_id, None);
                assert_eq!(capacity, 1);
                assert_eq!(buffer_after, None);
            }
            _ => panic!("expected InsertResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_resource_with_parent() {
        let sql = "INSERT INTO resources (id, parent_id) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV')";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertResource { id, parent_id, capacity, buffer_after } => {
                assert_eq!(id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
                assert_eq!(parent_id, Some(id));
                assert_eq!(capacity, 1);
                assert_eq!(buffer_after, None);
            }
            _ => panic!("expected InsertResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_resource_with_null_parent() {
        let sql = "INSERT INTO resources (id, parent_id) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', NULL)";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertResource { parent_id, .. } => {
                assert_eq!(parent_id, None);
            }
            _ => panic!("expected InsertResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_resource_with_capacity_and_buffer() {
        let sql = "INSERT INTO resources (id, parent_id, capacity, buffer_after) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', NULL, 20, 1800000)";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertResource { capacity, buffer_after, .. } => {
                assert_eq!(capacity, 20);
                assert_eq!(buffer_after, Some(1800000));
            }
            _ => panic!("expected InsertResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_resource_capacity_only() {
        let sql = "INSERT INTO resources (id, parent_id, capacity) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', NULL, 5)";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertResource { capacity, buffer_after, .. } => {
                assert_eq!(capacity, 5);
                assert_eq!(buffer_after, None);
            }
            _ => panic!("expected InsertResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_delete_resource() {
        let sql = "DELETE FROM resources WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::DeleteResource { id } => {
                assert_eq!(id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected DeleteResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_rule() {
        let sql = r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 1000, 2000, false)"#;
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertRule {
                start,
                end,
                blocking,
                ..
            } => {
                assert_eq!(start, 1000);
                assert_eq!(end, 2000);
                assert!(!blocking);
            }
            _ => panic!("expected InsertRule, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_hold() {
        let sql = r#"INSERT INTO holds (id, resource_id, start, "end", expires_at) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 1000, 2000, 3000)"#;
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertHold {
                start,
                end,
                expires_at,
                ..
            } => {
                assert_eq!(start, 1000);
                assert_eq!(end, 2000);
                assert_eq!(expires_at, 3000);
            }
            _ => panic!("expected InsertHold, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_booking() {
        let sql = r#"INSERT INTO bookings (id, resource_id, start, "end") VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 1000, 2000)"#;
        let cmd = parse_sql(sql).unwrap();
        assert!(matches!(cmd, Command::InsertBooking { .. }));
    }

    #[test]
    fn parse_delete_hold() {
        let sql = "DELETE FROM holds WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        assert!(matches!(cmd, Command::DeleteHold { .. }));
    }

    #[test]
    fn parse_select_availability() {
        let sql = "SELECT * FROM availability WHERE resource_id = '01ARZ3NDEKTSV4RRFFQ69G5FAV' AND start >= 1000 AND \"end\" <= 2000";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::SelectAvailability {
                resource_id,
                start,
                end,
                min_duration,
            } => {
                assert_eq!(resource_id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
                assert_eq!(start, 1000);
                assert_eq!(end, 2000);
                assert_eq!(min_duration, None);
            }
            _ => panic!("expected SelectAvailability, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_availability_with_min_duration() {
        let sql = "SELECT * FROM availability WHERE resource_id = '01ARZ3NDEKTSV4RRFFQ69G5FAV' AND start >= 1000 AND \"end\" <= 2000 AND min_duration = 1800000";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::SelectAvailability { min_duration, .. } => {
                assert_eq!(min_duration, Some(1800000));
            }
            _ => panic!("expected SelectAvailability, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_listen() {
        let sql = "LISTEN resource_01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::Listen { channel } => {
                assert_eq!(channel, "resource_01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected Listen, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_unknown_table_errors() {
        let sql = "INSERT INTO foobar (id) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV')";
        assert!(parse_sql(sql).is_err());
    }

    #[test]
    fn parse_batch_insert_bookings() {
        let sql = r#"INSERT INTO bookings (id, resource_id, start, "end") VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 1000, 2000), ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 3000, 4000)"#;
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::BatchInsertBookings { bookings } => {
                assert_eq!(bookings.len(), 2);
                assert_eq!(bookings[0].2, 1000);
                assert_eq!(bookings[0].3, 2000);
                assert_eq!(bookings[1].2, 3000);
                assert_eq!(bookings[1].3, 4000);
            }
            _ => panic!("expected BatchInsertBookings, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_single_insert_booking_not_batch() {
        // A single-row INSERT should still produce InsertBooking, not BatchInsertBookings
        let sql = r#"INSERT INTO bookings (id, resource_id, start, "end") VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 1000, 2000)"#;
        let cmd = parse_sql(sql).unwrap();
        assert!(matches!(cmd, Command::InsertBooking { .. }));
    }

    #[test]
    fn parse_empty_errors() {
        assert!(matches!(parse_sql(""), Err(SqlError::Empty)));
    }
}
