use sqlparser::ast::{self, Expr, FromTable, ObjectNamePart, SetExpr, Statement, TableFactor, TableObject, Value, ValueWithSpan};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use ulid::Ulid;

use crate::limits::MAX_IN_CLAUSE_IDS;
use crate::model::*;

/// Parsed command from SQL input.
#[derive(Debug, PartialEq)]
pub enum Command {
    InsertResource {
        id: Ulid,
        parent_id: Option<Ulid>,
        name: Option<String>,
        capacity: u32,
        buffer_after: Option<Ms>,
    },
    UpdateResource {
        id: Ulid,
        name: Option<String>,
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
    UpdateRule {
        id: Ulid,
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
        label: Option<String>,
    },
    BatchInsertBookings {
        bookings: Vec<(Ulid, Ulid, Ms, Ms, Option<String>)>, // (id, resource_id, start, end, label)
    },
    DeleteBooking {
        id: Ulid,
    },
    SelectResources {
        parent_id: Option<Option<Ulid>>, // None = no filter, Some(None) = root only, Some(Some(id)) = children of id
    },
    SelectRules {
        resource_id: Ulid,
    },
    SelectBookings {
        resource_id: Ulid,
    },
    SelectHolds {
        resource_id: Ulid,
    },
    SelectAvailability {
        resource_id: Ulid,
        start: Ms,
        end: Ms,
        min_duration: Option<Ms>,
    },
    SelectMultiAvailability {
        resource_ids: Vec<Ulid>,
        start: Ms,
        end: Ms,
        min_available: usize,
        min_duration: Option<Ms>,
    },
    Listen {
        channel: String,
    },
    Unlisten {
        channel: String,
    },
    UnlistenAll,
}

pub fn parse_sql(sql: &str) -> Result<Command, SqlError> {
    let trimmed = sql.trim();
    if trimmed.to_uppercase().starts_with("LISTEN ") {
        let channel = trimmed[7..].trim().trim_matches(';').trim_matches('"').to_string();
        return Ok(Command::Listen { channel });
    }
    if trimmed.to_uppercase().starts_with("UNLISTEN ") {
        let rest = trimmed[9..].trim().trim_matches(';').trim_matches('"');
        if rest == "*" {
            return Ok(Command::UnlistenAll);
        }
        return Ok(Command::Unlisten { channel: rest.to_string() });
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
        Statement::Update { table, assignments, selection, .. } => parse_update(table, assignments, selection),
        other => Err(SqlError::Unsupported(format!("{other}"))),
    }
}

fn parse_insert(insert: &ast::Insert) -> Result<Command, SqlError> {
    let table = insert_table_name(insert)?;
    let values = extract_insert_values(insert)?;
    let columns = extract_column_names(insert);

    match table.as_str() {
        "resources" => {
            if values.is_empty() {
                return Err(SqlError::WrongArity("resources", 1, 0));
            }
            // Column-aware parsing: map column name → value index
            let col_idx = |name: &str| -> Option<usize> {
                if columns.is_empty() { None } else { columns.iter().position(|c| c == name) }
            };

            let id = parse_ulid(&values[col_idx("id").unwrap_or(0)])?;
            let parent_id = col_idx("parent_id").or(if columns.is_empty() && values.len() >= 2 { Some(1) } else { None })
                .map(|i| parse_ulid_or_null(&values[i]))
                .transpose()?
                .flatten();
            let name = col_idx("name")
                .map(|i| parse_string_or_null(&values[i]))
                .transpose()?
                .flatten();

            // For capacity/buffer_after, use column names if present; otherwise use positional
            // (skipping over name if it wasn't in columns)
            let capacity = if let Some(i) = col_idx("capacity") {
                parse_u32(&values[i])?
            } else if columns.is_empty() {
                // No columns specified: old positional order (id, parent_id, capacity, buffer_after)
                if values.len() >= 3 { parse_u32(&values[2])? } else { 1 }
            } else {
                1
            };
            let buffer_after = if let Some(i) = col_idx("buffer_after") {
                parse_i64_or_null(&values[i])?
            } else if columns.is_empty() {
                if values.len() >= 4 { parse_i64_or_null(&values[3])? } else { None }
            } else {
                None
            };

            Ok(Command::InsertResource { id, parent_id, name, capacity, buffer_after })
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
            let label_idx = columns.iter().position(|c| c == "label");

            if all_rows.len() == 1 {
                let values = &all_rows[0];
                if values.len() < 4 {
                    return Err(SqlError::WrongArity("bookings", 4, values.len()));
                }
                let label = label_idx
                    .map(|i| parse_string_or_null(&values[i]))
                    .transpose()?
                    .flatten();
                Ok(Command::InsertBooking {
                    id: parse_ulid(&values[0])?,
                    resource_id: parse_ulid(&values[1])?,
                    start: parse_i64(&values[2])?,
                    end: parse_i64(&values[3])?,
                    label,
                })
            } else {
                let mut bookings = Vec::with_capacity(all_rows.len());
                for (i, row) in all_rows.iter().enumerate() {
                    if row.len() < 4 {
                        return Err(SqlError::WrongArity("bookings row", 4, row.len()));
                    }
                    let label = label_idx
                        .map(|j| parse_string_or_null(&row[j]).map_err(|e| SqlError::Parse(format!("row {i}: {e}"))))
                        .transpose()?
                        .flatten();
                    bookings.push((
                        parse_ulid(&row[0]).map_err(|e| SqlError::Parse(format!("row {i}: {e}")))?,
                        parse_ulid(&row[1]).map_err(|e| SqlError::Parse(format!("row {i}: {e}")))?,
                        parse_i64(&row[2]).map_err(|e| SqlError::Parse(format!("row {i}: {e}")))?,
                        parse_i64(&row[3]).map_err(|e| SqlError::Parse(format!("row {i}: {e}")))?,
                        label,
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

    match table.as_str() {
        "availability" => {
            let mut filters = AvailabilityFilters::default();
            if let Some(selection) = &select.selection {
                extract_availability_filters(selection, &mut filters)?;
            }

            let start = filters.start.ok_or(SqlError::MissingFilter("start"))?;
            let end = filters.end.ok_or(SqlError::MissingFilter("end"))?;

            if !filters.resource_ids.is_empty() {
                let count = filters.resource_ids.len();
                Ok(Command::SelectMultiAvailability {
                    resource_ids: filters.resource_ids,
                    start,
                    end,
                    min_available: filters.min_available.unwrap_or(count),
                    min_duration: filters.min_duration,
                })
            } else {
                Ok(Command::SelectAvailability {
                    resource_id: filters.resource_id.ok_or(SqlError::MissingFilter("resource_id"))?,
                    start,
                    end,
                    min_duration: filters.min_duration,
                })
            }
        }
        "resources" => {
            // Optional: WHERE parent_id = 'X' or WHERE parent_id IS NULL
            let parent_id = if let Some(selection) = &select.selection {
                Some(extract_parent_id_filter(selection)?)
            } else {
                None
            };
            Ok(Command::SelectResources { parent_id })
        }
        "rules" => {
            let resource_id = extract_resource_id_filter(&select.selection)?;
            Ok(Command::SelectRules { resource_id })
        }
        "bookings" => {
            let resource_id = extract_resource_id_filter(&select.selection)?;
            Ok(Command::SelectBookings { resource_id })
        }
        "holds" => {
            let resource_id = extract_resource_id_filter(&select.selection)?;
            Ok(Command::SelectHolds { resource_id })
        }
        _ => Err(SqlError::UnknownTable(table)),
    }
}

#[derive(Default)]
struct AvailabilityFilters {
    resource_id: Option<Ulid>,
    resource_ids: Vec<Ulid>,
    start: Option<Ms>,
    end: Option<Ms>,
    min_duration: Option<Ms>,
    min_available: Option<usize>,
}

fn extract_availability_filters(
    expr: &Expr,
    f: &mut AvailabilityFilters,
) -> Result<(), SqlError> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            ast::BinaryOperator::And => {
                extract_availability_filters(left, f)?;
                extract_availability_filters(right, f)?;
            }
            ast::BinaryOperator::Eq => {
                let col = expr_column_name(left);
                if col.as_deref() == Some("resource_id") {
                    f.resource_id = Some(parse_ulid_expr(right)?);
                } else if col.as_deref() == Some("min_duration") {
                    f.min_duration = Some(parse_i64_expr(right)?);
                } else if col.as_deref() == Some("min_available") {
                    let v = parse_i64_expr(right)?;
                    f.min_available = Some(v as usize);
                }
            }
            ast::BinaryOperator::GtEq => {
                if expr_column_name(left).as_deref() == Some("start") {
                    f.start = Some(parse_i64_expr(right)?);
                }
            }
            ast::BinaryOperator::LtEq => {
                if expr_column_name(left).as_deref() == Some("end") {
                    f.end = Some(parse_i64_expr(right)?);
                }
            }
            _ => {}
        },
        // resource_id IN ('id1', 'id2', ...)
        Expr::InList { expr: col_expr, list, negated } if !negated => {
            if expr_column_name(col_expr).as_deref() == Some("resource_id") {
                if list.len() > MAX_IN_CLAUSE_IDS {
                    return Err(SqlError::Parse(format!(
                        "IN clause too large: {} IDs (max {})",
                        list.len(),
                        MAX_IN_CLAUSE_IDS
                    )));
                }
                for item in list {
                    f.resource_ids.push(parse_ulid_expr(item)?);
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn parse_update(
    table: &ast::TableWithJoins,
    assignments: &[ast::Assignment],
    selection: &Option<Expr>,
) -> Result<Command, SqlError> {
    let table_name = table_factor_name(&table.relation)?;
    let id = extract_where_id(selection)?;

    match table_name.as_str() {
        "resources" => {
            let mut name: Option<String> = None;
            let mut capacity: Option<u32> = None;
            let mut buffer_after: Option<Option<Ms>> = None;

            for a in assignments {
                let col = assignment_column_name(a)?;
                match col.as_str() {
                    "name" => name = parse_string_or_null(&a.value)?,
                    "capacity" => capacity = Some(parse_u32(&a.value)?),
                    "buffer_after" => buffer_after = Some(parse_i64_or_null(&a.value)?),
                    _ => {}
                }
            }

            Ok(Command::UpdateResource {
                id,
                name,
                capacity: capacity.unwrap_or(1),
                buffer_after: buffer_after.unwrap_or(None),
            })
        }
        "rules" => {
            let mut start: Option<Ms> = None;
            let mut end: Option<Ms> = None;
            let mut blocking: Option<bool> = None;

            for a in assignments {
                let col = assignment_column_name(a)?;
                match col.as_str() {
                    "start" => start = Some(parse_i64_expr(&a.value)?),
                    "end" => end = Some(parse_i64_expr(&a.value)?),
                    "blocking" => blocking = Some(parse_bool(&a.value)?),
                    _ => {}
                }
            }

            Ok(Command::UpdateRule {
                id,
                start: start.ok_or(SqlError::MissingFilter("start"))?,
                end: end.ok_or(SqlError::MissingFilter("end"))?,
                blocking: blocking.ok_or(SqlError::MissingFilter("blocking"))?,
            })
        }
        _ => Err(SqlError::Unsupported(format!("UPDATE {table_name}"))),
    }
}

fn assignment_column_name(a: &ast::Assignment) -> Result<String, SqlError> {
    match &a.target {
        ast::AssignmentTarget::ColumnName(name) => {
            object_name_last(name).ok_or_else(|| SqlError::Parse("empty assignment target".into()))
        }
        _ => Err(SqlError::Parse("unsupported assignment target".into())),
    }
}

fn extract_resource_id_filter(selection: &Option<Expr>) -> Result<Ulid, SqlError> {
    let sel = selection.as_ref().ok_or(SqlError::MissingFilter("resource_id"))?;
    match sel {
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } => {
            if expr_column_name(left).as_deref() == Some("resource_id") {
                parse_ulid_expr(right)
            } else {
                Err(SqlError::MissingFilter("resource_id"))
            }
        }
        // Handle AND expressions — find resource_id = X within ANDs
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => {
            extract_resource_id_filter(&Some(*left.clone()))
                .or_else(|_| extract_resource_id_filter(&Some(*right.clone())))
        }
        _ => Err(SqlError::MissingFilter("resource_id")),
    }
}

fn extract_parent_id_filter(selection: &Expr) -> Result<Option<Ulid>, SqlError> {
    match selection {
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } => {
            if expr_column_name(left).as_deref() == Some("parent_id") {
                Ok(Some(parse_ulid_expr(right)?))
            } else {
                Err(SqlError::MissingFilter("parent_id"))
            }
        }
        Expr::IsNull(inner) => {
            if expr_column_name(inner).as_deref() == Some("parent_id") {
                Ok(None)
            } else {
                Err(SqlError::MissingFilter("parent_id"))
            }
        }
        _ => Err(SqlError::MissingFilter("parent_id")),
    }
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

fn extract_column_names(insert: &ast::Insert) -> Vec<String> {
    insert.columns.iter().map(|c| c.value.to_lowercase()).collect()
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

fn parse_string_or_null(expr: &Expr) -> Result<Option<String>, SqlError> {
    if let Some(value) = extract_value(expr) {
        match value {
            Value::Null => Ok(None),
            Value::SingleQuotedString(s) => Ok(Some(s.clone())),
            _ => Err(SqlError::Parse(format!("expected string or NULL, got {value:?}"))),
        }
    } else {
        Err(SqlError::Parse(format!("expected value, got {expr:?}")))
    }
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
            Command::InsertResource { id, parent_id, name: _, capacity, buffer_after } => {
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
            Command::InsertResource { id, parent_id, name: _, capacity, buffer_after } => {
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
    fn parse_select_multi_availability() {
        let id1 = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let id2 = "01BRZ3NDEKTSV4RRFFQ69G5FAV";
        let sql = format!(
            "SELECT * FROM availability WHERE resource_id IN ('{id1}', '{id2}') AND start >= 1000 AND \"end\" <= 2000"
        );
        let cmd = parse_sql(&sql).unwrap();
        match cmd {
            Command::SelectMultiAvailability {
                resource_ids,
                start,
                end,
                min_available,
                min_duration,
            } => {
                assert_eq!(resource_ids.len(), 2);
                assert_eq!(resource_ids[0].to_string(), id1);
                assert_eq!(resource_ids[1].to_string(), id2);
                assert_eq!(start, 1000);
                assert_eq!(end, 2000);
                assert_eq!(min_available, 2); // default = ALL
                assert_eq!(min_duration, None);
            }
            _ => panic!("expected SelectMultiAvailability, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_multi_availability_with_min_available() {
        let id1 = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let id2 = "01BRZ3NDEKTSV4RRFFQ69G5FAV";
        let id3 = "01CRZ3NDEKTSV4RRFFQ69G5FAV";
        let sql = format!(
            "SELECT * FROM availability WHERE resource_id IN ('{id1}', '{id2}', '{id3}') AND start >= 1000 AND \"end\" <= 5000 AND min_available = 1"
        );
        let cmd = parse_sql(&sql).unwrap();
        match cmd {
            Command::SelectMultiAvailability {
                resource_ids,
                min_available,
                ..
            } => {
                assert_eq!(resource_ids.len(), 3);
                assert_eq!(min_available, 1); // ANY
            }
            _ => panic!("expected SelectMultiAvailability, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_multi_availability_single_id_in_list() {
        // Single ID in IN list should still produce SelectMultiAvailability
        let id1 = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let sql = format!(
            "SELECT * FROM availability WHERE resource_id IN ('{id1}') AND start >= 0 AND \"end\" <= 10000"
        );
        let cmd = parse_sql(&sql).unwrap();
        match cmd {
            Command::SelectMultiAvailability {
                resource_ids,
                min_available,
                ..
            } => {
                assert_eq!(resource_ids.len(), 1);
                assert_eq!(min_available, 1); // default = count = 1
            }
            _ => panic!("expected SelectMultiAvailability, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_multi_availability_with_min_duration() {
        let id1 = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let id2 = "01BRZ3NDEKTSV4RRFFQ69G5FAV";
        let sql = format!(
            "SELECT * FROM availability WHERE resource_id IN ('{id1}', '{id2}') AND start >= 0 AND \"end\" <= 10000 AND min_available = 1 AND min_duration = 3600000"
        );
        let cmd = parse_sql(&sql).unwrap();
        match cmd {
            Command::SelectMultiAvailability {
                resource_ids,
                min_available,
                min_duration,
                ..
            } => {
                assert_eq!(resource_ids.len(), 2);
                assert_eq!(min_available, 1);
                assert_eq!(min_duration, Some(3600000));
            }
            _ => panic!("expected SelectMultiAvailability, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_single_resource_still_works() {
        // resource_id = '...' (not IN) should still produce SelectAvailability
        let sql = "SELECT * FROM availability WHERE resource_id = '01ARZ3NDEKTSV4RRFFQ69G5FAV' AND start >= 1000 AND \"end\" <= 2000";
        let cmd = parse_sql(sql).unwrap();
        assert!(matches!(cmd, Command::SelectAvailability { .. }));
    }

    #[test]
    fn parse_select_multi_large_in_list() {
        // 5 IDs in the IN list
        let ids: Vec<String> = (0..5).map(|i| format!("01ARZ3NDEKTSV4RRFFQ69G5FA{i}")).collect();
        let in_list = ids.iter().map(|id| format!("'{id}'")).collect::<Vec<_>>().join(", ");
        let sql = format!(
            "SELECT * FROM availability WHERE resource_id IN ({in_list}) AND start >= 0 AND \"end\" <= 100000 AND min_available = 3"
        );
        let cmd = parse_sql(&sql).unwrap();
        match cmd {
            Command::SelectMultiAvailability {
                resource_ids,
                min_available,
                ..
            } => {
                assert_eq!(resource_ids.len(), 5);
                assert_eq!(min_available, 3);
            }
            _ => panic!("expected SelectMultiAvailability, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_empty_errors() {
        assert!(matches!(parse_sql(""), Err(SqlError::Empty)));
    }

    // ── SELECT resources ─────────────────────────────────────────

    #[test]
    fn parse_select_resources_all() {
        let cmd = parse_sql("SELECT * FROM resources").unwrap();
        match cmd {
            Command::SelectResources { parent_id } => assert_eq!(parent_id, None),
            _ => panic!("expected SelectResources, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_resources_by_parent_id() {
        let sql = "SELECT * FROM resources WHERE parent_id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::SelectResources { parent_id } => {
                let uid = Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
                assert_eq!(parent_id, Some(Some(uid)));
            }
            _ => panic!("expected SelectResources, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_resources_roots_only() {
        let sql = "SELECT * FROM resources WHERE parent_id IS NULL";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::SelectResources { parent_id } => {
                assert_eq!(parent_id, Some(None)); // Some(None) = root only
            }
            _ => panic!("expected SelectResources, got {cmd:?}"),
        }
    }

    // ── SELECT rules/bookings/holds ──────────────────────────────

    #[test]
    fn parse_select_rules() {
        let sql = "SELECT * FROM rules WHERE resource_id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::SelectRules { resource_id } => {
                assert_eq!(resource_id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected SelectRules, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_bookings() {
        let sql = "SELECT * FROM bookings WHERE resource_id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::SelectBookings { resource_id } => {
                assert_eq!(resource_id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected SelectBookings, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_holds() {
        let sql = "SELECT * FROM holds WHERE resource_id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::SelectHolds { resource_id } => {
                assert_eq!(resource_id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected SelectHolds, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_rules_missing_filter() {
        let sql = "SELECT * FROM rules";
        assert!(parse_sql(sql).is_err());
    }

    #[test]
    fn parse_select_bookings_missing_filter() {
        let sql = "SELECT * FROM bookings";
        assert!(parse_sql(sql).is_err());
    }

    // ── UPDATE resources ─────────────────────────────────────────

    #[test]
    fn parse_update_resource_name_and_capacity() {
        let sql = "UPDATE resources SET name = 'Meeting Room A', capacity = 5 WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::UpdateResource { id, name, capacity, buffer_after } => {
                assert_eq!(id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
                assert_eq!(name, Some("Meeting Room A".to_string()));
                assert_eq!(capacity, 5);
                assert_eq!(buffer_after, None);
            }
            _ => panic!("expected UpdateResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_update_resource_with_buffer() {
        let sql = "UPDATE resources SET capacity = 10, buffer_after = 900000 WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::UpdateResource { capacity, buffer_after, .. } => {
                assert_eq!(capacity, 10);
                assert_eq!(buffer_after, Some(900000));
            }
            _ => panic!("expected UpdateResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_update_resource_null_buffer() {
        let sql = "UPDATE resources SET buffer_after = NULL WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::UpdateResource { buffer_after, .. } => {
                assert_eq!(buffer_after, None);
            }
            _ => panic!("expected UpdateResource, got {cmd:?}"),
        }
    }

    // ── UPDATE rules ─────────────────────────────────────────────

    #[test]
    fn parse_update_rule() {
        let sql = r#"UPDATE rules SET start = 5000, "end" = 10000, blocking = true WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'"#;
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::UpdateRule { id, start, end, blocking } => {
                assert_eq!(id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
                assert_eq!(start, 5000);
                assert_eq!(end, 10000);
                assert!(blocking);
            }
            _ => panic!("expected UpdateRule, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_update_rule_missing_field() {
        // Missing blocking field should error
        let sql = r#"UPDATE rules SET start = 5000, "end" = 10000 WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'"#;
        assert!(parse_sql(sql).is_err());
    }

    #[test]
    fn parse_update_unknown_table() {
        let sql = "UPDATE foobar SET x = 1 WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        assert!(parse_sql(sql).is_err());
    }

    // ── INSERT with name / label ─────────────────────────────────

    #[test]
    fn parse_insert_resource_with_name() {
        let sql = "INSERT INTO resources (id, parent_id, name, capacity, buffer_after) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', NULL, 'Room 101', 3, NULL)";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertResource { id, parent_id, name, capacity, buffer_after } => {
                assert_eq!(id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
                assert_eq!(parent_id, None);
                assert_eq!(name, Some("Room 101".to_string()));
                assert_eq!(capacity, 3);
                assert_eq!(buffer_after, None);
            }
            _ => panic!("expected InsertResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_resource_with_null_name() {
        let sql = "INSERT INTO resources (id, parent_id, name, capacity) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', NULL, NULL, 1)";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertResource { name, .. } => {
                assert_eq!(name, None);
            }
            _ => panic!("expected InsertResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_resource_positional_backward_compat() {
        // Old positional format without column names: (id, parent_id, capacity, buffer_after)
        let sql = "INSERT INTO resources VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', NULL, 20, 1800000)";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertResource { capacity, buffer_after, name, .. } => {
                assert_eq!(capacity, 20);
                assert_eq!(buffer_after, Some(1800000));
                assert_eq!(name, None); // no name in positional format
            }
            _ => panic!("expected InsertResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_booking_with_label() {
        let sql = r#"INSERT INTO bookings (id, resource_id, start, "end", label) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 1000, 2000, 'Team Meeting')"#;
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertBooking { label, start, end, .. } => {
                assert_eq!(label, Some("Team Meeting".to_string()));
                assert_eq!(start, 1000);
                assert_eq!(end, 2000);
            }
            _ => panic!("expected InsertBooking, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_booking_with_null_label() {
        let sql = r#"INSERT INTO bookings (id, resource_id, start, "end", label) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 1000, 2000, NULL)"#;
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertBooking { label, .. } => {
                assert_eq!(label, None);
            }
            _ => panic!("expected InsertBooking, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_booking_without_label() {
        // No label column at all — should default to None
        let sql = r#"INSERT INTO bookings (id, resource_id, start, "end") VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 1000, 2000)"#;
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertBooking { label, .. } => {
                assert_eq!(label, None);
            }
            _ => panic!("expected InsertBooking, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_batch_insert_bookings_with_labels() {
        let sql = r#"INSERT INTO bookings (id, resource_id, start, "end", label) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 1000, 2000, 'Morning'), ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 3000, 4000, NULL)"#;
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::BatchInsertBookings { bookings } => {
                assert_eq!(bookings.len(), 2);
                assert_eq!(bookings[0].4, Some("Morning".to_string()));
                assert_eq!(bookings[1].4, None);
            }
            _ => panic!("expected BatchInsertBookings, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_delete_booking() {
        let sql = "DELETE FROM bookings WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        assert!(matches!(cmd, Command::DeleteBooking { .. }));
    }

    #[test]
    fn parse_delete_rule() {
        let sql = "DELETE FROM rules WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        assert!(matches!(cmd, Command::DeleteRule { .. }));
    }

    #[test]
    fn in_clause_too_many_ids() {
        let ids: Vec<String> = (0..MAX_IN_CLAUSE_IDS + 1)
            .map(|_| format!("'{}'", ulid::Ulid::new()))
            .collect();
        let sql = format!(
            "SELECT * FROM availability WHERE resource_id IN ({}) AND start >= 0 AND \"end\" <= 10000",
            ids.join(", ")
        );
        let result = parse_sql(&sql);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("IN clause too large"));
    }

    #[test]
    fn parse_unlisten() {
        let sql = "UNLISTEN resource_01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::Unlisten { channel } => {
                assert_eq!(channel, "resource_01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected Unlisten, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_unlisten_all() {
        let sql = "UNLISTEN *";
        let cmd = parse_sql(sql).unwrap();
        assert!(matches!(cmd, Command::UnlistenAll));
    }

    #[test]
    fn parse_unlisten_with_semicolon() {
        let sql = "UNLISTEN resource_01ARZ3NDEKTSV4RRFFQ69G5FAV;";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::Unlisten { channel } => {
                assert_eq!(channel, "resource_01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected Unlisten, got {cmd:?}"),
        }
    }

    #[test]
    fn in_clause_at_limit() {
        let ids: Vec<String> = (0..MAX_IN_CLAUSE_IDS)
            .map(|_| format!("'{}'", ulid::Ulid::new()))
            .collect();
        let sql = format!(
            "SELECT * FROM availability WHERE resource_id IN ({}) AND start >= 0 AND \"end\" <= 10000",
            ids.join(", ")
        );
        let result = parse_sql(&sql);
        assert!(result.is_ok());
    }

    #[test]
    fn parse_listen_case_insensitive() {
        let sql = "listen resource_01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::Listen { channel } => {
                assert_eq!(channel, "resource_01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected Listen, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_listen_with_semicolon() {
        let sql = "LISTEN resource_01ARZ3NDEKTSV4RRFFQ69G5FAV;";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::Listen { channel } => {
                assert_eq!(channel, "resource_01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected Listen, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_unlisten_all_with_semicolon() {
        let sql = "UNLISTEN *;";
        let cmd = parse_sql(sql).unwrap();
        assert!(matches!(cmd, Command::UnlistenAll));
    }

    #[test]
    fn parse_listen_quoted_channel() {
        // postgres.js sends LISTEN "channel_name" with double quotes
        let sql = r#"LISTEN "resource_01ARZ3NDEKTSV4RRFFQ69G5FAV""#;
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::Listen { channel } => {
                assert_eq!(channel, "resource_01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected Listen, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_unlisten_quoted_channel() {
        let sql = r#"UNLISTEN "resource_01ARZ3NDEKTSV4RRFFQ69G5FAV""#;
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::Unlisten { channel } => {
                assert_eq!(channel, "resource_01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected Unlisten, got {cmd:?}"),
        }
    }
}
