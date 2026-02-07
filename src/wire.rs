use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use futures::Sink;
use pgwire::api::auth::cleartext::CleartextPasswordAuthStartupHandler;
use pgwire::api::auth::{DefaultServerParameterProvider, StartupHandler};
use pgwire::api::copy::CopyHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, NoopHandler, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use ulid::Ulid;

use crate::auth::DeltaTAuthSource;
use crate::engine::Engine;
use crate::model::*;
use crate::sql::{self, Command};
use crate::tenant::TenantManager;

pub struct DeltaTHandler {
    tenant_manager: Arc<TenantManager>,
    query_parser: Arc<DeltaTQueryParser>,
}

impl DeltaTHandler {
    pub fn new(tenant_manager: Arc<TenantManager>) -> Self {
        Self {
            tenant_manager,
            query_parser: Arc::new(DeltaTQueryParser),
        }
    }

    fn resolve_engine<C: ClientInfo>(&self, client: &C) -> PgWireResult<Arc<Engine>> {
        let db = client
            .metadata()
            .get("database")
            .cloned()
            .unwrap_or_else(|| "default".to_string());
        self.tenant_manager.get_or_create(&db).map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".into(),
                "08006".into(),
                format!("tenant error: {e}"),
            )))
        })
    }

    async fn execute_command(
        &self,
        engine: &Engine,
        cmd: Command,
    ) -> PgWireResult<Vec<Response>> {
        match cmd {
            Command::InsertResource {
                id,
                parent_id,
                capacity,
                buffer_after,
            } => {
                engine
                    .create_resource(id, parent_id, capacity, buffer_after)
                    .await
                    .map_err(engine_err)?;
                Ok(vec![Response::Execution(Tag::new("INSERT").with_rows(1))])
            }
            Command::DeleteResource { id } => {
                engine.delete_resource(id).await.map_err(engine_err)?;
                Ok(vec![Response::Execution(Tag::new("DELETE").with_rows(1))])
            }
            Command::InsertRule {
                id,
                resource_id,
                start,
                end,
                blocking,
            } => {
                engine
                    .add_rule(id, resource_id, Span::new(start, end), blocking)
                    .await
                    .map_err(engine_err)?;
                Ok(vec![Response::Execution(Tag::new("INSERT").with_rows(1))])
            }
            Command::DeleteRule { id } => {
                engine.remove_rule(id).await.map_err(engine_err)?;
                Ok(vec![Response::Execution(Tag::new("DELETE").with_rows(1))])
            }
            Command::InsertHold {
                id,
                resource_id,
                start,
                end,
                expires_at,
            } => {
                engine
                    .place_hold(id, resource_id, Span::new(start, end), expires_at)
                    .await
                    .map_err(engine_err)?;
                Ok(vec![Response::Execution(Tag::new("INSERT").with_rows(1))])
            }
            Command::DeleteHold { id } => {
                engine.release_hold(id).await.map_err(engine_err)?;
                Ok(vec![Response::Execution(Tag::new("DELETE").with_rows(1))])
            }
            Command::InsertBooking {
                id,
                resource_id,
                start,
                end,
            } => {
                engine
                    .confirm_booking(id, resource_id, Span::new(start, end))
                    .await
                    .map_err(engine_err)?;
                Ok(vec![Response::Execution(Tag::new("INSERT").with_rows(1))])
            }
            Command::BatchInsertBookings { bookings } => {
                let count = bookings.len();
                let batch: Vec<_> = bookings
                    .into_iter()
                    .map(|(id, resource_id, start, end)| (id, resource_id, Span::new(start, end)))
                    .collect();
                engine
                    .batch_confirm_bookings(batch)
                    .await
                    .map_err(engine_err)?;
                Ok(vec![Response::Execution(Tag::new("INSERT").with_rows(count))])
            }
            Command::DeleteBooking { id } => {
                engine.cancel_booking(id).await.map_err(engine_err)?;
                Ok(vec![Response::Execution(Tag::new("DELETE").with_rows(1))])
            }
            Command::SelectAvailability {
                resource_id,
                start,
                end,
                min_duration,
            } => {
                let slots = engine
                    .compute_availability(resource_id, start, end, min_duration)
                    .await
                    .map_err(engine_err)?;

                let schema = Arc::new(availability_schema());

                let rid_str = resource_id.to_string();
                let rows: Vec<PgWireResult<_>> = slots
                    .into_iter()
                    .map(|slot| {
                        let mut encoder = DataRowEncoder::new(schema.clone());
                        encoder.encode_field(&rid_str)?;
                        encoder.encode_field(&slot.start)?;
                        encoder.encode_field(&slot.end)?;
                        Ok(encoder.take_row())
                    })
                    .collect();

                Ok(vec![Response::Query(QueryResponse::new(
                    schema,
                    stream::iter(rows),
                ))])
            }
            Command::Listen { channel } => {
                let resource_id_str = channel.strip_prefix("resource_").ok_or_else(|| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".into(),
                        "42000".into(),
                        format!("invalid channel: {channel} (expected resource_{{id}})"),
                    )))
                })?;
                let _resource_id = Ulid::from_string(resource_id_str).map_err(|e| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".into(),
                        "42000".into(),
                        format!("bad ULID in channel: {e}"),
                    )))
                })?;
                Ok(vec![Response::Execution(Tag::new("LISTEN"))])
            }
        }
    }
}

fn availability_schema() -> Vec<FieldInfo> {
    vec![
        FieldInfo::new(
            "resource_id".into(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        ),
        FieldInfo::new("start".into(), None, None, Type::INT8, FieldFormat::Text),
        FieldInfo::new("end".into(), None, None, Type::INT8, FieldFormat::Text),
    ]
}

#[async_trait]
impl SimpleQueryHandler for DeltaTHandler {
    async fn do_query<C>(
        &self,
        client: &mut C,
        query: &str,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<C::Error>,
    {
        let engine = self.resolve_engine(client)?;
        let cmd = sql::parse_sql(query).map_err(sql_err)?;
        self.execute_command(&engine, cmd).await
    }
}

// ── Extended Query Protocol ──────────────────────────────────────

#[derive(Debug)]
pub struct DeltaTQueryParser;

#[async_trait]
impl QueryParser for DeltaTQueryParser {
    type Statement = String;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        _types: &[Option<Type>],
    ) -> PgWireResult<String>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(sql.to_string())
    }

    fn get_parameter_types(&self, stmt: &String) -> PgWireResult<Vec<Type>> {
        Ok(vec![Type::VARCHAR; count_params(stmt)])
    }

    fn get_result_schema(
        &self,
        stmt: &String,
        _column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        let upper = stmt.to_uppercase();
        if upper.contains("SELECT") && upper.contains("AVAILABILITY") {
            Ok(availability_schema())
        } else {
            Ok(vec![])
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for DeltaTHandler {
    type Statement = String;
    type QueryParser = DeltaTQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<C::Error>,
    {
        let engine = self.resolve_engine(client)?;
        let sql = substitute_params(portal);
        let cmd = sql::parse_sql(&sql).map_err(sql_err)?;
        let mut responses = self.execute_command(&engine, cmd).await?;
        Ok(responses.remove(0))
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<C::Error>,
    {
        let param_types = vec![Type::VARCHAR; count_params(&target.statement)];
        let sql_upper = target.statement.to_uppercase();
        if sql_upper.contains("SELECT") && sql_upper.contains("AVAILABILITY") {
            Ok(DescribeStatementResponse::new(
                param_types,
                availability_schema(),
            ))
        } else {
            Ok(DescribeStatementResponse::new(param_types, vec![]))
        }
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<C::Error>,
    {
        let sql_upper = target.statement.statement.to_uppercase();
        if sql_upper.contains("SELECT") && sql_upper.contains("AVAILABILITY") {
            Ok(DescribePortalResponse::new(availability_schema()))
        } else {
            Ok(DescribePortalResponse::new(vec![]))
        }
    }
}

/// Count the highest $N parameter placeholder in the SQL string.
fn count_params(sql: &str) -> usize {
    let mut max = 0usize;
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            i += 1;
            let start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            if i > start {
                if let Ok(n) = sql[start..i].parse::<usize>() {
                    if n > max {
                        max = n;
                    }
                }
            }
        } else {
            i += 1;
        }
    }
    max
}

/// Substitute $1, $2, ... placeholders with bound parameter values (text format).
fn substitute_params(portal: &Portal<String>) -> String {
    let sql = portal.statement.statement.to_string();
    let params = &portal.parameters;
    let mut result = sql;

    for (i, param) in params.iter().enumerate().rev() {
        let placeholder = format!("${}", i + 1);
        let value = match param {
            Some(bytes) => {
                let text = String::from_utf8_lossy(bytes);
                format!("'{}'", text.replace('\'', "''"))
            }
            None => "NULL".to_string(),
        };
        result = result.replace(&placeholder, &value);
    }

    result
}

// ── Factory ──────────────────────────────────────────────────────

pub struct DeltaTFactory {
    handler: Arc<DeltaTHandler>,
    auth_handler:
        Arc<CleartextPasswordAuthStartupHandler<DeltaTAuthSource, DefaultServerParameterProvider>>,
    noop: Arc<NoopHandler>,
}

impl DeltaTFactory {
    pub fn new(tenant_manager: Arc<TenantManager>, password: String) -> Self {
        let auth_source = DeltaTAuthSource::new(password);
        let param_provider = DefaultServerParameterProvider::default();
        Self {
            handler: Arc::new(DeltaTHandler::new(tenant_manager)),
            auth_handler: Arc::new(CleartextPasswordAuthStartupHandler::new(
                auth_source,
                param_provider,
            )),
            noop: Arc::new(NoopHandler),
        }
    }
}

impl PgWireServerHandlers for DeltaTFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.auth_handler.clone()
    }

    fn copy_handler(&self) -> Arc<impl CopyHandler> {
        self.noop.clone()
    }
}

fn engine_err(e: crate::engine::EngineError) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        "P0001".into(),
        e.to_string(),
    )))
}

fn sql_err(e: crate::sql::SqlError) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        "42601".into(),
        e.to_string(),
    )))
}
