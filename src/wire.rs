use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream;
use futures::{Sink, SinkExt, StreamExt};
use pgwire::api::auth::cleartext::CleartextPasswordAuthStartupHandler;
use pgwire::api::auth::DefaultServerParameterProvider;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, NoopHandler, PgWireConnectionState, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::NotificationResponse;
use pgwire::messages::PgWireBackendMessage;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::codec::Framed;
use ulid::Ulid;

use crate::auth::DeltaTAuthSource;
use crate::engine::Engine;
use crate::limits::{MAX_QUERY_LEN, MAX_SUBSCRIPTIONS_PER_CONNECTION};
use crate::model::*;
use crate::sql::{self, Command};
use crate::tenant::TenantManager;

// ── Subscription plumbing ────────────────────────────────────────

pub enum SubscriptionCommand {
    Subscribe(Ulid),
    Unsubscribe(Ulid),
    UnsubscribeAll,
}

pub struct DeltaTHandler {
    tenant_manager: Arc<TenantManager>,
    query_parser: Arc<DeltaTQueryParser>,
    subscribe_tx: Option<mpsc::UnboundedSender<SubscriptionCommand>>,
}

impl DeltaTHandler {
    #[allow(dead_code)]
    pub fn new(tenant_manager: Arc<TenantManager>) -> Self {
        Self {
            tenant_manager,
            query_parser: Arc::new(DeltaTQueryParser),
            subscribe_tx: None,
        }
    }

    pub fn with_subscriptions(
        tenant_manager: Arc<TenantManager>,
        subscribe_tx: mpsc::UnboundedSender<SubscriptionCommand>,
    ) -> Self {
        Self {
            tenant_manager,
            query_parser: Arc::new(DeltaTQueryParser),
            subscribe_tx: Some(subscribe_tx),
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

    fn parse_channel_resource_id(channel: &str) -> PgWireResult<Ulid> {
        let resource_id_str = channel.strip_prefix("resource_").ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".into(),
                "42000".into(),
                format!("invalid channel: {channel} (expected resource_{{id}})"),
            )))
        })?;
        Ulid::from_string(resource_id_str).map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".into(),
                "42000".into(),
                format!("bad ULID in channel: {e}"),
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
                name,
                capacity,
                buffer_after,
            } => {
                engine
                    .create_resource(id, parent_id, name, capacity, buffer_after)
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
                label,
            } => {
                engine
                    .confirm_booking(id, resource_id, Span::new(start, end), label)
                    .await
                    .map_err(engine_err)?;
                Ok(vec![Response::Execution(Tag::new("INSERT").with_rows(1))])
            }
            Command::BatchInsertBookings { bookings } => {
                let count = bookings.len();
                let batch: Vec<_> = bookings
                    .into_iter()
                    .map(|(id, resource_id, start, end, label)| (id, resource_id, Span::new(start, end), label))
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
            Command::SelectMultiAvailability {
                resource_ids,
                start,
                end,
                min_available,
                min_duration,
            } => {
                let slots = engine
                    .compute_multi_availability(&resource_ids, start, end, min_available, min_duration)
                    .await
                    .map_err(engine_err)?;

                let schema = Arc::new(multi_availability_schema());

                let rows: Vec<PgWireResult<_>> = slots
                    .into_iter()
                    .map(|slot| {
                        let mut encoder = DataRowEncoder::new(schema.clone());
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
            Command::UpdateResource { id, name, capacity, buffer_after } => {
                engine
                    .update_resource(id, name, capacity, buffer_after)
                    .await
                    .map_err(engine_err)?;
                Ok(vec![Response::Execution(Tag::new("UPDATE").with_rows(1))])
            }
            Command::UpdateRule { id, start, end, blocking } => {
                engine
                    .update_rule(id, Span::new(start, end), blocking)
                    .await
                    .map_err(engine_err)?;
                Ok(vec![Response::Execution(Tag::new("UPDATE").with_rows(1))])
            }
            Command::SelectResources { parent_id } => {
                let all = engine.list_resources();
                let filtered: Vec<_> = match parent_id {
                    None => all,
                    Some(None) => all.into_iter().filter(|r| r.parent_id.is_none()).collect(),
                    Some(Some(pid)) => all.into_iter().filter(|r| r.parent_id == Some(pid)).collect(),
                };

                let schema = Arc::new(resources_schema());
                let rows: Vec<PgWireResult<_>> = filtered
                    .into_iter()
                    .map(|r| {
                        let mut encoder = DataRowEncoder::new(schema.clone());
                        encoder.encode_field(&r.id.to_string())?;
                        encoder.encode_field(&r.parent_id.map(|p| p.to_string()))?;
                        encoder.encode_field(&r.name)?;
                        encoder.encode_field(&(r.capacity as i64))?;
                        encoder.encode_field(&r.buffer_after)?;
                        Ok(encoder.take_row())
                    })
                    .collect();
                Ok(vec![Response::Query(QueryResponse::new(schema, stream::iter(rows)))])
            }
            Command::SelectRules { resource_id } => {
                let rules = engine.get_rules(resource_id).await.map_err(engine_err)?;
                let schema = Arc::new(rules_schema());
                let rows: Vec<PgWireResult<_>> = rules
                    .into_iter()
                    .map(|r| {
                        let mut encoder = DataRowEncoder::new(schema.clone());
                        encoder.encode_field(&r.id.to_string())?;
                        encoder.encode_field(&r.resource_id.to_string())?;
                        encoder.encode_field(&r.start)?;
                        encoder.encode_field(&r.end)?;
                        encoder.encode_field(&r.blocking)?;
                        Ok(encoder.take_row())
                    })
                    .collect();
                Ok(vec![Response::Query(QueryResponse::new(schema, stream::iter(rows)))])
            }
            Command::SelectBookings { resource_id } => {
                let bookings = engine.get_bookings(resource_id).await.map_err(engine_err)?;
                let schema = Arc::new(bookings_schema());
                let rows: Vec<PgWireResult<_>> = bookings
                    .into_iter()
                    .map(|b| {
                        let mut encoder = DataRowEncoder::new(schema.clone());
                        encoder.encode_field(&b.id.to_string())?;
                        encoder.encode_field(&b.resource_id.to_string())?;
                        encoder.encode_field(&b.start)?;
                        encoder.encode_field(&b.end)?;
                        encoder.encode_field(&b.label)?;
                        Ok(encoder.take_row())
                    })
                    .collect();
                Ok(vec![Response::Query(QueryResponse::new(schema, stream::iter(rows)))])
            }
            Command::SelectHolds { resource_id } => {
                let holds = engine.get_holds(resource_id).await.map_err(engine_err)?;
                let schema = Arc::new(holds_schema());
                let rows: Vec<PgWireResult<_>> = holds
                    .into_iter()
                    .map(|h| {
                        let mut encoder = DataRowEncoder::new(schema.clone());
                        encoder.encode_field(&h.id.to_string())?;
                        encoder.encode_field(&h.resource_id.to_string())?;
                        encoder.encode_field(&h.start)?;
                        encoder.encode_field(&h.end)?;
                        encoder.encode_field(&h.expires_at)?;
                        Ok(encoder.take_row())
                    })
                    .collect();
                Ok(vec![Response::Query(QueryResponse::new(schema, stream::iter(rows)))])
            }
            Command::Listen { channel } => {
                let resource_id = Self::parse_channel_resource_id(&channel)?;
                if let Some(ref tx) = self.subscribe_tx {
                    let _ = tx.send(SubscriptionCommand::Subscribe(resource_id));
                }
                Ok(vec![Response::Execution(Tag::new("LISTEN"))])
            }
            Command::Unlisten { channel } => {
                let resource_id = Self::parse_channel_resource_id(&channel)?;
                if let Some(ref tx) = self.subscribe_tx {
                    let _ = tx.send(SubscriptionCommand::Unsubscribe(resource_id));
                }
                Ok(vec![Response::Execution(Tag::new("UNLISTEN"))])
            }
            Command::UnlistenAll => {
                if let Some(ref tx) = self.subscribe_tx {
                    let _ = tx.send(SubscriptionCommand::UnsubscribeAll);
                }
                Ok(vec![Response::Execution(Tag::new("UNLISTEN"))])
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

fn multi_availability_schema() -> Vec<FieldInfo> {
    vec![
        FieldInfo::new("start".into(), None, None, Type::INT8, FieldFormat::Text),
        FieldInfo::new("end".into(), None, None, Type::INT8, FieldFormat::Text),
    ]
}

fn resources_schema() -> Vec<FieldInfo> {
    vec![
        FieldInfo::new("id".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("parent_id".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("name".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("capacity".into(), None, None, Type::INT8, FieldFormat::Text),
        FieldInfo::new("buffer_after".into(), None, None, Type::INT8, FieldFormat::Text),
    ]
}

fn rules_schema() -> Vec<FieldInfo> {
    vec![
        FieldInfo::new("id".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("resource_id".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("start".into(), None, None, Type::INT8, FieldFormat::Text),
        FieldInfo::new("end".into(), None, None, Type::INT8, FieldFormat::Text),
        FieldInfo::new("blocking".into(), None, None, Type::BOOL, FieldFormat::Text),
    ]
}

fn bookings_schema() -> Vec<FieldInfo> {
    vec![
        FieldInfo::new("id".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("resource_id".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("start".into(), None, None, Type::INT8, FieldFormat::Text),
        FieldInfo::new("end".into(), None, None, Type::INT8, FieldFormat::Text),
        FieldInfo::new("label".into(), None, None, Type::VARCHAR, FieldFormat::Text),
    ]
}

fn holds_schema() -> Vec<FieldInfo> {
    vec![
        FieldInfo::new("id".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("resource_id".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("start".into(), None, None, Type::INT8, FieldFormat::Text),
        FieldInfo::new("end".into(), None, None, Type::INT8, FieldFormat::Text),
        FieldInfo::new("expires_at".into(), None, None, Type::INT8, FieldFormat::Text),
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
        if query.len() > MAX_QUERY_LEN {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".into(),
                "54000".into(),
                "query too long".into(),
            ))));
        }
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
        Ok(schema_for_sql(stmt))
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
        if sql.len() > MAX_QUERY_LEN {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".into(),
                "54000".into(),
                "query too long".into(),
            ))));
        }
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
        Ok(DescribeStatementResponse::new(param_types, schema_for_sql(&target.statement)))
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
        Ok(DescribePortalResponse::new(schema_for_sql(&target.statement.statement)))
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

// ── Custom connection loop with LISTEN/NOTIFY ────────────────────

pub async fn process_connection(
    tcp_socket: TcpStream,
    tenant_manager: Arc<TenantManager>,
    password: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1. Negotiate TLS (no TLS acceptor for now)
    let mut socket: Framed<
        pgwire::tokio::server::MaybeTls,
        pgwire::tokio::server::PgWireMessageServerCodec<String>,
    > = match pgwire::tokio::server::negotiate_tls::<String>(tcp_socket, None).await? {
        Some(s) => s,
        None => return Ok(()),
    };

    // 2. Per-connection channels
    let (subscribe_tx, mut subscribe_rx) = mpsc::unbounded_channel::<SubscriptionCommand>();
    let (notify_tx, mut notify_rx) = mpsc::unbounded_channel::<NotificationResponse>();

    // 3. Per-connection handlers
    let auth_handler = Arc::new(CleartextPasswordAuthStartupHandler::new(
        DeltaTAuthSource::new(password),
        DefaultServerParameterProvider::default(),
    ));
    let handler = Arc::new(DeltaTHandler::with_subscriptions(
        tenant_manager.clone(),
        subscribe_tx,
    ));
    let noop = Arc::new(NoopHandler);

    // 4. Forwarder tasks state
    let mut forwarders: HashMap<Ulid, JoinHandle<()>> = HashMap::new();
    let startup_deadline = tokio::time::sleep(Duration::from_secs(60));
    tokio::pin!(startup_deadline);

    // 5. Main loop
    loop {
        let in_startup = matches!(
            socket.state(),
            PgWireConnectionState::AwaitingStartup
                | PgWireConnectionState::AuthenticationInProgress
        );

        if in_startup {
            let msg = tokio::select! {
                _ = &mut startup_deadline => break,
                msg = socket.next() => msg,
            };
            match msg {
                Some(Ok(msg)) => {
                    if let Err(e) = pgwire::tokio::server::process_message(
                        msg,
                        &mut socket,
                        auth_handler.clone(),
                        handler.clone(),
                        handler.clone(),
                        noop.clone(),
                        noop.clone(),
                    )
                    .await
                    {
                        tracing::debug!("startup error: {e}");
                        break;
                    }
                }
                _ => break,
            }
        } else {
            enum Action<M, E> {
                Message(Option<Result<M, E>>),
                Subscribe(Option<SubscriptionCommand>),
                Notify(Option<NotificationResponse>),
            }

            let action = tokio::select! {
                msg = socket.next() => Action::Message(msg),
                cmd = subscribe_rx.recv() => Action::Subscribe(cmd),
                notif = notify_rx.recv() => Action::Notify(notif),
            };

            match action {
                Action::Message(Some(Ok(msg))) => {
                    let is_extended = match socket.state() {
                        PgWireConnectionState::CopyInProgress(ext) => ext,
                        _ => msg.is_extended_query(),
                    };
                    if let Err(e) = pgwire::tokio::server::process_message(
                        msg,
                        &mut socket,
                        auth_handler.clone(),
                        handler.clone(),
                        handler.clone(),
                        noop.clone(),
                        noop.clone(),
                    )
                    .await
                    {
                        if pgwire::tokio::server::process_error(
                            &mut socket,
                            e,
                            is_extended,
                        )
                        .await
                        .is_err()
                        {
                            break;
                        }
                    }
                }
                Action::Message(_) => break,
                Action::Subscribe(Some(cmd)) => {
                    match cmd {
                        SubscriptionCommand::Subscribe(rid) => {
                            if forwarders.contains_key(&rid) {
                                continue; // already subscribed
                            }
                            if forwarders.len() >= MAX_SUBSCRIPTIONS_PER_CONNECTION {
                                continue; // limit reached
                            }
                            // Resolve the engine to get the notify hub
                            let engine = match handler.resolve_engine(&socket) {
                                Ok(e) => e,
                                Err(_) => continue,
                            };
                            let mut rx = engine.notify.subscribe(rid);
                            let tx = notify_tx.clone();
                            let channel = format!("resource_{rid}");
                            forwarders.insert(
                                rid,
                                tokio::spawn(async move {
                                    while let Ok(event) = rx.recv().await {
                                        let payload =
                                            serde_json::to_string(&event).unwrap_or_default();
                                        let notif =
                                            NotificationResponse::new(0, channel.clone(), payload);
                                        if tx.send(notif).is_err() {
                                            break;
                                        }
                                    }
                                }),
                            );
                        }
                        SubscriptionCommand::Unsubscribe(rid) => {
                            if let Some(handle) = forwarders.remove(&rid) {
                                handle.abort();
                            }
                        }
                        SubscriptionCommand::UnsubscribeAll => {
                            for (_, handle) in forwarders.drain() {
                                handle.abort();
                            }
                        }
                    }
                }
                Action::Subscribe(None) => {}
                Action::Notify(Some(notif)) => {
                    let msg: PgWireBackendMessage =
                        PgWireBackendMessage::NotificationResponse(notif);
                    if SinkExt::send(&mut socket, msg).await.is_err() {
                        break;
                    }
                }
                Action::Notify(None) => {}
            }
        }
    }

    // Cleanup: abort all forwarder tasks
    for (_, handle) in forwarders {
        handle.abort();
    }
    Ok(())
}

fn schema_for_sql(sql: &str) -> Vec<FieldInfo> {
    let upper = sql.to_uppercase();
    if !upper.contains("SELECT") {
        return vec![];
    }
    if upper.contains("AVAILABILITY") {
        if upper.contains(" IN ") {
            multi_availability_schema()
        } else {
            availability_schema()
        }
    } else if upper.contains("RESOURCES") {
        resources_schema()
    } else if upper.contains("RULES") {
        rules_schema()
    } else if upper.contains("BOOKINGS") {
        bookings_schema()
    } else if upper.contains("HOLDS") {
        holds_schema()
    } else {
        vec![]
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

#[cfg(test)]
mod tests {
    use super::*;

    // ── count_params ─────────────────────────────────────────────

    #[test]
    fn count_params_none() {
        assert_eq!(count_params("SELECT * FROM resources"), 0);
    }

    #[test]
    fn count_params_single() {
        assert_eq!(count_params("SELECT * FROM resources WHERE id = $1"), 1);
    }

    #[test]
    fn count_params_multiple() {
        assert_eq!(
            count_params("INSERT INTO rules (id, resource_id, start, \"end\", blocking) VALUES ($1, $2, $3, $4, $5)"),
            5
        );
    }

    #[test]
    fn count_params_out_of_order() {
        assert_eq!(count_params("SELECT $3, $1, $2"), 3);
    }

    #[test]
    fn count_params_double_digit() {
        assert_eq!(count_params("SELECT $10, $1"), 10);
    }

    #[test]
    fn count_params_dollar_no_digit() {
        // "$" followed by non-digit should not count
        assert_eq!(count_params("SELECT $foo"), 0);
    }

    // ── schema_for_sql ───────────────────────────────────────────

    #[test]
    fn schema_for_select_availability() {
        let schema = schema_for_sql("SELECT * FROM availability WHERE resource_id = $1");
        assert_eq!(schema.len(), 3);
        assert_eq!(schema[0].name(), "resource_id");
    }

    #[test]
    fn schema_for_select_multi_availability() {
        let schema = schema_for_sql("SELECT * FROM availability WHERE resource_id IN ($1, $2)");
        assert_eq!(schema.len(), 2); // multi-availability has just start, end
        assert_eq!(schema[0].name(), "start");
    }

    #[test]
    fn schema_for_select_resources() {
        let schema = schema_for_sql("SELECT * FROM resources");
        assert_eq!(schema.len(), 5);
        assert_eq!(schema[0].name(), "id");
        assert_eq!(schema[2].name(), "name");
    }

    #[test]
    fn schema_for_select_rules() {
        let schema = schema_for_sql("SELECT * FROM rules WHERE resource_id = $1");
        assert_eq!(schema.len(), 5);
        assert_eq!(schema[4].name(), "blocking");
    }

    #[test]
    fn schema_for_select_bookings() {
        let schema = schema_for_sql("SELECT * FROM bookings WHERE resource_id = $1");
        assert_eq!(schema.len(), 5);
        assert_eq!(schema[4].name(), "label");
    }

    #[test]
    fn schema_for_select_holds() {
        let schema = schema_for_sql("SELECT * FROM holds WHERE resource_id = $1");
        assert_eq!(schema.len(), 5);
        assert_eq!(schema[4].name(), "expires_at");
    }

    #[test]
    fn schema_for_insert_returns_empty() {
        let schema = schema_for_sql("INSERT INTO resources (id) VALUES ($1)");
        assert!(schema.is_empty());
    }

    #[test]
    fn schema_for_delete_returns_empty() {
        let schema = schema_for_sql("DELETE FROM resources WHERE id = $1");
        assert!(schema.is_empty());
    }

    // ── substitute_params ────────────────────────────────────────

    fn make_portal(sql: &str, params: Vec<Option<bytes::Bytes>>) -> Portal<String> {
        let stored = Arc::new(StoredStatement::new(
            String::new(),
            sql.to_string(),
            vec![None; params.len()],
        ));
        let mut portal = Portal::<String>::default();
        portal.statement = stored;
        portal.parameters = params;
        portal
    }

    #[test]
    fn substitute_basic() {
        let portal = make_portal(
            "SELECT * FROM resources WHERE id = $1",
            vec![Some(bytes::Bytes::from_static(b"01ARZ3NDEKTSV4RRFFQ69G5FAV"))],
        );
        let result = substitute_params(&portal);
        assert_eq!(
            result,
            "SELECT * FROM resources WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'"
        );
    }

    #[test]
    fn substitute_null_param() {
        let portal = make_portal(
            "INSERT INTO resources (id, parent_id) VALUES ($1, $2)",
            vec![
                Some(bytes::Bytes::from_static(b"01ARZ3NDEKTSV4RRFFQ69G5FAV")),
                None,
            ],
        );
        let result = substitute_params(&portal);
        assert!(result.contains("NULL"));
        assert!(result.contains("'01ARZ3NDEKTSV4RRFFQ69G5FAV'"));
    }

    #[test]
    fn substitute_escapes_quotes() {
        let portal = make_portal(
            "INSERT INTO resources (id, parent_id, name) VALUES ($1, NULL, $2)",
            vec![
                Some(bytes::Bytes::from_static(b"01ARZ3NDEKTSV4RRFFQ69G5FAV")),
                Some(bytes::Bytes::from_static(b"O'Brien's Room")),
            ],
        );
        let result = substitute_params(&portal);
        assert!(result.contains("'O''Brien''s Room'"));
    }

    #[test]
    fn substitute_multiple_params() {
        let portal = make_portal(
            "INSERT INTO rules (id, resource_id, start, \"end\", blocking) VALUES ($1, $2, $3, $4, $5)",
            vec![
                Some(bytes::Bytes::from_static(b"RULE_ID")),
                Some(bytes::Bytes::from_static(b"RES_ID")),
                Some(bytes::Bytes::from_static(b"1000")),
                Some(bytes::Bytes::from_static(b"2000")),
                Some(bytes::Bytes::from_static(b"false")),
            ],
        );
        let result = substitute_params(&portal);
        assert!(result.contains("'RULE_ID'"));
        assert!(result.contains("'RES_ID'"));
        assert!(result.contains("'1000'"));
        assert!(result.contains("'2000'"));
        assert!(result.contains("'false'"));
    }

    // ── parse_channel_resource_id ───────────────────────────────

    #[test]
    fn parse_channel_valid() {
        let ulid = Ulid::new();
        let channel = format!("resource_{ulid}");
        let result = DeltaTHandler::parse_channel_resource_id(&channel).unwrap();
        assert_eq!(result, ulid);
    }

    #[test]
    fn parse_channel_missing_prefix() {
        let result = DeltaTHandler::parse_channel_resource_id("foobar_01ARZ3NDEKTSV4RRFFQ69G5FAV");
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("invalid channel"));
    }

    #[test]
    fn parse_channel_bad_ulid() {
        let result = DeltaTHandler::parse_channel_resource_id("resource_notaulid");
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("bad ULID"));
    }

    #[test]
    fn parse_channel_empty_after_prefix() {
        let result = DeltaTHandler::parse_channel_resource_id("resource_");
        assert!(result.is_err());
    }

    // ── execute_command: Listen / Unlisten / UnlistenAll ─────────

    use crate::notify::NotifyHub;
    use std::path::PathBuf;

    fn test_wal_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("deltat_test_wire");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        let _ = std::fs::remove_file(&path);
        path
    }

    fn setup_handler_with_subs() -> (
        DeltaTHandler,
        mpsc::UnboundedReceiver<SubscriptionCommand>,
        Arc<Engine>,
    ) {
        let notify = Arc::new(NotifyHub::new());
        let path = test_wal_path(&format!("wire_{}.wal", Ulid::new()));
        let engine = Arc::new(Engine::new(path, notify).unwrap());

        let tm = Arc::new(TenantManager::new(
            std::env::temp_dir().join("deltat_test_wire_tm"),
            1000,
        ));

        let (tx, rx) = mpsc::unbounded_channel();
        let handler = DeltaTHandler::with_subscriptions(tm, tx);
        (handler, rx, engine)
    }

    #[tokio::test]
    async fn execute_listen_sends_subscribe() {
        let (handler, mut rx, engine) = setup_handler_with_subs();
        let rid = Ulid::new();
        let channel = format!("resource_{rid}");
        let cmd = Command::Listen { channel };
        let responses = handler.execute_command(&engine, cmd).await.unwrap();
        assert_eq!(responses.len(), 1);

        let sub_cmd = rx.try_recv().unwrap();
        match sub_cmd {
            SubscriptionCommand::Subscribe(id) => assert_eq!(id, rid),
            _ => panic!("expected Subscribe"),
        }
    }

    #[tokio::test]
    async fn execute_listen_invalid_channel() {
        let (handler, _rx, engine) = setup_handler_with_subs();
        let cmd = Command::Listen {
            channel: "bad_channel".into(),
        };
        let result = handler.execute_command(&engine, cmd).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn execute_listen_without_subscribe_tx() {
        let tm = Arc::new(TenantManager::new(
            std::env::temp_dir().join("deltat_test_wire_no_sub"),
            1000,
        ));
        let handler = DeltaTHandler::new(tm);

        let notify = Arc::new(NotifyHub::new());
        let path = test_wal_path(&format!("wire_nosub_{}.wal", Ulid::new()));
        let engine = Arc::new(Engine::new(path, notify).unwrap());

        let rid = Ulid::new();
        let channel = format!("resource_{rid}");
        let cmd = Command::Listen { channel };
        let responses = handler.execute_command(&engine, cmd).await.unwrap();
        assert_eq!(responses.len(), 1);
        // No subscribe_tx, so no command sent — just returns LISTEN tag
    }

    #[tokio::test]
    async fn execute_unlisten_sends_unsubscribe() {
        let (handler, mut rx, engine) = setup_handler_with_subs();
        let rid = Ulid::new();
        let channel = format!("resource_{rid}");
        let cmd = Command::Unlisten { channel };
        let responses = handler.execute_command(&engine, cmd).await.unwrap();
        assert_eq!(responses.len(), 1);

        let sub_cmd = rx.try_recv().unwrap();
        match sub_cmd {
            SubscriptionCommand::Unsubscribe(id) => assert_eq!(id, rid),
            _ => panic!("expected Unsubscribe"),
        }
    }

    #[tokio::test]
    async fn execute_unlisten_all_sends_command() {
        let (handler, mut rx, engine) = setup_handler_with_subs();
        let cmd = Command::UnlistenAll;
        let responses = handler.execute_command(&engine, cmd).await.unwrap();
        assert_eq!(responses.len(), 1);

        let sub_cmd = rx.try_recv().unwrap();
        assert!(matches!(sub_cmd, SubscriptionCommand::UnsubscribeAll));
    }
}
