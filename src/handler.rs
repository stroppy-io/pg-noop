use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use futures::Sink;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::CopyHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    CopyResponse, DescribePortalResponse, DescribeResponse, DescribeStatementResponse, FieldFormat,
    FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};
use pgwire::messages::PgWireBackendMessage;
use postgres_types::Kind;

pub struct NoopHandler;

// Accept all connections without authentication.
impl NoopStartupHandler for NoopHandler {}

/// Returns a `Type` with OID 0. When pgtype v5 sees OID 0 it falls back to
/// Go-type based codec lookup (TypeForValue), so int64, string, float64, etc.
/// all encode correctly without us knowing the actual schema.
fn oid_zero() -> Type {
    Type::new("?".to_owned(), 0, Kind::Simple, "pg_catalog".to_owned())
}

/// Counts SELECT columns at paren-depth 0 between SELECT and FROM.
/// Works reliably for pgx's `select "c1", "c2" from "t"` pattern.
fn count_select_columns(sql: &str) -> usize {
    let upper = sql.trim().to_ascii_uppercase();
    let body = match upper.strip_prefix("SELECT ") {
        Some(rest) => rest,
        None => return 1,
    };
    // Find " FROM " outside parens
    let mut depth: u32 = 0;
    let bytes = body.as_bytes();
    let mut from_at = body.len();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            b' ' if depth == 0 => {
                if body[i..].starts_with(" FROM ") {
                    from_at = i;
                    break;
                }
            }
            _ => {}
        }
        i += 1;
    }
    let select_list = &body[..from_at];
    // Count commas at depth 0
    depth = 0;
    let mut cols: usize = 1;
    for &b in select_list.as_bytes() {
        match b {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => cols += 1,
            _ => {}
        }
    }
    cols
}

/// Counts columns in `COPY table (col1, col2) FROM STDIN` by finding the
/// paren-group between the table name and FROM.
fn count_copy_columns(sql: &str) -> usize {
    let upper = sql.trim().to_ascii_uppercase();
    // Take the part before " FROM "
    let before_from = match upper.find(" FROM ") {
        Some(i) => &sql[..i],
        None => return 1,
    };
    // Find the last '(' — that's the column list
    let open = match before_from.rfind('(') {
        Some(i) => i,
        None => return 1, // no explicit column list
    };
    let after_open = &before_from[open + 1..];
    let close = match after_open.find(')') {
        Some(i) => i,
        None => return 1,
    };
    let cols_str = after_open[..close].trim();
    if cols_str.is_empty() {
        return 1;
    }
    cols_str.bytes().filter(|&b| b == b',').count() + 1
}

fn copy_is_binary(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    upper.contains("FORMAT BINARY") || upper.ends_with(" BINARY") || upper.ends_with(" BINARY;")
}

/// Builds N dummy FieldInfo entries with OID 0 so drivers can resolve
/// encoders by their own Go/native type rather than by a schema OID.
fn dummy_fields(n: usize) -> Vec<FieldInfo> {
    let t = oid_zero();
    (0..n)
        .map(|_| FieldInfo::new("?column?".into(), None, None, t.clone(), FieldFormat::Text))
        .collect()
}

fn classify_simple(sql: &str) -> Vec<Response> {
    let upper = sql.trim().to_ascii_uppercase();
    if upper.starts_with("SELECT")
        || upper.starts_with("WITH")
        || upper.starts_with("TABLE")
        || upper.starts_with("VALUES")
    {
        vec![Response::Query(QueryResponse::new(
            Arc::new(vec![]),
            stream::empty(),
        ))]
    } else if upper.starts_with("INSERT") {
        vec![Response::Execution(Tag::new("INSERT").with_oid(0).with_rows(0))]
    } else if upper.starts_with("UPDATE") {
        vec![Response::Execution(Tag::new("UPDATE").with_rows(0))]
    } else if upper.starts_with("DELETE") {
        vec![Response::Execution(Tag::new("DELETE").with_rows(0))]
    } else if upper.starts_with("BEGIN") {
        vec![Response::TransactionStart(Tag::new("BEGIN"))]
    } else if upper.starts_with("COMMIT") {
        vec![Response::TransactionEnd(Tag::new("COMMIT"))]
    } else if upper.starts_with("ROLLBACK") {
        vec![Response::TransactionEnd(Tag::new("ROLLBACK"))]
    } else if upper.starts_with("COPY") {
        if upper.contains("FROM STDIN") {
            let cols = count_copy_columns(sql);
            let fmt: i8 = if copy_is_binary(sql) { 1 } else { 0 };
            vec![Response::CopyIn(CopyResponse::new(
                fmt,
                cols,
                stream::empty::<PgWireResult<CopyData>>(),
            ))]
        } else {
            vec![Response::CopyOut(CopyResponse::new(
                0,
                0,
                stream::empty::<PgWireResult<CopyData>>(),
            ))]
        }
    } else {
        vec![Response::Execution(Tag::new("OK"))]
    }
}

fn classify_extended(sql: &str) -> Response {
    let upper = sql.trim().to_ascii_uppercase();
    if upper.starts_with("SELECT")
        || upper.starts_with("WITH")
        || upper.starts_with("TABLE")
        || upper.starts_with("VALUES")
    {
        Response::Execution(Tag::new("SELECT").with_rows(0))
    } else if upper.starts_with("INSERT") {
        Response::Execution(Tag::new("INSERT").with_oid(0).with_rows(0))
    } else if upper.starts_with("UPDATE") {
        Response::Execution(Tag::new("UPDATE").with_rows(0))
    } else if upper.starts_with("DELETE") {
        Response::Execution(Tag::new("DELETE").with_rows(0))
    } else if upper.starts_with("BEGIN") {
        Response::TransactionStart(Tag::new("BEGIN"))
    } else if upper.starts_with("COMMIT") {
        Response::TransactionEnd(Tag::new("COMMIT"))
    } else if upper.starts_with("ROLLBACK") {
        Response::TransactionEnd(Tag::new("ROLLBACK"))
    } else {
        Response::Execution(Tag::new("OK"))
    }
}

#[async_trait]
impl SimpleQueryHandler for NoopHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(classify_simple(query))
    }
}

#[async_trait]
impl ExtendedQueryHandler for NoopHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser)
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(classify_extended(&portal.statement.statement))
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let upper = stmt.statement.trim().to_ascii_uppercase();
        if upper.starts_with("SELECT") || upper.starts_with("WITH") {
            let n = count_select_columns(&stmt.statement);
            Ok(DescribeStatementResponse::new(vec![], dummy_fields(n)))
        } else {
            Ok(DescribeStatementResponse::no_data())
        }
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let upper = portal.statement.statement.trim().to_ascii_uppercase();
        if upper.starts_with("SELECT") || upper.starts_with("WITH") {
            let n = count_select_columns(&portal.statement.statement);
            Ok(DescribePortalResponse::new(dummy_fields(n)))
        } else {
            Ok(DescribePortalResponse::no_data())
        }
    }
}

#[async_trait]
impl CopyHandler for NoopHandler {
    async fn on_copy_data<C>(&self, _client: &mut C, _data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(())
    }

    async fn on_copy_done<C>(&self, _client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(())
    }

    async fn on_copy_fail<C>(&self, _client: &mut C, fail: CopyFail) -> PgWireError
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "XX000".to_owned(),
            format!("COPY failed: {}", fail.message),
        )))
    }
}
