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
    CopyResponse, DescribePortalResponse, DescribeResponse, DescribeStatementResponse,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::ClientInfo;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};
use pgwire::messages::PgWireBackendMessage;

pub struct NoopHandler;

// Accept all connections without authentication.
impl NoopStartupHandler for NoopHandler {}

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
            vec![Response::CopyIn(CopyResponse::new(
                0,
                0,
                stream::empty::<PgWireResult<CopyData>>(),
            ))]
        } else {
            // COPY TO STDOUT — return empty stream
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
        _stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(DescribeStatementResponse::no_data())
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        _portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(DescribePortalResponse::no_data())
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
