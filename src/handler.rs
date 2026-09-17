use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use futures::Sink;
use futures::{future, stream};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::CopyHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    CopyResponse, DataRowEncoder, DescribePortalResponse, DescribeResponse,
    DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};
use pgwire::messages::PgWireBackendMessage;

pub struct NoopHandler {
    catalog: RwLock<SchemaCatalog>,
}

impl NoopHandler {
    pub fn new() -> Self {
        Self {
            catalog: RwLock::new(SchemaCatalog::default()),
        }
    }

    fn fields_for_metadata_select(&self, sql: &str) -> Option<Arc<Vec<FieldInfo>>> {
        let (table_name, selected_columns) = parse_metadata_select(sql)?;
        let catalog = self.catalog.read().expect("schema catalog poisoned");
        catalog.fields_for_select(&table_name, &selected_columns)
    }

    fn apply_schema_change(&self, sql: &str) {
        if let Some((table_name, columns)) = parse_create_table(sql) {
            let mut catalog = self.catalog.write().expect("schema catalog poisoned");
            catalog.insert_table(&table_name, columns);
            return;
        }

        if let Some(table_names) = parse_drop_table(sql) {
            let mut catalog = self.catalog.write().expect("schema catalog poisoned");
            for table_name in table_names {
                catalog.drop_table(&table_name);
            }
        }
    }
}

impl Default for NoopHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
struct SchemaCatalog {
    tables: HashMap<String, Arc<TableSchema>>,
}

struct TableSchema {
    columns: Vec<ColumnInfo>,
}

struct ColumnInfo {
    name: String,
    normalized_name: String,
    typ: Type,
}

impl SchemaCatalog {
    fn insert_table(&mut self, table_name: &[String], columns: Vec<ColumnInfo>) {
        let table = Arc::new(TableSchema { columns });
        for key in table_keys(table_name) {
            self.tables.insert(key, Arc::clone(&table));
        }
    }

    fn drop_table(&mut self, table_name: &[String]) {
        for key in table_keys(table_name) {
            self.tables.remove(&key);
        }

        if table_name.len() == 1 {
            let suffix = format!(".{}", normalize_ident(&table_name[0]));
            self.tables.retain(|key, _| !key.ends_with(&suffix));
        }
    }

    fn fields_for_select(
        &self,
        table_name: &[String],
        selected_columns: &[SelectColumn],
    ) -> Option<Arc<Vec<FieldInfo>>> {
        let table = self.lookup_table(table_name)?;
        let fields: Vec<FieldInfo> = if selected_columns.len() == 1 && selected_columns[0].is_star {
            table
                .columns
                .iter()
                .map(|column| {
                    FieldInfo::new(
                        column.name.clone(),
                        None,
                        None,
                        column.typ.clone(),
                        FieldFormat::Text,
                    )
                })
                .collect()
        } else {
            selected_columns
                .iter()
                .map(|selected| {
                    let selected_name = selected
                        .name
                        .as_deref()
                        .expect("non-star select column has a name");
                    let column = table
                        .columns
                        .iter()
                        .find(|column| column.normalized_name == normalize_ident(selected_name));
                    FieldInfo::new(
                        column
                            .map(|column| column.name.clone())
                            .unwrap_or_else(|| selected_name.to_string()),
                        None,
                        None,
                        column
                            .map(|column| column.typ.clone())
                            .unwrap_or(Type::TEXT),
                        FieldFormat::Text,
                    )
                })
                .collect()
        };

        Some(Arc::new(fields))
    }

    fn lookup_table(&self, table_name: &[String]) -> Option<&TableSchema> {
        for key in table_lookup_keys(table_name) {
            if let Some(table) = self.tables.get(&key) {
                return Some(table.as_ref());
            }
        }
        None
    }
}

// Accept all connections without authentication.
impl NoopStartupHandler for NoopHandler {}

/// Builds N FieldInfo entries typed as INT8 (text format). Matches the stroppy
/// noop driver which returns int64(1) for every column, giving workloads a
/// non-null, non-zero value so null-row checks and counting guards execute
/// without errors.
fn stub_fields(n: usize) -> Arc<Vec<FieldInfo>> {
    Arc::new(
        (0..n)
            .map(|_| FieldInfo::new("?column?".into(), None, None, Type::INT8, FieldFormat::Text))
            .collect(),
    )
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
            b' ' if depth == 0 && body[i..].starts_with(" FROM ") => {
                from_at = i;
                break;
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

#[derive(Debug)]
struct SelectColumn {
    name: Option<String>,
    is_star: bool,
}

#[derive(Debug)]
struct ParsedIdent {
    name: String,
    quoted: bool,
}

fn parse_create_table(sql: &str) -> Option<(Vec<String>, Vec<ColumnInfo>)> {
    let mut pos = skip_ws(sql, 0);
    pos = consume_keyword(sql, pos, "CREATE")?;

    if let Some(next) = consume_keyword(sql, pos, "UNLOGGED") {
        pos = next;
    } else if let Some(next) = consume_keyword(sql, pos, "TEMPORARY") {
        pos = next;
    } else if let Some(next) = consume_keyword(sql, pos, "TEMP") {
        pos = next;
    }

    pos = consume_keyword(sql, pos, "TABLE")?;

    if let Some(if_pos) = consume_keyword(sql, pos, "IF") {
        let not_pos = consume_keyword(sql, if_pos, "NOT")?;
        pos = consume_keyword(sql, not_pos, "EXISTS")?;
    }

    let (table_name, after_name) = parse_qualified_name(sql, pos)?;
    pos = skip_ws(sql, after_name);
    if sql.as_bytes().get(pos) != Some(&b'(') {
        return None;
    }

    let close = find_matching_paren(sql, pos)?;
    let columns = parse_column_defs(&sql[pos + 1..close]);
    if columns.is_empty() {
        return None;
    }

    Some((table_name, columns))
}

fn parse_drop_table(sql: &str) -> Option<Vec<Vec<String>>> {
    let mut pos = skip_ws(sql, 0);
    pos = consume_keyword(sql, pos, "DROP")?;
    pos = consume_keyword(sql, pos, "TABLE")?;

    if let Some(if_pos) = consume_keyword(sql, pos, "IF") {
        pos = consume_keyword(sql, if_pos, "EXISTS")?;
    }

    let names = sql[pos..].trim().trim_end_matches(';').trim();
    if names.is_empty() {
        return None;
    }

    let mut tables = Vec::new();
    for part in split_top_level(names, ',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        let (table_name, after_name) = parse_qualified_name(part, 0)?;
        let rest = part[after_name..].trim();
        if rest.is_empty()
            || rest.eq_ignore_ascii_case("CASCADE")
            || rest.eq_ignore_ascii_case("RESTRICT")
        {
            tables.push(table_name);
        }
    }

    if tables.is_empty() {
        None
    } else {
        Some(tables)
    }
}

fn parse_column_defs(defs: &str) -> Vec<ColumnInfo> {
    split_top_level(defs, ',')
        .into_iter()
        .filter_map(parse_column_def)
        .collect()
}

fn parse_column_def(def: &str) -> Option<ColumnInfo> {
    let def = def.trim();
    if def.is_empty() {
        return None;
    }

    let (ident, after_ident) = parse_identifier(def, 0)?;
    if !ident.quoted && is_table_constraint_keyword(&ident.name) {
        return None;
    }

    let type_sql = def[after_ident..].trim_start();
    if type_sql.is_empty() {
        return None;
    }

    Some(ColumnInfo {
        normalized_name: normalize_ident(&ident.name),
        name: ident.name,
        typ: map_sql_type(type_sql),
    })
}

fn is_table_constraint_keyword(word: &str) -> bool {
    matches!(
        word.to_ascii_uppercase().as_str(),
        "PRIMARY" | "FOREIGN" | "UNIQUE" | "CHECK" | "CONSTRAINT"
    )
}

fn map_sql_type(type_sql: &str) -> Type {
    let words = type_words(type_sql, 4);
    let first = words.first().map(String::as_str).unwrap_or("");
    let second = words.get(1).map(String::as_str).unwrap_or("");

    match first {
        "INTEGER" | "INT" | "INT4" => Type::INT4,
        "BIGINT" | "INT8" => Type::INT8,
        "SMALLINT" | "INT2" => Type::INT2,
        "CHAR" => Type::BPCHAR,
        "CHARACTER" if second == "VARYING" => Type::VARCHAR,
        "CHARACTER" => Type::BPCHAR,
        "VARCHAR" => Type::VARCHAR,
        "TEXT" => Type::TEXT,
        "DECIMAL" | "NUMERIC" => Type::NUMERIC,
        "DATE" => Type::DATE,
        "TIMESTAMP" => Type::TIMESTAMP,
        "BOOLEAN" | "BOOL" => Type::BOOL,
        _ => Type::TEXT,
    }
}

fn type_words(sql: &str, max_words: usize) -> Vec<String> {
    let mut words = Vec::new();
    let mut pos = 0;
    while words.len() < max_words {
        pos = skip_ws(sql, pos);
        if sql.as_bytes().get(pos) == Some(&b'(') {
            pos = match find_matching_paren(sql, pos) {
                Some(close) => close + 1,
                None => break,
            };
            continue;
        }

        let start = pos;
        while pos < sql.len() && is_ident_continue(sql.as_bytes()[pos]) {
            pos += 1;
        }

        if start == pos {
            break;
        }

        words.push(sql[start..pos].to_ascii_uppercase());
    }
    words
}

fn parse_metadata_select(sql: &str) -> Option<(Vec<String>, Vec<SelectColumn>)> {
    let sql = sql.trim().trim_end_matches(';').trim();
    let select_pos = consume_keyword(sql, 0, "SELECT")?;
    let from_pos = find_top_level_keyword(sql, select_pos, "FROM")?;
    let select_list = sql[select_pos..from_pos].trim();
    if select_list.is_empty() {
        return None;
    }

    let after_from = from_pos + "FROM".len();
    let (table_name, after_table) = parse_qualified_name(sql, after_from)?;
    if !sql[after_table..].trim().is_empty() {
        return None;
    }

    let columns = parse_select_columns(select_list)?;
    Some((table_name, columns))
}

fn parse_select_columns(select_list: &str) -> Option<Vec<SelectColumn>> {
    let mut columns = Vec::new();
    for item in split_top_level(select_list, ',') {
        let item = item.trim();
        if item == "*" {
            columns.push(SelectColumn {
                name: None,
                is_star: true,
            });
            continue;
        }

        let (name_parts, after_name) = parse_qualified_name(item, 0)?;
        if !item[after_name..].trim().is_empty() {
            return None;
        }

        columns.push(SelectColumn {
            name: name_parts.last().cloned(),
            is_star: false,
        });
    }

    if columns.is_empty() {
        None
    } else {
        Some(columns)
    }
}

fn parse_qualified_name(sql: &str, pos: usize) -> Option<(Vec<String>, usize)> {
    let (ident, mut pos) = parse_identifier(sql, pos)?;
    let mut parts = vec![ident.name];

    loop {
        pos = skip_ws(sql, pos);
        if sql.as_bytes().get(pos) != Some(&b'.') {
            break;
        }
        let (ident, next_pos) = parse_identifier(sql, pos + 1)?;
        parts.push(ident.name);
        pos = next_pos;
    }

    Some((parts, pos))
}

fn parse_identifier(sql: &str, pos: usize) -> Option<(ParsedIdent, usize)> {
    let mut pos = skip_ws(sql, pos);
    let bytes = sql.as_bytes();
    if bytes.get(pos) == Some(&b'"') {
        pos += 1;
        let mut value = String::new();
        let mut chunk_start = pos;
        while pos < bytes.len() {
            if bytes[pos] == b'"' {
                value.push_str(&sql[chunk_start..pos]);
                if bytes.get(pos + 1) == Some(&b'"') {
                    value.push('"');
                    pos += 2;
                    chunk_start = pos;
                    continue;
                }

                return Some((
                    ParsedIdent {
                        name: value,
                        quoted: true,
                    },
                    pos + 1,
                ));
            }
            pos += 1;
        }
        return None;
    }

    if !bytes.get(pos).is_some_and(|b| is_ident_start(*b)) {
        return None;
    }

    let start = pos;
    pos += 1;
    while pos < bytes.len() && is_ident_continue(bytes[pos]) {
        pos += 1;
    }

    Some((
        ParsedIdent {
            name: sql[start..pos].to_string(),
            quoted: false,
        },
        pos,
    ))
}

fn split_top_level(sql: &str, delimiter: char) -> Vec<&str> {
    let delimiter = delimiter as u8;
    let bytes = sql.as_bytes();
    let mut parts = Vec::new();
    let mut start = 0;
    let mut pos = 0;
    let mut depth: u32 = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    while pos < bytes.len() {
        let byte = bytes[pos];
        if in_single_quote {
            if byte == b'\'' {
                if bytes.get(pos + 1) == Some(&b'\'') {
                    pos += 2;
                    continue;
                }
                in_single_quote = false;
            }
        } else if in_double_quote {
            if byte == b'"' {
                if bytes.get(pos + 1) == Some(&b'"') {
                    pos += 2;
                    continue;
                }
                in_double_quote = false;
            }
        } else {
            match byte {
                b'\'' => in_single_quote = true,
                b'"' => in_double_quote = true,
                b'(' => depth += 1,
                b')' => depth = depth.saturating_sub(1),
                b if b == delimiter && depth == 0 => {
                    parts.push(&sql[start..pos]);
                    start = pos + 1;
                }
                _ => {}
            }
        }
        pos += 1;
    }

    parts.push(&sql[start..]);
    parts
}

fn find_matching_paren(sql: &str, open_pos: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    if bytes.get(open_pos) != Some(&b'(') {
        return None;
    }

    let mut pos = open_pos;
    let mut depth: u32 = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    while pos < bytes.len() {
        let byte = bytes[pos];
        if in_single_quote {
            if byte == b'\'' {
                if bytes.get(pos + 1) == Some(&b'\'') {
                    pos += 2;
                    continue;
                }
                in_single_quote = false;
            }
        } else if in_double_quote {
            if byte == b'"' {
                if bytes.get(pos + 1) == Some(&b'"') {
                    pos += 2;
                    continue;
                }
                in_double_quote = false;
            }
        } else {
            match byte {
                b'\'' => in_single_quote = true,
                b'"' => in_double_quote = true,
                b'(' => depth += 1,
                b')' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return Some(pos);
                    }
                }
                _ => {}
            }
        }
        pos += 1;
    }

    None
}

fn find_top_level_keyword(sql: &str, start: usize, keyword: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut pos = start;
    let mut depth: u32 = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    while pos < bytes.len() {
        let byte = bytes[pos];
        if in_single_quote {
            if byte == b'\'' {
                if bytes.get(pos + 1) == Some(&b'\'') {
                    pos += 2;
                    continue;
                }
                in_single_quote = false;
            }
        } else if in_double_quote {
            if byte == b'"' {
                if bytes.get(pos + 1) == Some(&b'"') {
                    pos += 2;
                    continue;
                }
                in_double_quote = false;
            }
        } else {
            match byte {
                b'\'' => in_single_quote = true,
                b'"' => in_double_quote = true,
                b'(' => depth += 1,
                b')' => depth = depth.saturating_sub(1),
                _ if depth == 0 && keyword_at(sql, pos, keyword) => return Some(pos),
                _ => {}
            }
        }
        pos += 1;
    }

    None
}

fn consume_keyword(sql: &str, pos: usize, keyword: &str) -> Option<usize> {
    let pos = skip_ws(sql, pos);
    if keyword_at(sql, pos, keyword) {
        Some(pos + keyword.len())
    } else {
        None
    }
}

fn keyword_at(sql: &str, pos: usize, keyword: &str) -> bool {
    let end = pos + keyword.len();
    if end > sql.len() || !sql[pos..end].eq_ignore_ascii_case(keyword) {
        return false;
    }

    if pos > 0 && is_ident_continue(sql.as_bytes()[pos - 1]) {
        return false;
    }

    if end < sql.len() && is_ident_continue(sql.as_bytes()[end]) {
        return false;
    }

    true
}

fn skip_ws(sql: &str, mut pos: usize) -> usize {
    while pos < sql.len() && sql.as_bytes()[pos].is_ascii_whitespace() {
        pos += 1;
    }
    pos
}

fn is_ident_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_ident_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$'
}

fn normalize_ident(ident: &str) -> String {
    ident.to_ascii_lowercase()
}

fn table_keys(table_name: &[String]) -> Vec<String> {
    let mut keys = Vec::new();
    if table_name.is_empty() {
        return keys;
    }

    if table_name.len() > 1 {
        keys.push(
            table_name
                .iter()
                .map(|part| normalize_ident(part))
                .collect::<Vec<_>>()
                .join("."),
        );
    }

    keys.push(normalize_ident(
        table_name.last().expect("non-empty table name"),
    ));
    keys
}

fn table_lookup_keys(table_name: &[String]) -> Vec<String> {
    table_keys(table_name)
}

/// Builds a single DataRow with `n` columns, each containing int64(1).
/// Mirrors the stroppy noop driver's one-row stub: using 1 rather than 0
/// prevents workload guards like `if (nameCount === 0) throw` from tripping.
fn stub_row(fields: &Arc<Vec<FieldInfo>>) -> QueryResponse {
    let mut encoder = DataRowEncoder::new(Arc::clone(fields));
    for _ in 0..fields.len() {
        encoder.encode_field(&1i64).unwrap();
    }
    let row = encoder.take_row();
    QueryResponse::new(Arc::clone(fields), stream::once(future::ready(Ok(row))))
}

fn empty_query(fields: &Arc<Vec<FieldInfo>>) -> QueryResponse {
    QueryResponse::new(Arc::clone(fields), stream::empty())
}

fn classify_simple(handler: &NoopHandler, sql: &str) -> Vec<Response> {
    handler.apply_schema_change(sql);

    let upper = sql.trim().to_ascii_uppercase();
    if upper.starts_with("SELECT")
        || upper.starts_with("WITH")
        || upper.starts_with("TABLE")
        || upper.starts_with("VALUES")
    {
        if let Some(fields) = handler.fields_for_metadata_select(sql) {
            vec![Response::Query(empty_query(&fields))]
        } else {
            let n = count_select_columns(sql);
            let fields = stub_fields(n);
            vec![Response::Query(stub_row(&fields))]
        }
    } else if upper.starts_with("INSERT") {
        vec![Response::Execution(
            Tag::new("INSERT").with_oid(0).with_rows(1),
        )]
    } else if upper.starts_with("UPDATE") {
        vec![Response::Execution(Tag::new("UPDATE").with_rows(1))]
    } else if upper.starts_with("DELETE") {
        vec![Response::Execution(Tag::new("DELETE").with_rows(1))]
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

fn classify_extended(handler: &NoopHandler, sql: &str) -> Response {
    handler.apply_schema_change(sql);

    let upper = sql.trim().to_ascii_uppercase();
    if upper.starts_with("SELECT")
        || upper.starts_with("WITH")
        || upper.starts_with("TABLE")
        || upper.starts_with("VALUES")
    {
        if let Some(fields) = handler.fields_for_metadata_select(sql) {
            Response::Query(empty_query(&fields))
        } else {
            let n = count_select_columns(sql);
            let fields = stub_fields(n);
            Response::Query(stub_row(&fields))
        }
    } else if upper.starts_with("INSERT") {
        Response::Execution(Tag::new("INSERT").with_oid(0).with_rows(1))
    } else if upper.starts_with("UPDATE") {
        Response::Execution(Tag::new("UPDATE").with_rows(1))
    } else if upper.starts_with("DELETE") {
        Response::Execution(Tag::new("DELETE").with_rows(1))
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
        Ok(classify_simple(self, query))
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
        Ok(classify_extended(self, &portal.statement.statement))
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
            let fields = self
                .fields_for_metadata_select(&stmt.statement)
                .unwrap_or_else(|| stub_fields(count_select_columns(&stmt.statement)));
            Ok(DescribeStatementResponse::new(vec![], fields.to_vec()))
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
            let fields = self
                .fields_for_metadata_select(&portal.statement.statement)
                .unwrap_or_else(|| stub_fields(count_select_columns(&portal.statement.statement)));
            Ok(DescribePortalResponse::new(fields.to_vec()))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_table_parser_maps_stroppy_types() {
        let (_, columns) = parse_create_table(
            r#"
            CREATE UNLOGGED TABLE "public"."region" (
                r_regionkey  INTEGER         NOT NULL,
                r_name       CHAR(25)        NOT NULL,
                r_comment    VARCHAR(152),
                r_amount     DECIMAL(12,2),
                r_created_at TIMESTAMP WITHOUT TIME ZONE,
                r_active     BOOLEAN,
                PRIMARY KEY (r_regionkey)
            );
            "#,
        )
        .expect("create table parses");

        assert_eq!(columns.len(), 6);
        assert_eq!(columns[0].typ, Type::INT4);
        assert_eq!(columns[1].typ, Type::BPCHAR);
        assert_eq!(columns[2].typ, Type::VARCHAR);
        assert_eq!(columns[3].typ, Type::NUMERIC);
        assert_eq!(columns[4].typ, Type::TIMESTAMP);
        assert_eq!(columns[5].typ, Type::BOOL);
    }

    #[test]
    fn metadata_select_parser_accepts_quoted_schema_and_columns() {
        let (table, columns) =
            parse_metadata_select(r#"select "r_regionkey", "r_name" from "public"."region";"#)
                .expect("metadata select parses");

        assert_eq!(table, vec!["public".to_string(), "region".to_string()]);
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name.as_deref(), Some("r_regionkey"));
        assert_eq!(columns[1].name.as_deref(), Some("r_name"));
    }

    #[test]
    fn drop_table_parser_handles_tpcc_list() {
        let tables = parse_drop_table(
            "DROP TABLE IF EXISTS order_line, new_order, orders, history, stock, customer, district, warehouse, item CASCADE;",
        )
        .expect("drop table parses");

        assert_eq!(tables.len(), 9);
        assert_eq!(tables[0], vec!["order_line".to_string()]);
        assert_eq!(tables[8], vec!["item".to_string()]);
    }
}
