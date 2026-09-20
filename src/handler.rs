use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, OnceLock, RwLock};

use async_trait::async_trait;
use futures::Sink;
use futures::{future, stream};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::CopyHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    CopyResponse, DataRowEncoder, DescribePortalResponse, DescribeResponse,
    DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};
use pgwire::messages::PgWireBackendMessage;

pub struct NoopHandler {
    catalog: RwLock<SchemaCatalog>,
    /// One parser for the life of the server. `query_parser()` is called on the
    /// extended-query path for EVERY statement, and it used to return
    /// `Arc::new(NoopQueryParser)` -- a heap allocation per query to hand back a
    /// zero-sized type.
    parser: Arc<PlanParser>,
}

impl NoopHandler {
    pub fn new() -> Self {
        Self {
            catalog: RwLock::new(SchemaCatalog::default()),
            parser: Arc::new(PlanParser),
        }
    }

    fn fields_for_metadata_select(&self, sql: &str) -> Option<Arc<Vec<FieldInfo>>> {
        let (table_name, selected_columns) = parse_metadata_select(sql)?;

        self.fields_for_parsed_select(&table_name, &selected_columns)
    }

    /// The catalog half, split out so a plan prepared once can reuse it without
    /// re-parsing the statement on every execution.
    fn fields_for_parsed_select(
        &self,
        table_name: &[String],
        selected_columns: &[SelectColumn],
    ) -> Option<Arc<Vec<FieldInfo>>> {
        let catalog = self.catalog.read().expect("schema catalog poisoned");

        catalog.fields_for_select(table_name, selected_columns)
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
                        column.name.clone().into(),
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
                            .unwrap_or_else(|| selected_name.to_string())
                            .into(),
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
/// True when `sql`, ignoring leading whitespace, begins with `kw` compared
/// case-insensitively.
///
/// This exists to delete `sql.trim().to_ascii_uppercase()` from the hot path.
/// That call allocated and copied the WHOLE statement so the next line could
/// look at its first six bytes, and it happened at least twice per query --
/// once in classify_*, once more in count_select_columns.
fn starts_with_keyword(sql: &str, kw: &str) -> bool {
    let bytes = sql.trim_start().as_bytes();
    let kw_bytes = kw.as_bytes();

    bytes.len() >= kw_bytes.len() && bytes[..kw_bytes.len()].eq_ignore_ascii_case(kw_bytes)
}

/// The leading alphabetic keyword of `sql`, as a slice into it -- no allocation.
///
/// The classify chains used to build an uppercase COPY of the whole statement
/// and then ask it eleven `starts_with` questions. Matching the first word in
/// place answers the same questions, and is strictly more precise: `starts_with`
/// also accepted "SELECTED" as a SELECT.
fn first_keyword(sql: &str) -> &str {
    let rest = sql.trim_start();
    let end = rest
        .find(|c: char| !c.is_ascii_alphabetic())
        .unwrap_or(rest.len());

    &rest[..end]
}

/// Pre-built stub field vectors for the column counts a benchmark actually
/// produces. `stub_fields` allocated an Arc, a Vec and a String per column on
/// every SELECT that was not a metadata select -- which is every `select 1`.
static STUB_FIELD_CACHE: OnceLock<Vec<Arc<Vec<FieldInfo>>>> = OnceLock::new();

const STUB_FIELD_CACHE_MAX: usize = 16;

fn stub_fields(n: usize) -> Arc<Vec<FieldInfo>> {
    let cache =
        STUB_FIELD_CACHE.get_or_init(|| (0..=STUB_FIELD_CACHE_MAX).map(build_stub_fields).collect());

    match cache.get(n) {
        Some(fields) => Arc::clone(fields),
        // Wider than anything cached: build it, as before.
        None => build_stub_fields(n),
    }
}

fn build_stub_fields(n: usize) -> Arc<Vec<FieldInfo>> {
    Arc::new(
        (0..n)
            .map(|_| FieldInfo::new("?column?".into(), None, None, Type::INT8, FieldFormat::Text))
            .collect(),
    )
}

/// Counts SELECT columns at paren-depth 0 between SELECT and FROM.
/// Works reliably for pgx's `select "c1", "c2" from "t"` pattern.
fn count_select_columns(sql: &str) -> usize {
    // No uppercase copy: the scan below only looks at ASCII punctuation and at
    // the " FROM " keyword, which is matched case-insensitively in place.
    let trimmed = sql.trim();
    if !starts_with_keyword(trimmed, "SELECT ") {
        return 1;
    }
    let body = &trimmed["SELECT ".len()..];
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
                if body.as_bytes()[i..].len() >= 6
                    && body.as_bytes()[i..i + 6].eq_ignore_ascii_case(b" FROM ")
                {
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

/// Which way a COPY moves rows, decided from the statement text.
///
/// One matcher, used by every caller that needs to know. The alternative was
/// what the code did: `wire.rs` matched one or two spaces and `handler.rs`
/// matched exactly one, so `COPY t FROM  STDIN` was two different statements
/// depending on which path asked -- a conversation on one and a finished
/// statement on the other, which desynchronises the stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CopyDirection {
    /// `COPY ... FROM STDIN`: the client streams rows next. A conversation.
    FromStdin,
    /// `COPY ... TO STDOUT`: the server would stream rows. A blackhole has none.
    ToStdout,
    /// `COPY ... FROM '/path'`: the server reads a file. No conversation.
    FromFile,
}

/// The direction of a COPY statement, or `None` if it is not a COPY.
///
/// SQL whitespace between the keywords is whatever the client sent: spaces,
/// tabs, newlines, any number. Matching literal spellings is what made tabs and
/// three spaces behave differently from one space.
pub(crate) fn copy_direction(sql: &str) -> Option<CopyDirection> {
    let mut words = SqlWords::new(sql);

    if !words.next_is("COPY") {
        return None;
    }

    // Skip the table name and any column list, then read the direction.
    // `COPY table (a, b) FROM STDIN` and `COPY table TO STDOUT` both land here.
    loop {
        let Some(word) = words.next() else {
            return None;
        };

        if word.eq_ignore_ascii_case("FROM") {
            // `FROM STDIN` is a stream; `FROM 'file'` is a path. A path arrives
            // as a quoted literal, which `next` reports as one word.
            return match words.next() {
                Some(target) if target.eq_ignore_ascii_case("STDIN") => {
                    Some(CopyDirection::FromStdin)
                }
                Some(_) => Some(CopyDirection::FromFile),
                None => None,
            };
        }

        if word.eq_ignore_ascii_case("TO") {
            return match words.next() {
                Some(target) if target.eq_ignore_ascii_case("STDOUT") => {
                    Some(CopyDirection::ToStdout)
                }
                // `TO '/path'` writes a file, which is the same non-conversation
                // as `FROM '/path'` from a client's point of view.
                Some(_) => Some(CopyDirection::FromFile),
                None => None,
            };
        }

        // The table name, a schema-qualified name, a column list: keep walking.
        // A quoted identifier or literal is one word, so a comma inside it does
        // not split it.
    }
}

/// Whether the statement asks for the binary COPY format.
///
/// pgx sends `copy t ( a ) from stdin binary`; libpq sends `... WITH (FORMAT
/// binary)`. Both are the same request, and the format decides how the rows are
/// framed -- which is the difference between counting rows and counting
/// newlines.
pub(crate) fn copy_is_binary(sql: &str) -> bool {
    fn contains_binary(sql: &str) -> bool {
        let mut words = SqlWords::new(sql);

        while let Some(word) = words.next() {
            if word.eq_ignore_ascii_case("BINARY") {
                return true;
            }

            // A parenthesised group is walked in turn: libpq's spelling is
            // `WITH (FORMAT binary)`, which arrives here as one word.
            if let Some(inner) = word.strip_prefix('(') {
                let inner = inner.strip_suffix(')').unwrap_or(inner);
                if contains_binary(inner) {
                    return true;
                }
            }
        }

        false
    }

    contains_binary(sql)
}

/// Walks a SQL statement and yields one word at a time.
///
/// Handles what a COPY statement actually contains: whitespace of any kind,
/// quoted identifiers (`"my table"`), string literals (`'/tmp/x.csv'`, doubled
/// quotes inside) and parenthesised column lists, which are skipped rather than
/// split. Enough to find the keywords without pretending to be a parser.
struct SqlWords<'a> {
    rest: &'a str,
}

impl<'a> SqlWords<'a> {
    fn new(sql: &'a str) -> Self {
        SqlWords { rest: sql }
    }

    fn next_is(&mut self, word: &str) -> bool {
        matches!(self.next(), Some(w) if w.eq_ignore_ascii_case(word))
    }

    fn next(&mut self) -> Option<&'a str> {
        self.rest = self.rest.trim_start_matches(|c: char| c.is_ascii_whitespace() || c == ';');

        let mut chars = self.rest.char_indices();

        let (_, first) = chars.next()?;

        // A quoted identifier or string: one word, up to its closing quote.
        if first == '"' || first == '\'' {
            let mut end = None;
            let mut i = 1;

            while i < self.rest.len() {
                if self.rest.as_bytes()[i] == first as u8 {
                    // A doubled quote is an escaped one, not the end.
                    if self.rest.as_bytes().get(i + 1) == Some(&(first as u8)) {
                        i += 2;

                        continue;
                    }

                    end = Some(i + 1);

                    break;
                }

                i += 1;
            }

            let end = end.unwrap_or(self.rest.len());
            let word = &self.rest[..end];
            self.rest = &self.rest[end..];

            return Some(word);
        }

        // A parenthesised group: skipped whole, so a comma inside does not
        // become a word and a newline inside does not end the search.
        if first == '(' {
            let end = self.rest.find(')').map_or(self.rest.len(), |i| i + 1);
            let word = &self.rest[..end];
            self.rest = &self.rest[end..];

            return Some(word);
        }

        let end = self
            .rest
            .find(|c: char| c.is_ascii_whitespace() || c == '(' || c == ';')
            .unwrap_or(self.rest.len());

        let word = &self.rest[..end];
        self.rest = &self.rest[end..];

        Some(word)
    }
}

/// Counts columns in `COPY table (col1, col2) FROM STDIN` by finding the
/// paren-group between the table name and the direction keyword.
pub(crate) fn count_copy_columns(sql: &str) -> usize {
    // **One string, walked once.**
    //
    // Two bugs lived here. The first: this `find`-ed in
    // `sql.trim().to_ascii_uppercase()` and sliced `sql`, so a byte offset from
    // the trimmed string cut the untrimmed one short by exactly the leading
    // whitespace -- ` COPY t (a,b) FROM STDIN` lost its `)`, `rfind('(')` still
    // matched, the closing paren did not, and the answer was 1, so
    // `CopyInResponse` announced one format code for a two-column copy.
    //
    // The second was the match itself: `" FROM "` is one spelling of the
    // separator, and SQL allows tabs, newlines and any number of spaces. The
    // walker below takes the separator as *any* whitespace, which is the only
    // version of this that is true for what clients actually send.
    let mut words = SqlWords::new(sql);

    if !words.next_is("COPY") {
        return 1;
    }

    loop {
        let Some(word) = words.next() else {
            return 1;
        };

        // The column list arrives as one word, parentheses included.
        if let Some(inner) = word.strip_prefix('(') {
            let inner = inner.strip_suffix(')').unwrap_or(inner);
            if inner.trim().is_empty() {
                return 1;
            }

            return count_top_level_commas(inner) + 1;
        }

        // The direction keyword, with no column list between the table and it.
        if word.eq_ignore_ascii_case("FROM") || word.eq_ignore_ascii_case("TO") {
            return 1;
        }
    }
}

/// Commas that separate items, not commas inside a nested group or a quoted
/// name.
fn count_top_level_commas(s: &str) -> usize {
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    let mut commas = 0usize;

    for c in s.chars() {
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                '\'' | '"' => quote = Some(c),
                '(' => depth += 1,
                ')' => depth = depth.saturating_sub(1),
                ',' if depth == 0 => commas += 1,
                _ => {}
            },
        }
    }

    commas
}

#[derive(Debug, Clone)]
pub struct SelectColumn {
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

/// The schema, as the sans-io codec is allowed to see it.
///
/// Two methods, both of which the codec needs and neither of which involves a
/// socket. It exists so `wire.rs` can stay pure: the codec is handed a view and
/// never learns there is a lock behind it.
#[derive(Clone)]
pub struct CatalogView(pub Arc<NoopHandler>);

impl CatalogView {
    /// Apply a CREATE/DROP TABLE. Called at EXECUTION, never at Parse: a
    /// prepared-but-never-executed DDL must not change the schema.
    pub fn apply(&self, sql: &str) {
        self.0.apply_schema_change(sql);
    }

    /// The (name, type oid) of each column a metadata select asked for, or None
    /// when the table is unknown -- in which case the caller falls back to stubs.
    pub fn columns_for(
        &self,
        table: &[String],
        columns: &[SelectColumn],
    ) -> Option<Vec<(String, u32)>> {
        let fields = self.0.fields_for_parsed_select(table, columns)?;

        Some(
            fields
                .iter()
                .map(|f| (f.name().to_string(), f.datatype().oid()))
                .collect(),
        )
    }
}

/// What a statement will do, decided ONCE when it is prepared.
///
/// pgwire's own `NoopQueryParser` sets `Statement = String`, so the server gets
/// the raw SQL back on every Execute and has to work out the answer again --
/// for a client-cached prepared statement that is the identical bytes, every
/// time, forever. Measured: a `select 1` re-ran a failed CREATE TABLE
/// parse, a failed DROP TABLE parse, a scan for FROM and a comma count on every
/// single execution.
///
/// `QueryParser::parse_sql` is called once, at Parse. Everything that is a pure
/// function of the SQL TEXT belongs there. What deliberately does NOT is the
/// catalog lookup: a table can be created after a statement is prepared, so the
/// metadata select keeps its parsed table/column names here and resolves them
/// against the catalog at execution.
#[derive(Debug, Clone)]
pub enum PlanKind {
    /// A SELECT naming a table: resolve against the catalog at execution.
    SelectMeta {
        table: Vec<String>,
        columns: Vec<SelectColumn>,
    },
    /// A SELECT that names no table: hand back n stub columns.
    SelectStub { columns: usize },
    Insert,
    Update,
    Delete,
    Begin,
    Commit,
    Rollback,
    /// DDL, and the only kind that still touches the SQL text at execution,
    /// because it mutates the catalog.
    Ddl,
    /// A COPY, with the direction its text declared.
    ///
    /// The direction is the whole reason this is a `PlanKind` rather than part
    /// of `FromText`: `FROM STDIN` is a conversation that must be answered with
    /// `CopyInResponse` before the client speaks, `TO STDOUT` is a
    /// `CopyOutResponse` followed by no rows at all, and `FROM '/path'` is a
    /// file the server reads with no conversation. All three used to answer
    /// `CommandComplete("SELECT 0")`, which is not a thing a client can be told
    /// for any of them.
    Copy {
        direction: CopyDirection,
        /// Columns announced in `CopyInResponse`; the format code count has to
        /// match the client's.
        columns: usize,
        /// Whether the rows arrive in PostgreSQL's binary COPY framing, which
        /// is what decides how they are counted.
        binary: bool,
    },
    /// Anything unrecognised: decided from the text as before. Rare, and not on
    /// any benchmark's hot path.
    FromText,
}

#[derive(Debug, Clone)]
pub struct PreparedPlan {
    pub sql: String,
    pub kind: PlanKind,
}

impl PreparedPlan {
    pub fn build(sql: &str) -> Self {
        let head = first_keyword(sql);

        let kind = if head.eq_ignore_ascii_case("SELECT")
            || head.eq_ignore_ascii_case("WITH")
            || head.eq_ignore_ascii_case("TABLE")
            || head.eq_ignore_ascii_case("VALUES")
        {
            match parse_metadata_select(sql) {
                Some((table, columns)) => PlanKind::SelectMeta { table, columns },
                None => PlanKind::SelectStub {
                    columns: count_select_columns(sql),
                },
            }
        } else if head.eq_ignore_ascii_case("INSERT") {
            PlanKind::Insert
        } else if head.eq_ignore_ascii_case("UPDATE") {
            PlanKind::Update
        } else if head.eq_ignore_ascii_case("DELETE") {
            PlanKind::Delete
        } else if head.eq_ignore_ascii_case("BEGIN") {
            PlanKind::Begin
        } else if head.eq_ignore_ascii_case("COMMIT") {
            PlanKind::Commit
        } else if head.eq_ignore_ascii_case("ROLLBACK") {
            PlanKind::Rollback
        } else if head.eq_ignore_ascii_case("CREATE") || head.eq_ignore_ascii_case("DROP") {
            PlanKind::Ddl
        } else if let Some(direction) = copy_direction(sql) {
            PlanKind::Copy {
                direction,
                columns: count_copy_columns(sql),
                binary: copy_is_binary(sql),
            }
        } else {
            PlanKind::FromText
        };

        PreparedPlan {
            sql: sql.to_string(),
            kind,
        }
    }

    /// The fields a Describe should report, resolved now rather than at Parse
    /// so a table created since preparation is still seen.
    fn describe_fields(&self, handler: &NoopHandler) -> Option<Arc<Vec<FieldInfo>>> {
        match &self.kind {
            PlanKind::SelectMeta { table, columns } => Some(
                handler
                    .fields_for_parsed_select(table, columns)
                    .unwrap_or_else(|| stub_fields(columns.len().max(1))),
            ),
            PlanKind::SelectStub { columns } => Some(stub_fields(*columns)),
            _ => None,
        }
    }
}

/// Our parser: does the text work once, at Parse.
pub struct PlanParser;

#[async_trait]
impl QueryParser for PlanParser {
    type Statement = PreparedPlan;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        _types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(PreparedPlan::build(sql))
    }

    /// A blackhole resolves no parameter types: it never looks at the values,
    /// and claiming a type it did not derive would be a lie the client acts on.
    fn get_parameter_types(&self, _stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        Ok(vec![])
    }

    /// The result schema IS known at Parse for a stub select, but not for one
    /// naming a table -- that needs the catalog, which the parser does not hold.
    /// The handler's own do_describe_* resolve it, so report nothing here rather
    /// than report it wrongly.
    fn get_result_schema(
        &self,
        _stmt: &Self::Statement,
        _column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        Ok(vec![])
    }
}

/// Execute a prepared plan. No text scanning except for DDL and COPY.
fn respond_to_plan(handler: &NoopHandler, plan: &PreparedPlan) -> Response {
    match &plan.kind {
        PlanKind::SelectMeta { table, columns } => {
            match handler.fields_for_parsed_select(table, columns) {
                Some(fields) => Response::Query(empty_query(&fields)),
                None => {
                    let fields = stub_fields(columns.len().max(1));
                    Response::Query(stub_row(&fields))
                }
            }
        }
        PlanKind::SelectStub { columns } => {
            let fields = stub_fields(*columns);
            Response::Query(stub_row(&fields))
        }
        PlanKind::Insert => Response::Execution(Tag::new("INSERT").with_oid(0).with_rows(1)),
        PlanKind::Update => Response::Execution(Tag::new("UPDATE").with_rows(1)),
        PlanKind::Delete => Response::Execution(Tag::new("DELETE").with_rows(1)),
        PlanKind::Begin => Response::TransactionStart(Tag::new("BEGIN")),
        PlanKind::Commit => Response::TransactionEnd(Tag::new("COMMIT")),
        PlanKind::Rollback => Response::TransactionEnd(Tag::new("ROLLBACK")),
        // The catalog is mutated here, not at Parse: a prepared DDL statement
        // that is never executed must not change the schema.
        PlanKind::Ddl => {
            handler.apply_schema_change(&plan.sql);
            classify_extended(handler, &plan.sql)
        }
        PlanKind::Copy {
            direction,
            columns,
            binary,
        } => match direction {
            CopyDirection::FromStdin => {
                Response::CopyIn(CopyResponse::new(
                    i8::from(*binary),
                    *columns,
                    stream::empty::<PgWireResult<CopyData>>(),
                ))
            }
            CopyDirection::ToStdout => Response::CopyOut(CopyResponse::new(
                0,
                0,
                stream::empty::<PgWireResult<CopyData>>(),
            )),
            CopyDirection::FromFile => Response::Execution(Tag::new("COPY").with_rows(0)),
        },
        PlanKind::FromText => classify_extended(handler, &plan.sql),
    }
}

fn classify_simple(handler: &NoopHandler, sql: &str) -> Vec<Response> {
    handler.apply_schema_change(sql);

    let head = first_keyword(sql);
    if head.eq_ignore_ascii_case("SELECT")
        || head.eq_ignore_ascii_case("WITH")
        || head.eq_ignore_ascii_case("TABLE")
        || head.eq_ignore_ascii_case("VALUES")
    {
        if let Some(fields) = handler.fields_for_metadata_select(sql) {
            vec![Response::Query(empty_query(&fields))]
        } else {
            let n = count_select_columns(sql);
            let fields = stub_fields(n);
            vec![Response::Query(stub_row(&fields))]
        }
    } else if head.eq_ignore_ascii_case("INSERT") {
        vec![Response::Execution(
            Tag::new("INSERT").with_oid(0).with_rows(1),
        )]
    } else if head.eq_ignore_ascii_case("UPDATE") {
        vec![Response::Execution(Tag::new("UPDATE").with_rows(1))]
    } else if head.eq_ignore_ascii_case("DELETE") {
        vec![Response::Execution(Tag::new("DELETE").with_rows(1))]
    } else if head.eq_ignore_ascii_case("BEGIN") {
        vec![Response::TransactionStart(Tag::new("BEGIN"))]
    } else if head.eq_ignore_ascii_case("COMMIT") {
        vec![Response::TransactionEnd(Tag::new("COMMIT"))]
    } else if head.eq_ignore_ascii_case("ROLLBACK") {
        vec![Response::TransactionEnd(Tag::new("ROLLBACK"))]
    } else if head.eq_ignore_ascii_case("COPY") {
        // The direction comes from the shared matcher, not from a literal
        // spelling: `FROM STDIN` with a tab or three spaces is the same
        // statement, and answering it as `CopyOut` desynchronised the stream.
        vector_for_copy(sql)
    } else {
        vec![Response::Execution(Tag::new("OK"))]
    }
}

/// The pgwire-handler replies for a COPY, by direction. Used by the handler
/// path; the codec path builds its own bytes from the same direction.
fn vector_for_copy(sql: &str) -> Vec<Response> {
    let columns = count_copy_columns(sql);
    let binary = copy_is_binary(sql);

    match copy_direction(sql) {
        Some(CopyDirection::FromStdin) => vec![Response::CopyIn(CopyResponse::new(
            i8::from(binary),
            columns,
            stream::empty::<PgWireResult<CopyData>>(),
        ))],
        // `TO STDOUT` streams rows out; a blackhole streams none.
        Some(CopyDirection::ToStdout) => vec![Response::CopyOut(CopyResponse::new(
            0,
            0,
            stream::empty::<PgWireResult<CopyData>>(),
        ))],
        // `FROM '/path'`: the server reads a file and copied nothing from it.
        Some(CopyDirection::FromFile) => {
            vec![Response::Execution(Tag::new("COPY").with_rows(0))]
        }
        None => vec![Response::Execution(Tag::new("OK"))],
    }
}

fn classify_extended(handler: &NoopHandler, sql: &str) -> Response {
    handler.apply_schema_change(sql);

    let head = first_keyword(sql);
    if head.eq_ignore_ascii_case("SELECT")
        || head.eq_ignore_ascii_case("WITH")
        || head.eq_ignore_ascii_case("TABLE")
        || head.eq_ignore_ascii_case("VALUES")
    {
        if let Some(fields) = handler.fields_for_metadata_select(sql) {
            Response::Query(empty_query(&fields))
        } else {
            let n = count_select_columns(sql);
            let fields = stub_fields(n);
            Response::Query(stub_row(&fields))
        }
    } else if head.eq_ignore_ascii_case("INSERT") {
        Response::Execution(Tag::new("INSERT").with_oid(0).with_rows(1))
    } else if head.eq_ignore_ascii_case("UPDATE") {
        Response::Execution(Tag::new("UPDATE").with_rows(1))
    } else if head.eq_ignore_ascii_case("DELETE") {
        Response::Execution(Tag::new("DELETE").with_rows(1))
    } else if head.eq_ignore_ascii_case("BEGIN") {
        Response::TransactionStart(Tag::new("BEGIN"))
    } else if head.eq_ignore_ascii_case("COMMIT") {
        Response::TransactionEnd(Tag::new("COMMIT"))
    } else if head.eq_ignore_ascii_case("ROLLBACK") {
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
    type Statement = PreparedPlan;
    type QueryParser = PlanParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::clone(&self.parser)
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
        Ok(respond_to_plan(self, &portal.statement.statement))
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        match stmt.statement.describe_fields(self) {
            Some(fields) => Ok(DescribeStatementResponse::new(vec![], fields.to_vec())),
            None => Ok(DescribeStatementResponse::no_data()),
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
        match portal.statement.statement.describe_fields(self) {
            Some(fields) => Ok(DescribePortalResponse::new(fields.to_vec())),
            None => Ok(DescribePortalResponse::no_data()),
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

#[cfg(test)]
mod copy_columns {
    use super::count_copy_columns;

    /// **The index came from the trimmed string and sliced the untrimmed one.**
    ///
    /// `upper.find(" FROM ")` counts bytes in `sql.trim().to_ascii_uppercase()`,
    /// and `&sql[..i]` then cuts the ORIGINAL — short by exactly the leading
    /// whitespace. ` COPY t (a,b) FROM STDIN` lost its `)`, `rfind('(')` still
    /// matched, `after_open.find(')')` did not, and the function returned 1.
    ///
    /// `CopyInResponse` then announces one format code for a two-column copy,
    /// which a client reads as a malformed message. Latent while this was only
    /// called from the pgwire path; reachable the moment `wire.rs` started
    /// calling it, which is what a reviewer noticed and these tests did not.
    #[test]
    fn leading_whitespace_does_not_shift_the_from_offset() {
        for (sql, want) in [
            ("COPY t (a,b) FROM STDIN", 2),
            (" COPY t (a,b) FROM STDIN", 2),
            ("\n\t  COPY t (a, b, c) FROM STDIN", 3),
            ("   copy warehouse (w_id, w_name) from stdin", 2),
        ] {
            assert_eq!(count_copy_columns(sql), want, "{sql:?}");
        }
    }

    /// No column list is one column, and that must survive the same shift.
    #[test]
    fn a_copy_without_a_column_list_is_one_column() {
        for sql in ["COPY t FROM STDIN", "  COPY t FROM STDIN", "COPY t () FROM STDIN"] {
            assert_eq!(count_copy_columns(sql), 1, "{sql:?}");
        }
    }

    /// SQL whitespace is not one space. The column list is found by walking the
    /// statement, so tabs, newlines and any number of spaces all land the same.
    #[test]
    fn any_whitespace_finds_the_column_list() {
        for (sql, want) in [
            ("COPY t (a, b) FROM    STDIN", 2),
            ("COPY t (a,b,c) FROM\tSTDIN", 3),
            ("COPY t ( a , b ) FROM\nSTDIN", 2),
            ("copy\tt\t(a, b)\tfrom\tstdin", 2),
            ("COPY t (a, b) TO STDOUT", 2),
        ] {
            assert_eq!(count_copy_columns(sql), want, "{sql:?}");
        }
    }

    /// A quoted name is one thing, even with a comma in it -- otherwise a table
    /// called `"a,b"` would look like a two-column list.
    #[test]
    fn commas_inside_a_quoted_name_do_not_count() {
        assert_eq!(count_copy_columns(r#"COPY "a,b" (x) FROM STDIN"#), 1);
        assert_eq!(count_copy_columns(r#"COPY t (x, "y,z") FROM STDIN"#), 2);
    }
}

#[cfg(test)]
mod copy_direction_tests {
    use super::{CopyDirection, copy_direction, copy_is_binary};

    /// **The reviewer's case.** `COPY t FROM    STDIN` used to answer `SELECT 0`
    /// because the match allowed one or two spaces, and the following `CopyData`
    /// then arrived outside a COPY -- every later reply read against the wrong
    /// message. Three spaces, a tab and a newline are the same statement.
    #[test]
    fn from_stdin_survives_any_whitespace() {
        for sql in [
            "COPY t FROM STDIN",
            "COPY t FROM  STDIN",
            "COPY t FROM    STDIN",
            "COPY t FROM\tSTDIN",
            "COPY t FROM\n STDIN",
            "  COPY t (a, b) FROM\t\tSTDIN  ",
            "copy warehouse (w_id, w_name) from   stdin",
        ] {
            assert_eq!(copy_direction(sql), Some(CopyDirection::FromStdin), "{sql:?}");
        }
    }

    /// The other two directions, which used to be the same case.
    #[test]
    fn direction_is_read_from_the_statement() {
        for sql in ["COPY t TO STDOUT", "COPY t TO  STDOUT", "COPY t (a) TO\nSTDOUT"] {
            assert_eq!(copy_direction(sql), Some(CopyDirection::ToStdout), "{sql:?}");
        }

        for sql in ["COPY t FROM '/tmp/x.csv'", "COPY t TO '/tmp/x.csv'"] {
            assert_eq!(copy_direction(sql), Some(CopyDirection::FromFile), "{sql:?}");
        }

        assert_eq!(copy_direction("SELECT 1"), None);
        assert_eq!(copy_direction("COPY"), None);
    }

    /// A table name containing the word STDOUT or STDIN is not a direction: the
    /// keyword is only read after the table name and any column list.
    #[test]
    fn a_table_name_is_not_a_direction() {
        assert_eq!(copy_direction(r#"COPY "stdin" (a) FROM STDIN"#), Some(CopyDirection::FromStdin));
        assert_eq!(copy_direction("COPY stdout FROM STDIN"), Some(CopyDirection::FromStdin));
    }

    /// Both spellings of the binary request, because they come from different
    /// clients: pgx appends `binary`, libpq wraps it in a WITH clause.
    #[test]
    fn binary_is_recognised_in_both_spellings() {
        for sql in [
            "copy t ( a, b ) from stdin binary;",
            "COPY t FROM STDIN BINARY",
            "COPY t FROM STDIN WITH (FORMAT binary)",
            "COPY t FROM STDIN WITH (FORMAT BINARY)",
        ] {
            assert!(copy_is_binary(sql), "{sql:?}");
        }

        for sql in ["COPY t FROM STDIN", "COPY t FROM STDIN WITH (FORMAT csv)"] {
            assert!(!copy_is_binary(sql), "{sql:?}");
        }
    }
}
