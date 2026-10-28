use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

pub struct NoopHandler {
    catalog: RwLock<SchemaCatalog>,
}

impl NoopHandler {
    pub fn new() -> Self {
        Self {
            catalog: RwLock::new(SchemaCatalog::default()),
        }
    }

    /// The catalog half, split out so a plan prepared once can reuse it without
    /// re-parsing the statement on every execution.
    fn fields_for_parsed_select(
        &self,
        table_name: &[String],
        selected_columns: &[SelectColumn],
    ) -> Option<Arc<Vec<(String, u32)>>> {
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
    oid: u32,
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
    ) -> Option<Arc<Vec<(String, u32)>>> {
        let table = self.lookup_table(table_name)?;
        let fields: Vec<(String, u32)> =
            if selected_columns.len() == 1 && selected_columns[0].is_star {
                table
                    .columns
                    .iter()
                    .map(|column| (column.name.clone(), column.oid))
                    .collect()
            } else {
                selected_columns
                    .iter()
                    .map(|selected| {
                        let selected_name = selected
                            .name
                            .as_deref()
                            .expect("non-star select column has a name");
                        let column = table.columns.iter().find(|column| {
                            column.normalized_name == normalize_ident(selected_name)
                        });
                        (
                            column
                                .map(|column| column.name.clone())
                                .unwrap_or_else(|| selected_name.to_string()),
                            column.map(|column| column.oid).unwrap_or(25),
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
/// Builds N FieldInfo entries typed as INT8 (text format).
fn starts_with_keyword(sql: &str, kw: &str) -> bool {
    let bytes = sql.trim_start().as_bytes();
    let kw_bytes = kw.as_bytes();

    bytes.len() >= kw_bytes.len() && bytes[..kw_bytes.len()].eq_ignore_ascii_case(kw_bytes)
}

/// Counts SELECT columns at paren-depth 0 between SELECT and FROM.
/// Works reliably for pgx's `select "c1", "c2" from "t"` pattern.
fn count_select_columns(sql: &str) -> usize {
    // No uppercase copy: the scan below only looks at ASCII punctuation and at the "
    // FROM " keyword, which is matched case-insensitively in place.
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
            // Case-insensitive in place: this branch removed the uppercase copy of the
            // whole statement, so the keyword has to be matched here rather than in an
            // uppercased buffer.
            b' ' if depth == 0
                && body.as_bytes()[i..].len() >= 6
                && body.as_bytes()[i..i + 6].eq_ignore_ascii_case(b" FROM ") =>
            {
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

/// The row format a COPY declares in its options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    Text,
    Csv,
    Binary,
}

/// Which way a COPY moves rows, decided from the statement text.
/// One matcher, used by every caller that needs to know.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyDirection {
    /// `COPY ...
    /// FROM STDIN`: the client streams rows next.
    /// A conversation.
    FromStdin,
    /// `COPY ...
    /// TO STDOUT`: the server would stream rows.
    /// A blackhole has none.
    ToStdout,
    /// `COPY ...
    /// FROM '/path'`: the server reads a file.
    /// No conversation.
    FromFile,
}

/// The direction of a COPY statement, or `None` if it is not a COPY.
/// SQL whitespace between the keywords is whatever the client sent: spaces, tabs,
/// newlines, any number.
pub(crate) fn copy_direction(sql: &str) -> Option<CopyDirection> {
    let mut words = SqlWords::new(sql);

    if !words.next_is("COPY") {
        return None;
    }

    // Skip the table name and any column list, then read the direction.
    // `COPY table (a, b) FROM STDIN` and `COPY table TO STDOUT` both land here.
    loop {
        let word = words.next()?;

        if word.eq_ignore_ascii_case("FROM") {
            // `FROM STDIN` is a stream; `FROM 'file'` is a path.
            // A path arrives as a quoted literal, which `next` reports as one word.
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
                // `TO '/path'` writes a file, which is the same non-conversation as
                // `FROM '/path'` from a client's point of view.
                Some(_) => Some(CopyDirection::FromFile),
                None => None,
            };
        }

        // The table name, a schema-qualified name, a column list: keep walking.
        // A quoted identifier or literal is one word, so a comma inside it does not
        // split it.
    }
}

/// The format the statement's options declare.
/// pgx sends `copy t ( a ) from stdin binary`; libpq sends `...
/// WITH (FORMAT binary)`; the old syntax allows `WITH BINARY` and `CSV HEADER`.
pub(crate) fn copy_format(sql: &str) -> CopyFormat {
    let mut words = SqlWords::new(sql);

    if !words.next_is("COPY") {
        return CopyFormat::Text;
    }

    // Past the direction and its target.
    loop {
        let Some(word) = words.next() else {
            return CopyFormat::Text;
        };
        if word.eq_ignore_ascii_case("FROM") || word.eq_ignore_ascii_case("TO") {
            if words.next().is_none() {
                return CopyFormat::Text;
            }
            break;
        }
    }

    // The options: bare keywords, or a parenthesised list of `name value`.
    while let Some(word) = words.next() {
        if word.eq_ignore_ascii_case("BINARY") {
            return CopyFormat::Binary;
        }
        if word.eq_ignore_ascii_case("CSV") {
            return CopyFormat::Csv;
        }
        if let Some(inner) = word.strip_prefix('(') {
            let inner = inner.strip_suffix(')').unwrap_or(inner);
            for option in crate::sql::split_top_level(inner) {
                let mut kv = SqlWords::new(option);
                if kv.next_is("FORMAT") {
                    return match kv.next().map(|v| v.trim_matches('\'')) {
                        Some(v) if v.eq_ignore_ascii_case("binary") => CopyFormat::Binary,
                        Some(v) if v.eq_ignore_ascii_case("csv") => CopyFormat::Csv,
                        _ => CopyFormat::Text,
                    };
                }
            }
        }
    }

    CopyFormat::Text
}

/// The comma-separated pieces of an option list, ignoring commas inside quotes and
/// nested parentheses.
/// Walks a SQL statement and yields one word at a time.
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
        loop {
            self.rest = self
                .rest
                .trim_start_matches(|c: char| c.is_ascii_whitespace() || c == ';');
            if self.rest.is_empty() {
                return None;
            }
            match crate::sql::skip_comment(self.rest.as_bytes(), 0) {
                Some(next) if next > 0 => self.rest = &self.rest[next..],
                _ => break,
            }
        }

        let bytes = self.rest.as_bytes();
        let end = match bytes[0] {
            b'\'' | b'"' | b'`' | b'$' => {
                crate::sql::skip_literal(bytes, 0).max(1).min(bytes.len())
            }
            b'(' => crate::sql::matching_paren(bytes, 0).unwrap_or(bytes.len()),
            _ => (0..bytes.len())
                .find(|&i| {
                    bytes[i].is_ascii_whitespace()
                        || bytes[i] == b'('
                        || bytes[i] == b';'
                        || crate::sql::skip_comment(bytes, i).is_some()
                })
                .unwrap_or(bytes.len())
                .max(1),
        };
        let word = &self.rest[..end];
        self.rest = &self.rest[end..];

        Some(word)
    }
}

/// Counts columns in `COPY table (col1, col2) FROM STDIN` by finding the paren-group
/// between the table name and the direction keyword.
pub(crate) fn count_copy_columns(sql: &str) -> usize {
    // One string, walked once.
    // Two bugs lived here.
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

            return crate::sql::split_top_level(inner).len();
        }

        // The direction keyword, with no column list between the table and it.
        if word.eq_ignore_ascii_case("FROM") || word.eq_ignore_ascii_case("TO") {
            return 1;
        }
    }
}

/// Commas that separate items, not commas inside a nested group or a quoted name.

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
        oid: map_sql_oid(type_sql),
    })
}

fn is_table_constraint_keyword(word: &str) -> bool {
    matches!(
        word.to_ascii_uppercase().as_str(),
        "PRIMARY" | "FOREIGN" | "UNIQUE" | "CHECK" | "CONSTRAINT"
    )
}

/// The most parameters a statement may declare: ParameterDescription carries the count
/// as an int16, and PostgreSQL has the same ceiling.
pub(crate) const MAX_PARAMETERS: usize = 65535;

/// A placeholder PostgreSQL would refuse, with the SQLSTATE it uses.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PlaceholderError {
    /// `$0`: parameters are numbered from 1.
    Zero,
    /// `$n` past `MAX_PARAMETERS`, or too long to be a number at all.
    TooMany,
}

impl PlaceholderError {
    pub(crate) fn sqlstate(&self) -> &'static str {
        match self {
            PlaceholderError::Zero => "42P02",
            PlaceholderError::TooMany => "54000",
        }
    }

    pub(crate) fn message(&self) -> String {
        match self {
            PlaceholderError::Zero => "there is no parameter $0".to_string(),
            PlaceholderError::TooMany => {
                format!("cannot use more than {MAX_PARAMETERS} parameters in a statement")
            }
        }
    }
}

/// The parameter types a Parse declares, completed from the SQL text.
/// `declared` is the Parse message's list, where 0 means "you decide".
pub(crate) fn parameter_types(sql: &str, declared: &[u32]) -> Result<Vec<u32>, PlaceholderError> {
    let found = placeholders(sql)?;
    let highest = found.iter().map(|&(n, _)| n).max().unwrap_or(0);
    let n = declared.len().max(highest).min(MAX_PARAMETERS);

    Ok((0..n)
        .map(|i| match declared.get(i) {
            Some(&oid) if oid != 0 => oid,
            _ => cast_on_placeholder(sql, &found, i + 1)
                .map(map_sql_oid)
                .unwrap_or(0),
        })
        .collect())
}

/// Every `$n` where SQL would read one: its number and the index after it.
fn placeholders(sql: &str) -> Result<Vec<(usize, usize)>, PlaceholderError> {
    let b = sql.as_bytes();
    let mut found = Vec::new();
    let mut i = 0;

    while i < b.len() {
        match b[i] {
            b'\'' | b'"' => {
                i = crate::sql::skip_literal(b, i);

                continue;
            }
            b'-' | b'#' | b'/' => {
                if let Some(next) = crate::sql::skip_comment(b, i) {
                    i = next;

                    continue;
                }
                i += 1;

                continue;
            }
            b'$' => {
                if b.get(i + 1).is_some_and(u8::is_ascii_digit) {
                    let start = i + 1;
                    let mut end = start;
                    let mut n: usize = 0;
                    while end < b.len() && b[end].is_ascii_digit() {
                        n = n
                            .saturating_mul(10)
                            .saturating_add(usize::from(b[end] - b'0'));
                        end += 1;
                    }
                    if n == 0 {
                        return Err(PlaceholderError::Zero);
                    }
                    if n > MAX_PARAMETERS {
                        return Err(PlaceholderError::TooMany);
                    }
                    found.push((n, end));
                    i = end;

                    continue;
                }
                i = crate::sql::skip_literal(b, i);

                continue;
            }
            _ => i += 1,
        }
    }

    Ok(found)
}

/// The type name after `$n::`, if the placeholder is cast directly.
fn cast_on_placeholder<'a>(sql: &'a str, found: &[(usize, usize)], n: usize) -> Option<&'a str> {
    let b = sql.as_bytes();

    for &(at, after) in found {
        if at != n {
            continue;
        }

        let mut i = skip_ws(sql, after);
        if b.get(i) != Some(&b':') || b.get(i + 1) != Some(&b':') {
            continue;
        }
        i = skip_ws(sql, i + 2);

        // The type name, possibly two words ("double precision", "character varying"),
        // possibly with a modifier in parentheses.
        let start = i;
        let mut end = start;
        while end < b.len() && is_ident_continue(b[end]) {
            end += 1;
        }
        if end == start {
            continue;
        }
        let second = skip_ws(sql, end);
        let mut second_end = second;
        while second_end < b.len() && is_ident_continue(b[second_end]) {
            second_end += 1;
        }
        if second_end > second
            && (sql[second..second_end].eq_ignore_ascii_case("precision")
                || sql[second..second_end].eq_ignore_ascii_case("varying"))
        {
            end = second_end;
        }

        return Some(&sql[start..end]);
    }

    None
}

/// The type oid for a column's declared type.
fn map_sql_oid(type_sql: &str) -> u32 {
    let words = type_words(type_sql, 4);
    let first = words.first().map(String::as_str).unwrap_or("");
    let second = words.get(1).map(String::as_str).unwrap_or("");

    match first {
        "INTEGER" | "INT" | "INT4" => 23,
        "BIGINT" | "INT8" => 20,
        "SMALLINT" | "INT2" => 21,
        "CHAR" => 1042,
        "CHARACTER" if second == "VARYING" => 1043,
        "CHARACTER" => 1042,
        "VARCHAR" => 1043,
        "TEXT" => 25,
        "DECIMAL" | "NUMERIC" => 1700,
        "DATE" => 1082,
        "TIMESTAMP" => 1114,
        "BOOLEAN" | "BOOL" => 16,
        "REAL" | "FLOAT4" => 700,
        "DOUBLE" | "FLOAT8" => 701,
        "TIMESTAMPTZ" => 1184,
        "UUID" => 2950,
        "BYTEA" => 17,
        "JSON" => 114,
        "JSONB" => 3802,
        _ => 25,
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

/// The schema, as the sans-io codec is allowed to see it.
/// Two methods, both of which the codec needs and neither of which involves a socket.
#[derive(Clone)]
pub struct CatalogView(pub Arc<NoopHandler>);

impl CatalogView {
    /// Apply a CREATE/DROP TABLE.
    /// Called at EXECUTION, never at Parse: a prepared-but-never-executed DDL must not
    /// change the schema.
    pub fn apply(&self, sql: &str) {
        self.0.apply_schema_change(sql);
    }

    /// The (name, type oid) of each column a metadata select asked for, or None when
    /// the table is unknown -- in which case the caller falls back to stubs.
    pub fn columns_for(
        &self,
        table: &[String],
        columns: &[SelectColumn],
    ) -> Option<Vec<(String, u32)>> {
        Some(
            self.0
                .fields_for_parsed_select(table, columns)?
                .as_ref()
                .clone(),
        )
    }
}

/// What a statement will do, decided ONCE when it is prepared.
#[derive(Debug, Clone)]
pub enum PlanKind {
    /// A SELECT naming a table: resolve against the catalog at execution.
    SelectMeta {
        table: Vec<String>,
        columns: Vec<SelectColumn>,
    },
    /// A SELECT that names no table and no literal: `n` stub columns and no rows, which
    /// is what a SELECT on a table the server has never seen also answers.
    SelectStub {
        columns: usize,
    },
    /// A select list of literals and variables: one row of exactly those values.
    Constant(Vec<crate::sql::Constant>),
    Insert,
    Update,
    Delete,
    Begin,
    Commit,
    Rollback,
    /// DDL, and the only kind that still touches the SQL text at execution, because it
    /// mutates the catalog.
    Ddl,
    /// A COPY, with the direction its text declared.
    Copy {
        direction: CopyDirection,
        /// Columns announced in `CopyInResponse`; the format code count has to match
        /// the client's.
        columns: usize,
        /// How the rows are framed, which is what decides how they are counted.
        format: CopyFormat,
    },
    /// Nothing at all: `""`, `";"`, or whitespace and semicolons.
    /// PostgreSQL answers EmptyQueryResponse, and a client that sends one -- some do,
    /// as a liveness probe -- reads that byte and not a CommandComplete.
    Empty,
    /// Anything unrecognised: decided from the text as before.
    /// Rare, and not on any benchmark's hot path.
    FromText,
}

#[derive(Debug, Clone)]
pub struct PreparedPlan {
    pub sql: String,
    pub kind: PlanKind,
}

impl PreparedPlan {
    pub fn build(sql: &str) -> Self {
        let head = crate::sql::first_word(sql);

        let kind = if head.eq_ignore_ascii_case("SELECT")
            || head.eq_ignore_ascii_case("WITH")
            || head.eq_ignore_ascii_case("TABLE")
            || head.eq_ignore_ascii_case("VALUES")
        {
            // A literal answers a row; anything naming data answers none.
            // `select 1` and `select @@version` are values the server can state without
            // knowing anything, and both protocols answer them the same way.
            if let Some(items) = crate::sql::constant_select(sql) {
                PlanKind::Constant(items)
            } else {
                match parse_metadata_select(sql) {
                    Some((table, columns)) => PlanKind::SelectMeta { table, columns },
                    None => PlanKind::SelectStub {
                        columns: count_select_columns(sql),
                    },
                }
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
                format: copy_format(sql),
            }
        } else if sql.bytes().all(|b| b.is_ascii_whitespace() || b == b';') {
            PlanKind::Empty
        } else {
            PlanKind::FromText
        };

        PreparedPlan {
            sql: sql.to_string(),
            kind,
        }
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
        assert_eq!(columns[0].oid, 23);
        assert_eq!(columns[1].oid, 1042);
        assert_eq!(columns[2].oid, 1043);
        assert_eq!(columns[3].oid, 1700);
        assert_eq!(columns[4].oid, 1114);
        assert_eq!(columns[5].oid, 16);
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

    /// The index came from the trimmed string and sliced the untrimmed one.
    /// `upper.find(" FROM ")` counts bytes in `sql.trim().to_ascii_uppercase()`, and
    /// `&sql[..i]` then cuts the ORIGINAL — short by exactly the leading whitespace.
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
        for sql in [
            "COPY t FROM STDIN",
            "  COPY t FROM STDIN",
            "COPY t () FROM STDIN",
        ] {
            assert_eq!(count_copy_columns(sql), 1, "{sql:?}");
        }
    }

    /// SQL whitespace is not one space.
    /// The column list is found by walking the statement, so tabs, newlines and any
    /// number of spaces all land the same.
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

    /// A quoted name is one thing, even with a comma in it -- otherwise a table called
    /// `"a,b"` would look like a two-column list.
    #[test]
    fn commas_inside_a_quoted_name_do_not_count() {
        assert_eq!(count_copy_columns(r#"COPY "a,b" (x) FROM STDIN"#), 1);
        assert_eq!(count_copy_columns(r#"COPY t (x, "y,z") FROM STDIN"#), 2);
    }
}

#[cfg(test)]
mod copy_direction_tests {
    use super::{copy_direction, copy_format, CopyDirection, CopyFormat};

    /// A comment is not syntax.
    #[test]
    fn comments_are_skipped_by_the_copy_walker() {
        assert_eq!(
            copy_direction("COPY t /* FROM STDIN */ TO STDOUT"),
            Some(CopyDirection::ToStdout)
        );
        assert_eq!(
            copy_direction("COPY t /* TO STDOUT */ FROM STDIN"),
            Some(CopyDirection::FromStdin)
        );
        assert_eq!(
            copy_direction("COPY t -- TO STDOUT\n FROM STDIN"),
            Some(CopyDirection::FromStdin)
        );
        assert_eq!(
            copy_direction("/* leading */ COPY t FROM STDIN"),
            Some(CopyDirection::FromStdin)
        );
        assert_eq!(
            copy_format("COPY t FROM STDIN /* binary */"),
            CopyFormat::Text
        );
        assert_eq!(copy_format("COPY t FROM STDIN -- csv\n"), CopyFormat::Text);
    }

    /// A comment glued to a token is still a comment.
    /// Comments were skipped only between words: while a word was being read, `/` was
    /// not a boundary, so `t/*x*/FROM` was one word and the direction was never found.
    #[test]
    fn a_comment_adjacent_to_a_token_ends_the_token() {
        for sql in [
            "COPY t/*x*/FROM STDIN",
            "COPY t FROM/*x*/STDIN",
            "COPY/*x*/t FROM STDIN",
            "COPY t FROM STDIN/*x*/",
            "COPY t--x\nFROM STDIN",
            "COPY t FROM--x\nSTDIN",
        ] {
            assert_eq!(
                copy_direction(sql),
                Some(CopyDirection::FromStdin),
                "{sql:?}"
            );
        }
        assert_eq!(
            copy_direction("COPY t/*x*/TO/*y*/STDOUT"),
            Some(CopyDirection::ToStdout)
        );
        assert_eq!(
            copy_format("COPY t FROM STDIN/*x*/BINARY"),
            CopyFormat::Binary
        );
        assert_eq!(
            copy_format("COPY t FROM STDIN BINARY/*x*/"),
            CopyFormat::Binary
        );
        assert_eq!(copy_format("COPY t FROM STDIN/*binary*/"), CopyFormat::Text);
    }

    /// A parenthesised group ends at its matching parenthesis.
    #[test]
    fn a_parenthesised_group_honours_quotes_and_nesting() {
        for (sql, columns) in [
            ("COPY t (\"a)\", b) FROM STDIN", 2),
            ("COPY t (\"a)\") FROM STDIN", 1),
            ("COPY t (\"a\"\")\", b) FROM STDIN", 2),
            ("COPY t (a, \"b(\") FROM STDIN", 2),
            ("COPY t (a, b /* ) */) FROM STDIN", 2),
            ("COPY t (a, b) FROM STDIN WITH (FORMAT csv, QUOTE ')')", 2),
        ] {
            assert_eq!(
                copy_direction(sql),
                Some(CopyDirection::FromStdin),
                "{sql:?}"
            );
            assert_eq!(super::count_copy_columns(sql), columns, "{sql:?}");
        }
        assert_eq!(
            copy_format("COPY t (\"a)\") FROM STDIN WITH (FORMAT binary)"),
            CopyFormat::Binary
        );
        assert_eq!(
            copy_format("COPY t FROM STDIN WITH (DELIMITER ')', FORMAT csv)"),
            CopyFormat::Csv
        );
    }

    /// The format is an OPTION, read after the direction's target, not any `BINARY` or
    /// `CSV` token anywhere in the statement.
    #[test]
    fn format_is_read_from_the_options_clause() {
        for (sql, want) in [
            ("copy t ( a, b ) from stdin binary;", CopyFormat::Binary),
            ("COPY t FROM STDIN BINARY", CopyFormat::Binary),
            ("COPY t FROM STDIN WITH BINARY", CopyFormat::Binary),
            ("COPY t FROM STDIN WITH (FORMAT binary)", CopyFormat::Binary),
            (
                "COPY t FROM STDIN WITH (FORMAT 'binary')",
                CopyFormat::Binary,
            ),
            (
                "COPY t FROM STDIN (FORMAT BINARY, FREEZE)",
                CopyFormat::Binary,
            ),
            ("COPY t FROM STDIN CSV", CopyFormat::Csv),
            ("COPY t FROM STDIN WITH CSV HEADER", CopyFormat::Csv),
            (
                "COPY t FROM STDIN WITH (FORMAT csv, HEADER true)",
                CopyFormat::Csv,
            ),
            (
                "COPY t FROM STDIN WITH (DELIMITER ',', FORMAT csv)",
                CopyFormat::Csv,
            ),
            ("COPY t FROM STDIN", CopyFormat::Text),
            ("COPY t FROM STDIN WITH (FORMAT text)", CopyFormat::Text),
            ("COPY t (a, binary, csv) FROM STDIN", CopyFormat::Text),
            ("COPY \"binary\" FROM STDIN", CopyFormat::Text),
            (
                "COPY t FROM STDIN WITH (DELIMITER 'binary')",
                CopyFormat::Text,
            ),
        ] {
            assert_eq!(copy_format(sql), want, "{sql:?}");
        }
    }

    /// Three spaces, a tab and a newline are the same statement.
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
            assert_eq!(
                copy_direction(sql),
                Some(CopyDirection::FromStdin),
                "{sql:?}"
            );
        }
    }

    /// The direction is read from the statement's options.
    #[test]
    fn direction_is_read_from_the_statement() {
        for sql in [
            "COPY t TO STDOUT",
            "COPY t TO  STDOUT",
            "COPY t (a) TO\nSTDOUT",
        ] {
            assert_eq!(
                copy_direction(sql),
                Some(CopyDirection::ToStdout),
                "{sql:?}"
            );
        }

        for sql in ["COPY t FROM '/tmp/x.csv'", "COPY t TO '/tmp/x.csv'"] {
            assert_eq!(
                copy_direction(sql),
                Some(CopyDirection::FromFile),
                "{sql:?}"
            );
        }

        assert_eq!(copy_direction("SELECT 1"), None);
        assert_eq!(copy_direction("COPY"), None);
    }

    /// A table name containing the word STDOUT or STDIN is not a direction: the keyword
    /// is only read after the table name and any column list.
    #[test]
    fn a_table_name_is_not_a_direction() {
        assert_eq!(
            copy_direction(r#"COPY "stdin" (a) FROM STDIN"#),
            Some(CopyDirection::FromStdin)
        );
        assert_eq!(
            copy_direction("COPY stdout FROM STDIN"),
            Some(CopyDirection::FromStdin)
        );
    }

    /// Both spellings of the binary request, because they come from different clients:
    /// pgx appends `binary`, libpq wraps it in a WITH clause.
    #[test]
    fn binary_is_recognised_in_both_spellings() {
        for sql in [
            "copy t ( a, b ) from stdin binary;",
            "COPY t FROM STDIN BINARY",
            "COPY t FROM STDIN WITH (FORMAT binary)",
            "COPY t FROM STDIN WITH (FORMAT BINARY)",
        ] {
            assert_eq!(copy_format(sql), CopyFormat::Binary, "{sql:?}");
        }

        for sql in ["COPY t FROM STDIN", "COPY t FROM STDIN WITH (FORMAT csv)"] {
            assert_ne!(copy_format(sql), CopyFormat::Binary, "{sql:?}");
        }
    }
}

#[cfg(test)]
mod placeholder_tests {
    use super::*;

    fn count(sql: &str) -> usize {
        parameter_types(sql, &[]).expect(sql).len()
    }

    /// **A leading comment does not hide the statement.** The keyword scanner
    /// started at the comment, so `/* x */ INSERT` classified as unknown text and
    /// PostgreSQL answered `SELECT 0` where MySQL answered the insert.
    #[test]
    fn a_leading_comment_does_not_hide_the_statement() {
        for (sql, want) in [
            ("/* x */ insert into t values (1)", "insert"),
            ("-- x\nupdate t set a = 1", "update"),
            ("/* a /* nested */ b */ delete from t", "delete"),
        ] {
            assert_eq!(crate::sql::first_word(sql), want, "{sql}");
            let plan = PreparedPlan::build(sql);
            assert!(
                matches!(
                    plan.kind,
                    PlanKind::Insert | PlanKind::Update | PlanKind::Delete
                ),
                "{sql} classified as {:?}",
                plan.kind
            );
        }
    }

    /// `$n` is a placeholder only where SQL would read one.
    #[test]
    fn placeholders_in_comments_and_quotes_are_not_parameters() {
        for sql in [
            "select 1 -- $1",
            "select 1 -- $1\n",
            "select 1 /* $1 */",
            "select 1 /* outer /* $1 */ still comment */",
            "select $$literal $1$$",
            "select $tag$ $1 $tag$",
            "select $_x1$ $1 $_x1$",
            "select \"$1\" from t",
            "select '$1'",
            "select 'it''s $1'",
            "select E'\\' $1'",
            "select e'\\\\' , 1",
        ] {
            assert_eq!(count(sql), 0, "{sql:?}");
        }
    }

    /// And where SQL would read one, it still is -- after a comment, after a string,
    /// and inside parentheses.
    #[test]
    fn real_placeholders_are_still_found() {
        assert_eq!(count("select $1 -- $2"), 1);
        assert_eq!(count("select /* a */ $1, /* b */ $2"), 2);
        assert_eq!(count("select '$9', $1"), 1);
        assert_eq!(count("select $$x$$, $3"), 3);
        assert_eq!(count("select f($1,$2)"), 2);
        assert_eq!(count("select $1::int8"), 1);
        assert_eq!(count("select E'\\'' || $1"), 1);
    }

    /// `$0` is not a parameter in PostgreSQL, and the count is bounded before anything
    /// is allocated: one unauthenticated statement can otherwise ask for gigabytes.
    #[test]
    fn zero_and_oversized_indices_are_refused_before_allocating() {
        assert!(parameter_types("select $0", &[]).is_err());
        assert!(parameter_types("select $5000000", &[]).is_err());
        assert!(parameter_types("select $99999999999999999999999", &[]).is_err());
        assert!(parameter_types("select $65536", &[]).is_err());
        assert_eq!(count("select $65535"), 65535);
    }
}
