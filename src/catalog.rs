//! The schema a DDL statement built, and the entities that answer a metadata
//! SELECT from it. Nothing here is on the path of a statement that is not DDL or
//! metadata.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::plan::{parse_identifier, parse_qualified_name, SelectColumn};
use crate::sql::{self, Dialect};

/// The keyword at `pos` (after any whitespace and comments), and where it ends.
fn consume_keyword(sql: &str, pos: usize, keyword: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let at = sql::skip_space(bytes, pos, Dialect::Postgres);
    let end = sql::word_end(bytes, at)?;
    if sql[at..end].eq_ignore_ascii_case(keyword) {
        Some(end)
    } else {
        None
    }
}

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
pub(crate) struct SchemaCatalog {
    tables: HashMap<String, Arc<TableSchema>>,
}

pub(crate) struct TableSchema {
    columns: Vec<ColumnInfo>,
}

pub(crate) struct ColumnInfo {
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

pub(crate) fn parse_create_table(sql: &str) -> Option<(Vec<String>, Vec<ColumnInfo>)> {
    let mut pos = sql::skip_space(sql.as_bytes(), 0, Dialect::Postgres);
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
    pos = sql::skip_space(sql.as_bytes(), after_name, Dialect::Postgres);
    if sql.as_bytes().get(pos) != Some(&b'(') {
        return None;
    }

    let close = sql::matching_paren(sql.as_bytes(), pos, Dialect::Postgres)?;
    let columns = parse_column_defs(&sql[pos + 1..close]);
    if columns.is_empty() {
        return None;
    }

    Some((table_name, columns))
}

pub(crate) fn parse_drop_table(sql: &str) -> Option<Vec<Vec<String>>> {
    let mut pos = sql::skip_space(sql.as_bytes(), 0, Dialect::Postgres);
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
    for part in sql::split_top_level(names, Dialect::Postgres) {
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
    sql::split_top_level(defs, Dialect::Postgres)
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

/// The type oid for a column's declared type.
pub(crate) fn map_sql_oid(type_sql: &str) -> u32 {
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

pub(crate) fn type_words(sql: &str, max_words: usize) -> Vec<String> {
    let mut words = Vec::new();
    let mut pos = 0;
    while words.len() < max_words {
        pos = sql::skip_space(sql.as_bytes(), pos, Dialect::Postgres);
        if sql.as_bytes().get(pos) == Some(&b'(') {
            pos = match sql::matching_paren(sql.as_bytes(), pos, Dialect::Postgres) {
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

pub(crate) fn is_ident_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

pub(crate) fn is_ident_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$'
}

pub(crate) fn normalize_ident(ident: &str) -> String {
    ident.to_ascii_lowercase()
}

pub(crate) fn table_keys(table_name: &[String]) -> Vec<String> {
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

pub(crate) fn table_lookup_keys(table_name: &[String]) -> Vec<String> {
    table_keys(table_name)
}

/// The schema, as the codec sees it: two methods, neither of which involves a socket.
#[derive(Clone)]
pub(crate) struct CatalogView(pub Arc<NoopHandler>);

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

    /// **A comment before CREATE does not keep the table out of the catalog.** The
    /// keyword walk used a whitespace-only skip, so `/* x */ CREATE TABLE t (a int)`
    /// was tagged as DDL and then parsed as nothing.
    #[test]
    fn a_comment_before_create_still_builds_the_table() {
        let view = CatalogView(Arc::new(NoopHandler::new()));

        view.apply("/* x */ CREATE TABLE t (a INT, b TEXT)");

        let columns = view
            .columns_for(
                &["t".to_string()],
                &[SelectColumn {
                    name: Some("a".to_string()),
                    is_star: false,
                }],
            )
            .expect("the table is in the catalog");
        assert_eq!(columns, vec![("a".to_string(), 23)]);

        // And a comment inside the column list is not part of a column.
        view.apply("CREATE TABLE u (a INT /* the id */, b TEXT)");
        let columns = view
            .columns_for(
                &["u".to_string()],
                &[SelectColumn {
                    name: None,
                    is_star: true,
                }],
            )
            .expect("the table is in the catalog");
        assert_eq!(columns.len(), 2, "{columns:?}");
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
