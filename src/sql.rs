//! The little SQL this server reads: the first word, the select list, the
//! statement boundaries and the placeholders. Nothing here interprets a value.

/// What the MySQL handshake calls itself, which `@@version` answers.
pub const SERVER_VERSION: &str = "8.0.0-db-noop";

/// A comma- or semicolon-level split, with the delimiters left out.
fn split_on(text: &str, delimiter: u8, parens: bool) -> Vec<&str> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    let mut depth = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'(' if parens => {
                depth += 1;
                i += 1;
            }
            b')' if parens => {
                depth = depth.saturating_sub(1);
                i += 1;
            }
            b'\'' | b'"' | b'`' | b'$' => i = skip_literal(bytes, i),
            b'-' | b'#' | b'/' => match skip_comment(bytes, i) {
                Some(next) => i = next,
                None => i += 1,
            },
            b if b == delimiter && depth == 0 => {
                out.push(&text[start..i]);
                i += 1;
                start = i;
            }
            _ => i += 1,
        }
    }
    if start < bytes.len() {
        out.push(&text[start..]);
    }

    out
}

/// Split a multi-statement query on top-level `;`, keeping empty segments: which of
/// them answer is the caller's rule.
/// A stored routine's body is one statement.
pub fn split_statements(sql: &str) -> Vec<&str> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    // The statements's words so far: what says whether it is a routine at all.
    let mut words = 0usize;
    let mut routine = false;
    let mut routine_named = false;
    // Some(depth) while inside a BEGIN…END body.
    let mut body: Option<usize> = None;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'`' | b'$' => i = skip_literal(bytes, i),
            b'-' | b'#' | b'/' => match skip_comment(bytes, i) {
                Some(next) => i = next,
                None => i += 1,
            },
            c if is_word_byte(c) => {
                let end = word_end(bytes, i).unwrap_or(bytes.len());
                let word = &sql[i..end];
                i = end;
                words += 1;

                if body.is_none() {
                    // `CREATE [DEFINER = …] PROCEDURE|FUNCTION|TRIGGER|EVENT`, and the
                    // few words in between, before the body starts.
                    if words == 1 {
                        routine = word.eq_ignore_ascii_case("CREATE");
                    } else if routine && !routine_named && words <= 8 {
                        routine_named = ["PROCEDURE", "FUNCTION", "TRIGGER", "EVENT"]
                            .iter()
                            .any(|kind| word.eq_ignore_ascii_case(kind));
                    }
                    if routine_named && word.eq_ignore_ascii_case("BEGIN") {
                        body = Some(1);
                    }
                } else if word.eq_ignore_ascii_case("BEGIN") || word.eq_ignore_ascii_case("CASE") {
                    // A block opens with BEGIN, and a CASE -- expression or statement
                    // -- closes with an END of its own.
                    *body.as_mut().expect("in a body") += 1;
                } else if word.eq_ignore_ascii_case("END") {
                    let next = skip_space(bytes, i);
                    // `END IF`, `END LOOP`, `END WHILE` and `END REPEAT` close forms
                    // that opened nothing here.
                    let closes_nothing = word_end(bytes, next).is_some_and(|end| {
                        ["IF", "LOOP", "WHILE", "REPEAT"]
                            .iter()
                            .any(|kind| sql[next..end].eq_ignore_ascii_case(kind))
                    });
                    if !closes_nothing {
                        let depth = body.as_mut().expect("in a body");
                        *depth -= 1;
                        if *depth == 0 {
                            body = None;
                        }
                    }
                }
            }
            b';' if body.is_none() => {
                out.push(&sql[start..i]);
                i += 1;
                start = i;
                words = 0;
                routine = false;
                routine_named = false;
            }
            _ => i += 1,
        }
    }
    if start < bytes.len() {
        out.push(&sql[start..]);
    }

    out
}

/// Split on top-level commas.
pub fn split_top_level(text: &str) -> Vec<&str> {
    split_on(text, b',', true)
}

/// How many placeholders a statement has: `?` outside quotes, comments and identifiers.
pub fn count_placeholders(sql: &str) -> u16 {
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut count = 0u16;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'`' | b'$' => i = skip_literal(bytes, i),
            b'-' | b'#' | b'/' => match skip_comment(bytes, i) {
                Some(next) => i = next,
                None => i += 1,
            },
            b'?' => {
                count = count.saturating_add(1);
                i += 1;
            }
            _ => i += 1,
        }
    }

    count
}

/// The offset of the first top-level `keyword` at or after `from`.
/// Top level is outside parentheses, quotes and comments: `select coalesce(a, 'from')`
/// names no table, and `select (select 1 from t)` does.
pub fn find_keyword(sql: &str, from: usize, keyword: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut i = from;
    let mut depth = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                i += 1;
            }
            b'\'' | b'"' | b'`' | b'$' => i = skip_literal(bytes, i),
            b'-' | b'#' | b'/' => match skip_comment(bytes, i) {
                Some(next) => i = next,
                None => i += 1,
            },
            c if depth == 0 && is_word_byte(c) => {
                let end = word_end(bytes, i)?;
                if sql[i..end].eq_ignore_ascii_case(keyword) {
                    return Some(i);
                }
                i = end;
            }
            _ => i += 1,
        }
    }

    None
}

/// The text of a keyword's argument when it is a single-quoted literal: `LIKE 'Com%'`
/// gives `Com%`.
pub fn quoted_argument<'a>(sql: &'a str, keyword: &str) -> Option<&'a str> {
    let at = find_keyword(sql, 0, keyword)?;
    let rest = &sql[at + keyword.len()..];
    let rest = trim_leading(rest);
    let rest = rest.strip_prefix('\'')?;
    let end = rest.find('\'')?;

    Some(&rest[..end])
}

/// The words that end a select list: everything after the list is a clause about the
/// rows rather than a column of them.
const CLAUSES: [&str; 10] = [
    "FROM",
    "LIMIT",
    "ORDER",
    "GROUP",
    "HAVING",
    "OFFSET",
    "UNION",
    "INTO",
    "FOR",
    "PROCEDURE",
];

/// The select list: the text between `SELECT` (and `DISTINCT`) and the first top-level
/// clause -- `FROM`, or a `LIMIT`/`ORDER`/… with no table at all.
/// `None` when the statement does not start with `SELECT` or has no list.
pub fn select_list(sql: &str) -> Option<&str> {
    let bytes = sql.as_bytes();
    let start = skip_space(bytes, 0);
    if !matches!(word_end(bytes, start), Some(end) if sql[start..end].eq_ignore_ascii_case("SELECT"))
    {
        return None;
    }
    let mut i = skip_space(bytes, word_end(bytes, start)?);
    // `DISTINCT` / `ALL` / `DISTINCTROW`
    if let Some(end) = word_end(bytes, i) {
        if ["DISTINCT", "ALL", "DISTINCTROW"]
            .iter()
            .any(|kw| sql[i..end].eq_ignore_ascii_case(kw))
        {
            i = skip_space(bytes, end);
        }
    }

    let stop = CLAUSES
        .iter()
        .filter_map(|clause| find_keyword(sql, i, clause))
        .min()
        .unwrap_or(bytes.len());
    let list = &sql[i..stop];
    if list.trim().is_empty() {
        return None;
    }

    Some(list)
}

/// One item of a constant select list: the text a client wrote, the value the server
/// answers, and what kind of value it is.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Constant {
    pub name: String,
    pub value: String,
    pub kind: ConstantKind,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ConstantKind {
    /// A number, sent in the type a client reads numbers with.
    Integer,
    /// A string, or a system variable's value.
    Text,
}

/// A select list of literals and variables, which is a row the server can state without
/// knowing anything about the data.
pub fn constant_select(sql: &str) -> Option<Vec<Constant>> {
    // A select over rows is not a row of constants.
    if find_keyword(sql, first_word(sql).len(), "FROM").is_some() {
        return None;
    }
    let list = select_list(sql)?;

    let mut items = Vec::new();
    for item in split_top_level(list) {
        items.push(constant_item(trim_leading(item))?);
    }
    if items.is_empty() {
        return None;
    }

    Some(items)
}

/// One item of a select list, when it is a literal or a variable.
fn constant_item(item: &str) -> Option<Constant> {
    // A trailing comment is not part of the value: `select 1 /* padding */` is still
    // the literal 1, which is what a padded query is for.
    let item = strip_trailing_comment(item).trim_end();
    if item.is_empty() {
        return None;
    }

    // A quoted string: its own text, without the quotes.
    if let Some(inner) = item.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')) {
        if inner.len() + 2 != item.len() {
            return None;
        }

        return Some(Constant {
            name: item.to_string(),
            value: inner.replace("''", "'"),
            kind: ConstantKind::Text,
        });
    }

    // `@@name`, `@@session.name`, `@name`: a variable, by the value a client reads it
    // as.
    for prefix in ["@@", "@"] {
        if let Some(name) = item.strip_prefix(prefix) {
            if prefix == "@" && name.starts_with('@') {
                continue;
            }
            let name = name
                .strip_prefix("session.")
                .or_else(|| name.strip_prefix("global."))
                .or_else(|| name.strip_prefix("local."))
                .unwrap_or(name);
            let (value, kind) = variable(name.trim_matches('`'))?;

            return Some(Constant {
                name: item.to_string(),
                value,
                kind,
            });
        }
    }

    // An integer literal.
    if let Ok(n) = item.parse::<i64>() {
        return Some(Constant {
            name: item.to_string(),
            value: n.to_string(),
            kind: ConstantKind::Integer,
        });
    }

    None
}

/// A statement item with any trailing comment removed.
fn strip_trailing_comment(item: &str) -> &str {
    let bytes = item.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'`' | b'$' => i = skip_literal(bytes, i),
            b'-' | b'#' | b'/' => match skip_comment(bytes, i) {
                Some(_) => return &item[..i],
                None => i += 1,
            },
            _ => i += 1,
        }
    }

    item
}

/// SQL `PREPARE name AS body`, which is a prepared statement made from a statement
/// rather than from a Parse message.
pub fn sql_prepare(sql: &str) -> Option<(&str, &str)> {
    let bytes = sql.as_bytes();
    let start = skip_space(bytes, 0);
    let end = word_end(bytes, start)?;
    if !sql[start..end].eq_ignore_ascii_case("PREPARE") {
        return None;
    }

    // `PREPARE name AS body`, with the name optionally quoted.
    let at = skip_space(bytes, end);
    let (name, after) = if bytes.get(at) == Some(&b'"') {
        let close = skip_quoted(bytes, at);
        (&sql[at + 1..close.saturating_sub(1)], close)
    } else {
        let name_end = word_end(bytes, at)?;
        (&sql[at..name_end], name_end)
    };
    let at = skip_space(bytes, after);
    let as_end = word_end(bytes, at)?;
    if !sql[at..as_end].eq_ignore_ascii_case("AS") {
        return None;
    }

    let body = trim_leading(&sql[skip_space(bytes, as_end)..]);
    if body.is_empty() || name.is_empty() {
        return None;
    }

    Some((name, body))
}

/// The value of a variable a client reads at connect.
fn variable(name: &str) -> Option<(String, ConstantKind)> {
    let text = |value: &str| Some((value.to_string(), ConstantKind::Text));
    let number = |value: &str| Some((value.to_string(), ConstantKind::Integer));

    match name.to_ascii_lowercase().as_str() {
        "version" => text(SERVER_VERSION),
        "version_comment" => text("db-noop: a no-op server for measuring clients"),
        "max_allowed_packet" => number("16777216"),
        "autocommit" => number("1"),
        "auto_increment_increment" => number("1"),
        "auto_increment_offset" => number("1"),
        "tx_isolation" | "transaction_isolation" => text("REPEATABLE-READ"),
        "tx_read_only" | "transaction_read_only" | "read_only" => number("0"),
        "sql_mode" => text(""),
        "character_set_client"
        | "character_set_connection"
        | "character_set_results"
        | "character_set_database"
        | "character_set_server" => text("utf8mb4"),
        "collation_connection" | "collation_server" | "collation_database" => {
            text("utf8mb4_general_ci")
        }
        "time_zone" => text("SYSTEM"),
        "system_time_zone" => text("UTC"),
        "lower_case_table_names" => number("0"),
        "net_write_timeout" | "net_read_timeout" | "wait_timeout" | "interactive_timeout" => {
            number("28800")
        }
        "init_connect" => text(""),
        "license" => text("MIT OR Apache-2.0"),
        "have_query_cache" => text("NO"),
        _ => number("1"),
    }
}

/// Skip a literal or a quoted identifier, returning the offset after it.
///
/// `$tag$ ... $tag$` is PostgreSQL's other literal, and its body may hold
/// anything, semicolons included.
pub fn skip_literal(bytes: &[u8], i: usize) -> usize {
    match bytes.get(i) {
        Some(b'\'' | b'"' | b'`') => skip_quoted(bytes, i),
        Some(b'$') => skip_dollar(bytes, i),
        _ => i + 1,
    }
}

/// Skip `$tag$ ... $tag$`, `i` at its opening `$`. A `$` that opens no tag is a
/// single byte.
fn skip_dollar(bytes: &[u8], i: usize) -> usize {
    let mut end = i + 1;
    while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
        end += 1;
    }
    if bytes.get(end) != Some(&b'$') {
        return i + 1;
    }

    let open = &bytes[i..=end];
    let mut j = end + 1;
    while j + open.len() <= bytes.len() {
        if &bytes[j..j + open.len()] == open {
            return j + open.len();
        }
        j += 1;
    }

    bytes.len()
}

/// The offset after the `)` matching the `(` at `open`, if there is one.
pub fn matching_paren(bytes: &[u8], open: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut i = open;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth = depth.checked_sub(1)?;
                i += 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            b'\'' | b'"' | b'`' | b'$' => i = skip_literal(bytes, i),
            b'-' | b'#' | b'/' => match skip_comment(bytes, i) {
                Some(next) => i = next,
                None => i += 1,
            },
            _ => i += 1,
        }
    }

    None
}

/// Skip a quoted run, `i` at its opening quote.
/// A doubled quote inside is an escaped one, which is how SQL spells a quote in a
/// literal.
pub fn skip_quoted(bytes: &[u8], i: usize) -> usize {
    let quote = bytes[i];
    let mut i = i + 1;
    // A backtick is an identifier: it has no backslash escape.
    let escapes = quote != b'`';
    while i < bytes.len() {
        if escapes && bytes[i] == b'\\' {
            i += 2;

            continue;
        }
        if bytes[i] == quote {
            if bytes.get(i + 1) == Some(&quote) {
                i += 2;

                continue;
            }

            return i + 1;
        }
        i += 1;
    }

    bytes.len()
}

/// Skip a comment, `i` at its first byte, or `None` when what is there only looks like
/// one (`-` that is not `--`, `/` that is not `/*`).
pub fn skip_comment(bytes: &[u8], i: usize) -> Option<usize> {
    match *bytes.get(i)? {
        b'#' => Some(line_end(bytes, i + 1)),
        b'-' if bytes.get(i + 1) == Some(&b'-') => Some(line_end(bytes, i + 2)),
        b'/' if bytes.get(i + 1) == Some(&b'*') => {
            // PostgreSQL's block comments nest.
            let mut depth = 1;
            let mut j = i + 2;
            while j + 1 < bytes.len() {
                if bytes[j] == b'/' && bytes[j + 1] == b'*' {
                    depth += 1;
                    j += 2;
                } else if bytes[j] == b'*' && bytes[j + 1] == b'/' {
                    depth -= 1;
                    j += 2;
                    if depth == 0 {
                        return Some(j);
                    }
                } else {
                    j += 1;
                }
            }

            Some(bytes.len())
        }
        _ => None,
    }
}

fn line_end(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() && bytes[i] != b'\n' {
        i += 1;
    }

    i
}

/// Whitespace, comments included: `select /* x */ 1` is `select 1`.
pub fn skip_space(bytes: &[u8], mut i: usize) -> usize {
    loop {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        match skip_comment(bytes, i) {
            Some(next) if next > i => i = next,
            _ => return i,
        }
    }
}

/// The statement with leading whitespace and comments removed.
pub fn trim_leading(sql: &str) -> &str {
    let start = skip_space(sql.as_bytes(), 0);

    &sql[start..]
}

pub fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$'
}

/// The end of the word starting at `i`, or `None` when there is none there.
pub fn word_end(bytes: &[u8], i: usize) -> Option<usize> {
    if i >= bytes.len() || !is_word_byte(bytes[i]) {
        return None;
    }
    let mut j = i;
    while j < bytes.len() && is_word_byte(bytes[j]) {
        j += 1;
    }

    Some(j)
}

/// The first word of a statement.
pub fn first_word(sql: &str) -> &str {
    let bytes = sql.as_bytes();
    let start = skip_space(bytes, 0);
    match word_end(bytes, start) {
        Some(end) => &sql[start..end],
        None => "",
    }
}

/// The words of a statement, outside quotes and comments -- all this server reads of a
/// statement it does not interpret.
pub struct Words<'a> {
    sql: &'a str,
    pos: usize,
}

impl<'a> Words<'a> {
    pub fn new(sql: &'a str) -> Self {
        Words { sql, pos: 0 }
    }
}

impl<'a> Iterator for Words<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        // Copied out so that a word's borrow is the statement's, not the scan's.
        let sql: &'a str = self.sql;
        let bytes = sql.as_bytes();

        loop {
            self.pos = skip_space(bytes, self.pos);
            if self.pos >= bytes.len() {
                return None;
            }
            match bytes[self.pos] {
                // A literal or a quoted identifier is not a word.
                b'\'' | b'"' | b'`' | b'$' => self.pos = skip_literal(bytes, self.pos),
                c if is_word_byte(c) => {
                    let end = word_end(bytes, self.pos).unwrap_or(bytes.len());
                    let word = &sql[self.pos..end];
                    self.pos = end;

                    return Some(word);
                }
                _ => self.pos += 1,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A semicolon inside a literal or a comment is not a statement boundary, and an
    /// empty segment is a segment: which of them answer is the caller's rule.
    #[test]
    fn statements_split_outside_quotes_and_comments() {
        assert_eq!(
            split_statements("select 1; select 2"),
            vec!["select 1", " select 2"]
        );
        assert_eq!(
            split_statements("select ';' from t"),
            vec!["select ';' from t"]
        );
        assert_eq!(
            split_statements("select 1 -- a; comment\n;select 2"),
            vec!["select 1 -- a; comment\n", "select 2"]
        );
        assert_eq!(split_statements("a;;b"), vec!["a", "", "b"]);
        assert_eq!(split_statements(""), Vec::<&str>::new());
    }

    /// A comma inside a call or a literal is not a column boundary.
    #[test]
    fn commas_split_only_at_the_top_level() {
        assert_eq!(
            split_top_level("a, coalesce(b, 'x,y'), (select 1 from t)"),
            vec!["a", " coalesce(b, 'x,y')", " (select 1 from t)"]
        );
    }

    /// A `?` inside a literal, a comment or an identifier is not a placeholder.
    #[test]
    fn placeholders_are_counted_outside_quotes_and_comments() {
        assert_eq!(count_placeholders("select ?, ?"), 2);
        assert_eq!(count_placeholders("select '?' , ?"), 1);
        assert_eq!(count_placeholders("select `a?b`, ? -- ?\n"), 1);
        assert_eq!(count_placeholders("select /* ? */ ?"), 1);
    }

    /// The select list ends at a top-level clause -- not at one inside a call, a
    /// literal or a subquery's own `from`.
    #[test]
    fn the_select_list_ends_at_its_own_from() {
        assert_eq!(select_list("select 1, 2"), Some("1, 2"));
        assert_eq!(select_list("select a from t"), Some("a "));
        assert_eq!(
            select_list("select f('from'), b from t"),
            Some("f('from'), b ")
        );
        assert_eq!(
            select_list("select (select 1 from t) from u"),
            Some("(select 1 from t) ")
        );
        assert_eq!(select_list("select distinct a from t"), Some("a "));
        assert_eq!(select_list("select 1 limit 1"), Some("1 "));
        assert_eq!(
            select_list("select @@version_comment limit 1"),
            Some("@@version_comment ")
        );
        assert_eq!(select_list("insert into t values (1)"), None);
        assert_eq!(select_list("select"), None);
    }

    /// A keyword's quoted argument is read without its quotes, which is what `LIKE
    /// '<pattern>'` needs.
    #[test]
    fn a_quoted_argument_is_read() {
        assert_eq!(
            quoted_argument("show global status like 'Com%'", "LIKE"),
            Some("Com%")
        );
        assert_eq!(quoted_argument("show global status", "LIKE"), None);
    }

    /// A constant select is literals and variables, and nothing else.
    /// Both protocols answer one row for it and no rows for anything that names data.
    #[test]
    fn a_constant_select_is_literals_and_variables() {
        let items = constant_select("select 1, 'abc', @@version").expect("a constant select");
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].value, "1");
        assert_eq!(items[0].kind, ConstantKind::Integer);
        assert_eq!(items[1].value, "abc");
        assert_eq!(items[1].kind, ConstantKind::Text);
        assert_eq!(items[2].value, SERVER_VERSION);

        // A trailing clause is a clause, not part of the list, and neither is a
        // trailing comment: a padded query is still the literal it pads.
        let items = constant_select("select 1 limit 1").expect("still constant");
        assert_eq!(items.len(), 1);
        let items = constant_select("select 1 /* padding */").expect("still constant");
        assert_eq!(items[0].value, "1");

        for sql in [
            "select a from t",
            "select 1 from t",
            "select now()",
            "select a",
            "insert into t values (1)",
        ] {
            assert_eq!(constant_select(sql), None, "{sql}");
        }
    }

    /// `PREPARE name AS body` is read as a name and a statement, and a statement that
    /// is not one of those is `None`.
    #[test]
    fn a_sql_prepare_is_a_name_and_a_body() {
        assert_eq!(
            sql_prepare("PREPARE neword AS select 1"),
            Some(("neword", "select 1"))
        );
        assert_eq!(
            sql_prepare("prepare \"s1\" as select 1, 2"),
            Some(("s1", "select 1, 2"))
        );
        assert_eq!(sql_prepare("PREPARE x"), None, "no AS");
        assert_eq!(sql_prepare("PREPARE AS select 1"), None, "no name");
        assert_eq!(sql_prepare("select 1"), None);
        assert_eq!(sql_prepare("DEALLOCATE neword"), None);
    }

    /// **A dollar-quoted body is one literal**, and may hold anything.
    #[test]
    fn a_dollar_quoted_body_is_one_literal() {
        assert_eq!(
            split_statements("select $$ a; b $$; select 2"),
            vec!["select $$ a; b $$", " select 2"]
        );
        assert_eq!(
            split_statements("select $fn$ a; b $fn$, 1 from t"),
            vec!["select $fn$ a; b $fn$, 1 from t"]
        );
        assert_eq!(split_top_level("$q$a,b$q$, c"), vec!["$q$a,b$q$", " c"]);
        assert_eq!(skip_literal(b"$q$a;b$q$ tail", 0), 9);
    }

    /// **A leading comment is not a word.** `/* x */ INSERT` classified as
    /// unknown text on PostgreSQL, because its scanner started at the comment.
    #[test]
    fn a_leading_comment_is_not_the_first_word() {
        assert_eq!(first_word("/* x */ insert into t values (1)"), "insert");
        assert_eq!(first_word("-- x\ninsert into t values (1)"), "insert");
        assert_eq!(first_word("/* a /* nested */ b */ select 1"), "select");
        assert_eq!(first_word("  # x\nupdate t set a = 1"), "update");
    }

    /// The words of a statement skip literals, identifiers and comments, so a keyword
    /// inside one is not a word.
    #[test]
    fn words_skip_quotes_and_comments() {
        let words: Vec<&str> =
            Words::new("select 'call', `payment`, 1 -- neword\n from t").collect();
        assert_eq!(words, vec!["select", "1", "from", "t"]);
    }
}
