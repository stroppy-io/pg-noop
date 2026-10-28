//! The little SQL this server reads.
//!
//! A no-op does not parse SQL. It reads four things, and this is the code that
//! reads them: where a statement ends (multi-statement queries), how many
//! placeholders it has (a prepare response), the first word (which decides the
//! answer's shape), and the select list (the columns of a result set). The
//! TPC-C rules in `crate::tpcc` read the same words, which is why the scanner is
//! here rather than inside a protocol.
//!
//! Everything is done on bytes, outside quotes and comments: a `;` inside a
//! literal is data, a `-- from` comment is not a clause, and a comma inside a
//! function call is not a column boundary.

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
            b'\'' | b'"' | b'`' => i = skip_quoted(bytes, i),
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

/// Split a multi-statement query on top-level `;`, keeping empty segments: which
/// of them answer is the caller's rule.
///
/// **A stored routine's body is one statement.** `CREATE PROCEDURE … BEGIN … ;
/// … END` is how every driver sends one -- there is no DELIMITER on the wire --
/// and splitting it at the body's semicolons answers each line of the body as a
/// statement: the client sees "Commands out of sync", and the `BEGIN` piece
/// arrives as a transaction of its own. A `BEGIN … END` body is therefore
/// counted, and its semicolons do not split.
///
/// `END IF`, `END LOOP`, `END WHILE` and `END REPEAT` close forms that opened
/// nothing here, so they do not decrement; `BEGIN` and `CASE` do -- a `CASE`
/// expression inside a body closes with an `END` of its own, and counting it is
/// what keeps that `END` from ending the routine.
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
            b'\'' | b'"' | b'`' => i = skip_quoted(bytes, i),
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
                    // `CREATE [DEFINER = …] PROCEDURE|FUNCTION|TRIGGER|EVENT`,
                    // and the few words in between, before the body starts.
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
                    // A block opens with BEGIN, and a CASE -- expression or
                    // statement -- closes with an END of its own.
                    *body.as_mut().expect("in a body") += 1;
                } else if word.eq_ignore_ascii_case("END") {
                    let next = skip_space(bytes, i);
                    // `END IF`, `END LOOP`, `END WHILE` and `END REPEAT` close
                    // forms that opened nothing here.
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

/// How many placeholders a statement has: `?` outside quotes, comments and
/// identifiers.
pub fn count_placeholders(sql: &str) -> u16 {
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut count = 0u16;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'`' => i = skip_quoted(bytes, i),
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
///
/// Top level is outside parentheses, quotes and comments: `select coalesce(a,
/// 'from')` names no table, and `select (select 1 from t)` does.
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
            b'\'' | b'"' | b'`' => i = skip_quoted(bytes, i),
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

/// The text of a keyword's argument when it is a single-quoted literal:
/// `LIKE 'Com%'` gives `Com%`.
pub fn quoted_argument<'a>(sql: &'a str, keyword: &str) -> Option<&'a str> {
    let at = find_keyword(sql, 0, keyword)?;
    let rest = &sql[at + keyword.len()..];
    let rest = trim_leading(rest);
    let rest = rest.strip_prefix('\'')?;
    let end = rest.find('\'')?;

    Some(&rest[..end])
}

/// The select list: the text between `SELECT` (and `DISTINCT`) and the first
/// top-level `FROM`, or the end of the statement. `None` when the statement does
/// not start with `SELECT` or has no list.
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

    let stop = find_keyword(sql, i, "FROM").unwrap_or(bytes.len());
    let list = &sql[i..stop];
    if list.trim().is_empty() {
        return None;
    }

    Some(list)
}

/// Skip a quoted run, `i` at its opening quote. A doubled quote inside is an
/// escaped one, which is how SQL spells a quote in a literal.
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

/// Skip a comment, `i` at its first byte, or `None` when what is there only looks
/// like one (`-` that is not `--`, `/` that is not `/*`).
pub fn skip_comment(bytes: &[u8], i: usize) -> Option<usize> {
    match *bytes.get(i)? {
        b'#' => Some(line_end(bytes, i + 1)),
        b'-' if bytes.get(i + 1) == Some(&b'-') => Some(line_end(bytes, i + 2)),
        b'/' if bytes.get(i + 1) == Some(&b'*') => {
            let mut j = i + 2;
            while j + 1 < bytes.len() {
                if bytes[j] == b'*' && bytes[j + 1] == b'/' {
                    return Some(j + 2);
                }
                j += 1;
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

/// The words of a statement, outside quotes and comments -- all this server
/// reads of a statement it does not interpret.
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
                b'\'' | b'"' | b'`' => self.pos = skip_quoted(bytes, self.pos),
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

    /// A semicolon inside a literal or a comment is not a statement boundary,
    /// and an empty segment is a segment: which of them answer is the caller's
    /// rule.
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

    /// The select list ends at a top-level `FROM` -- not at one inside a call,
    /// a literal or a subquery's own `from`.
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
        assert_eq!(select_list("insert into t values (1)"), None);
        assert_eq!(select_list("select"), None);
    }

    /// A keyword's quoted argument is read without its quotes, which is what
    /// `LIKE '<pattern>'` needs.
    #[test]
    fn a_quoted_argument_is_read() {
        assert_eq!(
            quoted_argument("show global status like 'Com%'", "LIKE"),
            Some("Com%")
        );
        assert_eq!(quoted_argument("show global status", "LIKE"), None);
    }

    /// The words of a statement skip literals, identifiers and comments, so a
    /// keyword inside one is not a word.
    #[test]
    fn words_skip_quotes_and_comments() {
        let words: Vec<&str> =
            Words::new("select 'call', `payment`, 1 -- neword\n from t").collect();
        assert_eq!(words, vec!["select", "1", "from", "t"]);
    }
}
