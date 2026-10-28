//! The name the scripts written for the PostgreSQL-only server use.
//!
//! Kept as a second binary rather than an alias in the documentation: a script
//! with `pgnoop` in it keeps working after an upgrade, which is the whole point
//! of keeping the name.

fn main() {
    db_noop::run();
}
