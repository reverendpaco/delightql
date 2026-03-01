#[derive(Debug, Clone, Copy, PartialEq, Default)]
#[allow(dead_code)]
pub enum SqlDialect {
    #[default]
    SQLite,
    PostgreSQL,
    MySQL,
    SqlServer,
}
