// SQL AST V3 - A Proper SQL Syntax Tree
//
// This AST models SQL as SQL actually is - following its syntactic structure
// rather than trying to mix semantics and syntax. Based on SQLite's grammar
// but limited to the subset that DelightQL actually generates.
//
// Key principles:
// 1. CTEs live at statement level, not mixed into expressions
// 2. Subqueries are first-class with required aliases
// 3. No weird variants like CteRef - just tables and subqueries
// 4. Models SQL's actual grammar, not our semantic model

// ============================================================================
// SQL Grammar Reference (What We're Modeling)
// ============================================================================

// SQL Grammar Reference (subset that DelightQL generates):
//
//   statement       = [ with_clause ] query_expr
//   with_clause     = 'WITH' cte ( ',' cte )*
//   cte             = identifier 'AS' '(' query_expr ')'
//   query_expr      = select | query_expr 'UNION' ['ALL'] query_expr | 'VALUES' row (',' row)*
//   select          = 'SELECT' ['DISTINCT'] select_item (',' select_item)*
//                     ['FROM' table_expr (',' table_expr)*] ['WHERE' expr]
//                     ['GROUP BY' expr (',' expr)*] ['HAVING' expr]
//                     ['ORDER BY' order_term (',' order_term)*] ['LIMIT' number ['OFFSET' number]]
//   table_expr      = qualified_table [['AS'] alias] | '(' query_expr ')' ['AS'] alias
//                   | table_expr 'JOIN' table_expr 'ON' expr | table_expr 'LEFT JOIN' table_expr 'ON' expr
//                   | table_expr 'JOIN' table_expr 'USING' '(' column_list ')'
//   qualified_table = [ schema '.' ] table_name
//   select_item     = '*' | table_name '.' '*' | expr [['AS'] alias]
//   expr            = literal | [qualifier '.'] column_name | expr binary_op expr
//                   | function_name '(' [expr (',' expr)* | '*'] ')' | '(' expr ')' | 'CASE' ... 'END'
//   order_term      = expr ['ASC' | 'DESC']
//   row             = '(' expr (',' expr)* ')'

// Module declarations
pub mod builders;
pub mod expressions;
pub mod operators;
pub mod ordering;
pub mod query;
pub mod select_items;
pub mod statements;
pub mod table;

// Re-export all public types for backward compatibility
pub use builders::SelectBuilder;
pub use expressions::{
    ColumnQualifier, DomainExpression, QualifierParts, SqlFrameBound, SqlFrameMode, SqlWindowFrame,
    WhenClause,
};
pub use operators::{BinaryOperator, UnaryOperator};
pub use ordering::{OrderDirection, OrderTerm};
pub use query::{QueryExpression, SelectStatement, SetOperator};
pub use select_items::SelectItem;
pub use statements::{Cte, SqlStatement};
pub use table::{JoinCondition, JoinType, TableExpression, TvfArgument};
