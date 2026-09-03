// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// SQL Generator V3 - From SQL AST V3 to SQL String
//
// This generator converts our SQL AST V3 structures into actual SQL strings.
// It follows the principle of being a "trivial tree walker" that simply
// renders the AST to text, with proper formatting and dialect handling.
//
// Key principles:
// 1. Pure functions - no mutable state during generation
// 2. Dialect-aware - handle differences between SQL dialects
// 3. Proper formatting - indentation for readability
// 4. Safety - quote identifiers when needed

use std::sync::Arc;

use crate::bin_cartridge::registry::BinCartridgeRegistry;
use crate::names::{Baptised, ColId, EntityId, FnId, ScopeId, SqlOut};
use crate::pipeline::sql_ast::*;
use std::fmt::Write;

mod config;
mod dialect;
#[cfg(test)]
mod dialect_tests;
mod errors;
mod identifiers;
mod literals;
mod operators;

pub use config::GeneratorConfig;
pub use dialect::{RowClauseStyle, SqlDialect};
pub use errors::GeneratorError;

pub fn baptise_statements<'registry>(
    identities: &'registry crate::names::Registry,
    statements: &[&SqlStatement],
) -> Result<Baptised<'registry>, GeneratorError> {
    let bundle = crate::names::Bundle::gather(
        statements
            .iter()
            .map(|statement| {
                crate::pipeline::sql_ast::names::statement_names(statement, identities)
            })
            .collect(),
    )
    .reserve_authored(identities);
    crate::names::baptise(identities, &bundle)
        .map_err(|error| GeneratorError::Error(format!("SQL naming failed: {error:?}")))
}

/// The main SQL generator
/// Does this FROM entry stand on `scope`?
///
/// Only the entries the statement itself stands on — a scope named inside
/// a subquery beneath one belongs to that subquery's own emission.
fn reads_scope(table: &TableExpression, scope: ScopeId) -> bool {
    match table {
        TableExpression::Scope(found) | TableExpression::QualifiedScope { scope: found, .. } => {
            *found == scope
        }
        TableExpression::Join { left, right, .. } => {
            reads_scope(left, scope) || reads_scope(right, scope)
        }
        TableExpression::Entity {
            alias: Some(alias), ..
        } => *alias == scope,
        TableExpression::Subquery { alias, .. } | TableExpression::TVF { alias, .. } => {
            *alias == scope
        }
        _ => false,
    }
}

/// Where a statement emits from, and whether it also READS that scope.
///
/// A statement's own scope needs no qualifier — unless the statement
/// stands on it too. A recursive CTE's step member is the one shape that
/// does: `FROM c` inside the body of `WITH RECURSIVE c`. There a bare
/// column name is one two FROM entries may both publish, and only this
/// statement knows it.
#[derive(Clone, Copy)]
pub(crate) struct Emitting {
    pub scope: ScopeId,
    pub reflexive: bool,
    unqualified: bool,
}

impl Emitting {
    fn at(scope: ScopeId) -> Self {
        Self {
            scope,
            reflexive: false,
            unqualified: false,
        }
    }

    fn ddl(scope: ScopeId) -> Self {
        Self {
            scope,
            reflexive: false,
            unqualified: true,
        }
    }
}

pub struct SqlGenerator<'names, 'registry> {
    names: &'names Baptised<'registry>,
    config: GeneratorConfig,
    /// Bin cartridge registry for resolving rewrite-rule predicates.
    /// None for standalone/utility generation paths.
    bin_registry: Option<Arc<BinCartridgeRegistry>>,
}

impl<'names, 'registry> SqlGenerator<'names, 'registry> {
    pub fn new(names: &'names Baptised<'registry>) -> Self {
        SqlGenerator {
            names,
            config: GeneratorConfig::default(),
            bin_registry: None,
        }
    }

    pub fn with_bin_registry(mut self, registry: Arc<BinCartridgeRegistry>) -> Self {
        self.bin_registry = Some(registry);
        self
    }

    /// Target a specific SQL dialect (canonical default is SQLite).
    pub fn with_dialect(mut self, dialect: SqlDialect) -> Self {
        self.config.dialect = dialect;
        self
    }

    /// Attach the per-compile dialect pack (the in-memory image of the
    /// dialect_* targeting tables). Without one, every render falls back
    /// to the canonical code default.
    pub fn with_dialect_pack(
        mut self,
        pack: Arc<crate::pipeline::dialect_pack::DialectPack>,
    ) -> Self {
        self.config.dialect_pack = pack;
        self
    }

    /// Render a SQL-layer domain expression to a string.
    ///
    /// Used by the DDL pipeline generator for CHECK/DEFAULT expressions.
    pub(crate) fn render_expression(
        &self,
        expr: &DomainExpression,
        at: ScopeId,
    ) -> Result<String, GeneratorError> {
        let mut sql = String::new();
        self.generate_domain_expression(&mut sql, expr, Some(Emitting::at(at)))?;
        Ok(sql)
    }

    /// Render a DDL CHECK or DEFAULT expression. SQL column definitions do
    /// not introduce a table alias, so their column references are always
    /// unqualified even when resolution crossed an internal occurrence.
    pub(crate) fn render_ddl_expression(
        &self,
        expr: &DomainExpression,
        at: ScopeId,
    ) -> Result<String, GeneratorError> {
        let mut sql = String::new();
        self.generate_domain_expression(&mut sql, expr, Some(Emitting::ddl(at)))?;
        Ok(sql)
    }

    fn write_name(
        &self,
        sql: &mut String,
        write: impl FnOnce(&Baptised<'registry>, &mut SqlOut<'_>),
    ) -> Result<(), GeneratorError> {
        let mut dialect_writer = |output: &mut String, text: &str, stropped: bool| {
            if text == "." {
                output.push('.');
                return Ok(());
            }
            identifiers::write_identifier_with_stropping(
                output,
                text,
                stropped,
                self.config.dialect,
                &self.config.dialect_pack,
            )
            .map_err(|error| match error {
                GeneratorError::Error(message) => message,
                GeneratorError::Typed(error) => error.to_string(),
            })
        };
        let mut output = SqlOut::new(sql, &mut dialect_writer);
        write(self.names, &mut output);
        output.finish().map_err(GeneratorError::Error)
    }

    pub(crate) fn write_scope(
        &self,
        sql: &mut String,
        scope: ScopeId,
    ) -> Result<(), GeneratorError> {
        if !self.names.knows_scope(scope) {
            return Err(GeneratorError::Error(format!(
                "scope {scope:?} was not included in the baptism bundle"
            )));
        }
        self.write_name(sql, |names, output| names.write_scope(scope, output))
    }

    pub(crate) fn write_column(
        &self,
        sql: &mut String,
        column: ColId,
    ) -> Result<(), GeneratorError> {
        if !self.names.knows_column(column) {
            return Err(GeneratorError::Error(format!(
                "column {column:?} was not included in the baptism bundle"
            )));
        }
        self.write_name(sql, |names, output| names.write_column(column, output))
    }

    fn write_forced_identifier(
        &self,
        sql: &mut String,
        write: impl FnOnce(&Baptised<'registry>, &mut SqlOut<'_>),
    ) -> Result<(), GeneratorError> {
        let mut dialect_writer = |output: &mut String, text: &str, _stropped: bool| {
            if text == "." {
                output.push('.');
                return Ok(());
            }
            identifiers::write_identifier_with_stropping(
                output,
                text,
                true,
                self.config.dialect,
                &self.config.dialect_pack,
            )
            .map_err(|error| match error {
                GeneratorError::Error(message) => message,
                GeneratorError::Typed(error) => error.to_string(),
            })
        };
        let mut output = SqlOut::new(sql, &mut dialect_writer);
        write(self.names, &mut output);
        output.finish().map_err(GeneratorError::Error)
    }

    pub(crate) fn write_quoted_scope(
        &self,
        sql: &mut String,
        scope: ScopeId,
    ) -> Result<(), GeneratorError> {
        if !self.names.knows_scope(scope) {
            return Err(GeneratorError::Error(format!(
                "scope {scope:?} was not included in the baptism bundle"
            )));
        }
        self.write_forced_identifier(sql, |names, output| names.write_scope(scope, output))
    }

    pub(crate) fn write_quoted_column(
        &self,
        sql: &mut String,
        column: ColId,
    ) -> Result<(), GeneratorError> {
        if !self.names.knows_column(column) {
            return Err(GeneratorError::Error(format!(
                "column {column:?} was not included in the baptism bundle"
            )));
        }
        self.write_forced_identifier(sql, |names, output| names.write_column(column, output))
    }

    fn write_ref(
        &self,
        sql: &mut String,
        column: ColId,
        at: Emitting,
    ) -> Result<(), GeneratorError> {
        if !self.names.knows_column(column) {
            return Err(GeneratorError::Error(format!(
                "column reference {column:?} was not included in the baptism bundle"
            )));
        }
        if at.unqualified {
            self.write_column(sql, column)
        } else {
            self.write_name(sql, |names, output| {
                names.write_ref(column, at.scope, at.reflexive, output)
            })
        }
    }

    fn write_entity(&self, sql: &mut String, entity: EntityId) -> Result<(), GeneratorError> {
        self.write_name(sql, |names, output| names.write_entity(entity, output))
    }

    fn write_function_namespace(
        &self,
        sql: &mut String,
        function: FnId,
    ) -> Result<(), GeneratorError> {
        self.write_name(sql, |names, output| {
            names.write_function_namespace(function, output)
        })
    }

    fn write_tvf(
        &self,
        sql: &mut String,
        function: FnId,
        arguments: &[TvfArgument],
        alias: ScopeId,
        at: Emitting,
    ) -> Result<(), GeneratorError> {
        let mut rendered_args = Vec::with_capacity(arguments.len());
        for argument in arguments {
            let mut rendered = String::new();
            match argument {
                TvfArgument::Literal(value) => literals::generate_literal(
                    &mut rendered,
                    value,
                    self.config.dialect,
                    &self.config.dialect_pack,
                )?,
                TvfArgument::Column(column) => {
                    self.write_ref(&mut rendered, *column, at)?;
                }
            }
            rendered_args.push(rendered);
        }

        let origin = self.names.function_origin(function);
        let write_tvf_name =
            |output: &mut String,
             text: &str,
             stropped: bool,
             intrinsic: Option<crate::names::Intrinsic>| {
                let guarded_args = if intrinsic == Some(crate::names::Intrinsic::JsonEachArray) {
                    rendered_args
                        .iter()
                        .map(|argument| {
                            format!(
                            "CASE WHEN json_valid({argument}) AND json_type({argument}) = 'array' \
                             THEN {argument} END"
                        )
                        })
                        .collect::<Vec<_>>()
                } else {
                    rendered_args.clone()
                };
                let write_arguments = |output: &mut String, arguments: &[String]| {
                    output.push('(');
                    for (position, argument) in arguments.iter().enumerate() {
                        if position > 0 {
                            output.push_str(", ");
                        }
                        output.push_str(argument);
                    }
                    output.push(')');
                };

                let tvf_key = intrinsic.map_or_else(
                    || format!("tvf.{}", text.to_ascii_lowercase()),
                    |intrinsic| format!("tvf.{intrinsic:?}"),
                );
                let rule = match intrinsic {
                    Some(intrinsic) => self
                        .config
                        .dialect_pack
                        .render_intrinsic_tvf(self.config.dialect.family_name(), intrinsic),
                    None => self
                        .config
                        .dialect_pack
                        .render(self.config.dialect.family_name(), &tvf_key),
                };
                match rule {
                    Some(rule) if rule.rule_kind == "template" => {
                        let body = rule.template()?;
                        if body.contains('{') {
                            let arguments =
                                rendered_args.iter().map(String::as_str).collect::<Vec<_>>();
                            let applied =
                                crate::pipeline::dialect_pack::apply_template(body, &arguments)
                                    .map_err(|error| format!("{tvf_key}: {error}"))?;
                            output.push_str(&applied);
                        } else {
                            self.write_function_namespace(output, function)
                                .map_err(|error| match error {
                                    GeneratorError::Error(message) => message,
                                    GeneratorError::Typed(error) => error.to_string(),
                                })?;
                            output.push_str(body);
                            write_arguments(output, &guarded_args);
                        }
                    }
                    Some(rule) => {
                        return Err(format!(
                            "{}: unsupported rule_kind '{}' (no interpreter built for it)",
                            tvf_key, rule.rule_kind
                        ));
                    }
                    None => {
                        self.write_function_namespace(output, function).map_err(
                            |error| match error {
                                GeneratorError::Error(message) => message,
                                GeneratorError::Typed(error) => error.to_string(),
                            },
                        )?;
                        identifiers::write_identifier_with_stropping(
                            output,
                            text,
                            stropped,
                            self.config.dialect,
                            &self.config.dialect_pack,
                        )
                        .map_err(|error| match error {
                            GeneratorError::Error(message) => message,
                            GeneratorError::Typed(error) => error.to_string(),
                        })?;
                        write_arguments(output, &guarded_args);
                    }
                }
                Ok(())
            };
        match origin {
            crate::names::FnOrigin::User(_) => {
                let mut name_writer = |output: &mut String, text: &str, stropped: bool| {
                    write_tvf_name(output, text, stropped, None)
                };
                let mut output = SqlOut::new(sql, &mut name_writer);
                self.names
                    .write_function_name(function, &mut output)
                    .map_err(|error| {
                        GeneratorError::Error(format!(
                            "function {function:?} has no callable spelling: {error:?}"
                        ))
                    })?;
                output.finish().map_err(GeneratorError::Error)?;
            }
            crate::names::FnOrigin::Intrinsic(intrinsic) => {
                let canonical = intrinsic.canonical().ok_or_else(|| {
                    GeneratorError::Error(format!(
                        "intrinsic {intrinsic:?} has no callable TVF spelling"
                    ))
                })?;
                write_tvf_name(sql, canonical, false, Some(intrinsic))
                    .map_err(GeneratorError::Error)?;
            }
        }
        sql.push_str(" AS ");
        self.write_scope(sql, alias)
    }

    /// Write a reported name into a SQL string literal.
    ///
    /// What lands here is DQL source spelling — the characters a reader
    /// would have to type — so a segment the registry marks stropped is
    /// written with its delimiters. Dropping them yields `a b|1|`, which
    /// reaches nothing. A segment carrying no stropping is written plain,
    /// which is how a baptized heading name (matched against the wire
    /// heading, not typed) stays free of delimiters.
    fn write_report_literal(
        &self,
        sql: &mut String,
        report: impl FnOnce(&Baptised<'registry>, &mut SqlOut<'_>),
    ) -> Result<(), GeneratorError> {
        sql.push('\'');
        let mut literal_writer = |output: &mut String, text: &str, stropped: bool| {
            let quoted = text.replace('\'', "''");
            if stropped {
                output.push('`');
                output.push_str(&quoted);
                output.push('`');
            } else {
                output.push_str(&quoted);
            }
            Ok(())
        };
        let mut output = SqlOut::new(sql, &mut literal_writer);
        report(self.names, &mut output);
        output.finish().map_err(GeneratorError::Error)?;
        sql.push('\'');
        Ok(())
    }

    fn write_column_literal(&self, sql: &mut String, column: ColId) -> Result<(), GeneratorError> {
        if !self.names.knows_column(column) {
            return Err(GeneratorError::Error(format!(
                "literal column name {column:?} was not included in the baptism bundle"
            )));
        }
        self.write_report_literal(sql, |names, out| names.write_column_report(column, out))
    }

    /// A scope as a VALUE in a result row, which is not the alias it is
    /// called in the SQL text. Baptism is not asked: its aliases are
    /// invented per emission and uniquified against everything else that
    /// emission names, so reading one here would make the same construct
    /// over two sources report two different scopes.
    fn write_scope_literal(&self, sql: &mut String, scope: ScopeId) -> Result<(), GeneratorError> {
        self.write_report_literal(sql, |names, out| names.write_answers_to(scope, out))
    }

    fn write_json_path(
        &self,
        sql: &mut String,
        write_segments: impl FnOnce(&Baptised<'registry>, &mut SqlOut<'_>),
    ) -> Result<(), GeneratorError> {
        sql.push_str("'$");
        let mut path_writer = |output: &mut String, text: &str, _stropped: bool| {
            output.push_str(".\"");
            output.push_str(
                &text
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"")
                    .replace('\'', "''"),
            );
            output.push('"');
            Ok(())
        };
        let mut output = SqlOut::new(sql, &mut path_writer);
        write_segments(self.names, &mut output);
        output.finish().map_err(GeneratorError::Error)?;
        sql.push('\'');
        Ok(())
    }

    fn write_published_json_path(
        &self,
        sql: &mut String,
        column: ColId,
    ) -> Result<(), GeneratorError> {
        if !self.names.knows_column(column) {
            return Err(GeneratorError::Error(format!(
                "JSON-path column {column:?} was not included in the baptism bundle"
            )));
        }
        self.write_json_path(sql, |names, output| names.write_column(column, output))
    }

    /// A typed reach, rendered: keys quoted and escaped, indices
    /// subscripted. One renderer, so a reach means the same thing wherever
    /// it was declared.
    fn write_typed_json_path(&self, sql: &mut String, path: &crate::pipeline::asts::core::Path) {
        use crate::pipeline::asts::core::PathStep;
        sql.push_str("'$");
        for step in path.steps() {
            match step {
                PathStep::Key(key) => {
                    sql.push_str(".\"");
                    sql.push_str(
                        &key.replace('\\', "\\\\")
                            .replace('"', "\\\"")
                            .replace('\'', "''"),
                    );
                    sql.push('"');
                }
                PathStep::Index(index) => {
                    sql.push_str(&format!("[{index}]"));
                }
            }
        }
        sql.push('\'');
    }

    fn write_relation_target(
        &self,
        sql: &mut String,
        target: &crate::pipeline::sql_ast::statements::RelationTarget,
    ) -> Result<(), GeneratorError> {
        match target {
            crate::pipeline::sql_ast::statements::RelationTarget::Entity(entity) => {
                self.write_entity(sql, *entity)
            }
            crate::pipeline::sql_ast::statements::RelationTarget::Scope(scope) => {
                self.write_scope(sql, *scope)
            }
            crate::pipeline::sql_ast::statements::RelationTarget::QualifiedScope {
                schema,
                scope,
            } => {
                if schema == "temp" {
                    sql.push_str("\"temp\"");
                } else {
                    sql.push_str(schema);
                }
                sql.push('.');
                self.write_scope(sql, *scope)
            }
        }
    }

    /// Emit a `name(args)` call with the canonical shape (parens, DISTINCT,
    /// comma-joined args).
    fn write_fn_call(
        &self,
        sql: &mut String,
        name: &str,
        args: &[DomainExpression],
        distinct: bool,
        at: Option<Emitting>,
    ) -> Result<(), GeneratorError> {
        sql.push_str(name);
        sql.push('(');
        if distinct {
            sql.push_str("DISTINCT ");
        }
        for (i, arg) in args.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            self.generate_domain_expression(sql, arg, at)?;
        }
        sql.push(')');
        Ok(())
    }

    /// Render each function argument to its own string (for template and
    /// rust_handler render rules, which compose from rendered args).
    fn render_fn_args(
        &self,
        args: &[DomainExpression],
        at: Option<Emitting>,
    ) -> Result<Vec<String>, GeneratorError> {
        args.iter()
            .map(|arg| {
                let mut s = String::new();
                self.generate_domain_expression(&mut s, arg, at)?;
                Ok(s)
            })
            .collect()
    }

    /// Generate SQL from a complete statement
    pub fn generate_statement(&self, stmt: &SqlStatement) -> Result<String, GeneratorError> {
        let mut sql = String::new();

        match stmt {
            SqlStatement::DropTempTable { table } => {
                sql.push_str("DROP TABLE IF EXISTS ");
                self.write_scope(&mut sql, *table)?;
            }
            SqlStatement::Query { with_clause, query } => {
                // Generate WITH clause if present
                if let Some(ctes) = with_clause {
                    self.generate_with_clause(&mut sql, ctes, 0)?;
                    if self.config.pretty_print {
                        sql.push('\n');
                    } else {
                        sql.push(' ');
                    }
                }

                // Generate main query
                self.generate_query_expression(&mut sql, query, 0)?;
            }
            SqlStatement::CreateTempTable {
                table,
                with_clause,
                query,
            } => {
                // Generate CREATE TEMPORARY TABLE statement
                sql.push_str("CREATE TEMPORARY TABLE ");
                self.write_scope(&mut sql, *table)?;
                sql.push_str(" AS ");

                if self.config.pretty_print {
                    sql.push('\n');
                }

                // Generate WITH clause if present
                if let Some(ctes) = with_clause {
                    self.generate_with_clause(&mut sql, ctes, 0)?;
                    if self.config.pretty_print {
                        sql.push('\n');
                    } else {
                        sql.push(' ');
                    }
                }

                // Generate the query that populates the table
                self.generate_query_expression(&mut sql, query, 0)?;
            }
            SqlStatement::CreateTempView {
                view,
                with_clause,
                query,
            } => {
                // Generate CREATE TEMPORARY VIEW statement
                sql.push_str("CREATE TEMPORARY VIEW ");
                self.write_scope(&mut sql, *view)?;
                sql.push_str(" AS ");

                if self.config.pretty_print {
                    sql.push('\n');
                }

                // Generate WITH clause if present
                if let Some(ctes) = with_clause {
                    self.generate_with_clause(&mut sql, ctes, 0)?;
                    if self.config.pretty_print {
                        sql.push('\n');
                    } else {
                        sql.push(' ');
                    }
                }

                // Generate the query that defines the view
                self.generate_query_expression(&mut sql, query, 0)?;
            }
            SqlStatement::Delete {
                target,
                target_scope,
                with_clause,
                where_clause,
            } => {
                if let Some(ctes) = with_clause {
                    self.generate_with_clause(&mut sql, ctes, 0)?;
                    sql.push(' ');
                }
                sql.push_str("DELETE FROM ");
                self.write_relation_target(&mut sql, target)?;
                if let Some(wc) = where_clause {
                    sql.push_str(" WHERE ");
                    self.generate_domain_expression(
                        &mut sql,
                        wc,
                        Some(Emitting::at(*target_scope)),
                    )?;
                }
            }
            SqlStatement::Update {
                target,
                target_scope,
                with_clause,
                set_clause,
                where_clause,
            } => {
                if let Some(ctes) = with_clause {
                    self.generate_with_clause(&mut sql, ctes, 0)?;
                    sql.push(' ');
                }
                sql.push_str("UPDATE ");
                self.write_relation_target(&mut sql, target)?;
                sql.push_str(" SET ");
                for (i, (col, expr)) in set_clause.iter().enumerate() {
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    self.write_column(&mut sql, *col)?;
                    sql.push_str(" = ");
                    self.generate_domain_expression(
                        &mut sql,
                        expr,
                        Some(Emitting::at(*target_scope)),
                    )?;
                }
                if let Some(wc) = where_clause {
                    sql.push_str(" WHERE ");
                    self.generate_domain_expression(
                        &mut sql,
                        wc,
                        Some(Emitting::at(*target_scope)),
                    )?;
                }
            }
            SqlStatement::Insert {
                target,
                target_scope: _,
                columns,
                with_clause,
                source,
            } => {
                if let Some(ctes) = with_clause {
                    self.generate_with_clause(&mut sql, ctes, 0)?;
                    sql.push(' ');
                }
                sql.push_str("INSERT INTO ");
                self.write_relation_target(&mut sql, target)?;
                if !columns.is_empty() {
                    sql.push_str(" (");
                    for (i, col) in columns.iter().enumerate() {
                        if i > 0 {
                            sql.push_str(", ");
                        }
                        self.write_column(&mut sql, *col)?;
                    }
                    sql.push(')');
                }
                sql.push(' ');
                self.generate_query_expression(&mut sql, source, 0)?;
            }
        }

        Ok(sql)
    }

    /// Generate WITH clause
    fn generate_with_clause(
        &self,
        sql: &mut String,
        ctes: &[Cte],
        indent: usize,
    ) -> Result<(), GeneratorError> {
        // Check if any CTE is recursive
        let has_recursive = ctes.iter().any(|cte| cte.is_recursive());

        if has_recursive {
            sql.push_str("WITH RECURSIVE ");
        } else {
            sql.push_str("WITH ");
        }

        for (i, cte) in ctes.iter().enumerate() {
            if i > 0 {
                sql.push(',');
                if self.config.pretty_print {
                    sql.push('\n');
                    self.indent(sql, indent);
                } else {
                    sql.push(' ');
                }
            }

            // CTE name
            self.write_scope(sql, cte.scope())?;
            if cte.materialized_once() {
                // ONCE-ONLY IS A TARGET CAPABILITY, and this is where the
                // target answers. A closed configured rule value is
                // evaluated where it is constructed and read wherever it
                // is spent; three families say so in the CTE itself.
                // MySQL and SQL Server have no spelling that forbids
                // re-evaluation, so a plain CTE there would silently
                // re-run a volatile configuration once per spend. The
                // refusal is the honest answer; emitting `AS (` would be
                // a guarantee the target does not make.
                match self.config.dialect {
                    SqlDialect::SQLite | SqlDialect::PostgreSQL | SqlDialect::DuckDB => {
                        sql.push_str(" AS MATERIALIZED (");
                    }
                    SqlDialect::MySQL | SqlDialect::SqlServer => {
                        return Err(GeneratorError::Error(format!(
                            "{:?} cannot guarantee the required once-only materialization of a closed configured rule value",
                            self.config.dialect
                        )));
                    }
                }
            } else {
                sql.push_str(" AS (");
            }

            // CTE body (indented if pretty printing)
            if self.config.pretty_print {
                sql.push('\n');
                self.generate_cte_body(sql, cte.body(), indent + 1)?;
                sql.push('\n');
                self.indent(sql, indent);
            } else {
                self.generate_cte_body(sql, cte.body(), indent)?;
            }

            sql.push(')');
        }

        Ok(())
    }

    /// A CTE's body.
    ///
    /// A FIXPOINT EMITS ITS OWN PARTS. The anchor and the members are
    /// structure, and the keyword between them is the accumulation the
    /// recursion decision chose — read off the body, not matched against a
    /// token some node happens to carry. There is nothing here to place
    /// wrongly, so nothing to detect.
    #[stacksafe::stacksafe]
    fn generate_cte_body(
        &self,
        sql: &mut String,
        body: &crate::pipeline::sql_ast::CteBody,
        indent: usize,
    ) -> Result<(), GeneratorError> {
        use crate::pipeline::sql_ast::CteBody;
        match body {
            CteBody::Ordinary(query) => self.generate_query_expression(sql, query, indent),
            CteBody::Fixpoint(fixpoint) => {
                self.generate_query_expression(sql, fixpoint.anchor(), indent)?;
                for member in fixpoint.members() {
                    if self.config.pretty_print {
                        sql.push('\n');
                        self.indent(sql, indent);
                    } else {
                        sql.push(' ');
                    }
                    sql.push_str(fixpoint.keyword());
                    if self.config.pretty_print {
                        sql.push('\n');
                    } else {
                        sql.push(' ');
                    }
                    self.generate_query_expression(sql, member, indent)?;
                }
                Ok(())
            }
        }
    }

    #[stacksafe::stacksafe]
    fn generate_query_expression(
        &self,
        sql: &mut String,
        query: &QueryExpression,
        indent: usize,
    ) -> Result<(), GeneratorError> {
        match query {
            QueryExpression::Select(select) => {
                self.generate_select_statement(sql, select, indent)?;
            }
            QueryExpression::SetOperation { op, left, right } => {
                // Generate left side
                self.generate_query_expression(sql, left, indent)?;

                // Generate operator
                if self.config.pretty_print {
                    sql.push('\n');
                    self.indent(sql, indent);
                } else {
                    sql.push(' ');
                }

                sql.push_str(op.keyword());

                if self.config.pretty_print {
                    sql.push('\n');
                } else {
                    sql.push(' ');
                }

                // Generate right side
                self.generate_query_expression(sql, right, indent)?;
            }
            QueryExpression::Values { rows } => {
                sql.push_str("VALUES ");
                for (i, row) in rows.iter().enumerate() {
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    sql.push('(');
                    for (j, expr) in row.iter().enumerate() {
                        if j > 0 {
                            sql.push_str(", ");
                        }
                        self.generate_domain_expression(sql, expr, None)?;
                    }
                    sql.push(')');
                }
            }
            QueryExpression::WithCte { ctes, query } => {
                // A NESTED WITH IS THE SAME WITH. One road writes the
                // clause — recursion keyword, once-only materialization,
                // each body — so a target's materialization answer cannot
                // be given twice and differ.
                self.indent(sql, indent);
                self.generate_with_clause(sql, ctes, indent)?;
                if self.config.pretty_print {
                    sql.push('\n');
                }
                self.generate_query_expression(sql, query, indent)?;
            }
        }

        Ok(())
    }

    /// Generate a SELECT statement
    fn generate_select_statement(
        &self,
        sql: &mut String,
        select: &SelectStatement,
        indent: usize,
    ) -> Result<(), GeneratorError> {
        let at = Emitting {
            scope: select.at(),
            reflexive: select.from().is_some_and(|from| {
                !self.names.is_scratch_scope(select.at())
                    && from.iter().any(|table| reads_scope(table, select.at()))
            }),
            unqualified: false,
        };
        for item in select.select_list() {
            if let SelectItem::Publishing {
                slot: alias,
                printed: true,
                ..
            } = item
            {
                if !self.names.column_belongs_to(*alias, at.scope) {
                    let scope = at.scope;
                    return Err(GeneratorError::Error(format!(
                        "SELECT output {alias:?} does not belong to its result scope {scope:?}"
                    )));
                }
            }
        }
        // SELECT clause
        self.indent(sql, indent);
        sql.push_str("SELECT ");

        if select.is_distinct() {
            sql.push_str("DISTINCT ");
        }

        // A T-SQL CAP IS PART OF THE SELECT, not a trailing clause. `TOP`
        // needs no ordering beside it — which is what makes it the only
        // spelling for a cap of zero, since `FETCH NEXT` admits no such
        // count. A skip is handled below, where the ordering is.
        if let (RowClauseStyle::TopAndFetch, Some(limit)) =
            (self.config.dialect.row_clause_style(), select.limit())
        {
            if let (Some(count), None) = (limit.count(), limit.offset()) {
                write!(sql, "TOP {count} ").expect("Writing to String cannot fail");
            }
        }

        // Select list
        for (i, item) in select.select_list().iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            self.generate_select_item(sql, item, at)?;
        }

        // FROM clause
        if let Some(tables) = select.from() {
            if self.config.pretty_print {
                sql.push('\n');
                self.indent(sql, indent);
            } else {
                sql.push(' ');
            }
            sql.push_str("FROM ");

            for (i, table) in tables.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                self.generate_table_expression(sql, table, indent, at)?;
            }
        }

        // WHERE clause
        if let Some(where_clause) = select.where_clause() {
            if self.config.pretty_print {
                sql.push('\n');
                self.indent(sql, indent);
            } else {
                sql.push(' ');
            }
            sql.push_str("WHERE ");
            self.generate_domain_expression(sql, where_clause, Some(at))?;
        }

        // GROUP BY clause
        if let Some(group_by) = select.group_by() {
            if self.config.pretty_print {
                sql.push('\n');
                self.indent(sql, indent);
            } else {
                sql.push(' ');
            }
            sql.push_str("GROUP BY ");

            for (i, expr) in group_by.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                self.generate_domain_expression(sql, expr, Some(at))?;
            }
        }

        // HAVING clause
        if let Some(having) = select.having() {
            if self.config.pretty_print {
                sql.push('\n');
                self.indent(sql, indent);
            } else {
                sql.push(' ');
            }
            sql.push_str("HAVING ");
            self.generate_domain_expression(sql, having, Some(at))?;
        }

        let style = self.config.dialect.row_clause_style();
        // A T-SQL SKIP BELONGS TO THE ORDERING, so a block that skips writes
        // one whether or not the author ordered it. The constant ordering is
        // the target's own idiom for "this block imposes no order"; it makes
        // the clause legal and claims nothing the author did not.
        let skips = select.limit().is_some_and(|l| l.offset().is_some());
        let orders_for_the_skip = matches!(style, RowClauseStyle::TopAndFetch) && skips;

        // ORDER BY clause
        if select.order_by().is_some() || orders_for_the_skip {
            if self.config.pretty_print {
                sql.push('\n');
                self.indent(sql, indent);
            } else {
                sql.push(' ');
            }
            sql.push_str("ORDER BY ");

            match select.order_by() {
                Some(order_by) => {
                    for (i, term) in order_by.iter().enumerate() {
                        if i > 0 {
                            sql.push_str(", ");
                        }
                        self.generate_order_term(sql, term, at)?;
                    }
                }
                None => sql.push_str("(SELECT NULL)"),
            }
        }

        // The row clause, where this target keeps it
        if let Some(limit) = select.limit() {
            let trailing = match style {
                RowClauseStyle::Trailing { uncapped } => Some(uncapped),
                // The cap went out as `TOP` above; only a skip is left, and
                // it hangs off the ORDER BY just written.
                RowClauseStyle::TopAndFetch => None,
            };
            let clause = match (trailing, limit.count(), limit.offset()) {
                // A CLAUSE WITH NO MAXIMUM is the target's own spelling: a
                // sentinel count beside the offset, or the keyword the
                // standard gives.
                (Some(uncapped), None, Some(offset)) => {
                    Some(format!("LIMIT {uncapped} OFFSET {offset}"))
                }
                (Some(_), Some(count), Some(offset)) => {
                    Some(format!("LIMIT {count} OFFSET {offset}"))
                }
                (Some(_), Some(count), None) => Some(format!("LIMIT {count}")),
                (None, None, Some(offset)) => Some(format!("OFFSET {offset} ROWS")),
                (None, Some(count), Some(offset)) => {
                    Some(format!("OFFSET {offset} ROWS FETCH NEXT {count} ROWS ONLY"))
                }
                // Written as `TOP` already.
                (None, Some(_), None) => None,
                // Unconstructible: every door builds a count, an offset, or
                // both.
                (_, None, None) => None,
            };
            if let Some(clause) = clause {
                if self.config.pretty_print {
                    sql.push('\n');
                    self.indent(sql, indent);
                } else {
                    sql.push(' ');
                }
                sql.push_str(&clause);
            }
        }

        Ok(())
    }

    /// Generate a SELECT item
    fn generate_select_item(
        &self,
        sql: &mut String,
        item: &SelectItem,
        at: Emitting,
    ) -> Result<(), GeneratorError> {
        match item {
            SelectItem::Star { .. } => {
                sql.push('*');
            }
            SelectItem::Publishing { expr, .. } | SelectItem::Scaffolding { expr, .. } => {
                self.generate_domain_expression(sql, expr, Some(at))?;
                // THE ALIAS IS A RENDERING DECISION. The position's
                // identity is its slot, which every reader addresses it by;
                // whether SQL writes an `AS` is a separate question the
                // item already answered.
                if let Some(alias) = item.printed_alias() {
                    sql.push_str(" AS ");
                    self.write_column(sql, alias)?;
                }
            }
        }
        Ok(())
    }

    /// Generate a table expression (table, subquery, join)
    fn generate_table_expression(
        &self,
        sql: &mut String,
        table: &TableExpression,
        indent: usize,
        at: Emitting,
    ) -> Result<(), GeneratorError> {
        match table {
            TableExpression::Scope(scope) => self.write_scope(sql, *scope)?,
            TableExpression::QualifiedScope { schema, scope } => {
                if schema == "temp" {
                    sql.push_str("\"temp\"");
                } else {
                    sql.push_str(schema);
                }
                sql.push('.');
                self.write_scope(sql, *scope)?;
            }
            TableExpression::Entity { entity, alias } => {
                // A ground carries its occurrence scope so references have
                // something to qualify by, and that scope usually ends up
                // spelled as the entity itself. Writing `users AS users` is
                // the same FROM entry with a word that says nothing, and this
                // is the only place that knows both spellings — the AST holds
                // handles, and which of them collided is decided at baptism.
                let mut entity_sql = String::new();
                self.write_entity(&mut entity_sql, *entity)?;
                sql.push_str(&entity_sql);
                if let Some(alias) = alias {
                    let mut rendered = String::new();
                    self.write_scope(&mut rendered, *alias)?;
                    let unqualified = entity_sql.rsplit('.').next().unwrap_or(&entity_sql);
                    if rendered != unqualified {
                        sql.push_str(" AS ");
                        sql.push_str(&rendered);
                    }
                }
            }
            TableExpression::Subquery { query, alias } => {
                sql.push('(');
                if self.config.pretty_print {
                    sql.push('\n');
                    self.generate_query_expression(sql, query, indent + 1)?;
                    sql.push('\n');
                    self.indent(sql, indent);
                } else {
                    self.generate_query_expression(sql, query, indent)?;
                }
                sql.push_str(") AS ");
                self.write_scope(sql, *alias)?;
            }
            TableExpression::Join {
                left,
                right,
                join_type,
                join_condition,
            } => {
                // Generate left side
                self.generate_table_expression(sql, left, indent, at)?;

                // Generate join keyword
                if self.config.pretty_print {
                    sql.push('\n');
                    self.indent(sql, indent);
                } else {
                    sql.push(' ');
                }

                sql.push_str(match join_type {
                    JoinType::Inner => "INNER JOIN",
                    JoinType::Left => "LEFT JOIN",
                    JoinType::Right => "RIGHT JOIN",
                    JoinType::Full => "FULL OUTER JOIN",
                    JoinType::Cross => "CROSS JOIN",
                });

                sql.push(' ');

                // Generate right side
                self.generate_table_expression(sql, right, indent, at)?;

                // Generate join condition
                match join_condition {
                    JoinCondition::On(expr) => {
                        sql.push_str(" ON ");
                        self.generate_domain_expression(sql, expr, Some(at))?;
                    }
                    // Each pair is written as the equality of its two exact
                    // slots, each under its own qualifier: nothing here
                    // depends on the two sides sharing characters.
                    JoinCondition::Merge(pairs) => {
                        sql.push_str(" ON ");
                        for (i, pair) in pairs.iter().enumerate() {
                            if i > 0 {
                                sql.push_str(" AND ");
                            }
                            self.write_ref(sql, pair.left, at)?;
                            sql.push_str(" = ");
                            self.write_ref(sql, pair.right, at)?;
                        }
                    }
                    JoinCondition::Cartesian => {
                        // A deliberate cross spells no condition; dialects
                        // that reject the bare form were legalized upstream.
                    }
                }
            }
            TableExpression::TVF {
                function,
                arguments,
                alias,
            } => self.write_tvf(sql, *function, arguments, *alias, at)?,
        }
        Ok(())
    }

    /// Generate a domain expression
    fn generate_domain_expression(
        &self,
        sql: &mut String,
        expr: &DomainExpression,
        at: Option<Emitting>,
    ) -> Result<(), GeneratorError> {
        match expr {
            DomainExpression::Column(column) => {
                let at = at.ok_or_else(|| {
                    GeneratorError::Error(
                        "a column in a context-free VALUES query has no emitting scope".to_string(),
                    )
                })?;
                self.write_ref(sql, *column, at)?;
            }
            DomainExpression::Literal(value) => {
                literals::generate_literal(
                    sql,
                    value,
                    self.config.dialect,
                    &self.config.dialect_pack,
                )?;
            }
            DomainExpression::PublishedNameLiteral(column) => {
                self.write_column_literal(sql, *column)?;
            }
            DomainExpression::PublishedJsonPathLiteral(column) => {
                self.write_published_json_path(sql, *column)?;
            }
            DomainExpression::JsonPathLiteral(path) => {
                self.write_typed_json_path(sql, path);
            }
            DomainExpression::ScopeNameLiteral(scope) => {
                self.write_scope_literal(sql, *scope)?;
            }
            DomainExpression::Binary { left, op, right } => {
                // An op.* row whose body contains '{' is a FULL TEMPLATE over
                // the two rendered operands — for target spellings that change
                // SHAPE, not just token (mysql CONCAT({0}, {1}) is a function;
                // NOT ({0} <=> {1}) wraps). Same body-shape dispatch as the
                // fn.* arm. Templates own their
                // parentheses.
                let spelling = operators::binary_operator_to_sql(
                    op,
                    self.config.dialect,
                    &self.config.dialect_pack,
                )?;
                if spelling.contains('{') {
                    let mut left_sql = String::new();
                    self.generate_domain_expression(&mut left_sql, left, at)?;
                    let mut right_sql = String::new();
                    self.generate_domain_expression(&mut right_sql, right, at)?;
                    let applied = crate::pipeline::dialect_pack::apply_template(
                        spelling,
                        &[left_sql.as_str(), right_sql.as_str()],
                    )
                    .map_err(|e| GeneratorError::Error(format!("op template: {}", e)))?;
                    sql.push_str(&applied);
                } else {
                    // Handle special cases that might need parentheses
                    let needs_parens = matches!(op, BinaryOperator::And | BinaryOperator::Or);

                    if needs_parens {
                        sql.push('(');
                    }

                    self.generate_domain_expression(sql, left, at)?;
                    sql.push(' ');
                    sql.push_str(spelling);
                    sql.push(' ');
                    self.generate_domain_expression(sql, right, at)?;

                    if needs_parens {
                        sql.push(')');
                    }
                }
            }
            DomainExpression::Unary { op, expr } => {
                sql.push_str(operators::unary_operator_to_sql(op));
                sql.push(' ');
                self.generate_domain_expression(sql, expr, at)?;
            }
            DomainExpression::Cast { expr, type_name } => {
                // CAST(x AS T) is universal skeleton; only T's spelling is
                // per-target: `type.<name>` render rows, canonical =
                // uppercased DQL type word. Semantics are the target's cast.
                sql.push_str("CAST(");
                self.generate_domain_expression(sql, expr, at)?;
                sql.push_str(" AS ");
                let type_key = format!("type.{}", type_name.to_ascii_lowercase());
                match self
                    .config
                    .dialect_pack
                    .render(self.config.dialect.family_name(), &type_key)
                {
                    Some(rule) => sql.push_str(rule.template().map_err(GeneratorError::Error)?),
                    None => sql.push_str(&type_name.to_ascii_uppercase()),
                }
                sql.push(')');
            }
            DomainExpression::Function {
                name,
                args,
                distinct,
            } => {
                // Consult the dialect pack for a per-function render rule,
                // dispatching on rule_kind:
                //   template, bare-NAME body — rename, call shape and
                //     DISTINCT kept;
                //   template with '{'        — re-render from rendered args;
                //   rust_handler             — body names a compiled fn
                //     (argument transformation/synthesis templates can't do).
                // Intrinsic forms key the pack structurally, so an authored
                // name that merely LOOKS like a reserved intrinsic spelling
                // remains a user function.
                let (canonical_name, fn_key, rule) = match name {
                    crate::pipeline::sql_ast::FunctionName::User(name) => (
                        Some(name.as_str()),
                        format!("fn.{}", name.to_ascii_lowercase()),
                        self.config.dialect_pack.render(
                            self.config.dialect.family_name(),
                            &format!("fn.{}", name.to_ascii_lowercase()),
                        ),
                    ),
                    crate::pipeline::sql_ast::FunctionName::Intrinsic(intrinsic) => (
                        intrinsic.canonical(),
                        format!("fn.{intrinsic:?}"),
                        self.config.dialect_pack.render_intrinsic_function(
                            self.config.dialect.family_name(),
                            *intrinsic,
                        ),
                    ),
                };

                enum FnRender<'a> {
                    Canonical,
                    Rename(&'a str),
                    Template(&'a str),
                    Handler(crate::pipeline::dialect_pack::RustRenderHandler),
                }
                let plan = match rule {
                    None => FnRender::Canonical,
                    Some(rule) if rule.rule_kind == "template" => {
                        let body = rule.template().map_err(GeneratorError::Error)?;
                        if body.contains('{') {
                            FnRender::Template(body)
                        } else {
                            FnRender::Rename(body)
                        }
                    }
                    Some(rule) if rule.rule_kind == "rust_handler" => {
                        let handler =
                            crate::pipeline::dialect_pack::rust_render_handler(&rule.body)
                                .ok_or_else(|| {
                                    GeneratorError::Error(format!(
                                        "{}: unknown rust_handler '{}'",
                                        fn_key, rule.body
                                    ))
                                })?;
                        FnRender::Handler(handler)
                    }
                    Some(rule) => {
                        return Err(GeneratorError::Error(format!(
                            "{}: unsupported rule_kind '{}' (no interpreter built for it)",
                            fn_key, rule.rule_kind
                        )));
                    }
                };

                match plan {
                    FnRender::Template(template) => {
                        if *distinct {
                            return Err(GeneratorError::Error(format!(
                                "render rule '{}' is a full template and cannot carry DISTINCT",
                                fn_key
                            )));
                        }
                        let rendered = self.render_fn_args(args, at)?;
                        let refs: Vec<&str> = rendered.iter().map(String::as_str).collect();
                        let applied =
                            crate::pipeline::dialect_pack::apply_template(template, &refs)
                                .map_err(|e| GeneratorError::Error(format!("{}: {}", fn_key, e)))?;
                        sql.push_str(&applied);
                    }
                    FnRender::Handler(handler) => {
                        let rendered = self.render_fn_args(args, at)?;
                        let refs: Vec<&str> = rendered.iter().map(String::as_str).collect();
                        let applied = handler(&refs, *distinct)
                            .map_err(|e| GeneratorError::Error(format!("{}: {}", fn_key, e)))?;
                        sql.push_str(&applied);
                    }
                    FnRender::Rename(new_name) => {
                        self.write_fn_call(sql, new_name, args, *distinct, at)?;
                    }
                    FnRender::Canonical => {
                        // The arbitrary-witness form's canonical spelling is
                        // the bare argument (sqlite's relaxed GROUP BY) —
                        // identity isn't expressible as a rename row, so this
                        // one canonical rule lives in code like the rest of
                        // the canonical spellings.
                        if name
                            == &crate::pipeline::sql_ast::FunctionName::Intrinsic(
                                crate::names::Intrinsic::Arbitrary,
                            )
                        {
                            let [arg] = args.as_slice() else {
                                return Err(GeneratorError::Error(format!(
                                    "{}: expects exactly 1 argument, got {}",
                                    fn_key,
                                    args.len()
                                )));
                            };
                            self.generate_domain_expression(sql, arg, at)?;
                        } else {
                            self.write_fn_call(
                                sql,
                                canonical_name.expect("a callable function has a spelling"),
                                args,
                                *distinct,
                                at,
                            )?;
                        }
                    }
                }
            }
            DomainExpression::WindowFunction {
                name,
                args,
                distinct,
                partition_by,
                order_by,
                frame,
            } => {
                // Function call
                sql.push_str(name);
                sql.push('(');
                if *distinct {
                    sql.push_str("DISTINCT ");
                }
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    self.generate_domain_expression(sql, arg, at)?;
                }
                sql.push(')');

                // OVER clause
                sql.push_str(" OVER (");

                let mut has_content = false;

                // PARTITION BY
                if !partition_by.is_empty() {
                    sql.push_str("PARTITION BY ");
                    for (i, expr) in partition_by.iter().enumerate() {
                        if i > 0 {
                            sql.push_str(", ");
                        }
                        self.generate_domain_expression(sql, expr, at)?;
                    }
                    has_content = true;
                }

                // ORDER BY. A T-SQL frame is a clause OF the ordering and
                // cannot stand without one, so a frame that needs no order
                // still writes the constant one.
                if order_by.is_empty()
                    && frame.is_some()
                    && matches!(
                        self.config.dialect.row_clause_style(),
                        RowClauseStyle::TopAndFetch
                    )
                {
                    if has_content {
                        sql.push(' ');
                    }
                    sql.push_str("ORDER BY (SELECT NULL) ");
                    has_content = false;
                }
                if !order_by.is_empty() {
                    if has_content {
                        sql.push(' ');
                    }
                    sql.push_str("ORDER BY ");
                    for (i, (expr, sort_order)) in order_by.iter().enumerate() {
                        if i > 0 {
                            sql.push_str(", ");
                        }
                        self.generate_domain_expression(sql, expr, at)?;
                        match sort_order {
                            crate::pipeline::sql_ast::ordering::OrderDirection::Asc => {
                                sql.push_str(" ASC");
                            }
                            crate::pipeline::sql_ast::ordering::OrderDirection::Desc => {
                                sql.push_str(" DESC");
                            }
                        }
                    }
                    has_content = true;
                }

                // Frame specification
                if let Some(frame_spec) = frame {
                    if has_content {
                        sql.push(' ');
                    }
                    self.generate_window_frame(sql, frame_spec, at)?;
                }

                sql.push(')');
            }
            DomainExpression::Star => {
                sql.push('*');
            }
            DomainExpression::Parens(inner) => {
                sql.push('(');
                self.generate_domain_expression(sql, inner, at)?;
                sql.push(')');
            }
            DomainExpression::Case {
                expr,
                when_clauses,
                else_clause,
            } => {
                sql.push_str("CASE");
                if let Some(expr) = expr {
                    sql.push(' ');
                    self.generate_domain_expression(sql, expr, at)?;
                }
                for clause in when_clauses {
                    sql.push_str(" WHEN ");
                    self.generate_domain_expression(sql, clause.when(), at)?;
                    sql.push_str(" THEN ");
                    self.generate_domain_expression(sql, clause.then(), at)?;
                }
                if let Some(else_expr) = else_clause {
                    sql.push_str(" ELSE ");
                    self.generate_domain_expression(sql, else_expr, at)?;
                }
                sql.push_str(" END");
            }
            DomainExpression::Exists { not, query } => {
                if *not {
                    sql.push_str("NOT EXISTS (");
                } else {
                    sql.push_str("EXISTS (");
                }
                self.generate_query_expression(sql, query, 0)?;
                sql.push(')');
            }
            DomainExpression::Subquery(query) => {
                // Scalar subquery - just wrap in parens
                sql.push('(');
                self.generate_query_expression(sql, query, 0)?;
                sql.push(')');
            }
            DomainExpression::PredicateRewrite {
                name,
                namespace,
                args,
                negated,
            } => {
                self.generate_predicate_rewrite(sql, name, namespace, args, *negated, at)?;
            }
            // THE POLARITY OBSERVATION, in the target's own spelling. The
            // canonical form is SQL's `IS [NOT] TRUE`; a target without it
            // supplies an `op.is_true`/`op.is_not_true` row that says what
            // it writes instead. The template owns its parentheses.
            DomainExpression::Observation { expr, positive } => {
                let key = if *positive {
                    "op.is_true"
                } else {
                    "op.is_not_true"
                };
                let mut inner = String::new();
                self.generate_domain_expression(&mut inner, expr, at)?;
                let body = match self
                    .config
                    .dialect_pack
                    .render(self.config.dialect.family_name(), key)
                {
                    Some(rule) => rule.template().map_err(GeneratorError::Error)?.to_string(),
                    None if *positive => "({0}) IS TRUE".to_string(),
                    None => "({0}) IS NOT TRUE".to_string(),
                };
                let applied =
                    crate::pipeline::dialect_pack::apply_template(&body, &[inner.as_str()])
                        .map_err(|e| GeneratorError::Error(format!("{key}: {e}")))?;
                sql.push_str(&applied);
            }
        }
        Ok(())
    }

    /// Generate SQL for a predicate rewrite call: first consult the
    /// `dialect_form_rule` table (this call site IS the sigma-predicate
    /// form — code chooses the form), then fall back to the bin entity's
    /// canonical lowering via the bin_registry.
    fn generate_predicate_rewrite(
        &self,
        sql: &mut String,
        name: &str,
        namespace: &[String],
        args: &[DomainExpression],
        negated: bool,
        at: Option<Emitting>,
    ) -> Result<(), GeneratorError> {
        // Precedence: (entity+form+dialect) → (form+dialect) →
        // canonical code (the bin entity below).
        // The call site IS the sigma form — code chooses the form (as the enum,
        // STRING-FLOOR Tier 2c), data spells it.
        let form_type = crate::enums::EntityType::BinSigmaPredicate;
        if let Some(rule) =
            self.config
                .dialect_pack
                .form_rule(self.config.dialect.family_name(), form_type, name)
        {
            let rule_id = format!("form.sigma.{}", name);
            let rendered = self.render_fn_args(args, at)?;
            let refs: Vec<&str> = rendered.iter().map(String::as_str).collect();
            let applied = match rule.rule_kind.as_str() {
                // A template expresses the un-negated predicate; negation
                // wraps it (`NOT (x ILIKE y)` ≡ `x NOT ILIKE y`).
                "template" => {
                    let body = rule.template().map_err(GeneratorError::Error)?;
                    crate::pipeline::dialect_pack::apply_template(body, &refs)
                        .map_err(|e| GeneratorError::Error(format!("{}: {}", rule_id, e)))?
                }
                "rust_handler" => {
                    let handler = crate::pipeline::dialect_pack::rust_render_handler(&rule.body)
                        .ok_or_else(|| {
                            GeneratorError::Error(format!(
                                "{}: unknown rust_handler '{}'",
                                rule_id, rule.body
                            ))
                        })?;
                    // For predicate handlers the flag is NEGATED (they own
                    // their negation spelling; no outer NOT is added).
                    sql.push_str(
                        &handler(&refs, negated)
                            .map_err(|e| GeneratorError::Error(format!("{}: {}", rule_id, e)))?,
                    );
                    return Ok(());
                }
                other => {
                    return Err(GeneratorError::Error(format!(
                        "{}: unsupported rule_kind '{}' (no interpreter built for it)",
                        rule_id, other
                    )));
                }
            };
            if negated {
                sql.push_str("NOT (");
                sql.push_str(&applied);
                sql.push(')');
            } else {
                sql.push_str(&applied);
            }
            return Ok(());
        }

        let registry = self.bin_registry.as_ref().ok_or_else(|| {
            GeneratorError::Error(format!(
                "PredicateRewrite '{}' but no bin_registry available",
                name
            ))
        })?;

        // The identity the resolver selected: a qualified citation names its
        // namespace exactly, a bare one is the universally visible entity.
        let entity = registry
            .lookup_qualified_entity(namespace, name)
            .ok_or_else(|| {
                GeneratorError::Error(format!("Unknown predicate rewrite: '{}'", name))
            })?;

        let sql_gen = entity.as_sql_generatable().ok_or_else(|| {
            GeneratorError::Error(format!(
                "Entity '{}' does not implement SqlGeneratable",
                name
            ))
        })?;

        let render_fn = |expr: &DomainExpression| -> crate::error::Result<String> {
            let mut s = String::new();
            self.generate_domain_expression(&mut s, expr, at)
                .map_err(|error| error.into_delightql_error("sigma predicate argument"))?;
            Ok(s)
        };

        let gen_context = crate::bin_cartridge::GeneratorContext {
            _dialect: self.config.dialect,
            render_expr: &render_fn,
        };

        let sql_string = sql_gen
            .generate_sql(args, &gen_context, negated)
            .map_err(|e| GeneratorError::Typed(e))?;

        sql.push_str(&sql_string);
        Ok(())
    }

    /// Generate an ORDER BY term
    fn generate_order_term(
        &self,
        sql: &mut String,
        term: &OrderTerm,
        at: Emitting,
    ) -> Result<(), GeneratorError> {
        self.generate_domain_expression(sql, term.expr(), Some(at))?;
        if let Some(dir) = term.direction() {
            sql.push(' ');
            sql.push_str(match dir {
                OrderDirection::Asc => "ASC",
                OrderDirection::Desc => "DESC",
            });
        }
        Ok(())
    }

    /// Add indentation
    fn indent(&self, sql: &mut String, level: usize) {
        for _ in 0..(level * self.config.indent_width) {
            sql.push(' ');
        }
    }

    /// Generate window frame specification
    fn generate_window_frame(
        &self,
        sql: &mut String,
        frame: &crate::pipeline::sql_ast::SqlWindowFrame,
        at: Option<Emitting>,
    ) -> Result<(), GeneratorError> {
        use crate::pipeline::sql_ast::SqlFrameMode;

        // Frame mode
        match frame.mode {
            SqlFrameMode::Groups => sql.push_str("GROUPS"),
            SqlFrameMode::Rows => sql.push_str("ROWS"),
            SqlFrameMode::Range => sql.push_str("RANGE"),
        }

        sql.push_str(" BETWEEN ");

        // Start bound
        self.generate_frame_bound(sql, &frame.start, true, at)?;

        sql.push_str(" AND ");

        // End bound
        self.generate_frame_bound(sql, &frame.end, false, at)?;

        Ok(())
    }

    /// Generate frame bound
    fn generate_frame_bound(
        &self,
        sql: &mut String,
        bound: &crate::pipeline::sql_ast::SqlFrameBound,
        is_start: bool,
        at: Option<Emitting>,
    ) -> Result<(), GeneratorError> {
        use crate::pipeline::sql_ast::SqlFrameBound;

        match bound {
            SqlFrameBound::Unbounded => {
                if is_start {
                    sql.push_str("UNBOUNDED PRECEDING");
                } else {
                    sql.push_str("UNBOUNDED FOLLOWING");
                }
            }
            SqlFrameBound::CurrentRow => {
                sql.push_str("CURRENT ROW");
            }
            SqlFrameBound::Preceding(expr) => {
                self.generate_domain_expression(sql, expr, at)?;
                sql.push_str(" PRECEDING");
            }
            SqlFrameBound::Following(expr) => {
                self.generate_domain_expression(sql, expr, at)?;
                sql.push_str(" FOLLOWING");
            }
        }
        Ok(())
    }
}
