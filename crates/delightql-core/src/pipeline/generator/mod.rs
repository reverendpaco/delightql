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
pub use dialect::{DocumentNumberCategory, RowClauseStyle, SqlDialect};
pub use errors::GeneratorError;

/// [`baptise_statements`] for statements whose name status their producer
/// decided (`names::baptism::baptise_decided`).
pub(crate) fn baptise_statements_decided<'registry>(
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
    crate::names::baptism::baptise_decided(identities, &bundle)
        .map_err(|error| GeneratorError::Error(format!("SQL naming failed: {error:?}")))
}

/// The main SQL generator
/// Does this FROM entry stand on `scope`?
///
/// Only the entries the statement itself stands on — a scope named inside
/// a subquery beneath one belongs to that subquery's own emission.
fn reads_scope(table: &TableExpression, scope: ScopeId) -> bool {
    match table {
        TableExpression::Scope(found) => {
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
    /// The statement has not been legalized and is rendered to be read, not
    /// run (`inspecting`).
    inspecting: bool,
}

impl<'names, 'registry> SqlGenerator<'names, 'registry> {
    /// Render a SQL-layer domain expression to a string.
    ///
    /// Used by the DDL pipeline generator for CHECK/DEFAULT expressions.
    #[cfg(test)]
    pub(crate) fn render_expression(
        &self,
        expr: &DomainExpression,
        at: ScopeId,
    ) -> Result<String, GeneratorError> {
        let mut sql = String::new();
        self.generate_domain_expression(&mut sql, expr, Some(Emitting::at(at)))?;
        Ok(sql)
    }
    pub fn new(names: &'names Baptised<'registry>) -> Self {
        SqlGenerator {
            names,
            config: GeneratorConfig::default(),
            bin_registry: None,
            inspecting: false,
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

    /// Whether each column of `statement`'s result is authored or minted,
    /// in position order. Answered by the generator because the bundle it
    /// spells with is the one that drew the names. A position no occurrence
    /// stands at carries a name the engine chose, which nobody authored.
    pub fn heading_naming(
        &self,
        statement: &SqlStatement,
    ) -> Option<Vec<delightql_protocol::Naming>> {
        statement.heading().map(|heading| {
            heading
                .into_iter()
                .map(|slot| match slot {
                    Some(col) if !self.names.drew(col) => delightql_protocol::Naming::Authored,
                    _ => delightql_protocol::Naming::Minted,
                })
                .collect()
        })
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

    pub(crate) fn write_entity(
        &self,
        sql: &mut String,
        entity: EntityId,
    ) -> Result<(), GeneratorError> {
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
                // A target template owns its complete call and guard. Only
                // the generic json_each spelling needs this operation's
                // target judgment; otherwise a PostgreSQL template would be
                // judged by a guard it never emits.
                let guarded_args = if intrinsic == Some(crate::names::Intrinsic::JsonEachArray)
                    && rule.is_none()
                {
                    rendered_args
                        .iter()
                        .map(|argument| {
                            format!(
                                "CASE WHEN json_valid({argument}) AND json_type({argument}) = '{}' \
                                 THEN {argument} END",
                                self.config.dialect.json_each_array_guard_type()
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

    /// A PUBLISHED COLUMN READ AS A MEMBER: the one key of the reach is the
    /// column's emitted spelling, which baptism supplies here. The reach is
    /// then a typed path like any other and takes the same road to the
    /// target.
    fn published_json_path(
        &self,
        column: ColId,
    ) -> Result<crate::pipeline::asts::core::Path, GeneratorError> {
        if !self.names.knows_column(column) {
            return Err(GeneratorError::Error(format!(
                "JSON-path column {column:?} was not included in the baptism bundle"
            )));
        }
        let mut key = String::new();
        let mut key_writer = |output: &mut String, text: &str, _stropped: bool| {
            output.push_str(text);
            Ok(())
        };
        let mut output = SqlOut::new(&mut key, &mut key_writer);
        self.names.write_column(column, &mut output);
        output.finish().map_err(GeneratorError::Error)?;
        Ok(crate::pipeline::asts::core::Path::key(key))
    }

    /// A typed reach, rendered in THIS TARGET'S path representation by the
    /// dialect's one renderer.
    fn write_typed_json_path(
        &self,
        sql: &mut String,
        path: &crate::pipeline::asts::core::Path,
    ) -> Result<(), GeneratorError> {
        sql.push_str(
            &self
                .config
                .dialect
                .json_path_literal(path)
                .map_err(GeneratorError::Error)?,
        );
        Ok(())
    }

    /// THE REACHES AMONG A CALL'S ARGUMENTS, as typed paths: a reach the
    /// compiler made travels to a `rust_handler` as its steps, so a handler
    /// that must spell a path in its target's representation renders the
    /// steps rather than re-reading a spelling meant for another target. A
    /// published column read as a member is the one-key path baptism names.
    fn argument_paths(
        &self,
        args: &[DomainExpression],
    ) -> Result<Vec<Option<crate::pipeline::asts::core::Path>>, GeneratorError> {
        args.iter()
            .map(|arg| match arg {
                DomainExpression::JsonPathLiteral(path) => Ok(Some(path.clone())),
                DomainExpression::PublishedJsonPathLiteral(column) => {
                    self.published_json_path(*column).map(Some)
                }
                _ => Ok(None),
            })
            .collect()
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
        }
    }

    /// The spelling a user callee takes on this dialect: bare where the
    /// target accepts the word as a function name, delimited where its
    /// parser would refuse the call (`identifiers::callee_needs_quoting`).
    fn callee_spelling(&self, name: &str) -> Result<String, GeneratorError> {
        let mut spelled = String::new();
        if identifiers::callee_needs_quoting(name, self.config.dialect) {
            identifiers::write_delimited(
                &mut spelled,
                name,
                self.config.dialect,
                &self.config.dialect_pack,
            )?;
        } else {
            spelled.push_str(name);
        }
        Ok(spelled)
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

    /// Emit a call a dialect render rule spells: a rename keeps the call
    /// shape, a full template and a handler compose from the rendered
    /// arguments.
    fn write_ruled_call(
        &self,
        sql: &mut String,
        plan: RuledCall<'_>,
        fn_key: &str,
        args: &[DomainExpression],
        distinct: bool,
        at: Option<Emitting>,
    ) -> Result<(), GeneratorError> {
        match plan {
            RuledCall::Template(template) => {
                if distinct {
                    return Err(GeneratorError::Error(format!(
                        "render rule '{}' is a full template and cannot carry DISTINCT",
                        fn_key
                    )));
                }
                let rendered = self.render_fn_args(args, at)?;
                let refs: Vec<&str> = rendered.iter().map(String::as_str).collect();
                let applied = crate::pipeline::dialect_pack::apply_template(template, &refs)
                    .map_err(|e| GeneratorError::Error(format!("{}: {}", fn_key, e)))?;
                sql.push_str(&applied);
            }
            RuledCall::Handler(handler) => {
                let rendered = self.render_fn_args(args, at)?;
                let paths = self.argument_paths(args)?;
                let applied = handler(&render_args(&paths, &rendered), distinct)
                    .map_err(|e| GeneratorError::Error(format!("{}: {}", fn_key, e)))?;
                sql.push_str(&applied);
            }
            RuledCall::Rename(new_name) => {
                self.write_fn_call(sql, new_name, args, distinct, at)?;
            }
        }
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
            if let Some(columns) = cte.column_names() {
                sql.push('(');
                for (position, column) in columns.iter().enumerate() {
                    if position > 0 {
                        sql.push_str(", ");
                    }
                    self.write_column(sql, *column)?;
                }
                sql.push(')');
            }
            sql.push_str(" AS (");

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
                self.generate_fixpoint_term(sql, fixpoint.anchor(), indent, true)?;
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
                    self.generate_fixpoint_term(sql, member, indent, false)?;
                }
                Ok(())
            }
        }
    }

    #[stacksafe::stacksafe]
    fn generate_fixpoint_term(
        &self,
        sql: &mut String,
        query: &QueryExpression,
        indent: usize,
        anchor: bool,
    ) -> Result<(), GeneratorError> {
        if anchor
            && self.config.dialect.compound_term_requires_derived_limit()
            && matches!(query, QueryExpression::Select(select) if select.limit().is_some())
        {
            sql.push_str("SELECT * FROM (");
            self.generate_query_expression(sql, query, indent)?;
            sql.push(')');
        } else {
            self.generate_query_expression(sql, query, indent)?;
        }
        Ok(())
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
                self.generate_key(sql, expr, at, "GROUP BY")?;
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
                let path = self.published_json_path(*column)?;
                self.write_typed_json_path(sql, &path)?;
            }
            DomainExpression::JsonPathLiteral(path) => {
                self.write_typed_json_path(sql, path)?;
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
                sql.push_str(
                    &self
                        .config
                        .dialect_pack
                        .type_spelling(self.config.dialect.family_name(), type_name)
                        .map_err(GeneratorError::Error)?,
                );
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

                match RuledCall::of(rule, &fn_key)? {
                    None => {
                        // The arbitrary-witness form's canonical spelling is
                        // the bare argument (sqlite's relaxed GROUP BY) —
                        // identity isn't expressible as a rename row, so this
                        // one canonical rule lives in code like the rest of
                        // the canonical spellings.
                        if name
                            == &crate::pipeline::sql_ast::FunctionName::Intrinsic(
                                crate::names::Intrinsic::JsonEachDocument,
                            )
                        {
                            // THE ELEMENT AS ONE DOCUMENT, decided from the
                            // TVF's `type` column. A container element's
                            // value is its document; an atom's is quoted
                            // into one. The subtype `json_quote` would read
                            // on its own is not a value and does not survive
                            // a sorter or a materialized subquery.
                            let [value, kind] = args.as_slice() else {
                                return Err(GeneratorError::Error(format!(
                                    "{}: expects (value, kind), got {} arguments",
                                    fn_key,
                                    args.len()
                                )));
                            };
                            sql.push_str("CASE WHEN ");
                            self.generate_domain_expression(sql, kind, at)?;
                            sql.push_str(" IN ('object', 'array') THEN ");
                            self.generate_domain_expression(sql, value, at)?;
                            sql.push_str(" ELSE json_quote(");
                            self.generate_domain_expression(sql, value, at)?;
                            sql.push_str(") END");
                        } else if name
                            == &crate::pipeline::sql_ast::FunctionName::Intrinsic(
                                crate::names::Intrinsic::JsonScalar,
                            )
                        {
                            let [value] = args.as_slice() else {
                                return Err(GeneratorError::Error(format!(
                                    "{}: expects exactly 1 argument, got {}",
                                    fn_key,
                                    args.len()
                                )));
                            };
                            let mut member = String::new();
                            self.generate_domain_expression(&mut member, value, at)?;
                            write_exact_real(sql, &member, ExactReal::Member);
                        } else if name
                            == &crate::pipeline::sql_ast::FunctionName::Intrinsic(
                                crate::names::Intrinsic::JsonLabel,
                            )
                        {
                            let [value] = args.as_slice() else {
                                return Err(GeneratorError::Error(format!(
                                    "{}: expects exactly 1 argument, got {}",
                                    fn_key,
                                    args.len()
                                )));
                            };
                            let mut key = String::new();
                            self.generate_domain_expression(&mut key, value, at)?;
                            write_exact_real(sql, &key, ExactReal::Label);
                        } else if name
                            == &crate::pipeline::sql_ast::FunctionName::Intrinsic(
                                crate::names::Intrinsic::Exact,
                            )
                        {
                            // SQLite compares under a column's declared
                            // collation unless an operand states its own. A
                            // postfix collation binds tighter than every
                            // operator, so an operand that is an operation is
                            // parenthesized to keep it whole.
                            let [value] = args.as_slice() else {
                                return Err(GeneratorError::Error(format!(
                                    "{}: expects exactly 1 argument, got {}",
                                    fn_key,
                                    args.len()
                                )));
                            };
                            let operation = matches!(
                                value,
                                DomainExpression::Binary { .. }
                                    | DomainExpression::Unary { .. }
                                    | DomainExpression::Observation { .. }
                            );
                            if operation {
                                sql.push('(');
                            }
                            self.generate_domain_expression(sql, value, at)?;
                            sql.push_str(if operation { ") COLLATE BINARY" } else { " COLLATE BINARY" });
                        } else if name
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
                            // A USER callee is an admitted name and owes the
                            // target a lawful call: delimited exactly where
                            // this dialect refuses the bare word as a call.
                            // An intrinsic's canonical spelling is target
                            // syntax and is written as it is.
                            let spelled = match name {
                                crate::pipeline::sql_ast::FunctionName::User(user) => {
                                    self.callee_spelling(user)?
                                }
                                crate::pipeline::sql_ast::FunctionName::Intrinsic(_) => {
                                    canonical_name
                                        .expect("a callable function has a spelling")
                                        .to_string()
                                }
                            };
                            self.write_fn_call(sql, &spelled, args, *distinct, at)?;
                        }
                    }
                    Some(ruled) => {
                        self.write_ruled_call(sql, ruled, &fn_key, args, *distinct, at)?
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
                // A window callee is an admitted name like any other: it takes
                // the same per-dialect callee law and the same render rule as
                // its plain call, and the window clause stands after the call
                // the rule spelled.
                let fn_key = format!("fn.{}", name.to_ascii_lowercase());
                let rule = self
                    .config
                    .dialect_pack
                    .render(self.config.dialect.family_name(), &fn_key);
                match RuledCall::of(rule, &fn_key)? {
                    None => {
                        let spelled = self.callee_spelling(name)?;
                        self.write_fn_call(sql, &spelled, args, *distinct, at)?;
                    }
                    Some(ruled) => {
                        self.write_ruled_call(sql, ruled, &fn_key, args, *distinct, at)?
                    }
                }

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
                    let paths = self.argument_paths(args)?;
                    sql.push_str(
                        &handler(&render_args(&paths, &rendered), negated)
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

        // The identity the resolver selected: both qualified and bare sigma
        // citations carry the exact namespace/name that answered selection.
        // An empty namespace remains only for hand-built SQL-AST fixtures;
        // resolved calls never take that registration-order road.
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

    /// Write one ORDER BY or GROUP BY key. The AST holds every key as a
    /// value, and SQL reads an integer literal there as a select-list
    /// position. Legalization leaves no such key, so one here is refused
    /// rather than written with another meaning, unless the statement is
    /// only being shown.
    fn generate_key(
        &self,
        sql: &mut String,
        key: &DomainExpression,
        at: Emitting,
        clause: &str,
    ) -> Result<(), GeneratorError> {
        if key.reads_as_position() {
            if !self.inspecting {
                return Err(GeneratorError::Error(format!(
                    "a {clause} key SQL would read as a position reached generation: {key:?}"
                )));
            }
            sql.push_str("<value ");
            self.generate_domain_expression(sql, key, Some(at))?;
            sql.push('>');
            return Ok(());
        }
        self.generate_domain_expression(sql, key, Some(at))
    }

    /// Generate an ORDER BY term
    fn generate_order_term(
        &self,
        sql: &mut String,
        term: &OrderTerm,
        at: Emitting,
    ) -> Result<(), GeneratorError> {
        self.generate_key(sql, term.expr(), at, "ORDER BY")?;
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

/// A `rust_handler`'s argument row: the typed path where the argument is a
/// reach, the rendered SQL text elsewhere.
/// A call a dialect render rule spells, read once from the rule's kind: a
/// bare-name template renames and keeps the call shape and DISTINCT, a
/// template with placeholders re-renders from the rendered arguments, and a
/// handler names a compiled lowering. No rule is the canonical call.
enum RuledCall<'a> {
    Rename(&'a str),
    Template(&'a str),
    Handler(crate::pipeline::dialect_pack::RustRenderHandler),
}

impl<'a> RuledCall<'a> {
    fn of(
        rule: Option<&'a crate::pipeline::dialect_pack::RenderRule>,
        fn_key: &str,
    ) -> Result<Option<Self>, GeneratorError> {
        let Some(rule) = rule else {
            return Ok(None);
        };
        match rule.rule_kind.as_str() {
            "template" => {
                let body = rule.template().map_err(GeneratorError::Error)?;
                Ok(Some(if body.contains('{') {
                    RuledCall::Template(body)
                } else {
                    RuledCall::Rename(body)
                }))
            }
            "rust_handler" => crate::pipeline::dialect_pack::rust_render_handler(&rule.body)
                .map(|handler| Some(RuledCall::Handler(handler)))
                .ok_or_else(|| {
                    GeneratorError::Error(format!(
                        "{}: unknown rust_handler '{}'",
                        fn_key, rule.body
                    ))
                }),
            other => Err(GeneratorError::Error(format!(
                "{}: unsupported rule_kind '{}' (no interpreter built for it)",
                fn_key, other
            ))),
        }
    }
}

fn render_args<'a>(
    paths: &'a [Option<crate::pipeline::asts::core::Path>],
    rendered: &'a [String],
) -> Vec<crate::pipeline::dialect_pack::RenderArg<'a>> {
    paths
        .iter()
        .zip(rendered.iter())
        .map(|(path, sql)| match path {
            Some(path) => crate::pipeline::dialect_pack::RenderArg::Path(path),
            None => crate::pipeline::dialect_pack::RenderArg::Sql(sql),
        })
        .collect()
}

/// Where SQLite writes a REAL the language places in a document.
#[derive(Clone, Copy)]
enum ExactReal {
    /// A member: a JSON number.
    Member,
    /// A key: the text of the number.
    Label,
}

/// SQLite's admission of a value into a document the language makes.
///
/// SQLite prints a REAL with fifteen significant digits both into a
/// document and into a key, so two REALs that differ past the fifteenth
/// digit enter a document as the same number, or two partitions under one
/// key. A REAL is therefore written in the fewest of fifteen, sixteen or
/// seventeen digits whose text SQLite converts back to the same REAL — the
/// comparison runs the very conversion a document read or a cast of the key
/// performs, so the value reads back as itself. The `!` flag lifts printf's
/// sixteen-digit cap and keeps a decimal point, so an integral REAL stays
/// REAL; the `0` flag spells an infinity as `9.0e+999`, which reads back.
/// Fifteen digits is the writer's own spelling of a finite REAL, so a REAL
/// it already carried keeps its bytes.
///
/// SQLite's decimal conversions are exact only at moderate magnitudes; a
/// REAL no spelling returns refuses at runtime through a JSON path error
/// carrying `INEXACT_DOCUMENT_REAL` and the value, never by carrying a
/// neighbouring number. Every other storage class is the value itself, so a
/// container a path extraction yields keeps its JSON subtype through the
/// CASE. The value is evaluated for its storage class, again for each
/// spelling tried, and again for the one written.
fn write_exact_real(sql: &mut String, value: &str, place: ExactReal) {
    write!(sql, "CASE WHEN typeof({value}) <> 'real' THEN {value}")
        .expect("Writing to String cannot fail");
    for digits in [15, 16, 17] {
        let text = format!("printf('%!0.{digits}g', {value})");
        let written = match place {
            ExactReal::Member => format!("json({text})"),
            ExactReal::Label => text.clone(),
        };
        write!(sql, " WHEN CAST({text} AS REAL) = {value} THEN {written}")
            .expect("Writing to String cannot fail");
    }
    write!(
        sql,
        " ELSE json_extract('null', '{}: no spelling of ' || quote({value}) || \
         ' reads back as the same REAL') END",
        delightql_types::INEXACT_DOCUMENT_REAL
    )
    .expect("Writing to String cannot fail");
}
