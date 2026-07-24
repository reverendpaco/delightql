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
use crate::pipeline::sql_ast_v3::*;
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
pub use dialect::SqlDialect;
pub use errors::GeneratorError;

/// The main SQL generator
pub struct SqlGenerator {
    config: GeneratorConfig,
    /// Bin cartridge registry for resolving rewrite-rule predicates.
    /// None for standalone/utility generation paths.
    bin_registry: Option<Arc<BinCartridgeRegistry>>,
}

impl Default for SqlGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlGenerator {
    pub fn new() -> Self {
        SqlGenerator {
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
    ) -> Result<String, GeneratorError> {
        let mut sql = String::new();
        self.generate_domain_expression(&mut sql, expr)?;
        Ok(sql)
    }

    /// Emit a `name(args)` call with the canonical shape (parens, DISTINCT,
    /// comma-joined args).
    fn write_fn_call(
        &self,
        sql: &mut String,
        name: &str,
        args: &[DomainExpression],
        distinct: bool,
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
            self.generate_domain_expression(sql, arg)?;
        }
        sql.push(')');
        Ok(())
    }

    /// Render each function argument to its own string (for template and
    /// rust_handler render rules, which compose from rendered args).
    fn render_fn_args(&self, args: &[DomainExpression]) -> Result<Vec<String>, GeneratorError> {
        args.iter()
            .map(|arg| {
                let mut s = String::new();
                self.generate_domain_expression(&mut s, arg)?;
                Ok(s)
            })
            .collect()
    }

    /// Generate SQL from a complete statement
    pub fn generate_statement(&self, stmt: &SqlStatement) -> Result<String, GeneratorError> {
        let mut sql = String::new();

        match stmt {
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
                table_name,
                with_clause,
                query,
            } => {
                // Generate CREATE TEMPORARY TABLE statement
                sql.push_str("CREATE TEMPORARY TABLE ");
                identifiers::write_identifier(&mut sql, table_name, self.config.dialect, &self.config.dialect_pack)?;
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
                view_name,
                with_clause,
                query,
            } => {
                // Generate CREATE TEMPORARY VIEW statement
                sql.push_str("CREATE TEMPORARY VIEW ");
                identifiers::write_identifier(&mut sql, view_name, self.config.dialect, &self.config.dialect_pack)?;
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
                target_table,
                target_namespace,
                with_clause,
                where_clause,
            } => {
                if let Some(ctes) = with_clause {
                    self.generate_with_clause(&mut sql, ctes, 0)?;
                    sql.push(' ');
                }
                sql.push_str("DELETE FROM ");
                self.write_table_ref(&mut sql, target_namespace.as_deref(), target_table)?;
                if let Some(wc) = where_clause {
                    sql.push_str(" WHERE ");
                    self.generate_domain_expression(&mut sql, wc)?;
                }
            }
            SqlStatement::Update {
                target_table,
                target_namespace,
                with_clause,
                set_clause,
                where_clause,
            } => {
                if let Some(ctes) = with_clause {
                    self.generate_with_clause(&mut sql, ctes, 0)?;
                    sql.push(' ');
                }
                sql.push_str("UPDATE ");
                self.write_table_ref(&mut sql, target_namespace.as_deref(), target_table)?;
                sql.push_str(" SET ");
                for (i, (col, expr)) in set_clause.iter().enumerate() {
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    identifiers::write_identifier(&mut sql, col, self.config.dialect, &self.config.dialect_pack)?;
                    sql.push_str(" = ");
                    self.generate_domain_expression(&mut sql, expr)?;
                }
                if let Some(wc) = where_clause {
                    sql.push_str(" WHERE ");
                    self.generate_domain_expression(&mut sql, wc)?;
                }
            }
            SqlStatement::Insert {
                target_table,
                target_namespace,
                columns,
                with_clause,
                source,
            } => {
                if let Some(ctes) = with_clause {
                    self.generate_with_clause(&mut sql, ctes, 0)?;
                    sql.push(' ');
                }
                sql.push_str("INSERT INTO ");
                self.write_table_ref(&mut sql, target_namespace.as_deref(), target_table)?;
                if !columns.is_empty() {
                    sql.push_str(" (");
                    for (i, col) in columns.iter().enumerate() {
                        if i > 0 {
                            sql.push_str(", ");
                        }
                        identifiers::write_identifier(&mut sql, col, self.config.dialect, &self.config.dialect_pack)?;
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
            identifiers::write_identifier(sql, cte.name(), self.config.dialect, &self.config.dialect_pack)?;
            sql.push_str(" AS (");

            // CTE query (indented if pretty printing)
            if self.config.pretty_print {
                sql.push('\n');
                self.generate_query_expression(sql, cte.query(), indent + 1)?;
                sql.push('\n');
                self.indent(sql, indent);
            } else {
                self.generate_query_expression(sql, cte.query(), indent)?;
            }

            sql.push(')');
        }

        Ok(())
    }

    /// Generate a query expression (SELECT, UNION, VALUES)
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

                sql.push_str(match op {
                    SetOperator::Union => "UNION",
                    SetOperator::UnionAll => "UNION ALL",
                    SetOperator::Intersect => "INTERSECT",
                    SetOperator::Except => "EXCEPT",
                });

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
                        self.generate_domain_expression(sql, expr)?;
                    }
                    sql.push(')');
                }
            }
            QueryExpression::WithCte { ctes, query } => {
                // Generate nested WITH clause
                self.indent(sql, indent);

                // Check if any CTE is recursive
                let has_recursive = ctes.iter().any(|cte| cte.is_recursive());

                if has_recursive {
                    sql.push_str("WITH RECURSIVE ");
                } else {
                    sql.push_str("WITH ");
                }

                // Generate each CTE (inline the logic from generate_with_clause)
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
                    identifiers::write_identifier(sql, cte.name(), self.config.dialect, &self.config.dialect_pack)?;
                    sql.push_str(" AS (");

                    // CTE query (indented if pretty printing)
                    if self.config.pretty_print {
                        sql.push('\n');
                        self.generate_query_expression(sql, cte.query(), indent + 1)?;
                        sql.push('\n');
                        self.indent(sql, indent);
                    } else {
                        self.generate_query_expression(sql, cte.query(), indent)?;
                    }

                    sql.push(')');
                }

                if self.config.pretty_print {
                    sql.push('\n');
                }

                // Generate the inner query
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
        // SELECT clause
        self.indent(sql, indent);
        sql.push_str("SELECT ");

        if select.is_distinct() {
            sql.push_str("DISTINCT ");
        }

        // Select list
        for (i, item) in select.select_list().iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            self.generate_select_item(sql, item)?;
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
                self.generate_table_expression(sql, table, indent)?;
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
            self.generate_domain_expression(sql, where_clause)?;
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
                self.generate_domain_expression(sql, expr)?;
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
            self.generate_domain_expression(sql, having)?;
        }

        // ORDER BY clause
        if let Some(order_by) = select.order_by() {
            if self.config.pretty_print {
                sql.push('\n');
                self.indent(sql, indent);
            } else {
                sql.push(' ');
            }
            sql.push_str("ORDER BY ");

            for (i, term) in order_by.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                self.generate_order_term(sql, term)?;
            }
        }

        // LIMIT clause
        if let Some(limit) = select.limit() {
            if self.config.pretty_print {
                sql.push('\n');
                self.indent(sql, indent);
            } else {
                sql.push(' ');
            }
            write!(sql, "LIMIT {}", limit.count()).expect("Writing to String cannot fail");

            if let Some(offset) = limit.offset() {
                write!(sql, " OFFSET {}", offset).expect("Writing to String cannot fail");
            }
        }

        Ok(())
    }

    /// Generate a SELECT item
    fn generate_select_item(
        &self,
        sql: &mut String,
        item: &SelectItem,
    ) -> Result<(), GeneratorError> {
        match item {
            SelectItem::Star => {
                sql.push('*');
            }
            SelectItem::QualifiedStar { qualifier } => {
                self.generate_column_qualifier(sql, qualifier)?;
                sql.push_str(".*");
            }
            SelectItem::Expression { expr, alias } => {
                self.generate_domain_expression(sql, expr)?;
                if let Some(alias) = alias {
                    sql.push_str(" AS ");
                    identifiers::write_identifier(sql, alias, self.config.dialect, &self.config.dialect_pack)?;
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
    ) -> Result<(), GeneratorError> {
        match table {
            TableExpression::Table {
                schema,
                name,
                alias,
            } => {
                if let Some(schema) = schema {
                    identifiers::write_identifier(sql, schema, self.config.dialect, &self.config.dialect_pack)?;
                    sql.push('.');
                }
                identifiers::write_identifier(sql, name, self.config.dialect, &self.config.dialect_pack)?;
                if let Some(alias) = alias {
                    sql.push_str(" AS ");
                    identifiers::write_identifier(sql, alias, self.config.dialect, &self.config.dialect_pack)?;
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
                identifiers::write_identifier(sql, alias, self.config.dialect, &self.config.dialect_pack)?;
            }
            TableExpression::Join {
                left,
                right,
                join_type,
                join_condition,
            } => {
                // Generate left side
                self.generate_table_expression(sql, left, indent)?;

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
                self.generate_table_expression(sql, right, indent)?;

                // Generate join condition
                match join_condition {
                    JoinCondition::On(expr) => {
                        sql.push_str(" ON ");
                        self.generate_domain_expression(sql, expr)?;
                    }
                    JoinCondition::Using(columns) => {
                        sql.push_str(" USING (");
                        for (i, col) in columns.iter().enumerate() {
                            if i > 0 {
                                sql.push_str(", ");
                            }
                            identifiers::write_identifier(sql, col, self.config.dialect, &self.config.dialect_pack)?;
                        }
                        sql.push(')');
                    }
                    JoinCondition::Natural => {
                        // NATURAL is already in the join type
                    }
                }
            }
            TableExpression::Values { rows, alias } => {
                // Generate VALUES clause with alias
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
                        self.generate_domain_expression(sql, expr)?;
                    }
                    sql.push(')');
                }
                sql.push_str(" AS ");
                identifiers::write_identifier(sql, alias, self.config.dialect, &self.config.dialect_pack)?;
            }
            TableExpression::UnionTable { selects, alias } => {
                // Generate UNION ALL with column aliases in first SELECT
                sql.push('(');
                if self.config.pretty_print {
                    sql.push('\n');
                }

                for (i, select) in selects.iter().enumerate() {
                    if i > 0 {
                        if self.config.pretty_print {
                            sql.push('\n');
                            self.indent(sql, indent + 1);
                        } else {
                            sql.push(' ');
                        }
                        sql.push_str("UNION ALL");
                        if self.config.pretty_print {
                            sql.push('\n');
                        } else {
                            sql.push(' ');
                        }
                    }

                    if self.config.pretty_print && i == 0 {
                        self.indent(sql, indent + 1);
                    }
                    self.generate_query_expression(sql, select, indent + 1)?;
                }

                if self.config.pretty_print {
                    sql.push('\n');
                    self.indent(sql, indent);
                }
                sql.push_str(") AS ");
                identifiers::write_identifier(sql, alias, self.config.dialect, &self.config.dialect_pack)?;
            }

            TableExpression::TVF {
                schema,
                function,
                arguments,
                alias,
            } => {
                // Render arguments first — both the canonical call shape and
                // a dialect template consume them as text.
                let mut rendered_args: Vec<String> = Vec::with_capacity(arguments.len());
                for arg in arguments {
                    let mut text = String::new();
                    match arg {
                        TvfArgument::ColumnRef { qualifier, column } => {
                            self.generate_column_qualifier(&mut text, qualifier)?;
                            text.push('.');
                            identifiers::write_identifier(&mut text, column, self.config.dialect, &self.config.dialect_pack)?;
                        }
                        TvfArgument::QualifiedRef { qualifier, column } => {
                            identifiers::write_identifier(&mut text, qualifier, self.config.dialect, &self.config.dialect_pack)?;
                            text.push('.');
                            identifiers::write_identifier(&mut text, column, self.config.dialect, &self.config.dialect_pack)?;
                        }
                        _ => text.push_str(&arg.to_sql()),
                    }
                    rendered_args.push(text);
                }

                // Consult the dialect pack under `tvf.<name>` — same contract
                // as `fn.<name>`: internal `__dql_*` names key the pack under
                // their own render key but spell canonically via naming's map,
                // so they never leak into emitted SQL. A template body renders
                // the whole FROM item (minus alias); a bare-NAME body renames
                // the call. The alias is appended by code either way.
                // Sequence positions guard for sequence-ness (own-or-contain
                // ruling): a data-borne non-array or malformed value becomes
                // a NULL interior and takes the null-interior road (zero
                // rows) instead of leaking the backend's polymorphic
                // iteration as phantom rows (an object iterates its pairs, a
                // scalar yields an atom row). Compiler-built packets are
                // always arrays — the guard is inert for them. Template
                // dialects own their guard inside the template body.
                let guarded_args: Vec<String> = if function
                    == crate::pipeline::naming::INTERNAL_JSON_EACH_ARRAY
                {
                    rendered_args
                        .iter()
                        .map(|t| {
                            format!(
                                "CASE WHEN json_valid({t}) AND json_type({t}) = 'array' THEN {t} END"
                            )
                        })
                        .collect()
                } else {
                    rendered_args.clone()
                };

                let tvf_key = format!("tvf.{}", function.to_ascii_lowercase());
                let rule = self
                    .config
                    .dialect_pack
                    .render(self.config.dialect.family_name(), &tvf_key);
                match rule {
                    Some(rule) if rule.rule_kind == "template" => {
                        let body = rule.template().map_err(GeneratorError::Error)?;
                        if body.contains('{') {
                            let refs: Vec<&str> =
                                rendered_args.iter().map(String::as_str).collect();
                            let applied =
                                crate::pipeline::dialect_pack::apply_template(body, &refs)
                                    .map_err(|e| {
                                        GeneratorError::Error(format!("{}: {}", tvf_key, e))
                                    })?;
                            sql.push_str(&applied);
                        } else {
                            self.write_tvf_call(sql, schema.as_deref(), body, &guarded_args);
                        }
                    }
                    Some(rule) => {
                        return Err(GeneratorError::Error(format!(
                            "{}: unsupported rule_kind '{}' (no interpreter built for it)",
                            tvf_key, rule.rule_kind
                        )));
                    }
                    None => {
                        let canonical_name =
                            crate::pipeline::naming::internal_fn_canonical(function)
                                .unwrap_or(function);
                        self.write_tvf_call(sql, schema.as_deref(), canonical_name, &guarded_args);
                    }
                }

                if let Some(alias) = alias {
                    sql.push_str(" AS ");
                    identifiers::write_identifier(sql, alias, self.config.dialect, &self.config.dialect_pack)?;
                }
            }
        }
        Ok(())
    }

    /// Write a TVF call shape: `[schema.]name(arg1, arg2, ...)` from
    /// already-rendered argument texts.
    fn write_tvf_call(&self, sql: &mut String, schema: Option<&str>, name: &str, args: &[String]) {
        if let Some(schema) = schema {
            sql.push_str(schema);
            sql.push('.');
        }
        sql.push_str(name);
        sql.push('(');
        for (i, arg) in args.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(arg);
        }
        sql.push(')');
    }

    /// Write a potentially namespace-qualified table reference
    fn write_table_ref(
        &self,
        sql: &mut String,
        namespace: Option<&str>,
        table: &str,
    ) -> Result<(), GeneratorError> {
        if let Some(ns) = namespace {
            identifiers::write_identifier(sql, ns, self.config.dialect, &self.config.dialect_pack)?;
            sql.push('.');
        }
        identifiers::write_identifier(sql, table, self.config.dialect, &self.config.dialect_pack)?;
        Ok(())
    }

    /// Generate a domain expression
    fn generate_domain_expression(
        &self,
        sql: &mut String,
        expr: &DomainExpression,
    ) -> Result<(), GeneratorError> {
        match expr {
            DomainExpression::Column {
                name, qualifier, ..
            } => {
                // Special handling for NULL marker
                if name == "__NULL__" && qualifier.is_none() {
                    sql.push_str("NULL");
                } else {
                    if let Some(qual) = qualifier {
                        self.generate_column_qualifier(sql, qual)?;
                        sql.push('.');
                    }
                    identifiers::write_identifier(sql, name, self.config.dialect, &self.config.dialect_pack)?;
                }
            }
            DomainExpression::Literal(value) => {
                literals::generate_literal(sql, value, self.config.dialect, &self.config.dialect_pack)?;
            }
            DomainExpression::Binary { left, op, right } => {
                // An op.* row whose body contains '{' is a FULL TEMPLATE over
                // the two rendered operands — for target spellings that change
                // SHAPE, not just token (mysql CONCAT({0}, {1}) is a function;
                // NOT ({0} <=> {1}) wraps). Same body-shape dispatch as the
                // fn.* arm (DIALECT-CONTRACT.md B3). Templates own their
                // parentheses.
                let spelling =
                    operators::binary_operator_to_sql(op, self.config.dialect, &self.config.dialect_pack)?;
                if spelling.contains('{') {
                    let mut left_sql = String::new();
                    self.generate_domain_expression(&mut left_sql, left)?;
                    let mut right_sql = String::new();
                    self.generate_domain_expression(&mut right_sql, right)?;
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

                    self.generate_domain_expression(sql, left)?;
                    sql.push(' ');
                    sql.push_str(spelling);
                    sql.push(' ');
                    self.generate_domain_expression(sql, right)?;

                    if needs_parens {
                        sql.push(')');
                    }
                }
            }
            DomainExpression::Unary { op, expr } => {
                sql.push_str(operators::unary_operator_to_sql(op));
                sql.push(' ');
                self.generate_domain_expression(sql, expr)?;
            }
            DomainExpression::Cast { expr, type_name } => {
                // CAST(x AS T) is universal skeleton; only T's spelling is
                // per-target: `type.<name>` render rows, canonical =
                // uppercased DQL type word. Semantics are the target's cast.
                sql.push_str("CAST(");
                self.generate_domain_expression(sql, expr)?;
                sql.push_str(" AS ");
                let type_key = format!("type.{}", type_name.to_ascii_lowercase());
                match self
                    .config
                    .dialect_pack
                    .render(self.config.dialect.family_name(), &type_key)
                {
                    Some(rule) => {
                        sql.push_str(rule.template().map_err(GeneratorError::Error)?)
                    }
                    None => sql.push_str(&type_name.to_ascii_uppercase()),
                }
                sql.push(')');
            }
            DomainExpression::Function {
                name,
                args,
                distinct,
            } => {
                // Consult the dialect pack for a per-function render rule
                // (`fn.<name>`, lowercased — the AST carries mixed case),
                // dispatching on rule_kind (DESIGN §4.4):
                //   template, bare-NAME body — rename, call shape and
                //     DISTINCT kept;
                //   template with '{'        — re-render from rendered args;
                //   rust_handler             — body names a compiled fn
                //     (argument transformation/synthesis templates can't do).
                // Internal `__dql_*` names key the pack under their own
                // render key but spell canonically via naming's map — they
                // must never leak into emitted SQL.
                let canonical_name =
                    crate::pipeline::naming::internal_fn_canonical(name).unwrap_or(name);
                let fn_key = format!("fn.{}", name.to_ascii_lowercase());
                let rule = self
                    .config
                    .dialect_pack
                    .render(self.config.dialect.family_name(), &fn_key);

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
                        let rendered = self.render_fn_args(args)?;
                        let refs: Vec<&str> = rendered.iter().map(String::as_str).collect();
                        let applied =
                            crate::pipeline::dialect_pack::apply_template(template, &refs)
                                .map_err(|e| {
                                    GeneratorError::Error(format!("{}: {}", fn_key, e))
                                })?;
                        sql.push_str(&applied);
                    }
                    FnRender::Handler(handler) => {
                        let rendered = self.render_fn_args(args)?;
                        let refs: Vec<&str> = rendered.iter().map(String::as_str).collect();
                        let applied = handler(&refs, *distinct).map_err(|e| {
                            GeneratorError::Error(format!("{}: {}", fn_key, e))
                        })?;
                        sql.push_str(&applied);
                    }
                    FnRender::Rename(new_name) => {
                        self.write_fn_call(sql, new_name, args, *distinct)?;
                    }
                    FnRender::Canonical => {
                        // The arbitrary-witness form's canonical spelling is
                        // the bare argument (sqlite's relaxed GROUP BY) —
                        // identity isn't expressible as a rename row, so this
                        // one canonical rule lives in code like the rest of
                        // the canonical spellings.
                        if name == crate::pipeline::naming::INTERNAL_ARBITRARY {
                            let [arg] = args.as_slice() else {
                                return Err(GeneratorError::Error(format!(
                                    "{}: expects exactly 1 argument, got {}",
                                    fn_key,
                                    args.len()
                                )));
                            };
                            self.generate_domain_expression(sql, arg)?;
                        } else {
                            self.write_fn_call(sql, canonical_name, args, *distinct)?;
                        }
                    }
                }
            }
            DomainExpression::WindowFunction {
                name,
                args,
                partition_by,
                order_by,
                frame,
            } => {
                // Function call
                sql.push_str(name);
                sql.push('(');
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    self.generate_domain_expression(sql, arg)?;
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
                        self.generate_domain_expression(sql, expr)?;
                    }
                    has_content = true;
                }

                // ORDER BY
                if !order_by.is_empty() {
                    if has_content {
                        sql.push(' ');
                    }
                    sql.push_str("ORDER BY ");
                    for (i, (expr, sort_order)) in order_by.iter().enumerate() {
                        if i > 0 {
                            sql.push_str(", ");
                        }
                        self.generate_domain_expression(sql, expr)?;
                        match sort_order {
                            crate::pipeline::sql_ast_v3::ordering::OrderDirection::Asc => {
                                sql.push_str(" ASC");
                            }
                            crate::pipeline::sql_ast_v3::ordering::OrderDirection::Desc => {
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
                    self.generate_window_frame(sql, frame_spec)?;
                }

                sql.push(')');
            }
            DomainExpression::Star => {
                sql.push('*');
            }
            DomainExpression::Parens(inner) => {
                sql.push('(');
                self.generate_domain_expression(sql, inner)?;
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
                    self.generate_domain_expression(sql, expr)?;
                }
                for clause in when_clauses {
                    sql.push_str(" WHEN ");
                    self.generate_domain_expression(sql, clause.when())?;
                    sql.push_str(" THEN ");
                    self.generate_domain_expression(sql, clause.then())?;
                }
                if let Some(else_expr) = else_clause {
                    sql.push_str(" ELSE ");
                    self.generate_domain_expression(sql, else_expr)?;
                }
                sql.push_str(" END");
            }
            DomainExpression::InList { expr, not, values } => {
                self.generate_domain_expression(sql, expr)?;
                if *not {
                    sql.push_str(" NOT IN (");
                } else {
                    sql.push_str(" IN (");
                }
                for (i, val) in values.iter().enumerate() {
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    self.generate_domain_expression(sql, val)?;
                }
                sql.push(')');
            }
            DomainExpression::InSubquery { expr, not, query } => {
                self.generate_domain_expression(sql, expr)?;
                if *not {
                    sql.push_str(" NOT IN (");
                } else {
                    sql.push_str(" IN (");
                }
                self.generate_query_expression(sql, query, 0)?;
                sql.push(')');
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
            DomainExpression::Tuple(elements) => {
                sql.push('(');
                for (i, elem) in elements.iter().enumerate() {
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    self.generate_domain_expression(sql, elem)?;
                }
                sql.push(')');
            }
            DomainExpression::RawSql(raw) => {
                // EPOCH 7: Inject raw SQL for melt packets
                sql.push_str(raw);
            }
            DomainExpression::PredicateRewrite {
                name,
                args,
                negated,
            } => {
                self.generate_predicate_rewrite(sql, name, args, *negated)?;
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
        args: &[DomainExpression],
        negated: bool,
    ) -> Result<(), GeneratorError> {
        // DESIGN §4.1 precedence: (entity+form+dialect) → (form+dialect) →
        // canonical code (the bin entity below).
        // The call site IS the sigma form — code chooses the form (as the enum,
        // STRING-FLOOR Tier 2c), data spells it.
        let form_type = crate::enums::EntityType::BinSigmaPredicate;
        if let Some(rule) = self.config.dialect_pack.form_rule(
            self.config.dialect.family_name(),
            form_type,
            name,
        ) {
            let rule_id = format!("form.sigma.{}", name);
            let rendered = self.render_fn_args(args)?;
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
                    let handler =
                        crate::pipeline::dialect_pack::rust_render_handler(&rule.body)
                            .ok_or_else(|| {
                                GeneratorError::Error(format!(
                                    "{}: unknown rust_handler '{}'",
                                    rule_id, rule.body
                                ))
                            })?;
                    // For predicate handlers the flag is NEGATED (they own
                    // their negation spelling; no outer NOT is added).
                    sql.push_str(&handler(&refs, negated).map_err(|e| {
                        GeneratorError::Error(format!("{}: {}", rule_id, e))
                    })?);
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

        let entity = registry.lookup_entity(name).ok_or_else(|| {
            GeneratorError::Error(format!("Unknown predicate rewrite: '{}'", name))
        })?;

        let sql_gen = entity.as_sql_generatable().ok_or_else(|| {
            GeneratorError::Error(format!(
                "Entity '{}' does not implement SqlGeneratable",
                name
            ))
        })?;

        let render_fn = |expr: &DomainExpression| -> String {
            let mut s = String::new();
            // Use a fresh generator (same config) to avoid borrow issues
            let gen = SqlGenerator {
                config: self.config.clone(),
                bin_registry: None,
            };
            gen.generate_domain_expression(&mut s, expr).unwrap();
            s
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

    /// Generate a column qualifier
    fn generate_column_qualifier(
        &self,
        sql: &mut String,
        qual: &ColumnQualifier,
    ) -> Result<(), GeneratorError> {
        match qual.parts() {
            QualifierParts::Table(table) => {
                identifiers::write_identifier(sql, table, self.config.dialect, &self.config.dialect_pack)?;
            }
            QualifierParts::SchemaTable { schema, table } => {
                identifiers::write_identifier(sql, schema, self.config.dialect, &self.config.dialect_pack)?;
                sql.push('.');
                identifiers::write_identifier(sql, table, self.config.dialect, &self.config.dialect_pack)?;
            }
            QualifierParts::DatabaseSchemaTable {
                database,
                schema,
                table,
            } => {
                identifiers::write_identifier(sql, database, self.config.dialect, &self.config.dialect_pack)?;
                sql.push('.');
                identifiers::write_identifier(sql, schema, self.config.dialect, &self.config.dialect_pack)?;
                sql.push('.');
                identifiers::write_identifier(sql, table, self.config.dialect, &self.config.dialect_pack)?;
            }
        }
        Ok(())
    }

    /// Generate an ORDER BY term
    fn generate_order_term(
        &self,
        sql: &mut String,
        term: &OrderTerm,
    ) -> Result<(), GeneratorError> {
        self.generate_domain_expression(sql, term.expr())?;
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
        frame: &crate::pipeline::sql_ast_v3::SqlWindowFrame,
    ) -> Result<(), GeneratorError> {
        use crate::pipeline::sql_ast_v3::SqlFrameMode;

        // Frame mode
        match frame.mode {
            SqlFrameMode::Groups => sql.push_str("GROUPS"),
            SqlFrameMode::Rows => sql.push_str("ROWS"),
            SqlFrameMode::Range => sql.push_str("RANGE"),
        }

        sql.push_str(" BETWEEN ");

        // Start bound
        self.generate_frame_bound(sql, &frame.start, true)?;

        sql.push_str(" AND ");

        // End bound
        self.generate_frame_bound(sql, &frame.end, false)?;

        Ok(())
    }

    /// Generate frame bound
    fn generate_frame_bound(
        &self,
        sql: &mut String,
        bound: &crate::pipeline::sql_ast_v3::SqlFrameBound,
        is_start: bool,
    ) -> Result<(), GeneratorError> {
        use crate::pipeline::sql_ast_v3::SqlFrameBound;

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
                self.generate_domain_expression(sql, expr)?;
                sql.push_str(" PRECEDING");
            }
            SqlFrameBound::Following(expr) => {
                self.generate_domain_expression(sql, expr)?;
                sql.push_str(" FOLLOWING");
            }
        }
        Ok(())
    }
}
