//! Next Generation Pipeline - Full Intercalation Architecture
//!
//! This is a parallel implementation of the DelightQL pipeline with
//! proper type separation between stages.

// ============================================================================
// CRITICAL PIPELINE INVARIANTS - DO NOT MODIFY OR REMOVE
// ============================================================================
// These directives enforce exhaustive pattern matching across the ENTIRE
// pipeline. They are essential to the "NO LIES" principle that prevents
// silent failures and data loss.
//
// WHY THESE MATTER:
// - They force every enum variant to be explicitly handled
// - They prevent defaulting to wrong values when we don't know what to do
// - They make missing implementations visible at compile time (with clippy)
// - They ensure information flows forward without silent drops
//
// WHAT THEY DO:
// - unreachable_patterns: Catches duplicate/dead match arms (rustc built-in)
// - wildcard_enum_match_arm: Bans _ catch-alls in enum matches (clippy only)
// - match_wildcard_for_single_variants: Bans _ when specific variants exist
//
// IF YOU THINK YOU NEED TO DISABLE THESE:
// 1. You probably don't - rethink your approach
// 2. If you REALLY do, use #[allow(...)] at the specific location
// 3. Document WHY that specific case needs an exception
//
// These directives cascade to ALL modules under pipeline/, including:
// - All C-PASS modules (parser, builder, resolver, normalizer, etc.)
// - All CONTRACT modules (cst, ast_unresolved, ast_resolved, etc.)
// ============================================================================
#![deny(unreachable_patterns)] // Works with cargo build
#![deny(clippy::wildcard_enum_match_arm)] // Requires cargo clippy
#![deny(clippy::match_wildcard_for_single_variants)] // Requires cargo clippy

pub mod asts;
pub mod cst;
pub mod parser; // Phase 0: Text → CST // Shared AST core structures
pub mod query_features; // Query feature detection
pub use asts::addressed as ast_addressed;
pub use asts::refined as ast_refined;
pub use asts::resolved as ast_resolved; // Re-export for backward compatibility
pub use asts::unresolved as ast_unresolved; // Re-export for backward compatibility // Re-export for backward compatibility
                                            // Note: sql_ast v1 and sql_ast_v2 referenced below no longer exist in the codebase.
                                            // Only sql_ast_v3 remains as the production SQL AST structure.
                                            // pub mod sql_ast;       // CONTRACT for Phase 4 (OLD - v1 - REMOVED)
                                            // pub mod sql_ast_v2;    // CONTRACT for Phase 4 (OLD - v2 - inductive structure - REMOVED)
pub mod sql_ast_v3; // CONTRACT for Phase 4 (v3 - proper SQL syntax tree with builders - PRODUCTION)
                    // pub mod builder;       // Phase 1: CST → AST(unresolved) - OLD, REPLACED BY builder_v2
pub mod addresser;
pub mod builder_v2; // Phase 1: CST → AST(unresolved) - for recursive grammar
pub mod cfe_precompiler; // Phase 1.5: CFE precompilation (runs after builder, before resolver)
pub mod effect_executor; // Phase 1.X: Execute pseudo-predicates and rewrite AST
pub mod refiner;
pub mod resolver; // Phase 2: AST(unresolved) → AST(resolved) // Phase 3: AST(resolved) → AST(refined)
                  // transformer_v3 shelved — code preserved in transformer_v3BAK/ for reference.
                  // Use `./dqlOLD` binary for TV3 comparison.
pub mod ast_transform; // Unified AST walk infrastructure (JEDI Epoch 0 — replaced ast_fold)
pub mod cfe_substitution; // CFE parameter substitution (shared between transformers)
pub mod precedence; // Operator precedence helpers for infix expressions
pub mod sql_optimizer;
pub mod sql_rewriter;
pub mod transformer_v4; // Phase 4: AST → SQL AST (PRODUCTION)
                        // Note: generator v1 and generator_v2 referenced below no longer exist in the codebase.
                        // Only generator_v3 remains as the production SQL string generator.
                        // pub mod generator;     // Phase 5: SQL AST → SQL String (OLD - v1 - REMOVED)
                        // pub mod generator_v2;  // Phase 5: SQL AST v2 → SQL String (OLD - for transformer_v2 - REMOVED)
pub mod compiled_query; // Compiled query output bundle (primary SQL + assertions + emits)
pub mod danger_gates; // Danger gate system (named safety boundaries, OFF by default)
pub mod generator_v3; // Phase 5: SQL AST v3 → SQL String (PRODUCTION)
pub mod naming; // Common naming utilities for consistent naming across pipeline stages
pub mod option_map; // Option map system (strategy/preference selection)
pub mod pattern; // Pattern matching utilities for column selection
pub mod pipe_chain; // Pipe chain linearization utilities
#[cfg(feature = "recursion_stats")]
pub mod recursion_stats;
pub mod sequential; // Sequential compilation of multi-query source strings
pub mod verdict; // Verdict types for assertion and error hook outcomes // Per-function recursion depth tracking

// Re-export key types and functions

use crate::error::Result;
use crate::lispy::ToLispy;
use crate::sexp_formatter;
use crate::system::DelightQLSystem;
use std::collections::HashSet;
use tree_sitter::Tree;

/// Pipeline orchestrator with built-in diagnostics
///
/// This struct manages the entire compilation pipeline from source text to SQL,
/// maintaining state at each stage and collecting diagnostics along the way.
///
/// All internal state is private to enforce proper encapsulation.
/// Use the execution methods (execute_to_*) to advance the pipeline,
/// and getter methods for read-only access to results.
pub(crate) struct Pipeline<'a> {
    // System reference (provides access to main connection with attached schemas)
    // MUTABLE: Needed for pseudo-predicates that mutate system state (import!, etc.)
    system: &'a mut DelightQLSystem,

    // Source and configuration - PRIVATE
    query_text: String,
    resolution_config: resolver::ResolutionConfig,
    sql_optimization_level: sql_optimizer::OptimizationLevel,
    inline_ctes: bool,
    dialect: generator_v3::SqlDialect,
    is_repl: bool, // Whether this pipeline is for REPL mode (affects parsing)

    // Pipeline stages (cached after execution) - PRIVATE
    cst: Option<Tree>,
    query_unresolved: Option<ast_unresolved::Query>,
    query_features: Option<HashSet<query_features::QueryFeature>>,
    query_resolved: Option<ast_resolved::Query>,
    ast_refined: Option<ast_refined::RelationalExpression>,
    sql_ast: Option<sql_ast_v3::SqlStatement>,
    sql_string: Option<String>,
    sql_kind: compiled_query::SqlKind,

    // Data assertions (compiled from inline (~~assert ... ~~) hooks)
    assertion_specs: Vec<ast_unresolved::AssertionSpec>,
    assertion_sqls: Vec<(String, Option<(usize, usize)>)>,

    // Emit streams (compiled from inline (~~emit:name ... ~~) hooks)
    emit_specs: Vec<ast_unresolved::EmitSpec>,
    emit_sqls: Vec<compiled_query::EmitStream>,

    // Danger gate specs (per-query overrides from (~~danger://uri STATE~~) hooks)
    danger_specs: Vec<ast_unresolved::DangerSpec>,

    // CLI-level danger overrides (session baseline, applied before per-query specs)
    cli_danger_overrides: Vec<ast_unresolved::DangerSpec>,

    // Option specs (per-query overrides from (~~option://uri STATE~~) hooks)
    option_specs: Vec<ast_unresolved::OptionSpec>,

    // CLI-level option overrides (session baseline, applied before per-query specs)
    cli_option_overrides: Vec<ast_unresolved::OptionSpec>,

    // Inline DDL blocks (from (~~ddl ... ~~) annotations, processed before resolution)
    ddl_blocks: Vec<ast_unresolved::InlineDdlSpec>,

    // When true, skip DDL block processing (sequential mode handles it upfront)
    skip_ddl_processing: bool,

    // Connection routing - which connection should execute this query
    connection_id: Option<i64>,
}

// NOTE: Previously we had a custom Drop impl for WASM to leak Trees and prevent corruption.
// With tree-sitter-c2rust's pure Rust runtime, this is no longer needed!
// The Rust allocator handles cleanup correctly in WASM.

impl<'a> Pipeline<'a> {
    /// Create a new pipeline for the given source text
    pub fn new(source: &str, system: &'a mut DelightQLSystem) -> Self {
        Self::new_with_config(
            source,
            system,
            resolver::ResolutionConfig::default(),
            sql_optimizer::level_from_env(),
            false, // inline_ctes
            false, // is_repl
        )
    }

    /// Create a pipeline from a pre-built unresolved query, skipping parse.
    ///
    /// Used by the effect executor to compile pipe sources through the full
    /// pipeline when the source isn't a bare anonymous table.
    pub fn new_from_unresolved_query(
        query: ast_unresolved::Query,
        system: &'a mut DelightQLSystem,
    ) -> Self {
        let mut pipeline = Self::new_with_config(
            "<injected>",
            system,
            resolver::ResolutionConfig::default(),
            sql_optimizer::level_from_env(),
            false,
            false,
        );
        pipeline.query_unresolved = Some(query);
        pipeline.query_features = Some(HashSet::new());
        pipeline
    }

    /// Create a new pipeline with custom configuration
    pub fn new_with_config(
        source: &str,
        system: &'a mut DelightQLSystem,
        resolution_config: resolver::ResolutionConfig,
        sql_optimization_level: sql_optimizer::OptimizationLevel,
        inline_ctes: bool,
        is_repl: bool,
    ) -> Self {
        Self {
            system,
            query_text: source.to_string(),
            resolution_config,
            sql_optimization_level,
            inline_ctes,
            dialect: generator_v3::SqlDialect::SQLite, // Default to SQLite
            is_repl,
            cst: None,
            query_unresolved: None,
            query_features: None,
            query_resolved: None,
            ast_refined: None,
            sql_ast: None,
            sql_string: None,
            sql_kind: compiled_query::SqlKind::Query,
            assertion_specs: Vec::new(),
            assertion_sqls: Vec::new(),
            emit_specs: Vec::new(),
            emit_sqls: Vec::new(),
            danger_specs: Vec::new(),
            cli_danger_overrides: Vec::new(),
            option_specs: Vec::new(),
            cli_option_overrides: Vec::new(),
            ddl_blocks: Vec::new(),
            skip_ddl_processing: false,
            connection_id: None, // Will be set during resolution
        }
    }

    /// Get reference to the CST if available
    pub(crate) fn cst(&self) -> Option<&Tree> {
        self.cst.as_ref()
    }

    /// Get reference to the unresolved query if available
    pub fn query_unresolved(&self) -> Option<&ast_unresolved::Query> {
        self.query_unresolved.as_ref()
    }

    /// Get reference to the resolved query if available
    pub fn query_resolved(&self) -> Option<&ast_resolved::Query> {
        self.query_resolved.as_ref()
    }

    /// Get reference to the refined AST if available
    pub fn ast_refined(&self) -> Option<&ast_refined::RelationalExpression> {
        self.ast_refined.as_ref()
    }

    /// Get reference to the SQL AST if available
    pub fn sql_ast(&self) -> Option<&sql_ast_v3::SqlStatement> {
        self.sql_ast.as_ref()
    }

    /// Determine the connection ID for this query by analyzing the resolved query
    ///
    /// This inspects the first entity in the resolved query to determine which
    /// connection it belongs to. All entities in a query must be on the same connection.
    ///
    /// Returns Some(connection_id) if determined, None if query hasn't been resolved yet
    /// or doesn't contain namespace-qualified entities.
    pub fn determine_connection_id(&mut self) -> Result<Option<i64>> {
        // Connection ID is determined during resolution and cached.
        // If not set (pure literal query with no table references), default to user connection.
        if self.connection_id.is_none() {
            self.connection_id = Some(2); // Default to user connection
        }

        // MetaIze generates pure VALUES SQL with no table access.
        // Override routing to user connection regardless of which backend
        // owned the source relation. (Fixes: duckdb_metaize bug)
        if let Some(ref query) = self.query_resolved {
            if query_has_meta_ize(query) {
                self.connection_id = Some(2);
            }
        }

        Ok(self.connection_id)
    }

    /// Set CLI-level danger overrides (session baseline).
    /// These are applied before per-query inline overrides.
    /// Returns an error if any override targets a danger that is not CLI-overridable
    /// (semantic dangers that change language meaning must be specified inline).
    pub fn set_cli_danger_overrides(
        &mut self,
        overrides: Vec<ast_unresolved::DangerSpec>,
    ) -> Result<()> {
        for spec in &overrides {
            if !danger_gates::is_cli_overridable(&spec.uri) {
                return Err(crate::error::DelightQLError::validation_error(
                    format!(
                        "Danger '{}' cannot be overridden from the CLI. \
                         It changes language semantics and must be specified inline \
                         in the query text: (~~danger://{}~~)",
                        spec.uri, spec.uri
                    ),
                    "set_cli_danger_overrides",
                ));
            }
        }

        // Update danger table on bootstrap to reflect CLI overrides (live state)
        #[cfg(not(target_arch = "wasm32"))]
        {
            let conn = self
                .system
                .bootstrap_connection()
                .lock()
                .expect("FATAL: Failed to acquire bootstrap lock for danger override");
            for spec in &overrides {
                let _ = conn.execute(
                    "UPDATE danger SET state = ?1 WHERE uri = ?2",
                    rusqlite::params![spec.state.to_string(), spec.uri],
                );
            }
        }

        self.cli_danger_overrides = overrides;
        Ok(())
    }

    /// Set CLI-level option overrides (session baseline).
    /// These are applied before per-query inline overrides.
    pub fn set_cli_option_overrides(&mut self, overrides: Vec<ast_unresolved::OptionSpec>) {
        self.cli_option_overrides = overrides;
    }

    /// Compile the query and return a bundled result.
    ///
    /// Runs the full pipeline (CST → AST → SQL) and returns a
    /// `CompiledQuery` containing the primary SQL, assertion SQL,
    /// emit streams, and connection routing. The host executes
    /// each piece and decides how to display/route the results.
    pub fn compile(&mut self) -> Result<compiled_query::CompiledQuery> {
        self.execute_to_sql()?;
        let _ = self.determine_connection_id();
        Ok(compiled_query::CompiledQuery {
            primary_sql: self.sql_string.clone().unwrap_or_default(),
            _kind: self.sql_kind,
            assertion_sqls: self.assertion_sqls.clone(),
            emit_streams: self.emit_sqls.clone(),
            connection_id: self.connection_id,
        })
    }

    /// Render the pipeline output at a named stage as a pretty-printed string.
    ///
    /// This is the single source of truth for "execute to stage and serialize".
    /// Both the CLI `--to` handler and the `sys::execution.compile()` bin entity
    /// delegate to this method.
    ///
    /// Valid stages: `"cst"`, `"ast-unresolved"`, `"ast-resolved"`, `"ast-refined"`,
    /// `"ast-sql"`, `"sql"`.
    pub(crate) fn render_stage(&mut self, stage: &str) -> Result<String> {
        match stage {
            "cst" => {
                let tree = self.execute_to_cst_for_output()?;
                Ok(sexp_formatter::custom_pretty_print(&tree.root_node().to_sexp()))
            }
            "ast-unresolved" => {
                self.execute_to_query_unresolved()?;
                let query = self.query_unresolved().unwrap();
                Ok(sexp_formatter::custom_pretty_print(&query.to_lispy()))
            }
            "ast-resolved" => {
                self.execute_to_query_resolved()?;
                let query = self.query_resolved().unwrap();
                Ok(sexp_formatter::custom_pretty_print(&query.to_lispy()))
            }
            "ast-refined" => {
                self.execute_to_ast_refined()?;
                if let Some(ast) = self.ast_refined() {
                    Ok(sexp_formatter::custom_pretty_print(&ast.to_lispy()))
                } else {
                    // CFE queries: ast_refined is None, refine the full query instead
                    let query_resolved = self.query_resolved().unwrap();
                    let query_refined = refiner::refine_query(query_resolved.clone())?;
                    Ok(sexp_formatter::custom_pretty_print(&query_refined.to_lispy()))
                }
            }
            "ast-sql" => {
                self.execute_to_sql_ast()?;
                let sql_ast = self.sql_ast().unwrap();
                let generator = generator_v3::SqlGenerator::new();
                generator.generate_statement(sql_ast)
                    .map_err(|e| e.into_delightql_error("SQL AST rendering error"))
            }
            "sql" => {
                let sql = self.execute_to_sql()?;
                Ok(sql.to_string())
            }
            _ => Err(crate::error::DelightQLError::database_error(
                format!("Unknown stage: '{}'. Valid: cst, ast-unresolved, ast-resolved, ast-refined, ast-sql, sql", stage),
                "Invalid stage",
            )),
        }
    }

    // ========================================================================
    // Diagnostics
    // ========================================================================

    /// Record a DelightQLError to the errors table on bootstrap for session-level error history.
    pub fn record_delightql_error(&self, _e: &crate::error::DelightQLError) {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let uri = _e.error_uri();
            let message = _e.to_string();

            let conn = self
                .system
                .bootstrap_connection()
                .lock()
                .expect("FATAL: Failed to acquire bootstrap lock for errors recording");
            let _ = conn.execute(
                "INSERT INTO errors (uri, message, query_text) VALUES (?1, ?2, ?3)",
                rusqlite::params![uri, message, self.query_text],
            );
        }
    }

    /// Execute pipeline to CST (parse only)
    pub fn execute_to_cst(&mut self) -> Result<&Tree> {
        if self.cst.is_some() {
            return Ok(self.cst.as_ref().unwrap());
        }

        let tree = parser::parse(&self.query_text).map_err(|e| {
            self.record_delightql_error(&e);
            e
        })?;

        self.cst = Some(tree);
        Ok(self.cst.as_ref().unwrap())
    }

    /// Execute pipeline to CST for output (includes ERROR nodes for display)
    ///
    /// This variant uses a special parser that preserves ERROR nodes in the CST,
    /// which is useful for displaying parse errors in CST output mode.
    pub fn execute_to_cst_for_output(&mut self) -> Result<&Tree> {
        if self.cst.is_some() {
            return Ok(self.cst.as_ref().unwrap());
        }

        let tree = parser::parse_for_cst_output(&self.query_text).map_err(|e| {
            self.record_delightql_error(&e);
            e
        })?;

        self.cst = Some(tree);
        Ok(self.cst.as_ref().unwrap())
    }

    /// Execute pipeline to unresolved Query
    pub fn execute_to_query_unresolved(&mut self) -> Result<&ast_unresolved::Query> {
        if self.query_unresolved.is_some() {
            return Ok(self.query_unresolved.as_ref().unwrap());
        }

        // For REPL mode, use REPL-aware parsers; otherwise use standard parsers
        let (query, features, assertions, emits, dangers, options, ddl_blocks) = if self.is_repl {
            // REPL mode: Use parse_repl and parse_repl_input
            let tree = parser::parse_repl(&self.query_text).map_err(|e| {
                self.record_delightql_error(&e);
                e
            })?;

            let result = builder_v2::parse_repl_input(&tree, &self.query_text).map_err(|e| {
                self.record_delightql_error(&e);
                e
            })?;

            // Store CST for later access
            self.cst = Some(tree);
            result
        } else {
            // Standard mode: Use regular parsers via execute_to_cst
            self.execute_to_cst()?;
            let tree = self.cst.as_ref().unwrap();

            builder_v2::parse_query(tree, &self.query_text).map_err(|e| {
                self.record_delightql_error(&e);
                e
            })?
        };

        // DEBUG: Compare unresolved ASTs between REPL and sequential paths
        {
            let variant = match &query {
                ast_unresolved::Query::Relational(_) => "Relational",
                ast_unresolved::Query::WithCtes { ctes, .. } => {
                    log::debug!("  unresolved CTE count: {}", ctes.len());
                    "WithCtes"
                }
                ast_unresolved::Query::WithCfes { .. } => "WithCfes",
                ast_unresolved::Query::WithPrecompiledCfes { .. } => "WithPrecompiledCfes",
                ast_unresolved::Query::ReplTempTable { .. } => "ReplTempTable",
                ast_unresolved::Query::ReplTempView { .. } => "ReplTempView",
                ast_unresolved::Query::WithErContext { .. } => "WithErContext",
            };
            log::debug!(
                "execute_to_query_unresolved: is_repl={}, variant={}",
                self.is_repl,
                variant
            );
        }

        self.query_unresolved = Some(query);
        self.query_features = Some(features);
        self.assertion_specs = assertions;
        self.emit_specs = emits;
        self.danger_specs = dangers;
        self.option_specs = options;
        self.ddl_blocks = ddl_blocks;
        Ok(self.query_unresolved.as_ref().unwrap())
    }

    /// Execute pipeline to resolved Query (Phase 2: uses injected schema)
    ///
    /// Gets the database schema from the system (injected at construction) rather
    /// than taking it as a parameter, maintaining clean architecture.
    pub fn execute_to_query_resolved(&mut self) -> Result<&ast_resolved::Query> {
        if self.query_resolved.is_some() {
            return Ok(self.query_resolved.as_ref().unwrap());
        }

        // First get unresolved query
        self.execute_to_query_unresolved()?;
        let query_unresolved = self.query_unresolved.as_ref().unwrap();

        // Process inline DDL blocks before effects and resolution
        // (skipped in sequential mode — sequential handles DDL upfront for cross-query visibility)
        if !self.skip_ddl_processing {
            for ddl in std::mem::take(&mut self.ddl_blocks) {
                let suffix = ddl.namespace.as_deref().unwrap_or("user");
                let namespace = format!("main::{}", suffix);
                sequential::process_inline_ddl_block(&ddl.body, &namespace, self.system).map_err(
                    |e| {
                        crate::error::DelightQLError::database_error(
                            format!("Inline DDL error: {}", e),
                            "inline DDL",
                        )
                    },
                )?;
            }
        }

        // Phase 1.X: Execute pseudo-predicates and rewrite AST
        // This must happen BEFORE CFE precompilation because pseudo-predicates
        // might register namespaces needed by CFEs
        let query_after_effects =
            effect_executor::execute_effects(query_unresolved.clone(), &mut self.system).map_err(
                |e| {
                    self.record_delightql_error(&e);
                    e
                },
            )?;

        // Get schema from system (injected by CLI) - NO coupling to backends!
        let schema = self.system.get_schema()?;

        // Precompile CFEs if present (runs resolve+refine on CFE bodies)
        // This happens AFTER builder and effect execution but BEFORE main query resolution
        let query_with_precompiled_cfes =
            cfe_precompiler::precompile_query_cfes(query_after_effects, schema, Some(self.system))
                .map_err(|e| {
                    self.record_delightql_error(&e);
                    e
                })?;

        // Resolve (passing system for namespace resolution)
        let resolution_result = resolver::resolve_query(
            query_with_precompiled_cfes,
            schema,
            Some(self.system),
            &self.resolution_config,
        )
        .map_err(|e| {
            self.record_delightql_error(&e);
            e
        })?;

        // Store connection_id for routing during execution
        self.connection_id = resolution_result.connection_id;
        self.query_resolved = Some(resolution_result.query);
        Ok(self.query_resolved.as_ref().unwrap())
    }

    /// Execute pipeline to refined AST (Phase 2: uses injected schema)
    pub fn execute_to_ast_refined(&mut self) -> Result<Option<&ast_refined::RelationalExpression>> {
        if self.ast_refined.is_some() {
            return Ok(self.ast_refined.as_ref().map(|r| r));
        }

        // First get resolved query (schema is now obtained internally)
        self.execute_to_query_resolved()?;
        let query_resolved = self.query_resolved.as_ref().unwrap();

        // Refine (only works for relational queries, not CTEs/CFEs)
        match query_resolved {
            ast_resolved::Query::Relational(expr) => {
                let refined = refiner::refine(expr.clone()).map_err(|e| {
                    self.record_delightql_error(&e);
                    e
                })?;
                self.ast_refined = Some(refined);
                Ok(self.ast_refined.as_ref().map(|r| r))
            }
            other => panic!("catch-all hit in mod.rs execute_to_query_refined: unexpected resolved Query variant: {:?}", other),
        }
    }

    /// Execute pipeline to SQL AST (Phase 2: uses injected schema)
    pub fn execute_to_sql_ast(&mut self) -> Result<&sql_ast_v3::SqlStatement> {
        if self.sql_ast.is_some() {
            return Ok(self.sql_ast.as_ref().unwrap());
        }

        // First get resolved query (schema is now obtained internally)
        self.execute_to_query_resolved()?;
        let query_resolved = self.query_resolved.as_ref().unwrap();

        // Build danger gate map from per-query overrides (needed by refiner and transformer)
        let mut query_danger_gates = danger_gates::DangerGateMap::with_defaults();
        query_danger_gates.apply_overrides(&self.cli_danger_overrides);
        query_danger_gates.apply_overrides(&self.danger_specs);

        // Refine and transform
        let refined_query = refiner::refine_query_with_gates(
            query_resolved.clone(),
            query_danger_gates.clone(),
        )
        .map_err(|e| {
            self.record_delightql_error(&e);
            e
        })?;
        let addressed_query = addresser::address_query(refined_query).map_err(|e| {
            self.record_delightql_error(&e);
            e
        })?;

        let force_ctes = !self.inline_ctes;
        let bin_registry = self.system.bin_registry();

        // Build option map from per-query overrides
        let mut options = option_map::OptionMap::with_defaults();
        options.apply_overrides(&self.cli_option_overrides); // Session baseline (CLI --option)
        options.apply_overrides(&self.option_specs); // Per-query inline overrides

        let ctx = transformer_v4::TransformCtx {
            cfes: vec![],
            names: transformer_v4::builder::NameGenerator::new(),
            outer_columns: vec![],
            danger_gates: query_danger_gates.clone(),
        };
        let sql_ast = transformer_v4::transform(addressed_query, &ctx).map_err(|e| {
            self.record_delightql_error(&e);
            e
        })?;

        // DEBUG: Compare SQL AST structure between REPL and sequential paths
        {
            let gen = generator_v3::SqlGenerator::new();
            if let Ok(sql_preview) = gen.generate_statement(&sql_ast) {
                log::debug!(
                    "execute_to_sql_ast: is_repl={}, sql_preview={}",
                    self.is_repl,
                    sql_preview
                );
            }
        }

        self.sql_ast = Some(sql_ast);
        Ok(self.sql_ast.as_ref().unwrap())
    }

    /// Execute full pipeline to SQL string (Phase 2: uses injected schema)
    pub fn execute_to_sql(&mut self) -> Result<&str> {
        if self.sql_string.is_some() {
            return Ok(self.sql_string.as_ref().unwrap());
        }

        // First get SQL AST (schema is now obtained internally)
        self.execute_to_sql_ast()?;
        let sql_ast = self.sql_ast.as_ref().unwrap();

        // Dialect-aware rewriting, then optimize
        let rewritten = sql_rewriter::rewrite(sql_ast.clone(), self.dialect)?;
        let optimized = sql_optimizer::optimize(rewritten, self.sql_optimization_level)
            .map_err(|e| {
                self.record_delightql_error(&e);
                e
            })?;

        // Generate SQL string
        let generator =
            generator_v3::SqlGenerator::new().with_bin_registry(self.system.bin_registry());
        let sql = generator
            .generate_statement(&optimized)
            .map_err(|e| e.into_delightql_error("SQL generation error"))?;

        self.sql_string = Some(sql);

        // Determine SQL kind from the AST
        self.sql_kind = match sql_ast {
            sql_ast_v3::SqlStatement::Delete { .. }
            | sql_ast_v3::SqlStatement::Update { .. }
            | sql_ast_v3::SqlStatement::Insert { .. } => compiled_query::SqlKind::Dml,
            sql_ast_v3::SqlStatement::Query { .. }
            | sql_ast_v3::SqlStatement::CreateTempTable { .. }
            | sql_ast_v3::SqlStatement::CreateTempView { .. } => compiled_query::SqlKind::Query,
        };

        // Compile assertion bodies to SQL
        if !self.assertion_specs.is_empty() {
            let schema = self.system.get_schema()?;
            let bin_registry = self.system.bin_registry();
            let specs = std::mem::take(&mut self.assertion_specs);
            let mut compiled = Vec::with_capacity(specs.len());

            // Rebuild danger/option maps so assertions inherit per-query gates
            let mut assert_danger_gates = danger_gates::DangerGateMap::with_defaults();
            assert_danger_gates.apply_overrides(&self.cli_danger_overrides);
            assert_danger_gates.apply_overrides(&self.danger_specs);
            let mut assert_options = option_map::OptionMap::with_defaults();
            assert_options.apply_overrides(&self.cli_option_overrides);
            assert_options.apply_overrides(&self.option_specs);

            // Extract CTEs and CFEs from the main query so assertions can reference
            // CTE names and CFE definitions from the outer scope.
            let (outer_ctes, outer_cfes): (
                Vec<ast_unresolved::CteBinding>,
                Vec<ast_unresolved::CfeDefinition>,
            ) = match self.query_unresolved.as_ref() {
                Some(ast_unresolved::Query::WithCtes { ctes, .. }) => (ctes.clone(), vec![]),
                Some(ast_unresolved::Query::WithCfes { cfes, query, .. }) => {
                    let ctes = match query.as_ref() {
                            ast_unresolved::Query::WithCtes { ctes, .. } => ctes.clone(),
                            ast_unresolved::Query::Relational(_) => vec![],
                            other => panic!("catch-all hit in mod.rs outer_ctes extraction: unexpected inner Query variant: {:?}", other),
                        };
                    (ctes, cfes.clone())
                }
                Some(ast_unresolved::Query::WithPrecompiledCfes { query, .. }) => {
                    let ctes = match query.as_ref() {
                            ast_unresolved::Query::WithCtes { ctes, .. } => ctes.clone(),
                            ast_unresolved::Query::Relational(_) => vec![],
                            other => panic!("catch-all hit in mod.rs outer_ctes extraction: unexpected inner Query variant: {:?}", other),
                        };
                    (ctes, vec![])
                }
                _ => (vec![], vec![]),
            };

            for spec in &specs {
                // Wrap the assertion body in a Query, including outer CTEs and CFEs
                // so CTE references and CFE definitions resolve correctly inside assertions.
                let mut assertion_query = ast_unresolved::Query::Relational(spec.body.clone());

                // Wrap with CTEs first (innermost layer)
                if !outer_ctes.is_empty() {
                    assertion_query = ast_unresolved::Query::WithCtes {
                        ctes: outer_ctes.clone(),
                        query: match assertion_query {
                            ast_unresolved::Query::Relational(expr) => expr,
                            other => panic!("unexpected assertion query variant: {:?}", other),
                        },
                    };
                }

                // Wrap with CFEs (outermost layer) — precompiler will resolve+refine them
                if !outer_cfes.is_empty() {
                    assertion_query = ast_unresolved::Query::WithCfes {
                        cfes: outer_cfes.clone(),
                        query: Box::new(assertion_query),
                    };
                }

                // Precompile CFEs in the assertion query (resolve+refine CFE bodies)
                let assertion_query = cfe_precompiler::precompile_query_cfes(
                    assertion_query,
                    schema,
                    Some(self.system),
                )
                .map_err(|e| {
                    self.record_delightql_error(&e);
                    e
                })?;

                // Resolve
                let resolved_result = resolver::resolve_query(
                    assertion_query,
                    schema,
                    Some(self.system),
                    &self.resolution_config,
                )
                .map_err(|e| {
                    self.record_delightql_error(&e);
                    e
                })?;

                // Refine (assertion uses same danger gates as main query)
                let refined = refiner::refine_query_with_gates(
                    resolved_result.query,
                    assert_danger_gates.clone(),
                )
                .map_err(|e| {
                    self.record_delightql_error(&e);
                    e
                })?;
                let addressed = addresser::address_query(refined).map_err(|e| {
                    self.record_delightql_error(&e);
                    e
                })?;

                // Transform to SQL AST
                let assert_ctx = transformer_v4::TransformCtx {
                    cfes: vec![],
                    names: transformer_v4::builder::NameGenerator::new(),
                    outer_columns: vec![],
                    danger_gates: assert_danger_gates.clone(),
                };
                let sql_ast = transformer_v4::transform(addressed, &assert_ctx).map_err(|e| {
                    self.record_delightql_error(&e);
                    e
                })?;

                // Dialect-aware rewriting, then optimize
                let rewritten = sql_rewriter::rewrite(sql_ast, self.dialect)?;
                let optimized = sql_optimizer::optimize(rewritten, self.sql_optimization_level)
                    .map_err(|e| {
                        self.record_delightql_error(&e);
                        e
                    })?;

                // Generate SQL string
                let generator =
                    generator_v3::SqlGenerator::new().with_bin_registry(bin_registry.clone());
                let assertion_sql = generator
                    .generate_statement(&optimized)
                    .map_err(|e| e.into_delightql_error("Assertion SQL generation error"))?;

                // Wrap the assertion SQL to produce a single-row,
                // single-column boolean result.
                let bool_sql = match spec.predicate {
                    ast_unresolved::AssertionPredicate::Exists => {
                        format!("SELECT EXISTS({}) AS bool", assertion_sql)
                    }
                    ast_unresolved::AssertionPredicate::NotExists => {
                        format!("SELECT NOT EXISTS({}) AS bool", assertion_sql)
                    }
                    ast_unresolved::AssertionPredicate::Forall => {
                        // Forall: the builder already negated the terminal
                        // predicates (`, P |> forall(*)` → `, NOT(P)`).
                        // Just wrap in NOT EXISTS, same as NotExists.
                        format!("SELECT NOT EXISTS({}) AS bool", assertion_sql)
                    }
                    ast_unresolved::AssertionPredicate::Equals => {
                        // Equals: bag equality via symmetric difference.
                        // Compile right_operand to SQL and check that
                        // (left EXCEPT right) UNION ALL (right EXCEPT left)
                        // is empty.
                        let right_rel = spec
                            .right_operand
                            .as_ref()
                            .expect("Equals assertion must have right_operand");
                        // Include outer CTEs so the right operand can
                        // reference CTE names from the main query scope.
                        let right_query = if outer_ctes.is_empty() {
                            ast_unresolved::Query::Relational(right_rel.clone())
                        } else {
                            ast_unresolved::Query::WithCtes {
                                ctes: outer_ctes.clone(),
                                query: right_rel.clone(),
                            }
                        };
                        let right_resolved_result = resolver::resolve_query(
                            right_query,
                            schema,
                            Some(self.system),
                            &self.resolution_config,
                        )?;
                        let right_refined = refiner::refine_query_with_gates(
                            right_resolved_result.query,
                            assert_danger_gates.clone(),
                        )?;
                        let right_addressed = addresser::address_query(right_refined)?;
                        let right_ctx = transformer_v4::TransformCtx {
                            cfes: vec![],
                            names: transformer_v4::builder::NameGenerator::new(),
                            outer_columns: vec![],
                            danger_gates: danger_gates::DangerGateMap::with_defaults(),
                        };
                        let right_sql_ast = transformer_v4::transform(right_addressed, &right_ctx)?;
                        let right_rewritten = sql_rewriter::rewrite(right_sql_ast, self.dialect)?;
                        let right_optimized =
                            sql_optimizer::optimize(right_rewritten, self.sql_optimization_level)?;
                        let right_generator = generator_v3::SqlGenerator::new()
                            .with_bin_registry(bin_registry.clone());
                        let right_sql = right_generator
                            .generate_statement(&right_optimized)
                            .map_err(|e| {
                                e.into_delightql_error("Equals right SQL generation error")
                            })?;

                        // Bag equality: same row count AND same set of rows.
                        // Count check catches differing multiplicities.
                        // EXCEPT both ways catches differing content.
                        format!(
                            "SELECT (\
                            (SELECT COUNT(*) FROM ({left})) = (SELECT COUNT(*) FROM ({right})) \
                            AND NOT EXISTS(SELECT * FROM ({left}) EXCEPT SELECT * FROM ({right})) \
                            AND NOT EXISTS(SELECT * FROM ({right}) EXCEPT SELECT * FROM ({left}))\
                            ) AS bool",
                            left = assertion_sql,
                            right = right_sql,
                        )
                    }
                };

                compiled.push((bool_sql, spec.source_location));
            }

            self.assertion_specs = specs;
            self.assertion_sqls = compiled;
        }

        // Compile emit bodies to SQL
        if !self.emit_specs.is_empty() {
            let schema = self.system.get_schema()?;
            let bin_registry = self.system.bin_registry();
            let specs = std::mem::take(&mut self.emit_specs);
            let mut compiled_emits = Vec::with_capacity(specs.len());

            for spec in &specs {
                let emit_query = ast_unresolved::Query::Relational(spec.body.clone());

                let resolved_result = resolver::resolve_query(
                    emit_query,
                    schema,
                    Some(self.system),
                    &self.resolution_config,
                )
                .map_err(|e| {
                    self.record_delightql_error(&e);
                    e
                })?;

                let mut emit_danger_gates = danger_gates::DangerGateMap::with_defaults();
                emit_danger_gates.apply_overrides(&self.cli_danger_overrides);
                emit_danger_gates.apply_overrides(&self.danger_specs);

                // Emit streams use the same danger gates as the main query
                let refined = refiner::refine_query_with_gates(
                    resolved_result.query,
                    emit_danger_gates.clone(),
                )
                .map_err(|e| {
                    self.record_delightql_error(&e);
                    e
                })?;
                let addressed = addresser::address_query(refined).map_err(|e| {
                    self.record_delightql_error(&e);
                    e
                })?;
                let emit_ctx = transformer_v4::TransformCtx {
                    cfes: vec![],
                    names: transformer_v4::builder::NameGenerator::new(),
                    outer_columns: vec![],
                    danger_gates: emit_danger_gates,
                };
                let sql_ast = transformer_v4::transform(addressed, &emit_ctx).map_err(|e| {
                    self.record_delightql_error(&e);
                    e
                })?;

                let rewritten = sql_rewriter::rewrite(sql_ast, self.dialect)?;
                let optimized = sql_optimizer::optimize(rewritten, self.sql_optimization_level)
                    .map_err(|e| {
                        self.record_delightql_error(&e);
                        e
                    })?;

                let generator =
                    generator_v3::SqlGenerator::new().with_bin_registry(bin_registry.clone());
                let emit_sql = generator
                    .generate_statement(&optimized)
                    .map_err(|e| e.into_delightql_error("Emit SQL generation error"))?;

                compiled_emits.push(compiled_query::EmitStream {
                    name: spec.name.clone(),
                    sql: emit_sql,
                    _source_location: spec.source_location,
                });
            }

            self.emit_specs = specs;
            self.emit_sqls = compiled_emits;
        }

        Ok(self.sql_string.as_ref().unwrap())
    }
}

/// Check whether the resolved query's top-level pipe chain contains a MetaIze operator.
///
/// MetaIze generates pure VALUES SQL with no table access, so the query
/// must NOT be routed to an external backend (connection_id >= 3).
/// Only walks the source chain (Pipe sources and Filter sources) — if
/// MetaIze is buried inside a join arm, the outer query still needs the
/// real table's connection.
#[stacksafe::stacksafe]
fn query_has_meta_ize(query: &ast_resolved::Query) -> bool {
    fn expr_has_meta_ize(expr: &ast_resolved::RelationalExpression) -> bool {
        match expr {
            ast_resolved::RelationalExpression::Pipe(pipe) => {
                matches!(
                    pipe.operator,
                    ast_resolved::UnaryRelationalOperator::MetaIze { .. }
                ) || expr_has_meta_ize(&pipe.source)
            }
            ast_resolved::RelationalExpression::Filter { source, .. } => expr_has_meta_ize(source),
            ast_resolved::RelationalExpression::Relation(_)
            | ast_resolved::RelationalExpression::Join { .. }
            | ast_resolved::RelationalExpression::SetOperation { .. } => false,
            // ER chains consumed before meta-ize check
            ast_resolved::RelationalExpression::ErJoinChain { .. }
            | ast_resolved::RelationalExpression::ErTransitiveJoin { .. } => {
                unreachable!("ER chains consumed before meta-ize check")
            }
            ast_resolved::RelationalExpression::IntersectCorresponding { .. } => {
                unreachable!("IntersectCorresponding only exists in Refined/Addressed phases")
            }
        }
    }

    match query {
        ast_resolved::Query::Relational(expr) => expr_has_meta_ize(expr),
        ast_resolved::Query::WithCtes { query: body, .. } => expr_has_meta_ize(body),
        // Wrapper variants: recurse into inner query
        ast_resolved::Query::WithPrecompiledCfes { query: body, .. }
        | ast_resolved::Query::ReplTempTable { query: body, .. }
        | ast_resolved::Query::ReplTempView { query: body, .. } => query_has_meta_ize(body),
        // WithCfes consumed before resolution, WithErContext consumed by resolver
        ast_resolved::Query::WithCfes { .. } | ast_resolved::Query::WithErContext { .. } => {
            unreachable!("WithCfes/WithErContext consumed before meta-ize check")
        }
    }
}

/// Generate SQL string from a single addressed relational expression
fn generate_sql_v3_only(ast_addressed: ast_addressed::RelationalExpression) -> Result<String> {
    let ctx = transformer_v4::TransformCtx {
        cfes: vec![],
        names: transformer_v4::builder::NameGenerator::new(),
        outer_columns: vec![],
        danger_gates: danger_gates::DangerGateMap::with_defaults(),
    };
    let query = ast_addressed::Query::Relational(ast_addressed);
    let sql_ast_v3 = transformer_v4::transform(query, &ctx)?;
    let rewritten = sql_rewriter::rewrite(sql_ast_v3, generator_v3::SqlDialect::SQLite)?;
    let optimized_sql_ast_v3 =
        sql_optimizer::optimize(rewritten, sql_optimizer::level_from_env())?;
    let generator = generator_v3::SqlGenerator::new();
    generator
        .generate_statement(&optimized_sql_ast_v3)
        .map_err(|e| e.into_delightql_error("SQL generation error"))
}

/// Generate SQL string with CTE support using v3 pipeline
fn generate_sql_with_ctes(
    ctes: Vec<ast_resolved::CteBinding>,
    main_query: ast_resolved::RelationalExpression,
) -> Result<String> {
    use crate::pipeline::sql_ast_v3::{Cte, SqlStatement};

    // Step 1: Refine each CTE expression
    let mut addressed_ctes = Vec::new();
    for cte in ctes {
        let refined_expr = refiner::refine(cte.expression)?;
        let addressed_expr: ast_addressed::RelationalExpression = refined_expr.into();
        addressed_ctes.push((cte.name, addressed_expr));
    }

    // Step 2: Refine main query
    let refined_main = refiner::refine(main_query)?;
    let addressed_main: ast_addressed::RelationalExpression = refined_main.into();

    // Step 3: Transform each CTE to SQL AST
    let ctx = transformer_v4::TransformCtx {
        cfes: vec![],
        names: transformer_v4::builder::NameGenerator::new(),
        outer_columns: vec![],
        danger_gates: danger_gates::DangerGateMap::with_defaults(),
    };
    let mut sql_ctes = Vec::new();
    for (name, expr) in addressed_ctes {
        let cte_stmt = transformer_v4::transform(ast_addressed::Query::Relational(expr), &ctx)?;
        let cte_query_expr = match cte_stmt {
            SqlStatement::Query { query, .. } => query,
            _ => unreachable!("CTE body cannot be DML"),
        };
        sql_ctes.push(Cte::new(name, cte_query_expr));
    }

    // Step 4: Transform main query to SQL AST
    let main_stmt =
        transformer_v4::transform(ast_addressed::Query::Relational(addressed_main), &ctx)?;
    let main_query = match main_stmt {
        SqlStatement::Query { query, .. } => query,
        _ => unreachable!("main query in generate_sql_with_ctes cannot be DML"),
    };

    // Step 5: Create SQL statement with CTEs
    let with_clause = if sql_ctes.is_empty() {
        None
    } else {
        Some(sql_ctes)
    };
    let statement = SqlStatement::with_ctes(with_clause, main_query);

    // Step 6: Rewrite, optimize, and generate
    let rewritten = sql_rewriter::rewrite(statement, generator_v3::SqlDialect::SQLite)?;
    let optimized = sql_optimizer::optimize(rewritten, sql_optimizer::level_from_env())?;
    let generator = generator_v3::SqlGenerator::new();
    generator
        .generate_statement(&optimized)
        .map_err(|e| e.into_delightql_error("CTE SQL generation error"))
}

/// Compile DelightQL source text to SQL string (with CTE support)
///
/// Runs the complete pipeline with CTE support:
/// Text → CST → Query → AST(resolved) → AST(refined) → SQL AST → SQL String
///
/// This is the main entry point for compiling DelightQL queries with full CTE support.
pub(crate) fn compile_source_to_sql(
    source: &str,
    schema: &dyn resolver::DatabaseSchema,
) -> Result<String> {
    // Phase 0: Text → CST
    let tree = parser::parse(source)?;

    // Phase 1: CST → Query (supports CTEs)
    let (query, _features, _assertions, _emits, _dangers, _options, _ddl_blocks) =
        builder_v2::parse_query(&tree, source)?;

    // Phase 2: Query → AST(resolved) (with CTE support, no namespace resolution)
    let resolved_result =
        resolver::resolve_query(query, schema, None, &resolver::ResolutionConfig::default())?;
    // Note: connection_id from resolved_result is ignored here since this is a standalone compile function

    // Phase 3: Query(resolved) → Query(refined) - handle CTEs properly
    match resolved_result.query {
        ast_resolved::Query::Relational(expr) => {
            // Simple query - refine, address, and generate SQL directly
            let refined_expr = refiner::refine(expr)?;
            let addressed_expr: ast_addressed::RelationalExpression = refined_expr.into();
            generate_sql_v3_only(addressed_expr)
        }
        ast_resolved::Query::WithCtes {
            ctes,
            query: main_query,
        } => {
            // CTE query - implement proper CTE support
            generate_sql_with_ctes(ctes, main_query)
        }
        ast_resolved::Query::WithCfes { .. } | ast_resolved::Query::WithPrecompiledCfes { .. } => {
            Err(crate::error::DelightQLError::ParseError {
                message: "CFE queries not yet implemented".to_string(),
                source: None,
                subcategory: None,
            })
        }
        ast_resolved::Query::ReplTempTable { .. } | ast_resolved::Query::ReplTempView { .. } => {
            // REPL commands should use compile_resolved_query_to_sql instead
            Err(crate::error::DelightQLError::ParseError {
                message: "REPL commands should use compile_resolved_query_to_sql function"
                    .to_string(),
                source: None,
                subcategory: None,
            })
        }
        ast_resolved::Query::WithErContext { .. } => {
            unreachable!("ER-context consumed by resolver")
        }
    }
}
