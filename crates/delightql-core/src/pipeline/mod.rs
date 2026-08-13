// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The compilation pipeline: text to SQL, one typed stage at a time.
//!
//! Each stage takes the previous stage's type and returns its own, so a value
//! carries which stage produced it and nothing downstream can consume a stage
//! that has not run.

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
// - Every compilation stage (parse, normalize, resolver, refiner, etc.)
// - Every AST contract (asts::unresolved, asts::resolved, sql_ast, etc.)
// ============================================================================
#![deny(unreachable_patterns)] // Works with cargo build
#![deny(clippy::wildcard_enum_match_arm)] // Requires cargo clippy
#![deny(clippy::match_wildcard_for_single_variants)] // Requires cargo clippy

pub mod asts;
pub mod normalize; // Phase 1: typed CST → AST(unresolved)
pub mod parse; // Phase 0: Text → typed CST
pub mod query_features; // Query feature detection
pub mod syntax; // The typed-CST boundary, re-exported under one internal name

// The phase ASTs under their short internal names.
pub use asts::refined as ast_refined;
pub use asts::resolved as ast_resolved;
pub use asts::unresolved as ast_unresolved;

// A comment here TRAILS the module it describes, in the order rustfmt
// already sorts these declarations into. A leading comment does not survive:
// the formatter reorders a run of `mod` declarations and leaves comments
// where they stood, so one written above a module lands above whichever
// module sorted into that line.
pub mod ast_transform; // Unified AST walk infrastructure
pub mod ast_visit; // Non-consuming whole-tree inspection/collection sibling of ast_transform
pub mod compiled_query; // Compiled query output bundle (primary SQL + assertions + emits)
pub mod effect_executor; // Phase 1.X: Execute pseudo-predicates and rewrite AST
pub mod refiner; // Phase 3: AST(resolved) → AST(refined)
pub mod resolver; // Phase 2: AST(unresolved) → AST(resolved)
pub mod sql_ast; // CONTRACT for Phase 4 (proper SQL syntax tree with builders - PRODUCTION)
pub mod sql_optimizer;
pub mod sql_rewriter;
pub mod sql_self_check; // Post-lowering name-binding verification (the transpiler's L1)

pub mod danger_gates; // Danger gate system (named safety boundaries, OFF by default)
pub mod dialect_pack; // Per-compile image of the dialect_* targeting tables
pub mod transformer; // Phase 4: AST → SQL AST (PRODUCTION)

// The effect transformer: consulted effect bodies → CompiledPlan.
// dead_code allowed outside tests: its entry points get their production
// callers with run!/run_namespace!; the module's own test suite
// exercises everything today and still surfaces real dead code.
#[cfg_attr(not(test), allow(dead_code))]
pub mod effect_transformer;

pub mod generator; // Phase 5: SQL AST v3 → SQL String (PRODUCTION)
pub mod option_map; // Option map system (strategy/preference selection)
pub mod pattern; // Pattern matching utilities for column selection

// Per-function recursion depth tracking
#[cfg(feature = "recursion_stats")]
pub mod recursion_stats;

pub mod inline_ddl; // Registration of typed inline (~~ddl ~~) blocks
pub mod verdict; // Verdict types for assertion and error hook outcomes

// Re-export key types and functions

use crate::error::{DelightQLError, Result};
use crate::lispy::ToLispy;
use crate::names::Registry;
use crate::probe;
use crate::probe::{probe, probing};
use crate::sexp_formatter;
use crate::system::DelightQLSystem;
use std::rc::Rc;
use syntax::SyntaxTree;

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

    // One identity arena for this compilation. Nested pipeline work shares
    // this allocation; a separate top-level compilation constructs another.
    registry: Rc<Registry>,

    // The reconstruction memo's scope. A stored definition asked for five
    // times during one compilation is read once, and the memo dies with the
    // pipeline that opened it — so nothing a compilation reconstructed can be
    // handed to the next.
    _reconstruction: crate::ddl::reconstruct::Compilation,

    // Source and configuration - PRIVATE
    query_text: String,
    resolution_config: resolver::ResolutionConfig,
    sql_optimization_level: sql_optimizer::OptimizationLevel,
    dialect_override: Option<generator::SqlDialect>,

    // Pipeline stages (cached after execution) - PRIVATE
    cst: Option<SyntaxTree>,
    query_unresolved: Option<ast_unresolved::Query>,
    query_resolved: Option<ast_resolved::Query>,
    ast_refined: Option<ast_refined::Chain>,
    sql_ast: Option<sql_ast::SqlStatement>,
    sql_string: Option<String>,
    sql_kind: compiled_query::SqlKind,

    // Data assertions (compiled from inline (~~assert ... ~~) hooks)
    assertion_specs: Vec<ast_unresolved::AssertionSpec>,
    assertion_sqls: Vec<compiled_query::CompiledAssertion>,
    /// Reads the primary statement may not run without, rendered.
    obligations: Vec<compiled_query::CompiledObligation>,
    /// The same, before the lowering sandwich and the generator.
    lowered_obligations: Vec<transformer::Obligation>,
    /// Statements that stage what the primary statement reads, and the
    /// statements that retire them again.
    prepare_sqls: Vec<String>,
    cleanup_sqls: Vec<String>,
    lowered_prepare: Vec<sql_ast::SqlStatement>,
    staged_scopes: Vec<crate::names::ScopeId>,

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

    // Connection routing - which connection should execute this query
    connection_id: Option<i64>,
}

struct PendingAssertionSql {
    statement: sql_ast::SqlStatement,
    right: Option<sql_ast::SqlStatement>,
    name: Option<String>,
}

impl<'a> Pipeline<'a> {
    /// Create a new pipeline for the given source text
    pub fn new(source: &str, system: &'a mut DelightQLSystem) -> Self {
        Self::new_with_config(
            source,
            system,
            resolver::ResolutionConfig::default(),
            sql_optimizer::OptimizationLevel::Basic,
        )
    }

    /// Create a pipeline from a NORMALIZED goal, skipping parse.
    ///
    /// The relay's entrance. One normalization answers the whole submission,
    /// and each goal arrives here with the sidecars its own text declared —
    /// there is no second reading of the query's bytes, so nothing can
    /// disagree with the first.
    pub(crate) fn from_goal(
        goal: normalize::Goal,
        source: &str,
        system: &'a mut DelightQLSystem,
        resolution_config: resolver::ResolutionConfig,
        sql_optimization_level: sql_optimizer::OptimizationLevel,
        registry: Rc<Registry>,
    ) -> Self {
        let mut pipeline = Self::new_with_config_and_registry(
            source,
            system,
            resolution_config,
            sql_optimization_level,
            registry,
        );
        pipeline.query_unresolved = Some(goal.query);
        pipeline.assertion_specs = goal.declared.assertions;
        pipeline.danger_specs = goal.declared.dangers;
        pipeline.option_specs = goal.declared.options;
        pipeline.ddl_blocks = goal.declared.ddl_blocks;
        pipeline
    }

    /// Create a pipeline from a pre-built unresolved query, skipping parse.
    ///
    /// Used by the effect executor to compile pipe sources through the full
    /// pipeline when the source isn't a bare anonymous table.
    pub fn new_from_unresolved_query(
        query: ast_unresolved::Query,
        system: &'a mut DelightQLSystem,
        registry: Rc<Registry>,
    ) -> Self {
        let mut pipeline = Self::new_with_config_and_registry(
            "<injected>",
            system,
            resolver::ResolutionConfig::default(),
            sql_optimizer::OptimizationLevel::Basic,
            registry,
        );
        pipeline.query_unresolved = Some(query);
        pipeline
    }

    /// Create a new pipeline with custom configuration
    pub fn new_with_config(
        source: &str,
        system: &'a mut DelightQLSystem,
        resolution_config: resolver::ResolutionConfig,
        sql_optimization_level: sql_optimizer::OptimizationLevel,
    ) -> Self {
        Self::new_with_config_and_registry(
            source,
            system,
            resolution_config,
            sql_optimization_level,
            Rc::new(Registry::new(&[])),
        )
    }

    pub(crate) fn new_with_config_and_registry(
        source: &str,
        system: &'a mut DelightQLSystem,
        resolution_config: resolver::ResolutionConfig,
        sql_optimization_level: sql_optimizer::OptimizationLevel,
        registry: Rc<Registry>,
    ) -> Self {
        // COMPILATION ENTRY. The registry armed both budgets where its arena
        // was minted — shared with whatever compilation was EXECUTING then,
        // or from policy if none was — and what is published is exactly that,
        // never a re-read of policy a host may have moved since. Nothing below
        // consults policy: the parse measures against
        // `registry.limits().nesting()`, work too deep to be handed anything
        // reads it off the extent each execution enters, and the refiner never
        // asks SQLite while it walks.
        system.publish_compiler_limits(registry.limits());
        Self {
            system,
            registry,
            _reconstruction: crate::ddl::reconstruct::Compilation::open(),
            query_text: source.to_string(),
            resolution_config,
            sql_optimization_level,
            // Explicit override only (--dialect / DQL_DIALECT); without it
            // the dialect derives from the routed connection at compile
            // time (effective_dialect).
            dialect_override: generator::SqlDialect::override_from_env(),
            cst: None,
            query_unresolved: None,
            query_resolved: None,
            ast_refined: None,
            sql_ast: None,
            sql_string: None,
            sql_kind: compiled_query::SqlKind::Query,
            assertion_specs: Vec::new(),
            assertion_sqls: Vec::new(),
            obligations: Vec::new(),
            lowered_obligations: Vec::new(),
            prepare_sqls: Vec::new(),
            cleanup_sqls: Vec::new(),
            lowered_prepare: Vec::new(),
            staged_scopes: Vec::new(),
            danger_specs: Vec::new(),
            cli_danger_overrides: Vec::new(),
            option_specs: Vec::new(),
            cli_option_overrides: Vec::new(),
            ddl_blocks: Vec::new(),
            connection_id: None, // Will be set during resolution
        }
    }

    /// Get reference to the unresolved query if available
    pub fn query_unresolved(&self) -> Option<&ast_unresolved::Query> {
        self.query_unresolved.as_ref()
    }

    /// Whether the source carries inline `(~~ddl ~~)` blocks. Processing
    /// them registers namespaces/entities, so a pure inspection surface
    /// (compile purity) must refuse before that happens.
    pub(crate) fn has_inline_ddl_blocks(&self) -> bool {
        !self.ddl_blocks.is_empty()
    }

    /// Get reference to the resolved query if available
    pub fn query_resolved(&self) -> Option<&ast_resolved::Query> {
        self.query_resolved.as_ref()
    }

    /// Get reference to the refined AST if available
    pub fn ast_refined(&self) -> Option<&ast_refined::Chain> {
        self.ast_refined.as_ref()
    }

    /// Get reference to the SQL AST if available
    pub fn sql_ast(&self) -> Option<&sql_ast::SqlStatement> {
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
                        spec.uri,
                        spec.uri.trim_start_matches(danger_gates::DANGER_URI_SCHEME)
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
        let _running = self.running();
        self.execute_to_sql()?;
        let _ = self.determine_connection_id();
        Ok(compiled_query::CompiledQuery {
            primary_sql: self.sql_string.clone().unwrap_or_default(),
            _kind: self.sql_kind,
            assertion_sqls: self.assertion_sqls.clone(),
            obligations: self.obligations.clone(),
            prepare_sqls: self.prepare_sqls.clone(),
            cleanup_sqls: self.cleanup_sqls.clone(),
            connection_id: self.connection_id,
        })
    }

    /// Compile to the generalized plan form used by the effect algebra.
    ///
    /// A plain query yields the degenerate plan: its assertions, emits, and
    /// primary statement as ordered entries in the relay's execution order
    /// (see `From<CompiledQuery> for CompiledPlan`; order pinned by
    /// `compiled_query::tests::degenerate_entry_order_mirrors_relay`).
    /// Multi-entry plans arrive with the effect transformer.
    #[allow(dead_code)] // effect-algebra entry point; degenerate mapping pinned by compiled_query tests
    pub fn compile_plan(&mut self) -> Result<compiled_query::CompiledPlan> {
        let _running = self.running();
        Ok(self.compile()?.into())
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
                Ok(sexp_formatter::custom_pretty_print(
                    &tree.raw().root_node().to_sexp(),
                ))
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
                    let query_refined = refiner::refine_query(
                        query_resolved.clone(),
                        Rc::clone(&self.registry),
                    )?;
                    Ok(sexp_formatter::custom_pretty_print(&query_refined.to_lispy()))
                }
            }
            "ast-sql" => {
                self.execute_to_sql_ast()?;
                let sql_ast = self.sql_ast().unwrap();
                let names = generator::baptise_statements(&self.registry, &[sql_ast])
                    .map_err(|e| e.into_delightql_error("SQL AST naming error"))?;
                let generator = generator::SqlGenerator::new(&names);
                generator
                    .generate_statement(sql_ast)
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

    /// The extent of this pipeline's EXECUTION.
    ///
    /// Opened by every method that runs compiler work and closed when it
    /// returns, so what answers a parse reached from too deep to be handed
    /// anything is the compilation whose work it is — not whichever pipeline
    /// object happens to still be alive beside it.
    fn running(&self) -> crate::compiler_limits::Running {
        crate::compiler_limits::Running::under(self.registry.limits_shared())
    }

    /// Execute pipeline to CST (parse only)
    ///
    /// One submission, at the entrance it names. Unmarked text takes the
    /// prompt wrap — the host prepends `?-` so the parser receives canonical
    /// text, which keeps interactive convenience outside the grammar.
    pub fn execute_to_cst(&mut self) -> Result<&SyntaxTree> {
        let _running = self.running();
        if self.cst.is_some() {
            return Ok(self.cst.as_ref().unwrap());
        }

        let tree =
            parse::submission(&self.query_text, self.registry.limits().nesting()).map_err(|e| {
                self.record_delightql_error(&e);
                e
            })?;

        self.cst = Some(tree);
        Ok(self.cst.as_ref().unwrap())
    }

    /// Execute pipeline to CST for output (includes ERROR nodes for display)
    ///
    /// Showing a bad parse is this entry's whole point; the nesting budget
    /// still applies, because rendering the tree walks it recursively.
    pub fn execute_to_cst_for_output(&mut self) -> Result<&SyntaxTree> {
        let _running = self.running();
        if self.cst.is_some() {
            return Ok(self.cst.as_ref().unwrap());
        }

        let tree =
            parse::submission_showing_defects(&self.query_text, self.registry.limits().nesting())
                .map_err(|e| {
                self.record_delightql_error(&e);
                e
            })?;

        self.cst = Some(tree);
        Ok(self.cst.as_ref().unwrap())
    }

    /// Execute pipeline to unresolved Query
    pub fn execute_to_query_unresolved(&mut self) -> Result<&ast_unresolved::Query> {
        let _running = self.running();
        if self.query_unresolved.is_some() {
            return Ok(self.query_unresolved.as_ref().unwrap());
        }

        self.execute_to_cst()?;
        let tree = self.cst.as_ref().unwrap();
        let normalized = normalize::submission(tree, Rc::clone(&self.registry));
        let normalized = normalized.map_err(|e| {
            self.record_delightql_error(&e);
            e
        })?;

        let goal = one_goal(normalized).map_err(|e| {
            self.record_delightql_error(&e);
            e
        })?;

        self.query_unresolved = Some(goal.query);
        self.assertion_specs = goal.declared.assertions;
        self.danger_specs = goal.declared.dangers;
        self.option_specs = goal.declared.options;
        self.ddl_blocks = goal.declared.ddl_blocks;
        Ok(self.query_unresolved.as_ref().unwrap())
    }

    /// Execute pipeline to resolved Query (Phase 2: uses injected schema)
    ///
    /// Gets the database schema from the system (injected at construction) rather
    /// than taking it as a parameter, maintaining clean architecture.
    pub fn execute_to_query_resolved(&mut self) -> Result<&ast_resolved::Query> {
        let _running = self.running();
        if self.query_resolved.is_some() {
            return Ok(self.query_resolved.as_ref().unwrap());
        }

        // First get unresolved query
        self.execute_to_query_unresolved()?;
        let query_unresolved = self.query_unresolved.as_ref().unwrap();

        // Process inline DDL blocks before effects and resolution.
        for ddl in std::mem::take(&mut self.ddl_blocks) {
            // Scratch routes to `home`. Named blocks become
            // ordinary lib-kind children `home::<name>`; the unnamed block lands
            // its entities DIRECTLY in `home` — bare because home is enlisted, not
            // because of any special rule for unnamed blocks.
            let namespace = match ddl.namespace.as_deref() {
                Some(suffix) => {
                    // System name guard (catechism Deviation #3): the
                    // user-typed scratch name is a creation target under
                    // home. Guard it as `home::<suffix>` so prong (c) (the
                    // `_` reservation) fires while prong (b) relaxes —
                    // `home::sysinfo` legal, `home::_x` refused.
                    let fq = format!("home::{}", suffix);
                    crate::system::validate_user_namespace_target(&fq)?;
                    fq
                }
                None => "home".to_string(),
            };
            inline_ddl::register_inline_ddl_block(&ddl.body, &namespace, self.system).map_err(
                |e| {
                    crate::error::DelightQLError::database_error(
                        format!("Inline DDL error: {}", e),
                        "inline DDL",
                    )
                },
            )?;
        }

        // Execute pseudo-predicates and rewrite AST
        // This must happen BEFORE resolution because pseudo-predicates
        // might register namespaces needed by CFEs
        let query_after_effects = effect_executor::execute_effects(
            query_unresolved.clone(),
            &mut self.system,
            &self.registry,
        )
        .map_err(|e| {
            self.record_delightql_error(&e);
            e
        })?;

        // Get schema from system (injected by CLI) - NO coupling to backends!
        let schema = self.system.get_schema()?;

        // Resolve (passing system for namespace resolution). Query-scoped
        // definitions ride WithCfes into the resolver, which spends them at
        // their call sites.
        let resolution_result = resolver::resolve_query(
            query_after_effects,
            schema,
            Some(self.system),
            &self.resolution_config,
            Rc::clone(&self.registry),
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
    pub fn execute_to_ast_refined(&mut self) -> Result<Option<&ast_refined::Chain>> {
        let _running = self.running();
        if self.ast_refined.is_some() {
            return Ok(self.ast_refined.as_ref().map(|r| r));
        }

        // First get resolved query (schema is now obtained internally)
        self.execute_to_query_resolved()?;
        let query_resolved = self.query_resolved.as_ref().unwrap();

        // Refine (only works for bare bodies — this inspection stage
        // predates CTE support and still presents one chain)
        if !query_resolved.ctes.is_empty() {
            panic!(
                "catch-all hit in mod.rs execute_to_query_refined: unexpected resolved \
                 Query bindings"
            );
        }
        let refined =
            refiner::refine(query_resolved.body.clone(), Rc::clone(&self.registry)).map_err(
                |e| {
                    self.record_delightql_error(&e);
                    e
                },
            )?;
        self.ast_refined = Some(refined);
        Ok(self.ast_refined.as_ref().map(|r| r))
    }

    /// Execute pipeline to SQL AST (Phase 2: uses injected schema)
    pub fn execute_to_sql_ast(&mut self) -> Result<&sql_ast::SqlStatement> {
        let _running = self.running();
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
            Rc::clone(&self.registry),
        )
        .map_err(|e| {
            self.record_delightql_error(&e);
            e
        })?;

        // Build option map from per-query overrides
        let mut options = option_map::OptionMap::with_defaults();
        options.apply_overrides(&self.cli_option_overrides); // Session baseline (CLI --option)
        options.apply_overrides(&self.option_specs); // Per-query inline overrides

        let ctx = transformer::TransformCtx {
            identities: Rc::clone(&self.registry),
            names: transformer::builder::NameGenerator::new(Rc::clone(&self.registry)),
            outer_columns: vec![],
            danger_gates: query_danger_gates.clone(),
        };
        let lowered = transformer::transform(refined_query, &ctx).map_err(|e| {
            self.record_delightql_error(&e);
            e
        })?;
        self.lowered_obligations = lowered.obligations;
        self.lowered_prepare = lowered.prepare;
        self.staged_scopes = lowered.staged;
        let sql_ast = lowered.statement;

        if log::log_enabled!(log::Level::Debug) {
            if let Ok(names) = generator::baptise_statements(&self.registry, &[&sql_ast]) {
                let gen = generator::SqlGenerator::new(&names);
                if let Ok(sql_preview) = gen.generate_statement(&sql_ast) {
                    log::debug!("execute_to_sql_ast: sql_preview={sql_preview}");
                }
            }
        }

        self.sql_ast = Some(sql_ast);
        Ok(self.sql_ast.as_ref().unwrap())
    }

    /// Resolve the dialect pack for this compile: one read of the
    /// `dialect_*` bootstrap tables into an in-memory map, rebuilt at the
    /// start of each query so a mid-session pack change is picked up by
    /// the next compile.
    fn load_dialect_pack(&self) -> Result<std::sync::Arc<dialect_pack::DialectPack>> {
        let conn = self
            .system
            .bootstrap_connection()
            .lock()
            .expect("FATAL: Failed to acquire bootstrap lock for dialect pack");
        let pack = dialect_pack::DialectPack::load(&conn).map_err(|e| {
            crate::error::DelightQLError::database_error(
                format!("Failed to load dialect pack: {}", e),
                e.to_string(),
            )
        })?;
        Ok(std::sync::Arc::new(pack))
    }

    /// The dialect this compile emits: an explicit `--dialect`/`DQL_DIALECT`
    /// override wins; otherwise the dialect of the connection the query
    /// routes to (dialect-from-connection — a mounted or primary
    /// postgres/duckdb connection gets target-spelled SQL automatically).
    fn effective_dialect(&self) -> generator::SqlDialect {
        self.dialect_override
            .unwrap_or_else(|| self.system.dialect_for_connection(self.connection_id))
    }

    /// Execute full pipeline to SQL string (Phase 2: uses injected schema)
    pub fn execute_to_sql(&mut self) -> Result<&str> {
        let _running = self.running();
        if self.sql_string.is_some() {
            return Ok(self.sql_string.as_ref().unwrap());
        }

        // First get SQL AST (schema is now obtained internally)
        self.execute_to_sql_ast()?;
        let sql_ast = self.sql_ast.as_ref().unwrap();

        // Resolve the dialect AFTER resolution: connection routing is known
        // by now, so dialect-from-connection can fire (override wins).
        let dialect = self.effective_dialect();

        // The lowering sandwich: expand → cleanup → legalize (final word).
        let optimized = lower_statement(
            sql_ast.clone(),
            dialect,
            self.sql_optimization_level,
            &self.registry,
        )
        .map_err(|e| {
            self.record_delightql_error(&e);
            e
        })?;

        // Resolve the dialect pack for this compile: one read of the
        // dialect_* tables, shared by every generator this compile
        // constructs (main + assertions + emits).
        let dialect_pack = self.load_dialect_pack()?;

        // The staging statements and the checks, through the same sandwich as
        // the statement itself — and, below, through the same BUNDLE: one
        // name per relation across all of them, or the check and the
        // mutation would read two different tables that were meant to be one.
        let staging: Vec<sql_ast::SqlStatement> = std::mem::take(&mut self.lowered_prepare)
            .into_iter()
            .map(|statement| {
                lower_statement(
                    statement,
                    dialect,
                    self.sql_optimization_level,
                    &self.registry,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let retirement: Vec<sql_ast::SqlStatement> = std::mem::take(&mut self.staged_scopes)
            .into_iter()
            .map(|table| {
                lower_statement(
                    sql_ast::SqlStatement::DropTempTable { table },
                    dialect,
                    self.sql_optimization_level,
                    &self.registry,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let mut refusals = Vec::new();
        let checks: Vec<sql_ast::SqlStatement> = std::mem::take(&mut self.lowered_obligations)
            .into_iter()
            .map(|obligation| {
                refusals.push(obligation.refusal);
                lower_statement(
                    obligation.statement,
                    dialect,
                    self.sql_optimization_level,
                    &self.registry,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        // Determine SQL kind from the AST
        self.sql_kind = match sql_ast {
            sql_ast::SqlStatement::Delete { .. }
            | sql_ast::SqlStatement::Update { .. }
            | sql_ast::SqlStatement::Insert { .. } => compiled_query::SqlKind::Dml,
            sql_ast::SqlStatement::Query { .. }
            | sql_ast::SqlStatement::CreateTempTable { .. }
            | sql_ast::SqlStatement::CreateTempView { .. }
            | sql_ast::SqlStatement::DropTempTable { .. } => compiled_query::SqlKind::Query,
        };

        let mut pending_assertions = Vec::new();

        // Compile assertion bodies to SQL
        if !self.assertion_specs.is_empty() {
            let schema = self.system.get_schema()?;
            let specs = std::mem::take(&mut self.assertion_specs);
            pending_assertions.reserve(specs.len());

            // Rebuild danger/option maps so assertions inherit per-query gates
            let mut assert_danger_gates = danger_gates::DangerGateMap::with_defaults();
            assert_danger_gates.apply_overrides(&self.cli_danger_overrides);
            assert_danger_gates.apply_overrides(&self.danger_specs);
            let mut assert_options = option_map::OptionMap::with_defaults();
            assert_options.apply_overrides(&self.cli_option_overrides);
            assert_options.apply_overrides(&self.option_specs);

            // The main query's bindings, so assertions can reference CTE
            // names and CFE definitions from the outer scope.
            let (outer_ctes, outer_cfes): (
                Vec<ast_unresolved::CteBinding>,
                Vec<ast_unresolved::CfeDefinition>,
            ) = match self.query_unresolved.as_ref() {
                Some(query) => (query.ctes.clone(), query.cfes.clone()),
                None => (vec![], vec![]),
            };

            for spec in &specs {
                let body_demands_directive =
                    !asts::effects::collect_directive_invocations(&spec.body).is_empty();
                let right_demands_directive = spec.right_operand.as_ref().is_some_and(|right| {
                    !asts::effects::collect_directive_invocations(right).is_empty()
                });
                if body_demands_directive || right_demands_directive {
                    let error = DelightQLError::validation_error_categorized(
                        "assertion/directive/not_permitted",
                        "directives are not permitted in assertions",
                        "assertions are read-only checks",
                    );
                    self.record_delightql_error(&error);
                    return Err(error);
                }

                // The assertion body rides the outer bindings, so CTE
                // references and CFE calls resolve exactly as in the main
                // query.
                let assertion_query = ast_unresolved::Query {
                    cfes: outer_cfes.clone(),
                    ctes: outer_ctes.clone(),
                    body: spec.body.clone(),
                };

                // Resolve
                let resolved_result = resolver::resolve_query(
                    assertion_query,
                    schema,
                    Some(self.system),
                    &self.resolution_config,
                    Rc::clone(&self.registry),
                )
                .map_err(|e| {
                    self.record_delightql_error(&e);
                    e
                })?;

                // Refine (assertion uses same danger gates as main query)
                let refined = refiner::refine_query_with_gates(
                    resolved_result.query,
                    assert_danger_gates.clone(),
                    Rc::clone(&self.registry),
                )
                .map_err(|e| {
                    self.record_delightql_error(&e);
                    e
                })?;
                // Transform to SQL AST
                let assert_ctx = transformer::TransformCtx {
                    identities: Rc::clone(&self.registry),
                    names: transformer::builder::NameGenerator::new(Rc::clone(&self.registry)),
                    outer_columns: vec![],
                    danger_gates: assert_danger_gates.clone(),
                };
                let sql_ast = transformer::transform(refined, &assert_ctx)
                    .and_then(transformer::Lowered::without_obligations)
                    .map_err(|e| {
                        self.record_delightql_error(&e);
                        e
                    })?;

                // The lowering sandwich: expand → cleanup → legalize.
                let optimized = lower_statement(
                    sql_ast,
                    dialect,
                    self.sql_optimization_level,
                    &self.registry,
                )
                .map_err(|e| {
                    self.record_delightql_error(&e);
                    e
                })?;

                // `equals` compiles its right operand through the same
                // pipeline, under the outer CTEs so a CTE name written on
                // the right still resolves.
                let right = match spec.right_operand.as_ref() {
                    None => None,
                    Some(right_rel) => {
                        let right_query = ast_unresolved::Query {
                            cfes: outer_cfes.clone(),
                            ctes: outer_ctes.clone(),
                            body: right_rel.clone(),
                        };
                        let right_resolved_result = resolver::resolve_query(
                            right_query,
                            schema,
                            Some(self.system),
                            &self.resolution_config,
                            Rc::clone(&self.registry),
                        )?;
                        let right_refined = refiner::refine_query_with_gates(
                            right_resolved_result.query,
                            assert_danger_gates.clone(),
                            Rc::clone(&self.registry),
                        )?;
                        let right_ctx = transformer::TransformCtx {
                            identities: Rc::clone(&self.registry),
                            names: transformer::builder::NameGenerator::new(Rc::clone(
                                &self.registry,
                            )),
                            outer_columns: vec![],
                            danger_gates: danger_gates::DangerGateMap::with_defaults(),
                        };
                        let right_sql_ast = transformer::transform(right_refined, &right_ctx)?
                            .without_obligations()?;
                        let right_optimized = lower_statement(
                            right_sql_ast,
                            dialect,
                            self.sql_optimization_level,
                            &self.registry,
                        )?;
                        Some(right_optimized)
                    }
                };

                pending_assertions.push(PendingAssertionSql {
                    statement: optimized,
                    right,
                    name: spec.name.clone(),
                });
            }

            self.assertion_specs = specs;
        }

        let mut statements = Vec::with_capacity(
            1 + pending_assertions.len()
                + pending_assertions
                    .iter()
                    .filter(|assertion| assertion.right.is_some())
                    .count(),
        );
        statements.push(&optimized);
        statements.extend(staging.iter());
        statements.extend(retirement.iter());
        statements.extend(checks.iter());
        for assertion in &pending_assertions {
            statements.push(&assertion.statement);
            if let Some(right) = &assertion.right {
                statements.push(right);
            }
        }
        let names = generator::baptise_statements(&self.registry, &statements)
            .map_err(|e| e.into_delightql_error("SQL bundle naming error"))?;
        let generator = generator::SqlGenerator::new(&names)
            .with_dialect(dialect)
            .with_bin_registry(self.system.bin_registry())
            .with_dialect_pack(dialect_pack);
        self.sql_string = Some(
            generator
                .generate_statement(&optimized)
                .map_err(|e| e.into_delightql_error("SQL generation error"))?,
        );
        self.prepare_sqls = staging
            .iter()
            .map(|statement| {
                generator
                    .generate_statement(statement)
                    .map_err(|e| e.into_delightql_error("staging SQL generation error"))
            })
            .collect::<Result<Vec<_>>>()?;
        self.cleanup_sqls = retirement
            .iter()
            .map(|statement| {
                generator
                    .generate_statement(statement)
                    .map_err(|e| e.into_delightql_error("retirement SQL generation error"))
            })
            .collect::<Result<Vec<_>>>()?;
        self.obligations = checks
            .iter()
            .zip(refusals)
            .map(|(statement, refusal)| {
                Ok(compiled_query::CompiledObligation {
                    sql: generator
                        .generate_statement(statement)
                        .map_err(|e| e.into_delightql_error("obligation SQL generation error"))?,
                    refusal,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.assertion_sqls = pending_assertions
            .into_iter()
            .map(|assertion| {
                let left = generator
                    .generate_statement(&assertion.statement)
                    .map_err(|e| e.into_delightql_error("Assertion SQL generation error"))?;
                let right = assertion
                    .right
                    .as_ref()
                    .map(|right| {
                        generator.generate_statement(right).map_err(|e| {
                            e.into_delightql_error("Equals right SQL generation error")
                        })
                    })
                    .transpose()?;
                Ok(compiled_query::CompiledAssertion {
                    sql: assertion_bool_wrap(&left, right.as_deref()),
                    name: assertion.name,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(self.sql_string.as_ref().unwrap())
    }
}

/// The standalone compiler helper answers to the compilation that called it.
///
/// `compile_source_to_sql` mints its own arena, which is exactly the shape
/// that used to re-read policy: mint, then parse through an entrance that
/// asked the process again. Inside a running compilation the two reads can
/// straddle a host's setting change, and the body would then be judged by a
/// depth its caller never armed and the catalog never reported.
///
/// Every pin reads the budget off a REFUSAL, on a ladder past both budgets
/// under test. Nothing here walks a tree that deep: a test thread's stack is
/// a fraction of the main one's, and a walk near the ceiling aborts the
/// process rather than failing a test.
#[cfg(test)]
mod standalone_helper_depth_tests {
    use crate::compiler_limits::{ArmedLimits, ProcessLimitLease, Running, NESTING};
    use delightql_types::schema::{ColumnInfo, DatabaseSchema};

    struct OneTable;

    impl DatabaseSchema for OneTable {
        fn get_table_columns(
            &self,
            _schema: Option<&str>,
            table: &str,
        ) -> delightql_types::Result<Option<Vec<ColumnInfo>>> {
            if table != "users" {
                return Ok(None);
            }
            Ok(Some(vec![ColumnInfo {
                name: "age".into(),
                nullable: true,
                position: 0,
                declared_type: Some("INTEGER".to_string()),
            }]))
        }

        fn table_exists(
            &self,
            _schema: Option<&str>,
            table: &str,
        ) -> delightql_types::Result<bool> {
            Ok(table == "users")
        }
    }

    const DEEP: usize = 1090;
    const LOWER: usize = 700;
    const HIGHER: usize = 1000;

    fn ladder() -> String {
        format!(
            "users(*) |> ({}age{} as v)",
            "(".repeat(DEEP),
            ")".repeat(DEEP)
        )
    }

    fn refused_budget(error: crate::error::DelightQLError) -> String {
        assert!(
            error.error_uri().contains("operational/resource/nesting"),
            "expected the depth refusal, got {}",
            error.error_uri()
        );
        error.to_string()
    }

    #[test]
    fn the_helper_is_judged_by_the_running_compilation_not_later_policy() {
        let _lease = ProcessLimitLease::take();
        NESTING.set(LOWER);
        let _running = Running::under(std::rc::Rc::new(ArmedLimits::from_policy()));
        NESTING.set(HIGHER);

        let refused = refused_budget(
            super::compile_source_to_sql(&ladder(), &OneTable).expect_err("past every budget"),
        );
        assert!(
            refused.contains(&LOWER.to_string()),
            "the helper must answer to the compilation that called it: {refused}"
        );
        assert!(
            !refused.contains(&HIGHER.to_string()),
            "and not to the policy that moved under it: {refused}"
        );
    }

    /// The same claim the other way round.
    #[test]
    fn arming_high_and_lowering_policy_still_answers_with_the_armed_value() {
        let _lease = ProcessLimitLease::take();
        NESTING.set(HIGHER);
        let _running = Running::under(std::rc::Rc::new(ArmedLimits::from_policy()));
        NESTING.set(LOWER);

        let refused = refused_budget(
            super::compile_source_to_sql(&ladder(), &OneTable).expect_err("past every budget"),
        );
        assert!(
            refused.contains(&HIGHER.to_string()),
            "the helper kept the depth its caller armed: {refused}"
        );
    }

    /// Standing alone, with no compilation running, it arms at its own door —
    /// and sees policy move between calls, which a compilation may not.
    #[test]
    fn the_helper_arms_at_its_own_door_when_nothing_is_running() {
        let _lease = ProcessLimitLease::take();

        NESTING.set(LOWER);
        let first = refused_budget(
            super::compile_source_to_sql(&ladder(), &OneTable).expect_err("past every budget"),
        );
        assert!(first.contains(&LOWER.to_string()), "{first}");

        NESTING.set(HIGHER);
        let second = refused_budget(
            super::compile_source_to_sql(&ladder(), &OneTable).expect_err("past every budget"),
        );
        assert!(second.contains(&HIGHER.to_string()), "{second}");
    }

    /// The guard is not simply refusing everything.
    #[test]
    fn an_ordinary_body_still_compiles() {
        let _lease = ProcessLimitLease::take();
        NESTING.set(LOWER);
        let _running = Running::under(std::rc::Rc::new(ArmedLimits::from_policy()));
        NESTING.set(HIGHER);

        let sql = super::compile_source_to_sql("users(*) |> (age as v)", &OneTable)
            .expect("a shallow body is afforded");
        assert!(sql.contains("users"), "{sql}");
    }
}

/// Split a query sequence into its individual statement texts.
///
/// One `String` per statement, cut at the boundaries the sequence root draws.
/// A consumer that executes one statement per call sends these.
///
/// A DEFECTIVE submission still divides. The statement that failed is the one
/// that should carry the refusal — with the teaching its own tokens choose and
/// the hook its own text declares — and a splitter that refused the whole
/// submission instead would hand every statement one statement's failure.
/// What recovery could not divide travels as one piece, which is the same
/// answer the ownership rule gives everywhere else.
pub fn split_queries(source: &str) -> Result<Vec<String>> {
    let tree = parse::query_sequence_showing_defects(source)?;
    let extents = parse::statement_extents(&tree);
    if extents.is_empty() {
        return Err(DelightQLError::parse_error("no queries found in source"));
    }
    Ok(extents.into_iter().map(|s| source[s].to_string()).collect())
}

/// The ONE goal a single submission carries, with everything it declared.
///
/// A prompt wrap admits exactly one top-level goal, so a submission with two
/// is a caller that should have used the sequence entrance, and one with none
/// declared nothing to run. Whatever the submission stated OUTSIDE the goal —
/// a definition's own declarations — travels with it: there is one form here,
/// so there is nothing for a file-level sidecar to belong to instead.
pub(crate) fn one_goal(mut normalized: normalize::Normalized) -> Result<normalize::Goal> {
    if normalized.queries.len() > 1 {
        // ONE FACT, ONE TEACHING. A submission holding several queries is
        // refused here when it PARSED as a sequence and at the entrance when
        // it did not; both are the same fact about the same submission, so
        // they carry the same identity and say the same thing.
        return Err(DelightQLError::ParseError {
            message: format!(
                "multi-query input rejected: found {} queries in one submission \
                 (send each query separately, or run the file through the \
                 sequential entrance)",
                normalized.queries.len()
            ),
            source: None,
            subcategory: Some("multi_query"),
        });
    }
    let mut goal = normalized
        .queries
        .pop()
        .ok_or_else(|| DelightQLError::parse_error("this submission declares nothing to run"))?;
    let file_level = std::mem::take(&mut normalized.declared);
    goal.declared.assertions.extend(file_level.assertions);
    goal.declared.dangers.extend(file_level.dangers);
    goal.declared.options.extend(file_level.options);
    goal.declared.ddl_blocks.extend(file_level.ddl_blocks);
    if goal.declared.expected_error.is_none() {
        goal.declared.expected_error = file_level.expected_error;
    }
    Ok(goal)
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
    // Rides the chain's own `source_spine`: a MetaIze buried in a join arm
    // the real table's connection, so the spine STOPS at composites (returns
    // false) — byte-equivalent to the old `Pipe→(op?/source), Filter→source,
    // still needs the outer wrapper.
    fn expr_has_meta_ize(expr: &ast_resolved::Chain) -> bool {
        use crate::pipeline::asts::core::expressions::chain::SpineStep;
        expr.source_spine().any(|step| {
            matches!(
                step,
                SpineStep::Structural(crate::pipeline::asts::core::StructuralForm::Meta)
            )
        })
    }

    expr_has_meta_ize(&query.body)
}

/// The lowering sandwich: dialect
/// expansions, then optional cleanup, then the mandatory legalization
/// word. Legalization runs LAST — nothing rewrites the tree after it, so
/// "never illegal SQL" holds by construction. Every path from SQL AST to
/// the generator must go through here.
fn lower_statement(
    statement: sql_ast::SqlStatement,
    dialect: generator::SqlDialect,
    level: sql_optimizer::OptimizationLevel,
    identities: &Registry,
) -> Result<sql_ast::SqlStatement> {
    let expanded = sql_rewriter::rewrite(statement, dialect, identities)?;
    let cleaned = if probe::enabled("noopt") {
        expanded
    } else {
        sql_optimizer::optimize(expanded, level)?
    };
    let legal = sql_rewriter::legalize(cleaned, dialect)?;
    probe!(ast, "{legal:#?}");
    probing!(sql, {
        probe!(sql, "{}", rendered_sql(&legal, dialect, identities));
    });
    match sql_self_check::check(&legal, identities) {
        Ok(()) => {}
        // Reading the SQL behind a refusal is the only way to tell a real
        // dangling reference from one whose spelling happens to resolve; the
        // check has to be let past for the engine to answer that.
        Err(error) if probe::enabled("nocheck") => probe!(nocheck, "{error}"),
        Err(error) => return Err(error),
    }
    Ok(legal)
}

fn rendered_sql(
    statement: &sql_ast::SqlStatement,
    dialect: generator::SqlDialect,
    identities: &Registry,
) -> String {
    let Ok(baptised) = generator::baptise_statements(identities, &[statement]) else {
        return "<unbaptisable>".to_string();
    };
    generator::SqlGenerator::new(&baptised)
        .with_dialect(dialect)
        .generate_statement(statement)
        .unwrap_or_else(|error| format!("<ungenerable: {error:?}>"))
}

/// Generate SQL string from a single refined relational expression
fn generate_sql_v3_only(ast_refined: ast_refined::Chain, registry: Rc<Registry>) -> Result<String> {
    let ctx = transformer::TransformCtx {
        identities: Rc::clone(&registry),
        names: transformer::builder::NameGenerator::new(Rc::clone(&registry)),
        outer_columns: vec![],
        danger_gates: danger_gates::DangerGateMap::with_defaults(),
    };
    let query = ast_refined::Query::relational(ast_refined);
    let sql_ast = transformer::transform(query, &ctx)?.without_obligations()?;
    let optimized_sql_ast = lower_statement(
        sql_ast,
        generator::SqlDialect::SQLite,
        sql_optimizer::OptimizationLevel::Basic,
        &registry,
    )?;
    let names = generator::baptise_statements(&registry, &[&optimized_sql_ast])
        .map_err(|e| e.into_delightql_error("SQL naming error"))?;
    let generator = generator::SqlGenerator::new(&names);
    generator
        .generate_statement(&optimized_sql_ast)
        .map_err(|e| e.into_delightql_error("SQL generation error"))
}

/// Generate SQL string with CTE support using v3 pipeline
fn generate_sql_with_ctes(
    ctes: Vec<ast_resolved::CteBinding>,
    main_query: ast_resolved::Chain,
    registry: Rc<Registry>,
) -> Result<String> {
    use crate::pipeline::sql_ast::{Cte, SqlStatement};

    // Step 1: Refine each CTE expression
    let mut refined_ctes = Vec::new();
    for cte in ctes {
        // The resolver's decisions travel with the binding to its SQL
        // constructor: the recursion fact AND the bound scope. Re-deriving
        // the scope from the refined body names the body's own scope, which
        // is not the one references through the binding were addressed
        // against.
        let recursive = cte.recursion.is_recursive();
        let scope = cte.subject;
        let refined_expr = refiner::refine(cte.expression, Rc::clone(&registry))?;
        refined_ctes.push((scope, refined_expr, recursive));
    }

    // Step 2: Refine main query
    let refined_main = refiner::refine(main_query, Rc::clone(&registry))?;

    // Step 3: Transform each CTE to SQL AST
    let ctx = transformer::TransformCtx {
        identities: Rc::clone(&registry),
        names: transformer::builder::NameGenerator::new(Rc::clone(&registry)),
        outer_columns: vec![],
        danger_gates: danger_gates::DangerGateMap::with_defaults(),
    };
    let mut sql_ctes = Vec::new();
    for (scope, expr, recursive) in refined_ctes {
        let cte_stmt = transformer::transform(ast_refined::Query::relational(expr), &ctx)?
            .without_obligations()?;
        let cte_query_expr = match cte_stmt {
            SqlStatement::Query { query, .. } => query,
            _ => unreachable!("CTE body cannot be DML"),
        };
        sql_ctes.push(if recursive {
            Cte::new_recursive(scope, cte_query_expr)
        } else {
            Cte::new(scope, cte_query_expr)
        });
    }

    // Step 4: Transform main query to SQL AST
    let main_stmt = transformer::transform(ast_refined::Query::relational(refined_main), &ctx)?
        .without_obligations()?;
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

    // Step 6: Lower (expand → cleanup → legalize) and generate
    let optimized = lower_statement(
        statement,
        generator::SqlDialect::SQLite,
        sql_optimizer::OptimizationLevel::Basic,
        &registry,
    )?;
    let names = generator::baptise_statements(&registry, &[&optimized])
        .map_err(|e| e.into_delightql_error("CTE SQL naming error"))?;
    let generator = generator::SqlGenerator::new(&names);
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

/// The assertion body → boolean-SQL wrap (one row, one column).
/// Shared vocabulary for the ordinary pipeline's assertion compilation
/// and the typed assertion steps; the two sites compile their operand
/// SQL differently but wrap identically.
///
/// A body that ends in a receipt view has already reduced its
/// proposition to a presence, so testing the body for a row is the whole
/// of it — the negation lives in `notexists`, not here.
///
/// `equals` is the exception, and it is the POSITIONAL pairing that makes
/// it one: `EXCEPT` compares columns by position, which is a different
/// predicate from the by-name relational minus. Bag equality needs the
/// count too — `EXCEPT` is set-valued and cannot tell `{1, 1}` from `{1}`.
pub(crate) fn assertion_bool_wrap(left_sql: &str, right_sql: Option<&str>) -> String {
    let Some(right) = right_sql else {
        return format!("SELECT EXISTS({}) AS bool", left_sql);
    };
    format!(
        "SELECT (\
        (SELECT COUNT(*) FROM ({left})) = (SELECT COUNT(*) FROM ({right})) \
        AND NOT EXISTS(SELECT * FROM ({left}) EXCEPT SELECT * FROM ({right})) \
        AND NOT EXISTS(SELECT * FROM ({right}) EXCEPT SELECT * FROM ({left}))\
        ) AS bool",
        left = left_sql,
        right = right,
    )
}

/// Compile one body to SQL against a supplied schema, on its own arena.
///
/// A SUB-COMPILATION when a compilation is running — a stored view's body, a
/// manifest entry — so it inherits that compilation's nesting budget rather
/// than asking policy again, and holds the extent open so its own interior
/// reads the same number. Standing alone, it arms at its own door.
pub(crate) fn compile_source_to_sql(
    source: &str,
    schema: &dyn resolver::DatabaseSchema,
) -> Result<String> {
    let registry = Rc::new(Registry::new(&[]));
    let _running = crate::compiler_limits::Running::under(registry.limits_shared());

    // Phase 0: Text → typed CST
    let tree = parse::prompt(source)?;

    // Phase 1: typed CST → Query (supports CTEs)
    let query = one_goal(normalize::definition_file(&tree, Rc::clone(&registry))?)?.query;

    // Phase 2: Query → AST(resolved) (with CTE support, no namespace resolution)
    let resolved_result = resolver::resolve_query(
        query,
        schema,
        None,
        &resolver::ResolutionConfig::default(),
        Rc::clone(&registry),
    )?;
    // Note: connection_id from resolved_result is ignored here since this is a standalone compile function

    // Phase 3: Query(resolved) → Query(refined) - handle CTEs properly
    let ast_resolved::Query { cfes: (), ctes, body } = resolved_result.query;
    if ctes.is_empty() {
        // Simple query - refine, address, and generate SQL directly
        let refined_expr = refiner::refine(body, Rc::clone(&registry))?;
        generate_sql_v3_only(refined_expr, Rc::clone(&registry))
    } else {
        generate_sql_with_ctes(ctes, body, Rc::clone(&registry))
    }
}
