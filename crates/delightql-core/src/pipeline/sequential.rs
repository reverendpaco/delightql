//! Sequential compilation of multi-query source strings.
//!
//! `compile_sequential` takes a source string containing multiple queries,
//! splits them at CST boundaries, and compiles each through its own Pipeline.
//! Per-query metadata (assertions, dangers, emits, options) is preserved
//! because each Pipeline runs the full source-text path with its own
//! FeatureCollector.
//!
//! Effects (consult!, engage!) accumulate on the shared DelightQLSystem
//! between queries — query N's side effects are visible to query N+1.

use crate::error::DelightQLError;
use crate::system::DelightQLSystem;

use super::builder_v2;
use super::compiled_query;
use super::parser;
#[cfg(feature = "recursion_stats")]
use super::recursion_stats;
use super::resolver;
use super::sql_optimizer;
use super::verdict;
use super::Pipeline;

use super::asts::unresolved as ast_unresolved;

/// Configuration for sequential compilation.
pub struct SequentialConfig {
    pub resolution_config: resolver::ResolutionConfig,
    pub sql_optimization_level: sql_optimizer::OptimizationLevel,
    pub inline_ctes: bool,
    pub danger_overrides: Vec<ast_unresolved::DangerSpec>,
    pub option_overrides: Vec<ast_unresolved::OptionSpec>,
}

/// Outcome for a single query in sequential compilation.
pub enum SingleQueryOutcome {
    /// Compiled successfully — caller should execute SQL and evaluate assertions.
    Compiled(compiled_query::CompiledQuery),

    /// Error hook produced a verdict (compile-time error matched or mismatched).
    ErrorVerdict(verdict::Verdict),

    /// Runtime error hook — compiled but expects execution failure.
    /// Caller must execute SQL, match error against `expected`, produce verdict.
    PendingRuntimeErrorHook {
        compiled: compiled_query::CompiledQuery,
        expected: builder_v2::ExpectedError,
    },
}

/// Result for one query in a sequential compilation run.
pub struct PerQueryResult {
    /// What happened with this query.
    pub outcome: SingleQueryOutcome,
    /// Zero-based index in the source file.
    pub index: usize,
    /// The source text of this individual query.
    pub _source: String,
}

/// Parse a multi-query source string and compile each query sequentially.
///
/// Effects from earlier queries (consult!, engage!) are visible to later
/// queries because the same `system` is shared across all compilations.
///
/// Returns one `PerQueryResult` per query. On compilation error (without
/// an error hook to catch it), returns `Err` immediately — remaining
/// queries are not compiled.
pub fn compile_sequential(
    source: &str,
    system: &mut DelightQLSystem,
    config: &SequentialConfig,
) -> std::result::Result<Vec<PerQueryResult>, anyhow::Error> {
    let tree = parser::parse(source)?;
    let root = tree.root_node();

    let mut cursor = root.walk();
    let query_spans: Vec<(usize, usize)> = root
        .children(&mut cursor)
        .filter(|c| c.kind() == "query")
        .map(|c| (c.start_byte(), c.end_byte()))
        .collect();

    if query_spans.is_empty() {
        // DDL-only input: no query nodes, but ddl_annotation nodes present.
        // Feed entire source to the builder, which creates a synthetic query
        // and attaches DDL blocks for processing during resolution.
        let mut cursor2 = root.walk();
        let has_ddl = root
            .children(&mut cursor2)
            .any(|c| c.kind() == "ddl_annotation");
        if has_ddl {
            let outcome = compile_single_query(source, system, config)?;
            return Ok(vec![PerQueryResult {
                outcome,
                index: 0,
                _source: source.to_string(),
            }]);
        }
        return Err(anyhow::anyhow!("No queries found in source"));
    }

    let mut results = Vec::with_capacity(query_spans.len());

    for (i, (start, end)) in query_spans.iter().enumerate() {
        let query_source = &source[*start..*end];

        let outcome = compile_single_query(query_source, system, config)?;

        results.push(PerQueryResult {
            outcome,
            index: i,
            _source: query_source.to_string(),
        });
    }

    Ok(results)
}

/// Compile a single query from source text with error hook handling.
fn compile_single_query(
    query_source: &str,
    system: &mut DelightQLSystem,
    config: &SequentialConfig,
) -> std::result::Result<SingleQueryOutcome, anyhow::Error> {
    // Grab bootstrap connection before pipeline borrows system mutably
    let _bootstrap_conn = system.get_bootstrap_connection();

    // Create Pipeline from source text (full source-text path)
    let mut pipeline = Pipeline::new_with_config(
        query_source,
        system,
        config.resolution_config.clone(),
        config.sql_optimization_level,
        config.inline_ctes,
        false, // is_repl
    );
    pipeline.set_cli_danger_overrides(config.danger_overrides.clone())?;
    pipeline.set_cli_option_overrides(config.option_overrides.clone());

    // Parse to CST so we can pre-scan for error hooks
    pipeline.execute_to_cst()?;

    // Pre-scan for error hook
    let error_hook = {
        let tree = pipeline.cst().unwrap();
        let root = tree.root_node();
        let mut cursor = root.walk();
        let query_node = root.children(&mut cursor).find(|c| c.kind() == "query");
        query_node.and_then(|qnode| {
            builder_v2::pre_scan_error_hook(&qnode, query_source)
                .ok()
                .flatten()
        })
    };

    let result = match error_hook {
        Some(expected) => compile_with_error_hook(pipeline, expected),
        None => {
            // Normal compilation — capture errors for stats flush
            match pipeline.compile() {
                Ok(compiled) => Ok(SingleQueryOutcome::Compiled(compiled)),
                Err(e) => Err(e.into()),
            }
        }
    };

    // Flush compilation stats to sys::execution tables
    {
        let (_sql_out, _err_msg): (Option<&str>, Option<String>) = match &result {
            Ok(SingleQueryOutcome::Compiled(c)) => (Some(&c.primary_sql), None),
            Ok(SingleQueryOutcome::PendingRuntimeErrorHook { compiled, .. }) => {
                (Some(&compiled.primary_sql), None)
            }
            Ok(SingleQueryOutcome::ErrorVerdict(v)) => (None, v.detail.clone()),
            Err(e) => (None, Some(e.to_string())),
        };
        #[cfg(feature = "recursion_stats")]
        {
            if let Ok(conn) = _bootstrap_conn.lock() {
                recursion_stats::flush_to_db(
                    &conn,
                    query_source,
                    _sql_out,
                    None, // CTE count not readily available in sequential path
                    _err_msg.as_deref(),
                );
            } else {
                recursion_stats::reset();
            }
        }
    }

    result
}

/// Handle compilation when an error hook is present.
fn compile_with_error_hook(
    mut pipeline: Pipeline<'_>,
    expected: builder_v2::ExpectedError,
) -> std::result::Result<SingleQueryOutcome, anyhow::Error> {
    let identity = verdict::VerdictIdentity {
        _name: None,
        _source_location: None,
        body_text: expected.display_uri(),
    };

    match pipeline.execute_to_sql() {
        Err(e) => {
            // Compilation failed — check if error matches expected
            let actual_uri = e.error_uri();
            let outcome = if expected.matches(&actual_uri) {
                verdict::VerdictOutcome::Pass
            } else {
                verdict::VerdictOutcome::Fail
            };

            Ok(SingleQueryOutcome::ErrorVerdict(verdict::Verdict {
                outcome,
                identity,
                detail: Some(format!("{}: {}", actual_uri, e)),
                _intent: None,
            }))
        }
        Ok(_) => {
            // Compilation succeeded — check if expected is runtime error
            let expects_runtime = expected
                .uri_segments
                .first()
                .map(|s| s == "dql")
                .unwrap_or(false)
                && expected
                    .uri_segments
                    .get(1)
                    .map(|s| s == "runtime")
                    .unwrap_or(false);

            if expects_runtime {
                // Return compiled SQL + expected for CLI to handle runtime check
                let compiled = pipeline.compile()?;
                Ok(SingleQueryOutcome::PendingRuntimeErrorHook { compiled, expected })
            } else {
                // Expected compile error but compilation succeeded
                Ok(SingleQueryOutcome::ErrorVerdict(verdict::Verdict {
                    outcome: verdict::VerdictOutcome::Fail,
                    identity,
                    detail: Some(format!(
                        "Expected failure matching '{}' but query compiled successfully",
                        expected.display_uri()
                    )),
                    _intent: None,
                }))
            }
        }
    }
}

/// Returns true if the source contains any `(~~ddl` markers.
///
/// Note: This is a quick text-level check for the CLI to detect when
/// sequential mode should be forced. The actual DDL extraction is
/// grammar-based (via ddl_annotation CST nodes).
pub fn has_inline_ddl(source: &str) -> bool {
    source.contains("(~~ddl")
}

/// Process a single inline DDL block: parse as DDL and register definitions.
///
/// Returns the names of any entities that were replaced (drop-and-replace semantics).
pub fn process_inline_ddl_block(
    body: &str,
    namespace: &str,
    system: &mut DelightQLSystem,
) -> std::result::Result<Vec<String>, anyhow::Error> {
    // Skip empty blocks
    if body.trim().is_empty() {
        return Ok(Vec::new());
    }

    let mut ddl = parser::parse_ddl_file(body)
        .map_err(|e| anyhow::anyhow!("Inline DDL parse error: {}", e))?;

    // Guard: inline DDL must contain definitions, not queries
    if ddl.definitions.is_empty() && ddl.inline_ddl_blocks.is_empty() {
        return Err(anyhow::anyhow!(
            "Inline DDL block contains no definitions. \
             (~~ddl:\"name\" ~~) expects rules (:-), tables (:=), or function definitions."
        ));
    }
    if !ddl.query_statements.is_empty() {
        return Err(anyhow::anyhow!(
            "Inline DDL block contains query statements (?-). \
             (~~ddl:\"name\" ~~) expects only definitions, not queries."
        ));
    }

    // Extract nested inline DDL blocks before consuming ddl
    let nested_blocks = std::mem::take(&mut ddl.inline_ddl_blocks);

    let result = system
        .consult_file("(inline)", namespace, ddl)
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Inline DDL registration failed: {}", e),
                "consult error",
            )
        })?;

    // Recursively process nested inline DDL blocks
    for block in &nested_blocks {
        let child_ns = match &block.namespace {
            Some(suffix) => format!("{}::{}", namespace, suffix),
            None => namespace.to_string(),
        };
        process_inline_ddl_block(&block.body, &child_ns, system)?;
    }

    // Auto-enlist default namespace ("main::user") so definitions are immediately usable
    if namespace == "main::user" {
        let _ = system.enlist_namespace("main::user");
    }

    Ok(result.replaced_entities)
}
