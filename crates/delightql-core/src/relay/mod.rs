// RelayParty — Front-End Seam (Epoch 6)
//
// RelayParty is the front-end seam: DQL in, protocol terms out.
// The back-end seam (SqlParty, SisoParty, etc.) handles SQL execution.
//
// Generic over T: Transport so it can wrap any backend party via the
// protocol stack. SqlParty uses streaming cursors (rusqlite); SisoParty
// uses the DatabaseConnection trait (eager, buffered).

use std::collections::HashMap;

use delightql_protocol::{
    decode_cell_to_text, ByteSeq, Cell, ClientTerm, CloseResponse, Dimension, ErrorKind,
    FetchResponse, Handle, Handler, MetaItem, Orientation, Projection, QueryHandle, QueryResponse,
    ServerTerm, Session, Transport, CELL_TAG_TEXT,
};
#[cfg(not(target_arch = "wasm32"))]
use rusqlite;

use crate::{
    pipeline::{self, builder_v2, compiled_query, resolver::ResolutionConfig, verdict, Pipeline},
    system::DelightQLSystem,
};

/// Buffered eager results for non-streaming connections (bootstrap, imported).
struct EagerBuffer {
    #[allow(dead_code)]
    dimensions: Vec<Dimension>,
    rows: Vec<Vec<Cell>>,
    cursor: usize,
}

#[cfg(test)]
mod tests;

// --- Hooks ---

/// Hooks for non-relational side effects during query execution.
///
/// The CLI wires these to print verdicts, route emit streams to sinks, etc.
/// If no hook is set, the relay handles the effect internally (assertions
/// become protocol errors, emits are silently executed).
pub struct RelayHooks {
    /// Called for each emit stream after execution.
    /// Args: (stream_name, columns, rows).
    pub on_emit: Option<Box<dyn FnMut(&str, &[String], &[Vec<String>])>>,

    /// Called for each assertion verdict (pass or fail).
    pub on_verdict: Option<Box<dyn FnMut(&verdict::Verdict)>>,

    /// Called when an error hook fires (compile-time or runtime).
    pub on_error_hook: Option<Box<dyn FnMut(&verdict::Verdict)>>,
}

impl Default for RelayHooks {
    fn default() -> Self {
        Self {
            on_emit: None,
            on_verdict: None,
            on_error_hook: None,
        }
    }
}

// --- RelayParty ---

pub struct RelayParty<'a, T: Transport> {
    system: &'a mut DelightQLSystem,
    sql_session: Session<T>,
    handles: HashMap<Handle, QueryHandle>, // frontend handle → backend QueryHandle
    eager_buffers: HashMap<Handle, EagerBuffer>, // frontend handle → eager results
    next_handle_id: u64,
    danger_overrides: Vec<pipeline::ast_unresolved::DangerSpec>,
    option_overrides: Vec<pipeline::ast_unresolved::OptionSpec>,
    is_repl: bool,
    sql_optimization_level: pipeline::sql_optimizer::OptimizationLevel,
    inline_ctes: bool,
    hooks: RelayHooks,
}

impl<'a, T: Transport> RelayParty<'a, T> {
    pub fn new(system: &'a mut DelightQLSystem, sql_session: Session<T>) -> Self {
        RelayParty {
            system,
            sql_session,
            handles: HashMap::new(),
            eager_buffers: HashMap::new(),
            next_handle_id: 1,
            danger_overrides: Vec::new(),
            option_overrides: Vec::new(),
            is_repl: false,
            sql_optimization_level: pipeline::sql_optimizer::OptimizationLevel::None,
            inline_ctes: false,
            hooks: RelayHooks::default(),
        }
    }

    /// Handle a Reset control operation: close all open handles and reinit the system.
    pub fn handle_reset(&mut self) -> Result<(), crate::error::DelightQLError> {
        for (_frontend, backend) in self.handles.drain() {
            let _ = self.sql_session.close(backend);
        }
        self.eager_buffers.clear();
        self.next_handle_id = 1;
        self.system.reinit_bootstrap()
    }

    fn handle_query(&mut self, text: ByteSeq) -> ServerTerm {
        let dql = match String::from_utf8(text) {
            Ok(s) => s,
            Err(e) => {
                return ServerTerm::Error {
                    kind: ErrorKind::Syntax,
                    identity: vec![],
                    message: format!("invalid UTF-8 in query text: {}", e).into_bytes(),
                }
            }
        };

        // Parse CST once for multi-query check and error hook pre-scan.
        // Also check for inline DDL blocks which require sequential mode.
        let has_ddl = pipeline::sequential::has_inline_ddl(&dql);
        let error_hook = if let Ok(tree) = crate::pipeline::parser::parse(&dql) {
            let root = tree.root_node();
            let mut cursor = root.walk();
            let query_count = root
                .children(&mut cursor)
                .filter(|c| c.kind() == "query")
                .count();
            if query_count > 1 {
                // Protocol violation: one Query term → one Header or one Error.
                // Multi-query input must be sent as separate Query terms by the client.
                return ServerTerm::Error {
                    kind: ErrorKind::Syntax,
                    identity: b"dql/parse/multi_query".to_vec(),
                    message: format!(
                        "multi-query input rejected: found {} queries in a single Query term \
                         (send each query as a separate Query message)",
                        query_count
                    )
                    .into_bytes(),
                };
            }
            if has_ddl {
                return self.handle_sequential_query(&dql);
            }
            // Pre-scan for error hook annotation
            let mut cursor2 = root.walk();
            let hook = root
                .children(&mut cursor2)
                .find(|c| c.kind() == "query")
                .and_then(|qnode| builder_v2::pre_scan_error_hook(&qnode, &dql).ok().flatten());
            hook
        } else {
            None // Parse error — Pipeline will catch it with a proper error message
        };

        // Error hook path: handle both compile-time and runtime error hooks
        if let Some(expected) = error_hook {
            return self.handle_error_hook_query(&dql, expected);
        }

        // Normal single-query path: compile DQL → SQL via the pipeline
        let mut pipeline = Pipeline::new_with_config(
            &dql,
            &mut *self.system,
            ResolutionConfig::default(),
            self.sql_optimization_level,
            self.inline_ctes,
            self.is_repl,
        );

        // Apply CLI-level overrides
        if let Err(e) = pipeline.set_cli_danger_overrides(self.danger_overrides.clone()) {
            return ServerTerm::Error {
                kind: ErrorKind::Syntax,
                identity: e.error_uri().into_bytes(),
                message: format!("{}", e).into_bytes(),
            };
        }
        pipeline.set_cli_option_overrides(self.option_overrides.clone());

        let compiled = match pipeline.compile() {
            Ok(c) => c,
            Err(e) => {
                return ServerTerm::Error {
                    kind: ErrorKind::Syntax,
                    identity: e.error_uri().into_bytes(),
                    message: format!("{}", e).into_bytes(),
                }
            }
        };

        // Capture emit streams, assertion data, and connection routing before evaluating
        let emit_streams: Vec<compiled_query::EmitStream> = compiled.emit_streams.clone();
        let assertion_sqls = compiled.assertion_sqls.clone();
        let connection_id = compiled.connection_id;
        let primary_sql = compiled.primary_sql.clone();

        // Drop pipeline to release borrow on self.system
        drop(pipeline);

        // Evaluate assertions on the routed connection
        for (i, (assertion_sql, _location)) in assertion_sqls.iter().enumerate() {
            match self.execute_sql_routed(assertion_sql, connection_id) {
                Ok((_cols, rows)) => {
                    let passed = rows
                        .first()
                        .and_then(|row| row.first())
                        .map(|v| matches!(v.as_str(), "1" | "true" | "t"))
                        .unwrap_or(false);

                    if let Some(ref mut hook) = self.hooks.on_verdict {
                        let v = verdict::Verdict {
                            outcome: if passed {
                                verdict::VerdictOutcome::Pass
                            } else {
                                verdict::VerdictOutcome::Fail
                            },
                            identity: verdict::VerdictIdentity {
                                _name: None,
                                _source_location: None,
                                body_text: assertion_sql.clone(),
                            },
                            detail: if passed {
                                None
                            } else {
                                Some(format!(
                                    "Assertion {} failed\n  SQL: {}",
                                    i + 1,
                                    assertion_sql
                                ))
                            },
                            _intent: None,
                        };
                        hook(&v);
                    }

                    if !passed {
                        return ServerTerm::Error {
                            kind: ErrorKind::Permission,
                            identity: b"dql/runtime/assertion".to_vec(),
                            message: format!(
                                "Assertion {} failed\n  SQL: {}",
                                i + 1,
                                assertion_sql
                            )
                            .into_bytes(),
                        };
                    }
                }
                Err(msg) => {
                    return ServerTerm::Error {
                        kind: ErrorKind::Permission,
                        identity: b"dql/runtime/assertion".to_vec(),
                        message: format!("Assertion {} execution error: {}", i + 1, msg)
                            .into_bytes(),
                    };
                }
            }
        }

        // Execute emit streams on the routed connection
        for emit in &emit_streams {
            match self.execute_sql_routed(&emit.sql, connection_id) {
                Ok((columns, rows)) => {
                    if let Some(ref mut hook) = self.hooks.on_emit {
                        hook(&emit.name, &columns, &rows);
                    }
                }
                Err(msg) => {
                    if let Some(ref mut hook) = self.hooks.on_error_hook {
                        let v = verdict::Verdict {
                            outcome: verdict::VerdictOutcome::Fail,
                            identity: verdict::VerdictIdentity {
                                _name: Some(emit.name.clone()),
                                _source_location: None,
                                body_text: format!(
                                    "Emit '{}' execution failed: {}",
                                    emit.name, msg
                                ),
                            },
                            detail: Some(msg),
                            _intent: None,
                        };
                        hook(&v);
                    }
                }
            }
        }

        // Route primary SQL based on connection_id
        let cid = connection_id.unwrap_or(2);
        if cid == 2 {
            // Streaming path: forward to sql_session
            let sql_bytes = primary_sql.as_bytes().to_vec();
            match self.sql_session.query(sql_bytes) {
                Ok(QueryResponse::Header {
                    handle: backend_handle,
                    dimensions,
                }) => {
                    let frontend_handle = self.next_handle();
                    self.handles.insert(frontend_handle.clone(), backend_handle);
                    ServerTerm::Header {
                        handle: frontend_handle,
                        dimensions,
                    }
                }
                Ok(QueryResponse::Error {
                    kind,
                    identity,
                    message,
                }) => ServerTerm::Error {
                    kind,
                    identity,
                    message,
                },
                Err(e) => ServerTerm::Error {
                    kind: ErrorKind::Connection,
                    identity: vec![],
                    message: e.message.into_bytes(),
                },
            }
        } else {
            // Eager path: execute on bootstrap or imported connection, buffer results
            match self.execute_sql_routed(&primary_sql, connection_id) {
                Ok((columns, rows)) => {
                    let (dimensions, cells) = Self::strings_to_eager_buffer(&columns, &rows);
                    let handle = self.next_handle();
                    self.eager_buffers.insert(
                        handle.clone(),
                        EagerBuffer {
                            dimensions: dimensions.clone(),
                            rows: cells,
                            cursor: 0,
                        },
                    );
                    ServerTerm::Header { handle, dimensions }
                }
                Err(msg) => ServerTerm::Error {
                    kind: ErrorKind::Connection,
                    identity: vec![],
                    message: msg.into_bytes(),
                },
            }
        }
    }

    /// Compile and execute a sequential (multi-statement) query, returning the
    /// last statement's results through the protocol.
    fn handle_sequential_query(&mut self, dql: &str) -> ServerTerm {
        use pipeline::sequential::{compile_sequential, SequentialConfig, SingleQueryOutcome};

        let config = SequentialConfig {
            resolution_config: ResolutionConfig::default(),
            sql_optimization_level: self.sql_optimization_level,
            inline_ctes: self.inline_ctes,
            danger_overrides: self.danger_overrides.clone(),
            option_overrides: self.option_overrides.clone(),
        };

        let results = match compile_sequential(dql, self.system, &config) {
            Ok(r) => r,
            Err(e) => {
                let identity = e
                    .downcast_ref::<crate::error::DelightQLError>()
                    .map(|de| de.error_uri().into_bytes())
                    .unwrap_or_default();
                return ServerTerm::Error {
                    kind: ErrorKind::Syntax,
                    identity,
                    message: format!("{}", e).into_bytes(),
                };
            }
        };

        let total = results.len();

        for pq in results {
            let is_last = pq.index == total - 1;

            match pq.outcome {
                SingleQueryOutcome::ErrorVerdict(v) => {
                    if let Some(ref mut hook) = self.hooks.on_error_hook {
                        hook(&v);
                    }
                    if matches!(v.outcome, verdict::VerdictOutcome::Fail) {
                        return ServerTerm::Error {
                            kind: ErrorKind::Permission,
                            identity: vec![],
                            message: format!(
                                "Error hook: {}",
                                v.detail.as_deref().unwrap_or(&v.identity.body_text)
                            )
                            .into_bytes(),
                        };
                    }
                    if is_last {
                        return self.empty_header_response();
                    }
                }

                SingleQueryOutcome::PendingRuntimeErrorHook { compiled, expected } => {
                    match self.execute_sql_routed(&compiled.primary_sql, compiled.connection_id) {
                        Err(e) => {
                            let actual_uri = "dql/runtime/bug";
                            let v = verdict::Verdict {
                                outcome: if expected.matches(actual_uri) {
                                    verdict::VerdictOutcome::Pass
                                } else {
                                    verdict::VerdictOutcome::Fail
                                },
                                identity: verdict::VerdictIdentity {
                                    _name: None,
                                    _source_location: None,
                                    body_text: expected.display_uri(),
                                },
                                detail: Some(format!("{}: {}", actual_uri, e)),
                                _intent: None,
                            };
                            if let Some(ref mut hook) = self.hooks.on_error_hook {
                                hook(&v);
                            }
                            if matches!(v.outcome, verdict::VerdictOutcome::Fail) {
                                return ServerTerm::Error {
                                    kind: ErrorKind::Permission,
                                    identity: vec![],
                                    message: format!(
                                        "Error hook: expected '{}' but got '{}': {}",
                                        expected.display_uri(),
                                        actual_uri,
                                        e
                                    )
                                    .into_bytes(),
                                };
                            }
                        }
                        Ok(_) => {
                            // SQL succeeded — check assertions for runtime error
                            let mut assertion_matched = false;
                            for (sql, _loc) in &compiled.assertion_sqls {
                                match self.execute_sql_routed(sql, compiled.connection_id) {
                                    Ok((_cols, rows)) => {
                                        let passed = rows
                                            .first()
                                            .and_then(|r| r.first())
                                            .map(|v| matches!(v.as_str(), "1" | "true" | "t"))
                                            .unwrap_or(false);
                                        if !passed {
                                            let actual_uri = "dql/runtime/assertion";
                                            let v = verdict::Verdict {
                                                outcome: if expected.matches(actual_uri) {
                                                    verdict::VerdictOutcome::Pass
                                                } else {
                                                    verdict::VerdictOutcome::Fail
                                                },
                                                identity: verdict::VerdictIdentity {
                                                    _name: None,
                                                    _source_location: None,
                                                    body_text: expected.display_uri(),
                                                },
                                                detail: Some(
                                                    "Runtime assertion failed".to_string(),
                                                ),
                                                _intent: None,
                                            };
                                            if let Some(ref mut hook) = self.hooks.on_error_hook {
                                                hook(&v);
                                            }
                                            if matches!(v.outcome, verdict::VerdictOutcome::Fail) {
                                                return ServerTerm::Error {
                                                    kind: ErrorKind::Permission,
                                                    identity: vec![],
                                                    message: format!(
                                                        "Error hook: expected '{}' but got '{}'",
                                                        expected.display_uri(),
                                                        actual_uri
                                                    )
                                                    .into_bytes(),
                                                };
                                            }
                                            assertion_matched = true;
                                            break;
                                        }
                                    }
                                    Err(msg) => {
                                        return ServerTerm::Error {
                                            kind: ErrorKind::Permission,
                                            identity: b"dql/runtime/assertion".to_vec(),
                                            message: format!("Assertion execution error: {}", msg)
                                                .into_bytes(),
                                        };
                                    }
                                }
                            }
                            if !assertion_matched {
                                let v = verdict::Verdict {
                                    outcome: verdict::VerdictOutcome::Fail,
                                    identity: verdict::VerdictIdentity {
                                        _name: None,
                                        _source_location: None,
                                        body_text: expected.display_uri(),
                                    },
                                    detail: Some(format!(
                                        "Expected failure matching '{}' but query executed successfully",
                                        expected.display_uri()
                                    )),
                                    _intent: None,
                                };
                                if let Some(ref mut hook) = self.hooks.on_error_hook {
                                    hook(&v);
                                }
                                return ServerTerm::Error {
                                    kind: ErrorKind::Permission,
                                    identity: vec![],
                                    message: format!(
                                        "Error hook: Expected failure matching '{}' but query executed successfully",
                                        expected.display_uri()
                                    )
                                    .into_bytes(),
                                };
                            }
                        }
                    }
                    if is_last {
                        return self.empty_header_response();
                    }
                }

                SingleQueryOutcome::Compiled(compiled) => {
                    let connection_id = compiled.connection_id;

                    // Evaluate assertions
                    for (i, (sql, _loc)) in compiled.assertion_sqls.iter().enumerate() {
                        match self.execute_sql_routed(sql, connection_id) {
                            Ok((_cols, rows)) => {
                                let passed = rows
                                    .first()
                                    .and_then(|r| r.first())
                                    .map(|v| matches!(v.as_str(), "1" | "true" | "t"))
                                    .unwrap_or(false);

                                if let Some(ref mut hook) = self.hooks.on_verdict {
                                    let v = verdict::Verdict {
                                        outcome: if passed {
                                            verdict::VerdictOutcome::Pass
                                        } else {
                                            verdict::VerdictOutcome::Fail
                                        },
                                        identity: verdict::VerdictIdentity {
                                            _name: None,
                                            _source_location: None,
                                            body_text: sql.clone(),
                                        },
                                        detail: if passed {
                                            None
                                        } else {
                                            Some(format!(
                                                "Assertion {} failed\n  SQL: {}",
                                                i + 1,
                                                sql
                                            ))
                                        },
                                        _intent: None,
                                    };
                                    hook(&v);
                                }

                                if !passed {
                                    return ServerTerm::Error {
                                        kind: ErrorKind::Permission,
                                        identity: b"dql/runtime/assertion".to_vec(),
                                        message: format!(
                                            "Assertion {} failed\n  SQL: {}",
                                            i + 1,
                                            sql
                                        )
                                        .into_bytes(),
                                    };
                                }
                            }
                            Err(msg) => {
                                return ServerTerm::Error {
                                    kind: ErrorKind::Permission,
                                    identity: b"dql/runtime/assertion".to_vec(),
                                    message: format!(
                                        "Assertion {} execution error: {}",
                                        i + 1,
                                        msg
                                    )
                                    .into_bytes(),
                                };
                            }
                        }
                    }

                    // Execute emit streams
                    for emit in &compiled.emit_streams {
                        match self.execute_sql_routed(&emit.sql, connection_id) {
                            Ok((columns, rows)) => {
                                if let Some(ref mut hook) = self.hooks.on_emit {
                                    hook(&emit.name, &columns, &rows);
                                }
                            }
                            Err(msg) => {
                                if let Some(ref mut hook) = self.hooks.on_error_hook {
                                    let v = verdict::Verdict {
                                        outcome: verdict::VerdictOutcome::Fail,
                                        identity: verdict::VerdictIdentity {
                                            _name: Some(emit.name.clone()),
                                            _source_location: None,
                                            body_text: format!(
                                                "Emit '{}' execution failed: {}",
                                                emit.name, msg
                                            ),
                                        },
                                        detail: Some(msg),
                                        _intent: None,
                                    };
                                    hook(&v);
                                }
                            }
                        }
                    }

                    // Execute primary SQL
                    if is_last {
                        // Return last query's result as a protocol Header
                        let cid = connection_id.unwrap_or(2);
                        if cid == 2 {
                            let sql_bytes = compiled.primary_sql.as_bytes().to_vec();
                            match self.sql_session.query(sql_bytes) {
                                Ok(QueryResponse::Header {
                                    handle: backend_handle,
                                    dimensions,
                                }) => {
                                    let frontend_handle = self.next_handle();
                                    self.handles.insert(frontend_handle.clone(), backend_handle);
                                    return ServerTerm::Header {
                                        handle: frontend_handle,
                                        dimensions,
                                    };
                                }
                                Ok(QueryResponse::Error {
                                    kind,
                                    identity,
                                    message,
                                }) => {
                                    return ServerTerm::Error {
                                        kind,
                                        identity,
                                        message,
                                    };
                                }
                                Err(e) => {
                                    return ServerTerm::Error {
                                        kind: ErrorKind::Connection,
                                        identity: vec![],
                                        message: e.message.into_bytes(),
                                    };
                                }
                            }
                        } else {
                            match self.execute_sql_routed(&compiled.primary_sql, connection_id) {
                                Ok((columns, rows)) => {
                                    let (dimensions, cells) =
                                        Self::strings_to_eager_buffer(&columns, &rows);
                                    let handle = self.next_handle();
                                    self.eager_buffers.insert(
                                        handle.clone(),
                                        EagerBuffer {
                                            dimensions: dimensions.clone(),
                                            rows: cells,
                                            cursor: 0,
                                        },
                                    );
                                    return ServerTerm::Header { handle, dimensions };
                                }
                                Err(msg) => {
                                    return ServerTerm::Error {
                                        kind: ErrorKind::Connection,
                                        identity: vec![],
                                        message: msg.into_bytes(),
                                    };
                                }
                            }
                        }
                    } else {
                        // Intermediate query: execute and discard results
                        if let Err(msg) =
                            self.execute_sql_routed(&compiled.primary_sql, connection_id)
                        {
                            return ServerTerm::Error {
                                kind: ErrorKind::Connection,
                                identity: vec![],
                                message: msg.into_bytes(),
                            };
                        }
                    }
                }
            }
        }

        // Should not reach here (we return from the last query),
        // but handle gracefully with an empty result set.
        self.empty_header_response()
    }

    /// Handle a single query with an error hook annotation.
    ///
    /// Supports both compile-time error hooks (query fails to compile) and
    /// runtime error hooks (query compiles but fails at execution or assertion).
    fn handle_error_hook_query(
        &mut self,
        dql: &str,
        expected: builder_v2::ExpectedError,
    ) -> ServerTerm {
        let identity = verdict::VerdictIdentity {
            _name: None,
            _source_location: None,
            body_text: expected.display_uri(),
        };

        // Try to compile the query
        let mut pipeline = Pipeline::new_with_config(
            dql,
            &mut *self.system,
            ResolutionConfig::default(),
            self.sql_optimization_level,
            self.inline_ctes,
            self.is_repl,
        );
        if let Err(e) = pipeline.set_cli_danger_overrides(self.danger_overrides.clone()) {
            return ServerTerm::Error {
                kind: ErrorKind::Syntax,
                identity: e.error_uri().into_bytes(),
                message: format!("{}", e).into_bytes(),
            };
        }
        pipeline.set_cli_option_overrides(self.option_overrides.clone());

        let compiled = match pipeline.compile() {
            Err(e) => {
                // Compile error — match against expected error URI
                let actual_uri = e.error_uri();
                let v = verdict::Verdict {
                    outcome: if expected.matches(&actual_uri) {
                        verdict::VerdictOutcome::Pass
                    } else {
                        verdict::VerdictOutcome::Fail
                    },
                    identity,
                    detail: Some(format!("{}: {}", actual_uri, e)),
                    _intent: None,
                };
                if let Some(ref mut hook) = self.hooks.on_error_hook {
                    hook(&v);
                }
                return self.verdict_response(&v);
            }
            Ok(c) => c,
        };

        // Compilation succeeded. Check if we expect a runtime error.
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

        if !expects_runtime {
            // Expected a compile error but query compiled successfully
            let v = verdict::Verdict {
                outcome: verdict::VerdictOutcome::Fail,
                identity,
                detail: Some(format!(
                    "Expected failure matching '{}' but query compiled successfully",
                    expected.display_uri()
                )),
                _intent: None,
            };
            if let Some(ref mut hook) = self.hooks.on_error_hook {
                hook(&v);
            }
            return self.verdict_response(&v);
        }

        // Runtime error hook: compile succeeded, now execute and check for failure.
        // Drop pipeline to release borrow on self.system.
        let connection_id = compiled.connection_id;
        let primary_sql = compiled.primary_sql.clone();
        let assertion_sqls = compiled.assertion_sqls.clone();
        drop(pipeline);

        // Execute primary SQL
        match self.execute_sql_routed(&primary_sql, connection_id) {
            Err(e) => {
                // SQL execution failed — match against expected
                let actual_uri = "dql/runtime/bug";
                let v = verdict::Verdict {
                    outcome: if expected.matches(actual_uri) {
                        verdict::VerdictOutcome::Pass
                    } else {
                        verdict::VerdictOutcome::Fail
                    },
                    identity,
                    detail: Some(format!("{}: {}", actual_uri, e)),
                    _intent: None,
                };
                if let Some(ref mut hook) = self.hooks.on_error_hook {
                    hook(&v);
                }
                return self.verdict_response(&v);
            }
            Ok(_) => {
                // SQL succeeded — check assertions for runtime errors
                for (sql, _loc) in &assertion_sqls {
                    match self.execute_sql_routed(sql, connection_id) {
                        Ok((_cols, rows)) => {
                            let passed = rows
                                .first()
                                .and_then(|r| r.first())
                                .map(|v| matches!(v.as_str(), "1" | "true" | "t"))
                                .unwrap_or(false);
                            if !passed {
                                let actual_uri = "dql/runtime/assertion";
                                let v = verdict::Verdict {
                                    outcome: if expected.matches(actual_uri) {
                                        verdict::VerdictOutcome::Pass
                                    } else {
                                        verdict::VerdictOutcome::Fail
                                    },
                                    identity,
                                    detail: Some("Runtime assertion failed".to_string()),
                                    _intent: None,
                                };
                                if let Some(ref mut hook) = self.hooks.on_error_hook {
                                    hook(&v);
                                }
                                return self.verdict_response(&v);
                            }
                        }
                        Err(msg) => {
                            return ServerTerm::Error {
                                kind: ErrorKind::Permission,
                                identity: b"dql/runtime/assertion".to_vec(),
                                message: format!("Assertion execution error: {}", msg).into_bytes(),
                            };
                        }
                    }
                }

                // Everything succeeded but we expected failure
                let v = verdict::Verdict {
                    outcome: verdict::VerdictOutcome::Fail,
                    identity,
                    detail: Some(format!(
                        "Expected failure matching '{}' but query executed successfully",
                        expected.display_uri()
                    )),
                    _intent: None,
                };
                if let Some(ref mut hook) = self.hooks.on_error_hook {
                    hook(&v);
                }
                self.verdict_response(&v)
            }
        }
    }

    fn handle_fetch(
        &mut self,
        handle: Handle,
        projection: Projection,
        count: u64,
        orientation: Orientation,
    ) -> ServerTerm {
        // Check eager buffers first (bootstrap/imported connections)
        if let Some(buffer) = self.eager_buffers.get_mut(&handle) {
            if buffer.cursor >= buffer.rows.len() {
                return ServerTerm::End;
            }
            let end = std::cmp::min(buffer.cursor + count as usize, buffer.rows.len());
            let batch = buffer.rows[buffer.cursor..end].to_vec();
            buffer.cursor = end;
            return ServerTerm::Data { cells: batch };
        }

        // Streaming path: forward to sql_session
        let backend_handle = match self.handles.get(&handle) {
            Some(bh) => bh,
            None => {
                return ServerTerm::Error {
                    kind: ErrorKind::Connection,
                    identity: vec![],
                    message: b"unknown handle".to_vec(),
                }
            }
        };

        let agreed = match self.sql_session.agreed_orientation(orientation) {
            Some(a) => a,
            None => {
                return ServerTerm::Error {
                    kind: ErrorKind::Connection,
                    identity: vec![],
                    message: b"orientation not agreed".to_vec(),
                }
            }
        };

        match self
            .sql_session
            .fetch(backend_handle, projection, count, agreed)
        {
            Ok(FetchResponse::Data { cells }) => ServerTerm::Data { cells },
            Ok(FetchResponse::End) => ServerTerm::End,
            Ok(FetchResponse::Error {
                kind,
                identity,
                message,
            }) => ServerTerm::Error {
                kind,
                identity,
                message,
            },
            Err(e) => ServerTerm::Error {
                kind: ErrorKind::Connection,
                identity: vec![],
                message: e.message.into_bytes(),
            },
        }
    }

    fn handle_stat(&self, handle: Handle) -> ServerTerm {
        if self.eager_buffers.contains_key(&handle) {
            return ServerTerm::Metadata {
                items: vec![MetaItem::Backend(
                    b"sqlite".to_vec(),
                    b"relay-eager".to_vec(),
                )],
            };
        }
        if !self.handles.contains_key(&handle) {
            return ServerTerm::Error {
                kind: ErrorKind::Connection,
                identity: vec![],
                message: b"unknown handle".to_vec(),
            };
        }
        ServerTerm::Metadata {
            items: vec![MetaItem::Backend(
                b"sqlite".to_vec(),
                b"relay-epoch6".to_vec(),
            )],
        }
    }

    fn handle_close(&mut self, handle: Handle) -> ServerTerm {
        // Check eager buffers first
        if self.eager_buffers.remove(&handle).is_some() {
            return ServerTerm::Ok { count_hint: 0 };
        }

        match self.handles.remove(&handle) {
            Some(backend_handle) => match self.sql_session.close(backend_handle) {
                Ok(CloseResponse::Ok) => ServerTerm::Ok { count_hint: 0 },
                Ok(CloseResponse::Error {
                    kind,
                    identity,
                    message,
                }) => ServerTerm::Error {
                    kind,
                    identity,
                    message,
                },
                Err(e) => ServerTerm::Error {
                    kind: ErrorKind::Connection,
                    identity: vec![],
                    message: e.message.into_bytes(),
                },
            },
            None => ServerTerm::Error {
                kind: ErrorKind::Connection,
                identity: vec![],
                message: b"unknown handle".to_vec(),
            },
        }
    }

    fn next_handle(&mut self) -> Handle {
        let id = self.next_handle_id;
        self.next_handle_id += 1;
        format!("h{}", id).into_bytes()
    }

    /// Return the appropriate protocol response for an error hook verdict.
    /// Pass → empty header (the hook matched). Fail → protocol error.
    fn verdict_response(&mut self, v: &verdict::Verdict) -> ServerTerm {
        match v.outcome {
            verdict::VerdictOutcome::Pass => self.empty_header_response(),
            verdict::VerdictOutcome::Fail => ServerTerm::Error {
                kind: ErrorKind::Constraint,
                identity: vec![],
                message: v
                    .detail
                    .as_deref()
                    .unwrap_or("Error hook verdict: FAIL")
                    .as_bytes()
                    .to_vec(),
            },
        }
    }

    fn empty_header_response(&mut self) -> ServerTerm {
        let handle = self.next_handle();
        self.eager_buffers.insert(
            handle.clone(),
            EagerBuffer {
                dimensions: vec![],
                rows: vec![],
                cursor: 0,
            },
        );
        ServerTerm::Header {
            handle,
            dimensions: vec![],
        }
    }

    // --- Connection routing ---

    /// Execute SQL eagerly on the bootstrap connection (connection_id=1).
    #[cfg(not(target_arch = "wasm32"))]
    fn execute_eager_on_bootstrap(
        &self,
        sql: &str,
    ) -> Result<(Vec<String>, Vec<Vec<String>>), String> {
        let conn = self.system.get_bootstrap_connection();
        let conn_guard = conn.lock().map_err(|e| format!("Bootstrap lock: {}", e))?;
        let mut stmt = conn_guard
            .prepare(sql)
            .map_err(|e| format!("Bootstrap prepare: {}", e))?;
        let col_count = stmt.column_count();
        let column_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let rows_result = stmt
            .query_map([], |row| {
                let mut values = Vec::new();
                for idx in 0..col_count {
                    let val: rusqlite::types::Value = row.get(idx)?;
                    let s = match val {
                        rusqlite::types::Value::Null => "NULL".to_string(),
                        rusqlite::types::Value::Integer(i) => i.to_string(),
                        rusqlite::types::Value::Real(f) => f.to_string(),
                        rusqlite::types::Value::Text(s) => s,
                        rusqlite::types::Value::Blob(b) => {
                            format!("<blob {} bytes>", b.len())
                        }
                    };
                    values.push(s);
                }
                Ok(values)
            })
            .map_err(|e| format!("Bootstrap query: {}", e))?;
        let mut result_rows = Vec::new();
        for r in rows_result {
            result_rows.push(r.map_err(|e| format!("Bootstrap fetch: {}", e))?);
        }
        Ok((column_names, result_rows))
    }

    /// Execute SQL eagerly on an imported connection (connection_id >= 3).
    fn execute_eager_on_imported(
        &self,
        sql: &str,
        connection_id: i64,
    ) -> Result<(Vec<String>, Vec<Vec<String>>), String> {
        let conn_arc = self
            .system
            .get_connection(connection_id)
            .map_err(|e| format!("{}", e))?;
        let conn_guard = conn_arc
            .lock()
            .map_err(|e| format!("Connection {} lock: {}", connection_id, e))?;
        conn_guard
            .query_all_string_rows(sql, &[])
            .map_err(|e| format!("{}", e))
    }

    /// Execute SQL on the appropriate connection based on connection_id.
    ///
    /// - `None` or `2`: route through the streaming backend protocol (sql_session)
    /// - `1`: execute eagerly on the bootstrap connection
    /// - `>= 3`: execute eagerly on an imported connection
    fn execute_sql_routed(
        &mut self,
        sql: &str,
        connection_id: Option<i64>,
    ) -> Result<(Vec<String>, Vec<Vec<String>>), String> {
        match connection_id.unwrap_or(2) {
            2 => self.execute_emit_through_protocol(sql),
            1 => {
                #[cfg(not(target_arch = "wasm32"))]
                {
                    self.execute_eager_on_bootstrap(sql)
                }
                #[cfg(target_arch = "wasm32")]
                {
                    let _ = sql;
                    Err("bootstrap queries not supported on wasm32".to_string())
                }
            }
            id => self.execute_eager_on_imported(sql, id),
        }
    }

    /// Convert string-based query results to protocol cell format.
    fn strings_to_eager_buffer(
        columns: &[String],
        rows: &[Vec<String>],
    ) -> (Vec<Dimension>, Vec<Vec<Cell>>) {
        let dimensions: Vec<Dimension> = columns
            .iter()
            .enumerate()
            .map(|(i, name)| Dimension {
                position: i as u64,
                name: name.as_bytes().to_vec(),
                descriptor: Vec::new(),
            })
            .collect();
        let cells: Vec<Vec<Cell>> = rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|val| {
                        if val == "NULL" {
                            None
                        } else {
                            let mut v = vec![CELL_TAG_TEXT];
                            v.extend_from_slice(val.as_bytes());
                            Some(v)
                        }
                    })
                    .collect()
            })
            .collect();
        (dimensions, cells)
    }

    /// Execute SQL through the backend protocol and return (columns, rows).
    fn execute_emit_through_protocol(
        &mut self,
        sql: &str,
    ) -> Result<(Vec<String>, Vec<Vec<String>>), String> {
        let rows_orient = self
            .sql_session
            .agreed_orientation(Orientation::Rows)
            .ok_or_else(|| "Rows orientation not agreed".to_string())?;

        let resp = self
            .sql_session
            .query(sql.as_bytes().to_vec())
            .map_err(|e| e.message)?;

        let (handle, dimensions) = match resp {
            QueryResponse::Header { handle, dimensions } => (handle, dimensions),
            QueryResponse::Error { message, .. } => {
                return Err(String::from_utf8_lossy(&message).to_string());
            }
        };

        let columns: Vec<String> = dimensions
            .iter()
            .map(|d| String::from_utf8_lossy(&d.name).to_string())
            .collect();

        let mut all_rows = Vec::new();
        loop {
            let fetch_resp =
                match self
                    .sql_session
                    .fetch(&handle, Projection::All, u64::MAX, rows_orient)
                {
                    Ok(resp) => resp,
                    Err(e) => {
                        let _ = self.sql_session.close(handle);
                        return Err(e.message);
                    }
                };

            match fetch_resp {
                FetchResponse::Data { cells } => {
                    for row in &cells {
                        all_rows.push(
                            row.iter()
                                .map(|cell| match cell {
                                    Some(bytes) => decode_cell_to_text(bytes),
                                    None => "NULL".to_string(),
                                })
                                .collect(),
                        );
                    }
                }
                FetchResponse::End => break,
                FetchResponse::Error { message, .. } => {
                    let _ = self.sql_session.close(handle);
                    return Err(String::from_utf8_lossy(&message).to_string());
                }
            }
        }

        let _ = self.sql_session.close(handle);
        Ok((columns, all_rows))
    }
}

impl<'a, T: Transport> crate::api::ServerRelay for RelayParty<'a, T> {
    fn handle_reset(&mut self) -> Result<(), String> {
        RelayParty::handle_reset(self).map_err(|e| e.to_string())
    }
}

impl<'a, T: Transport> Handler for RelayParty<'a, T> {
    fn handle(&mut self, term: ClientTerm) -> ServerTerm {
        match term {
            ClientTerm::Version {
                max_message_size,
                protocol_version,
                lease_ms,
                orientations,
            } => {
                let supported = vec![Orientation::Rows];
                let agreed: Vec<Orientation> = orientations
                    .iter()
                    .copied()
                    .filter(|o| supported.contains(o))
                    .collect();
                if agreed.is_empty() {
                    ServerTerm::Error {
                        kind: ErrorKind::Connection,
                        identity: vec![],
                        message: b"no common orientation".to_vec(),
                    }
                } else {
                    ServerTerm::Version {
                        max_message_size,
                        protocol_version,
                        lease_ms,
                        orientations: agreed,
                    }
                }
            }

            ClientTerm::Query { text } => self.handle_query(text),

            ClientTerm::Fetch {
                handle,
                projection,
                count,
                orientation,
            } => self.handle_fetch(handle, projection, count, orientation),

            ClientTerm::Stat { handle } => self.handle_stat(handle),

            ClientTerm::Close { handle } => self.handle_close(handle),

            ClientTerm::Prepare { .. } => ServerTerm::Error {
                kind: ErrorKind::Permission,
                identity: vec![],
                message: b"Prepare not implemented".to_vec(),
            },

            ClientTerm::Offer { .. } => ServerTerm::Error {
                kind: ErrorKind::Permission,
                identity: vec![],
                message: b"Offer not implemented".to_vec(),
            },
        }
    }
}
