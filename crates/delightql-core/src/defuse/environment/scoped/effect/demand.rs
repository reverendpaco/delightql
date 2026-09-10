// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE DIRECTIVE HANDLERS (the eight-emission table): each takes a walked
//! source — the PURE remainder of the syntax the walk has already read —
//! resolves it in the walk's world, and asks the planner to store and emit
//! what it resolved to.

use super::walk::{
    collect_ground_names, descriptor_echo_values, dml_kind_name, effect_head_predicate_unsupported,
    make_pipe, named_ground_read, rename_ground_reads, require_whole_access, scratch_read,
    FuseOutcome,
};
use super::{EffectWalk, ReceiptNaming, WalkCtx};
use crate::diagnostic::EffectDdl;
use crate::error::{DelightQLError, Result};
use crate::names::DmlVerb;
use crate::pipeline::ast_unresolved::{Chain, Continuation, PipeOp, Relation};
use crate::pipeline::asts::core::operators::HoArgument;
use crate::pipeline::asts::core::Comparison;
use crate::pipeline::asts::core::{
    Access, DomainExpression, FunctorCall, GroundForm, ReductionPlan, Step,
};
use crate::pipeline::asts::effects;
use crate::pipeline::compiled_query::PlanCreatedObject;
use crate::pipeline::effect_transformer::{
    internal, precount_query, stamp_statement, unsupported, DeferredSql, MarkedStepKind,
    PendingPlanStatement, ReceiptGate, ReceiptShape,
};
use crate::pipeline::generator;
use crate::pipeline::sql_ast::{QueryExpression, SqlStatement};
use std::collections::HashSet;

impl<'p, 'a> EffectWalk<'p, 'a> {
    /// DML directive → today's DML machinery per statement +
    /// receipt insert IMMEDIATELY after (pinned by
    /// `receipt_insert_is_adjacent_to_its_dml`). The `!!` mutation-marker
    /// discipline is enforced by the resolver this statement routes
    /// through (resolver_fold.rs) — pinned red-first by the `dml_marker_*`
    /// tests in this module.
    pub(super) fn handle_dml(
        &mut self,
        walked_source: Chain,
        kind: DmlVerb,
        target: String,
        target_namespace: Option<String>,
        target_relation: Chain,
        callee: crate::pipeline::asts::vocabulary::Ref,
        access: Access,
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        // The lowering walker closes every recursive position where a
        // directive can hide: the DML
        // terminal's access spec is not on the lowered spine; a directive hidden
        // in a scalar subquery there would reach SQL unprocessed. Refuse it
        // honestly. Pinned at the constructible AST boundary by
        // `dml_access_spec_directive_refuses_at_lowering` and the collector test
        // `access_demands_directive_reaches_positional_scalar_subquery`.
        if effects::access_demands_directive(&access) {
            return Err(effect_head_predicate_unsupported(
                "a DML terminal's access specification",
            ));
        }
        // ENGINE OWNERSHIP: a
        // system-kind namespace is engine-owned — programs cannot mutate
        // its rows, refused at compile on this mutation path. Pinned by
        // directive_contract 42 (a forged effect_plan insert succeeds
        // without this check).
        self.plan
            .refuse_system_namespace_target(&target, target_namespace.as_deref(), "DML")?;
        // A self-referential mutation whose source
        // reads the target THROUGH a plan-created view materializes the
        // derived relation first (pinned by
        // `self_referential_dml_materializes_view_source`).
        let walked_source = if matches!(kind, DmlVerb::Update | DmlVerb::Delete) {
            self.materialize_hazardous_views(walked_source, &target, ctx)?
        } else {
            walked_source
        };

        let operation = format!("{}!", dml_kind_name(&kind));
        // The synthesized statement is the same relation-position call an
        // authored mutation normalizes to: [target, source] in the
        // descriptor's layout, no operator carrier.
        let dml_call = FunctorCall {
            callee,
            arguments: crate::pipeline::asts::core::operators::CallArguments::higher_order(vec![
                HoArgument::Relation(target_relation),
                HoArgument::Relation(walked_source),
            ]),
            marks: Default::default(),
        };
        let dml_expr = Chain::authored(GroundForm::Reference(Relation::FunctorCall {
            call: dml_call.into(),
            alias: None,
        }));
        let mut compiled = self.compile_statement(ctx, ctx.pure_query(dml_expr))?;
        let gates = self.gate_exprs(ctx, true)?;
        stamp_statement(&mut compiled.stmt, gates, &self.plan.names());
        let conn = self.plan.route(compiled.connection_id)?;

        // THE SOURCE IS STAGED FIRST, and the plan's trailing cleanup drops
        // it. Everything after this — the obligation and the mutation —
        // reads the staged relation, so the check and the write see one
        // set of rows rather than two evaluations of one definition.
        let prepare = std::mem::take(&mut compiled.prepare);
        if !prepare.is_empty() {
            let armed = self.plan.exit_armed();
            for statement in prepare {
                let sql = self.plan.finish_statement(&statement)?;
                self.plan
                    .comment_next(|| "stage the source, once".to_string());
                self.plan.emit_statement(sql, conn);
            }
            self.mark_step(
                MarkedStepKind::Stage,
                dml_kind_name(&kind),
                Some(ctx),
                armed,
            )?;
        }
        self.plan
            .retain_staged(std::mem::take(&mut compiled.staged));

        // WHAT THE MUTATION MAY NOT RUN WITHOUT. Each obligation is its own
        // check step, standing immediately before the
        // mutation it guards: the plan runs steps in order and a false
        // verdict aborts the run and rolls the bracket back, so the mutation
        // does not happen and the program is told. Folding the check into
        // the mutation's own WHERE could only have made it match no rows,
        // and a mutation that quietly does nothing is not a refusal.
        //
        // The same requirement edges the mutation carries are attached here,
        // so a step the plan declines to run does not have its obligation
        // checked either.
        for obligation in std::mem::take(&mut compiled.obligations) {
            let sql = self.plan.finish_statement(&obligation.statement)?;
            let armed = self.plan.exit_armed();
            self.plan
                .comment_next(|| "obligation: one source tuple per target row".to_string());
            self.plan.refuse_next_step(obligation.refusal);
            self.plan.emit_statement(sql, conn);
            self.mark_step(
                MarkedStepKind::Check,
                dml_kind_name(&kind),
                Some(ctx),
                armed,
            )?;
        }

        // The receipt: the core + the descriptor's declared `target` echo
        // (descriptor authority).
        let display_target = match &target_namespace {
            Some(ns) => format!("{}.{}", ns, target),
            None => target.clone(),
        };
        let shape = ReceiptShape {
            echoes: descriptor_echo_values(&operation, vec![display_target]),
            operation,
            scratch_name: format!("__r_{}", ctx.receipt_name),
        };
        let table = self.receipt_table_for(ctx, &shape)?;

        // The gate's FORM per dialect ("code chooses the form") —
        // see `ReceiptGate` for the three forms and their pins. The
        // SQLite arm is today's emission byte-identically.
        match self.plan.dialect() {
            generator::SqlDialect::PostgreSQL => {
                // The fused wCTE REPLACES the DML+receipt pair with ONE
                // statement: WITH <dml-cte> AS (<DML> RETURNING 1) <receipt>.
                let input = match &compiled.stmt {
                    SqlStatement::Delete { target_scope, .. }
                    | SqlStatement::Update { target_scope, .. }
                    | SqlStatement::Insert { target_scope, .. } => *target_scope,
                    _ => {
                        return Err(internal(
                            "a DML directive compiled to a non-DML statement".to_string(),
                        ))
                    }
                };
                let fused_scope = self.plan.names().cte_scope(
                    input,
                    crate::names::CteRole::Materialize,
                    crate::names::CteLabel::Exact(self.plan.names().intern("__dml", false)),
                );
                let dml_sql = self.plan.finish_statement(&compiled.stmt)?;
                let receipt_sql = self.build_receipt_insert_sql(
                    table.relation(),
                    &shape,
                    ReceiptGate::FusedDml(fused_scope),
                    ctx,
                )?;
                let fused = DeferredSql::concat([
                    DeferredSql::text("WITH "),
                    DeferredSql::Scope(fused_scope),
                    DeferredSql::text(" AS ("),
                    dml_sql,
                    DeferredSql::text(" RETURNING 1)\n"),
                    receipt_sql,
                ]);
                self.plan.emit_statement(fused, conn);
                self.mutation_epoch += 1;
            }
            generator::SqlDialect::DuckDB => {
                // The PRE-COUNT form: stage the DML's matched/source
                // cardinality into scratch IMMEDIATELY before the
                // mutation (same serial session and transaction —
                // load-bearing here), then gate the receipt on
                // it. The stage is built from the STAMPED statement, so
                // the count sees the same guards/exit gates the DML does.
                let aff_scope = self
                    .plan
                    .alloc_scratch(
                        crate::names::ScratchRole::Barrier,
                        "__aff",
                        &["c".to_string()],
                        None,
                    )?
                    .relation();
                let (with_clause, count_query) =
                    precount_query(&compiled.stmt, &self.plan.names(), aff_scope)?;
                let stage = SqlStatement::CreateTempTable {
                    table: aff_scope.scope(),
                    with_clause,
                    query: count_query,
                };
                let stage_sql = self.plan.finish_statement(&stage)?;
                let scratch_schema = self.plan.scratch_schema()?;
                // Adjacent drop-before-create for in-bracket scratch
                // (the replace treatment; see `splice_bound_input`):
                // an exit-taken prior run skips the trailing cleanup.
                self.plan.emit_commented(
                    DeferredSql::concat([
                        DeferredSql::text(format!("DROP TABLE IF EXISTS {}.", scratch_schema)),
                        DeferredSql::Scope(aff_scope.scope()),
                    ]),
                    conn,
                    None,
                );
                self.plan.emit_commented(
                    stage_sql,
                    conn,
                    Some(
                        "pre-count: the DML's matched cardinality, staged (R-T6 DuckDB form)"
                            .to_string(),
                    ),
                );
                let sql = self.plan.finish_statement(&compiled.stmt)?;
                self.plan.emit_statement(sql, conn);
                self.mutation_epoch += 1;
                self.emit_receipt_insert(
                    table.relation(),
                    &shape,
                    ReceiptGate::Precount(aff_scope),
                    ctx,
                )?;
            }
            _ => {
                // SQLite (canonical; also the unreachable mysql/sqlserver
                // families — no connection type maps to them today).
                let sql = self.plan.finish_statement(&compiled.stmt)?;
                self.plan.emit_statement(sql, conn);
                self.mutation_epoch += 1;
                // The changes() gate is connection state —
                // the receipt insert follows its DML immediately, nothing
                // between.
                self.emit_receipt_insert(table.relation(), &shape, ReceiptGate::Changes, ctx)?;
            }
        }
        Ok(scratch_read(table))
    }

    /// DDL directive → CTAS / CREATE VIEW + UNCONDITIONAL
    /// receipt insert; the created object's schema becomes
    /// a plan note so later statements resolve against it.
    /// `handle_ddl` with a namespace-qualified target designator: the
    /// namespace must route to the SAME connection the source routes to
    /// (connections are counted after resolution); a
    /// cross-connection placement refuses with a teaching diagnostic
    /// rather than creating somewhere surprising.
    pub(super) fn handle_ddl_namespaced(
        &mut self,
        walked_source: Chain,
        bare: &str,
        target: &str,
        target_namespace: Option<&str>,
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        // ENGINE OWNERSHIP: same refusal as DML —
        // a system-kind namespace is never a creation target.
        self.plan
            .refuse_system_namespace_target(target, target_namespace, "DDL")?;
        if let Some(ns) = target_namespace {
            let compiled = self.compile_statement(ctx, ctx.pure_query(walked_source.clone()))?;
            let source_conn = self.plan.route(compiled.connection_id)?;
            let ns_path = delightql_types::namespace::NamespacePath::from_fq_string(ns);
            let resolved = self
                .plan
                .system()
                .resolve_namespace_path(&ns_path)
                .map_err(|e| {
                    DelightQLError::from(EffectDdl::TargetNamespace {
                        message: format!("{bare}!'s target namespace '{ns}' does not resolve: {e}"),
                    })
                })?;
            let Some((_, ns_conn)) = resolved else {
                return Err(DelightQLError::from(EffectDdl::TargetNamespace {
                    message: format!("{bare}!'s target namespace '{ns}' is not a known namespace"),
                }));
            };
            if let Some(sc) = source_conn {
                let nc = ns_conn;
                if sc != nc {
                    return Err(DelightQLError::from(EffectDdl::TargetNamespace {
                        message: format!(
                            "{bare}!({ns}.{target}) refuses: the target namespace \
                             routes to a different connection than the source \
                             reads from — cross-connection placement is not \
                             supported (materialize-pipe §2)"
                        ),
                    }));
                }
            }
        }
        self.handle_ddl(walked_source, bare, target, ctx)
    }

    pub(super) fn handle_ddl(
        &mut self,
        walked_source: Chain,
        bare: &str,
        target: &str,
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        // THE BOOTSTRAP IS A SOURCE, NEVER A TARGET (materialization-law
        // §2): its reads are served as literal snapshots DURING resolution
        // — for every materializer and every target dialect — so
        // connection 1 is absent from the attribution set, a sys::-only
        // source reaches zero target connections and lands on primary, and
        // one user connection plus sys:: attributes to that user
        // connection with the sys rows carried in the compiled source.
        let compiled =
            self.compile_statement_with(ctx, ctx.pure_query(walked_source.clone()), true, None)?;
        // Route on the attribution: durable placement, the durable clash
        // universe, and the cross-kind holder probe are all keyed on the
        // statement's CONNECTION (counted after resolution).
        let conn = self.plan.route(compiled.connection_id)?;
        self.plan
            .system()
            .refuse_unregistrable_created_object(bare, target, conn)?;
        // Durable name clash REFUSES:
        // replacement of a durable is worn in the name — `table_replace!`
        // is the reserved spelling for that intent (the
        // imprint!/imprint_replace! precedent). The clash check is the
        // session catalog (this plan's own earlier creations + everything
        // resolution can reach bare in the connection's own namespace); an
        // object minted outside the catalog mid-session still surfaces as
        // the engine's own CREATE error. Temp creations REPLACE instead
        // (the adjacent DROP below). Pinned by the effects ball's
        // clash--55_durable_table_refused.
        if bare == "table" {
            let clash_ns = match conn {
                Some(c) => self
                    .plan
                    .system()
                    .connection_namespace_fq(c)?
                    .unwrap_or_else(|| "main".to_string()),
                None => "main".to_string(),
            };
            let clashes = self.plan.created_objects().iter().any(|o| o.name == target)
                || matches!(
                    self.plan
                        .system()
                        .resolve_unqualified_entity(target, &clash_ns, None),
                    Ok(Some(_))
                );
            if clashes {
                return Err(DelightQLError::from(EffectDdl::DurableClash {
                    message: format!(
                        "table!({0}) refuses: '{0}' already exists, and \
                         replacement of a durable object must be worn in the \
                         name (EFFECT-ALGEBRA §3) — table_replace! is \
                         reserved for that intent (§6)",
                        target
                    ),
                }));
            }
        }
        let source_query = match compiled.stmt {
            SqlStatement::Query { with_clause, query } => match with_clause {
                Some(ctes) => QueryExpression::WithCte {
                    ctes,
                    query: Box::new(query),
                },
                None => query,
            },
            _ => {
                return Err(unsupported(format!(
                    "the source of {}!({}) did not compile to a SELECT",
                    bare, target
                )))
            }
        };

        // CTAS and CREATE VIEW have no statement-level WHERE: placing the
        // predicate inside their SELECT would gate content, not creation.
        // The typed absence requirement therefore skips the complete DDL
        // step after exit on every engine.
        // THE CREATED OBJECT'S RELATION, derived with the heading the
        // statement that creates it emits. One derivation: the name the
        // CREATE renders and the note later statements resolve against are
        // the same relation, so there is no interface to grow afterwards.
        let target_scope = self.plan.create_object_relation(target, &compiled.ports)?;
        let sql = if bare == "table" {
            // sql_ast has no durable-CTAS variant, so `table!` renders
            // its SELECT through the ordinary chain and takes the CREATE
            // TABLE AS prefix as text — the same raw-DDL convention the
            // receipt shells use. Pinned by the effects ball's
            // ddl_receipt--13_table_ctas_read.
            let select_sql = self.plan.finish_statement(&SqlStatement::Query {
                with_clause: None,
                query: source_query,
            })?;
            // DURABLE PLACEMENT, per engine: the durable home is a compile-time fact of the
            // object's CONNECTION, never of engine session state.
            let durable_conn = conn.unwrap_or(2);
            match self.plan.dialect() {
                generator::SqlDialect::PostgreSQL => {
                    // A DQL namespace maps to exactly ONE engine schema,
                    // and the mount introspects one hardcoded schema
                    // (`public` — fatboy_exec.rs default_schema), so the
                    // CTAS spells the MOUNTED SCHEMA explicitly: zero
                    // current_schema()/search_path dependence (three
                    // silent breakages: empty path errors,
                    // pg_temp-first mints a silent temp, missing schemas
                    // skip). Unknowable schema → REFUSE, never an
                    // unqualified durable CTAS on PG. This
                    // refusal arm is DEFENSIVE: the only topology that
                    // reaches it (siso-typed postgres, connection_type 6)
                    // refuses earlier at route()'s latch (the
                    // siso refusal, pinned by
                    // `pg_table_bang_on_siso_connection_hits_the_siso_refusal_first`);
                    // it stays because the durable-placement invariant must
                    // hold even against topologies that don't exist yet.
                    // Pinned by
                    // `pg_table_bang_ctas_spells_the_mounted_schema_and_registers_on_the_connection`.
                    match self
                        .plan
                        .system()
                        .mounted_engine_schema_for_connection(durable_conn)?
                    {
                        Some(schema) => DeferredSql::concat([
                            DeferredSql::text(format!("CREATE TABLE {}.", schema)),
                            DeferredSql::Scope(target_scope.scope()),
                            DeferredSql::text(" AS "),
                            select_sql,
                        ]),
                        None => {
                            return Err(DelightQLError::from(EffectDdl::DurableSchemaUnknown {
                                message: format!(
                                    "table!({0}) refuses: connection {1}'s mounted schema is \
                                     unknowable, and a durable CREATE on postgres must spell \
                                     its schema explicitly — unqualified durable DDL is \
                                     search_path-fragile (R-T4; REPORT-T-P1 §E)",
                                    target, durable_conn
                                ),
                            }))
                        }
                    }
                }
                generator::SqlDialect::DuckDB => {
                    // The DuckDB backend opens the user file DIRECTLY
                    // (delightql-backends duckdb/connection.rs), so
                    // the unqualified CREATE lands in the opened file's
                    // catalog — which IS the durable home: abstention is
                    // CORRECT here, not a fallback. ATTACH-mounts do not
                    // exist through the fatboy today (fatboy_exec.rs: "No
                    // ATTACH semantics through the fatboy"); when they do,
                    // the recipe is alias recovery over DuckDB's
                    // SQLite-shaped PRAGMA database_list with PATH
                    // CANONICALIZATION of its as-opened paths — which
                    // `physical_schema_alias_for_namespace` already
                    // performs on both sides. Pinned by
                    // `duckdb_table_bang_on_the_direct_open_primary_stays_unqualified`.
                    DeferredSql::concat([
                        DeferredSql::text("CREATE TABLE "),
                        DeferredSql::Scope(target_scope.scope()),
                        DeferredSql::text(" AS "),
                        select_sql,
                    ])
                }
                _ => {
                    // SQLite (and the unreachable mysql/sqlserver arms),
                    // BYTE-IDENTICAL: the
                    // CLI's primary schema is ephemeral (`:memory:` with
                    // the user db ATTACHed under `_imported_N`), so the
                    // CREATE spells the PRAGMA-recovered backend alias of
                    // the connection the source reads from. No recoverable
                    // alias → abstain, unqualified — never a guessed prefix.
                    // Pinned by the CLI integration test
                    // `table_bang_persists_to_the_db_file_across_sessions`;
                    // the abstention by the lib test
                    // `durable_ctas_spells_unqualified_when_no_alias_is_recoverable`.
                    let alias = match self.plan.system().connection_namespace_fq(durable_conn)? {
                        Some(ns) => self
                            .plan
                            .system()
                            .physical_schema_alias_for_namespace(&ns, durable_conn)?,
                        None => None,
                    };
                    match alias {
                        Some(alias) => DeferredSql::concat([
                            DeferredSql::text(format!("CREATE TABLE {}.", alias)),
                            DeferredSql::Scope(target_scope.scope()),
                            DeferredSql::text(" AS "),
                            select_sql,
                        ]),
                        None => DeferredSql::concat([
                            DeferredSql::text("CREATE TABLE "),
                            DeferredSql::Scope(target_scope.scope()),
                            DeferredSql::text(" AS "),
                            select_sql,
                        ]),
                    }
                }
            }
        } else {
            let ddl_stmt = if bare == "temp_table" {
                SqlStatement::CreateTempTable {
                    table: target_scope.scope(),
                    with_clause: None,
                    query: source_query,
                }
            } else {
                SqlStatement::CreateTempView {
                    view: target_scope.scope(),
                    with_clause: None,
                    query: source_query,
                }
            };
            self.plan.finish_statement(&ddl_stmt)?
        };
        // Temp name clash REPLACES:
        // the DROP is adjacent to its CREATE, INSIDE the bracket, so an
        // abort's ROLLBACK restores the previous object (SQLite rolls back
        // temp DDL) and a script re-runs on one session without ceremony.
        // Two same-name creations in one plan = last
        // wins, deliberately (mention is instantiation). Replacement
        // is by NAME, not kind: when the catalog
        // knows the name is HELD by the other kind — this plan's own
        // earlier creation, or a prior run's registration — the holder's
        // kind-matched DROP is emitted first (SQLite refuses a wrong-kind
        // DROP even with IF EXISTS), then the directive's own kind DROP
        // (a no-op after the holder falls; keeps same-kind re-runs
        // covered). An object minted outside the catalog still surfaces
        // the engine's own error. Pinned by the lib
        // tests cross_kind_replace_*_in_plan (same-plan holder) and the
        // CLI tests temp_view_over_temp_table_replaces_the_table /
        // temp_table_over_temp_view_replaces_the_view (cross-plan holder);
        // same-kind replace by main--26_run_twice_temp_replace.
        // The temp qualifier is the `scratch.schema` dialect slot.
        if bare != "table" {
            let scratch_schema = self.plan.scratch_schema()?;
            let creating_view = bare == "temp_view";
            let holder_is_view = self
                .plan
                .created_objects()
                .iter()
                .rev()
                .find(|o| o.name == target)
                .map(|o| o.is_view)
                .or_else(|| {
                    self.plan
                        .system()
                        .session_created_object_kind(target, conn.unwrap_or(2))
                        .ok()
                        .flatten()
                });
            if let Some(holder_is_view) = holder_is_view {
                if holder_is_view != creating_view {
                    let holder_drop = DeferredSql::concat([
                        DeferredSql::text(format!(
                            "DROP {} IF EXISTS {}.",
                            if holder_is_view { "VIEW" } else { "TABLE" },
                            scratch_schema
                        )),
                        DeferredSql::Scope(target_scope.scope()),
                    ]);
                    self.plan.emit_ddl_action(
                        holder_drop,
                        conn,
                        Some("name clash: cross-kind holder drops first (§3)".to_string()),
                    );
                }
            }
            let drop_sql = DeferredSql::concat([
                DeferredSql::text(format!(
                    "DROP {} IF EXISTS {}.",
                    if creating_view { "VIEW" } else { "TABLE" },
                    scratch_schema
                )),
                DeferredSql::Scope(target_scope.scope()),
            ]);
            self.plan.emit_ddl_action(
                drop_sql,
                conn,
                Some("name clash: temp creations replace (§3)".to_string()),
            );
        }
        let create_comment = self.plan.take_comment();
        self.plan.emit_ddl_action(sql, conn, create_comment);
        // Surfaced as `CompiledPlan::created_objects` for the entry point's
        // post-run catalog registration (the created
        // object resolves bare for the rest of the session — pinned by
        // ddl_receipt--12/--13/--14 and util--36).
        let interior_positions = compiled
            .ports
            .iter()
            .enumerate()
            .filter(|(_, port)| self.plan.names().is_tree_valued(port.column()))
            .map(|(position, _)| position)
            .collect();
        self.plan.note_created_object(PlanCreatedObject {
            name: target.to_string(),
            is_view: bare == "temp_view",
            connection_id: conn,
            interior_positions,
        });

        if bare == "temp_view" {
            // The self-reference hazard map: which base tables this view reads.
            let mut bases = collect_ground_names(&walked_source);
            // A view over a view reads the inner view's bases too.
            let transitive: HashSet<String> = bases
                .iter()
                .flat_map(|b| self.view_bases.get(b).cloned().unwrap_or_default())
                .collect();
            bases.extend(transitive);
            self.view_bases.insert(target.to_string(), bases);
        } else {
            // A temp table materializes data: state changed.
            self.mutation_epoch += 1;
        }

        let shape = ReceiptShape {
            operation: format!("{}!", bare),
            echoes: descriptor_echo_values(bare, vec![target.to_string()]),
            scratch_name: match crate::pipeline::asts::effects::DirectiveKind::from_name(bare) {
                Some(crate::pipeline::asts::effects::DirectiveKind::TempView) => "__r_v",
                Some(
                    crate::pipeline::asts::effects::DirectiveKind::TempTable
                    | crate::pipeline::asts::effects::DirectiveKind::Table,
                ) => "__r_s",
                _ => "__r_main",
            }
            .to_string(),
        };
        let table = self.receipt_table_for(ctx, &shape)?;
        // Creation receipts are UNCONDITIONAL (no rowcount
        // gate — CTAS from an empty source still creates the object); the
        // exit guard still applies (oracle arm v!).
        self.emit_receipt_insert(table.relation(), &shape, ReceiptGate::Unconditional, ctx)?;
        Ok(scratch_read(table))
    }

    /// Emission 6: stdout! ships its input and passes it through. The pure
    /// prefix re-evaluates into the consumer statement — legal because the
    /// ship and the consumer are emitted adjacently, with no mutation
    /// between (invariant §5.8; pinned by
    /// `stdout_prefix_reevaluates_adjacently`).
    /// Wrap a walked relational value as an inline payload RECEIPT
    /// (EFFECT-ALGEBRA §3/§5): one row — `success`, `operation` —
    /// whose `returned` interior relation is the tree-grouped payload.
    /// Construction is the ordinary machinery a programmer could write:
    ///
    /// ```text
    /// payload ~> {*} as returned
    ///         |> +(1 as success, "op!" as operation)
    ///         |> (success, operation, returned)
    /// ```
    ///
    /// The whole-table aggregate yields exactly ONE row (an empty payload
    /// packages as the empty interior, which releases zero rows under the
    /// NULL-interior-is-empty law), and the tree-group construction is what
    /// makes `returned` a schema-known interior for drills, narrows, and
    /// `!>` in the SAME statement chain.
    /// The observed-payload fusion body: the
    /// tail directive of `source` is inspected for DESCRIPTOR-PROVEN
    /// payload provenance. `Input` without side effects fuses to pure
    /// substitution (the payload IS the piped relation); `Input` WITH
    /// side effects snapshots ONCE into plan scratch, runs the
    /// directive's OWN emission over the snapshot (its host action
    /// observes exactly the rows passed downstream — ship-once by
    /// construction), and continues from the snapshot; `OtherRelation`
    /// demands the piped input for its effects (the walk registers them;
    /// the value is discarded — the directive's existing sequencing
    /// semantics) and continues with the OTHER relation directly. The
    /// snapshot is a typed relational scratch table — native heading and
    /// values, NOT the prohibited JSON round trip. Anything else —
    /// produced payloads, user-authored interiors, unproven provenance —
    /// is handed back untouched for the general receipt semantics.
    pub(super) fn try_fuse_released_payload(
        &mut self,
        source: Chain,
        receipt: Access,
        ctx: &WalkCtx,
    ) -> Result<FuseOutcome> {
        use crate::pipeline::asts::effects::ReceiptPayload;
        // The builder substitutes every piped input into the call's table
        // argument, so the canonical release arrives as a bare
        // Relation::FunctorCall head with no continuations.
        let receipt = source.head_access().cloned().unwrap_or(receipt);
        let call = match source.head().form() {
            GroundForm::Reference(Relation::FunctorCall { call, .. }) if !source.has_steps() => {
                call.clone()
            }
            _ => return Ok(FuseOutcome::NotApplicable(source)),
        };
        let (first, second, extra) = {
            let mut tables = call.call().relations().cloned();
            (tables.next(), tables.next(), tables.next().is_some())
        };
        if extra || first.is_none() {
            return Ok(FuseOutcome::NotApplicable(source));
        }
        let (input, other_argument) = (second.clone().or(first.clone()), second.and(first));
        let input = input.expect("canonical call has an input table");
        let provenance = {
            effects::descriptor_for_reference(&call.call().callee)
                .map(|d| (d.receipt_payload, d.side_effects))
        };
        match provenance {
            Some((ReceiptPayload::Input, side_effects)) => {
                let walked = self.walk_value(input, &ctx.without_sink())?;
                if !side_effects {
                    return Ok(FuseOutcome::Fused(walked));
                }
                let snap = self.snapshot_relation(walked, ctx)?;
                let mut replay = call;
                replay
                    .call_mut()
                    .arguments
                    .replace_first_relation(scratch_read(snap));
                // A replayed effect step is executed for its receipt; the read
                // it stands for was already named where it stood.
                let _receipt = self.walk_functor_call(replay, None, receipt.clone(), ctx)?;
                Ok(FuseOutcome::Fused(scratch_read(snap)))
            }
            Some((ReceiptPayload::OtherRelation, _)) => {
                let name = call.call().callee.name_text();
                let access = receipt.clone();
                let argument = other_argument
                    .ok_or_else(|| internal("fusion: invocation has no other relation"))?;
                require_whole_access(&name, &access)?;
                let _ = self.walk_value(input, &ctx.without_sink())?;
                let fused = self.walk_value(argument, ctx)?;
                Ok(FuseOutcome::Fused(fused))
            }
            Some((ReceiptPayload::Assertion, _)) => {
                let property = call
                    .call()
                    .arguments
                    .ho_members()
                    .find_map(|member| match member {
                        HoArgument::Rule(rule) => Some(rule.clone()),
                        HoArgument::Relation(_)
                        | HoArgument::Landed(_)
                        | HoArgument::Value(_)
                        | HoArgument::Landing(_)
                        | HoArgument::Skip => None,
                    })
                    .or(other_argument)
                    .ok_or_else(|| internal("assert! fusion has no property value".to_string()))?;
                let values = call
                    .call()
                    .arguments
                    .ho_members()
                    .filter_map(|member| member.scalar_domain().cloned())
                    .chain(
                        call.call()
                            .arguments
                            .scalar_members()
                            .iter()
                            .filter_map(|member| member.scalar_domain().cloned()),
                    )
                    .collect::<Vec<_>>();
                let released =
                    self.walk_assert_terminal(input, property, &values, receipt, ctx, true)?;
                Ok(FuseOutcome::Fused(released))
            }
            _ => Ok(FuseOutcome::NotApplicable(source)),
        }
    }

    /// Materialize a walked relation ONCE into a typed plan-scratch table
    /// (the fusion snapshot: native heading and values). The DROP+CTAS
    /// land in the CURRENT step (`mark_step` spans from the previous
    /// mark), so a closed edge skips snapshot and consumer together.
    pub(super) fn snapshot_relation(
        &mut self,
        walked: Chain,
        ctx: &WalkCtx,
    ) -> Result<crate::relation::ScratchRow> {
        let compiled = self
            .compile_statement(ctx, ctx.pure_query(walked))
            .map_err(|e| {
                internal(format!(
                    "observed-payload snapshot failed to compile its source: {e}"
                ))
            })?;
        let snapshot = self.plan.alloc_scratch(
            crate::names::ScratchRole::Tee,
            "__tee_stdout",
            &[],
            Some(&compiled.relation),
        )?;
        let source_query = match compiled.stmt {
            SqlStatement::Query { query, .. } => query,
            _ => {
                return Err(internal(
                    "snapshot source did not compile to a SELECT".to_string(),
                ))
            }
        };
        let mut source_query = source_query;
        self.plan
            .stage_onto_scratch(&mut source_query, &compiled.columns, &snapshot.relation())?;
        let ctas = SqlStatement::CreateTempTable {
            table: snapshot.relation().scope(),
            with_clause: None,
            query: source_query,
        };
        let sql = self.plan.finish_statement(&ctas)?;
        let conn = self.plan.route(compiled.connection_id)?;
        let scratch_schema = self.plan.scratch_schema()?;
        self.plan.emit_commented(
            DeferredSql::concat([
                DeferredSql::text(format!("DROP TABLE IF EXISTS {}.", scratch_schema)),
                DeferredSql::Scope(snapshot.relation().scope()),
            ]),
            conn,
            None,
        );
        self.plan.emit_statement(sql, conn);
        Ok(snapshot)
    }

    /// THE CATALOG SNAPSHOT: execute a bootstrap-only source at plan build
    /// and answer with the literal SELECT its rows spell.
    ///
    /// The bootstrap is a SOURCE, never a target (materialization-law §2),
    /// and no engine connection reads another's tables — so a
    /// materialization over `sys::` reads the catalog here, where the
    /// catalog lives, and the created object carries the rows as the
    /// snapshot the directive already promises. The bootstrap's engine is
    /// SQLite whatever the plan's target is, so the source lowers and
    /// renders under the SQLite dialect.
    #[cfg(not(target_arch = "wasm32"))]

    pub(super) fn inline_payload_receipt(payload: Chain, operation: &str) -> Chain {
        use crate::pipeline::asts::core::literals::LiteralValue;
        use crate::pipeline::asts::core::specs::{GroupSpec, OneOut, OutItem, ReductionItem};

        use crate::pipeline::asts::core::{Enclyph, Record, RecordMember};
        use crate::pipeline::asts::core::{Glob, Spread};

        let record = DomainExpression::Application(
            crate::pipeline::asts::core::FunctionApplication::Enclyph(Enclyph::Record(
                Record::plain(crate::pipeline::asts::vocabulary::Vec1::new(
                    RecordMember::Spread(Spread::Glob(Glob::whole())),
                )),
            )),
        );
        let grouped = make_pipe(
            payload,
            PipeOp::Group(GroupSpec::Reduce {
                plan: ReductionPlan::empty(),
                keys: Vec::new(),
                reductions: crate::pipeline::asts::vocabulary::Vec1::new(ReductionItem::Out(
                    OutItem::One(OneOut::authored(record, Some("returned".into()))),
                )),
            }),
        );
        let widened = make_pipe(
            grouped,
            PipeOp::Project(
                crate::pipeline::asts::vocabulary::Vec1::try_from_vec(vec![
                    OutItem::Many(Spread::Glob(Glob::whole())),
                    OutItem::One(OneOut::authored(
                        DomainExpression::Application(
                            crate::pipeline::asts::core::FunctionApplication::Ground(
                                LiteralValue::Number("1".to_string()),
                            ),
                        ),
                        Some("success".into()),
                    )),
                    OutItem::One(OneOut::authored(
                        DomainExpression::Application(
                            crate::pipeline::asts::core::FunctionApplication::Ground(
                                LiteralValue::String(format!("{operation}!")),
                            ),
                        ),
                        Some("operation".into()),
                    )),
                ])
                .expect("the synthesized receipt projection is nonempty"),
            ),
        );
        make_pipe(
            widened,
            PipeOp::Project(
                crate::pipeline::asts::vocabulary::Vec1::try_from_vec(vec![
                    OutItem::one(OneOut::authored(
                        DomainExpression::lvar_builder("success".to_string()).build(),
                        None,
                    )),
                    OutItem::one(OneOut::authored(
                        DomainExpression::lvar_builder("operation".to_string()).build(),
                        None,
                    )),
                    OutItem::one(OneOut::authored(
                        DomainExpression::lvar_builder("returned".to_string()).build(),
                        None,
                    )),
                ])
                .expect("the synthesized receipt projection is nonempty"),
            ),
        )
    }

    pub(super) fn assert_receipt(witnesses: Chain, returned: Chain, label: String) -> Chain {
        use crate::pipeline::asts::core::literals::LiteralValue;
        use crate::pipeline::asts::core::specs::{GroupSpec, OneOut, OutItem, ReductionItem};
        use crate::pipeline::asts::core::{Enclyph, Glob, Record, RecordMember, Spread};

        let package = |payload: Chain, name: &'static str| {
            let record = DomainExpression::Application(
                crate::pipeline::asts::core::FunctionApplication::Enclyph(Enclyph::Record(
                    Record::plain(crate::pipeline::asts::vocabulary::Vec1::new(
                        RecordMember::Spread(Spread::Glob(Glob::whole())),
                    )),
                )),
            );
            make_pipe(
                payload,
                PipeOp::Group(GroupSpec::Reduce {
                    plan: ReductionPlan::empty(),
                    keys: Vec::new(),
                    reductions: crate::pipeline::asts::vocabulary::Vec1::new(ReductionItem::Out(
                        OutItem::One(OneOut::authored(record, Some(name.into()))),
                    )),
                }),
            )
        };
        let joined = package(witnesses, "witnesses").then(Step::authored(Continuation::Member {
            rhs: package(returned, "returned"),
            correlation: None,
            join_type: None,
        }));
        make_pipe(
            joined,
            PipeOp::Project(
                crate::pipeline::asts::vocabulary::Vec1::try_from_vec(vec![
                    OutItem::One(OneOut::authored(
                        DomainExpression::Application(
                            crate::pipeline::asts::core::FunctionApplication::Ground(
                                LiteralValue::Number("1".to_string()),
                            ),
                        ),
                        Some("success".into()),
                    )),
                    OutItem::One(OneOut::authored(
                        DomainExpression::Application(
                            crate::pipeline::asts::core::FunctionApplication::Ground(
                                LiteralValue::String("assert!".to_string()),
                            ),
                        ),
                        Some("operation".into()),
                    )),
                    OutItem::One(OneOut::authored(
                        DomainExpression::Application(
                            crate::pipeline::asts::core::FunctionApplication::Ground(
                                LiteralValue::String(label),
                            ),
                        ),
                        Some("label".into()),
                    )),
                    OutItem::One(OneOut::authored(
                        DomainExpression::lvar_builder("witnesses".to_string()).build(),
                        None,
                    )),
                    OutItem::One(OneOut::authored(
                        DomainExpression::lvar_builder("returned".to_string()).build(),
                        None,
                    )),
                ])
                .expect("the synthesized assert receipt projection is nonempty"),
            ),
        )
    }

    pub(super) fn handle_stdout(&mut self, walked_source: Chain, ctx: &WalkCtx) -> Result<Chain> {
        let text = self.compile_value_text(&walked_source, ctx)?;
        let gates = self.gate_exprs(ctx, false)?;
        let sql = self.plan.wrap_shipped_with_gates(text.sql, gates)?;
        let conn = self.plan.route(text.connection_id)?;
        let comment = self
            .plan
            .take_comment()
            .map(|c| format!("{} stdout!", c))
            .unwrap_or_else(|| "stdout!".to_string());
        self.plan.emit_shipped(sql, conn, Some(comment));
        // stdout!'s receipt packages its input — the payload
        // is what makes the generic unwrap (`!>`) tee-like for it.
        Ok(Self::inline_payload_receipt(walked_source, "stdout"))
    }

    /// exit! sets the flag; the demand context (left-conjunct
    /// guards, or the piped input) is the condition. From here on the
    /// walker stamps later DML with a `NOT EXISTS` check against the same
    /// scope and later shipped SELECTs with an outer guard.
    pub(super) fn handle_exit(&mut self, piped: Option<Chain>, ctx: &WalkCtx) -> Result<Chain> {
        self.plan.ensure_exit_shell()?;

        // Condition: every enclosing guard plus the piped input, as EXISTS
        // conjuncts on a one-row SELECT.
        let mut gates = self.gate_exprs(ctx, false)?;
        if let Some(p) = piped {
            gates.push(self.guard_to_sql(ctx, &self.guard_from_value(&p))?);
        }
        if self.plan.exit_armed() {
            gates.push(self.plan.exit_gate());
        }
        self.plan.emit_exit_latch(gates)?;

        // exit! never returns: its "receipt" table exists for the
        // ledger's NO-arm proxy row and is never written (oracle arm x!).
        let shape = ReceiptShape {
            operation: "exit!".to_string(),
            echoes: vec![],
            scratch_name: "__r_x".to_string(),
        };
        let table = self.receipt_table_for(ctx, &shape)?;
        Ok(scratch_read(table))
    }

    /// Fundamental abort: lower the exact input to a SELECT owned by this
    /// terminal step. The runner interprets only row presence, preserving any
    /// error raised while evaluating the SELECT and applying the typed abort
    /// disposition only after a row is actually observed.
    pub(super) fn handle_abort(
        &mut self,
        input: Chain,
        ctx: &WalkCtx,
    ) -> Result<(Chain, PendingPlanStatement)> {
        let probe = self.compile_value_text(&input, ctx)?;
        let connection_id = self.plan.route(probe.connection_id)?;
        let probe = PendingPlanStatement {
            sql: probe.sql,
            connection_id,
            comment: Some("abort probe".to_string()),
        };
        let shape = ReceiptShape {
            operation: "abort!".to_string(),
            echoes: vec![],
            scratch_name: "__r_abort".to_string(),
        };
        let table = self.receipt_table_for(ctx, &shape)?;
        Ok((scratch_read(table), probe))
    }

    pub(super) fn close_builtin_rule_value(
        &mut self,
        designator: &Chain,
        expected: &crate::pipeline::asts::core::definitions::ResidualSignature,
        evaluation_relation: crate::relation::ScratchRow,
        ctx: &WalkCtx,
    ) -> Result<crate::defuse::ho::RuleValueId> {
        let id = if self.plan.discovering() {
            let id = {
                let mut registry = self.plan.resolver_core()?;
                // The demand stands inside the body's own declared block, on
                // this very world: nothing needs re-declaring for the closing.
                ctx.world.close_rule_value(
                    &mut registry,
                    self.plan.config().clone(),
                    designator,
                    expected,
                    Some(evaluation_relation),
                )?
            };
            self.plan.record_builtin_rule_value(id);
            id
        } else {
            self.plan.replayed_builtin_rule_value()?
        };
        Ok(id)
    }

    pub(super) fn handle_assert(
        &mut self,
        property: Chain,
        input: Chain,
        label: String,
        ctx: &WalkCtx,
    ) -> Result<(Chain, Chain, PendingPlanStatement)> {
        use crate::pipeline::asts::core::definitions::{
            HeadItems, ResidualMode, ResidualSignature,
        };
        use crate::pipeline::asts::core::expressions::metadata_types::FilterOrigin;
        use crate::pipeline::asts::core::literals::LiteralValue;

        let input = self.stage_ho_input(input, ctx)?;
        let signature = ResidualSignature {
            remaining: vec![ResidualMode::Relation {
                name: "T".into(),
                cols: HeadItems::Glob,
            }],
            output: HeadItems::Glob,
        };
        let value = self.close_builtin_rule_value(&property, &signature, input, ctx)?;

        let formal: delightql_types::SqlIdentifier = "__assert_property".into();
        let application = Chain::read(
            Relation::FunctorCall {
                call: crate::pipeline::asts::core::FunctorCall::written(
                    crate::pipeline::asts::vocabulary::Ref::synthetic_with_display(
                        self.plan.names(),
                        crate::pipeline::asts::vocabulary::SyntheticReason::EffectReceipt,
                        formal.as_str(),
                    ),
                    vec![HoArgument::Relation(scratch_read(input))],
                )
                .into(),
                alias: None,
            },
            Access::All,
        );
        let witness =
            self.compile_rule_application(ctx, ctx.pure_query(application), formal, value)?;
        let witness = self
            .plan
            .stage_compiled_input(witness, "__assert_witness")?;

        let absent = scratch_read(witness)
            .then(Step::authored(Continuation::Structural(
                crate::pipeline::asts::core::StructuralStep {
                    form: crate::pipeline::asts::core::StructuralForm::Witness {
                        polarity: crate::pipeline::asts::core::Polarity::Positive,
                    },
                    named: Default::default(),
                },
            )))
            .then(Step::authored(Continuation::Restrict {
                condition: crate::pipeline::asts::core::TruthExpression::Comparison(Comparison {
                    operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
                    left: Box::new(DomainExpression::lvar_builder("met".to_string()).build()),
                    right: Box::new(DomainExpression::Application(
                        crate::pipeline::asts::core::FunctionApplication::Ground(
                            LiteralValue::Number("0".to_string()),
                        ),
                    )),
                }),
                origin: FilterOrigin::UserWritten,
            }));
        let (_, probe) = self.handle_abort(absent, ctx)?;
        let returned = scratch_read(input);
        Ok((
            Self::assert_receipt(scratch_read(witness), returned.clone(), label),
            returned,
            probe,
        ))
    }

    /// Materialize a PIPED rule input ONCE, at the demand site, in the
    /// demand site's own world: the input is a caller actual, and it
    /// crosses into the rule as a scratch read BY ITS RECEIPT.
    /// A later mutation cannot re-evaluate the pure prefix (invariant
    /// §5.8) because the snapshot precedes it by construction.
    pub(super) fn stage_ho_input(
        &mut self,
        walked: Chain,
        ctx: &WalkCtx,
    ) -> Result<crate::relation::ScratchRow> {
        let compiled = self.compile_statement(ctx, ctx.pure_query(walked))?;
        self.plan.stage_compiled_input(compiled, "__src_in")
    }

    /// Replace reads of plan-created VIEWS whose base
    /// set contains the mutation target with materialized snapshots.
    pub(super) fn materialize_hazardous_views(
        &mut self,
        source: Chain,
        target: &str,
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        let referenced = collect_ground_names(&source);
        let hazardous: Vec<String> = referenced
            .into_iter()
            .filter(|name| {
                self.view_bases
                    .get(name)
                    .is_some_and(|bases| bases.contains(target))
            })
            .collect();
        let mut rewritten = source;
        for view in hazardous {
            let compiled = self.compile_statement(ctx, ctx.pure_query(named_ground_read(&view)))?;
            let snapshot = self.plan.alloc_scratch(
                crate::names::ScratchRole::Snapshot,
                "__snap",
                &[],
                Some(&compiled.relation),
            )?;
            let source_query = match compiled.stmt {
                SqlStatement::Query { query, .. } => query,
                _ => {
                    return Err(internal(
                        "view read did not compile to a SELECT".to_string(),
                    ))
                }
            };
            let mut source_query = source_query;
            self.plan.stage_onto_scratch(
                &mut source_query,
                &compiled.columns,
                &snapshot.relation(),
            )?;
            let ctas = SqlStatement::CreateTempTable {
                table: snapshot.relation().scope(),
                with_clause: None,
                query: source_query,
            };
            let sql = self.plan.finish_statement(&ctas)?;
            let conn = self.plan.route(compiled.connection_id)?;
            // Adjacent drop-before-create for in-bracket scratch (the
            // replace treatment; see `splice_bound_input`).
            let scratch_schema = self.plan.scratch_schema()?;
            self.plan.emit_commented(
                DeferredSql::concat([
                    DeferredSql::text(format!("DROP TABLE IF EXISTS {}.", scratch_schema)),
                    DeferredSql::Scope(snapshot.relation().scope()),
                ]),
                conn,
                None,
            );
            self.plan.comment_next(|| {
                format!(
                    "materialized '{}' (invariant §5.4: self-referential DML \
                     reads its target through a derived relation)",
                    view
                )
            });
            self.plan.emit_statement(sql, conn);
            rewritten = rename_ground_reads(
                rewritten,
                crate::relation::NamedScratch::under(
                    snapshot,
                    delightql_types::SqlIdentifier::new(view),
                    ReceiptNaming(()),
                ),
            );
        }
        Ok(rewritten)
    }
}
