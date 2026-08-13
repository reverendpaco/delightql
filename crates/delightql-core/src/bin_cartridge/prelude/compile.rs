// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `sys::execution.compile()` bin relation implementation
//!
//! Syntax: `sys::execution.compile("stage", """source""")`
//!
//! Returns a 1-row relation:
//! `(stage, query, representation, error, error_message)`
//!
//! On success, `representation` contains the compiled output and `error`
//! and `error_message` are NULL. On failure, `representation` is NULL,
//! `error` contains the error URI, and `error_message` the full prose —
//! the inspection surface must never know less than the execution
//! surface about why a compile failed (`error_message` is additive;
//! `error` stays URI-only for consumers that parse it).
//!
//! Stages: "cst", "ast-unresolved", "ast-resolved", "ast-refined", "sql"

use crate::bin_cartridge::{
    BinEntity, EffectExecutable, EntityResult, EntitySignature, OutputSchema, Parameter,
};
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::literals::LiteralValue;
use crate::pipeline::asts::core::Comparison;
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::Pipeline;

pub struct CompilePredicate;

impl BinEntity for CompilePredicate {
    fn name(&self) -> &str {
        "compile"
    }

    fn namespace_override(&self) -> Option<&str> {
        // Deliberate catalog identity: compile lives under sys::execution,
        // not std::prelude, and is therefore reachable ONLY qualified —
        // never through universal unqualified visibility.
        Some("sys::execution")
    }

    fn entity_type(&self) -> EntityType {
        EntityType::BinRelation
    }

    fn signature(&self) -> EntitySignature {
        EntitySignature {
            parameters: vec![
                Parameter {
                    name: "stage".to_string(),
                    data_type: "String".to_string(),
                    _is_optional: false,
                },
                Parameter {
                    name: "source".to_string(),
                    data_type: "String".to_string(),
                    _is_optional: false,
                },
            ],
            output_schema: OutputSchema::Relation(vec![
                ("stage".to_string(), "String".to_string()),
                ("query".to_string(), "String".to_string()),
                ("representation".to_string(), "String".to_string()),
                ("error".to_string(), "String".to_string()),
                ("error_message".to_string(), "String".to_string()),
            ]),
        }
    }

    fn has_side_effects(&self) -> bool {
        false
    }

    fn as_effect_executable(&self) -> Option<&dyn EffectExecutable> {
        Some(self)
    }
}

impl EffectExecutable for CompilePredicate {
    fn execute(
        &self,
        arguments: &[DomainExpression],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        if arguments.len() != 2 {
            return Err(DelightQLError::database_error(
                format!(
                    "sys::execution.compile() expects 2 arguments (stage, source), got {}",
                    arguments.len()
                ),
                "Invalid argument count",
            ));
        }

        let stage = extract_string_literal(&arguments[0], "stage")?;
        let source = extract_string_literal(&arguments[1], "source")?;

        let (representation, error) = match compile_to_stage(system, &stage, &source) {
            Ok(repr) => (Some(repr), None),
            Err(e) => (None, Some((e.error_uri(), e.to_string()))),
        };

        let relation = build_compile_result(&stage, &source, representation, error, alias);
        Ok(EntityResult::Relation(relation))
    }

    /// compile is a PURE elementwise inspection — receiving the
    /// lifted (stage, source) relation whole and mapping it row by row IS
    /// its setwise semantics (one call, one combined result relation; no
    /// effect executes, so per-element application raises no at-most-once
    /// question). This preserves the pinned multi-row pipe contract
    /// (sef_structures bin_compile--08/--10/… compile every row).
    fn execute_lifted(
        &self,
        rows: &[Vec<DomainExpression>],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        // An EMPTY lift still reaches compile once — zero elements map to
        // the empty result WITH the declared heading (a one-NULL-row
        // source filtered false, the lowerable spelling of an empty
        // relation).
        if rows.is_empty() {
            let headers: Vec<DomainExpression> =
                ["stage", "query", "representation", "error", "error_message"]
                    .iter()
                    .map(|h| DomainExpression::lvar_builder(h.to_string()).build())
                    .collect();
            let null_row = (0..5)
                .map(|_| {
                    DomainExpression::Application(FunctionApplication::Ground(LiteralValue::Null))
                })
                .collect();
            let source = AnonRelation::plain(
                AnonTable::from_values(Some(headers), vec![null_row], ())
                    .expect("compile's empty relation has a heading and a row"),
            );
            let lit = |n: &str| {
                Box::new(DomainExpression::Application(FunctionApplication::Ground(
                    LiteralValue::Number(n.to_string()),
                )))
            };
            let empty = Chain::ground(Grelex::Literal(source)).then(Continuation::Restrict {
                condition: TruthExpression::Comparison(Comparison {
                    operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
                    left: lit("1"),
                    right: lit("0"),
                }),
                origin: crate::pipeline::asts::core::FilterOrigin::default(),
                cpr_schema: (),
            });
            let identifier = alias.as_deref().unwrap_or("compile");
            return Ok(EntityResult::Relation(Grelex::Reference(Relation::InnerRelation {
                pattern:
                    crate::pipeline::asts::core::expressions::relational::InnerRelationPattern::Indeterminate {
                        identifier:
                            crate::pipeline::asts::core::expressions::helpers::QualifiedName {
                                namespace_path:
                                    crate::pipeline::asts::core::metadata::NamespacePath::empty(),
                                name: identifier.into(),
                            },
                        subquery: Box::new(empty),
                    },
                preminted_scope: None,
                alias: alias.map(Into::into),
                outer: false,
                cpr_schema: (),
            })));
        }
        let mut all_rows = Vec::new();
        let mut headers = None;
        for row in rows {
            let EntityResult::Relation(head) = self.execute(row, alias.clone(), system)?;
            if let Grelex::Literal(AnonRelation { table, .. }) = head {
                if headers.is_none() {
                    headers = table.body.header;
                }
                all_rows.extend(table.body.rows.into_vec());
            }
        }
        Ok(EntityResult::Relation(Grelex::Literal(AnonRelation {
            table: AnonTable {
                body: TabularBody {
                    header: headers,
                    rows: crate::pipeline::asts::vocabulary::Vec1::try_from_vec(all_rows)
                        .expect("a nonempty lift produces a row per input"),
                },
                cpr_schema: (),
            },
            alias: alias.map(|s| s.into()),
            outer: false,
        })))
    }
}

fn compile_to_stage(
    system: &mut crate::system::DelightQLSystem,
    stage: &str,
    source: &str,
) -> Result<String> {
    // Compile purity: rendering "cst" or "ast-unresolved" never enters
    // the effect executor, so any source may be inspected shallowly. Every deeper
    // stage would run the effect executor
    // (and inline-DDL processing) against the SHARED system — compiling a
    // consult!/run!/enlist! must not consult, run, or enlist. Walk the
    // unresolved AST first and refuse executing demands cleanly; the
    // refusal surfaces through compile's ordinary error columns.
    let registry = system.bin_registry();
    let mut pipeline = Pipeline::new(source, system);
    if !matches!(stage, "cst" | "ast-unresolved") {
        pipeline.execute_to_query_unresolved()?;
        if pipeline.has_inline_ddl_blocks() {
            return Err(DelightQLError::validation_error_categorized(
                "effect/compile/purity",
                format!(
                    "sys::execution.compile is pure: compiling to stage '{stage}' \
                     would process an inline (~~ddl ~~) block, which registers \
                     namespaces and entities in the session. Compile to 'cst' or \
                     'ast-unresolved' to inspect this source, or run it as a query \
                     to execute it."
                ),
                "compile purity",
            ));
        }
        let query = pipeline
            .query_unresolved()
            .expect("execute_to_query_unresolved populates the unresolved query");
        crate::pipeline::effect_executor::refuse_executing_demands_for_inspection(
            query, &registry, stage,
        )?;
    }
    pipeline.render_stage(stage)
}

fn extract_string_literal(expr: &DomainExpression, arg_name: &str) -> Result<String> {
    match expr {
        DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(s))) => {
            Ok(s.clone())
        }
        _ => Err(DelightQLError::database_error(
            format!(
                "sys::execution.compile() {} must be a string literal",
                arg_name
            ),
            "Invalid argument type",
        )),
    }
}

fn string_literal(val: &str) -> DomainExpression {
    DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(
        val.to_string(),
    )))
}

fn null_literal() -> DomainExpression {
    DomainExpression::Application(FunctionApplication::Ground(LiteralValue::Null))
}

fn build_compile_result(
    stage: &str,
    query: &str,
    representation: Option<String>,
    error: Option<(String, String)>,
    alias: Option<String>,
) -> Grelex {
    let headers = vec![
        DomainExpression::lvar_builder("stage".to_string()).build(),
        DomainExpression::lvar_builder("query".to_string()).build(),
        DomainExpression::lvar_builder("representation".to_string()).build(),
        DomainExpression::lvar_builder("error".to_string()).build(),
        DomainExpression::lvar_builder("error_message".to_string()).build(),
    ];
    let row = vec![
        string_literal(stage),
        string_literal(query),
        match &representation {
            Some(repr) => string_literal(repr),
            None => null_literal(),
        },
        match &error {
            Some((uri, _)) => string_literal(uri),
            None => null_literal(),
        },
        match &error {
            Some((_, message)) => string_literal(message),
            None => null_literal(),
        },
    ];
    Grelex::Literal(AnonRelation {
        table: AnonTable::from_values(Some(headers), vec![row], ())
            .expect("compile publishes one nonempty row"),
        alias: alias.map(|s| s.into()),
        outer: false,
    })
}

#[cfg(test)]
mod purity_tests {
    //! Compile purity: inspecting source must never enter the effect executor
    //! execution against the shared system. Executing demands refuse
    //! cleanly at deep stages, stay inspectable at shallow stages, and
    //! DML remains compilable because it lowers to SQL without effect
    //! execution.

    use super::*;
    use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
    use delightql_types::test_utils::MockDatabaseConnection;
    use std::sync::{Arc, Mutex};

    struct EmptyIntrospector;
    impl DatabaseIntrospector for EmptyIntrospector {
        fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
        fn introspect_entities_in_schema(
            &self,
            _schema: &str,
        ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }

    fn fresh_system() -> crate::system::DelightQLSystem {
        let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        crate::system::DelightQLSystem::new(conn, Box::new(EmptyIntrospector), "sqlite")
            .expect("fresh in-memory system should build")
    }

    #[test]
    fn deep_stage_refuses_session_directive_demand() {
        let mut system = fresh_system();
        let err = compile_to_stage(&mut system, "sql", r#"enlist!("std::string")"#)
            .expect_err("compiling a session directive to sql must refuse");
        let msg = err.to_string();
        assert!(msg.contains("compile is pure"), "{msg}");
        assert!(msg.contains("enlist"), "{msg}");
    }

    #[test]
    fn deep_stage_refuses_execution_directive_demand() {
        let mut system = fresh_system();
        let err = compile_to_stage(&mut system, "ast-resolved", r#"run!("f.dql")(*)"#)
            .expect_err("compiling run! to a deep stage must refuse");
        assert!(err.to_string().contains("compile is pure"), "{err}");
    }

    #[test]
    fn shallow_stage_inspects_the_same_source() {
        let mut system = fresh_system();
        let repr = compile_to_stage(&mut system, "ast-unresolved", r#"enlist!("std::string")"#)
            .expect("ast-unresolved never enters the effect executor");
        assert!(repr.contains("enlist"), "{repr}");
    }

    /// EXECUTION IS JUDGED BY THE COMPILATION THAT CAUSED IT.
    ///
    /// Through the PRODUCTION road: a DQL query pipes a source into the
    /// runtime-served relation, the effect executor reaches it, and the body
    /// it is handed is compiled inside the outer compilation. Policy moves
    /// after the outer arena is armed and before the query runs, so a nested
    /// arena that re-read policy instead of inheriting would answer with the
    /// moved number and this would see it.
    ///
    /// The probe is SHALLOW on purpose: the armed depth is small and the body
    /// is just past it, so nothing here walks near either real ceiling.
    #[test]
    fn the_served_compilation_inherits_the_causing_one_s_budget() {
        use crate::compiler_limits::{ProcessLimitLease, NESTING};

        const ARMED: usize = 20;
        const MOVED_TO: usize = 900;
        let depth = ARMED * 2;

        let _lease = ProcessLimitLease::take();
        NESTING.set(ARMED);

        let body = format!(
            "_(x @ 1) |> ({}x{} as v)",
            "(".repeat(depth),
            ")".repeat(depth)
        );
        let query = format!(r#"_("sql", "{body}") |> sys::execution.compile(*)"#);

        let mut system = fresh_system();
        // ARMED here, by the pipeline the query belongs to.
        let mut pipeline = Pipeline::new(&query, &mut system);
        // The host moves policy while that compilation is in flight.
        NESTING.set(MOVED_TO);

        let sql = pipeline
            .execute_to_sql()
            .expect("compile reports the inner refusal in its own columns")
            .to_string();

        assert!(
            sql.contains("operational/resource/nesting"),
            "the served body must refuse on depth: {sql}"
        );
        assert!(
            sql.contains(&format!("budget is {ARMED}")),
            "the served compilation must answer to its caller's arming: {sql}"
        );
        assert!(
            !sql.contains(&format!("budget is {MOVED_TO}")),
            "and not to the policy that moved under it: {sql}"
        );
    }
}
