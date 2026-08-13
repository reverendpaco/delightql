// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `sys::execution.explain_run()` bin relation — D4's acceptance vehicle
//! (DOGFOODING-EFFECT-EXECUTION-PLAN §9 D4; Q-D4's owner-named consumer).
//!
//! Syntax: `sys::execution.explain_run("runfile.dql")`
//!
//! Consult the file (exactly as `run!` would: liminal directives execute
//! at load, rules register, an existing namespace reconsults), COMPILE its
//! `main!` into the typed effect plan, execute NOTHING of it, and RETURN
//! the plan's scheduled steps directly as the result — the ruled
//! observation contract. Heading:
//!
//! ```text
//! plan_id, step_id, ordinal, occurrence_id, step_kind, action_kind,
//! operation, route, sql_display, requires ⟦guard_id, polarity, reason,
//! guard_sql⟧
//! ```
//!
//! `requires` is the step's requirement edges as a schema-known interior
//! (the return-side convenience; the NORMALIZED effect_plan /
//! effect_guard / effect_requirement relations are the artifact's
//! persistent form and land with the materialization step). A step with
//! no edges carries the EMPTY interior (`[]`, via all-NULL contributor
//! elision) — `always` is the absence of a requirement row, structurally.

use crate::bin_cartridge::{
    BinEntity, EffectExecutable, EntityResult, EntitySignature, OutputSchema, Parameter,
};
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::expressions::relational::InnerRelationPattern;
use crate::pipeline::asts::core::literals::LiteralValue;
use crate::pipeline::asts::core::metadata::NamespacePath;
use crate::pipeline::asts::core::specs::{GroupSpec, OneOut, OutItem, ReductionItem};
use crate::pipeline::asts::core::FunctionApplication;
use crate::pipeline::asts::core::OutValue;
use crate::pipeline::asts::core::RecordMember;
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::compiled_query::{EffectStep, TypedEffectPlan};

pub struct ExplainRunPredicate;

impl BinEntity for ExplainRunPredicate {
    fn name(&self) -> &str {
        "explain_run"
    }

    fn namespace_override(&self) -> Option<&str> {
        // Same deliberate catalog identity as its sibling `compile`:
        // reachable ONLY qualified under sys::execution.
        Some("sys::execution")
    }

    fn entity_type(&self) -> EntityType {
        EntityType::BinRelation
    }

    fn signature(&self) -> EntitySignature {
        EntitySignature {
            parameters: vec![Parameter {
                name: "file_path".to_string(),
                data_type: "String".to_string(),
                _is_optional: false,
            }],
            output_schema: OutputSchema::Relation(vec![
                ("plan_id".to_string(), "Integer".to_string()),
                ("step_id".to_string(), "Integer".to_string()),
                ("ordinal".to_string(), "Integer".to_string()),
                ("occurrence_id".to_string(), "String".to_string()),
                ("step_kind".to_string(), "String".to_string()),
                ("action_kind".to_string(), "String".to_string()),
                ("operation".to_string(), "String".to_string()),
                ("route".to_string(), "Integer".to_string()),
                ("sql_display".to_string(), "String".to_string()),
                ("requires".to_string(), "Interior".to_string()),
            ]),
        }
    }

    fn has_side_effects(&self) -> bool {
        // Consulting the file shapes the session (exactly as run! does);
        // the PLAN itself executes nothing.
        true
    }

    fn as_effect_executable(&self) -> Option<&dyn EffectExecutable> {
        Some(self)
    }
}

impl EffectExecutable for ExplainRunPredicate {
    fn execute(
        &self,
        arguments: &[DomainExpression],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        if arguments.len() != 1 {
            return Err(DelightQLError::database_error(
                format!(
                    "sys::execution.explain_run() expects 1 argument (file_path), got {}",
                    arguments.len()
                ),
                "Invalid argument count",
            ));
        }
        let path = match &arguments[0] {
            DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(s))) => s.clone(),
            other => {
                return Err(DelightQLError::database_error(
                    format!("explain_run() file_path must be a string literal, got {other:?}"),
                    "Invalid argument type",
                ))
            }
        };

        // Consult-then-compile, mirroring run!'s consult_for_run: a fresh
        // namespace consults; an existing one reconsults (lib/scratch
        // reload; other kinds surface reconsult's own curated refusal).
        let namespace = namespace_from_path(&path);
        if let Err(consult_err) = super::consult::execute_consult(system, &path, &namespace, None) {
            system
                .reconsult_namespace(&namespace, Some(&path))
                .map_err(|_| consult_err)?;
        }

        let plan = crate::pipeline::effect_transformer::compile_namespace_main(system, &namespace)?;
        let typed = plan.typed.as_ref().ok_or_else(|| {
            DelightQLError::database_error(
                "explain_run: the compiled plan carries no typed layer",
                "internal invariant",
            )
        })?;

        // D4: the queryable artifact — populate the sys::execution
        // relations (clear-then-insert; explain's whole purpose is the
        // artifact, so a materialization failure surfaces).
        system.materialize_effect_plan(typed)?;

        Ok(EntityResult::Relation(build_explained_plan(typed, alias)))
    }
}

/// `run!`'s namespace convention (relay/entry.rs `namespace_from_path`),
/// mirrored for the Phase-1.X path.
fn namespace_from_path(path: &str) -> String {
    let stem = std::path::Path::new(path)
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    let sanitized: String = stem
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "script".to_string()
    } else {
        sanitized
    }
}

/// Build the explained plan as ordinary constructed relations: one row
/// per scheduled step (flat columns joined with a one-row `requires`
/// tree-group), unioned corresponding across steps, wrapped in an inner
/// relation so it splices where a Relation sits. The tree-group
/// construction is what makes `requires` a schema-known interior for
/// drills; a step with no edges contributes one all-NULL requirement row,
/// which the tree-group constructor ELIDES into the empty interior.
fn build_explained_plan(typed: &TypedEffectPlan, alias: Option<String>) -> Grelex {
    let pipe = |source: Chain, operator: PipeOp| {
        source.then(Continuation::Pipe {
            operator: operator,
            named: None,
            cpr_schema: (),
        })
    };

    let step_expr = |ordinal: usize, step: &EffectStep| -> Chain {
        let (step_kind, action_kind) = step.kind().projection_kinds();
        let s = |v: &str| DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(v.to_string()),));
        let n = |v: usize| DomainExpression::Application(FunctionApplication::Ground(LiteralValue::Number(v.to_string()),));
        let flat_headers: Vec<DomainExpression> = [
            "plan_id",
            "step_id",
            "ordinal",
            "occurrence_id",
            "step_kind",
            "action_kind",
            "operation",
            "route",
            "sql_display",
        ]
        .iter()
        .map(|h| DomainExpression::lvar_builder(h.to_string()).build())
        .collect();
        let sql_display = step.sql_display();
        let route = match step.route {
            Some(c) => DomainExpression::Application(FunctionApplication::Ground(LiteralValue::Number(c.to_string()),)),
            None => DomainExpression::Application(FunctionApplication::Ground(LiteralValue::Null,)),
        };
        let flat = AnonTable::from_values(
            Some(flat_headers),
            vec![vec![
                n(1),
                n(ordinal),
                n(ordinal),
                s(&step.occurrence),
                s(step_kind),
                s(action_kind),
                s(&step.operation),
                route,
                s(&sql_display),
            ]],
            (),
        )
        .expect("an explain step has one nonempty row");

        let req_headers: Vec<DomainExpression> = ["guard_id", "polarity", "reason", "guard_sql"]
            .iter()
            .map(|h| DomainExpression::lvar_builder(h.to_string()).build())
            .collect();
        let req_rows: Vec<Vec<DomainExpression>> = if step.requirements.is_empty() {
            // One all-NULL contributor row: the tree-group constructor
            // elides it into the empty interior `[]` — `always` is the
            // absence of edges, kept schema-known.
            vec![(0..4)
                .map(|_| DomainExpression::Application(FunctionApplication::Ground(LiteralValue::Null,)))
                .collect()]
        } else {
            step.requirements
                .iter()
                .map(|r| {
                    vec![
                        n(r.guard_id),
                        s(match r.polarity {
                            crate::pipeline::compiled_query::GuardPolarity::Present => "present",
                            crate::pipeline::compiled_query::GuardPolarity::Absent => "absent",
                        }),
                        s(r.reason),
                        s(&typed.guards[r.guard_id].sql),
                    ]
                })
                .collect()
        };
        let req_src = AnonTable::from_values(Some(req_headers), req_rows, ())
            .expect("an explain requirement source has a nonempty row");
        let grouped = pipe(
            Chain::ground(Grelex::Literal(AnonRelation::plain(req_src))),
            PipeOp::Group(GroupSpec::Reduce {
                    plan: ReductionPlan::empty(),
                    keys: Vec::new(),
                    reductions: crate::pipeline::asts::vocabulary::Vec1::new(ReductionItem::Out(OutItem::One(OneOut {
                        expr: OutValue::Domain(DomainExpression::Application(
                            FunctionApplication::Enclyph(
                                crate::pipeline::asts::core::Enclyph::Record(
                                    crate::pipeline::asts::core::Record::plain(
                                        crate::pipeline::asts::vocabulary::Vec1::new(RecordMember::Spread(
                                            crate::pipeline::asts::core::Spread::Glob(
                                                crate::pipeline::asts::core::Glob::whole(),
                                            ),
                                        )),
                                    ),
                                ),
                            ),
                        )),
                        naming: Some("requires".into()),
                        output: (),
                    }))),
                }),
        );
        Chain::ground(Grelex::Literal(AnonRelation::plain(flat))).then(Continuation::Member {
            rhs: grouped,
            correlation: None,
            join_type: None,
            cpr_schema: (),
        })
    };

    let arms: Vec<Chain> = typed
        .steps
        .iter()
        .enumerate()
        .map(|(i, step)| step_expr(i, step))
        .collect();
    let mut arms = arms.into_iter();
    let mut unioned = arms.next().expect("a run has at least one step");
    for arm in arms {
        unioned = unioned.bag_op(
            crate::pipeline::asts::core::expressions::SetOperator::UnionCorresponding,
            arm,
            (),
            (),
        );
    }

    let identifier = alias.as_deref().unwrap_or("explain_run");
    Grelex::Reference(Relation::InnerRelation {
        pattern: InnerRelationPattern::Indeterminate {
            identifier: crate::pipeline::asts::core::expressions::helpers::QualifiedName {
                namespace_path: NamespacePath::empty(),
                name: identifier.into(),
            },
            subquery: Box::new(unioned),
        },
        preminted_scope: None,
        alias: alias.map(Into::into),
        outer: false,
        cpr_schema: (),
    })
}
