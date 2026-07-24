// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Prelude Cartridge - Built-in pseudo-predicates
//!
//! This cartridge provides the core pseudo-predicates that are available
//! by default in all DelightQL sessions:
//!
//! - `mount!(db_path, namespace)` - Mount a database
//! - `enlist!(namespace)` - Enable unqualified access to namespace entities
//! - `delist!(namespace)` - Remove unqualified access to namespace entities
//! - `run!(file_path)` - Execute queries from a file
//!
//! The prelude cartridge is registered in the `std::prelude` namespace
//! and is marked as universal, making these predicates available everywhere
//! without needing an explicit `enlist!()`.

mod alias;
mod compile;
pub(crate) mod consult;
mod consult_tree;
mod delist;
mod doc;
mod enlist;
mod explain_run;
mod ground;
mod imprint;
mod mount;
mod mount_new;
mod mount_tree;
mod reconsult;
mod refresh;
mod run;
mod unconsult;
mod unmount;

pub use alias::AliasPredicate;
pub use compile::CompilePredicate;
pub use consult::{ConsultConcatPredicate, ConsultPredicate};
pub use consult_tree::ConsultTreePredicate;
pub use delist::DelistPredicate;
pub use doc::DocPredicate;
pub use enlist::EnlistPredicate;
pub use explain_run::ExplainRunPredicate;
pub use ground::GroundPredicate;
pub use imprint::{ImprintPredicate, ImprintReplacePredicate};
pub use mount::MountPredicate;
pub use mount_new::MountNewPredicate;
pub use mount_tree::MountTreePredicate;
pub use reconsult::ReconsultPredicate;
pub use refresh::RefreshPredicate;
pub use run::{RunNamespacePredicate, RunPredicate};
pub use unconsult::UnconsultPredicate;
pub use unmount::UnmountPredicate;

use super::{BinCartridge, BinCartridgeMetadata, BinEntity};
use crate::enums::Language;
use crate::pipeline::asts::unresolved::*;
use std::sync::Arc;

/// The descriptor-declared receipt heading for a built-in directive
/// (Phase 6 slice 2 — descriptor authority): entities' `output_schema`
/// calls THIS instead of hand-copying columns, so the descriptor is the
/// source and there is no second authority to drift.
pub(crate) fn descriptor_receipt_schema(bare_name: &str) -> Vec<(String, String)> {
    crate::pipeline::asts::effects::descriptor(bare_name)
        .unwrap_or_else(|| panic!("no directive descriptor for '{bare_name}'"))
        .receipt_columns()
}

/// Build a directive's CORE receipt FROM its descriptor (Phase 6 slice
/// 2): the echo column NAMES come from the descriptor's declared
/// `receipt_echoes`; the caller supplies only the VALUES, in ledger
/// order. An arity mismatch is an internal invariant violation — the
/// entity and its descriptor disagree — and panics rather than shipping
/// a receipt the declared heading disowns.
pub(crate) fn descriptor_core_receipt(
    bare_name: &str,
    values: &[Option<String>],
    alias: Option<String>,
) -> Relation {
    let desc = crate::pipeline::asts::effects::descriptor(bare_name)
        .unwrap_or_else(|| panic!("no directive descriptor for '{bare_name}'"));
    assert_eq!(
        desc.receipt_echoes.len(),
        values.len(),
        "'{bare_name}!': echo values disagree with the descriptor's declared echoes"
    );
    let echoes: Vec<(&str, Option<String>)> = desc
        .receipt_echoes
        .iter()
        .zip(values)
        .map(|(e, v)| (e.name, v.clone()))
        .collect();
    core_receipt_result(&format!("{bare_name}!"), &echoes, alias)
}

/// A CORE receipt with flat scalar echoes (EFFECT-ALGEBRA §3): one row —
/// `(success, operation, <declared echo columns>)`. The echoes are the
/// directive's §8 additions; an optional echo is present with NULL.
/// Reach it through `descriptor_core_receipt` — direct calls re-state
/// echo names the descriptor already declares.
fn core_receipt_result(
    operation: &str,
    echoes: &[(&str, Option<String>)],
    alias: Option<String>,
) -> Relation {
    let mut headers = vec![
        DomainExpression::lvar_builder("success".to_string()).build(),
        DomainExpression::lvar_builder("operation".to_string()).build(),
    ];
    headers.extend(
        echoes
            .iter()
            .map(|(name, _)| DomainExpression::lvar_builder(name.to_string()).build()),
    );
    let mut values = vec![
        DomainExpression::Literal {
            value: LiteralValue::Number("1".to_string()),
            alias: None,
        },
        DomainExpression::Literal {
            value: LiteralValue::String(operation.to_string()),
            alias: None,
        },
    ];
    values.extend(echoes.iter().map(|(_, v)| DomainExpression::Literal {
        value: match v {
            Some(s) => LiteralValue::String(s.clone()),
            None => LiteralValue::Null,
        },
        alias: None,
    }));
    Relation::Anonymous {
        column_headers: Some(headers),
        rows: vec![Row { values }],
        alias: alias.map(|s| s.into()),
        outer: false,
        exists_mode: false,
            negated: false,
        qua_target: None,
        cpr_schema: PhaseBox::phantom(),
    }
}

/// A receipt with interior DECLARED ADDITIONS (EFFECT-ALGEBRA §3):
/// one row `(success, operation, input, returned)`
/// where `input` is the faithful interior echo of the lifted argument
/// table and `returned` carries the directive's produced result.
///
/// The receipt is CONSTRUCTED through the ordinary operators — two
/// tree-groups (`~> {*} as input` / `as returned`) joined, widened with
/// the guaranteed core, reordered — exactly what a programmer could
/// write, so the resolver derives the interior schemas and
/// drill/narrow/brace release work with no receipt-special machinery.
/// The whole construction is wrapped in an aliased inner relation so it
/// splices wherever a Relation sits (the Phase-1.X convention).
pub(crate) fn interior_receipt_result(
    operation: &str,
    input_heading: &[&str],
    input_rows: &[Vec<Option<String>>],
    returned_heading: &[&str],
    returned_rows: &[Vec<Option<String>>],
    alias: Option<String>,
) -> Relation {
    receipt_with_interiors(
        operation,
        &[],
        &[
            ("input", input_heading, input_rows),
            ("returned", returned_heading, returned_rows),
        ],
        alias,
    )
}

/// A descriptor-driven receipt with flat echoes AND a `returned`
/// tree-group payload (Phase 6 slice 3 — the tree directives): echo
/// NAMES come from the descriptor's declared `receipt_echoes`; the
/// caller supplies echo VALUES in ledger order plus the payload's
/// heading and rows (one interior row per member of the produced
/// collection — an all-NULL row elides to the empty `[]`).
pub(crate) fn descriptor_tree_receipt(
    bare_name: &str,
    echo_values: &[Option<String>],
    returned_heading: &[&str],
    returned_rows: &[Vec<Option<String>>],
    alias: Option<String>,
) -> Relation {
    let desc = crate::pipeline::asts::effects::descriptor(bare_name)
        .unwrap_or_else(|| panic!("no directive descriptor for '{bare_name}'"));
    assert!(
        desc.receipt_payload != crate::pipeline::asts::effects::ReceiptPayload::None,
        "'{bare_name}!': a tree receipt requires a declared `returned` payload"
    );
    assert_eq!(
        desc.receipt_echoes.len(),
        echo_values.len(),
        "'{bare_name}!': echo values disagree with the descriptor's declared echoes"
    );
    let echoes: Vec<(&str, Option<String>)> = desc
        .receipt_echoes
        .iter()
        .zip(echo_values)
        .map(|(e, v)| (e.name, v.clone()))
        .collect();
    receipt_with_interiors(
        &format!("{bare_name}!"),
        &echoes,
        &[("returned", returned_heading, returned_rows)],
        alias,
    )
}

/// A receipt whose only declared addition is the interior `input` echo
/// (EFFECT-ALGEBRA §3): `(success, operation, input)` — the shape of a
/// payload-free directive that takes a lifted argument relation (doc!).
pub(crate) fn input_receipt_result(
    operation: &str,
    input_heading: &[&str],
    input_rows: &[Vec<Option<String>>],
    alias: Option<String>,
) -> Relation {
    receipt_with_interiors(
        operation,
        &[],
        &[("input", input_heading, input_rows)],
        alias,
    )
}

fn receipt_with_interiors(
    operation: &str,
    echoes: &[(&str, Option<String>)],
    interiors: &[(&str, &[&str], &[Vec<Option<String>>])],
    alias: Option<String>,
) -> Relation {
    use crate::pipeline::asts::core::expressions::domain::ProjectionExpr;
    use crate::pipeline::asts::core::expressions::functions::CurlyMember;
    use crate::pipeline::asts::core::expressions::relational::InnerRelationPattern;
    use crate::pipeline::asts::core::expressions::PipeExpression;
    use crate::pipeline::asts::core::metadata::NamespacePath;
    use crate::pipeline::asts::core::specs::{ModuloSpec, OutputDomainExpression};
    use crate::pipeline::asts::core::ContainmentSemantic;
    use crate::pipeline::asts::core::FunctionExpression;

    let anon = |heading: &[&str], rows: &[Vec<Option<String>>]| {
        Relation::Anonymous {
            column_headers: Some(
                heading
                    .iter()
                    .map(|h| DomainExpression::lvar_builder(h.to_string()).build())
                    .collect(),
            ),
            rows: rows
                .iter()
                .map(|vals| Row {
                    values: vals
                        .iter()
                        .map(|v| DomainExpression::Literal {
                            value: match v {
                                Some(s) => LiteralValue::String(s.clone()),
                                // An all-NULL contributor row elides to the
                                // empty interior `[]` (finding 1: an empty
                                // lift still reaches the callee once).
                                None => LiteralValue::Null,
                            },
                            alias: None,
                        })
                        .collect(),
                })
                .collect(),
            alias: None,
            outer: false,
            exists_mode: false,
            negated: false,
            qua_target: None,
            cpr_schema: PhaseBox::phantom(),
        }
    };
    let pipe = |source: RelationalExpression, operator: UnaryRelationalOperator| {
        RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(PipeExpression {
            source,
            operator,
            cpr_schema: PhaseBox::phantom(),
        })))
    };
    let grouped = |source: Relation, interior_name: &str| {
        pipe(
            RelationalExpression::Relation(source),
            UnaryRelationalOperator::Modulo {
                containment_semantic: ContainmentSemantic::Parenthesis,
                spec: ModuloSpec::GroupBy {
                    reducing_by: Vec::new(),
                    reducing_on: vec![OutputDomainExpression {
                        expr: DomainExpression::Function(FunctionExpression::Curly {
                            members: vec![CurlyMember::Glob],
                            inner_grouping_keys: Vec::new(),
                            cte_requirements: None,
                            alias: Some(interior_name.into()),
                        }),
                        output: PhaseBox::phantom(),
                    }],
                    delegates: Vec::new(),
                },
            },
        )
    };

    // One-row groups (one per declared interior) cross-join left to right.
    let mut groups = interiors
        .iter()
        .map(|(col, heading, rows)| grouped(anon(heading, rows), col));
    let mut joined = groups.next().expect("at least one declared interior");
    for right in groups {
        joined = RelationalExpression::Join {
            left: Box::new(joined),
            right: Box::new(right),
            join_condition: None,
            join_type: None,
            cpr_schema: PhaseBox::phantom(),
        };
    }
    let mut widening = vec![
        DomainExpression::Projection(ProjectionExpr::Glob {
            qualifier: None,
            namespace_path: NamespacePath::empty(),
        }),
        DomainExpression::Literal {
            value: LiteralValue::Number("1".to_string()),
            alias: Some("success".into()),
        },
        DomainExpression::Literal {
            value: LiteralValue::String(operation.to_string()),
            alias: Some("operation".into()),
        },
    ];
    // Flat echoes widen exactly like the core (an optional echo is a
    // NULL literal), so the ledger order below can place them between
    // the core and the interiors.
    widening.extend(echoes.iter().map(|(name, v)| DomainExpression::Literal {
        value: match v {
            Some(s) => LiteralValue::String(s.clone()),
            None => LiteralValue::Null,
        },
        alias: Some((*name).into()),
    }));
    let widened = pipe(
        joined,
        UnaryRelationalOperator::General {
            containment_semantic: ContainmentSemantic::Parenthesis,
            expressions: widening,
        },
    );
    let order: Vec<&str> = ["success", "operation"]
        .into_iter()
        .chain(echoes.iter().map(|(name, _)| *name))
        .chain(interiors.iter().map(|(col, _, _)| *col))
        .collect();
    let ordered = pipe(
        widened,
        UnaryRelationalOperator::General {
            containment_semantic: ContainmentSemantic::Parenthesis,
            expressions: order
                .iter()
                .map(|n| DomainExpression::lvar_builder(n.to_string()).build())
                .collect(),
        },
    );

    let wrapper = alias.unwrap_or_else(|| {
        format!("__r_{}", operation.trim_end_matches('!'))
    });
    Relation::InnerRelation {
        pattern: InnerRelationPattern::Indeterminate {
            identifier: crate::pipeline::asts::core::expressions::helpers::QualifiedName {
                namespace_path: NamespacePath::empty(),
                name: wrapper.clone().into(),
                grounding: None,
            },
            subquery: Box::new(ordered),
        },
        alias: Some(wrapper.into()),
        outer: false,
        cpr_schema: PhaseBox::phantom(),
    }
}

/// Prelude cartridge - provides core pseudo-predicates
pub struct PreludeCartridge;

impl BinCartridge for PreludeCartridge {
    fn metadata(&self) -> BinCartridgeMetadata {
        BinCartridgeMetadata {
            source_uri: "bootstrap://prelude".to_string(),
            namespace_path: "std::prelude".to_string(),
            is_universal: true, // Available everywhere without enlist
            language: Language::DqlStandard,
            _description: Some("Built-in pseudo-predicates for state manipulation".to_string()),
        }
    }

    fn entities(&self) -> Vec<Arc<dyn BinEntity>> {
        vec![
            Arc::new(MountPredicate) as Arc<dyn BinEntity>,
            Arc::new(MountNewPredicate) as Arc<dyn BinEntity>,
            Arc::new(MountTreePredicate) as Arc<dyn BinEntity>,
            Arc::new(EnlistPredicate) as Arc<dyn BinEntity>,
            Arc::new(DelistPredicate) as Arc<dyn BinEntity>,
            Arc::new(RunPredicate) as Arc<dyn BinEntity>,
            Arc::new(RunNamespacePredicate) as Arc<dyn BinEntity>,
            Arc::new(ConsultPredicate) as Arc<dyn BinEntity>,
            Arc::new(ConsultConcatPredicate) as Arc<dyn BinEntity>,
            Arc::new(ConsultTreePredicate) as Arc<dyn BinEntity>,
            Arc::new(GroundPredicate) as Arc<dyn BinEntity>,
            Arc::new(ImprintPredicate) as Arc<dyn BinEntity>,
            Arc::new(ImprintReplacePredicate) as Arc<dyn BinEntity>,
            Arc::new(AliasPredicate) as Arc<dyn BinEntity>,
            Arc::new(UnmountPredicate) as Arc<dyn BinEntity>,
            Arc::new(UnconsultPredicate) as Arc<dyn BinEntity>,
            Arc::new(RefreshPredicate) as Arc<dyn BinEntity>,
            Arc::new(ReconsultPredicate) as Arc<dyn BinEntity>,
            Arc::new(CompilePredicate) as Arc<dyn BinEntity>,
            Arc::new(ExplainRunPredicate) as Arc<dyn BinEntity>,
            Arc::new(DocPredicate) as Arc<dyn BinEntity>,
        ]
    }
}

/// Create a prelude cartridge instance
pub fn create_prelude_cartridge() -> Arc<dyn BinCartridge> {
    Arc::new(PreludeCartridge)
}

#[cfg(test)]
mod descriptor_agreement {
    //! The authoritative-descriptor pins (DIRECTIVE-CONVERGENCE-PLAN Phase 2).
    //! The descriptor table is the single authority; entity-local metadata
    //! must AGREE with it, and these tests are what makes drift impossible
    //! rather than merely discouraged.

    use super::*;
    use crate::enums::EntityType;
    use crate::pipeline::asts::effects::{
        descriptor, directive_category, is_liminal_eligible, DirectiveCategory,
        DirectiveRealization, DIRECTIVE_DESCRIPTORS,
    };

    #[test]
    fn twenty_nine_unique_descriptors_with_ruled_category_counts() {
        assert_eq!(DIRECTIVE_DESCRIPTORS.len(), 30);
        let mut names: Vec<&str> = DIRECTIVE_DESCRIPTORS.iter().map(|d| d.name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), 30, "descriptor names must be unique");

        let count = |c: DirectiveCategory| {
            DIRECTIVE_DESCRIPTORS
                .iter()
                .filter(|d| d.category == c)
                .count()
        };
        assert_eq!(count(DirectiveCategory::Session), 16);
        assert_eq!(count(DirectiveCategory::Ddl), 5);
        assert_eq!(count(DirectiveCategory::Dml), 3);
        assert_eq!(count(DirectiveCategory::Execution), 2);
        assert_eq!(count(DirectiveCategory::Utility), 4);
    }

    #[test]
    fn liminal_eligibility_is_exactly_the_session_category() {
        for d in DIRECTIVE_DESCRIPTORS {
            assert_eq!(
                is_liminal_eligible(d.name),
                d.category == DirectiveCategory::Session,
                "{}",
                d.name
            );
        }
        assert!(!is_liminal_eligible("some_user_rule"));
        assert_eq!(directive_category("no_such"), DirectiveCategory::User);
    }

    #[test]
    fn every_registered_directive_entity_agrees_with_its_descriptor() {
        let cartridge = PreludeCartridge;
        for entity in cartridge.entities() {
            if entity.entity_type() != EntityType::BinPseudoPredicate {
                continue; // compile is a BinRelation, not one of the 29
            }
            let name = entity.name();
            let desc = descriptor(name).unwrap_or_else(|| {
                panic!("registered directive entity '{name}' has no descriptor")
            });
            assert_eq!(
                desc.realization,
                DirectiveRealization::Entity,
                "'{name}' is registered but its descriptor denies Entity realization"
            );
            let sig = entity.signature();
            assert_eq!(
                sig.parameters.len(),
                desc.params.len(),
                "'{name}': entity arity disagrees with descriptor"
            );
            for (ep, dp) in sig.parameters.iter().zip(desc.params.iter()) {
                assert_eq!(ep.name, dp.name, "'{name}': parameter name drift");
                assert_eq!(
                    ep._is_optional, dp.optional,
                    "'{name}': parameter optionality drift"
                );
            }
        }
    }

    #[test]
    fn every_entity_descriptor_has_a_registration_and_absences_are_policy() {
        let cartridge = PreludeCartridge;
        let registered: Vec<String> = cartridge
            .entities()
            .iter()
            .filter(|e| e.entity_type() == EntityType::BinPseudoPredicate)
            .map(|e| e.name().trim_end_matches('!').to_string())
            .collect();
        for d in DIRECTIVE_DESCRIPTORS {
            match d.realization {
                DirectiveRealization::Entity => assert!(
                    registered.iter().any(|r| r == d.name),
                    "descriptor '{}' claims Entity realization but no prelude entity registers it",
                    d.name
                ),
                DirectiveRealization::SyntaxPipeTerminal | DirectiveRealization::LiminalOnly => {
                    assert!(
                        !registered.iter().any(|r| r == d.name),
                        "descriptor '{}' declares a contextual absence but an entity IS registered",
                        d.name
                    )
                }
            }
        }
    }

    #[test]
    fn entity_output_schemas_are_the_descriptor_receipt_columns() {
        // Every pre-§3 legacy receipt has migrated (slices 1–6);
        // the list stays for the NEXT holdout someone introduces.
        const LEGACY: &[&str] = &[];
        let cartridge = PreludeCartridge;
        for entity in cartridge.entities() {
            if entity.entity_type() != EntityType::BinPseudoPredicate {
                continue;
            }
            let name = entity.name();
            let desc = crate::pipeline::asts::effects::descriptor(name)
                .unwrap_or_else(|| panic!("'{name}' has no descriptor"));
            let crate::bin_cartridge::OutputSchema::Relation(cols) =
                entity.signature().output_schema
            else {
                panic!("'{name}': directive entities declare Relation schemas");
            };
            if LEGACY.contains(&name) {
                assert_ne!(
                    cols,
                    desc.receipt_columns(),
                    "'{name}' now matches its descriptor — remove it from LEGACY"
                );
            } else {
                assert_eq!(
                    cols,
                    desc.receipt_columns(),
                    "'{name}': the output schema must BE the descriptor's receipt columns"
                );
            }
        }
    }

    #[test]
    fn declared_echoes_are_unique_and_never_shadow_core_or_interior_columns() {
        for d in DIRECTIVE_DESCRIPTORS {
            let mut seen = std::collections::HashSet::new();
            for e in d.receipt_echoes {
                assert!(
                    !matches!(e.name, "success" | "operation" | "input" | "returned"),
                    "'{}': echo '{}' shadows a core/interior column",
                    d.name,
                    e.name
                );
                assert!(
                    seen.insert(e.name),
                    "'{}': duplicate echo '{}'",
                    d.name,
                    e.name
                );
            }
        }
    }

    #[test]
    fn interior_additions_are_declared_where_shipped() {
        // The slice's motivating exhibit: consult!'s descriptor used to
        // say ReceiptPayload::None while the entity shipped `returned` —
        // a stale annotation beside the authority. Now the descriptor
        // DECLARES both interior additions where the entities ship them.
        use crate::pipeline::asts::effects::{descriptor, ReceiptPayload};
        let consult = descriptor("consult").unwrap();
        assert!(consult.receipt_input_echo);
        assert_eq!(consult.receipt_payload, ReceiptPayload::Namespaces);
        let doc = descriptor("doc").unwrap();
        assert!(doc.receipt_input_echo);
        assert_eq!(doc.receipt_payload, ReceiptPayload::None);
    }

    #[test]
    fn compile_identity_is_qualified_only() {
        let mut registry = crate::bin_cartridge::registry::BinCartridgeRegistry::new();
        registry.register_cartridge(create_prelude_cartridge());
        // Qualified identity resolves...
        assert!(registry
            .lookup_qualified_entity(&["sys", "execution"], "compile")
            .is_some());
        // ...unqualified visibility refuses (sys::execution is not universal)...
        assert!(registry.lookup_entity("compile").is_none());
        // ...and the historical single-string bypass is dead.
        assert!(registry.lookup_entity("sys::execution.compile").is_none());
        // The prelude namespace does NOT own compile.
        assert!(registry
            .lookup_qualified_entity(&["std", "prelude"], "compile")
            .is_none());
        // While ordinary prelude directives resolve both ways.
        assert!(registry.lookup_entity("enlist!").is_some());
        assert!(registry
            .lookup_qualified_entity(&["std", "prelude"], "enlist!")
            .is_some());
    }
}
