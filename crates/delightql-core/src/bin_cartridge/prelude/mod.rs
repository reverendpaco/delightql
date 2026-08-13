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
use crate::pipeline::asts::core::OutValue;
use crate::pipeline::asts::unresolved::*;
use std::sync::Arc;

/// The descriptor-declared receipt heading for a built-in directive
/// Descriptor authority: entities' `output_schema`
/// calls THIS instead of hand-copying columns, so the descriptor is the
/// source and there is no second authority to drift.
pub(crate) fn descriptor_receipt_schema(bare_name: &str) -> Vec<(String, String)> {
    crate::pipeline::asts::effects::descriptor(bare_name)
        .unwrap_or_else(|| panic!("no directive descriptor for '{bare_name}'"))
        .receipt_columns()
}

/// Build a directive's CORE receipt FROM its descriptor (slice
/// 2): the echo column NAMES come from the descriptor's declared
/// `receipt_echoes`; the caller supplies only the VALUES, in ledger
/// order. An arity mismatch is an internal invariant violation — the
/// entity and its descriptor disagree — and panics rather than shipping
/// a receipt the declared heading disowns.
pub(crate) fn descriptor_core_receipt(
    bare_name: &str,
    values: &[Option<String>],
    alias: Option<String>,
) -> Grelex {
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
) -> Grelex {
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
        DomainExpression::Application(FunctionApplication::Ground(LiteralValue::Number(
            "1".to_string(),
        ))),
        DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(
            operation.to_string(),
        ))),
    ];
    values.extend(echoes.iter().map(|(_, v)| {
        DomainExpression::Application(FunctionApplication::Ground(match v {
            Some(s) => LiteralValue::String(s.clone()),
            None => LiteralValue::Null,
        }))
    }));
    Grelex::Literal(AnonRelation {
        table: AnonTable::from_values(Some(headers), vec![values], ())
            .expect("an effect receipt has one nonempty row"),
        alias: alias.map(|s| s.into()),
        outer: false,
    })
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
/// The whole construction is wrapped in an inner relation so it splices
/// wherever a Relation sits.
pub(crate) fn interior_receipt_result(
    operation: &str,
    input_heading: &[&str],
    input_rows: &[Vec<Option<String>>],
    returned_heading: &[&str],
    returned_rows: &[Vec<Option<String>>],
    alias: Option<String>,
) -> Grelex {
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
/// tree-group payload (the tree directives): echo
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
) -> Grelex {
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
) -> Grelex {
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
) -> Grelex {
    use crate::pipeline::asts::core::expressions::relational::InnerRelationPattern;
    use crate::pipeline::asts::core::metadata::NamespacePath;
    use crate::pipeline::asts::core::specs::{GroupSpec, OneOut, OutItem, ReductionItem};
    use crate::pipeline::asts::core::FunctionApplication;
    use crate::pipeline::asts::core::RecordMember;
    use crate::pipeline::asts::core::{Glob, Spread};

    let anon = |heading: &[&str], rows: &[Vec<Option<String>>]| {
        AnonTable::from_values(
            Some(
                heading
                    .iter()
                    .map(|h| DomainExpression::lvar_builder(h.to_string()).build())
                    .collect(),
            ),
            rows.iter()
                .map(|vals| {
                    vals.iter()
                        .map(|v| {
                            DomainExpression::Application(FunctionApplication::Ground(match v {
                                Some(s) => LiteralValue::String(s.clone()),
                                // An all-NULL contributor row elides to the
                                // empty interior `[]` (finding 1: an empty
                                // lift still reaches the callee once).
                                None => LiteralValue::Null,
                            }))
                        })
                        .collect()
                })
                .collect(),
            (),
        )
        .expect("an interior receipt has a nonempty heading and body")
    };
    let pipe = |source: Chain, operator: PipeOp| {
        source.then(Continuation::Pipe {
            operator: operator,
            named: None,
            cpr_schema: (),
        })
    };
    let grouped = |source: AnonTable, interior_name: &str| {
        pipe(
            Chain::ground(Grelex::Literal(AnonRelation::plain(source))),
            PipeOp::Group(GroupSpec::Reduce {
                plan: ReductionPlan::empty(),
                keys: Vec::new(),
                reductions: crate::pipeline::asts::vocabulary::Vec1::new(ReductionItem::Out(OutItem::One(
                    OneOut {
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
                        naming: Some(interior_name.into()),
                        output: (),
                    },
                ))),
            }),
        )
    };

    // One-row groups (one per declared interior) cross-join left to right.
    let mut groups = interiors
        .iter()
        .map(|(col, heading, rows)| grouped(anon(heading, rows), col));
    let mut joined = groups.next().expect("at least one declared interior");
    for right in groups {
        joined = joined.then(Continuation::Member {
            rhs: right,
            correlation: None,
            join_type: None,
            cpr_schema: (),
        });
    }
    let named = |expr, naming: delightql_types::SqlIdentifier| {
        OutItem::One(OneOut {
            expr: OutValue::Domain(expr),
            naming: Some(naming),
            output: (),
        })
    };
    let mut widening = vec![
        OutItem::Many(Spread::Glob(Glob::whole())),
        named(
            DomainExpression::Application(FunctionApplication::Ground(LiteralValue::Number(
                "1".to_string(),
            ))),
            "success".into(),
        ),
        named(
            DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(
                operation.to_string(),
            ))),
            "operation".into(),
        ),
    ];
    // Flat echoes widen exactly like the core (an optional echo is a
    // NULL literal), so the ledger order below can place them between
    // the core and the interiors.
    widening.extend(echoes.iter().map(|(name, v)| {
        named(
            DomainExpression::Application(FunctionApplication::Ground(match v {
                Some(s) => LiteralValue::String(s.clone()),
                None => LiteralValue::Null,
            })),
            (*name).into(),
        )
    }));
    // The receipt projection always carries `success` and `operation`.
    let widening = crate::pipeline::asts::vocabulary::Vec1::try_from_vec(widening)
        .expect("the receipt projection carries success and operation");
    let widened = pipe(joined, PipeOp::Project(widening));
    let order: Vec<&str> = ["success", "operation"]
        .into_iter()
        .chain(echoes.iter().map(|(name, _)| *name))
        .chain(interiors.iter().map(|(col, _, _)| *col))
        .collect();
    let ordered = pipe(
        widened,
        PipeOp::Project(
            crate::pipeline::asts::vocabulary::Vec1::try_from_vec(
                order
                    .iter()
                    .map(|n| {
                        OutItem::plain(DomainExpression::lvar_builder(n.to_string()).build(), ())
                    })
                    .collect(),
            )
            .expect("the receipt ordering names success and operation"),
        ),
    );

    let identifier = alias
        .as_deref()
        .unwrap_or_else(|| operation.trim_end_matches('!'));
    Grelex::Reference(Relation::InnerRelation {
        pattern: InnerRelationPattern::Indeterminate {
            identifier: crate::pipeline::asts::core::expressions::helpers::QualifiedName {
                namespace_path: NamespacePath::empty(),
                name: identifier.into(),
            },
            subquery: Box::new(ordered),
        },
        preminted_scope: None,
        alias: alias.map(Into::into),
        outer: false,
        cpr_schema: (),
    })
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
        // REALIZATION CONSUMES THE CLOSED IDENTITY: the directive entities
        // are derived from the one declaration, and construction asserts
        // that each realization site agrees with its declared realization
        // and its descriptor — drift is loud in every run, not in one test.
        let mut entities: Vec<Arc<dyn BinEntity>> = Vec::new();
        for kind in crate::pipeline::asts::effects::DirectiveKind::ALL {
            let descriptor = kind.descriptor();
            use crate::pipeline::asts::effects::DirectiveRealization;
            match (directive_realization(*kind), descriptor.realization) {
                (Some(entity), DirectiveRealization::Entity) => {
                    assert_directive_entity_agrees(descriptor, entity.as_ref());
                    entities.push(entity);
                }
                (
                    None,
                    DirectiveRealization::SyntaxPipeTerminal | DirectiveRealization::LiminalOnly,
                ) => {}
                (Some(_), realization) => panic!(
                    "directive '{}' supplies an entity its declared realization \
                     ({realization:?}) does not admit",
                    descriptor.name
                ),
                (None, DirectiveRealization::Entity) => panic!(
                    "directive '{}' declares Entity realization but supplies no entity",
                    descriptor.name
                ),
            }
        }
        // The prelude's NON-directive entities: catalog identities of their
        // own (`sys::execution`), never part of the directive universe.
        entities.push(Arc::new(CompilePredicate) as Arc<dyn BinEntity>);
        entities.push(Arc::new(ExplainRunPredicate) as Arc<dyn BinEntity>);
        entities
    }
}

/// One realization site per declared directive, exhaustive over the closed
/// kind: adding a declaration forces an answer HERE at compile time. `None`
/// answers for the syntax-terminal and liminal-only realizations —
/// `entities()` refuses a `None` whose declaration says Entity, so an
/// unanswered realization cannot ship as an accidental absence.
fn directive_realization(
    kind: crate::pipeline::asts::effects::DirectiveKind,
) -> Option<Arc<dyn BinEntity>> {
    use crate::pipeline::asts::effects::DirectiveKind as K;
    match kind {
        K::Consult => Some(Arc::new(ConsultPredicate)),
        K::ConsultConcatIntoNs => Some(Arc::new(ConsultConcatPredicate)),
        K::ConsultTree => Some(Arc::new(ConsultTreePredicate)),
        K::Reconsult => Some(Arc::new(ReconsultPredicate)),
        K::Unconsult => Some(Arc::new(UnconsultPredicate)),
        K::Mount => Some(Arc::new(MountPredicate)),
        K::MountNew => Some(Arc::new(MountNewPredicate)),
        K::MountTree => Some(Arc::new(MountTreePredicate)),
        K::Unmount => Some(Arc::new(UnmountPredicate)),
        K::Refresh => Some(Arc::new(RefreshPredicate)),
        K::Ground => Some(Arc::new(GroundPredicate)),
        K::Enlist => Some(Arc::new(EnlistPredicate)),
        K::Delist => Some(Arc::new(DelistPredicate)),
        K::Alias => Some(Arc::new(AliasPredicate)),
        K::Doc => Some(Arc::new(DocPredicate)),
        K::Imprint => Some(Arc::new(ImprintPredicate)),
        K::ImprintReplace => Some(Arc::new(ImprintReplacePredicate)),
        K::Run => Some(Arc::new(RunPredicate)),
        K::RunNamespace => Some(Arc::new(RunNamespacePredicate)),
        // Identity without an entity: the realization is a syntax terminal
        // or the liminal space, and the declaration says which.
        K::Expose
        | K::TempTable
        | K::Table
        | K::TempView
        | K::Insert
        | K::Update
        | K::Delete
        | K::Exit
        | K::Returning
        | K::ReturningOther
        | K::Stdout => None,
    }
}

/// The construction-time agreement fence: an Entity-realized directive's
/// entity must carry the declared identity — the bang name, the
/// descriptor's parameters (names, arity, optionality), and the
/// descriptor's receipt columns as its output schema. Disagreement is a
/// construction bug and panics where the cartridge is built.
fn assert_directive_entity_agrees(
    descriptor: &crate::pipeline::asts::effects::DirectiveDescriptor,
    entity: &dyn BinEntity,
) {
    let name = descriptor.name;
    assert_eq!(
        entity.name(),
        format!("{name}!"),
        "directive '{name}': entity name disagrees with the declaration"
    );
    assert_eq!(
        entity.entity_type(),
        crate::enums::EntityType::BinPseudoPredicate,
        "directive '{name}': an Entity-realized directive is a BinPseudoPredicate"
    );
    let signature = entity.signature();
    assert_eq!(
        signature.parameters.len(),
        descriptor.params.len(),
        "directive '{name}': entity arity disagrees with the descriptor"
    );
    for (entity_param, declared) in signature.parameters.iter().zip(descriptor.params.iter()) {
        assert_eq!(
            entity_param.name, declared.name,
            "directive '{name}': parameter name drift"
        );
        assert_eq!(
            entity_param._is_optional, declared.optional,
            "directive '{name}': parameter optionality drift"
        );
    }
    let crate::bin_cartridge::OutputSchema::Relation(columns) = signature.output_schema else {
        panic!("directive '{name}': directive entities declare Relation schemas");
    };
    assert_eq!(
        columns,
        descriptor.receipt_columns(),
        "directive '{name}': the output schema must BE the descriptor's receipt columns"
    );
}

/// Create a prelude cartridge instance
pub fn create_prelude_cartridge() -> Arc<dyn BinCartridge> {
    Arc::new(PreludeCartridge)
}

#[cfg(test)]
mod descriptor_agreement {
    //! The authoritative-descriptor pins.
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
    fn the_declaration_carries_the_ruled_category_counts() {
        assert_eq!(DIRECTIVE_DESCRIPTORS.len(), 30);
        let mut names: Vec<&str> = DIRECTIVE_DESCRIPTORS.iter().map(|d| d.name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), 30, "descriptor names must be unique");

        let count = |want: fn(&DirectiveCategory) -> bool| {
            DIRECTIVE_DESCRIPTORS
                .iter()
                .filter(|d| want(&d.category))
                .count()
        };
        assert_eq!(count(|c| matches!(c, DirectiveCategory::Session)), 16);
        assert_eq!(count(|c| matches!(c, DirectiveCategory::Ddl)), 5);
        assert_eq!(count(|c| matches!(c, DirectiveCategory::Dml(_))), 3);
        assert_eq!(count(|c| matches!(c, DirectiveCategory::Execution)), 2);
        assert_eq!(count(|c| matches!(c, DirectiveCategory::Utility)), 4);
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
    fn construction_runs_the_agreement_fences() {
        // entities() asserts entity/descriptor agreement at construction —
        // name, type, arity, parameter names/optionality, output schema —
        // so building the cartridge IS the check the old welding tests ran,
        // in every run rather than in one test.
        let built = PreludeCartridge.entities();
        let directive_entities = built
            .iter()
            .filter(|e| e.entity_type() == EntityType::BinPseudoPredicate)
            .count();
        let declared_entities = DIRECTIVE_DESCRIPTORS
            .iter()
            .filter(|d| d.realization == DirectiveRealization::Entity)
            .count();
        assert_eq!(directive_entities, declared_entities);
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
