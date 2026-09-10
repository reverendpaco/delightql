// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! What the binding must refuse, and what it must get right that no
//! recovery search could.

use super::{BranchLayout, SqlOutput};
use crate::relation::form::{AnonymousShape, AnonymousSlot, AnonymousSpec};
use crate::relation::{RelForm, SemanticRelation};

/// One arm publishing the given names, through the one entrance.
fn arm(registry: &crate::relation::Planning, names: &[&str]) -> SemanticRelation {
    let spellings: Vec<_> = names
        .iter()
        .map(|name| registry.intern(name, false))
        .collect();
    arm_of(registry, &spellings)
}

/// The same, from spellings already interned.
///
/// A published position answers to the SPELLING it was published under, not
/// to characters, so a replacement that stands where an operand stood
/// carries that operand's spellings. Two arms built from one interning are
/// how a test says two relations publish one heading.
fn arm_of(
    registry: &crate::relation::Planning,
    spellings: &[crate::names::Spelling],
) -> SemanticRelation {
    let slots: Vec<AnonymousSlot> = spellings
        .iter()
        .enumerate()
        .map(|(position, spelling)| AnonymousSlot::Declared {
            position: position as u32,
            named: Some(*spelling),
        })
        .collect();
    registry
        .authority()
        .derive(RelForm::Anonymous(AnonymousSpec::plain(
            AnonymousShape::Tabular,
            &slots,
            None,
        )))
        .expect("an anonymous relation is built")
}

/// The same heading under a new occurrence, BUILT FROM the operand — what a
/// refinement that re-exports a relation produces.
fn export_of(registry: &crate::relation::Planning, input: SemanticRelation) -> SemanticRelation {
    registry
        .authority()
        .derive(RelForm::Export(crate::relation::form::ExportSpec {
            input,
            why: crate::relation::form::ExportWhy::EmissionAlias,
        }))
        .expect("an export of a built relation")
}

/// The operand's whole heading with one position added — what a refinement
/// that injects a carrier produces.
fn embed_of(
    registry: &crate::relation::Planning,
    input: SemanticRelation,
    named: crate::names::Spelling,
) -> SemanticRelation {
    let ports = crate::relation::published_ports(registry, &input).expect("its own epoch");
    let slots = [crate::relation::form::ProjectSlot::Computed {
        naming: crate::relation::form::Naming::Authored(named),
        shape: crate::names::ValueShape::Unknown,
    }];
    let _ = ports;
    registry
        .authority()
        .derive(RelForm::Embed(crate::relation::form::ProjectSpec {
            input,
            why: crate::relation::form::ProjectWhy::Stage,
            slots: &slots,
        }))
        .expect("an embed over a built relation")
}

/// The ordered columns an arm publishes, standing for what a branch emits.
fn emitted(
    registry: &crate::relation::Planning,
    relation: &SemanticRelation,
) -> Vec<crate::names::ColId> {
    registry
        .authority()
        .interface(relation)
        .expect("its own epoch reads it")
        .ports()
        .iter()
        .map(|port| port.column())
        .collect()
}

/// A branch laid out for an arm.
///
/// Production reaches `BranchLayout` only from a `SetArm`, which owns the
/// statement and the relation together; a test says the two halves out loud
/// because the point of most of these witnesses is what happens when they
/// disagree.
fn layout(arm: &SemanticRelation, columns: Vec<crate::names::ColId>) -> BranchLayout {
    crate::pipeline::transformer::BranchLayout::for_test(*arm, columns)
}

/// The ordinary case: the branch emits exactly what its arm publishes.
fn laid_out(registry: &crate::relation::Planning, arm: &SemanticRelation) -> BranchLayout {
    layout(arm, emitted(registry, arm))
}

fn step(
    registry: &crate::relation::Planning,
    operator: crate::pipeline::asts::core::SetOperator,
    arms: &[SemanticRelation],
) -> SemanticRelation {
    registry
        .authority()
        .set_step(operator, arms)
        .expect("the arms correspond")
        .result()
}

const CORRESPONDING: crate::pipeline::asts::core::SetOperator =
    crate::pipeline::asts::core::SetOperator::UnionCorresponding;
const POSITIONAL: crate::pipeline::asts::core::SetOperator =
    crate::pipeline::asts::core::SetOperator::UnionAllPositional;

/// TWO PUBLICATIONS OF ONE VALUE ARE TWO OUTPUT POSITIONS, and the binding
/// keeps them apart.
///
/// The discriminating case for every recovery road: `q.*, q.*` puts one
/// value through two positions, so nothing about the value, the name, or
/// the lineage distinguishes the branch's first emitted column from its
/// second. Only the position does, and only because it was recorded.
#[test]
fn repeated_publications_of_one_value_bind_to_their_own_columns() {
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let left = arm(&registry, &["a", "b"]);
    let right = arm(&registry, &["a", "b"]);
    let result = step(&registry, POSITIONAL, &[left, right]);

    let left_columns = emitted(&registry, &left);
    let right_columns = emitted(&registry, &right);
    let map = registry.bindings();
    let binding = map
        .bind_run(
            &registry,
            &[result],
            &[
                layout(&left, left_columns.clone()),
                layout(&right, right_columns.clone()),
            ],
        )
        .expect("two branches, two arms");

    let slots = |branch| {
        map.branch(binding, branch)
            .expect("the branch is bound")
            .iter()
            .map(|(_, output)| match output {
                SqlOutput::Slot(slot) => slot.column(),
                SqlOutput::Pad(_) => panic!("a positional set never pads"),
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        slots(0),
        left_columns,
        "the kth position binds the kth emitted column, whatever value it carries"
    );
    assert_eq!(slots(1), right_columns);
}

/// A CORRESPONDING PAD IS A MEMBER, not a missing binding.
#[test]
fn a_padded_cell_binds_to_a_padding_rather_than_to_nothing() {
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let left = arm(&registry, &["a", "b"]);
    let right = arm(&registry, &["a", "c"]);
    let result = step(&registry, CORRESPONDING, &[left, right]);

    let map = registry.bindings();
    let binding = map
        .bind_run(
            &registry,
            &[result],
            &[laid_out(&registry, &left), laid_out(&registry, &right)],
        )
        .expect("two branches, two arms");

    let pads: Vec<_> = (0..2)
        .map(|branch| {
            map.branch(binding, branch)
                .expect("bound")
                .iter()
                .filter(|(_, output)| matches!(output, SqlOutput::Pad(_)))
                .count()
        })
        .collect();
    assert_eq!(
        pads,
        vec![1, 1],
        "`c` is absent from the left branch and `b` from the right, and each \
         absence is one padding the branch emits"
    );
    assert_eq!(
        map.branch(binding, 0).expect("bound").len(),
        3,
        "every position of the merged heading is bound in every branch"
    );
}

/// A BRANCH OF ANOTHER WIDTH IS NOT THIS ARM BEING EMITTED.
///
/// The refusal that replaces a search: the old bridge would have hunted the
/// short list for something plausible.
#[test]
fn a_branch_that_emits_a_different_width_refuses() {
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let left = arm(&registry, &["a", "b"]);
    let right = arm(&registry, &["a", "b"]);
    let result = step(&registry, POSITIONAL, &[left, right]);

    let mut short = emitted(&registry, &right);
    short.pop();
    let map = registry.bindings();
    assert!(
        map.bind_run(
            &registry,
            &[result],
            &[laid_out(&registry, &left), layout(&right, short)]
        )
        .is_err(),
        "a branch emitting fewer columns than its arm publishes is not that arm"
    );
}

/// A run has one step per operator and one branch per arm.
#[test]
fn a_run_whose_branch_count_disagrees_with_its_steps_refuses() {
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let left = arm(&registry, &["a"]);
    let right = arm(&registry, &["a"]);
    let result = step(&registry, POSITIONAL, &[left, right]);
    let map = registry.bindings();
    assert!(
        map.bind_run(&registry, &[result], &[laid_out(&registry, &left)])
            .is_err(),
        "one step needs two branches"
    );
    assert!(
        map.bind_run(&registry, &[], &[laid_out(&registry, &left)])
            .is_err(),
        "a run with no step binds nothing"
    );
}

/// A NESTED RUN COMPOSES ITS STEPS' TABLES; the leaves are what SQL emits.
#[test]
fn a_three_arm_run_binds_every_leaf_branch() {
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let a = arm(&registry, &["x", "y"]);
    let b = arm(&registry, &["x", "z"]);
    let c = arm(&registry, &["x", "w"]);
    let inner = step(&registry, CORRESPONDING, &[a, b]);
    let outer = step(&registry, CORRESPONDING, &[inner, c]);

    let map = registry.bindings();
    let binding = map
        .bind_run(
            &registry,
            &[inner, outer],
            &[
                laid_out(&registry, &a),
                laid_out(&registry, &b),
                laid_out(&registry, &c),
            ],
        )
        .expect("two steps, three branches");
    for branch in 0..3 {
        let row = map.branch(binding, branch).expect("every leaf is bound");
        assert_eq!(
            row.len(),
            4,
            "x, y, z and w are the run's four positions, and every branch binds each"
        );
        assert_eq!(
            row.iter()
                .filter(|(_, output)| matches!(output, SqlOutput::Slot(_)))
                .count(),
            2,
            "each arm publishes two of the four, and pads the rest"
        );
    }
}

/// EVIDENCE FROM ANOTHER COMPILATION IS NOT EVIDENCE HERE.
#[test]
fn a_result_from_another_compilation_refuses() {
    let theirs = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let left = arm(&theirs, &["a"]);
    let right = arm(&theirs, &["a"]);
    let result = step(&theirs, POSITIONAL, &[left, right]);
    let columns = emitted(&theirs, &left);

    let ours = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    assert!(
        ours.bindings()
            .bind_run(
                &ours,
                &[result],
                &[layout(&left, columns.clone()), layout(&right, columns)]
            )
            .is_err(),
        "another compilation's set result cannot be bound against this one's \
         emitted columns"
    );
}

/// A relation that is not a set has no table to bind against, and that is a
/// refusal rather than an empty binding.
#[test]
fn a_relation_that_is_not_a_set_refuses() {
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let plain = arm(&registry, &["a"]);
    let columns = emitted(&registry, &plain);
    let map = registry.bindings();
    assert!(map
        .bind_run(
            &registry,
            &[plain],
            &[layout(&plain, columns.clone()), layout(&plain, columns)]
        )
        .is_err());
}

/// A HANDLE IS A KEY OF ONE COMPILATION'S MAP, and both maps are POPULATED
/// AT THE SAME ORDINAL.
///
/// The discriminating shape: an empty second map refuses a foreign handle
/// only because index zero is absent. Two compilations that have each bound
/// their first compound both HAVE an index zero, so nothing but the epoch
/// separates them — and the outputs the wrong map would return are ordinary
/// registry identities with ordinary-looking indices.
#[test]
fn a_populated_map_refuses_another_compilations_handle_at_the_same_ordinal() {
    let bind = |registry: &crate::relation::Planning| {
        let left = arm(registry, &["a"]);
        let right = arm(registry, &["a"]);
        let result = step(registry, POSITIONAL, &[left, right]);
        registry
            .bindings()
            .bind_run(
                registry,
                &[result],
                &[laid_out(registry, &left), laid_out(registry, &right)],
            )
            .expect("two branches")
    };
    let first = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let second = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let theirs = bind(&first);
    let ours = bind(&second);

    // Both maps hold a record at the ordinal the foreign handle names —
    // that is what these two reads establish — so the refusal below is the
    // epoch's and not an absent index.
    assert!(first.bindings().branch(theirs, 0).is_ok());
    assert!(second.bindings().branch(ours, 0).is_ok());
    assert!(
        second.bindings().branch(theirs, 0).is_err(),
        "a handle another compilation issued names nothing here, however \
         populated this map is"
    );
}

/// A LAYOUT IS CHECKED AGAINST THE RELATION IT WAS LAID OUT FOR.
///
/// Two arms of one width and a third of another. Binding a branch laid out
/// for the wider relation against a narrower arm's evidence refuses: the
/// layout and the evidence disagree about what is being emitted, and a
/// positional zip over two relations is exactly what this authority
/// exists to stop.
#[test]
fn a_layout_whose_relation_publishes_another_heading_refuses() {
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let left = arm(&registry, &["a", "b"]);
    let right = arm(&registry, &["a", "b"]);
    let wide = arm(&registry, &["a", "b", "c"]);
    let result = step(&registry, POSITIONAL, &[left, right]);

    assert!(
        registry
            .bindings()
            .bind_run(
                &registry,
                &[result],
                &[laid_out(&registry, &left), laid_out(&registry, &right)],
            )
            .is_ok(),
        "each branch laid out for its own arm binds"
    );
    assert!(
        registry
            .bindings()
            .bind_run(
                &registry,
                &[result],
                &[
                    layout(&wide, emitted(&registry, &left)),
                    laid_out(&registry, &right)
                ],
            )
            .is_err(),
        "a branch whose relation publishes three positions is not this \
         two-position arm being emitted, however many columns it hands over"
    );
}

/// A SAME-WIDTH WRONG ARM REFUSES.
///
/// Two relations of one width, publishing the same names in the same order,
/// so nothing about the columns tells them apart. The binding refuses the
/// one the step's evidence does not name and that no refinement reported as
/// its replacement. The refusal is the runtime authority's — no source-text
/// allowlist is consulted.
#[test]
fn a_same_width_wrong_arm_refuses() {
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let left = arm(&registry, &["a", "b"]);
    let right = arm(&registry, &["a", "b"]);
    let stranger = arm(&registry, &["a", "b"]);
    let result = step(&registry, POSITIONAL, &[left, right]);

    assert!(
        registry
            .bindings()
            .bind_run(
                &registry,
                &[result],
                &[laid_out(&registry, &left), laid_out(&registry, &right)],
            )
            .is_ok(),
        "each branch laid out for its own arm binds"
    );
    assert!(
        registry
            .bindings()
            .bind_run(
                &registry,
                &[result],
                &[laid_out(&registry, &stranger), laid_out(&registry, &right)],
            )
            .is_err(),
        "a relation nobody reported as this arm's replacement is another \
         operand, however alike its heading"
    );
}

/// A LAWFUL REBUILD IS CARRIED, and the binding translates through it.
///
/// Refinement replaces a set operand — hoisting a witness into a join,
/// binding an outer context onto a ground read — by BUILDING the
/// replacement from the operand. The lineage that build records is the
/// total port map; the binding then translates each recorded port through
/// it and requires the answer to be the position the branch actually emits.
#[test]
fn a_reported_replacement_binds_and_an_unreported_one_does_not() {
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let heading = [registry.intern("a", false), registry.intern("b", false)];
    let left = arm_of(&registry, &heading);
    let right = arm_of(&registry, &heading);
    let result = step(&registry, POSITIONAL, &[left, right]);
    // THE REBUILD IS BUILT FROM THE OPERAND. A relation minted beside it
    // carries no lineage, and no spelling comparison can invent one.
    let rebuilt = export_of(&registry, left);

    let bind = || {
        registry.bindings().bind_run(
            &registry,
            &[result],
            &[laid_out(&registry, &rebuilt), laid_out(&registry, &right)],
        )
    };
    assert!(
        bind().is_err(),
        "before the report, the replacement is just another relation"
    );

    registry
        .authority()
        .report_replacement_for_test(left, rebuilt)
        .expect("the replacement publishes what it replaces");

    let binding = bind().expect("a reported replacement stands where its operand stood");
    assert_eq!(
        registry
            .bindings()
            .branch(binding, 0)
            .expect("bound")
            .iter()
            .map(|(_, output)| match output {
                SqlOutput::Slot(slot) => slot.column(),
                SqlOutput::Pad(_) => panic!("a positional set never pads"),
            })
            .collect::<Vec<_>>(),
        emitted(&registry, &rebuilt),
        "the recorded ports bind to the REPLACEMENT's emitted columns"
    );
}

/// A REPLACEMENT THAT DOES NOT STAND WHERE ITS OPERAND STOOD REFUSES AT THE
/// AUTHORITY, before any binding reads it.
#[test]
fn a_replacement_may_append_but_cannot_move_a_position() {
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let (a, b) = (registry.intern("a", false), registry.intern("b", false));
    let operand = arm_of(&registry, &[a, b]);
    let refine_into = |produced: SemanticRelation| {
        registry
            .authority()
            .report_replacement_for_test(operand, produced)
    };
    // AN EMBED CARRIES THE OPERAND'S WHOLE HEADING and adds to it, so every
    // old position has a recorded landing.
    assert!(
        refine_into(embed_of(&registry, operand, registry.intern("c", false))).is_ok(),
        "an appended position leaves a total old-port-to-new-port map"
    );
    assert!(
        refine_into(arm_of(&registry, &[b, a])).is_err(),
        "a relation built beside the operand carries none of its positions, \
         whatever it spells"
    );
    assert!(
        refine_into(arm(&registry, &["a", "b"])).is_err(),
        "a relation that merely spells the same characters is not the one it \
         would replace"
    );
}

/// CONSTRUCTION OCCURRENCE IS NOT HEADING EQUALITY.
///
/// Crossing a closed residual may replace the relation that constructed its
/// configured prefix. A rebuilt occurrence is that relation; an independently
/// minted relation with the same names is not.
#[test]
fn exact_continuation_distinguishes_a_same_heading_sibling() {
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let heading = [registry.intern("id", false), registry.intern("n", false)];
    let source = arm_of(&registry, &heading);
    let sibling = arm_of(&registry, &heading);
    let rebuilt = export_of(&registry, source);

    assert!(
        registry
            .authority()
            .continues_exactly(source, rebuilt)
            .expect("the occurrence judgment is total"),
        "a construction-recorded rebuild is the source occurrence"
    );
    assert!(
        !registry
            .authority()
            .continues_exactly(source, sibling)
            .expect("the occurrence judgment is total"),
        "an independently minted same-heading relation is not the source occurrence"
    );
}

/// TWO READS OF ONE SOURCE NEVER ANSWER FOR EACH OTHER AT A SQL SITE.
///
/// A fresh read of a definition is its own occurrence: its site answers
/// its own positions and the positions construction carried into them,
/// and nothing else. A sibling read of the same source, and a rebuild that
/// replaced the source, are not realized at that site, whatever they share
/// with it. The lawful directed roads stay: a rebuild's site answers the
/// position it replaced, and a read's site answers the source position it
/// carries.
#[test]
fn a_fresh_read_never_answers_for_a_sibling_read_or_a_rebuild_of_its_source() {
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let source = arm(&registry, &["v"]);
    let rebuilt = export_of(&registry, source);
    registry
        .authority()
        .report_replacement_for_test(source, rebuilt)
        .expect("an export carries every position of its operand");
    let instance = |registry: &crate::relation::Planning| {
        registry
            .authority()
            .derive(RelForm::Instantiate(crate::relation::form::InstanceSpec {
                kind: crate::relation::form::DefinitionKind::Cte,
                template: source,
                answers_to: None,
            }))
            .expect("a fresh read of the source")
    };
    let outer = instance(&registry);
    let inner = instance(&registry);
    let port_of = |relation: &SemanticRelation| {
        crate::relation::published_ports(&registry, relation).expect("one position")[0]
    };
    let (source_port, rebuilt_port, outer_port, inner_port) = (
        port_of(&source),
        port_of(&rebuilt),
        port_of(&outer),
        port_of(&inner),
    );
    assert_ne!(outer_port, inner_port, "two reads publish two occurrences");

    let names = registry.names();
    let sealed = registry.seal();
    let bindings = names.bindings();
    let inner_site = bindings
        .bind_interface(&sealed, &inner)
        .expect("the inner read emits its own interface");
    let rebuilt_site = bindings
        .bind_interface(&sealed, &rebuilt)
        .expect("the rebuild emits its own interface");

    assert_eq!(
        bindings
            .at(inner_site, inner_port)
            .expect("a site answers its own position"),
        inner_port.column()
    );
    assert!(
        bindings.at(inner_site, outer_port).is_err(),
        "the inner read's site must not answer the outer read's occurrence"
    );
    assert!(
        bindings.at(inner_site, rebuilt_port).is_err(),
        "the inner read's site must not answer a rebuild of its source"
    );
    assert_eq!(
        bindings
            .at(rebuilt_site, source_port)
            .expect("a rebuild answers what it replaced"),
        rebuilt_port.column(),
        "the directed replacement translation stands"
    );
    assert!(
        bindings.at(rebuilt_site, outer_port).is_err()
            && bindings.at(rebuilt_site, inner_port).is_err(),
        "a rebuild of the source answers for neither read of it"
    );
}

/// A fresh read of one source, through the one instantiating act.
fn read_of(registry: &crate::relation::Planning, source: SemanticRelation) -> SemanticRelation {
    registry
        .authority()
        .derive(RelForm::Instantiate(crate::relation::form::InstanceSpec {
            kind: crate::relation::form::DefinitionKind::Cte,
            template: source,
            answers_to: None,
        }))
        .expect("a fresh read of the source")
}

/// The one position a single-column relation publishes.
fn only_port(
    registry: &crate::relation::Planning,
    relation: &SemanticRelation,
) -> crate::relation::PortId {
    crate::relation::published_ports(registry, relation).expect("one position")[0]
}

/// A chain publishing a relation, the way a rebuild receives its operands.
fn chain_over(
    registry: &crate::relation::Planning,
    relation: SemanticRelation,
) -> crate::pipeline::asts::core::Chain<crate::pipeline::asts::core::Refined> {
    registry
        .authority()
        .ground_read::<crate::pipeline::asts::core::Refined>(
            crate::pipeline::asts::core::Access::All,
            false,
            relation,
        )
        .expect("a chain reading the relation")
}

/// A correspondence merging one position of each side.
fn merging(
    left: crate::relation::PortId,
    right: crate::relation::PortId,
) -> crate::pipeline::asts::core::MemberCorrelation<crate::pipeline::asts::core::Refined> {
    crate::pipeline::asts::core::MemberCorrelation::Correspond(
        crate::pipeline::asts::core::Correspondence::new(vec![crate::relation::form::MergedKey {
            left,
            right,
        }]),
    )
}

/// A REBUILD CERTIFIES ONLY WHAT IT DERIVED OVER WHAT ITS OPERAND STANDS ON.
///
/// Two joins of three reads of one source, `A+B` and `A+C`, genuinely share
/// the occurrence `A`. A rebuild opened over `A+B`'s own occurrences cannot
/// be driven to join `C`, cannot be closed on a product it did not derive,
/// and records nothing; so `B`'s position stays absent at `A+C`'s SQL site.
/// The same operation, driven over `B`, certifies the genuine rebuild and
/// its site answers both the old merged position and `B`.
#[test]
fn a_rebuild_over_a_shared_operand_does_not_certify_a_join_of_another_operand() {
    use crate::relation::form::{JoinKind, JoinSpec, MergedKey};
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let source = arm(&registry, &["v"]);
    let (a, b, c) = (
        read_of(&registry, source),
        read_of(&registry, source),
        read_of(&registry, source),
    );
    let (a_port, b_port, c_port) = (
        only_port(&registry, &a),
        only_port(&registry, &b),
        only_port(&registry, &c),
    );
    let join = |right: SemanticRelation, right_port| {
        registry
            .authority()
            .derive(RelForm::Join(JoinSpec {
                left: a,
                right,
                kind: JoinKind::FullOuter,
                merged: &[MergedKey {
                    left: a_port,
                    right: right_port,
                }],
            }))
            .expect("a join of two reads")
    };
    // The resolver's join of A and B, merged on v, and a product assembled
    // elsewhere over A and C.
    let old = join(b, b_port);
    let old_merged = only_port(&registry, &old);
    let elsewhere = join(c, c_port);
    let full = Some(crate::pipeline::asts::core::JoinType::FullOuter);

    // Driving the rebuild toward C refuses; closing it on the foreign
    // product refuses.
    let mut rebuild = registry
        .authority()
        .rebuilding(old, &[a, b])
        .expect("A+B stands over A and B");
    rebuild
        .begins_with(&chain_over(&registry, a))
        .expect("the rebuild begins with A");
    assert!(
        rebuild
            .join(
                chain_over(&registry, a),
                chain_over(&registry, c),
                merging(a_port, c_port),
                full.clone(),
            )
            .is_err(),
        "a rebuild over A and B does not join C"
    );
    assert!(
        rebuild.finish(chain_over(&registry, elsewhere)).is_err(),
        "a rebuild certifies only the product it derived"
    );
    // Opening over occurrences the operand does not stand over derives a
    // product and certifies nothing.
    let mut wrong = registry
        .authority()
        .rebuilding(old, &[a, c])
        .expect("opening judges; it does not refuse the operand");
    wrong
        .begins_with(&chain_over(&registry, a))
        .expect("the rebuild begins with A");
    let product = wrong
        .join(
            chain_over(&registry, a),
            chain_over(&registry, c),
            merging(a_port, c_port),
            full.clone(),
        )
        .expect("A+C is derived over A and C");
    let derived_elsewhere = product.semantic_relation();
    wrong
        .finish(product)
        .expect("the product is returned uncertified");
    let names = registry.names();
    for candidate in [&elsewhere, &derived_elsewhere] {
        assert!(
            crate::relation::replacement(&names, old.relation(), candidate)
                .expect("an epoch-checked read")
                .is_none(),
            "A+B is not recorded as replaced by a join of A and C"
        );
    }

    // The genuine rebuild, over B, is certified.
    let mut rebuild = registry
        .authority()
        .rebuilding(old, &[a, b])
        .expect("A+B stands over A and B");
    rebuild
        .begins_with(&chain_over(&registry, a))
        .expect("the rebuild begins with A");
    let product = rebuild
        .join(
            chain_over(&registry, a),
            chain_over(&registry, b),
            merging(a_port, b_port),
            full.clone(),
        )
        .expect("A+B is derived again over A and B");
    let new = product.semantic_relation();
    let new_merged = only_port(&registry, &new);
    rebuild
        .finish(product)
        .expect("the rebuild closes on its product");
    assert!(
        crate::relation::replacement(&names, old.relation(), &new)
            .expect("an epoch-checked read")
            .is_some(),
        "the rebuild it derived is recorded"
    );

    let sealed = registry.seal();
    let bindings = names.bindings();
    let foreign_site = bindings
        .bind_interface(&sealed, &derived_elsewhere)
        .expect("A+C emits its own interface");
    let answer = bindings.at(foreign_site, b_port);
    assert!(
        answer.is_err(),
        "absent B {b_port:?} answered at A+C as {answer:?}"
    );
    assert!(bindings.at(foreign_site, old_merged).is_err());
    let site = bindings
        .bind_interface(&sealed, &new)
        .expect("the rebuilt A+B emits its own interface");
    assert_eq!(
        bindings
            .at(site, old_merged)
            .expect("the rebuild answers the merged position it replaced"),
        new_merged.column()
    );
    assert_eq!(
        bindings
            .at(site, b_port)
            .expect("the rebuild answers B, which it stands over"),
        new_merged.column()
    );
}

/// CARRYING ONE ARM'S POSITIONS DOES NOT MAKE A SET THAT ARM'S REPUBLICATION.
///
/// A positional union of two reads carries its opening arm's positions, and
/// the other arm lives in its contribution table. A rebuild opened over an
/// export of `A` alone admits neither the union as `A`'s realization nor,
/// opened the other way, the union as an operand standing over `A` alone.
/// The whole union, republished, is certified as itself.
#[test]
fn a_set_is_not_admitted_as_a_republication_of_one_of_its_arms() {
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let source = arm(&registry, &["v"]);
    let a = read_of(&registry, source);
    let b = read_of(&registry, source);
    let old = export_of(&registry, a);
    let old_port = only_port(&registry, &old);
    let union = step(&registry, POSITIONAL, &[a, b]);
    let union_port = only_port(&registry, &union);
    let names = registry.names();

    // Forward: the union is not A standing again.
    let mut rebuild = registry
        .authority()
        .rebuilding(old, &[a])
        .expect("an export of A stands over A");
    assert!(
        rebuild.begins_with(&chain_over(&registry, union)).is_err(),
        "a union carrying A's positions beside B is not a republication of A"
    );
    // Inverse: the union does not stand over A alone.
    let mut inverse = registry
        .authority()
        .rebuilding(union, &[a])
        .expect("opening judges; it does not refuse the operand");
    inverse
        .begins_with(&chain_over(&registry, old))
        .expect("an export of A stands in A's place");
    inverse
        .finish(chain_over(&registry, old))
        .expect("the product is returned uncertified");
    assert!(
        crate::relation::replacement(&names, old.relation(), &union)
            .expect("an epoch-checked read")
            .is_none()
            && crate::relation::replacement(&names, union.relation(), &old)
                .expect("an epoch-checked read")
                .is_none(),
        "neither direction records a replacement"
    );

    // Control: the whole union, republished, is certified as itself.
    let whole = export_of(&registry, union);
    let whole_port = only_port(&registry, &whole);
    let again = export_of(&registry, union);
    let again_port = only_port(&registry, &again);
    let mut rebuild = registry
        .authority()
        .rebuilding(whole, &[union])
        .expect("an export of the union stands over the union");
    rebuild
        .begins_with(&chain_over(&registry, again))
        .expect("another export of the union stands in its place");
    rebuild
        .finish(chain_over(&registry, again))
        .expect("the rebuild closes on its product");
    assert!(
        crate::relation::replacement(&names, whole.relation(), &again)
            .expect("an epoch-checked read")
            .is_some(),
        "republishing the whole union is certified"
    );

    let sealed = registry.seal();
    let bindings = names.bindings();
    let union_site = bindings
        .bind_interface(&sealed, &union)
        .expect("the union emits its own interface");
    assert_eq!(
        bindings
            .at(union_site, union_port)
            .expect("a site answers its own position"),
        union_port.column()
    );
    let answer = bindings.at(union_site, old_port);
    assert!(
        answer.is_err(),
        "A-only's publication {old_port:?} answered at the union as {answer:?}"
    );
    let again_site = bindings
        .bind_interface(&sealed, &again)
        .expect("the republished union emits its own interface");
    assert_eq!(
        bindings
            .at(again_site, whole_port)
            .expect("the certified republication answers the position it replaced"),
        again_port.column()
    );
}

/// AN OPERAND DOES NOT STOP PARTICIPATING WHEN ITS COLUMN DISAPPEARS.
///
/// A corresponding union and a full outer join of `A(v)` and `B(w)` both
/// depend on `B` for their rows — padding and unmatched rows publish NULL
/// into `v` — and projecting `w` out changes none of that. A rebuild opened
/// over `A` alone judges the old operation by its recorded operands, finds
/// `B`, and certifies nothing; opened over both, the genuine rebuild of the
/// join is certified and answers the projected position.
#[test]
fn projecting_an_operand_away_does_not_erase_its_participation() {
    use crate::relation::form::{JoinKind, JoinSpec, ProjectOutSpec};
    for is_set in [true, false] {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let a = read_of(&registry, arm(&registry, &["v"]));
        let b = read_of(&registry, arm(&registry, &["w"]));
        let combined = if is_set {
            step(&registry, CORRESPONDING, &[a, b])
        } else {
            registry
                .authority()
                .derive(RelForm::Join(JoinSpec {
                    left: a,
                    right: b,
                    kind: JoinKind::FullOuter,
                    merged: &[],
                }))
                .expect("a full outer join of two reads")
        };
        let ports = crate::relation::published_ports(&registry, &combined).expect("ports");
        let old = registry
            .authority()
            .derive(RelForm::ProjectOut(ProjectOutSpec {
                input: combined,
                removed: &ports[1..],
            }))
            .expect("the compound with w projected out");
        let old_port = only_port(&registry, &old);
        let a_only = export_of(&registry, a);
        let mut rebuild = registry
            .authority()
            .rebuilding(old, &[a])
            .expect("opening judges; it does not refuse the operand");
        rebuild
            .begins_with(&chain_over(&registry, a_only))
            .expect("an export of A stands in A's place");
        rebuild
            .finish(chain_over(&registry, a_only))
            .expect("the product is returned uncertified");
        let names = registry.names();
        assert!(
            crate::relation::replacement(&names, old.relation(), &a_only)
                .expect("an epoch-checked read")
                .is_none(),
            "is_set={is_set}: a compound over A and B is not replaced by A alone"
        );
        // The genuine rebuild over both operands is certified (join case).
        let certified = if is_set {
            None
        } else {
            let mut rebuild = registry
                .authority()
                .rebuilding(old, &[a, b])
                .expect("the projected join stands over A and B");
            rebuild
                .begins_with(&chain_over(&registry, a))
                .expect("the rebuild begins with A");
            let product = rebuild
                .join(
                    chain_over(&registry, a),
                    chain_over(&registry, b),
                    crate::pipeline::asts::core::MemberCorrelation::Cartesian(()),
                    Some(crate::pipeline::asts::core::JoinType::FullOuter),
                )
                .expect("A+B is derived over A and B");
            let new = product.semantic_relation();
            rebuild
                .finish(product)
                .expect("the rebuild closes on its product");
            assert!(
                crate::relation::replacement(&names, old.relation(), &new)
                    .expect("an epoch-checked read")
                    .is_some(),
                "the rebuild over both operands is certified"
            );
            Some(new)
        };
        let sealed = registry.seal();
        let bindings = names.bindings();
        let site = bindings
            .bind_interface(&sealed, &a_only)
            .expect("A alone emits its own interface");
        let answer = bindings.at(site, old_port);
        assert!(
            answer.is_err(),
            "is_set={is_set}: the compound's position {old_port:?} answered at A alone as {answer:?}"
        );
        if let Some(new) = certified {
            let site = bindings
                .bind_interface(&sealed, &new)
                .expect("the rebuilt join emits its own interface");
            assert!(
                bindings.at(site, old_port).is_ok(),
                "the certified rebuild answers the projected position"
            );
        }
    }
}

/// A WITNESS IS NOT A REPUBLICATION OF ITS OPERAND.
///
/// A signed witness totalizes an empty operand into one proxy row, and an
/// existence witness collapses its operand to one verdict row; neither
/// keeps the operand's rows. Hiding the verdict column does not undo that,
/// so a witness over `A` never stands in `A`'s place.
#[test]
fn a_witness_is_not_admitted_as_a_republication_of_its_operand() {
    use crate::relation::form::{ProjectOutSpec, SignedWitnessSpec};
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let a = read_of(&registry, arm(&registry, &["v"]));
    let old = export_of(&registry, a);
    let old_port = only_port(&registry, &old);
    let witness = registry
        .authority()
        .derive(RelForm::SignedWitness(SignedWitnessSpec { input: a }))
        .expect("a signed witness over A");
    let ports = crate::relation::published_ports(&registry, &witness).expect("ports");
    let totalized = registry
        .authority()
        .derive(RelForm::ProjectOut(ProjectOutSpec {
            input: witness,
            removed: &ports[1..],
        }))
        .expect("the witness with its verdict projected out");
    let mut rebuild = registry
        .authority()
        .rebuilding(old, &[a])
        .expect("an export of A stands over A");
    assert!(
        rebuild
            .begins_with(&chain_over(&registry, totalized))
            .is_err(),
        "a totalized witness over A does not stand in A's place"
    );
    let names = registry.names();
    assert!(
        crate::relation::replacement(&names, old.relation(), &totalized)
            .expect("an epoch-checked read")
            .is_none()
    );
    let sealed = registry.seal();
    let bindings = names.bindings();
    let site = bindings
        .bind_interface(&sealed, &totalized)
        .expect("the witness emits its own interface");
    assert!(bindings.at(site, old_port).is_err());
}

/// AN EMPTY MAP DOES NOT MAKE A GROUPING A NARROWING.
///
/// A grouping of a grid to no columns and an empty-column access over the
/// same grid both publish nothing and both stand on the grid, yet one row of
/// the grouping stands for many rows of the grid. Only two recorded
/// narrowings of one grid replace each other; a grouping is refused, and a
/// rebuild over a join with the grouping cannot consume the narrowing in its
/// place.
#[test]
fn an_empty_map_does_not_make_a_grouping_a_narrowing() {
    use crate::relation::form::{
        AccessShape, AccessSpec, GroupKind, GroupSpec, JoinKind, JoinSpec,
    };
    for group in [false, true] {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let a = read_of(&registry, arm(&registry, &["v"]));
        let grid = arm(&registry, &["w"]);
        let empty_access = |registry: &crate::relation::Planning| {
            registry
                .authority()
                .derive(RelForm::Access(AccessSpec {
                    input: grid,
                    shape: AccessShape::Empty,
                    slots: &[],
                    dependencies: &[],
                }))
                .expect("an empty-column access over the grid")
        };
        let consumed = if group {
            registry
                .authority()
                .derive(RelForm::Group(GroupSpec {
                    input: grid,
                    kind: GroupKind::Distinct,
                    keys: &[],
                    reductions: &[],
                }))
                .expect("a grouping of the grid to no columns")
        } else {
            empty_access(&registry)
        };
        let narrowed = empty_access(&registry);
        let old = registry
            .authority()
            .derive(RelForm::Join(JoinSpec {
                left: a,
                right: consumed,
                kind: JoinKind::Inner,
                merged: &[],
            }))
            .expect("a join of A with the zero-width operand");
        let old_port = only_port(&registry, &old);
        let paired = registry
            .authority()
            .narrowed_again(consumed, &narrowed)
            .is_ok();
        assert_eq!(
            paired, !group,
            "only two recorded narrowings of one grid replace each other"
        );
        let mut rebuild = registry
            .authority()
            .rebuilding(old, &[a, consumed])
            .expect("the join stands over A and its zero-width operand");
        rebuild
            .begins_with(&chain_over(&registry, a))
            .expect("the rebuild begins with A");
        let product = rebuild.join(
            chain_over(&registry, a),
            chain_over(&registry, narrowed),
            crate::pipeline::asts::core::MemberCorrelation::Cartesian(()),
            Some(crate::pipeline::asts::core::JoinType::Inner),
        );
        let names = registry.names();
        match product {
            Ok(product) if !group => {
                let new = product.semantic_relation();
                rebuild
                    .finish(product)
                    .expect("the rebuild closes on its product");
                assert!(
                    crate::relation::replacement(&names, old.relation(), &new)
                        .expect("an epoch-checked read")
                        .is_some(),
                    "a genuine re-narrowing lets the rebuild be certified"
                );
                let sealed = registry.seal();
                let bindings = names.bindings();
                let site = bindings
                    .bind_interface(&sealed, &new)
                    .expect("the rebuilt join emits its own interface");
                assert!(bindings.at(site, old_port).is_ok());
            }
            Ok(_) => panic!("a narrowing stood in for a grouping"),
            Err(_) => assert!(
                group,
                "a genuine re-narrowing is consumed in the narrowing's place"
            ),
        }
    }
}

/// A GROUP STEP DOES NOT BECOME A STAGE REPUBLICATION WHEN RE-APPENDED.
///
/// Re-appending a step over the relation that replaced its operand derives a
/// fresh stage export only for a step whose result the record shows as a
/// republication of the replaced relation. A grouping stands on one input
/// too, but a distinct bag re-derived as a stage would be an A-only
/// publication a later rebuild could certify as A itself.
#[test]
fn a_group_step_does_not_become_a_stage_republication() {
    use crate::pipeline::asts::core::{Access, Resolved};
    use crate::relation::pending::{GroupShape, Pending, Position};
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let a = read_of(&registry, arm(&registry, &["v"]));
    let a_port = only_port(&registry, &a);
    let (step, _) = registry
        .authority()
        .bind(Pending::Group {
            input: a,
            keys: vec![Position::restating_expanded(a_port)],
            shape: GroupShape::Distinct,
        })
        .expect("a distinct grouping over A");
    let grouped = *step.result();
    let moved = export_of(&registry, a);
    let old_chain = registry
        .authority()
        .ground_read::<Resolved>(Access::All, false, a)
        .expect("a chain reading A");
    let new_chain = registry
        .authority()
        .ground_read::<Resolved>(Access::All, false, moved)
        .expect("a chain reading the export of A");
    registry
        .authority()
        .refine_relation(old_chain, |_| Ok(new_chain.clone()))
        .expect("an export of A replaces A");
    assert!(
        registry
            .authority()
            .continue_over(new_chain, step, a)
            .is_err(),
        "a grouping step is not re-derived as a stage republication"
    );
    assert!(
        crate::relation::replacement(&registry.names(), grouped.relation(), &moved)
            .expect("an epoch-checked read")
            .is_none(),
        "no record relates the grouping to a publication over A"
    );
}

/// TWO FRESH READS OF ONE SOURCE CANNOT BE PAIRED BY ANY STATED OCCURRENCES.
///
/// A rebuild opened over the definition, over either read, or over both
/// never certifies one read as the other's replacement: the read it is
/// asked to begin with is not the occurrence it was opened over, or the
/// operand stands over something unstated. The inner read's SQL site never
/// answers the outer read's occurrence.
#[test]
fn a_rebuild_cannot_certify_one_fresh_read_as_another_read_s_replacement() {
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let source = arm(&registry, &["v"]);
    let outer = read_of(&registry, source);
    let inner = read_of(&registry, source);
    let (outer_port, inner_port) = (only_port(&registry, &outer), only_port(&registry, &inner));
    for over in [vec![source], vec![outer], vec![inner], vec![outer, inner]] {
        let mut rebuild = registry
            .authority()
            .rebuilding(outer, &over)
            .expect("opening judges; it does not refuse the operand");
        match rebuild.begins_with(&chain_over(&registry, inner)) {
            // The inner read is not the first stated occurrence.
            Err(_) => continue,
            // Opened over the inner read itself it may begin, but the outer
            // read does not stand over that, so closing certifies nothing.
            Ok(()) => {
                assert!(
                    over[0] == inner,
                    "only the inner read itself begins a rebuild with it"
                );
                rebuild
                    .finish(chain_over(&registry, inner))
                    .expect("the product is returned uncertified");
            }
        }
    }
    let names = registry.names();
    assert!(
        crate::relation::replacement(&names, outer.relation(), &inner)
            .expect("an epoch-checked read")
            .is_none(),
        "no stated occurrences record one read as the other's replacement"
    );
    let sealed = registry.seal();
    let bindings = names.bindings();
    let site = bindings
        .bind_interface(&sealed, &inner)
        .expect("the inner read emits its own interface");
    assert_eq!(
        bindings
            .at(site, inner_port)
            .expect("a site answers its own position"),
        inner_port.column()
    );
    let answer = bindings.at(site, outer_port);
    assert!(
        answer.is_err(),
        "outer {outer_port:?} answered at the inner read's site as {answer:?}"
    );
}

/// A REBUILD OVER THE READ IT REPLACES IS CERTIFIED BY THE SAME OPERATION.
///
/// The single-table road: a relation built over a read occurrence begins
/// the rebuild in the read's place, the rebuild closes on it, and the
/// rebuild's SQL site answers the position it replaced.
#[test]
fn a_rebuild_over_the_read_it_replaces_is_certified_and_answers_for_it() {
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let source = arm(&registry, &["v"]);
    let read = read_of(&registry, source);
    let rebuilt = export_of(&registry, read);
    let (read_port, rebuilt_port) = (only_port(&registry, &read), only_port(&registry, &rebuilt));
    let mut rebuild = registry
        .authority()
        .rebuilding(read, &[read])
        .expect("a read stands over itself");
    rebuild
        .begins_with(&chain_over(&registry, rebuilt))
        .expect("a republication over the read stands in its place");
    rebuild
        .finish(chain_over(&registry, rebuilt))
        .expect("the rebuild closes on its product");
    let names = registry.names();
    assert!(
        crate::relation::replacement(&names, read.relation(), &rebuilt)
            .expect("an epoch-checked read")
            .is_some()
    );
    let sealed = registry.seal();
    let bindings = names.bindings();
    let site = bindings
        .bind_interface(&sealed, &rebuilt)
        .expect("the rebuild emits its own interface");
    assert_eq!(
        bindings
            .at(site, read_port)
            .expect("the rebuild answers the read it stands over"),
        rebuilt_port.column()
    );
}

///
/// Its evidence is the exact-heading map rather than a contribution table,
/// and it has one emitting branch — but the road, the refusals and the
/// answer shape are the run's.
#[test]
fn a_minus_binds_its_left_export_through_the_one_road() {
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let left = arm(&registry, &["a", "b"]);
    let right = arm(&registry, &["a", "b"]);
    let result = registry
        .authority()
        .set_step(
            crate::pipeline::asts::core::SetOperator::MinusCorresponding,
            &[left, right],
        )
        .expect("two exact headings")
        .result();

    let columns = emitted(&registry, &left);
    let binding = registry
        .bindings()
        .bind_export(&registry, &result, &layout(&left, columns.clone()))
        .expect("a minus exports its left operand");
    let row = registry.bindings().branch(binding, 0).expect("one branch");
    assert_eq!(
        row.iter()
            .map(|(_, output)| match output {
                SqlOutput::Slot(slot) => slot.column(),
                SqlOutput::Pad(_) => panic!("a minus never pads"),
            })
            .collect::<Vec<_>>(),
        columns,
        "result position k carries the left operand's kth emitted column"
    );
    assert!(
        registry.bindings().branch(binding, 1).is_err(),
        "a minus emits one branch; its right operand is probed, never stacked"
    );
    let mut short = columns;
    short.pop();
    assert!(
        registry
            .bindings()
            .bind_export(&registry, &result, &layout(&left, short))
            .is_err(),
        "a branch of another width is not this operand being exported"
    );
}

/// One arm's port, republished into a branch under the same name.
///
/// Two crossings of ONE occurrence come back as two distinct columns that
/// share a published name, a value class and a chain — which is to say
/// nothing but the position tells them apart.
fn republished(
    registry: &crate::relation::Planning,
    branch: crate::names::ScopeId,
    port: crate::names::ColId,
) -> crate::names::ColId {
    registry.rebind_sql_column(port, branch, registry.published(port))
}

fn branch_scope(registry: &crate::relation::Planning) -> crate::names::ScopeId {
    registry.anonymous_scope(None)
}

/// A SIBLING CROSSING BINDS WHERE IT STANDS.
///
/// Two crossings of one arm port carry one name, one value class and one
/// chain, so every signal the deleted bridge ran on — republication chain,
/// published name, sole value carrier — either answers identically for both
/// or refuses between them. The layout says which is emitted, and swapping
/// it swaps the answer.
#[test]
fn a_sibling_crossing_binds_where_the_layout_puts_it() {
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let left = arm(&registry, &["a", "b"]);
    let right = arm(&registry, &["a", "b"]);
    let result = step(&registry, POSITIONAL, &[left, right]);
    let ports = emitted(&registry, &left);

    let scope = branch_scope(&registry);
    let first = republished(&registry, scope, ports[0]);
    let second = republished(&registry, scope, ports[0]);
    let tail = republished(&registry, scope, ports[1]);
    assert_ne!(first, second, "two crossings mint two columns");
    assert_eq!(
        registry.published(first),
        registry.published(second),
        "and the two answer to one name"
    );

    let bind = |branch: Vec<crate::names::ColId>| {
        let map = registry.bindings();
        let binding = map
            .bind_run(
                &registry,
                &[result],
                &[layout(&left, branch), laid_out(&registry, &right)],
            )
            .expect("two branches, two arms");
        match map.branch(binding, 0).expect("bound")[0].1 {
            SqlOutput::Slot(slot) => slot.column(),
            SqlOutput::Pad(_) => panic!("a positional set never pads"),
        }
    };
    assert_eq!(bind(vec![first, tail]), first);
    assert_eq!(bind(vec![second, tail]), second);
}

/// REMOVING A BINDING REFUSES rather than being repaired.
///
/// The arm publishes one name at two positions and the branch emits only
/// one column for them. This is exactly where the deleted bridge answered:
/// its chain tier gave that column for the first position and its
/// published-name tier gave the SAME column for the second, so a branch
/// that had lost a column emitted a set anyway, with one column standing in
/// two places.
#[test]
fn removing_one_binding_refuses_rather_than_being_repaired() {
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let left = arm(&registry, &["a", "a"]);
    let right = arm(&registry, &["a", "a"]);
    let result = step(&registry, POSITIONAL, &[left, right]);
    let ports = emitted(&registry, &left);
    let scope = branch_scope(&registry);
    let kept = republished(&registry, scope, ports[0]);

    let map = registry.bindings();
    assert!(
        map.bind_run(
            &registry,
            &[result],
            &[layout(&left, vec![kept]), laid_out(&registry, &right)]
        )
        .is_err(),
        "a branch missing one of its arm's positions is refused, not completed \
         from the position beside it"
    );
}

/// THE BINDING AUTHORITY HAS NO ROAD TO A RECOVERY SIGNAL.
///
/// The runtime witnesses show the binding answering from the layout and the
/// evidence. This is the structural half: the module cannot consult a name,
/// a value class, a chain or a carrier search, because it never names one.
#[test]
fn the_binding_authority_names_no_recovery_signal() {
    // Assembled from halves so neither this file nor the identity-surface
    // inventory fence matches it.
    let forbidden = [
        String::from("published_") + "sym",
        String::from("value_") + "class",
        String::from("descend") + "ant",
        String::from("sole_") + "carrier",
        String::from("corresponding_") + "slots",
        String::from("same_") + "value",
        String::from("progen") + "itor",
        String::from("stable_name_") + "alignment",
        String::from("match_") + "output",
    ];
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("sql_binding");
    let source = std::fs::read_to_string(root.join("mod.rs")).expect("the authority is readable");
    let offenders: Vec<&String> = forbidden
        .iter()
        .filter(|needle| source.contains(needle.as_str()))
        .collect();
    assert!(
        offenders.is_empty(),
        "the physical binding authority reaches a recovery signal: {offenders:?}"
    );
    assert!(
        std::fs::read_dir(&root)
            .expect("the authority directory is readable")
            .filter_map(|entry| entry.ok())
            .all(|entry| {
                let name = entry.file_name();
                name == "mod.rs" || name == "tests.rs"
            }),
        "an unwalked file joined the binding authority"
    );
}

/// A RECEIPT SHELL PUBLISHES ITS HEADING WITH ITS RELATION.
///
/// The shape that used to reach the zero-position road: two effect results
/// unioned. The planner minted their receipt columns after the authority
/// derived them, so the recorded interface said zero while each branch
/// emitted three, and lowering invented a publication to stack them under.
/// Stated at derivation, the set publishes three positions and binds like
/// any other.
#[test]
fn a_scratch_that_states_its_heading_binds_like_any_other_arm() {
    use crate::relation::form::{ScratchSlot, ScratchSpec, ScratchWhy};
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let receipt = |registry: &crate::relation::Planning| {
        let slots: Vec<ScratchSlot> = ["success", "operation", "returned"]
            .iter()
            .enumerate()
            .map(|(position, name)| ScratchSlot {
                position: position as u32,
                named: registry.intern(name, false),
            })
            .collect();
        registry
            .authority()
            .derive(crate::relation::RelForm::Scratch(ScratchSpec::stating(
                ScratchWhy::Result,
                Some(registry.intern("__r_main", false)),
                &slots,
            )))
            .expect("a scratch takes no operand to refuse")
    };
    let left = receipt(&registry);
    let right = receipt(&registry);
    assert_eq!(
        emitted(&registry, &left).len(),
        3,
        "the shell publishes the positions it was stated with"
    );

    let result = step(&registry, CORRESPONDING, &[left, right]);
    let binding = registry
        .bindings()
        .bind_run(
            &registry,
            &[result],
            &[laid_out(&registry, &left), laid_out(&registry, &right)],
        )
        .expect("two receipt shells correspond");
    assert_eq!(
        registry.bindings().branch(binding, 0).expect("bound").len(),
        3,
        "and the set publishes three positions rather than none"
    );
}

/// EVERY SCRATCH ROAD PUBLISHES WHAT ITS TABLE HOLDS.
///
/// The tee, bound-input and hazardous-view snapshots are created FROM a
/// compiled statement's select list, so they republish exactly those
/// occurrences — the stored ports ARE the emitted plan columns, in order,
/// and one of them can stand as a set operand like any other arm.
#[test]
fn a_scratch_holding_a_statement_publishes_the_columns_it_holds() {
    use crate::relation::form::{ScratchSpec, ScratchWhy};
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    // What a compiled statement emits: three occurrences of some scope.
    let source = arm(&registry, &["a", "b", "c"]);
    let emits = crate::relation::published_ports(&registry, &source).expect("source interface");

    let holder = |why| {
        registry
            .authority()
            .derive(crate::relation::RelForm::Scratch(ScratchSpec::holding(
                why,
                Some(registry.intern("__snap", false)),
                &source,
            )))
            .expect("a scratch takes no operand to refuse")
    };
    for why in [ScratchWhy::Tee, ScratchWhy::Insert, ScratchWhy::Snapshot] {
        let scratch = holder(why);
        let ports = emitted(&registry, &scratch);
        assert_eq!(
            ports.len(),
            emits.len(),
            "the stored ports are the emitted plan columns, one for one"
        );
        assert_eq!(
            ports
                .iter()
                .map(|port| registry.published(*port))
                .collect::<Vec<_>>(),
            emits
                .iter()
                .map(|port| registry.published(port.column()))
                .collect::<Vec<_>>(),
            "and each answers to the spelling the statement gave it"
        );
    }

    // AND ONE OF THEM IS A LAWFUL SET OPERAND: the false-zero state that
    // made lowering invent a publication is unrepresentable here.
    let (left, right) = (holder(ScratchWhy::Tee), holder(ScratchWhy::Snapshot));
    let result = step(&registry, CORRESPONDING, &[left, right]);
    assert_eq!(
        emitted(&registry, &result).len(),
        3,
        "a set over two snapshots publishes three positions, not none"
    );
    assert!(registry
        .bindings()
        .bind_run(
            &registry,
            &[result],
            &[laid_out(&registry, &left), laid_out(&registry, &right)]
        )
        .is_ok());
}

/// A SCRATCH THAT STANDS FOR NOTHING AND STATES NOTHING PUBLISHES NOTHING.
///
/// Width zero is a fact, not an omission: a barrier orders and publishes
/// no position, and saying so is different from a shell whose heading
/// arrived later.
#[test]
fn a_barrier_scratch_publishes_no_position() {
    use crate::relation::form::{ScratchSpec, ScratchWhy};
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let barrier = registry
        .authority()
        .derive(crate::relation::RelForm::Scratch(ScratchSpec::stating(
            ScratchWhy::Barrier,
            None,
            &[],
        )))
        .expect("a barrier takes no operand to refuse");
    assert!(emitted(&registry, &barrier).is_empty());
}

/// EVERY PRODUCTION SCRATCH AND PLAN-NOTE ROAD DERIVES ITS HEADING.
///
/// The inventory, walked rather than recited: every construction of a
/// `ScratchSpec` or of a plan note in production, and the assertion that
/// none of them can publish afterwards. `ScratchSpec` has no public field
/// — its two constructors are the only roads — so a road that wanted a
/// late heading would have to add one here first.
///
/// The runtime half is the two witnesses above plus
/// `a_scratch_holding_a_statement_publishes_the_columns_it_holds`; this is
/// the structural half, and what it watches is that no NEW road appears
/// that could grow one.
#[test]
fn every_scratch_and_note_road_states_its_heading_at_derivation() {
    /// Every production site that derives a plan-lifetime relation, and
    /// the shape it states.
    const ROADS: &[(&str, &str)] = &[
        // The effect planner's one allocation, reached by every scratch.
        ("pipeline/effect_transformer/mod.rs", "ScratchSpec::holding"),
        ("pipeline/effect_transformer/mod.rs", "ScratchSpec::stating"),
        // The created object a plan note stands for.
        ("pipeline/effect_transformer/mod.rs", "SourceSpec {"),
        // DML staging, which knows its source heading before it stages.
        ("pipeline/resolver/resolver_fold.rs", "ScratchSpec::holding"),
        // Test-only construction of the zero-width compiler scratch.
        ("relation/mod.rs", "ScratchSpec::stating"),
        // Focused witnesses construct their scratch with the same closed form.
        (
            "defuse/environment/scoped/effect/tests.rs",
            "ScratchSpec::stating",
        ),
        (
            "pipeline/resolver/plan_note_injection_tests.rs",
            "ScratchSpec::stating",
        ),
    ];
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for (file, road) in ROADS {
        let text = std::fs::read_to_string(src.join(file)).expect("source file is readable");
        assert!(
            text.contains(road),
            "{file} no longer takes {road}, so this inventory watches nothing"
        );
    }

    // No source file constructs a `ScratchSpec` any other way. The
    // struct's fields are private, so this is a change detector over the
    // two constructors rather than the wall itself.
    let mut files = Vec::new();
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("source tree is readable") {
            let path = entry.expect("directory entry is readable").path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
    walk(&src, &mut files);
    let constructions: usize = files
        .iter()
        .filter(|path| !path.ends_with("sql_binding/tests.rs"))
        .map(|path| {
            std::fs::read_to_string(path)
                .expect("source file is readable")
                .matches(&(String::from("ScratchSpec") + "::"))
                .count()
        })
        .sum();
    assert_eq!(
        constructions, 6,
        "a scratch construction road appeared or vanished; each one states \
         its heading at derivation, and this count is where a reviewer asks \
         whether the new one does"
    );

    // The post-derivation publication road is GONE, not merely unused.
    // Named as a CALL so the resolver's plan-note test modules — which name
    // the feature, not the road — do not read as a reintroduction.
    let bricked = String::from("plan_") + "note(";
    let offenders: Vec<String> = files
        .iter()
        .filter(|path| {
            std::fs::read_to_string(path)
                .expect("source file is readable")
                .contains(&bricked)
        })
        .map(|path| path.display().to_string())
        .collect();
    assert!(
        offenders.is_empty(),
        "the road that published into a scope after its relation was derived \
         is back: {offenders:?}"
    );
}

/// A PLAN SCRATCH HOLDING A SELECT LIST REPUBLISHES IT: the stored
/// positions are occurrences of their own, stated at their birth — a stage
/// over the scratch continues the stored position, not the select list's.
#[test]
fn a_scratch_holding_a_select_list_republishes_its_positions() {
    use crate::relation::form::{
        AnonymousShape, AnonymousSlot, AnonymousSpec, ExportSpec, ExportWhy, ScratchSpec,
        ScratchWhy,
    };
    use crate::relation::{published_ports, Planning, RelForm};
    let planning = Planning::open(crate::names::Registry::new(&[]));
    let slots = [AnonymousSlot::Binder {
        position: 0,
        named: planning.intern("x", false),
        declared_type: None,
        shape: crate::names::ValueShape::Unknown,
    }];
    let base = planning
        .authority()
        .derive(RelForm::Anonymous(AnonymousSpec::plain(
            AnonymousShape::Tabular,
            &slots,
            None,
        )))
        .expect("an anonymous relation derives");
    let stage = |input| {
        planning
            .authority()
            .derive(RelForm::Export(ExportSpec {
                input,
                why: ExportWhy::Stage,
            }))
            .expect("a stage derives")
    };
    let emitted = stage(base);
    let scratch = planning
        .authority()
        .derive(RelForm::Scratch(ScratchSpec::holding(
            ScratchWhy::Snapshot,
            None,
            &emitted,
        )))
        .expect("a scratch derives");
    let held = published_ports(&planning, &emitted).expect("interface")[0];
    let stored = published_ports(&planning, &scratch).expect("interface")[0];
    assert!(
        !planning.continues_occurrence(stored, held),
        "a stored position is an occurrence of its own, not the select list's"
    );
    let read = published_ports(&planning, &stage(scratch)).expect("interface")[0];
    assert!(
        planning.continues_occurrence(read, stored),
        "a stage over the scratch continues the stored position"
    );
}
