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
        .derive(RelForm::Anonymous(AnonymousSpec {
            shape: AnonymousShape::Tabular,
            slots: &slots,
            answers_to: None,
        }))
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
            dependencies: &[],
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

/// A MINUS BINDS THROUGH THE SAME AUTHORITY.
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
            "pipeline/effect_transformer/tests.rs",
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
        .derive(RelForm::Anonymous(AnonymousSpec {
            shape: AnonymousShape::Tabular,
            slots: &slots,
            answers_to: None,
        }))
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
