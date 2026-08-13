// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! ACCESS IS A CONTINUATION — the discriminating pins and the structural
//! fences for the one carrier.
//!
//! Every assertion here reads the typed tree. A pin that matched source text
//! or a variant's spelling would pass over a second carrier reintroduced
//! under a different name, which is the one thing these exist to catch.

use super::support::*;
use crate::pipeline::asts::core::operators::{HoArgument, PipeOp};
use crate::pipeline::asts::core::*;

/// The chain a query is, when it is one relational expression.
fn chain(source: &str) -> Chain<Unresolved> {
    let query = query(source);
    match query.into_bare_body() {
        Ok(chain) => chain,
        Err(other) => panic!("expected a relational query, got {other:?}"),
    }
}

/// Every access a chain carries, in authored order — read off the carrier,
/// never off a spelling.
fn accesses(chain: &Chain<Unresolved>) -> Vec<&Access<Unresolved>> {
    chain
        .continuations
        .iter()
        .filter_map(|continuation| match continuation {
            Continuation::Access { access, .. } => Some(access),
            _ => None,
        })
        .collect()
}

/// A tag for an access ALTERNATIVE, so a pin can say which of the five was
/// built without depending on how one prints.
fn tag(access: &Access<Unresolved>) -> &'static str {
    match access {
        Access::Unasked => "unasked",
        Access::All => "all",
        Access::Slots(_) => "slots",
        Access::Dequalify(_) => "dequalify",
        Access::DequalifyAll => "dequalify-all",
    }
}

// ---------------------------------------------------------------------
// One carrier, every position
// ---------------------------------------------------------------------

/// `users(*)` and `users()*` are the same query, so the mention's own parens
/// and the postfix step are the same continuation at the same index. The
/// TYPES cannot say this — both spellings could have built different nodes —
/// so the tree is read.
#[test]
fn absorbed_and_postfix_access_are_one_structural_carrier() {
    let absorbed = chain("users(*.(a))");
    let postfix = chain("users()*.(a)");
    assert_eq!(lispy_chain(&absorbed), lispy_chain(&postfix));
    assert_eq!(
        accesses(&absorbed).into_iter().map(tag).collect::<Vec<_>>(),
        vec!["dequalify"],
    );
    assert_eq!(absorbed.head_span(), 1, "the access is the head's own read");
    assert!(!absorbed.has_steps(), "and nothing else consumed it");
}

/// The five alternatives reach the one carrier from the spellings that name
/// them. Whole activation, the selected merge, and the every-name merge are
/// three values of one type in one position, not three positions.
#[test]
fn the_access_alternatives_reach_one_position() {
    for (source, expected) in [
        ("users()", "unasked"),
        ("users(*)", "all"),
        ("users(*.(a))", "dequalify"),
        ("users(.*)", "dequalify-all"),
        ("users(id, name, _, _, _, _, _, _, _, _)", "slots"),
    ] {
        let read = chain(source);
        assert_eq!(
            accesses(&read).into_iter().map(tag).collect::<Vec<_>>(),
            vec![expected],
            "{source}"
        );
        assert_eq!(read.head_span(), 1, "{source}: the read carries its access");
    }
}

/// A GROUND read and a CALLABLE relation publish through the same
/// continuation shape: the access group of `f(x)(*)` stands where the parens
/// of `users(*)` stand.
#[test]
fn a_ground_read_and_a_callable_relation_share_the_shape() {
    let ground = chain("users(*)");
    let callable = chain("wrap(users(*))(*)");
    assert!(matches!(
        ground.head,
        Grelex::Reference(Relation::Ground { .. })
    ));
    assert!(matches!(
        callable.head,
        Grelex::Reference(Relation::FunctorCall { .. })
    ));
    for read in [&ground, &callable] {
        assert_eq!(read.head_span(), 1);
        assert_eq!(
            accesses(read).into_iter().map(tag).collect::<Vec<_>>(),
            ["all"]
        );
    }
}

/// A caller pattern has already said which dimensions the mention asks for,
/// so the run after it is a STEP on the result — a second access at index 1,
/// in the same carrier as the first.
#[test]
fn a_step_access_stands_past_the_read() {
    let stepped = chain("users(id, a, b, c, d, e, g, h, i, j).(id)");
    assert_eq!(
        accesses(&stepped).into_iter().map(tag).collect::<Vec<_>>(),
        vec!["slots", "dequalify"],
    );
    assert_eq!(stepped.head_span(), 1, "the pattern is the read's");
    assert!(
        matches!(stepped.steps(), [Continuation::Access { .. }]),
        "the run is one step on the read's result: {:?}",
        stepped.steps()
    );
}

/// An ANONYMOUS table has no read to parameterize, so an access standing
/// after one is a step and never the head's own.
#[test]
fn a_literal_head_takes_no_access_of_its_own() {
    let melted = chain("_(1; 2)*");
    assert!(matches!(melted.head, Grelex::Literal(_)));
    assert_eq!(melted.head_span(), 0);
    assert!(matches!(melted.steps(), [Continuation::Access { .. }]));
}

// ---------------------------------------------------------------------
// Identity and stopping
// ---------------------------------------------------------------------

/// A STROP is what makes a name case-sensitive, and the lvar a dequalifying
/// step renames onto is found by the name the author spelled. The carrier
/// holds the spelling as written, in both positions.
#[test]
fn stropped_access_names_keep_their_identity() {
    for source in ["users(*.(`Id`))", "users()*.(`Id`)"] {
        let read = chain(source);
        let [Access::Dequalify(columns)] = accesses(&read)[..] else {
            panic!("{source}: expected one dequalifying access");
        };
        assert_eq!(columns.len(), 1, "{source}");
        assert_eq!(columns[0].as_str(), "Id", "{source}: the spelling is kept");
        assert!(columns[0].is_stropped(), "{source}: and so is the strop");
    }
}

/// A dequalifying step separated from its relation by a RESTRICTION is a
/// different query from one standing directly on it, and the two must not
/// normalize alike. The stopping behaviour is the carrier's position, so
/// reading it is reading where the access stands.
#[test]
fn a_restriction_stops_the_run_from_reaching_the_read() {
    let direct = chain("users(id, a, b, c, d, e, g, h, i, j).(id)");
    let separated = chain("users(id, a, b, c, d, e, g, h, i, j), age > 1 .(id)");
    assert_ne!(lispy_chain(&direct), lispy_chain(&separated));
    assert!(
        matches!(
            separated.steps(),
            [Continuation::Restrict { .. }, Continuation::Access { .. }]
        ),
        "the restriction stands between the read and the step: {:?}",
        separated.steps()
    );
}

// ---------------------------------------------------------------------
// Effects
// ---------------------------------------------------------------------

/// A RECEIPT IS THE ORDINARY ACCESS, standing in the effect position. The
/// direct spelling and the piped spelling reach the same carrier holding the
/// same value; what differs is the source's argument role, which is the
/// call's.
#[test]
fn direct_and_piped_receipts_reach_one_carrier() {
    // `doc!` declares its arguments, so both spellings are lawful: the
    // direct one writes them, the piped one adds a relation beside them.
    // (`stdout!` takes no message argument — EFFECT-ALGEBRA — so its two
    // spellings could not be compared here.)
    let direct = chain("doc!(\"main.users\", \"d\")(*)");
    let piped = chain("users(*) |> doc!(\"main.users\", \"d\")(*)");
    for read in [&direct, &piped] {
        assert!(matches!(
            read.head,
            Grelex::Reference(Relation::FunctorCall { .. })
        ));
        assert_eq!(
            accesses(read).into_iter().map(tag).collect::<Vec<_>>(),
            ["all"],
            "the receipt is the access after the call",
        );
        assert_eq!(read.head_span(), 1);
        assert!(read.steps().is_empty(), "a receipt leaves no operator");
    }
    // The piped spelling differs in ONE thing: the source stands as a
    // relation argument at the formal the landing chose — no role marks it,
    // so nothing downstream can tell a piped call from a direct one that
    // wrote the same relation there.
    assert!(
        call_of(&piped).relations().next().is_some(),
        "the piped source is a relation argument at its landing position",
    );
    assert!(call_of(&direct).relations().next().is_none());
}

/// A MUTATION's target is the call's argument and its source is the piped
/// relation: the roles are the descriptor's, and moving the receipt out of
/// call identity does not touch them.
#[test]
fn dml_target_and_source_roles_are_unchanged() {
    let mutation = chain("orders!!(*), total > 1 |> update!(orders(*))(*)");
    let call = call_of(&mutation);
    // THE POSITION IS THE ROLE: a mutation's layout is [target, source] —
    // the authored destination first, the piped relation after it — so the
    // descriptor reads both off their formals.
    let relations: Vec<_> = call.relations().collect();
    assert_eq!(
        relations.len(),
        2,
        "a mutation carries its target and its source as relation arguments",
    );
    assert!(
        relations[0].steps().is_empty(),
        "the authored target stands first",
    );
    assert!(
        !relations[1].steps().is_empty(),
        "the piped source, restriction and all, stands second",
    );
    assert_eq!(
        accesses(&mutation).into_iter().map(tag).collect::<Vec<_>>(),
        ["all"],
        "and the receipt is the access standing after the terminal",
    );
}

/// A LIMINAL statement takes the WHOLE receipt. The gate reads the access
/// where it now stands, so a reshaping receipt is still refused and the whole
/// one is still admitted.
#[test]
fn a_liminal_directive_still_demands_a_whole_receipt() {
    let whole = chain("engage!(\"ns\")(*)");
    assert_eq!(
        accesses(&whole).into_iter().map(tag).collect::<Vec<_>>(),
        ["all"]
    );
    let reshaping = chain("engage!(\"ns\")(namespace)");
    assert_eq!(
        accesses(&reshaping)
            .into_iter()
            .map(tag)
            .collect::<Vec<_>>(),
        ["slots"],
        "the gate has a shaping receipt to see, and refuses it",
    );
}

/// An access AND a destructure in one chain stay two steps of one run. The
/// access is a run step, not a relation the refiner stores whole and refines
/// again — which is how it re-entered the flatten/rebuild/refine cycle.
#[test]
fn an_access_beside_a_destructure_stays_one_run() {
    let both = chain("users(*) |> (id), payload ~= {.a} .(a)");
    assert!(
        matches!(
            both.steps(),
            [
                Continuation::Pipe { .. },
                Continuation::Destructure { .. },
                Continuation::Access { .. }
            ]
        ),
        "three steps on one read, none nesting the chain that holds it: {:?}",
        both.steps()
    );
    assert_eq!(both.head_span(), 1);
}

// ---------------------------------------------------------------------
// The fences
// ---------------------------------------------------------------------

/// THE GROUND READ HOLDS NO ACCESS. Exhaustive destructuring is the fence: a
/// field added back here stops this compiling, so a second carrier cannot
/// reappear beside the continuation.
#[test]
fn a_ground_relation_has_no_access_field() {
    let read = chain("users(*)");
    let Grelex::Reference(Relation::Ground {
        mention: _,
        outer: _,
        cpr_schema: _,
    }) = &read.head
    else {
        panic!("expected a ground read");
    };
}

/// THE CALL HOLDS NO ACCESS, NO GUARD AND NO WINDOW. The same fence over
/// call identity: a receipt field, an interior, an access group, or the
/// scalar position's guard and window added back stops this compiling.
#[test]
fn a_call_has_no_access_field() {
    let read = chain("stdout!(\"x\")(*)");
    let FunctorCall {
        callee: _,
        arguments: _,
        marks: _,
    } = call_of(&read);
}

/// NO PIPE OPERATOR IS AN ACCESS. The exhaustive match is the fence: an
/// access-bearing operator variant added back stops this compiling, so a
/// dimension access cannot travel as an operator again.
#[test]
fn no_unary_operator_carries_an_access() {
    fn operator_is_not_an_access(operator: &PipeOp<Unresolved>) -> bool {
        use PipeOp as Op;
        match operator {
            Op::Project(_)
            | Op::Embed(_)
            | Op::Group(_)
            | Op::MapCover { .. }
            | Op::ProjectOut(_)
            | Op::Rename(_)
            | Op::Transform { .. }
            | Op::EmbedMapCover { .. } => true,
        }
    }

    let piped = chain("users(*) |> (id) .(id)");
    for continuation in &piped.continuations {
        if let Continuation::Pipe { operator, .. } = continuation {
            assert!(operator_is_not_an_access(operator));
        }
    }
}

/// THE READING RULE IS ONE RULE. A relation that has a read to parameterize
/// answers yes and every other answers no, so no walk can decide for itself
/// whether a leading access belongs to the head.
#[test]
fn only_a_mention_takes_a_leading_access() {
    assert!(chain("users(*)").head_takes_an_access());
    assert!(chain("stdout!(\"x\")(*)").head_takes_an_access());
    assert!(!chain("_(1; 2)").head_takes_an_access());
    assert!(
        !chain("users(, age > 1)").head_takes_an_access(),
        "a derived table spent its access where the interior said it",
    );
}

// ---------------------------------------------------------------------
// Helpers that read the tree
// ---------------------------------------------------------------------

fn call_of(chain: &Chain<Unresolved>) -> &FunctorCall<Unresolved> {
    match &chain.head {
        Grelex::Reference(Relation::FunctorCall { call, .. }) => call.call(),
        other => panic!("expected a callable head, got {other:?}"),
    }
}

fn lispy_chain(chain: &Chain<Unresolved>) -> String {
    use crate::lispy::ToLispy;
    chain.to_lispy()
}
