// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

//! The formal-row grammar for closed residual rule values.
//!
//! **THE PARAMETER ROW IS AN INBOUND MODE.** The first group of a
//! parameterized relational or effect rule is one caller-supplied row whose
//! members state their own order: a bare formal is order 0, `T(*)` and
//! `T(a, b)` are order 1, a ground member is a clause discriminator, and
//! `P(... T(*))(*)` is an order-2 closed residual. Its ellipsis hides the
//! lawful prefix already sealed, while the following members and final group
//! state the exact remaining mode and publication. The final group after the
//! enclosing parameter row is still the enclosing rule's published head.
//!
//! **NO PHANTOM BINDER.** `P(T(*))(*)` is not the formal spelling: the
//! existential seal is mandatory. Nor is there an order above three, a
//! recursively rule-valued remaining member, or a rule-valued result.
//!
//! **EVERY NECK, ONE LAW.** The query-scoped neck reuses `ho_param`
//! unchanged, so a formal admitted in a file is admitted in a CHOE.
//!
//! Every refusal below is asserted in the SAME test as an admission, because
//! a grammar that refused every formal row would otherwise satisfy them all.

mod support;

use delightql_cst::cst::*;
use support::{admits, admits_file, count, refuses};

/// The ruling's own signature: three inbound members of three different
/// orders, and a body that applies the order-2 one.
#[test]
fn the_inbound_row_admits_one_order_two_member() {
    let tree = admits_file("verify(I(*), P(... T(*))(*), label)(*) :- I(*) |> P(*)");
    assert_eq!(count::<HoRule>(&tree), 1);
    assert_eq!(count::<RuleParam>(&tree), 1);
}

/// A rule-valued formal is lawful anywhere in the row, and several of them do
/// not raise the order.
#[test]
fn several_rule_valued_formals_share_one_row() {
    assert_eq!(
        count::<RuleParam>(&admits_file("compose(I(*), P(... T(*))(*), Q(... T(*))(*))(*) :- I(*) |> P(*) |> Q(*)")),
        2,
    );
    assert_eq!(
        count::<RuleParam>(&admits_file("first_slot(P(... T(*))(*), I(*))(*) :- I(*) |> P(*)")),
        1,
        "the order-2 member may stand first",
    );
}

/// EVERY NECK, ONE LAW — the query-scoped neck reuses `ho_param` unchanged,
/// so the same row is admitted there.
#[test]
fn the_query_scoped_neck_takes_the_same_row() {
    let tree = admits("local(I(*), P(... T(*))(*), label)(*) : I(*) |> P(*)  local(users(*), gtzero(*), \"x\")(*)");
    assert_eq!(count::<RuleParam>(&tree), 1);
    assert_eq!(count::<HoCte>(&tree), 1);
}

/// The effect mirror's inbound row is the same parameter row.
#[test]
fn the_effect_mirror_takes_the_same_row() {
    let tree = admits_file("stamp!(I(*), P(... T(*))(*))(*) :- I(*) |> P(*) |> exit!(*)");
    assert_eq!(count::<RuleParam>(&tree), 1);
}

/// NO PHANTOM BINDER. Paired with the admission so that refusing everything
/// cannot satisfy this test.
#[test]
fn an_inner_binder_is_not_the_formal_spelling() {
    admits_file("verify(I(*), P(... T(*))(*))(*) :- I(*) |> P(*)");
    refuses("verify(I(*), P(T(*))(*))(*) :- I(*) |> P(*)");
}

/// A fourth group is not a fourth order. Paired with the admission.
#[test]
fn there_is_no_order_above_three() {
    admits_file("tower(I(*), P(... T(*))(*))(*) :- I(*) |> P(*)");
    refuses("tower(I(*), P(... Q(... T(*))(*))(*))(*) :- I(*) |> P(*)");
}

/// Input and output headings are structural members of the residual contract.
#[test]
fn a_rule_valued_formal_carries_structural_headings() {
    admits_file("shaped(I(*), P(... T(*))(*))(*) :- I(*) |> P(*)");
    admits_file("shaped(I(*), P(... T(id, n))(id, n))(*) :- I(*) |> P(*)");
    admits_file("shaped(I(*), U(id, n))(*) :- I(*), U(*)");
    refuses("shaped(I(*), P(... T(id, n))())(*) :- I(*) |> P(*)");
}

/// NO RULE-VALUED RESULT — the final group is the enclosing rule's published
/// relational head, and a head is an ordered projection of its body's heading
/// (heads-law). Paired with the admission of the same rule's lawful head.
#[test]
fn the_head_group_is_not_a_rule_value() {
    admits_file("gives(I(*), P(... T(*))(*))(*) :- I(*) |> P(*)");
    admits_file("gives(I(*), P(... T(*))(*))(id, n) :- I(*) |> P(*)");
    refuses("gives(I(*), P(... T(*))(*))(P(... T(*))(*)) :- I(*) |> P(*)");
}
