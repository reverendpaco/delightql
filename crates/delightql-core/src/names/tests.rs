// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The five cases that decide the representation.
//!
//! These are the prototypes Phase D asked for, written against the types
//! rather than beside them. A module that merely compiles proves nothing; a
//! module in which the wrong thing cannot be written proves the constraint.
//!
//! Each case names the defect it would reproduce under today's model.

use super::sink::Probe;
use super::*;

/// A registry with one two-column table, for the cases that need a schema.
fn users(reg: &Registry) -> (EntityId, ScopeId, ColId, ColId) {
    let name = reg.intern("users", false);
    let entity = reg.mint_entity(name);
    let scope = reg.mint_scope(ScopeOrigin::BaseTable { entity }, Hint::User(name), None);
    let id_sp = reg.intern("id", false);
    let last_sp = reg.intern("last_name", false);
    let id = reg.mint_column(
        scope,
        ColumnOrigin::CatalogColumn {
            entity,
            position: 0,
        },
        Some(id_sp),
        Addressing::Published,
        ValueFacts::default(),
    );
    let last = reg.mint_column(
        scope,
        ColumnOrigin::CatalogColumn {
            entity,
            position: 1,
        },
        Some(last_sp),
        Addressing::Published,
        ValueFacts::default(),
    );
    (entity, scope, id, last)
}

// Assertions go through the test sink, because there is no road out of a
// `Baptised` that returns characters — which is the property under test as
// much as anything else here.
fn spell_col(b: &Baptised<'_>, c: ColId) -> String {
    let mut out = String::new();
    b.write_column(c, &mut Probe(&mut out));
    out
}

fn spell_scope(b: &Baptised<'_>, s: ScopeId) -> String {
    let mut out = String::new();
    b.write_scope(s, &mut Probe(&mut out));
    out
}

// -------------------------------------------------------------------------
// Case 1 — two equal names with different authored spellings
// -------------------------------------------------------------------------
//
// Today's failure: a single interned value is both the comparison key and
// the record of what was typed, so whichever spelling interns second is
// lost and the user is echoed a name they did not write.

#[test]
fn equal_names_keep_their_separate_spellings() {
    let reg = Registry::new(&[]);
    let upper = reg.intern("Name", false);
    let lower = reg.intern("name", false);

    // One identity...
    assert_eq!(reg.canonical(upper), reg.canonical(lower));
    // ...two spellings.
    assert_ne!(upper, lower);

    let mut a = String::new();
    let mut b = String::new();
    reg.write(upper, &mut Probe(&mut a));
    reg.write(lower, &mut Probe(&mut b));
    assert_eq!(a, "Name");
    assert_eq!(b, "name");
}

#[test]
fn stropping_is_carried_not_inferred() {
    let reg = Registry::new(&[]);
    let bare = reg.intern("Name", false);
    let stropped = reg.intern("Name", true);

    // Folded iff unstropped: `Name` stropped is not `name`.
    assert_ne!(
        reg.canonical(stropped),
        reg.canonical(reg.intern("name", false))
    );
    // But the unstropped `Name` is.
    assert_eq!(
        reg.canonical(bare),
        reg.canonical(reg.intern("name", false))
    );

    // The sink receives the bit rather than guessing from characters.
    let mut out = String::new();
    reg.write(stropped, &mut Probe(&mut out));
    assert_eq!(out, "`Name`");
}

// -------------------------------------------------------------------------
// Case 2 — the optimizer removes a boundary while the old tree is held
// -------------------------------------------------------------------------
//
// Today's failure: rebinding mutates identity globally, so the
// pre-optimization tree the compiler still holds is silently reinterpreted.

#[test]
fn republishing_does_not_mutate_the_source() {
    let reg = Registry::new(&[]);
    let (_e, base, id, _last) = users(&reg);

    let stage = reg.mint_scope(
        ScopeOrigin::PipeStage { input: base },
        Hint::None,
        Some(base),
    );
    let carried = reg.republish_heading(base, stage, Republish::Passthrough);

    // The new occurrence is a different identity...
    assert_ne!(*carried.in_order().next().unwrap(), id);
    // ...that still lives in its own scope...
    assert_eq!(reg.scope_of(*carried.in_order().next().unwrap()), stage);
    assert_eq!(reg.scope_of(id), base);
    // ...and the source column means exactly what it meant.
    assert_eq!(
        reg.origin_of_col(id),
        ColumnOrigin::CatalogColumn {
            entity: _e,
            position: 0
        }
    );
    // The link back survives an arbitrary number of boundaries.
    assert_eq!(reg.progenitor(*carried.in_order().next().unwrap()), id);
    assert!(reg.same_value(*carried.in_order().next().unwrap(), id));
}

#[test]
fn a_rename_chain_still_answers_to_one_value() {
    let reg = Registry::new(&[]);
    let (_e, base, id, _last) = users(&reg);

    let mut cur = id;
    let mut scope = base;
    for _ in 0..4 {
        let next_scope = reg.mint_scope(
            ScopeOrigin::PipeStage { input: scope },
            Hint::None,
            Some(scope),
        );
        cur = reg.mint_column(
            next_scope,
            ColumnOrigin::Republished {
                from: cur,
                how: Republish::Rename,
            },
            Some(reg.intern("k", false)),
            Addressing::Published,
            ValueFacts::default(),
        );
        scope = next_scope;
    }
    assert_eq!(reg.progenitor(cur), id);
    assert!(reg.same_value(cur, id));
}

#[test]
fn correspondence_preserves_distinct_anonymous_slots() {
    let reg = Registry::new(&[]);
    let arm = reg.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
    let first = reg.mint_column(
        arm,
        ColumnOrigin::Bound { position: 0 },
        None,
        Addressing::Bare,
        ValueFacts::default(),
    );
    let second = reg.mint_column(
        arm,
        ColumnOrigin::Bound { position: 1 },
        None,
        Addressing::Bare,
        ValueFacts::default(),
    );

    assert_eq!(
        reg.corresponding_slot(first, &[first, second]),
        Ok(Some(first))
    );
    let merged = reg
        .merge_corresponding(&[arm])
        .expect("the heading is unambiguous")
        .expect("one arm produces a heading");
    assert_eq!(
        reg.known_heading(merged)
            .expect("a heading this test built is known")
            .len(),
        2
    );
}

#[test]
fn correspondence_refuses_duplicate_authored_bindings() {
    let reg = Registry::new(&[]);
    let arm = reg.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
    let name = reg.intern("a", false);
    let symbol = reg.canonical(name);
    let first = reg.mint_column(
        arm,
        ColumnOrigin::Bound { position: 0 },
        Some(name),
        Addressing::BareAnswering(symbol),
        ValueFacts::default(),
    );
    let second = reg.mint_column(
        arm,
        ColumnOrigin::Bound { position: 1 },
        Some(name),
        Addressing::BareAnswering(symbol),
        ValueFacts::default(),
    );
    let other_arm = reg.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
    reg.mint_column(
        other_arm,
        ColumnOrigin::Bound { position: 0 },
        Some(name),
        Addressing::BareAnswering(symbol),
        ValueFacts::default(),
    );

    assert_eq!(
        reg.corresponding_slot(first, &[first, second]),
        Err(CorrespondenceError::Ambiguous)
    );
    assert_eq!(
        reg.merge_corresponding(&[arm, other_arm]),
        Err(CorrespondenceError::Ambiguous)
    );
}

#[test]
fn correspondence_aligns_repeated_published_occurrences_by_rank() {
    let reg = Registry::new(&[]);
    let name = reg.intern("success", false);
    let make_heading = |scope| {
        (0..2)
            .map(|position| {
                reg.mint_column(
                    scope,
                    ColumnOrigin::Bound { position },
                    Some(name),
                    Addressing::Published,
                    ValueFacts::default(),
                )
            })
            .collect::<Vec<_>>()
    };
    let outputs = make_heading(reg.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None));
    let candidates = make_heading(reg.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None));

    assert_eq!(
        reg.corresponding_slots(&outputs, &candidates),
        Ok(vec![Some(candidates[0]), Some(candidates[1])])
    );
    assert_eq!(
        reg.corresponding_slot(outputs[0], &candidates),
        Err(CorrespondenceError::Ambiguous)
    );
}

#[test]
fn correspondence_prefers_a_republication_chain_to_weaker_matches() {
    let reg = Registry::new(&[]);
    let (_entity, base, id, _last) = users(&reg);
    let projected = reg.mint_derived_scope(
        ScopeOrigin::Wrap {
            input: base,
            why: WrapReason::Projection,
        },
        Hint::None,
    );
    let first = reg.republish_column(
        id,
        projected,
        Republish::Passthrough,
        reg.published(id),
        Addressing::Published,
        |_| {},
    );
    let second = reg.republish_column(
        id,
        projected,
        Republish::Rename,
        Some(reg.intern("other_id", false)),
        Addressing::Published,
        |_| {},
    );

    assert_eq!(
        reg.corresponding_slot(first, &[first, second]),
        Ok(Some(first))
    );
    assert_eq!(
        reg.corresponding_slot(second, &[first, second]),
        Ok(Some(second))
    );
}

// -------------------------------------------------------------------------
// Case 3 — one temporary object across several statements of one program
// -------------------------------------------------------------------------
//
// Today's failure: naming is sealed per statement, so a scratch table
// written in one statement and read in another can be named twice.

#[test]
fn a_scratch_table_gets_one_name_across_a_bundle() {
    let reg = Registry::new(&[]);
    let scratch = reg.mint_scope(
        ScopeOrigin::Scratch {
            role: ScratchRole::Snapshot,
        },
        Hint::None,
        None,
    );
    let col = reg.mint_column(
        scratch,
        ColumnOrigin::Minted {
            by: MintReason::RowNumber,
        },
        None,
        Addressing::Hygienic,
        ValueFacts::default(),
    );

    // Three statements: create, fill, read — all naming the same scope.
    let stmt = |refs: Vec<ColId>| Statement {
        scopes: vec![scratch],
        headings: vec![vec![col]],
        refs,
    };
    let bundle = Bundle {
        statements: vec![stmt(vec![]), stmt(vec![col]), stmt(vec![col])],
    };

    let b = baptise(&reg, &bundle).expect("bundle names cleanly");
    assert!(b.knows_scope(scratch));
    let name = spell_scope(&b, scratch);
    assert!(!name.is_empty());
    // Named once. If naming were per-statement this would be three names,
    // and there would be no way to ask this question at all.
    assert_eq!(spell_scope(&b, scratch), name);
}

#[test]
fn a_reference_to_an_unnamed_scope_refuses() {
    let reg = Registry::new(&[]);
    let orphan = reg.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
    let col = reg.mint_column(
        orphan,
        ColumnOrigin::Bound { position: 0 },
        None,
        Addressing::Bare,
        ValueFacts::default(),
    );
    // The statement references the column but never declares its scope.
    let bundle = Bundle {
        statements: vec![Statement {
            scopes: vec![],
            headings: vec![],
            refs: vec![col],
        }],
    };
    // Matched rather than compared: `Baptised` deliberately has no `Debug`,
    // because a value that can print the name table is a road to characters.
    match baptise(&reg, &bundle) {
        Err(e) => assert_eq!(e, BaptismError::DanglingScope { col, scope: orphan }),
        Ok(_) => panic!("a reference to an unnamed scope must refuse"),
    }
}

// -------------------------------------------------------------------------
// Case 4 — the same relation resolved from two lexical positions
// -------------------------------------------------------------------------
//
// Today's failure: a visible-set attached to the relation, so a correlated
// subquery and a join condition see the same thing when they must not.

#[test]
fn two_accesses_to_one_table_are_two_scopes() {
    let reg = Registry::new(&["users"]);
    let (_e1, left, left_id, _l) = users(&reg);
    let (_e2, right, right_id, _r) = users(&reg);

    assert_ne!(left, right);
    assert_ne!(left_id, right_id);
    // Same catalog column, different occurrences — so NOT the same value.
    assert!(!reg.same_value(left_id, right_id));

    let bundle = Bundle {
        statements: vec![Statement {
            scopes: vec![left, right],
            headings: vec![vec![left_id], vec![right_id]],
            refs: vec![left_id, right_id],
        }],
    };
    let b = baptise(&reg, &bundle).unwrap();
    // The first access keeps the authored catalog spelling; the second
    // occurrence is distinct without changing what either scope denotes.
    assert_eq!(spell_scope(&b, left), "users");
    assert_eq!(spell_scope(&b, right), "users_2");
}

#[test]
fn visibility_belongs_to_the_position_not_the_relation() {
    let reg = Registry::new(&[]);
    let (_e1, outer, outer_id, _a) = users(&reg);
    let (_e2, inner, inner_id, _b) = users(&reg);

    let id = reg.canonical(reg.intern("id", false));
    let bare = Reference {
        qualifier: None,
        name: id,
    };

    // At a position that sees only the outer relation, `id` is unambiguous.
    let at_outer = ScopeEnv::at(vec![outer]);
    assert_eq!(reg.address(bare, &at_outer), Ok(outer_id));

    // At a position that sees only the inner one, the SAME reference
    // resolves elsewhere — the relation did not change, the position did.
    let at_inner = ScopeEnv::at(vec![inner]);
    assert_eq!(reg.address(bare, &at_inner), Ok(inner_id));

    // At a position that sees both, it is ambiguous rather than silently
    // picking one.
    let at_join = ScopeEnv::at(vec![outer, inner]);
    assert_eq!(reg.address(bare, &at_join), Err(AddressError::Ambiguous));
}

#[test]
fn an_anonymous_scope_does_not_win_an_ambiguous_bare_reference() {
    let reg = Registry::new(&[]);
    let (_entity, named, _named_id, _last) = users(&reg);
    let anonymous = reg.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
    let spelling = reg.intern("id", false);
    reg.mint_column(
        anonymous,
        ColumnOrigin::Bound { position: 0 },
        Some(spelling),
        Addressing::Published,
        ValueFacts::default(),
    );

    let reference = Reference {
        qualifier: None,
        name: reg.canonical(spelling),
    };
    assert_eq!(
        reg.address(reference, &ScopeEnv::at(vec![named, anonymous])),
        Err(AddressError::Ambiguous)
    );
}

#[test]
fn a_hygienic_column_answers_to_nothing() {
    let reg = Registry::new(&[]);
    let scope = reg.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
    let sp = reg.intern("hidden", false);
    reg.mint_column(
        scope,
        ColumnOrigin::Minted {
            by: MintReason::Correlation,
        },
        Some(sp),
        Addressing::Hygienic,
        ValueFacts::default(),
    );
    let r = Reference {
        qualifier: None,
        name: reg.canonical(sp),
    };
    // Published spelling present, addressing says no: unreachable, and the
    // two facts cannot drift apart because addressing is not a bool pair.
    assert_eq!(
        reg.address(r, &ScopeEnv::at(vec![scope])),
        Err(AddressError::NotFound)
    );
}

#[test]
fn a_qualifier_naming_no_visible_scope_says_so() {
    let reg = Registry::new(&[]);
    let (_e, scope, _id, _l) = users(&reg);
    let r = Reference {
        qualifier: Some(reg.canonical(reg.intern("orders", false))),
        name: reg.canonical(reg.intern("id", false)),
    };
    // Not "column not found" — the scope is what is missing.
    assert_eq!(
        reg.address(r, &ScopeEnv::at(vec![scope])),
        Err(AddressError::NoSuchScope)
    );
}

// -------------------------------------------------------------------------
// Case 5 — a nested compilation sharing identities with its parent
// -------------------------------------------------------------------------
//
// Today's failure: two process-global counters, so a nested compile draws
// from the same sequence as everything the process compiled before it.

#[test]
fn a_nested_compilation_shares_the_parents_identities() {
    let reg = Registry::new(&[]);
    let (_e, outer, outer_id, _l) = users(&reg);

    // The nested compile mints into the same registry, under the parent.
    let nested = reg.mint_scope(
        ScopeOrigin::Cte {
            input: outer,
            role: CteRole::Materialize,
        },
        Hint::None,
        Some(outer),
    );
    let carried = reg.republish_heading(outer, nested, Republish::BoundaryExport);

    assert_eq!(reg.parent_of(nested), Some(outer));
    assert!(reg.same_value(*carried.in_order().next().unwrap(), outer_id));

    let bundle = Bundle {
        statements: vec![Statement {
            scopes: vec![outer, nested],
            headings: vec![carried.to_vec()],
            refs: carried.to_vec(),
        }],
    };
    let b = baptise(&reg, &bundle).unwrap();
    assert_ne!(spell_scope(&b, outer), spell_scope(&b, nested));
}

#[test]
fn an_interior_scope_is_linked_both_ways() {
    let reg = Registry::new(&[]);
    let (_entity, outer, _id, owner) = users(&reg);

    let interior = reg.mint_interior_scope(owner, Hint::None);
    let nested = reg.mint_column(
        interior,
        ColumnOrigin::Bound { position: 0 },
        Some(reg.intern("nested", false)),
        Addressing::Bare,
        ValueFacts::default(),
    );

    assert_eq!(reg.origin_of(interior), ScopeOrigin::Interior { of: owner });
    assert_eq!(reg.parent_of(interior), Some(outer));
    assert_eq!(reg.facts(owner).interior, Some(interior));
    assert_eq!(reg.scope_of(nested), interior);
}

#[test]
#[should_panic(expected = "a derived scope's parent must agree with its origin")]
fn a_derived_scope_refuses_a_conflicting_parent() {
    let reg = Registry::new(&[]);
    let (_entity, input, _id, _last) = users(&reg);
    let unrelated = reg.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);

    reg.mint_scope(
        ScopeOrigin::PipeStage { input },
        Hint::None,
        Some(unrelated),
    );
}

#[test]
fn emitted_names_do_not_depend_on_registry_age() {
    // Two registries, one of which has already compiled a lot. The naming
    // pass is local, so the second bundle is spelled identically.
    let fresh = Registry::new(&[]);
    let (_e, s1, c1, _x) = users(&fresh);
    let stage1 = fresh.mint_scope(ScopeOrigin::PipeStage { input: s1 }, Hint::None, Some(s1));
    let carried1 = fresh.republish_heading(s1, stage1, Republish::Passthrough);
    let b1 = baptise(
        &fresh,
        &Bundle {
            statements: vec![Statement {
                scopes: vec![stage1],
                headings: vec![carried1.to_vec()],
                refs: carried1.to_vec(),
            }],
        },
    )
    .unwrap();

    let aged = Registry::new(&[]);
    for _ in 0..50 {
        let n = aged.intern("noise", false);
        let e = aged.mint_entity(n);
        aged.mint_scope(ScopeOrigin::BaseTable { entity: e }, Hint::None, None);
    }
    let (_e2, s2, c2, _y) = users(&aged);
    let stage2 = aged.mint_scope(ScopeOrigin::PipeStage { input: s2 }, Hint::None, Some(s2));
    let carried2 = aged.republish_heading(s2, stage2, Republish::Passthrough);
    let b2 = baptise(
        &aged,
        &Bundle {
            statements: vec![Statement {
                scopes: vec![stage2],
                headings: vec![carried2.to_vec()],
                refs: carried2.to_vec(),
            }],
        },
    )
    .unwrap();

    assert_eq!(spell_scope(&b1, stage1), spell_scope(&b2, stage2));
    assert_eq!(
        spell_col(&b1, *carried1.in_order().next().unwrap()),
        spell_col(&b2, *carried2.in_order().next().unwrap())
    );
    let _ = (c1, c2);
}

// -------------------------------------------------------------------------
// The naming pass itself
// -------------------------------------------------------------------------

#[test]
fn a_minted_name_cannot_collide_with_a_catalog_table() {
    // A user table literally called `t_1`.
    let reg = Registry::new(&["t_1"]);
    let (_e, base, id, _l) = users(&reg);
    let stage = reg.mint_scope(
        ScopeOrigin::PipeStage { input: base },
        Hint::None,
        Some(base),
    );
    let carried = reg.republish_heading(base, stage, Republish::Passthrough);
    let b = baptise(
        &reg,
        &Bundle {
            statements: vec![Statement {
                scopes: vec![stage],
                headings: vec![carried.to_vec()],
                refs: carried.to_vec(),
            }],
        },
    )
    .unwrap();
    assert_ne!(spell_scope(&b, stage), "t_1");
    let _ = id;
}

#[test]
fn authored_bundle_scope_reserves_its_name_before_scratch_is_named() {
    let reg = Registry::new(&["scratch_1"]);
    let scratch = reg.mint_scope(
        ScopeOrigin::Scratch {
            role: ScratchRole::Snapshot,
        },
        Hint::None,
        None,
    );
    let authored_spelling = reg.intern("scratch_1", false);
    let authored = reg.mint_scope(
        ScopeOrigin::AnonRelation,
        Hint::User(authored_spelling),
        None,
    );
    let b = baptise(
        &reg,
        &Bundle {
            statements: vec![
                Statement {
                    scopes: vec![scratch],
                    headings: Vec::new(),
                    refs: Vec::new(),
                },
                Statement {
                    scopes: vec![authored],
                    headings: Vec::new(),
                    refs: Vec::new(),
                },
            ],
        },
    )
    .unwrap();

    assert_eq!(spell_scope(&b, authored), "scratch_1");
    assert_eq!(spell_scope(&b, scratch), "scratch_1_2");
}

#[test]
fn one_heading_carrying_a_name_twice_poisons_both_occurrences() {
    let reg = Registry::new(&[]);
    let scope = reg.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
    let sp = reg.intern("name", false);
    let a = reg.mint_column(
        scope,
        ColumnOrigin::Bound { position: 0 },
        Some(sp),
        Addressing::Published,
        ValueFacts::default(),
    );
    let b_col = reg.mint_column(
        scope,
        ColumnOrigin::Bound { position: 1 },
        Some(sp),
        Addressing::Published,
        ValueFacts::default(),
    );

    // Neither occurrence is the real `name`, so neither keeps the spelling.
    // Privileging the first survivor with the bare name and suffixing the
    // second says one of them is authoritative, and nothing decided that.
    let bundle = Bundle {
        statements: vec![Statement {
            scopes: vec![scope],
            headings: vec![vec![a, b_col]],
            refs: vec![],
        }],
    };
    let bap = baptise(&reg, &bundle).unwrap();
    let first = spell_col(&bap, a);
    let second = spell_col(&bap, b_col);
    assert_ne!(first, "name");
    assert_ne!(second, "name");
    assert_ne!(first, second);

    // Group-relative, and the ambiguity is too: the same two columns in
    // headings of their own are each the only `name` there, so each keeps
    // the spelling the user wrote.
    let bundle2 = Bundle {
        statements: vec![Statement {
            scopes: vec![scope],
            headings: vec![vec![a], vec![b_col]],
            refs: vec![],
        }],
    };
    let bap2 = baptise(&reg, &bundle2).unwrap();
    assert_eq!(spell_col(&bap2, a), "name");
    assert_eq!(spell_col(&bap2, b_col), "name");
}

#[test]
fn an_uncontested_authored_name_is_never_minted_over() {
    // The whole point of poisoning is that it is confined to names nobody
    // chose. A heading where every spelling is the user's must come out
    // exactly as written, however many compilations it takes.
    let reg = Registry::new(&[]);
    let scope = reg.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
    let cols: Vec<ColId> = ["aid", "bid", "Name"]
        .into_iter()
        .enumerate()
        .map(|(position, text)| {
            reg.mint_column(
                scope,
                ColumnOrigin::Bound {
                    position: position as u32,
                },
                Some(reg.intern(text, false)),
                Addressing::Published,
                ValueFacts::default(),
            )
        })
        .collect();
    let bundle = Bundle {
        statements: vec![Statement {
            scopes: vec![scope],
            headings: vec![cols.clone()],
            refs: vec![],
        }],
    };
    for _ in 0..2 {
        let bap = baptise(&reg, &bundle).unwrap();
        let spelled: Vec<String> = cols.iter().map(|c| spell_col(&bap, *c)).collect();
        assert_eq!(spelled, ["aid", "bid", "Name"]);
    }
}

#[test]
fn a_column_nobody_named_is_minted_fresh_every_compilation() {
    let reg = Registry::new(&[]);
    let (scope, cols) = unnamed_pair(&reg, "", false);
    let bundle = Bundle {
        statements: vec![Statement {
            scopes: vec![scope],
            headings: vec![cols.clone()],
            refs: vec![],
        }],
    };
    let first = baptise(&reg, &bundle).unwrap();
    let second = baptise(&reg, &bundle).unwrap();
    // Distinct within one heading: two columns are two names.
    assert_ne!(spell_col(&first, cols[0]), spell_col(&first, cols[1]));
    // And drawn again next time, so nobody can hold one.
    assert_ne!(spell_col(&first, cols[0]), spell_col(&second, cols[0]));
}

#[test]
fn a_qualified_reference_is_derived_not_carried() {
    let reg = Registry::new(&[]);
    let (_e, base, id, _l) = users(&reg);
    let stage = reg.mint_scope(
        ScopeOrigin::PipeStage { input: base },
        Hint::None,
        Some(base),
    );
    let carried = reg.republish_heading(base, stage, Republish::Passthrough);
    let b = baptise(
        &reg,
        &Bundle {
            statements: vec![Statement {
                scopes: vec![base, stage],
                headings: vec![carried.to_vec()],
                refs: carried.to_vec(),
            }],
        },
    )
    .unwrap();

    // Written from inside its own scope: bare.
    let mut here = String::new();
    b.write_ref(
        *carried.in_order().next().unwrap(),
        stage,
        false,
        &mut Probe(&mut here),
    );
    assert!(!here.contains('.'));

    // Written from elsewhere: qualified, and the qualifier came from the
    // column's scope rather than from a field someone had to maintain.
    let mut there = String::new();
    b.write_ref(
        *carried.in_order().next().unwrap(),
        base,
        false,
        &mut Probe(&mut there),
    );
    assert!(there.contains('.'));

    // Written from inside a scope the statement also STANDS on — a
    // recursive step reads the CTE it defines — qualified, because the
    // bare name is one two FROM entries may both publish.
    let mut reflexive = String::new();
    b.write_ref(
        *carried.in_order().next().unwrap(),
        stage,
        true,
        &mut Probe(&mut reflexive),
    );
    assert!(reflexive.contains('.'));
    let _ = id;
}

#[test]
fn qualified_sql_literal_names_get_a_safe_spelling() {
    let reg = Registry::new(&[]);
    let scope = reg.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);

    for (name, expected) in [
        ("true", "_true"),
        ("FALSE", "_FALSE"),
        ("null", "_null"),
        ("ordinary", "ordinary"),
    ] {
        let spelling = reg.intern(name, false);
        let column = reg.mint_column(
            scope,
            ColumnOrigin::Bound { position: 0 },
            Some(spelling),
            Addressing::Published,
            ValueFacts::default(),
        );
        let safe = reg
            .qualified_safe_spelling(column)
            .expect("a published column has a safe spelling");
        let mut actual = String::new();
        reg.write(safe, &mut Probe(&mut actual));
        assert_eq!(actual, expected);
    }
}

#[test]
fn only_callable_intrinsics_have_a_canonical_spelling() {
    for (intrinsic, expected) in [
        (Intrinsic::JsonExtractRaw, Some("json_extract")),
        (Intrinsic::JsonEachArray, Some("json_each")),
        (Intrinsic::JsonEachObject, Some("json_each")),
        (Intrinsic::JsonObject, Some("json_object")),
        (Intrinsic::ScalarMax, Some("max")),
        (Intrinsic::ScalarMin, Some("min")),
        (Intrinsic::Round2, Some("round")),
        (Intrinsic::Arbitrary, None),
    ] {
        assert_eq!(intrinsic.canonical(), expected);
    }
}

/// THE ARITY DECIDES, for every reader. The overload is one answer shared by
/// the lowering's render form and resolution's window judgment, so the two
/// cannot disagree about which function a name means.
#[test]
fn the_arity_distinguished_overloads_answer_by_argument_row() {
    for (name, arity, expected) in [
        ("max", 1, None),
        ("max", 2, Some(Intrinsic::ScalarMax)),
        ("max", 3, Some(Intrinsic::ScalarMax)),
        ("MAX", 2, Some(Intrinsic::ScalarMax)),
        ("min", 1, None),
        ("min", 2, Some(Intrinsic::ScalarMin)),
        ("round", 1, None),
        ("round", 2, Some(Intrinsic::Round2)),
        // A three-argument round is neither overload: the name is not
        // overloaded THERE, and the caller's ordinary judgment stands.
        ("round", 3, None),
        ("sum", 2, None),
        ("upper", 2, None),
    ] {
        assert_eq!(
            Intrinsic::scalar_overload(name, arity),
            expected,
            "{name} at arity {arity}"
        );
    }
}

#[test]
fn uncallable_intrinsic_refuses_without_writing_characters() {
    let reg = Registry::new(&[]);
    let function = reg.mint_intrinsic(Intrinsic::Arbitrary);
    let names = baptise(&reg, &Bundle::default()).unwrap();

    let mut name = String::new();
    let error = names
        .write_function_name(function, &mut Probe(&mut name))
        .unwrap_err();
    assert_eq!(
        error,
        FunctionSpellingError::NoCanonicalSpelling {
            intrinsic: Intrinsic::Arbitrary,
        }
    );
    assert!(name.is_empty());

    let mut qualified = String::new();
    let error = names
        .write_function(function, &mut Probe(&mut qualified))
        .unwrap_err();
    assert_eq!(
        error,
        FunctionSpellingError::NoCanonicalSpelling {
            intrinsic: Intrinsic::Arbitrary,
        }
    );
    assert!(qualified.is_empty());
}

#[test]
fn callable_categories_are_registry_owned_and_compatibility_records_are_uncategorized() {
    let reg = Registry::new(&[]);
    let categories = [
        CallableCategory::Scalar,
        CallableCategory::Relational,
        CallableCategory::Effect,
        CallableCategory::Dml(DmlVerb::Insert),
        CallableCategory::Dml(DmlVerb::Update),
        CallableCategory::Dml(DmlVerb::Delete),
    ];

    for category in categories {
        let callable = reg.mint_callable(reg.intern("callable", false), Vec::new(), category);
        assert_eq!(reg.callable_category(callable), Some(category));
    }

    let compatibility = reg.mint_function(reg.intern("legacy", false), Vec::new());
    assert_eq!(reg.callable_category(compatibility), None);
    let intrinsic = reg.mint_intrinsic(Intrinsic::ScalarMax);
    assert_eq!(reg.callable_category(intrinsic), None);
}

#[test]
fn fn_id_is_the_same_identity_as_callable_id() {
    fn accepts_callable(_: CallableId) {}

    let reg = Registry::new(&[]);
    let id: FnId = reg.mint_function(reg.intern("legacy", false), Vec::new());
    accepts_callable(id);
}

// -------------------------------------------------------------------------
// Case 6 — the qualifier's two tiers, and the shadow between them
// -------------------------------------------------------------------------
//
// A qualifier names a scope, or a relation a column still answers under. The
// second tier exists because an endpoints-only export lands its columns in a
// scope of the compiler's own. It must not run when the FIRST tier found the
// scope: `q.x` where `q` is here and has no `x` is a missing column, and
// answering it from the second tier binds an unrelated occurrence that happens
// to carry `q` in its addressing.

/// A scope answering to `name`, holding one column per entry in `columns`.
fn answering(reg: &Registry, name: &str, columns: &[&str]) -> (ScopeId, Vec<ColId>) {
    let sp = reg.intern(name, false);
    let entity = reg.mint_entity(sp);
    let scope = reg.mint_scope(ScopeOrigin::BaseTable { entity }, Hint::User(sp), None);
    let cols = columns
        .iter()
        .enumerate()
        .map(|(position, column)| {
            reg.mint_column(
                scope,
                ColumnOrigin::CatalogColumn {
                    entity,
                    position: position as u32,
                },
                Some(reg.intern(column, false)),
                Addressing::Published,
                ValueFacts::default(),
            )
        })
        .collect();
    (scope, cols)
}

#[test]
fn a_qualifier_reaches_a_column_that_only_the_column_knows_about() {
    let reg = Registry::new(&[]);
    let (source, cols) = answering(&reg, "depts", &["id", "name"]);
    // An endpoints-only export: a scope of the compiler's own, whose columns
    // keep their names and carry the endpoint as a qualifier.
    let export = reg.mint_scope(
        ScopeOrigin::Wrap {
            input: source,
            why: WrapReason::Projection,
        },
        Hint::None,
        None,
    );
    let endpoint = reg.canonical(reg.intern("depts", false));
    let exported: Vec<_> = cols
        .iter()
        .map(|c| {
            reg.republish_column(
                *c,
                export,
                Republish::BoundaryExport,
                reg.published(*c),
                Addressing::BareAnswering(endpoint),
                |_| {},
            )
        })
        .collect();

    let env = ScopeEnv::at(vec![export]);
    let name = reg.canonical(reg.intern("name", false));
    assert_eq!(
        reg.address(
            Reference {
                qualifier: Some(endpoint),
                name
            },
            &env
        ),
        Ok(exported[1]),
        "the export's scope answers to nothing, so the column carries the answer"
    );
}

#[test]
fn a_named_scope_without_the_column_does_not_fall_through() {
    let reg = Registry::new(&[]);
    // `q` is here, and has no `x`.
    let (q_scope, _q_cols) = answering(&reg, "q", &["a"]);
    // Something else entirely, republished across a boundary and still
    // carrying `q` in its addressing — the shadow.
    let (other, other_cols) = answering(&reg, "other", &["x"]);
    let export = reg.mint_scope(
        ScopeOrigin::Wrap {
            input: other,
            why: WrapReason::Projection,
        },
        Hint::None,
        None,
    );
    let q = reg.canonical(reg.intern("q", false));
    let shadow = reg.republish_column(
        other_cols[0],
        export,
        Republish::BoundaryExport,
        reg.published(other_cols[0]),
        Addressing::BareAnswering(q),
        |_| {},
    );

    let env = ScopeEnv::at(vec![q_scope, export]);
    let x = reg.canonical(reg.intern("x", false));
    let answer = reg.address(
        Reference {
            qualifier: Some(q),
            name: x,
        },
        &env,
    );
    assert_ne!(
        answer,
        Ok(shadow),
        "the scope `q` is here and has no `x`; that is a missing column, not a \
         licence to bind whatever else still carries `q`"
    );
    assert_eq!(answer, Err(AddressError::NotFound));
}

#[test]
fn a_qualifier_reaches_through_a_join_arm() {
    let reg = Registry::new(&[]);
    let (users, user_cols) = answering(&reg, "users", &["id"]);
    let (orders, order_cols) = answering(&reg, "orders", &["id", "user_id"]);
    let join = reg.mint_scope(
        ScopeOrigin::Join {
            left: users,
            right: orders,
        },
        Hint::None,
        None,
    );
    let carried: Vec<_> = user_cols
        .iter()
        .chain(order_cols.iter())
        .map(|c| {
            reg.republish_column(
                *c,
                join,
                Republish::JoinArm,
                reg.published(*c),
                Addressing::Published,
                |_| {},
            )
        })
        .collect();

    // A join carries no SQL alias, so its arms are still the FROM entries and
    // `orders` still names one of them.
    let env = ScopeEnv::at(vec![join]);
    let orders_sym = reg.canonical(reg.intern("orders", false));
    let id = reg.canonical(reg.intern("id", false));
    assert_eq!(
        reg.address(
            Reference {
                qualifier: Some(orders_sym),
                name: id
            },
            &env
        ),
        Ok(carried[1]),
        "`orders.id` names the arm, not the join's own occurrence of users.id"
    );
}

#[test]
fn a_join_publishes_a_column_without_becoming_its_owner() {
    // Two relations nobody named — nothing to tell them apart by except
    // which relation each column is from. The join republishes both headings
    // into one scope so it has a heading of its own; reading THAT scope
    // reports four columns of one relation, which is the answer meta-ize's
    // `scope` cell must not give.
    let reg = Registry::new(&[]);
    let (left, left_cols) = unnamed_pair(&reg, "", false);
    let (right, right_cols) = unnamed_pair(&reg, "", false);
    let join = reg.mint_scope(ScopeOrigin::Join { left, right }, Hint::None, None);
    let published: Vec<_> = left_cols
        .iter()
        .chain(right_cols.iter())
        .map(|c| {
            reg.republish_column(
                *c,
                join,
                Republish::JoinArm,
                reg.published(*c),
                Addressing::Published,
                |_| {},
            )
        })
        .collect();

    for column in &published {
        assert_eq!(reg.scope_of(*column), join);
    }
    let owners: Vec<_> = published.iter().map(|c| reg.owner_of(*c)).collect();
    assert_eq!(owners, vec![left, left, right, right]);

    // And it survives the next boundary the join stands under, because that
    // boundary republishes the join's occurrences rather than the arms'.
    let stage = reg.mint_scope(
        ScopeOrigin::PipeStage { input: join },
        Hint::None,
        Some(join),
    );
    let carried = reg.republish_heading(join, stage, Republish::Passthrough);
    let carried: Vec<_> = carried.in_order().copied().collect();
    assert_eq!(
        carried.iter().map(|c| reg.owner_of(*c)).collect::<Vec<_>>(),
        vec![stage, stage, stage, stage],
        "a projection CONSUMED the join, so nothing earlier is left to own \
         its columns"
    );
}

#[test]
fn a_joined_heading_keeps_owner_and_name_through_the_emission_wrap() {
    // The production join road wraps each operand as a subquery before the
    // join republishes its heading, so a meta-ize input column stands two
    // republications from the relation it belongs to: JoinArm over the wrap,
    // EmissionWrap over the operand. Neither boundary consumed anything —
    // the ownership walk crosses both, and the report is the exact name the
    // occurrence has in the emitted heading.
    let reg = Registry::new(&[]);

    // `_(a@1) as q`: an aliased anonymous relation with one authored column.
    let q_spelling = reg.intern("q", false);
    let aliased = reg.mint_scope(ScopeOrigin::AnonRelation, Hint::User(q_spelling), None);
    let a = reg.mint_column(
        aliased,
        ColumnOrigin::Computed {
            via: Computation::Operator,
        },
        Some(reg.intern("a", false)),
        Addressing::Published,
        ValueFacts::default(),
    );

    // `_(1,2)`: an anonymous relation whose columns nobody named.
    let (anon, anon_cols) = unnamed_pair(&reg, "", false);

    // Each operand is wrapped for emission, then the join republishes.
    let wrap = |source: ScopeId, cols: &[ColId]| -> Vec<ColId> {
        let wrapped = reg.mint_scope(
            ScopeOrigin::Wrap {
                input: source,
                why: WrapReason::Projection,
            },
            Hint::None,
            None,
        );
        cols.iter()
            .map(|c| {
                reg.republish_column(
                    *c,
                    wrapped,
                    Republish::EmissionWrap,
                    reg.published(*c),
                    Addressing::Published,
                    |_| {},
                )
            })
            .collect()
    };
    let left_wrapped = wrap(aliased, &[a]);
    let right_wrapped = wrap(anon, &anon_cols);
    let join = reg.mint_scope(
        ScopeOrigin::Join {
            left: aliased,
            right: anon,
        },
        Hint::None,
        None,
    );
    let heading: Vec<ColId> = left_wrapped
        .iter()
        .chain(right_wrapped.iter())
        .map(|c| {
            reg.republish_column(
                *c,
                join,
                Republish::JoinArm,
                reg.published(*c),
                Addressing::Published,
                |_| {},
            )
        })
        .collect();

    // The owner walk crosses both non-consuming boundaries.
    assert_eq!(reg.owner_of(heading[0]), aliased);
    assert_eq!(reg.owner_of(heading[1]), anon);
    assert_eq!(reg.owner_of(heading[2]), anon);

    let baptised = baptise(
        &reg,
        &Bundle {
            statements: vec![Statement {
                scopes: vec![aliased, anon, join],
                headings: vec![heading.clone()],
                refs: vec![],
            }],
        },
    )
    .expect("all scopes listed");

    let scope_report = |s: ScopeId| {
        let mut out = String::new();
        baptised.write_answers_to(s, &mut Probe(&mut out));
        out
    };
    let name_report = |c: ColId| {
        let mut out = String::new();
        baptised.write_column_report(c, &mut Probe(&mut out));
        out
    };
    let emitted = |c: ColId| {
        let mut out = String::new();
        baptised.write_column(c, &mut Probe(&mut out));
        out
    };

    // Owner: authored verbatim, otherwise that relation's own report.
    assert_eq!(scope_report(reg.owner_of(heading[0])), "q");
    let anon_owner = scope_report(reg.owner_of(heading[1]));
    assert_ne!(anon_owner, "q");
    assert!(!anon_owner.is_empty());
    assert_eq!(scope_report(reg.owner_of(heading[2])), anon_owner);

    // The control: the report IS the emitted heading name, column for
    // column — authored `a` verbatim, and the anon columns under the very
    // characters the mint drew for the heading, never an ordinal spelling.
    for c in &heading {
        assert_eq!(name_report(*c), emitted(*c));
    }
    assert_eq!(name_report(heading[0]), "a");
    assert!(!name_report(heading[1]).contains('|'));
    assert!(!name_report(heading[2]).contains('|'));
    assert_ne!(name_report(heading[1]), name_report(heading[2]));
}

#[test]
fn a_qualified_reference_reaches_a_using_rider_but_bare_does_not() {
    let reg = Registry::new(&[]);
    let (users, user_cols) = answering(&reg, "users", &["id"]);
    let (orders, order_cols) = answering(&reg, "orders", &["id"]);
    let join = reg.mint_scope(
        ScopeOrigin::Join {
            left: users,
            right: orders,
        },
        Hint::None,
        None,
    );
    let merged = reg.republish_column(
        user_cols[0],
        join,
        Republish::JoinArm,
        reg.published(user_cols[0]),
        Addressing::Published,
        |_| {},
    );
    let rider = reg.carry_qualified(order_cols[0], join);
    let env = ScopeEnv::at(vec![join]);
    let orders_sym = reg.canonical(reg.intern("orders", false));
    let id = reg.canonical(reg.intern("id", false));

    assert_eq!(
        reg.known_heading(join)
            .expect("a heading this test built is known")
            .to_vec(),
        vec![merged],
        "the qualified carrier is not a second heading slot"
    );
    assert_eq!(
        reg.address(
            Reference {
                qualifier: Some(orders_sym),
                name: id,
            },
            &env,
        ),
        Ok(rider),
        "the explicit qualifier reaches the live right arm's merged key"
    );
    assert_eq!(
        reg.address(
            Reference {
                qualifier: None,
                name: id,
            },
            &env,
        ),
        Ok(merged),
        "the qualified carrier contributes no second bare key"
    );
    assert_eq!(
        reg.qualified_glob(orders_sym, &[merged]).to_vec(),
        vec![rider],
        "a qualified glob and a qualified reference use the same reach"
    );
}

#[test]
fn a_consumed_scope_is_not_a_qualifier() {
    let reg = Registry::new(&[]);
    let (users, cols) = answering(&reg, "users", &["country"]);
    // A pipe ENDS its input's life — unlike a join, nothing downstream still
    // reads through `users`.
    let stage = reg.mint_scope(ScopeOrigin::PipeStage { input: users }, Hint::None, None);
    let carried = reg.republish_column(
        cols[0],
        stage,
        Republish::BoundaryExport,
        reg.published(cols[0]),
        Addressing::Published,
        |_| {},
    );

    let env = ScopeEnv::at(vec![stage]);
    let users_sym = reg.canonical(reg.intern("users", false));
    let country = reg.canonical(reg.intern("country", false));
    assert_eq!(
        reg.address(
            Reference {
                qualifier: Some(users_sym),
                name: country
            },
            &env
        ),
        Err(AddressError::NoSuchScope),
        "the pipe consumed `users`; a birth frame is not a live qualifier"
    );
    // Both roads, one law. A glob that reached the birth frame where a
    // reference could not would make `users.*` and `users.country` resolve
    // against different sets one character apart.
    assert!(reg.qualified_glob(users_sym, &[carried]).is_empty());
}

#[test]
fn a_qualifier_naming_nothing_at_all_says_so() {
    let reg = Registry::new(&[]);
    let (scope, _) = answering(&reg, "q", &["a"]);
    let env = ScopeEnv::at(vec![scope]);
    let absent = reg.canonical(reg.intern("nowhere", false));
    let a = reg.canonical(reg.intern("a", false));
    assert_eq!(
        reg.address(
            Reference {
                qualifier: Some(absent),
                name: a
            },
            &env
        ),
        Err(AddressError::NoSuchScope)
    );
}

// -------------------------------------------------------------------------
// A scope reported as a VALUE, which is not the alias it is called in SQL

fn spell_answers_to(b: &Baptised<'_>, s: ScopeId) -> String {
    let mut out = String::new();
    b.write_answers_to(s, &mut Probe(&mut out));
    out
}

// An UNNAMED scope's reported value is a name minted per relation whose
// characters vary from run to run, so that nobody can read one and match on
// it later. The exact spelling is therefore the wrong thing for a test to
// hold; what is pinned below is that two relations get two answers and one
// relation gets one. The counted form of the same invariant lives in the
// corpus, where two relations stand side by side (mint_anon_*).

#[test]
fn two_unnamed_relations_report_two_different_names() {
    let reg = Registry::new(&[]);
    let (first, first_cols) = unnamed_pair(&reg, "", false);
    let (second, second_cols) = unnamed_pair(&reg, "", false);
    let baptised = baptise(
        &reg,
        &Bundle {
            statements: vec![Statement {
                scopes: vec![first, second],
                headings: vec![first_cols.clone(), second_cols.clone()],
                refs: vec![],
            }],
        },
    )
    .expect("both scopes are listed");
    let one = spell_answers_to(&baptised, first);
    let other = spell_answers_to(&baptised, second);
    assert!(
        !one.is_empty(),
        "a relation must be able to say which it is"
    );
    assert_ne!(
        one, other,
        "two relations nobody named are still two relations"
    );
}

#[test]
fn one_unnamed_relation_reports_one_name_however_often_it_is_asked() {
    let reg = Registry::new(&[]);
    let (scope, cols) = unnamed_pair(&reg, "", false);
    let baptised = baptise(
        &reg,
        &Bundle {
            statements: vec![Statement {
                scopes: vec![scope],
                headings: vec![cols],
                refs: vec![],
            }],
        },
    )
    .expect("the scope is listed");
    assert_eq!(
        spell_answers_to(&baptised, scope),
        spell_answers_to(&baptised, scope)
    );
}

#[test]
fn a_drawn_report_does_not_survive_the_bundle_that_drew_it() {
    let reg = Registry::new(&[]);
    let (scope, cols) = unnamed_pair(&reg, "", false);
    let bundle = Bundle {
        statements: vec![Statement {
            scopes: vec![scope],
            headings: vec![cols],
            refs: vec![],
        }],
    };
    let first = baptise(&reg, &bundle).expect("listed");
    let second = baptise(&reg, &bundle).expect("listed");
    assert_ne!(
        spell_answers_to(&first, scope),
        spell_answers_to(&second, scope),
        "a name nobody authored is drawn per compilation; a reader who saw \
         one must not find it again"
    );
}

#[test]
fn a_reported_scope_keeps_the_authored_spelling_baptism_had_to_disambiguate() {
    // Two occurrences answering to one authored name: SQL cannot call them
    // both `j`, so baptism moves one. The value must not move with it — the
    // author qualifies both occurrences `j`, and that is what is reported.
    let reg = Registry::new(&[]);
    let (first, first_cols) = answering(&reg, "j", &["a"]);
    let (second, second_cols) = answering(&reg, "j", &["b"]);
    let bundle = Bundle {
        statements: vec![Statement {
            scopes: vec![first, second],
            headings: vec![first_cols.clone(), second_cols.clone()],
            refs: vec![first_cols[0], second_cols[0]],
        }],
    };
    let baptised = baptise(&reg, &bundle).expect("both scopes are listed");
    assert_ne!(
        spell_scope(&baptised, first),
        spell_scope(&baptised, second)
    );
    assert_eq!(spell_answers_to(&baptised, first), "j");
    assert_eq!(spell_answers_to(&baptised, second), "j");
}

// -------------------------------------------------------------------------
// The ordinal report is a REFERENCE, so it must be typable
// -------------------------------------------------------------------------

/// A scope answering to `name` holding two never-named columns.
fn unnamed_pair(reg: &Registry, name: &str, stropped: bool) -> (ScopeId, Vec<ColId>) {
    let hint = match name {
        "" => Hint::None,
        text => Hint::User(reg.intern(text, stropped)),
    };
    let scope = reg.mint_scope(ScopeOrigin::AnonRelation, hint, None);
    let cols = (0..2)
        .map(|_| {
            reg.mint_column(
                scope,
                ColumnOrigin::Computed {
                    via: Computation::Operator,
                },
                None,
                Addressing::Published,
                ValueFacts::default(),
            )
        })
        .collect();
    (scope, cols)
}

fn spell_ordinal_report(reg: &Registry, c: ColId) -> Option<String> {
    let mut out = String::new();
    reg.write_ordinal_report(c, &mut Probe(&mut out))
        .then_some(out)
}

#[test]
fn an_unqualified_ordinal_report_is_the_bare_position() {
    let reg = Registry::new(&[]);
    let (_, cols) = unnamed_pair(&reg, "", false);
    assert_eq!(spell_ordinal_report(&reg, cols[1]).as_deref(), Some("|2|"));
}

#[test]
fn a_qualified_ordinal_report_carries_the_answering_name() {
    let reg = Registry::new(&[]);
    let (_, cols) = unnamed_pair(&reg, "t", false);
    assert_eq!(spell_ordinal_report(&reg, cols[0]).as_deref(), Some("t|1|"));
}

#[test]
fn a_stropped_qualifier_keeps_its_delimiters_in_the_report() {
    // `a b|1|` reaches nothing: the report promises the characters the
    // reader would type, and for a stropped name those include the
    // delimiters. The bit travels to the sink rather than being flattened
    // into one string, so the sink can spell it.
    let reg = Registry::new(&[]);
    let (_, cols) = unnamed_pair(&reg, "a b", true);
    assert_eq!(
        spell_ordinal_report(&reg, cols[0]).as_deref(),
        Some("`a b`|1|")
    );
}

#[test]
fn an_authored_column_reports_the_name_its_author_wrote() {
    // WHICH road a column takes is baptism's call — a spelling that lost an
    // ambiguity is emitted invented too, and this road cannot see that. What
    // it answers is the position, whenever asked; the report chooses.
    let reg = Registry::new(&[]);
    let (scope, cols) = answering(&reg, "t", &["id"]);
    let baptised = baptise(
        &reg,
        &Bundle {
            statements: vec![Statement {
                scopes: vec![scope],
                headings: vec![cols.clone()],
                refs: vec![],
            }],
        },
    )
    .expect("the scope is listed");
    let mut out = String::new();
    baptised.write_column_report(cols[0], &mut Probe(&mut out));
    assert_eq!(out, "id");
}

#[test]
fn a_poisoned_column_reports_its_position_not_the_drawn_characters() {
    // Two `id`s: neither keeps the spelling, and neither may report the one
    // drawn for it — a value in a row that no second run reproduces is not
    // an answer. The position is, and it is what reaches the column.
    let reg = Registry::new(&[]);
    let (scope, cols) = answering(&reg, "t", &["id", "id"]);
    let baptised = baptise(
        &reg,
        &Bundle {
            statements: vec![Statement {
                scopes: vec![scope],
                headings: vec![cols.clone()],
                refs: vec![],
            }],
        },
    )
    .expect("the scope is listed");
    let report = |c| {
        let mut out = String::new();
        baptised.write_column_report(c, &mut Probe(&mut out));
        out
    };
    assert_eq!(report(cols[0]), "t|1|");
    assert_eq!(report(cols[1]), "t|2|");
}

// ---------------------------------------------------------------------------
// Opacity is a capability, and it survives the operations that carry a relation
// ---------------------------------------------------------------------------

#[test]
fn an_opaque_heading_is_not_an_empty_one() {
    let reg = Registry::new(&[]);
    let opaque = reg.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
    reg.mark_heading_opaque(opaque);

    assert_eq!(reg.heading(opaque), HeadingKnowledge::Opaque);
    // A structural reader refuses rather than receiving a list it would
    // read as "this publishes nothing".
    assert!(reg.known_heading(opaque).is_err());
    // A gathering reader sees nothing, and the type it gets back is a plain
    // vector rather than an exhaustive enumeration, so nothing built from it
    // can be read as one.
    assert!(reg.heading(opaque).columns_seen().is_empty());
    assert!(reg.any_heading_opaque(&[opaque]));
}

#[test]
fn republishing_an_opaque_heading_leaves_the_destination_opaque() {
    let reg = Registry::new(&[]);
    let opaque = reg.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
    reg.mark_heading_opaque(opaque);
    let stage = reg.mint_scope(
        ScopeOrigin::PipeStage { input: opaque },
        Hint::None,
        Some(opaque),
    );

    let carried = reg.republish_heading(opaque, stage, Republish::Passthrough);

    // Nothing was minted, because nothing was enumerated...
    assert!(carried.into_vec().is_empty());
    // ...and the destination says so. A scope transition must not turn "the
    // heading is unknown" into "the heading has none".
    assert_eq!(reg.heading(stage), HeadingKnowledge::Opaque);
    assert!(reg.known_heading(stage).is_err());
}

#[test]
fn a_qualified_reference_to_a_known_scope_survives_an_unrelated_opaque_one() {
    let reg = Registry::new(&[]);
    let (_e, base, id, _last) = users(&reg);
    let opaque = reg.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
    reg.mark_heading_opaque(opaque);

    // The qualifier chooses the scopes the search runs over. The opaque
    // relation answers to nothing a user wrote, so it was never part of
    // this search and its being in view says nothing about it.
    let answer = reg.address(
        Reference {
            qualifier: Some(reg.canonical(reg.intern("users", false))),
            name: reg.published_sym(id).expect("a base column publishes"),
        },
        &ScopeEnv::at(vec![base, opaque]),
    );
    assert_eq!(answer, Ok(id));
}

#[test]
fn a_reference_over_an_opaque_scope_is_not_reported_absent() {
    let reg = Registry::new(&[]);
    let (_e, base, id, _last) = users(&reg);
    let opaque = reg.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
    reg.mark_heading_opaque(opaque);

    let missing = reg.intern("nowhere", false);
    let reference = Reference {
        qualifier: None,
        name: reg.canonical(missing),
    };

    // With only known scopes in view, absence is a fact the search proved.
    assert_eq!(
        reg.address(reference.clone(), &ScopeEnv::at(vec![base])),
        Err(AddressError::NotFound)
    );
    // With an opaque scope in view, the search never happened, and the
    // answer says that rather than claiming the name is not there.
    assert_eq!(
        reg.address(reference.clone(), &ScopeEnv::at(vec![base, opaque])),
        Err(AddressError::Incomplete)
    );
    // Including for a name the known scope DOES publish: the opaque relation
    // might publish it too, so "exactly one answers" was never established
    // either.
    assert_eq!(
        reg.address(
            Reference {
                qualifier: None,
                name: reg.published_sym(id).expect("a base column publishes"),
            },
            &ScopeEnv::at(vec![base, opaque]),
        ),
        Err(AddressError::Incomplete)
    );
}

// -------------------------------------------------------------------------
// Row-boundedness — a fact the relation carries, not a shape to be found
// -------------------------------------------------------------------------

/// A bound written on one relation reaches every relation built on it.
///
/// The mutation road asks the scope it is handed, once. What makes that
/// answer trustworthy is that the derivations below could not have dropped
/// it: each one is minted from an input, and a scope minted from a bounded
/// input is bounded.
#[test]
fn a_bound_reaches_every_relation_built_on_the_one_that_states_it() {
    let reg = Registry::new(&[]);
    let (entity, base, _, _) = users(&reg);
    let bounded = reg.mint_derived_scope(ScopeOrigin::PipeStage { input: base }, Hint::None);
    assert!(!reg.is_row_bounded(bounded));
    reg.mark_row_bounded(bounded);

    let alias = reg.mint_derived_scope(ScopeOrigin::UserAlias { of: bounded }, Hint::None);
    let named = reg.mint_derived_scope(
        ScopeOrigin::Cte {
            input: alias,
            role: CteRole::Materialize,
        },
        Hint::None,
    );
    let wrapped = reg.mint_derived_scope(
        ScopeOrigin::Wrap {
            input: named,
            why: WrapReason::Projection,
        },
        Hint::None,
    );
    let arm = reg.mint_derived_scope(
        ScopeOrigin::SetArm {
            of: wrapped,
            arm: 0,
        },
        Hint::None,
    );
    for scope in [alias, named, wrapped, arm] {
        assert!(
            reg.is_row_bounded(scope),
            "a relation standing on a bounded one offers its rows"
        );
    }

    // A fresh read of the same table shares nothing with the bounded one.
    let other = reg.mint_scope(ScopeOrigin::BaseTable { entity }, Hint::None, None);
    assert!(!reg.is_row_bounded(other));

    // A join is bounded by either side, and only by a side it actually has.
    let joined = reg.mint_scope(
        ScopeOrigin::Join {
            left: other,
            right: arm,
        },
        Hint::None,
        None,
    );
    assert!(reg.is_row_bounded(joined));
    let unbounded_join = reg.mint_scope(
        ScopeOrigin::Join {
            left: other,
            right: base,
        },
        Hint::None,
        None,
    );
    assert!(!reg.is_row_bounded(unbounded_join));
}

/// The mark lands before the layers above it are built, so a scope minted
/// BEFORE the bound was stated does not acquire it.
///
/// This is what makes the fact readable in one lookup: propagation happens
/// at mint, and nothing walks back down afterwards.
#[test]
fn a_relation_that_existed_before_the_bound_does_not_acquire_it() {
    let reg = Registry::new(&[]);
    let (_, base, _, _) = users(&reg);
    let earlier = reg.mint_derived_scope(ScopeOrigin::PipeStage { input: base }, Hint::None);
    reg.mark_row_bounded(base);
    assert!(reg.is_row_bounded(base));
    assert!(!reg.is_row_bounded(earlier));
}

// -------------------------------------------------------------------------
// A relation the statement names directly keeps its own spelling
// -------------------------------------------------------------------------

/// A mutation writes its target's name and then writes it again in every
/// correlated read. Those are one word or the statement is malformed, so the
/// target's scope does not arbitrate for the spelling — the rival moves.
#[test]
fn a_fixed_relation_keeps_its_name_and_the_rival_moves() {
    let reg = Registry::new(&[]);
    let (entity, target, id, _) = users(&reg);
    let name = reg.intern("users", false);
    // The source reads the same table under the same authored spelling.
    let source = reg.mint_scope(ScopeOrigin::BaseTable { entity }, Hint::User(name), None);
    let source_id = reg.republish_column(
        id,
        source,
        Republish::Passthrough,
        reg.published(id),
        Addressing::Published,
        |_| {},
    );
    reg.fix_relation_scope(target, entity);

    let bundle = Bundle {
        statements: vec![Statement {
            // The source is listed first: without the fix it would take the
            // spelling, which is exactly the collision this rules out.
            scopes: vec![source, target],
            headings: vec![vec![source_id], vec![id]],
            refs: vec![source_id, id],
        }],
    };
    let b = baptise(&reg, &bundle).expect("bundle names cleanly");
    assert_eq!(spell_scope(&b, target), "users");
    assert_ne!(spell_scope(&b, source), "users");
}

// -------------------------------------------------------------------------
// `!!` — evidence the relation carries
// -------------------------------------------------------------------------

/// A mark written on one relation reaches every relation built on it, and a
/// join carries both arms'.
///
/// The mutation contract counts what it is handed. What makes the count
/// trustworthy is that none of these derivations could have dropped a mark
/// or invented one: an alias, a name and a wrap carry their input's, a join
/// carries both, and a fresh read of the same table carries none.
#[test]
fn a_mutation_mark_reaches_every_relation_built_on_the_marked_one() {
    let reg = Registry::new(&[]);
    let (entity, base, _, _) = users(&reg);
    let spelling = reg.intern("users", false);
    assert!(reg.mutation_marks(base).is_empty());
    reg.mark_mutation_target(base, spelling);

    let named = reg.mint_derived_scope(
        ScopeOrigin::Cte {
            input: base,
            role: CteRole::Materialize,
        },
        Hint::None,
    );
    let alias = reg.mint_derived_scope(ScopeOrigin::UserAlias { of: named }, Hint::None);
    let piped = reg.mint_derived_scope(ScopeOrigin::PipeStage { input: alias }, Hint::None);
    for scope in [named, alias, piped] {
        assert_eq!(
            reg.mutation_marks(scope),
            vec![(base, spelling)],
            "a relation standing on a marked one carries its evidence"
        );
    }

    // An unmarked read of the same table carries nothing, so the evidence
    // belongs to the OCCURRENCE and not to the table.
    let other = reg.mint_scope(ScopeOrigin::BaseTable { entity }, Hint::None, None);
    assert!(reg.mutation_marks(other).is_empty());

    // A join carries both arms' — which is what makes "two relations are
    // marked" a count rather than a search that can come up short.
    let joined = reg.mint_scope(
        ScopeOrigin::Join {
            left: piped,
            right: other,
        },
        Hint::None,
        None,
    );
    assert_eq!(reg.mutation_marks(joined), vec![(base, spelling)]);

    let second = reg.intern("orders", false);
    reg.mark_mutation_target(other, second);
    let both = reg.mint_scope(
        ScopeOrigin::Join {
            left: piped,
            right: other,
        },
        Hint::None,
        None,
    );
    assert_eq!(
        both.pipe(|scope| reg.mutation_marks(scope)).len(),
        2,
        "two marked relations reach the contract as two"
    );
}

/// One occurrence is one mark, however many times it is resolved.
///
/// Re-resolution must not manufacture a second marked relation — that would
/// refuse a licensed mutation under the ambiguity rule, which is the exact
/// failure shape this evidence road replaced.
#[test]
fn marking_one_occurrence_twice_is_still_one_mark() {
    let reg = Registry::new(&[]);
    let (_, base, _, _) = users(&reg);
    let spelling = reg.intern("users", false);
    reg.mark_mutation_target(base, spelling);
    reg.mark_mutation_target(base, spelling);
    assert_eq!(reg.mutation_marks(base).len(), 1);
}

/// A small pipe helper, so the assertion above reads as one thought.
trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}
impl Pipe for ScopeId {}
