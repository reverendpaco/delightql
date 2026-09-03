// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The five cases that decide the representation.
//!
//! These are the prototypes Phase D asked for, written against the types
//! rather than beside them. A module that merely compiles proves nothing; a
//! module in which the wrong thing cannot be written proves the constraint.
//!
//! Each case names the defect it would reproduce under today's model.

use super::origin::Hint;
use super::sink::Probe;
use super::*;

/// A registry with one two-column table, for the cases that need a schema.
fn users(reg: &Registry) -> (EntityId, ScopeId, ColId, ColId) {
    let name = reg.intern("users", false);
    let entity = reg.mint_entity(name);
    let scope = reg.mint_scope(ScopeKind::BaseTable { entity }, Hint::User(name), None);
    let id_sp = reg.intern("id", false);
    let last_sp = reg.intern("last_name", false);
    let id = reg.sql_column(scope, Some(id_sp), Addressing::Published);
    let last = reg.sql_column(scope, Some(last_sp), Addressing::Published);
    (entity, scope, id, last)
}

fn restage(reg: &Registry, from: ScopeId, into: ScopeId) -> Vec<ColId> {
    reg.late_naming_columns(from)
        .into_iter()
        .map(|column| reg.rebind_sql_column(column, into, reg.published(column)))
        .collect()
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
        ScopeKind::Scratch {
            role: ScratchRole::Snapshot,
        },
        Hint::None,
        None,
    );
    let col = reg.sql_column(scratch, None, Addressing::Hygienic);

    // Three statements: create, fill, read — all naming the same scope.
    let stmt = |refs: Vec<ColId>| Statement {
        scopes: vec![scratch],
        headings: vec![vec![col]],
        refs,
    };
    let bundle = Bundle::gather(vec![stmt(vec![]), stmt(vec![col]), stmt(vec![col])]);

    let b = baptise(&reg, &bundle.clone().reserve_authored(&reg)).expect("bundle names cleanly");
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
    let orphan = reg.mint_scope(ScopeKind::AnonRelation, Hint::None, None);
    let col = reg.sql_column(orphan, None, Addressing::Bare);
    // The statement references the column but never declares its scope.
    let bundle = Bundle::gather(vec![Statement {
        scopes: vec![],
        headings: vec![],
        refs: vec![col],
    }]);
    // Matched rather than compared: `Baptised` deliberately has no `Debug`,
    // because a value that can print the name table is a road to characters.
    match baptise(&reg, &bundle.clone().reserve_authored(&reg)) {
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

// -------------------------------------------------------------------------
// Case 5 — a nested compilation sharing identities with its parent
// -------------------------------------------------------------------------
//
// Today's failure: two process-global counters, so a nested compile draws
// from the same sequence as everything the process compiled before it.

#[test]
fn an_interior_scope_is_linked_both_ways() {
    let reg = Registry::new(&[]);
    let (_entity, outer, _id, owner) = users(&reg);

    let interior = reg.mint_interior_scope(owner, Hint::None);
    let nested = reg.sql_column(
        interior,
        Some(reg.intern("nested", false)),
        Addressing::Bare,
    );

    assert_eq!(reg.kind_of(interior), ScopeKind::Interior);
    assert_eq!(reg.parent_of(interior), Some(outer));
    assert_eq!(reg.parent_of(interior), Some(reg.scope_of(owner)));
    assert_eq!(reg.scope_of(nested), interior);
}

#[test]
fn emitted_names_do_not_depend_on_registry_age() {
    // Two registries, one of which has already compiled a lot. The naming
    // pass is local, so the second bundle is spelled identically.
    let fresh = Registry::new(&[]);
    let (_e, s1, c1, _x) = users(&fresh);
    let stage1 = fresh.mint_scope(ScopeKind::PipeStage, Hint::None, Some(s1));
    let carried1 = restage(&fresh, s1, stage1);
    let b1 = baptise(
        &fresh,
        &Bundle::gather(vec![Statement {
            scopes: vec![stage1],
            headings: vec![carried1.to_vec()],
            refs: carried1.to_vec(),
        }])
        .reserve_authored(&fresh),
    )
    .unwrap();

    let aged = Registry::new(&[]);
    for _ in 0..50 {
        let n = aged.intern("noise", false);
        let e = aged.mint_entity(n);
        aged.mint_scope(ScopeKind::BaseTable { entity: e }, Hint::None, None);
    }
    let (_e2, s2, c2, _y) = users(&aged);
    let stage2 = aged.mint_scope(ScopeKind::PipeStage, Hint::None, Some(s2));
    let carried2 = restage(&aged, s2, stage2);
    let b2 = baptise(
        &aged,
        &Bundle::gather(vec![Statement {
            scopes: vec![stage2],
            headings: vec![carried2.to_vec()],
            refs: carried2.to_vec(),
        }])
        .reserve_authored(&aged),
    )
    .unwrap();

    assert_eq!(spell_scope(&b1, stage1), spell_scope(&b2, stage2));
    assert_eq!(
        spell_col(&b1, *carried1.iter().next().unwrap()),
        spell_col(&b2, *carried2.iter().next().unwrap())
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
    let stage = reg.mint_scope(ScopeKind::PipeStage, Hint::None, Some(base));
    let carried = restage(&reg, base, stage);
    let b = baptise(
        &reg,
        &Bundle::gather(vec![Statement {
            scopes: vec![stage],
            headings: vec![carried.to_vec()],
            refs: carried.to_vec(),
        }])
        .reserve_authored(&reg),
    )
    .unwrap();
    assert_ne!(spell_scope(&b, stage), "t_1");
    let _ = id;
}

#[test]
fn authored_bundle_scope_reserves_its_name_before_scratch_is_named() {
    let reg = Registry::new(&["scratch_1"]);
    let scratch = reg.mint_scope(
        ScopeKind::Scratch {
            role: ScratchRole::Snapshot,
        },
        Hint::None,
        None,
    );
    let authored_spelling = reg.intern("scratch_1", false);
    let authored = reg.mint_scope(ScopeKind::AnonRelation, Hint::User(authored_spelling), None);
    let b = baptise(
        &reg,
        &Bundle::gather(vec![
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
        ])
        .reserve_authored(&reg),
    )
    .unwrap();

    assert_eq!(spell_scope(&b, authored), "scratch_1");
    assert_eq!(spell_scope(&b, scratch), "scratch_1_2");
}

#[test]
fn one_heading_carrying_a_name_twice_poisons_both_occurrences() {
    let reg = Registry::new(&[]);
    let scope = reg.mint_scope(ScopeKind::AnonRelation, Hint::None, None);
    let sp = reg.intern("name", false);
    let a = reg.sql_column(scope, Some(sp), Addressing::Published);
    let b_col = reg.sql_column(scope, Some(sp), Addressing::Published);

    // Neither occurrence is the real `name`, so neither keeps the spelling.
    // Privileging the first survivor with the bare name and suffixing the
    // second says one of them is authoritative, and nothing decided that.
    let bundle = Bundle::gather(vec![Statement {
        scopes: vec![scope],
        headings: vec![vec![a, b_col]],
        refs: vec![],
    }]);
    let bap = baptise(&reg, &bundle.clone().reserve_authored(&reg)).unwrap();
    let first = spell_col(&bap, a);
    let second = spell_col(&bap, b_col);
    assert_ne!(first, "name");
    assert_ne!(second, "name");
    assert_ne!(first, second);

    // Group-relative, and the ambiguity is too: the same two columns in
    // headings of their own are each the only `name` there, so each keeps
    // the spelling the user wrote.
    let bundle2 = Bundle::gather(vec![Statement {
        scopes: vec![scope],
        headings: vec![vec![a], vec![b_col]],
        refs: vec![],
    }]);
    let bap2 = baptise(&reg, &bundle2.clone().reserve_authored(&reg)).unwrap();
    assert_eq!(spell_col(&bap2, a), "name");
    assert_eq!(spell_col(&bap2, b_col), "name");
}

#[test]
fn an_uncontested_authored_name_is_never_minted_over() {
    // The whole point of poisoning is that it is confined to names nobody
    // chose. A heading where every spelling is the user's must come out
    // exactly as written, however many compilations it takes.
    let reg = Registry::new(&[]);
    let scope = reg.mint_scope(ScopeKind::AnonRelation, Hint::None, None);
    let cols: Vec<ColId> = ["aid", "bid", "Name"]
        .into_iter()
        .enumerate()
        .map(|(_position, text)| {
            reg.sql_column(scope, Some(reg.intern(text, false)), Addressing::Published)
        })
        .collect();
    let bundle = Bundle::gather(vec![Statement {
        scopes: vec![scope],
        headings: vec![cols.clone()],
        refs: vec![],
    }]);
    for _ in 0..2 {
        let bap = baptise(&reg, &bundle.clone().reserve_authored(&reg)).unwrap();
        let spelled: Vec<String> = cols.iter().map(|c| spell_col(&bap, *c)).collect();
        assert_eq!(spelled, ["aid", "bid", "Name"]);
    }
}

#[test]
fn a_column_nobody_named_is_minted_fresh_every_compilation() {
    let reg = Registry::new(&[]);
    let (scope, cols) = unnamed_pair(&reg, "", false);
    let bundle = Bundle::gather(vec![Statement {
        scopes: vec![scope],
        headings: vec![cols.clone()],
        refs: vec![],
    }]);
    let first = baptise(&reg, &bundle.clone().reserve_authored(&reg)).unwrap();
    let second = baptise(&reg, &bundle.clone().reserve_authored(&reg)).unwrap();
    // Distinct within one heading: two columns are two names.
    assert_ne!(spell_col(&first, cols[0]), spell_col(&first, cols[1]));
    // And drawn again next time, so nobody can hold one.
    assert_ne!(spell_col(&first, cols[0]), spell_col(&second, cols[0]));
}

#[test]
fn a_qualified_reference_is_derived_not_carried() {
    let reg = Registry::new(&[]);
    let (_e, base, id, _l) = users(&reg);
    let stage = reg.mint_scope(ScopeKind::PipeStage, Hint::None, Some(base));
    let carried = restage(&reg, base, stage);
    let b = baptise(
        &reg,
        &Bundle::gather(vec![Statement {
            scopes: vec![base, stage],
            headings: vec![carried.to_vec()],
            refs: carried.to_vec(),
        }])
        .reserve_authored(&reg),
    )
    .unwrap();

    // Written from inside its own scope: bare.
    let mut here = String::new();
    b.write_ref(
        *carried.iter().next().unwrap(),
        stage,
        false,
        &mut Probe(&mut here),
    );
    assert!(!here.contains('.'));

    // Written from elsewhere: qualified, and the qualifier came from the
    // column's scope rather than from a field someone had to maintain.
    let mut there = String::new();
    b.write_ref(
        *carried.iter().next().unwrap(),
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
        *carried.iter().next().unwrap(),
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
    let scope = reg.mint_scope(ScopeKind::AnonRelation, Hint::None, None);

    for (name, expected) in [
        ("true", "_true"),
        ("FALSE", "_FALSE"),
        ("null", "_null"),
        ("ordinary", "ordinary"),
    ] {
        let spelling = reg.intern(name, false);
        let column = reg.sql_column(scope, Some(spelling), Addressing::Published);
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
    let names = baptise(&reg, &Bundle::default().reserve_authored(&reg)).unwrap();

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
    let scope = reg.mint_scope(ScopeKind::BaseTable { entity }, Hint::User(sp), None);
    let cols = columns
        .iter()
        .enumerate()
        .map(|(_position, column)| {
            reg.sql_column(
                scope,
                Some(reg.intern(column, false)),
                Addressing::Published,
            )
        })
        .collect();
    (scope, cols)
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
        &Bundle::gather(vec![Statement {
            scopes: vec![first, second],
            headings: vec![first_cols.clone(), second_cols.clone()],
            refs: vec![],
        }])
        .reserve_authored(&reg),
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
        &Bundle::gather(vec![Statement {
            scopes: vec![scope],
            headings: vec![cols],
            refs: vec![],
        }])
        .reserve_authored(&reg),
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
    let bundle = Bundle::gather(vec![Statement {
        scopes: vec![scope],
        headings: vec![cols],
        refs: vec![],
    }]);
    let first = baptise(&reg, &bundle.clone().reserve_authored(&reg)).expect("listed");
    let second = baptise(&reg, &bundle.clone().reserve_authored(&reg)).expect("listed");
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
    let bundle = Bundle::gather(vec![Statement {
        scopes: vec![first, second],
        headings: vec![first_cols.clone(), second_cols.clone()],
        refs: vec![first_cols[0], second_cols[0]],
    }]);
    let baptised =
        baptise(&reg, &bundle.clone().reserve_authored(&reg)).expect("both scopes are listed");
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
    let scope = reg.mint_scope(ScopeKind::AnonRelation, hint, None);
    let cols = (0..2)
        .map(|_| reg.sql_column(scope, None, Addressing::Published))
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
        &Bundle::gather(vec![Statement {
            scopes: vec![scope],
            headings: vec![cols.clone()],
            refs: vec![],
        }])
        .reserve_authored(&reg),
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
        &Bundle::gather(vec![Statement {
            scopes: vec![scope],
            headings: vec![cols.clone()],
            refs: vec![],
        }])
        .reserve_authored(&reg),
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
    let source = reg.mint_scope(ScopeKind::BaseTable { entity }, Hint::User(name), None);
    let source_id = reg.rebind_sql_column(id, source, reg.published(id));
    reg.fix_relation_scope(target, entity);

    let bundle = Bundle::gather(vec![Statement {
        // The source is listed first: without the fix it would take the
        // spelling, which is exactly the collision this rules out.
        scopes: vec![source, target],
        headings: vec![vec![source_id], vec![id]],
        refs: vec![source_id, id],
    }]);
    let b = baptise(&reg, &bundle.clone().reserve_authored(&reg)).expect("bundle names cleanly");
    assert_eq!(spell_scope(&b, target), "users");
    assert_ne!(spell_scope(&b, source), "users");
}

#[test]
fn an_authored_mint_spelling_preempts_the_canonical_draw() {
    use crate::names::Addressing;

    let reg = Registry::new(&[]);
    let scope = reg.anonymous_scope(None);
    // The author took the exact characters the canonical mint would draw
    // first. Stropped: `<mint:1>` is no classic identifier.
    let authored = reg.intern("<mint:1>", true);
    let named = reg.sql_column(scope, Some(authored), Addressing::Published);
    let anonymous = reg.sql_column(scope, None, Addressing::Published);
    let bundle = Bundle::gather(vec![Statement {
        scopes: vec![scope],
        headings: vec![vec![named, anonymous]],
        refs: vec![],
    }])
    .reserve_authored(&reg);
    let baptised = crate::names::baptism::baptise_with_policy(
        &reg,
        &bundle,
        crate::names::policy::NamePolicy::Canonical,
    )
    .expect("bundle names cleanly");
    let spell = |column| {
        let mut text = String::new();
        baptised.write_column(column, &mut crate::names::sink::Probe(&mut text));
        text
    };
    // ALIAS ALWAYS PRE-EMPTS A MINT: the invention skips the authored
    // characters, so no two outputs share one emitted name.
    assert_eq!(spell(named), "`<mint:1>`");
    assert_eq!(spell(anonymous), "<mint:2>");
}
