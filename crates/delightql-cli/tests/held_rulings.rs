// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE HELD QUESTIONS, RULED (RULINGS 2026-10-04, "Held questions, ruled one
//! at a time"). Each contract runs a statement through the CLI and reads
//! either its rows, as a bag, or the identity of its refusal. Expected rows
//! are written by hand.

use std::path::Path;
use std::process::{Command, Stdio};

/// What a statement answers: its rows, rendered as sorted JSON objects, or
/// the refusal text.
fn answer(dir: &Path, source: &str) -> Result<Vec<String>, String> {
    let database = dir.join("empty.sqlite");
    let output = Command::new(env!("CARGO_BIN_EXE_dql"))
        .args(["query", "--make-new-db-if-missing", "--sequential", "-f", "json", "--db"])
        .arg(&database)
        .arg(source)
        .current_dir(dir)
        .env("DQL_STATE_DIR", dir.join("state"))
        .env_remove("DQL_DIALECT")
        .stdin(Stdio::null())
        .output()
        .expect("spawn dql");
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned());
    }
    let rows: Vec<serde_json::Value> =
        serde_json::from_slice(&output.stdout).unwrap_or_else(|e| panic!("{source}: json: {e}"));
    let mut rendered: Vec<String> = rows.iter().map(|r| r.to_string()).collect();
    rendered.sort();
    Ok(rendered)
}

fn rows(source: &str, expected: &[&str]) {
    let dir = tempfile::tempdir().unwrap();
    let mut want: Vec<String> = expected
        .iter()
        .map(|e| serde_json::from_str::<serde_json::Value>(e).unwrap().to_string())
        .collect();
    want.sort();
    match answer(dir.path(), source) {
        Ok(got) => assert_eq!(got, want, "{source}"),
        Err(e) => panic!("{source}: refused: {e}"),
    }
}

fn refuses(source: &str, identity: &str) {
    let dir = tempfile::tempdir().unwrap();
    match answer(dir.path(), source) {
        Ok(got) => panic!("{source}: answered {got:?}, expected {identity}"),
        Err(e) => assert!(e.contains(identity), "{source}: expected {identity}, got: {e}"),
    }
}

// --- R1, R27: the min_multiplicity gate -----------------------------------

#[test]
fn r1_a_gate_no_correlated_union_spends_refuses() {
    refuses(
        "_(id @ 1; 2) as a (~~danger://semantics/min_multiplicity~~), _(id @ 1; 1) as b, a.id = b.id",
        "delightql-error://semantic/setop/min_multiplicity/unspent",
    );
}

#[test]
fn r1_a_gate_a_correlated_union_spends_pairs_copies() {
    rows(
        "_(id @ 1; 1; 2) as x (~~danger://semantics/min_multiplicity~~) ; _(id @ 1) as y, x.id = y.id",
        &[r#"{"id": 1}"#],
    );
}

#[test]
fn r1_a_gate_over_a_correlation_it_cannot_pair_is_uncovered() {
    refuses(
        "_(k @ 1) as a (~~danger://semantics/min_multiplicity~~) ; _(k @ 1) as b ; _(k @ 1) as c, b.k = c.k",
        "delightql-error://operational/uncovered",
    );
}

#[test]
fn r27_the_gate_over_a_partial_correlation_refuses() {
    refuses(
        r#"_(k, s @ "A1", "open"; "A1", "shut"; "A2", "open") as first (~~danger://semantics/min_multiplicity~~) ; _(k, s @ "A1", "open") as second, first.k = second.k"#,
        "delightql-error://semantic/setop/min_multiplicity/partial",
    );
}

#[test]
fn r27_the_gate_over_the_whole_row_pairs() {
    rows(
        r#"_(k, s @ "A1", "open"; "A1", "shut"; "A2", "open") as first (~~danger://semantics/min_multiplicity~~) ; _(k, s @ "A1", "open") as second, first.* = second.*"#,
        &[r#"{"k": "A1", "s": "open"}"#],
    );
}

// --- R30, R34: the scope/duplicate gate is retired ------------------------

#[test]
fn r30_two_live_scopes_sharing_a_name_refuse() {
    refuses("_(x @ 1) as t, _(y @ 2) as t", "delightql-error://semantic/scope/duplicate");
}

#[test]
fn r30_no_gate_admits_two_live_scopes_sharing_a_name() {
    refuses(
        "_(x @ 1) as t, _(y @ 2) as t (~~danger://scope/duplicate ~~)",
        "delightql-error://parse/danger/unknown",
    );
}

#[test]
fn r30_a_caller_gate_does_not_admit_a_consulted_body() {
    refuses(
        "(~~ddl both(*) :- _(x @ 1) as t, _(y @ 2) as t ~~)\nboth(*) (~~danger://scope/duplicate ~~)",
        "delightql-error://parse/danger/unknown",
    );
}

#[test]
fn r34_a_body_gate_does_not_admit_its_body() {
    refuses(
        "(~~ddl both(*) :- _(x @ 1) as t, _(y @ 2) as t (~~danger://scope/duplicate ~~) ~~)\nboth(*)",
        "delightql-error://parse/danger/unknown",
    );
}

#[test]
fn r30_the_alias_teaches_the_same_rows() {
    rows("_(x @ 1) as t, _(y @ 2) as u", &[r#"{"x": 1, "y": 2}"#]);
}

// --- R4: a project-out takes selectors -----------------------------------

#[test]
fn r4_a_project_out_removes_what_a_pattern_addresses() {
    rows(r#"_(BirthDate, HireDate, name @ "1990", "2015", "ann") |> -(/Date$/)"#, &[r#"{"name": "ann"}"#]);
}

#[test]
fn r4_a_project_out_removes_a_range_and_a_qualified_glob() {
    rows(r#"_(a, b, c @ 1, 2, 3) |> -(|1:2|)"#, &[r#"{"c": 3}"#]);
    rows(r#"_(a, b @ 1, 2) as t, _(c @ 3) as u |> -(t.*)"#, &[r#"{"c": 3}"#]);
}

// --- R6: a selector must address a column --------------------------------

#[test]
fn r6_a_pattern_matching_nothing_refuses_beside_other_items() {
    refuses(r#"_(name, age @ "ann", 3) |> (name, /^zzz/)"#, "delightql-error://semantic/constraint/selector_empty");
}

#[test]
fn r6_a_pattern_matching_nothing_refuses_in_a_record() {
    refuses(r#"_(name, age @ "ann", 3) ~> {name, /^zzz/} as bag"#, "delightql-error://semantic/constraint/selector_empty");
}

#[test]
fn r6_a_pattern_matching_nothing_refuses_in_a_project_out() {
    refuses(r#"_(name, age @ "ann", 3) |> -(/_tmp$/)"#, "delightql-error://semantic/constraint/selector_empty");
}

// --- R7, R14, R25: duplicate authored publications ------------------------

#[test]
fn r7_a_payload_naming_its_grouping_key_refuses() {
    refuses("_(k, v @ 1, 10; 1, 20; 2, 30) |> %(k ~> (k, v) <~ #(v desc))", "Duplicate column 'k'");
    rows(
        "_(k, v @ 1, 10; 1, 20; 2, 30) |> %(k ~> (v) <~ #(v desc))",
        &[r#"{"k": 1, "v": 20}"#, r#"{"k": 2, "v": 30}"#],
    );
}

#[test]
fn r7_a_payload_selector_covers_the_rest_of_the_row() {
    rows(
        "_(k, v, w @ 1, 10, 1; 1, 20, 2; 2, 30, 3) |> %(k ~> (*) <~ #(v desc))",
        &[r#"{"k": 1, "v": 20, "w": 2}"#, r#"{"k": 2, "v": 30, "w": 3}"#],
    );
    rows(
        "_(k, v, w @ 1, 10, 1; 1, 20, 2; 2, 30, 3) |> %(k ~> (/^[kv]$/) <~ #(v desc))",
        &[r#"{"k": 1, "v": 20}"#, r#"{"k": 2, "v": 30}"#],
    );
}

#[test]
fn r7_the_key_under_a_new_name_is_a_new_column() {
    rows(
        "_(k, v @ 1, 10; 1, 20; 2, 30) |> %(k ~> (v, k as kk) <~ #(v desc))",
        &[r#"{"k": 1, "v": 20, "kk": 1}"#, r#"{"k": 2, "v": 30, "kk": 2}"#],
    );
    refuses(
        "_(k, v @ 1, 10; 1, 20; 2, 30) |> %(k ~> (v as k) <~ #(v desc))",
        "delightql-error://semantic/constraint",
    );
}

#[test]
fn r14_overlapping_ranges_over_a_named_column_refuse() {
    refuses("_(a, b @ 1, 2) |> (|1:2|, |2:2|)", "Duplicate column 'b'");
}

#[test]
fn r14_overlapping_ranges_over_an_unnamed_column_mint() {
    let dir = tempfile::tempdir().unwrap();
    let got = answer(dir.path(), "_(2) as q |> (|1|, |1:1|)").expect("two minted columns");
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].matches(":2").count(), 2, "{got:?}");
}

#[test]
fn r25_a_cover_over_a_riding_name_refuses() {
    refuses(r#"_(a @ "x"; "y") |> +$(upper:() as :"{@}")(a)"#, "Duplicate column 'a'");
    rows(r#"_(a @ "x"; "y") |> +$(upper:() as :"{@}_up")(a)"#, &[r#"{"a": "x", "a_up": "X"}"#, r#"{"a": "y", "a_up": "Y"}"#]);
}

// --- R10, R33: name templates ---------------------------------------------

#[test]
fn r10_a_template_position_is_the_displayed_position() {
    rows(
        r#"_(id, first_name, last_name @ 1, "ann", "lee") |> *( /name/ as :"{@}_{#}" )"#,
        &[r#"{"id": 1, "first_name_2": "ann", "last_name_3": "lee"}"#],
    );
    rows(r#"_(a, b @ "x", "y") |> *( * as :"{#}" )"#, &[r#"{"1": "x", "2": "y"}"#]);
}

#[test]
fn r33_a_template_without_a_placeholder_refuses() {
    refuses(r#"_(a, b @ -1, -2) |> +$(abs:() as :"same")(a, b)"#, "writes no placeholder");
    refuses(r#"_(a, b @ -1, -2) |> +$(abs:() as :"same")(a)"#, "writes no placeholder");
    rows("_(a, b @ -1, -2) |> +(abs:(a) as same)", &[r#"{"a": -1, "b": -2, "same": 1}"#]);
}

/// `refuses`, with files written into the statement's directory first.
fn refuses_with(files: &[(&str, &str)], source: &str, identity: &str) {
    let dir = tempfile::tempdir().unwrap();
    for (path, text) in files {
        let at = dir.path().join(path);
        std::fs::create_dir_all(at.parent().unwrap()).unwrap();
        std::fs::write(at, text).unwrap();
    }
    match answer(dir.path(), source) {
        Ok(got) => panic!("{source}: answered {got:?}, expected {identity}"),
        Err(e) => assert!(e.contains(identity), "{source}: expected {identity}, got: {e}"),
    }
}

// --- R2, R17: the signed witness ------------------------------------------

#[test]
fn r2_the_signed_witness_widens_every_row() {
    rows("_(id @ 1; 2; 3) +-", &[r#"{"id": 1, "met": 1}"#, r#"{"id": 2, "met": 1}"#, r#"{"id": 3, "met": 1}"#]);
    rows("_(id @ 1; 2), id > 5 +-", &[r#"{"id": null, "met": 0}"#]);
}

#[test]
fn r17_a_clause_ending_in_a_witness_refuses() {
    refuses(
        "(~~ddl\na!(*) :- _(msg @ \"a\") |> insert!(log(*))(*)\nmain!(*) :- a!(+-)\n~~)\nmain!(*)",
        "delightql-error://semantic/effect/rule/ending",
    );
}

// --- R12: no once-only promise --------------------------------------------

#[test]
fn r12_a_volatile_configured_value_compiles_without_a_promise() {
    let dir = tempfile::tempdir().unwrap();
    let got = answer(
        dir.path(),
        "coin_gate(token, T(*))(*) : T(*), (abs:($.token) % 2) = 0\n\
         twice(P(... T(*))(*), I(*))(*) : I(*) |> P(*) |> P(*)\n\
         twice(coin_gate(random:()), _(id @ 1; 2; 3; 4; 5; 6; 7; 8))(*) ~> count:(*) as kept",
    )
    .expect("a count, any count from 0 to 8 (register U7)");
    assert_eq!(got.len(), 1, "{got:?}");
}

// --- R22, R32, R36, R37, R38: definitions ---------------------------------

#[test]
fn r22_a_correlated_population_through_an_instance_refuses() {
    refuses(
        "idr(T(*))(*) : T(*)\no(*) : _(uid, oid @ 1, 10; 1, 11; 2, 20)\n_(uid @ 1; 2) as u, o(, o.uid = u.uid |> idr(*)) |> (u.uid, oid)",
        "delightql-error://semantic/interior/correlation/support",
    );
    rows(
        "idr(T(*))(*) : T(*)\no(*) : _(uid, oid @ 1, 10; 1, 11; 2, 20)\noi(*) : o(*) |> idr(*)\n_(uid @ 1; 2) as u, oi(, oi.uid = u.uid) |> (u.uid, oid)",
        &[r#"{"uid": 1, "oid": 10}"#, r#"{"uid": 1, "oid": 11}"#, r#"{"uid": 2, "oid": 20}"#],
    );
}

#[test]
fn r32_a_declared_contract_refuses_an_open_family() {
    refuses_with(
        &[("ddl/lib.dql", "keep_small(T(*))(*) :- T(*), n < 10\n")],
        "consult!(\"ddl/lib.dql\", \"lib\")(*)\nenlist!(\"lib\")(*)\nnarrow(I(*), P(... T(*))(id))(id) : I(*) |> P(*)\nnarrow(_(id, n @ 1, 5; 2, 50), keep_small(*))(*)",
        "delightql-error://semantic/resolution/ho/residual-contract",
    );
}

#[test]
fn r36_an_empty_capture_refuses() {
    refuses("`double`:(..{}, x): (x * 2)\n_(age @ 3) |> (double:(age) as doubled)", "delightql-error://semantic/constraint");
}

#[test]
fn r37_an_implicit_capture_called_positionally_refuses() {
    refuses(
        "report:(.., id): :\"{id}: {first_name}\"\n_(id, first_name @ 1, \"ann\") |> (report:(first_name, id) as r)",
        "delightql-error://semantic/constraint",
    );
}

#[test]
fn r38_a_consulted_function_captures_its_callers_row() {
    rows(
        "(~~ddl\napply_tax:(.., rate) :- price * rate\n~~)\n_(name, price @ \"pen\", 10) |> (name, apply_tax:(.., 1.5) as taxed)",
        &[r#"{"name": "pen", "taxed": 15}"#],
    );
}

// --- D3, R20, R29: pivots --------------------------------------------------

#[test]
fn d3_a_text_key_names_its_column() {
    rows(
        r#"_(e, m, v @ "e1", "q 1", 10; "e1", "q2", 20), m in ("q 1"; "q2") |> %(e ~> max:(v) of m)"#,
        &[r#"{"e": "e1", "q 1": 10, "q2": 20}"#],
    );
}

#[test]
fn r29_a_number_key_refuses_and_a_template_names_it() {
    refuses(
        r#"_(e, m, v @ "e1", 1, 10; "e1", 2, 20), m in (1; 2) |> %(e ~> max:(v) of m)"#,
        "delightql-error://semantic/constraint/pivot",
    );
    rows(
        r#"_(e, m, v @ "e1", 1, 10; "e1", 2, 20), m in (1; 2) |> %(e ~> max:(v) of :"m{m}")"#,
        &[r#"{"e": "e1", "m1": 10, "m2": 20}"#],
    );
}

#[test]
fn r20_a_template_names_a_further_pivot() {
    rows(
        r#"_(name, subject, score, grade @ "Al", "Maths", 90, "A"; "Al", "Music", 85, "B"), subject in ("Maths"; "Music") |> %(name ~> max:(score) of subject, max:(grade) of :"{subject}_grade")"#,
        &[r#"{"name": "Al", "Maths": 90, "Music": 85, "Maths_grade": "A", "Music_grade": "B"}"#],
    );
}

// --- R3, R9, R28: documents ------------------------------------------------

#[test]
fn r3_iterating_a_made_record_refuses() {
    refuses(r#"_(x @ 1) |> ({"a": x} as r), r ~= ~> {a}"#, "delightql-error://semantic/narrowing/object_literal");
    rows(r#"_(x @ 1) |> ({"a": x} as r), r ~= {a}"#, &[r#"{"r": "{\"a\":1}", "a": 1}"#]);
}

#[test]
fn r9_a_truth_member_holds_the_targets_value() {
    rows(r#"_(a @ 1) ~> {"v": (a = 1)} as r"#, &[r#"{"r": "[{\"v\":1}]"}"#]);
}

#[test]
fn r28_side_by_side_iteration_refuses() {
    refuses(
        r#"_(d @ {"xs": [[1], [2]], "ys": [[10], [20]]}) |> (d), d ~= {"xs": ~> [.0 as x], "ys": ~> [.0 as y]}"#,
        "delightql-error://semantic/resolution/ambiguous",
    );
}

// --- R11, R19, R21: guards and the keyless delegate -------------------------

#[test]
fn r11_a_row_wise_guard_makes_the_value_null_where_it_fails() {
    rows(
        r#"_(name, country @ null, "US"; null, "SE"; "a", "SE") |> (ifnull:(name, "-" | country = "US") as n)"#,
        &[r#"{"n": "-"}"#, r#"{"n": null}"#, r#"{"n": null}"#],
    );
    rows("twice:(x) : x * 2\n_(v @ 1; 5) |> (twice:(v | v > 2) as t)", &[r#"{"t": null}"#, r#"{"t": 10}"#]);
}

#[test]
fn r19_a_windowed_guard_filters_the_frame() {
    rows(
        "_(id, v @ 1, 9; 2, -2; 3, 7) |> (id, sum:(v | v > 0 <~ #(id)) as s)",
        &[r#"{"id": 1, "s": 9}"#, r#"{"id": 2, "s": 9}"#, r#"{"id": 3, "s": 16}"#],
    );
    rows(
        "_(id, v @ 1, 9; 2, -2; 3, 7) |> (id, count:(* | v > 0 <~ #(id)) as c)",
        &[r#"{"id": 1, "c": 1}"#, r#"{"id": 2, "c": 1}"#, r#"{"id": 3, "c": 2}"#],
    );
    refuses(
        "_(id, v @ 1, 9; 2, -2; 3, 7) |> (id, row_number:(| v > 0 <~ #(id)) as r)",
        "delightql-error://semantic/window/guard",
    );
}

#[test]
fn r21_a_keyless_delegate_over_no_rows_publishes_one_row() {
    rows(
        r#"_(amount, rep @ 1, "a"), amount > 5 |> %(~> count:(*) as n, (rep) <~ #(amount desc))"#,
        &[r#"{"n": 0, "rep": null}"#],
    );
    rows(
        r#"_(amount, rep @ 1, "a"; 3, "b") |> %(~> count:(*) as n, (rep) <~ #(amount desc))"#,
        &[r#"{"n": 2, "rep": "b"}"#],
    );
}

// --- R5, R24, R26, R35, D1, D2: access, edges, targets -----------------------

fn rows_in(dir: &Path, source: &str, expected: &[&str]) {
    let mut want: Vec<String> = expected
        .iter()
        .map(|e| serde_json::from_str::<serde_json::Value>(e).unwrap().to_string())
        .collect();
    want.sort();
    match answer(dir, source) {
        Ok(got) => assert_eq!(got, want, "{source}"),
        Err(e) => panic!("{source}: refused: {e}"),
    }
}

fn with_files(files: &[(&str, &str)]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    for (path, text) in files {
        let at = dir.path().join(path);
        std::fs::create_dir_all(at.parent().unwrap()).unwrap();
        std::fs::write(at, text).unwrap();
    }
    dir
}

const KINDS: &str = "pair(*) :- _(a, b @ 1, \"x\"; 2, \"y\"; 3, \"z\")\n\
                     quad(*) :- _(a, c @ 1, 10; 2, 20)\n\
                     pair(*) &(::pq) quad(*) :- pair(*), quad(*), pair.a = quad.a\n";

#[test]
fn r5_a_comparison_over_unlike_arms_is_the_targets() {
    let dir = tempfile::tempdir().unwrap();
    let db = rusqlite::Connection::open(dir.path().join("empty.sqlite")).unwrap();
    db.execute_batch(
        "create table at(x TEXT, tag TEXT); insert into at values ('1', 't1');\n\
         create table ai(x INTEGER, tag TEXT); insert into ai values (1, 'i1');",
    )
    .unwrap();
    drop(db);
    rows_in(dir.path(), "at(*) ; ai(*), x = 1 |> (tag)", &[r#"{"tag": "i1"}"#]);
}

#[test]
fn r24_a_table_function_is_accessed_by_position() {
    refuses(r#"json_each("[7,8]")(key, value)"#, "delightql-error://semantic/arity");
    rows(
        r#"json_each("[7,8]")(*) |> (key, value)"#,
        &[r#"{"key": 0, "value": 7}"#, r#"{"key": 1, "value": 8}"#],
    );
    rows(
        r#"json_each("[7,8]")(k, v, _, _, _, _, _, _)"#,
        &[r#"{"k": 0, "v": 7}"#, r#"{"k": 1, "v": 8}"#],
    );
    rows(r#"json_each("[7,8]")(k, 8, _, _, _, _, _, _)"#, &[r#"{"k": 1}"#]);
    rows(
        r#"_(id, body @ 1, "[5]"; 2, "[]"), json_each?(body)(_, value, _, _, _, _, _, _) |> (id, value)"#,
        &[r#"{"id": 1, "value": 5}"#, r#"{"id": 2, "value": null}"#],
    );
}

#[test]
fn r26_a_qualified_use_finds_its_namespaces_edge() {
    let dir = with_files(&[("ddl/kinds.dql", KINDS)]);
    rows_in(
        dir.path(),
        "consult!(\"ddl/kinds.dql\", \"lib::k\")(*)\nlib::k.pair(*) &(::pq) lib::k.quad(*) |> (pair.a, b, c)",
        &[r#"{"a": 1, "b": "x", "c": 10}"#, r#"{"a": 2, "b": "y", "c": 20}"#],
    );
}

#[test]
fn r35_an_edge_over_another_relation_than_its_use_refuses() {
    refuses_with(
        &[("ddl/kinds.dql", KINDS)],
        "consult!(\"ddl/kinds.dql\", \"lib\")(*)\nenlist!(\"lib\")(*)\nquad(*) : _(a, c @ 1, 99)\npair(*) &(::pq) quad(*) |> (pair.a, b, c)",
        "delightql-error://semantic/grounding/er/term_world",
    );
    refuses_with(
        &[("ddl/kinds.dql", KINDS)],
        "consult!(\"ddl/kinds.dql\", \"lib\")(*)\nenlist!(\"lib\")(*)\npair(*) : _(a, b @ 1, \"q\")\npair(*) &(::pq) quad(*) |> (pair.a, b, c)",
        "delightql-error://semantic/grounding/er/term_world",
    );
    let dir = with_files(&[("ddl/kinds.dql", KINDS)]);
    rows_in(
        dir.path(),
        "consult!(\"ddl/kinds.dql\", \"lib\")(*)\nenlist!(\"lib\")(*)\npair(*) &(::pq) quad(*) |> (pair.a, b, c)",
        &[r#"{"a": 1, "b": "x", "c": 10}"#, r#"{"a": 2, "b": "y", "c": 20}"#],
    );
}

#[test]
fn d1_the_slash_names_an_engine_schema() {
    rows(
        "_(a @ 1; 2) |> temp_table!(staged(*))(*)\ntemp/staged(*)",
        &[r#"{"a": 1}"#, r#"{"a": 2}"#],
    );
    refuses("nope/users(*)", "delightql-error://semantic/resolution/schema");
}

#[test]
fn d2_a_patterned_boundary_after_a_set_operator_patterns_its_arm() {
    rows("_(a @ 1) ; _(a @ 2) as u(x) |> (x)", &[r#"{"x": null}"#, r#"{"x": 2}"#]);
}

// --- R13, R15, R16, R18, R23, R31, R39: set operations ----------------------

#[test]
fn r13_a_condition_naming_one_arm_filters_that_arm() {
    rows(
        r#"_(id, plan @ 1, "free") as a ; _(id, plan @ 2, "pro"; 3, "free") as b, b.plan = "pro""#,
        &[r#"{"id": 1, "plan": "free"}"#, r#"{"id": 2, "plan": "pro"}"#],
    );
    refuses(
        "_(id @ 1) as a ; _(id @ 1; 2) as b, id = b.id",
        "carried by more than one operand",
    );
}

#[test]
fn r15_a_positional_union_correlates_by_qualified_name() {
    rows(
        r#"_(a, b @ 1, "x"; 2, "y") as first || _(c, d @ 2, "p"; 4, "q") as second, first.a = second.c"#,
        &[r#"{"a": 2, "b": "y"}"#, r#"{"a": 2, "b": "p"}"#],
    );
}

#[test]
fn r16_any_two_arm_condition_is_a_correlation() {
    rows(
        "_(id, price @ 1, 10; 2, 30) as x ; _(id, price @ 3, 20) as y, x.price < y.price",
        &[r#"{"id": 1, "price": 10}"#, r#"{"id": 3, "price": 20}"#],
    );
    rows(
        "_(id, price @ 1, 10; 2, 30) as x - _(id, price @ 3, 20) as y, x.price < y.price",
        &[r#"{"id": 2, "price": 30}"#],
    );
    rows("_(k @ null) as x ; _(k @ null) as y, x.k = y.k", &[r#"{"k": null}"#, r#"{"k": null}"#]);
}

#[test]
fn r18_one_pair_is_one_correlation() {
    rows(
        r#"_(id, e @ 1, "a"; 1, "b") as x ; _(id, e @ 1, "a"; 1, "c") as y, x.id = y.id, x.e = y.e"#,
        &[r#"{"id": 1, "e": "a"}"#, r#"{"id": 1, "e": "a"}"#],
    );
}

#[test]
fn r23_a_correlation_reaches_exactly_its_two_arms() {
    rows(
        "_(k @ 1; 2) as a ; _(k @ 9) as b ; _(k @ 2; 3) as c, a.k = c.k",
        &[r#"{"k": 2}"#, r#"{"k": 9}"#, r#"{"k": 2}"#],
    );
    rows(
        "_(k @ 1; 2) as a ; _(k @ 2) as b - _(k @ 2) as c, a.k = c.k",
        &[r#"{"k": 1}"#, r#"{"k": 2}"#],
    );
}

#[test]
fn r31_a_whole_heading_correlation_sharing_no_name_refuses() {
    refuses("_(a @ 1) as x ; _(b @ 1) as y, x.* = y.*", "delightql-error://semantic/constraint/selector_empty");
}

#[test]
fn r39_a_correlation_names_only_what_its_arm_publishes() {
    refuses("_(id @ 1) as x ; _(id, w @ 1, 5) as y, x.w = y.w", "delightql-error://semantic/resolution/column");
}

// --- review.1 R1, R2: routing by resolved dependency; the gate's whole key --

#[test]
fn review1_r1_a_condition_routes_by_the_arms_it_reads() {
    let prefix = "a(*) : _(k @ 9)\n_(k @ 1;2) as a ; _(k @ 3;4) as b, ";
    rows(&format!("{prefix}+a(, a.k = 1)"), &[]);
    rows(&format!("{prefix}\\+a(, a.k = 9)"), &[]);
    rows(&format!("{prefix}+a(, k = 1)"), &[]);
    rows(
        "t(*) : _(k @ 9)\n_(k @ 1;2) as a ; _(k @ 3;4) as b, +t(, a.k = 1)",
        &[r#"{"k": 1}"#, r#"{"k": 3}"#, r#"{"k": 4}"#],
    );
}

#[test]
fn review1_r2_the_gate_judges_a_key_written_in_several_conjuncts_whole() {
    let gate = "_(k,v @ 1,2;1,2;1,3) as a (~~danger://semantics/min_multiplicity~~) ; _(k,v @ 1,2;1,2;1,2) as b, ";
    let matched = [r#"{"k": 1, "v": 2}"#, r#"{"k": 1, "v": 2}"#];
    rows(&format!("{gate}a.k = b.k, a.v = b.v"), &matched);
    rows(&format!("{gate}(a.k = b.k) and (a.v = b.v)"), &matched);
    rows(&format!("{gate}a.* = b.*"), &matched);
    refuses(&format!("{gate}a.k = b.k"), "delightql-error://semantic/setop/min_multiplicity/partial");
}

// --- review.2 R1: no spelling decides before resolution ---------------------

#[test]
fn review2_r1_a_local_qualifier_triggers_no_arm_check() {
    let defs = "a(*) : _(k @ 9)\n";
    rows(&format!("{defs}_(k @ 1;2) as a ; _(k @ 3;4) as b, (+a(, a.k = 1)) or (k = 1)"), &[r#"{"k": 1}"#]);
    rows(&format!("{defs}_(k @ 1;2) as a ; _(k @ 3;4) as b, (+a(, k = 1)) or (k = 1)"), &[r#"{"k": 1}"#]);
    rows(&format!("{defs}_(k @ 1;2) as a ; _(k @ 3;4) as a, +a(, a.k = 1)"), &[]);
    rows(&format!("{defs}_(k @ 1;2) as a ; _(k @ 3;4) as a, +a(, k = 1)"), &[]);
}

#[test]
fn review2_r1_an_arm_addressed_condition_keeps_its_refusals() {
    refuses("_(k @ 1;2) as a ; _(k @ 3;4) as a, a.k = 1", "is ambiguous");
    refuses("_(id @ 1) as a ; _(id @ 1; 2) as b, id = b.id", "carried by more than one operand");
    refuses("_(a @ 1) as x || _(z @ 2) as y, z = 2", "delightql-error://semantic/resolution/column");
}

// --- review.3 R1: a qualified-only arm scope stays so under nesting ---------

#[test]
fn review3_r1_a_nested_region_keeps_the_enclosing_arms_qualified_only() {
    let p = "a(*) : _(n @ 1)\n_(k @ 1) as x || _(z @ 2) as y, ";
    let column = "delightql-error://semantic/resolution/column";
    refuses(&format!("{p}(+a(, n = 1 ; _(n @ 2), n > 0)) or (z = 2)"), column);
    refuses(&format!("{p}(z = 2) or (+a(, n = 1 ; _(n @ 2), n > 0))"), column);
    refuses(&format!("{p}(+a(, n = 1)) or (z = 2)"), column);
    rows(&format!("{p}(+a(, n = 1 ; _(n @ 2), n > 0)) or (k = 1)"), &[r#"{"k": 1}"#, r#"{"k": 2}"#]);
    refuses(&format!("{p}(+a(, n = 1 ; _(n @ 2), +a(, n = 1 ; _(n @ 3), n > 0))) or (z = 2)"), column);
}

#[test]
fn review3_r1_an_inner_judgment_reaches_an_enclosing_arm_by_qualifier_only() {
    let p = "a(*) : _(n @ 1)\n_(k @ 1) as x || _(z @ 2) as y, ";
    rows(&format!("{p}+a(, n = 1 ; _(n @ 2), n = y.z)"), &[r#"{"k": 1}"#, r#"{"k": 2}"#]);
    refuses(&format!("{p}+a(, n = 1 ; _(n @ 2), n = z)"), "delightql-error://semantic/resolution/column");
}
