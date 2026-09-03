// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The cancellable entrances: a parse that completes answers the same tree
//! the uncancellable entrance answers, a predicate that says stop cancels,
//! and the submission road obeys the same framing law either way.

use delightql_cst::{CancellableParse, Parser, Root};

/// A completing cancellable parse is the ordinary parse: same defects
/// verdict, same root branch presence, same authored source.
#[test]
fn a_completing_parse_matches_the_uncancellable_entrance() {
    for input in ["users(*) |> (id)", "users(*) |>", "", ".help"] {
        let plain = Parser::new().parse_prompt(input);
        let mut never = |_: usize| false;
        match Parser::new().parse_prompt_cancellable(input, &mut never) {
            CancellableParse::Completed(tree) => {
                assert_eq!(tree.has_defects(), plain.has_defects(), "input: {input:?}");
                assert_eq!(
                    tree.root_branch().is_some(),
                    plain.root_branch().is_some(),
                    "input: {input:?}"
                );
                assert_eq!(tree.source(), input);
                assert_eq!(tree.entrance(), plain.entrance());
            }
            CancellableParse::Cancelled { .. } => {
                panic!("a never-cancelling predicate must complete: {input:?}")
            }
        }
    }
}

/// A predicate that answers stop cancels the parse. Large input guarantees
/// the runtime reaches a cooperative checkpoint at least once.
#[test]
fn an_always_stop_predicate_cancels() {
    let large = "users(*), ".repeat(20_000) + "users(*)";
    let mut polled = 0usize;
    let mut always = |_: usize| {
        polled += 1;
        true
    };
    match Parser::new().parse_prompt_cancellable(&large, &mut always) {
        CancellableParse::Cancelled { .. } => {}
        CancellableParse::Completed(_) => panic!("an always-stop predicate must cancel"),
    }
    assert!(polled > 0, "the predicate must have been polled");
}

/// The submission road frames exactly as the uncancellable one: marked text
/// takes the utility entrance, unmarked text takes the prompt wrap.
#[test]
fn the_submission_road_keeps_the_framing_law() {
    let mut never = |_: usize| false;
    let marked = "#!dql query-sequence\nusers(*)";
    match Parser::new().parse_submission_cancellable(marked, &mut never) {
        CancellableParse::Completed(tree) => {
            assert_eq!(tree.entrance(), Root::QuerySequence);
            assert!(!tree.has_defects());
        }
        CancellableParse::Cancelled { .. } => panic!("must complete"),
    }
    match Parser::new().parse_submission_cancellable("users(*)", &mut never) {
        CancellableParse::Completed(tree) => {
            assert_eq!(tree.entrance(), Root::DefinitionFile);
            assert!(!tree.has_defects());
        }
        CancellableParse::Cancelled { .. } => panic!("must complete"),
    }
}

/// A parser that cancelled is reusable: the next parse on the same parser
/// answers about the new text, not the aborted one.
#[test]
fn a_cancelled_parser_is_reusable() {
    let mut parser = Parser::new();
    let large = "users(*), ".repeat(20_000) + "users(*)";
    let mut always = |_: usize| true;
    assert!(matches!(
        parser.parse_prompt_cancellable(&large, &mut always),
        CancellableParse::Cancelled { .. }
    ));
    let mut never = |_: usize| false;
    match parser.parse_prompt_cancellable("users(*)", &mut never) {
        CancellableParse::Completed(tree) => {
            assert!(!tree.has_defects());
            assert_eq!(tree.source(), "users(*)");
        }
        CancellableParse::Cancelled { .. } => panic!("a fresh parse must complete"),
    }
    // The ordinary entrance works after a cancellation too.
    let plain = parser.parse_prompt("users(*) |> (id)");
    assert!(!plain.has_defects());
}

/// Measurement-only: representative prompt-parse costs, for selecting the
/// REPL parser budgets. Timing is reported, never asserted.
/// Run with:
/// `cargo test -p delightql-cst --test cancellable measure_prompt_parse_costs -- --ignored --nocapture`
#[test]
#[ignore = "measurement-only; no timing threshold belongs in CI"]
fn measure_prompt_parse_costs() {
    let large_valid = "users(*), ".repeat(10_000) + "users(*)"; // ~100 KB
    let large_broken = large_valid.replace("users(*)", "users(* |1|<");
    let cliff46 = "(~~ddl gen(step,stop)(*) :- _(1) : a a(*), |1|"; // one char short of the freeze
    let samples: &[(&str, &str)] = &[
        ("small_valid", "users(*) |> (id)"),
        ("small_malformed", "users(*) |>"),
        ("medium_valid", "users(uid, name), orders(_, uid, total) |> %(name ~> sum:(total) as spent)"),
        ("cliff_prefix_46", cliff46),
        ("large_valid_100k", &large_valid),
        ("large_malformed_100k", &large_broken),
    ];
    for (name, input) in samples {
        let mut parser = Parser::new();
        let mut never = |_: usize| false;
        // warm
        let _ = parser.parse_prompt_cancellable(input, &mut never);
        const N: usize = 20;
        let mut times = Vec::with_capacity(N);
        for _ in 0..N {
            let started = std::time::Instant::now();
            let done = parser.parse_prompt_cancellable(input, &mut never);
            times.push(started.elapsed().as_secs_f64() * 1000.0);
            assert!(matches!(done, CancellableParse::Completed(_)));
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        eprintln!(
            "{name}: bytes={} median={:.3}ms p95={:.3}ms max={:.3}ms",
            input.len(),
            times[N / 2],
            times[(N * 95).div_ceil(100) - 1],
            times[N - 1]
        );
    }
}

/// The framing-road answer matches what parse_submission does, without a
/// parse: unmarked → prompt wrap; authored header → utility entrance; a
/// MISPLACED header still names the utility entrance (the author said which
/// world the text is in, in the wrong place).
#[test]
fn submission_road_matches_the_framing_law() {
    use delightql_cst::submission_road;
    assert_eq!(submission_road("users(*)"), Root::DefinitionFile);
    assert_eq!(
        submission_road("#!dql query-sequence\nusers(*)"),
        Root::QuerySequence
    );
    assert_eq!(
        submission_road("users(*)\n#!dql query-sequence\n"),
        Root::QuerySequence,
        "a misplaced header names the same entrance"
    );
}

/// A cancelled submission parse reports the road it was on — the utility
/// entrance for marked bytes, the prompt wrap for unmarked.
#[test]
fn a_cancelled_submission_reports_its_entrance() {
    let large_marked = format!("#!dql query-sequence\n{}", "users(*)\n".repeat(30_000));
    let mut always = |_: usize| true;
    match Parser::new().parse_submission_cancellable(&large_marked, &mut always) {
        CancellableParse::Cancelled { entrance, .. } => {
            assert_eq!(entrance, Root::QuerySequence)
        }
        CancellableParse::Completed(_) => panic!("an always-stop predicate must cancel"),
    }
    let large_unmarked = "users(*), ".repeat(20_000) + "users(*)";
    let mut always = |_: usize| true;
    match Parser::new().parse_prompt_cancellable(&large_unmarked, &mut always) {
        CancellableParse::Cancelled { entrance, .. } => {
            assert_eq!(entrance, Root::DefinitionFile)
        }
        CancellableParse::Completed(_) => panic!("an always-stop predicate must cancel"),
    }
}
