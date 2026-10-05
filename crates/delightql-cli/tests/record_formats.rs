// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The record formats at the host boundary: what `-f csv` and `-f tsv`
//! print reaches a terminal unless the user opts out, so a dangerous control
//! byte is spelled out by default and raw only under `--no-sanitize`, while
//! the encodings keep the observation contract's values distinct both ways.

use std::path::Path;
use std::process::{Command, Output, Stdio};

fn run(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_dql"))
        .args(args)
        .current_dir(dir)
        .env("DQL_STATE_DIR", dir.join("state"))
        .env_remove("DQL_DIALECT")
        .env_remove("DQL_TEST_PANIC")
        .env_remove("RUST_BACKTRACE")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("dql runs")
}

fn stdout(dir: &Path, args: &[&str]) -> Vec<u8> {
    let out = run(dir, args);
    assert!(
        out.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}

#[test]
fn a_control_byte_crosses_a_record_format_only_under_the_explicit_opt_out() {
    let tmp = tempfile::tempdir().unwrap();
    let query = "_(x @ \"\u{1b}[31mRED\")";
    for format in ["csv", "tsv"] {
        let displayed = stdout(tmp.path(), &["query", "-n", "-f", format, query]);
        assert!(
            !displayed.contains(&0x1b),
            "{format}: ESC reached stdout: {displayed:?}"
        );
        // CSV carries the spelling as written; TSV doubles its backslash
        // under its own escape law, so a reader recovers the spelling.
        let spelled: &[u8] = if format == "csv" {
            b"\\x1B[31mRED"
        } else {
            b"\\\\x1B[31mRED"
        };
        assert!(
            displayed.starts_with(spelled),
            "{format}: the control byte is spelled out: {displayed:?}"
        );
        let faithful = stdout(
            tmp.path(),
            &["query", "-n", "-f", format, "--no-sanitize", query],
        );
        assert!(
            faithful.starts_with(b"\x1b[31mRED"),
            "{format}: the opt-out is faithful: {faithful:?}"
        );
    }
}

#[test]
fn the_record_encodings_stay_injective_on_both_sides_of_the_crossing() {
    let tmp = tempfile::tempdir().unwrap();
    for format in ["csv", "tsv"] {
        for opt_out in [&[][..], &["--no-sanitize"][..]] {
            let render = |query: &str| {
                let mut args = vec!["query", "-n", "-f", format];
                args.extend_from_slice(opt_out);
                args.push(query);
                stdout(tmp.path(), &args)
            };
            let three = [
                render("_(x @ null)"),
                render("_(x @ \"NULL\")"),
                render("_(x @ \"\")"),
            ];
            assert_ne!(three[0], three[1], "{format} {opt_out:?}");
            assert_ne!(three[1], three[2], "{format} {opt_out:?}");
            assert_ne!(three[0], three[2], "{format} {opt_out:?}");
            assert_ne!(
                render("_(x @ \"a\\nb\")"),
                render("_(x @ :\"a\\nb\")"),
                "{format} {opt_out:?}: spelled and real newline collide"
            );
            // The safety spelling itself: a real ESC and the authored text
            // `\x1B` are two values, with and without the opt-out.
            assert_ne!(
                render("_(x @ \"\u{1b}[31mRED\")"),
                render("_(x @ \"\\x1B[31mRED\")"),
                "{format} {opt_out:?}: ESC and authored \\x1B collide"
            );
        }
    }
}
