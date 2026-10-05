// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A tree pattern's published heading and the values under it, observed
//! together.
//!
//! A ball's result hash reads cell bytes only, so it cannot see which NAME a
//! value was published under unless the query projects by name. Each case
//! here reads the destructure's own published heading (`|> -(j)` drops only
//! the document) and compares the whole CSV — header row and data rows — with
//! an answer written from the document by hand. A value published under a
//! sibling's name, or a heading published out of pattern order, differs in
//! the text.

use std::io::Read;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn run(dir: &Path, query: &str) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_dql"));
    command
        .args(["query", query, "-f", "csv"])
        .current_dir(dir)
        .env("DQL_STATE_DIR", dir.join("state"))
        .env_remove("DQL_DIALECT")
        .env_remove("DQL_TEST_PANIC")
        .env_remove("RUST_BACKTRACE")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn dql");
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let out_reader = thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).unwrap();
        bytes
    });
    let err_reader = thread::spawn(move || {
        let mut bytes = Vec::new();
        stderr.read_to_end(&mut bytes).unwrap();
        bytes
    });
    let start = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll dql") {
            break status;
        }
        if start.elapsed() > Duration::from_secs(10) {
            let _ = child.kill();
            let _ = child.wait();
            let stdout = out_reader.join().unwrap();
            let stderr = err_reader.join().unwrap();
            panic!(
                "dql exceeded ten seconds: {}{}",
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr)
            );
        }
        thread::sleep(Duration::from_millis(10));
    };
    Output {
        status,
        stdout: out_reader.join().unwrap(),
        stderr: err_reader.join().unwrap(),
    }
}

fn assert_csv(query: &str, expected: &str) {
    let tmp = tempfile::tempdir().unwrap();
    let out = run(tmp.path(), query);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{query}\n{stderr}");
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        expected,
        "{query}\n{stderr}"
    );
}

#[test]
fn a_navigation_before_a_flat_sibling_publishes_each_value_under_its_own_name() {
    assert_csv(
        r#"_(j @ {"nm":"APP","dep":{"react":"R"}}), j ~= {"dep": {react}, nm} |> -(j)"#,
        "react,nm\nR,APP\n",
    );
}

#[test]
fn the_flat_first_permutation_publishes_the_same_pairs_in_its_own_order() {
    assert_csv(
        r#"_(j @ {"nm":"APP","dep":{"react":"R"}}), j ~= {nm, "dep": {react}} |> -(j)"#,
        "nm,react\nAPP,R\n",
    );
}

#[test]
fn an_iteration_before_a_flat_sibling() {
    assert_csv(
        r#"_(j @ {"nm":"APP","arr":[{"z":"Z"}]}), j ~= {"arr": ~> {z}, nm} |> -(j)"#,
        "z,nm\nZ,APP\n",
    );
}

#[test]
fn a_navigation_before_an_iteration_with_no_flat_sibling() {
    assert_csv(
        r#"_(j @ {"dep":{"react":"R"},"arr":[{"z":"Z"}]}), j ~= {"dep": {react}, "arr": ~> {z}} |> -(j)"#,
        "react,z\nR,Z\n",
    );
}

#[test]
fn a_renamed_nested_binder_and_a_path_sibling() {
    assert_csv(
        r#"_(j @ {"nm":"APP","dep":{"react":"R","next":"N"}}), j ~= {"dep": {"react": r}, .dep.next, nm} |> -(j)"#,
        "r,dep_next,nm\nR,N,APP\n",
    );
}

#[test]
fn indexed_members_nested_before_a_flat_sibling() {
    assert_csv(
        r#"_(j @ {"nm":"APP","pt":[3,"four"]}), j ~= {"pt": [.0 as x, .1 as y], nm} |> -(j)"#,
        "x,y,nm\n3,four,APP\n",
    );
}

#[test]
fn a_metadata_key_nested_before_a_flat_sibling() {
    assert_csv(
        r#"_(j @ {"nm":"APP","m":{"fr":[{"n":"A"}]}}), j ~= {"m": ~> c:~> {n}, nm} |> -(j)"#,
        "c,n,nm\nfr,A,APP\n",
    );
}

#[test]
fn three_levels_of_navigation_before_their_flat_siblings() {
    assert_csv(
        r#"_(j @ {"a":1,"b":{"c":"two","d":{"e":3.5}},"f":"eff"}), j ~= {"b": {"d": {e}, c}, a, f} |> -(j)"#,
        "e,c,a,f\n3.5,two,1,eff\n",
    );
}

#[test]
fn an_encoded_document_publishes_the_pairs_a_native_one_does() {
    assert_csv(
        r#"_(j @ """{"nm":"APP","dep":{"react":"R"}}"""), j ~= {"dep": {react}, nm} |> -(j)"#,
        "react,nm\nR,APP\n",
    );
}

#[test]
fn a_narrowing_publishes_its_members_in_written_order() {
    assert_csv(
        r#"_(j @ [{"z":"Z","a":{"b":"B"}}]) |> .j{.a.b, z}"#,
        "a_b,z\nB,Z\n",
    );
}
