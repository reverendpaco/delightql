// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The direct grammar census: every authored DelightQL source in the
//! repository, parsed by the consolidated grammar alone.
//!
//! Nothing here routes through normalization, resolution, or execution. A file
//! is ADMITTED when the root its category selects parses it with no `ERROR` and
//! no `MISSING` node, and defective otherwise. That is the only question this
//! measurement answers, which is what makes its answer usable: a source the
//! compiler rejects for a resolver reason is admitted here, and the difference
//! between the two counts is the worklist.
//!
//! THE HOST NAMES THE CATEGORY. The grammar's branches overlap — `f(1, 2)` is a
//! fact in a definition file and an argumentative query in a sequence, the same
//! bytes — so the entrance comes from what the repository already knows about
//! where a file sits, never from reading it. [`classify`] is that knowledge,
//! written once.
//!
//! EVERY FILE IS ACCOUNTED FOR. A source matching no class fails
//! [`every_source_is_classified`] rather than vanishing from the denominator;
//! a class the compiler never reads is named as such, with the reason.
//!
//! ```text
//! cargo test -p delightql-cst --test census -- --ignored --nocapture
//! ```

use delightql_cst::{Parser, SyntaxTree};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// The root a source is read through, or the reason nothing reads it.
///
/// The companion-cell root is deliberately absent: a companion cell is a CELL
/// in a constraint or default column, never a file, so no path selects it.
/// `tests/entrances.rs` exercises that root where it actually lives.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Class {
    /// The canonical entrance: definitions and explicit `?-` goals.
    Definitions,
    /// The utility entrance: a sequence of bare queries run in order.
    Queries,
    /// Not a compiler input. The reason is the class name.
    Unread(&'static str),
}

impl Class {
    fn label(self) -> &'static str {
        match self {
            Class::Definitions => "definition-file",
            Class::Queries => "query-sequence",
            Class::Unread(why) => why,
        }
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/<pkg> sits two levels under the workspace root")
        .to_path_buf()
}

/// What the repository knows about a source before anything reads its bytes.
///
/// A suite test's own `query.dql` is the sequence its runner executes; the
/// `ddl/` beside it holds the definition sources that same test consults. The
/// embedded `autoload/` modules are consulted at session start and the embedded
/// `seed/` programs are run as a sequence — the same two categories, named by
/// the loader rather than by a test directory.
fn classify(relative: &Path) -> Option<Class> {
    let path = relative.to_str()?;
    let name = relative.file_name()?.to_str()?;

    if let Some(rest) = path.strip_prefix("new_test_suite/balls/") {
        // Depth from `balls/`: 0 is the ball, 1 is the test directory, and
        // anything below that is the test's own material. A `ddl/` INSIDE a
        // test holds definition sources; a BALL named `ddl` is a ball like any
        // other, and matching the name alone would misfile 391 tests.
        let parts: Vec<&str> = rest.split('/').collect();
        return match parts.as_slice() {
            [_ball, _test, "query.dql"] => Some(Class::Queries),
            // A pinned baseline's provenance: the pure query whose rows the
            // recorded hash came from, run through the same utility entrance.
            [_ball, _test, "baseline.dql"] => Some(Class::Queries),
            [_ball, _test, "ddl", ..] => Some(Class::Definitions),
            [_ball, _test, "oracle.dql"] => Some(Class::Queries),
            _ => None,
        };
    }

    if path.starts_with("crates/delightql-core/autoload/") {
        return Some(Class::Definitions);
    }
    if path.starts_with("crates/delightql-core/seed/") {
        return Some(Class::Queries);
    }

    // Working papers and evidence, read by people and not by the compiler.
    if path.starts_with("just_the_facts/") {
        return Some(Class::Unread("evidence corpus"));
    }
    if path.starts_with("bugs/") {
        return Some(Class::Unread("bug reproduction"));
    }
    if path.starts_with("devtools/") {
        return Some(Class::Unread("editor-plugin fixture"));
    }
    // Consulted by a core test, so it is a definition source like any other —
    // a file the compiler reads is never a working paper.
    if name == "TORTURE-TEST.dql" {
        return Some(Class::Definitions);
    }
    None
}

/// Every authored DelightQL source in the repository.
///
/// `.ddl` counts: a test's `ddl/` directory admits both extensions and the
/// packer consults them identically, so a census keyed on `.dql` alone would
/// miss 118 definition sources.
///
/// A DENOMINATOR IS ONLY AS GOOD AS ITS ENUMERATION. Every ratio below and
/// every worklist read off them is a universal claim, so a directory this
/// cannot open and an entry it cannot read PANIC with the path. A walk that
/// skipped them would shrink the denominator and report a better number for
/// it; "did not find" is not "is not there", and an unreadable path is not
/// another [`Class::Unread`] — that carrier is for sources the compiler
/// deliberately never reads.
fn sources(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    // Directories are visited by their CANONICAL path. The suite links one
    // test's `databases/` into another's, so the same subtree is reachable by
    // several paths; visiting it once is what keeps a source from being
    // counted twice, and canonicalizing is what makes a symlink loop
    // terminate instead of hanging.
    let mut seen = std::collections::HashSet::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let canonical = std::fs::canonicalize(&dir)
            .unwrap_or_else(|error| panic!("resolving {}: {error}", dir.display()));
        if !seen.insert(canonical) {
            continue;
        }
        let entries = std::fs::read_dir(&dir)
            .unwrap_or_else(|error| panic!("reading {}: {error}", dir.display()));
        for entry in entries {
            let entry =
                entry.unwrap_or_else(|error| panic!("an entry of {}: {error}", dir.display()));
            let path = entry.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            // `is_dir` FOLLOWS: a linked directory is a directory, and a
            // subtree reachable only through a link is still part of the
            // denominator.
            if path.is_dir() {
                // Derived and vendored trees hold no authored DelightQL.
                if matches!(
                    name,
                    "target" | ".jj" | ".git" | "node_modules" | "ball_artifacts"
                ) {
                    continue;
                }
                stack.push(path);
                continue;
            }
            if matches!(
                path.extension().and_then(|e| e.to_str()),
                Some("dql") | Some("ddl")
            ) {
                found.push(path);
            }
        }
    }
    found.sort();
    found.dedup();
    found
}

/// The bytes of a source, or a failure naming it.
fn read(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("reading the source {}: {error}", path.display()))
}

/// The class selects the entrance; the ENTRANCE reads the bytes.
///
/// A query source may carry the ruled `#!dql query-sequence` header or omit
/// it — the utility entrance frames whichever it is handed, so the census
/// measures the same admission for both and never classifies by content.
fn parse(class: Class, source: &str) -> Option<SyntaxTree> {
    let mut parser = Parser::new();
    match class {
        Class::Definitions => Some(parser.parse_definition_file(source)),
        Class::Queries => Some(parser.parse_query_sequence(source)),
        Class::Unread(_) => None,
    }
}

/// The first defective span in authored order, with the productions it stands
/// inside.
///
/// A cascaded `ERROR` is not an independent defect: recovery reports the same
/// failure again at every enclosing level, so only the FIRST span by position
/// is counted and the rest of the tree is not mined for more. The ancestor
/// chain is what classifies the missing production — a defect's own bytes say
/// where the parse stopped, and the chain says what it was in the middle of.
struct FirstDefect {
    /// The authored bytes at and after the failure, trimmed for display.
    near: String,
    /// Innermost-first named ancestors of the failing span.
    within: Vec<String>,
}

fn first_defect(tree: &SyntaxTree) -> Option<FirstDefect> {
    let mut defects = tree.defects();
    defects.sort_by_key(|d| (d.byte_range.start, d.byte_range.end));
    let defect = defects.into_iter().next()?;
    let text = tree.source();
    let start = floor(text, defect.byte_range.start.min(text.len()));
    let end = floor(text, (start + 44).min(text.len()));
    let near: String = text[start..end]
        .chars()
        .map(|c| if c == '\n' { ' ' } else { c })
        .collect();

    // Back in PARSED coordinates: the raw tree is what carries the ancestry.
    let raw_start = defect.byte_range.start + tree.parsed_source().len() - text.len();
    let mut within = Vec::new();
    let mut node = tree
        .raw()
        .root_node()
        .descendant_for_byte_range(raw_start, raw_start);
    while let Some(current) = node {
        if current.is_named() && !current.is_error() {
            within.push(current.kind().to_string());
        }
        node = current.parent();
    }
    Some(FirstDefect { near, within })
}

fn floor(s: &str, index: usize) -> usize {
    let mut i = index.min(s.len());
    while !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

/// One line per source, so a run is diffable against the previous one.
struct Row {
    path: String,
    class: Class,
    defect: Option<FirstDefect>,
    /// The test this source belongs to pins an `error` baseline: a refusal is
    /// the outcome it was written for. Host-known metadata again — the census
    /// never reads a source to decide whether it is meant to fail.
    refusal_expected: bool,
}

/// Whether the test directory owning `path` pins an `error` baseline.
///
/// A pinned error can be a resolver's rather than a parser's, so this does not
/// prove the refusal belongs here — it separates "the corpus expects this to
/// fail somewhere" from "the corpus expects this to run", which is the
/// distinction a grammar gap hides behind.
fn pins_an_error(path: &Path) -> bool {
    let Some(test_dir) = path.parent() else {
        return false;
    };
    // A definition source sits one level deeper, under the test's `ddl/`.
    let candidates = [test_dir.to_path_buf(), test_dir.join("..")];
    candidates.iter().any(|dir| {
        let baselines = dir.join("baselines");
        // An ABSENT directory is the answer "this test pins nothing", and most
        // do not. Every OTHER failure is the census failing to look, which is a
        // different thing and must not be reported as the same one.
        let entries = match std::fs::read_dir(&baselines) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return false,
            Err(error) => panic!("reading {}: {error}", baselines.display()),
        };
        entries
            .map(|entry| {
                entry.unwrap_or_else(|error| panic!("an entry of {}: {error}", baselines.display()))
            })
            .any(|e| e.file_name().to_string_lossy().ends_with("--error"))
    })
}

/// One row per enumerated source — EVERY one. A source with no class fails
/// [`every_source_is_classified`]; the census itself would rather report a
/// missing class than quietly leave the file out of its own denominator.
fn census() -> Vec<Row> {
    let root = workspace_root();
    let all = sources(&root);
    let mut rows = Vec::with_capacity(all.len());
    for path in all {
        let relative = path.strip_prefix(&root).unwrap_or(&path);
        let Some(class) = classify(relative) else {
            continue;
        };
        let source = read(&path);
        let defect = parse(class, &source).and_then(|tree| first_defect(&tree));
        rows.push(Row {
            path: relative.display().to_string(),
            class,
            refusal_expected: defect.is_some() && pins_an_error(&path),
            defect,
        });
    }
    rows
}

/// A source the census cannot name is a hole in the denominator.
///
/// Adding a corpus directory without saying which entrance reads it would
/// quietly shrink every admission count that follows, so the unclassified set
/// is empty by test rather than by habit.
#[test]
fn every_source_is_classified() {
    let root = workspace_root();
    let unclassified: Vec<String> = sources(&root)
        .into_iter()
        .filter_map(|path| {
            let relative = path.strip_prefix(&root).unwrap_or(&path).to_path_buf();
            classify(&relative)
                .is_none()
                .then(|| relative.display().to_string())
        })
        .collect();
    assert!(
        unclassified.is_empty(),
        "{} sources match no census class; name the entrance that reads them \
         (or the reason nothing does):\n{}",
        unclassified.len(),
        unclassified.join("\n")
    );
}

/// The census itself: admission by class, then the defect classes ranked.
///
/// `DQL_CENSUS_ONLY` restricts the ranked listing to one entrance's sources —
/// the two roads move at different times, and reading one worklist should not
/// mean scrolling past the other's.
#[test]
#[ignore = "a repository-wide measurement; run deliberately"]
fn report() {
    let rows = census();
    let mut by_class: BTreeMap<&'static str, (usize, usize, usize)> = BTreeMap::new();
    for row in &rows {
        let entry = by_class.entry(row.class.label()).or_default();
        entry.0 += 1;
        if matches!(row.class, Class::Unread(_)) {
            continue;
        }
        if row.defect.is_none() {
            entry.1 += 1;
        } else if row.refusal_expected {
            entry.2 += 1;
        }
    }

    println!("\n== admission by class ==");
    println!("{:>6} {:>7}  {:>8}  {}", "admit", "of", "refusals", "class");
    for (label, (total, admitted, refusals)) in &by_class {
        println!("{admitted:6} / {total:<6}  {refusals:8}  {label}");
    }

    let read: Vec<&Row> = rows
        .iter()
        .filter(|r| !matches!(r.class, Class::Unread(_)))
        .collect();
    let admitted = read.iter().filter(|r| r.defect.is_none()).count();
    let refusals = read.iter().filter(|r| r.refusal_expected).count();
    println!(
        "\n{admitted} of {} sources admitted; {refusals} of the rest pin an error baseline",
        read.len()
    );

    let only = std::env::var("DQL_CENSUS_ONLY").ok();
    let selected: Vec<&&Row> = read
        .iter()
        .filter(|r| only.as_deref().is_none_or(|want| r.class.label() == want))
        .filter(|r| !r.refusal_expected || std::env::var("DQL_CENSUS_ALL").is_ok())
        .collect();

    // Grouped by the innermost production the failure stood in, then by the
    // bytes it stopped on. The production is what says which rule is missing;
    // the bytes say which spelling reached it.
    let mut classes: BTreeMap<(String, String), Vec<&str>> = BTreeMap::new();
    for row in &selected {
        let Some(defect) = &row.defect else { continue };
        let within = defect
            .within
            .first()
            .cloned()
            .unwrap_or_else(|| "<root>".to_string());
        classes
            .entry((within, defect.near.clone()))
            .or_default()
            .push(&row.path);
    }
    let mut ranked: Vec<_> = classes.into_iter().collect();
    ranked.sort_by_key(|(_, paths)| std::cmp::Reverse(paths.len()));

    println!("\n== first-defect classes ==");
    for ((within, near), paths) in &ranked {
        println!("{:5}  in {within:<28} near {near:?}", paths.len());
        for path in paths {
            println!("         {path}");
        }
    }
}
