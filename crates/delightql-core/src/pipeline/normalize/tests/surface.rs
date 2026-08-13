// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The whole-admitted-surface measurement.
//!
//! A corpus walk over the road the compiler itself takes: every `.dql` the
//! repository holds, admitted through the entrance its category selects, and
//! normalized. A file the grammar refuses never reaches normalization, so only
//! ADMITTED files count here.
//!
//! The result is a classification, not a pass mark: every failure is bucketed
//! by its refusal identity, so a remaining gap is NAMED rather than hidden
//! behind an aggregate. The ratchet is on the named buckets: a new bucket, or
//! a bucket that grows, fails.

use crate::pipeline::normalize;
use crate::pipeline::syntax::{Parser, SyntaxTree};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/<pkg> sits two levels under the workspace root")
        .to_path_buf()
}

/// The entrance a file's ROLE selects. A test's own `query.dql` is a utility
/// sequence; a definition source beside it under `ddl/` or `definitions/` is a
/// canonical file. Nothing is guessed from the text — that is the whole point
/// of the two entrances.
///
/// The NAME decides before the directory does. A ball may be called `ddl`, and
/// its tests' own queries are still queries.
fn entrance(path: &Path) -> Entrance {
    if path.file_name().and_then(|n| n.to_str()) == Some("query.dql") {
        return Entrance::QuerySequence;
    }
    let text = path.to_string_lossy();
    if text.contains("/ddl/") || text.contains("/definitions/") {
        Entrance::DefinitionFile
    } else {
        Entrance::QuerySequence
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Entrance {
    DefinitionFile,
    QuerySequence,
}

fn dql_files(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.join("new_test_suite"), root.join("book")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if path.is_dir() {
                if !matches!(name, "target" | "node_modules" | "__pycache__") {
                    stack.push(path);
                }
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) == Some("dql") {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}

fn parse(parser: &mut Parser, entrance: Entrance, source: &str) -> SyntaxTree {
    match entrance {
        Entrance::DefinitionFile => parser.parse_definition_file(source),
        Entrance::QuerySequence => parser.parse_query_sequence(source),
    }
}

/// The bucket a refusal falls into. The identity is the error's own
/// categorization where it has one, so a bucket names a LAW rather than a
/// message that can be reworded.
fn bucket(error: &crate::error::DelightQLError) -> String {
    let uri = error.error_uri();
    // A gap's FAMILY is what a reviewer needs; the identity alone would pool
    // every unbuilt form into one number. `gap()` writes "<family>: <detail>",
    // so the family is what stands before the first colon of the message.
    if uri.ends_with("/normalize/gap") {
        let rendered = error.to_string();
        let message = rendered
            .split_once("Parse error: ")
            .map(|(_, tail)| tail)
            .unwrap_or(&rendered);
        if let Some((family, _)) = message.split_once(": ") {
            return format!("{uri} [{family}]");
        }
    }
    uri
}

struct Measurement {
    admitted: usize,
    normalized: usize,
    buckets: BTreeMap<String, usize>,
    examples: BTreeMap<String, Vec<String>>,
}

fn measure() -> Measurement {
    let root = workspace_root();
    let mut parser = Parser::new();
    let mut measurement = Measurement {
        admitted: 0,
        normalized: 0,
        buckets: BTreeMap::new(),
        examples: BTreeMap::new(),
    };
    for path in dql_files(&root) {
        let Ok(source) = std::fs::read_to_string(&path) else {
            continue;
        };
        let entrance = entrance(&path);
        let tree = parse(&mut parser, entrance, &source);
        if tree.has_defects() {
            // Admission is the grammar's verdict; this road measures what
            // survives it.
            continue;
        }
        measurement.admitted += 1;
        let registry = std::rc::Rc::new(crate::names::Registry::new(&[]));
        let outcome = match entrance {
            Entrance::DefinitionFile => normalize::definition_file(&tree, registry),
            Entrance::QuerySequence => normalize::query_sequence(&tree, registry),
        };
        match outcome {
            Ok(_) => measurement.normalized += 1,
            Err(error) => {
                let bucket = bucket(&error);
                *measurement.buckets.entry(bucket.clone()).or_default() += 1;
                let seen = measurement.examples.entry(bucket).or_default();
                if seen.len() < 3 {
                    seen.push(format!("{}: {error}", path.display()));
                }
            }
        }
    }
    measurement
}

/// Every remaining refusal is NAMED, and named as one of two things.
///
/// A LAW is a refusal this road makes on purpose: the corpus files that reach
/// it are the tests whose point IS the refusal. A GAP is a form the
/// consolidated grammar admits and this road does not yet build.
///
/// The counts are ceilings, not equalities — a repair that shrinks a bucket
/// must not have to edit this table to land. A bucket that is on NEITHER list
/// fails here rather than being absorbed into a percentage.
const REFUSED_BY_LAW: &[(&str, usize)] = &[
    // THE SET IS CLOSED: `(~~emit …~~)` is reserved room.
    ("delightql-error://semantic/annotation/reserved", 4),
    // `equals` is assertion SYNTAX: binary, and only inside an assertion.
    ("delightql-error://semantic/assertion/equals_arity", 5),
    ("delightql-error://semantic/assertion/equals_context", 3),
    // The `!` on a binding ASSERTS the body is effectful; it cannot make it so.
    ("delightql-error://semantic/effect/cte/pure_mark", 5),
    // A SCALAR PARAMETER IS CODE: a bound naming an unbound identifier has
    // nothing to be substituted with.
    ("delightql-error://semantic/limit/value", 3),
    // A duplicate formal makes its earlier namesake unreachable: every
    // binding to either lands on one frame slot. Judged by the identifier
    // law at the definition.
    ("delightql-error://semantic/cfe/parameter/duplicate", 3),
    // THE WRITTEN NAME IS THE NAMING: a slot binds by position and publishes
    // no name for `as` to change. The grammar admits the form only so this
    // refusal can name the alias and point at the projection that renames.
    ("delightql-error://semantic/constraint/positional_alias", 4),
    // A column reference carries at most one qualifier. The grammar admits
    // the three-part form only so this refusal can name the segment that was
    // never read.
    (
        "delightql-error://semantic/reference/multi_segment_qualifier",
        4,
    ),
    // IDENTITY IS THE CANONICAL SPELLING: the term specification's own
    // refusals, made by the one canonicalizer.
    ("delightql-error://semantic/mention/identifier_interior", 2),
    ("delightql-error://semantic/mention/term/not_a_term", 2),
    // ONE SUBSTITUTION LAW: two landings refuse.
    ("delightql-error://semantic/resolution/ho/pipe_landing", 2),
    // THE SLOT IS ONE. `@` names nothing, so a second bare hole has no
    // reading; the binder that names the value has no such limit, but a name
    // it never uses receives nothing and a name written beside `@` spells the
    // flow twice.
    ("delightql-error://semantic/landing/two_holes", 1),
    ("delightql-error://semantic/landing/discarded", 2),
    ("delightql-error://semantic/landing/binder_and_hole", 1),
    // An expansion's parens NAME the interior heading; they do not shape or
    // compute one. A narrowing publishes its fields and nothing else. A
    // sourceless inner form's interior supplies its own base.
    ("delightql-error://semantic/expansion/shaping_interior", 3),
    ("delightql-error://semantic/expansion/interior_slot", 3),
    ("delightql-error://semantic/narrowing/member", 3),
    ("delightql-error://semantic/compression/sourceless_base", 3),
    // THE WHOLE HEADING CORRELATES in ONE mode, written with `=`, naming the
    // arms it addresses.
    ("delightql-error://semantic/set/correlation/operator", 3),
    ("delightql-error://semantic/set/correlation/mixed_modes", 3),
    ("delightql-error://semantic/set/correlation/unnamed_arm", 3),
    // One goal declares one expected error.
    ("delightql-error://parse/error_hook/repeated", 3),
    // A danger gate is a NAMED behavior: an unrecognized name refuses rather
    // than becoming a silent no-op.
    ("delightql-error://parse/danger/unknown", 5),
    ("delightql-error://parse/config/unknown", 5),
    // NO PRECEDENCE: an infix composition standing as a function pipe's
    // source has two readings and the language picks neither.
    ("delightql-error://parse/pony", 4),
    // A template's escapes are a closed set; an unknown one is a typo, not a
    // literal backslash.
    ("delightql-error://parse/template/escape", 4),
    // A SPARSE COLUMN IS ADDRESSED BY NAME: a column filled twice in one row
    // has two values and no rule for choosing between them, and a fill naming
    // a column the header never marked addresses nothing.
    ("delightql-error://semantic/anon/sparse_duplicate", 2),
    ("delightql-error://semantic/anon/sparse_fill_position", 2),
    ("delightql-error://semantic/anon/sparse_arity", 3),
    ("delightql-error://semantic/anon/sparse_header", 2),
    // A TABLE HAS ONE HEADING: a row of another width has cells belonging to
    // no column, and every reader downstream would have to decide which.
    ("delightql-error://parse/anon", 3),
    // TWO OFFERS, ONE POSITION. A stacked fact's header and a datum's own
    // `as` label both offer the position a name; disagreeing, they have no
    // rule for choosing, and choosing anyway makes the public name depend on
    // which offer was read first.
    ("delightql-error://semantic/ddl/head/name_conflict", 3),
    // A parameterized fact's heading is its header; a datum label with no
    // header has no verbose-form equivalent, so it refuses toward the
    // header spelling rather than silently disappearing.
    (
        "delightql-error://semantic/ddl/head/parameterized_fact_offer",
        2,
    ),
    // A DECLARED NAME IS DECLARED ONCE, OVER THE WHOLE HEADING; an output
    // cell reads a declared input and nothing else; the declared widths are
    // what every arm writes.
    ("delightql-error://semantic/fact_function/duplicate_name", 4),
    (
        "delightql-error://semantic/fact_function/output_reads_no_input",
        4,
    ),
    ("delightql-error://semantic/fact_function/width", 5),
    // A witness anonymous table (`+_` / `\+_`) is a membership test and
    // exports no columns, so an alias on one names nothing.
    (
        "delightql-error://semantic/resolution/anon/membership_alias",
        3,
    ),
    // THE MARKER LEADS: the context capture is the signature's first
    // declaration at both faces, so a `..` written after a parameter would
    // silently reorder every call.
    ("delightql-error://semantic/ddl/head/context_position", 2),
    // THE FIRST PARENTHESES ARE ARGUMENTS, NEVER A TABLE. A parameter
    // declared as a value takes a term; a relation standing there — including
    // the anonymous table a `;`-row spelling dissolves into — is a table
    // written where an argument belongs.
    ("delightql-error://semantic/effect/arguments/not_a_table", 3),
    // STRICT LANDING: a directive declaring no parameters has one slot and
    // the pipe fills it, so an argument written there leaves the piped
    // relation nowhere to land.
    ("delightql-error://semantic/effect/landing/nowhere", 2),
];

/// The DEFERRALS: lawful forms the surviving AST has no carrier for. The
/// vocabulary is `Deferred`, an enum, so this is not a prose list that can go
/// stale — `every_deferred_gap_is_reachable_and_named` enumerates it, and
/// nothing can exit through this door without appearing there first.
///
/// The corpus reaches NONE of them, and the ceiling is zero. That is the
/// ratchet: a regression shows up here as itself, with its family named,
/// rather than as a percentage that moved.
const NAMED_GAPS: &[(&str, usize)] = &[("delightql-error://parse/normalize/gap", 0)];

#[test]
fn the_admitted_surface_normalizes_or_names_its_gap() {
    let measurement = measure();
    assert!(
        measurement.admitted > 100,
        "the corpus walk found only {} admitted files; the walk itself is broken",
        measurement.admitted
    );

    let ledger = || REFUSED_BY_LAW.iter().chain(NAMED_GAPS.iter());
    let unnamed: Vec<&String> = measurement
        .buckets
        .keys()
        .filter(|bucket| {
            !ledger().any(|(named, _)| {
                bucket.as_str() == *named || bucket.starts_with(&format!("{named} ["))
            })
        })
        .collect();
    assert!(
        unnamed.is_empty(),
        "these refusal identities are on neither ledger:\n  {}",
        unnamed
            .iter()
            .map(|bucket| format!(
                "{bucket} ({}) e.g. {}",
                measurement.buckets[*bucket],
                measurement.examples[*bucket].join("\n       ")
            ))
            .collect::<Vec<_>>()
            .join("\n  ")
    );

    for (named, ceiling) in ledger() {
        let count: usize = measurement
            .buckets
            .iter()
            .filter(|(bucket, _)| {
                bucket.as_str() == *named || bucket.starts_with(&format!("{named} ["))
            })
            .map(|(_, count)| *count)
            .sum();
        assert!(
            count <= *ceiling,
            "'{named}' grew to {count}, above its ceiling of {ceiling}"
        );
    }

    // The measurement itself is the artifact: printed so a reviewer reads the
    // shape of the remainder rather than a single number.
    println!(
        "admitted {} | normalized {} | remaining {}",
        measurement.admitted,
        measurement.normalized,
        measurement.admitted - measurement.normalized
    );
    for (bucket, count) in &measurement.buckets {
        println!("  {count:5}  {bucket}");
        for example in &measurement.examples[bucket] {
            println!("         {example}");
        }
    }
}
