// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Compile-time-cheap, run-time-gated diagnostic writes.
//!
//! A defect in the pipeline is almost never visible in the SQL text: the text
//! spells `w_5.success` whether the occurrence behind it is the one the body
//! outputs or a sibling of it, and only the registry ids tell them apart. The
//! probes below print those ids at the points where a wrong one gets chosen —
//! which is why they live in the tree instead of being pasted in and deleted
//! each time. Adding one is cheaper than rebuilding to add one.
//!
//! Internal to this crate. Nothing here is part of the public surface, and the
//! macros are re-exported at `pub(crate)` rather than macro-exported so they
//! cannot become one by accident.
//!
//! `DQL_PROBE` is a comma-separated topic list.
//!
//! ```text
//! DQL_PROBE=selfcheck,rebind  dql query --to results < q.dql
//! ```
//!
//! **`all` turns on every topic that only reports.** It never turns on a topic
//! that changes what the compiler does — a switch meaning "show me everything"
//! must not also mean "and stop checking", or what gets diagnosed is a
//! different compiler than the one that ships. Those topics are listed in
//! `BEHAVIOURAL` and have to be named one at a time:
//!
//! - `noopt` skips the SQL optimizer, to separate "the transformer emitted
//!   this" from "a later pass rewrote it into this".
//! - `nocheck` downgrades a SQL self-check refusal to a warning so the SQL
//!   behind it can reach the engine and be read. It is compiled out entirely
//!   without debug assertions: a release build has no road to it at all.

use std::collections::BTreeSet;
use std::sync::OnceLock;

/// Topics that change what the compiler does rather than only reporting it.
///
/// Kept as data, not as scattered checks, so that `all` and the audit of what
/// `all` covers read off the same list.
const BEHAVIOURAL: [&str; 2] = ["noopt", "nocheck"];

/// Whether `topic` is named in `DQL_PROBE`.
///
/// The environment is read once. A probe in a hot loop still costs a set
/// lookup per call, so keep the arguments cheap to build or put them behind
/// the macro, which does not evaluate them when the topic is off.
pub(crate) fn enabled(topic: &str) -> bool {
    // Letting a refused statement through is a debugging act, and a build with
    // assertions disabled is not being debugged. Gated here rather than at the
    // call site so there is one place that decides it.
    if topic == "nocheck" && !cfg!(debug_assertions) {
        return false;
    }
    static TOPICS: OnceLock<BTreeSet<String>> = OnceLock::new();
    let topics = TOPICS.get_or_init(|| {
        std::env::var("DQL_PROBE")
            .unwrap_or_default()
            .split(',')
            .map(|part| part.trim().to_ascii_lowercase())
            .filter(|part| !part.is_empty())
            .collect()
    });
    resolve(topics, topic)
}

/// The topic rule, separated from where the topic list comes from so it can be
/// tested: the environment is read once per process and a test cannot vary it.
fn resolve(topics: &BTreeSet<String>, topic: &str) -> bool {
    topics.contains(topic) || (!BEHAVIOURAL.contains(&topic) && topics.contains("all"))
}

/// Write a line to stderr when its topic is on.
///
/// The topic is a bare word, not a string: `probe!(rebind, "…{x:?}")`. Every
/// argument is evaluated only when the topic is on.
macro_rules! probe {
    ($topic:ident, $($arg:tt)*) => {
        if $crate::probe::enabled(stringify!($topic)) {
            eprintln!("[{}] {}", stringify!($topic), format_args!($($arg)*));
        }
    };
}

/// Run `body` when its topic is on. For probes that need statements rather
/// than one line — a loop over candidates, a walk of a chain.
macro_rules! probing {
    ($topic:ident, $body:block) => {
        if $crate::probe::enabled(stringify!($topic)) {
            $body
        }
    };
}

pub(crate) use probe;
pub(crate) use probing;

/// The republication chain of an occurrence, oldest last.
///
/// Two occurrences that look alike in SQL are told apart here: one is on the
/// other's chain (a boundary made it), or they merely meet at a shared
/// ancestor (siblings, and no boundary relates them).
pub(crate) fn chain(
    identities: &crate::names::Registry,
    column: crate::names::ColId,
) -> Vec<String> {
    vec![format!("{column:?}@{:?}", identities.scope_of(column))]
}

/// The input chain of a scope, oldest last, with each hop's origin and the
/// name it answers to.
///
/// The scope-side counterpart of `chain`: whether two scopes spelling one name
/// are one occurrence or two is decided here. The name prints as its `Sym`,
/// not as characters — two scopes answering to one name carry one `Sym`, and
/// that is the comparison a reader has to make.
pub(crate) fn scope_chain(
    identities: &crate::names::Registry,
    scope: crate::names::ScopeId,
) -> Vec<String> {
    let answers = identities.answers_to(scope);
    vec![format!(
        "{scope:?}(answers {answers:?}) {:?}",
        identities.kind_of(scope)
    )]
}

#[cfg(test)]
mod tests {
    use super::{resolve, BEHAVIOURAL};
    use std::collections::BTreeSet;

    fn topics(list: &[&str]) -> BTreeSet<String> {
        list.iter().map(|part| part.to_string()).collect()
    }

    #[test]
    fn all_reaches_a_reporting_topic() {
        assert!(resolve(&topics(&["all"]), "selfcheck"));
    }

    #[test]
    fn all_reaches_no_behavioural_topic() {
        let all = topics(&["all"]);
        for topic in BEHAVIOURAL {
            assert!(
                !resolve(&all, topic),
                "`all` must not turn on {topic}: it changes what the compiler does"
            );
        }
    }

    #[test]
    fn a_behavioural_topic_is_reachable_by_name() {
        for topic in BEHAVIOURAL {
            assert!(resolve(&topics(&[topic]), topic));
        }
    }

    #[test]
    fn an_unnamed_topic_is_off() {
        assert!(!resolve(&topics(&["rebind"]), "selfcheck"));
    }
}
