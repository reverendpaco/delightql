// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Namespace descendancy: whether one catalog namespace lies under another.
//!
//! The tree is the `::` segment structure of `namespace.fq_name`. A
//! namespace is within an ancestor when the ancestor's segments are a
//! prefix of its own, compared byte for byte: case is significant, and `_`
//! and `%` are ordinary characters. The root row, spelled `_`, has no
//! segments, so every namespace is within it.
//!
//! Every road that asks a hierarchy question asks here: a remover's removal
//! set and the borrows that block it, an imprint's archived subtree,
//! blueprint inertness, an exposure's child, the system's reserved
//! subtrees, and a connection's common root.
//!
//! Two tempting substitutes answer a different question. SQL `LIKE` folds
//! ASCII case and reads `_` and `%` as wildcards, so `lib::a_b::%` matches
//! `lib::acb::x` and `lib::A::%` matches `lib::a::x`. `pid` is not the tree:
//! consulted namespaces leave it `NULL`, so a `pid` walk strands them.
//!
//! A catalog row's kind is decoded here too, by [`NamespaceKind`]; what a
//! kind admits is the lifecycle authority's judgment (`ddl::lifecycle`).

mod kind;
mod subtree;
pub(crate) use kind::NamespaceKind;
pub(crate) use subtree::{NamespaceNode, Subtree};

#[cfg(all(test, not(target_arch = "wasm32")))]
mod removal_tests;

/// The catalog spelling of the root namespace. It is never spelled in a
/// path; top-level namespaces are its children.
pub(crate) const ROOT: &str = "_";

fn segments(fq: &str) -> Vec<&str> {
    if fq == ROOT {
        Vec::new()
    } else {
        fq.split("::").collect()
    }
}

fn depth(fq: &str) -> usize {
    segments(fq).len()
}

fn join(segments: Vec<&str>) -> String {
    if segments.is_empty() {
        ROOT.to_string()
    } else {
        segments.join("::")
    }
}

/// `fq` is `ancestor` or lies under it.
pub(crate) fn is_within(fq: &str, ancestor: &str) -> bool {
    let path = segments(fq);
    let prefix = segments(ancestor);
    path.len() >= prefix.len() && path[..prefix.len()] == prefix[..]
}

/// `fq` lies under `ancestor` and is not `ancestor` itself.
pub(crate) fn is_under(fq: &str, ancestor: &str) -> bool {
    depth(fq) > depth(ancestor) && is_within(fq, ancestor)
}

/// `fq` moved from `from` to `to`: its segments below `from` placed under
/// `to`. `None` when `fq` is not within `from`.
pub(crate) fn rerooted(fq: &str, from: &str, to: &str) -> Option<String> {
    if !is_within(fq, from) {
        return None;
    }
    let path = segments(fq);
    let below = &path[depth(from)..];
    Some(join(
        segments(to)
            .into_iter()
            .chain(below.iter().copied())
            .collect(),
    ))
}

/// The deepest namespace every one of `fqs` is within. `None` when there
/// are none, or when the only namespace they share is the root.
pub(crate) fn deepest_common_ancestor<'a>(
    fqs: impl IntoIterator<Item = &'a str>,
) -> Option<String> {
    let mut fqs = fqs.into_iter();
    let mut shared = segments(fqs.next()?);
    for fq in fqs {
        let path = segments(fq);
        let common = shared.iter().zip(&path).take_while(|(a, b)| a == b).count();
        shared.truncate(common);
    }
    (!shared.is_empty()).then(|| shared.join("::"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_underscore_is_a_literal_character() {
        assert!(is_under("lib::a_b::x", "lib::a_b"));
        assert!(!is_within("lib::acb::x", "lib::a_b"));
        assert!(!is_within("lib::acb", "lib::a_b"));
        assert!(!is_within("lib::a_b::x", "lib::acb"));
    }

    #[test]
    fn a_percent_sign_is_a_literal_character() {
        assert!(is_under("lib::a%::x", "lib::a%"));
        assert!(!is_within("lib::ab::x", "lib::a%"));
        assert!(!is_within("lib::a::x", "lib::a%"));
    }

    #[test]
    fn case_is_significant() {
        assert!(!is_within("lib::a::x", "lib::A"));
        assert!(!is_within("lib::A::x", "lib::a"));
        assert!(!is_within("LIB::a", "lib"));
        assert!(is_under("lib::A::x", "lib::A"));
    }

    #[test]
    fn descent_is_bounded_by_whole_segments() {
        assert!(!is_within("lib::ab", "lib::a"));
        assert!(!is_within("lib::a", "lib::ab"));
        assert!(!is_within("libx::a", "lib"));
        assert!(is_under("lib::a::b::c::d::e", "lib::a"));
    }

    #[test]
    fn within_includes_itself_and_under_does_not() {
        assert!(is_within("lib::a", "lib::a"));
        assert!(!is_under("lib::a", "lib::a"));
        assert!(!is_within("lib", "lib::a"));
        assert!(!is_under("lib", "lib::a"));
    }

    #[test]
    fn every_namespace_is_within_the_root() {
        assert!(is_under("sys", ROOT));
        assert!(is_under("lib::a_b::x", ROOT));
        assert!(is_within(ROOT, ROOT));
        assert!(!is_under(ROOT, ROOT));
        assert!(!is_within(ROOT, "sys"));
    }

    #[test]
    fn rerooting_moves_the_segments_below_the_old_root() {
        assert_eq!(
            rerooted("lib::a_b::_internal", "lib::a_b", "data::_0_blueprint").as_deref(),
            Some("data::_0_blueprint::_internal")
        );
        assert_eq!(
            rerooted("lib::a_b", "lib::a_b", "data::_0_blueprint").as_deref(),
            Some("data::_0_blueprint")
        );
        assert_eq!(
            rerooted("lib::acb::x", "lib::a_b", "data::_0_blueprint"),
            None
        );
        assert_eq!(rerooted("lib::a::x", "lib::A", "data"), None);
    }

    #[test]
    fn the_common_ancestor_is_the_longest_shared_segment_prefix() {
        assert_eq!(
            deepest_common_ancestor(["lib::a_b::x", "lib::a_b::y"]).as_deref(),
            Some("lib::a_b")
        );
        assert_eq!(
            deepest_common_ancestor(["lib::a_b", "lib::acb"]).as_deref(),
            Some("lib")
        );
        assert_eq!(
            deepest_common_ancestor(["lib::ab::x", "lib::a::x"]).as_deref(),
            Some("lib")
        );
        assert_eq!(
            deepest_common_ancestor(["lib::A", "lib::a"]).as_deref(),
            Some("lib")
        );
        assert_eq!(
            deepest_common_ancestor(["tree::a"]).as_deref(),
            Some("tree::a")
        );
        assert_eq!(deepest_common_ancestor(["a", "b"]), None);
        assert_eq!(deepest_common_ancestor([]), None);
    }
}
