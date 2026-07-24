// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
/// Counter-based name generator for SQL aliases and CTE names.
///
/// Produces deterministic, sequential names: `t_1`, `t_2`, `cte_1`, etc.
/// The same name appears as both the SQL AS clause and the column qualifier,
/// ensuring structural consistency — a name generated here is always used
/// in exactly two places (AS and qualifier), never one without the other.
///
/// All builders in the same query transformation share a counter via
/// `Arc<AtomicUsize>`, so names are unique across the entire query even
/// when multiple builders are constructed independently (e.g., join children).
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::pipeline::asts::core::TableName;
use delightql_types::SqlIdentifier;

/// A compiler-minted alias. The field is private and there is no public
/// constructor — only `NameGenerator::fresh` produces values, so a sink
/// that demands `FreshAlias` cannot be fed a hard-coded literal: the
/// literal is a compile error, and uniqueness (via the shared counter)
/// holds by construction.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::pipeline::transformer_v4) struct FreshAlias(SqlIdentifier);

impl FreshAlias {
    pub(in crate::pipeline::transformer_v4) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Display for FreshAlias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}

impl From<FreshAlias> for TableName {
    fn from(fresh: FreshAlias) -> Self {
        TableName::Named(fresh.0)
    }
}

/// A scope name with declared provenance, demanded by the `Builder`
/// entry constructors. The `Fresh` arm cannot be forged (see
/// `FreshAlias`); the `Resolved` arm is an explicit claim at the call
/// site that the name came from the AST — a user alias or a base-table
/// name the resolver decided — and is greppable for audit. A bare
/// string or `TableName` no longer typechecks at those sinks.
#[derive(Clone, Debug)]
pub(in crate::pipeline::transformer_v4) enum ScopeName {
    Resolved(TableName),
    Fresh(FreshAlias),
}

impl ScopeName {
    pub(in crate::pipeline::transformer_v4) fn into_table_name(self) -> TableName {
        match self {
            ScopeName::Resolved(name) => name,
            ScopeName::Fresh(fresh) => fresh.into(),
        }
    }
}

impl From<FreshAlias> for ScopeName {
    fn from(fresh: FreshAlias) -> Self {
        ScopeName::Fresh(fresh)
    }
}

#[derive(Clone)]
pub(in crate::pipeline) struct NameGenerator {
    counter: Arc<AtomicUsize>,
}

impl std::fmt::Debug for NameGenerator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NameGenerator")
            .field("counter", &self.counter)
            .finish()
    }
}

impl NameGenerator {
    /// Create a new generator with its own counter starting at 0.
    pub(in crate::pipeline) fn new() -> Self {
        Self {
            counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a generator that shares the same counter as another.
    /// Used when constructing child builders (e.g., join operands)
    /// that must not collide with names from sibling builders.
    pub(in crate::pipeline::transformer_v4) fn fork(&self) -> Self {
        Self {
            counter: Arc::clone(&self.counter),
        }
    }

    /// Mint the next unique alias. The only constructor of `FreshAlias`.
    pub(in crate::pipeline::transformer_v4) fn fresh(&self, prefix: &str) -> FreshAlias {
        let n = self.counter.fetch_add(1, Ordering::Relaxed) + 1;
        FreshAlias(SqlIdentifier::from(format!("{}_{}", prefix, n).as_str()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequential_names() {
        let gen = NameGenerator::new();
        assert_eq!(gen.fresh("t").as_str(), "t_1");
        assert_eq!(gen.fresh("t").as_str(), "t_2");
        assert_eq!(gen.fresh("cte").as_str(), "cte_3");
    }

    #[test]
    fn forked_generators_share_counter() {
        let gen_a = NameGenerator::new();
        assert_eq!(gen_a.fresh("t").as_str(), "t_1");

        let gen_b = gen_a.fork();
        assert_eq!(gen_b.fresh("t").as_str(), "t_2"); // continues from shared counter

        assert_eq!(gen_a.fresh("t").as_str(), "t_3"); // still shared
    }
}
