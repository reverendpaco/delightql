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

    /// Generate the next unique name as a `TableName`.
    pub(in crate::pipeline::transformer_v4) fn next_table_name(&self, prefix: &str) -> TableName {
        let n = self.counter.fetch_add(1, Ordering::Relaxed) + 1;
        TableName::Named(SqlIdentifier::from(format!("{}_{}", prefix, n).as_str()))
    }

    /// Generate the next unique name as a raw string.
    /// Used for CTE names and SQL aliases.
    pub(in crate::pipeline::transformer_v4) fn next_name(&self, prefix: &str) -> String {
        let n = self.counter.fetch_add(1, Ordering::Relaxed) + 1;
        format!("{}_{}", prefix, n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequential_names() {
        let gen = NameGenerator::new();
        assert_eq!(gen.next_name("t"), "t_1");
        assert_eq!(gen.next_name("t"), "t_2");
        assert_eq!(gen.next_name("cte"), "cte_3");
    }

    #[test]
    fn forked_generators_share_counter() {
        let gen_a = NameGenerator::new();
        assert_eq!(gen_a.next_name("t"), "t_1");

        let gen_b = gen_a.fork();
        assert_eq!(gen_b.next_name("t"), "t_2"); // continues from shared counter

        assert_eq!(gen_a.next_name("t"), "t_3"); // still shared
    }
}
