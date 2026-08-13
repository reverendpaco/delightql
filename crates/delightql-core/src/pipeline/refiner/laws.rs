// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// laws.rs - Implementation of Laws 1-6
//
// These laws govern predicate classification and association

use super::types::*;
use crate::error::Result;
use crate::names::{ColId, Registry, ScopeId};
use crate::pipeline::ast_visit::{walk_visit_boolean, AstVisit, Descent};
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::asts::core::Resolved;
#[cfg(test)]
use crate::pipeline::asts::core::{Membership, Probe, ValueRow};
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::resolved;
use std::collections::HashSet;

/// Context needed for law checking
pub struct LawContext {
    /// Which segment tables ARE a bag operation's finished relation.
    pub bag_tables: HashSet<ScopeId>,
}

/// Law 1: Forbidden UL Fragment Join
///
/// A join condition on a relation produced by a bag operation must not
/// reach into one of its arms. After a set operation there is ONE relation
/// and no answer to which arm a row came from, so a condition addressing an
/// arm is asking a question the join has no way to answer.
pub fn check_law1(
    pred: &resolved::TruthExpression,
    left_table: ScopeId,
    right_table: ScopeId,
    context: &LawContext,
    identities: &Registry,
) -> Option<ForbiddenReason> {
    for table in [left_table, right_table] {
        if context.bag_tables.contains(&table) && references_table(pred, table, identities) {
            return Some(ForbiddenReason::Law1UlFragmentJoin);
        }
    }
    None
}

/// Law 4: Non-Intersection Filters
/// Filters on columns that don't exist in all operands become regular F filters
/// This is handled during classification, not as a forbidding law

/// Law 5: Scope Eagerness
/// Predicates eagerly attach to the earliest operation where all their
/// referenced symbols are in scope
///
/// This returns the earliest valid scope point for a predicate
pub fn find_earliest_scope(
    pred: &resolved::TruthExpression,
    tables_sequence: &[(usize, HashSet<ScopeId>)],
    identities: &Registry,
) -> ScopePoint {
    let referenced = extract_referenced_tables(pred, identities);

    // Find the earliest point where all referenced tables are in scope
    for (position, tables_in_scope) in tables_sequence {
        if referenced.is_subset(tables_in_scope) {
            return ScopePoint {
                position: *position,
                tables_in_scope: tables_in_scope.clone(),
            };
        }
    }

    // If no valid scope found, attach at the end
    let last = tables_sequence.last().unwrap();
    ScopePoint {
        position: last.0,
        tables_in_scope: last.1.clone(),
    }
}

// Helper functions

/// Law 1: does `pred` qualify-reference `table` anywhere — including inside a
/// nested `EXISTS`/`IN`/scalar subquery's own predicates?
///
/// An `AstVisit<Resolved>` finder rather than a hand-rolled match: the shared
/// closure descends EVERY query-bearing edge, so the boolean type is closed by
/// construction — a qualified `Lvar` `table.col` is found in any position, and
/// no variant can be reached without an arm. A hand-rolled trio answers "no"
/// for whatever it forgot to descend into, which reads as "does not reference".
fn references_table(
    pred: &resolved::TruthExpression,
    table: ScopeId,
    identities: &Registry,
) -> bool {
    let mut finder = QualifierRefFinder {
        table,
        identities,
        found: false,
    };
    // The finder never returns Err; the walk is infallible.
    let _ = walk_visit_boolean(&mut finder, pred);
    finder.found
}

/// Finds a qualified column reference `table.col` anywhere in a resolved boolean
/// subtree (mirrors the old semantics: reference is via a qualified `Lvar`).
struct QualifierRefFinder<'a> {
    table: ScopeId,
    identities: &'a Registry,
    found: bool,
}

impl AstVisit<Resolved> for QualifierRefFinder<'_> {
    fn enter_domain(&mut self, e: &resolved::DomainExpression) -> Result<Descent> {
        if let resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence {
                column,
                explicit_qualifier: true,
                ..
            },
        ))) = e
        {
            if self.identities.scope_of(*column) == self.table {
                self.found = true;
                return Ok(Descent::Break);
            }
        }
        Ok(Descent::Continue)
    }
}

fn extract_referenced_tables(
    pred: &resolved::TruthExpression,
    identities: &Registry,
) -> HashSet<ScopeId> {
    let mut finder = ColumnFinder::default();
    let _ = walk_visit_boolean(&mut finder, pred);
    finder
        .columns
        .into_iter()
        .map(|column| identities.scope_of(column))
        .collect()
}

#[derive(Default)]
struct ColumnFinder {
    columns: HashSet<ColId>,
}

impl AstVisit<Resolved> for ColumnFinder {
    fn enter_domain(&mut self, e: &resolved::DomainExpression) -> Result<Descent> {
        match e {
            resolved::DomainExpression::Reference(Reference::Named(NamedReference(
                ColumnOccurrence { column, .. },
            ))) => {
                self.columns.insert(*column);
            }
            _ => {}
        }
        Ok(Descent::Continue)
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::asts::vocabulary::Vec1;
    use super::*;

    fn lvar(column: ColId, explicit_qualifier: bool) -> resolved::DomainExpression {
        resolved::DomainExpression::Reference(Reference::Named(NamedReference(ColumnOccurrence {
            column,
            explicit_qualifier,
        })))
    }

    fn scope_with_column(
        registry: &Registry,
        scope_name: &str,
        column_name: &str,
    ) -> (ScopeId, ColId) {
        let scope_spelling = registry.intern(scope_name, false);
        let scope = registry.mint_scope(
            crate::names::ScopeOrigin::AnonRelation,
            crate::names::Hint::User(scope_spelling),
            None,
        );
        let column_spelling = registry.intern(column_name, false);
        let column = registry.mint_column(
            scope,
            crate::names::ColumnOrigin::Bound { position: 0 },
            Some(column_spelling),
            crate::names::Addressing::Published,
            crate::names::ValueFacts::default(),
        );
        (scope, column)
    }

    // `references_table` reaches the qualified `Lvar` inside `In.value` — a
    // position a top-level-only match never descends into.
    #[test]
    fn references_table_finds_qualifier_in_in_predicate() {
        let registry = Registry::new(&[]);
        let (x, id) = scope_with_column(&registry, "x", "id");
        let (z, a) = scope_with_column(&registry, "z", "a");
        let pred = resolved::TruthExpression::Membership(Membership {
            probe: Probe::Value(Box::new(lvar(id, true))),
            rows: Vec1::new(ValueRow(Vec1::new(lvar(a, false)))),
            negated: false,
            source: crate::pipeline::asts::core::MembershipSource::In,
        });
        assert!(references_table(&pred, x, &registry));
        assert!(!references_table(&pred, z, &registry));
    }
}
