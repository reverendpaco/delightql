// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// constraint_analyzer.rs - Extract constraints from table patterns and anonymous tables
//
// This module handles constraint extraction from positional patterns and anonymous table processing

use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::asts::core::Comparison;
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::resolved;
use crate::pipeline::refiner::flattener::{FlatOperator, FlatOperatorKind, FlatSegment, FlatTable};
use crate::pipeline::refiner::types::*;

/// Create join predicates for anonymous table constraints (Epoch 3 fix)
pub(super) fn create_anonymous_table_join_predicates(
    analyzed_predicates: &mut Vec<AnalyzedPredicate>,
    flat: &FlatSegment,
    identities: &crate::names::Registry,
) {
    log::debug!(
        "create_anonymous_table_join_predicates called with {} tables",
        flat.tables.len()
    );
    // For each anonymous table with column headers that contain constraints
    for (table_idx, table) in flat.tables.iter().enumerate() {
        log::debug!(
            "Table {}: has anon_data? {}",
            table_idx,
            table.anonymous_data.is_some()
        );
        if let Some(ref anon_data) = table.anonymous_data {
            log::debug!("Processing anonymous table at index {}", table_idx);
            if let Some(ref headers) = anon_data.body.header {
                log::debug!("Anonymous table has {} headers", headers.len());
                // Process each header - create constraints for all non-pure-Lvar expressions
                for (col_idx, item) in headers.iter().enumerate() {
                    let header = item.term().expect("an anonymous header has a domain term");
                    log::debug!("Processing header {} : {:?}", col_idx, header);

                    let anon_column = identities
                        .known_heading(table.identity)
                        .expect("an anonymous table publishes what it was written with")
                        .in_order()
                        .nth(col_idx)
                        .copied()
                        .expect("anonymous headers and their structural heading agree");
                    let should_create_constraint = !matches!(
                        &header,
                        resolved::DomainExpression::Reference(Reference::Named(NamedReference(ColumnOccurrence { column, .. })))
                            if *column == anon_column
                    );

                    // Create constraint for any non-pure-Lvar expression
                    if should_create_constraint {
                        let left = resolved::DomainExpression::Reference(Reference::Named(
                            NamedReference(ColumnOccurrence {
                                column: anon_column,
                                explicit_qualifier: false,
                            }),
                        ));

                        let right = header.clone();
                        let predicate = resolved::TruthExpression::Comparison(Comparison {
                            operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
                            left: Box::new(left),
                            right: Box::new(right),
                        });

                        let referenced_table = match &header {
                            resolved::DomainExpression::Reference(Reference::Named(
                                NamedReference(ColumnOccurrence { column, .. }),
                            )) => Some(identities.scope_of(*column)),
                            _ => None,
                        };

                        let class = if let Some(referenced_table) = referenced_table {
                            PredicateClass::FJC {
                                left: referenced_table,
                                right: table.identity,
                            }
                        } else {
                            PredicateClass::F {
                                table: table.identity,
                            }
                        };

                        // Find the join operator position
                        let operator_ref = if table_idx > 0 {
                            OperatorRef::Join {
                                position: table_idx - 1,
                            }
                        } else {
                            OperatorRef::TopLevel
                        };

                        analyzed_predicates.push(AnalyzedPredicate {
                            class,
                            expr: predicate,
                            operator_ref,
                            origin: resolved::FilterOrigin::Generated,
                        });
                    }
                }
            }
        }
    }
}

/// Attach each table's dequalifying names to the join that brings it in.
///
/// ONE ALGORITHM, TWO POSITIONS. A `.(cols)` step is the mention's own access
/// when the mention could absorb it (`orders(*.(status))`) and a step on the
/// mention's result when it could not (`orders(id, _) .(status)`); both hold
/// the same `Access::Dequalify`. What those names MEAN to the join is one
/// question, and this is the one place that answers it.
pub(super) fn process_glob_with_using(
    mut operators: Vec<FlatOperator>,
    tables: &[FlatTable],
    identities: &crate::names::Registry,
) -> Vec<FlatOperator> {
    crate::probe::probing!(using, {
        for (i, table) in tables.iter().enumerate() {
            crate::probe::probe!(
                using,
                "table {i} {:?} spec={:?} names={:?}",
                table.identity,
                table.access,
                published_names(table, identities)
            );
        }
        for (i, op) in operators.iter().enumerate() {
            crate::probe::probe!(using, "operator {i} {:?}", op.kind);
        }
    });
    for i in 0..tables.len() {
        let Some(using_cols) = dequalifying_names(&tables[i]) else {
            continue;
        };
        log::debug!(
            "Found a dequalifying access on table {:?} with columns: {:?}",
            tables[i].identity,
            using_cols
        );
        // The join at position i-1 joins table[i-1] to table[i], and a
        // dequalifying access on table[i] is about the join that brings it
        // in: `orders(*.(user_id))` means USING when joining TO orders.
        if i > 0 && i - 1 < operators.len() {
            log::debug!("Applying USING to join at position {}", i - 1);
            let FlatOperatorKind::Join {
                ref mut correspondence,
            } = &mut operators[i - 1].kind;
            *correspondence = Some(resolved::Correspondence::new(
                using_cols
                    .iter()
                    .map(|name| {
                        // Both pieces of the identifier: the strop decides
                        // whether the join corresponds on `Name` or on any
                        // casing of it, so a heading holding both publishes
                        // two columns and only the spelling says which was
                        // asked for.
                        let spelling = identities.intern(name.as_str(), name.is_stropped());
                        identities.canonical(spelling)
                    })
                    .collect(),
            ));
        }
    }

    operators
}

/// The names a table dequalifies onto, from whichever position holds them.
///
/// The mention's own access is asked FIRST: it is the position the author
/// wrote inside the parens, and a step on the result only exists when the
/// mention could not hold one, so the two are never both present for one
/// table.
fn dequalifying_names(table: &FlatTable) -> Option<Vec<delightql_types::SqlIdentifier>> {
    if let resolved::Access::Dequalify(columns) = &table.access {
        return Some(columns.clone());
    }
    table
        .pipe_expr
        .as_deref()
        .and_then(trailing_dequalifying_access)
}

/// The dequalifying access of a chain's trailing pipe run.
///
/// KEPT LOCAL (not routed through `Chain::source_spine`): this reads the pipe
/// run ONLY and stops at a restriction — a `.(cols)` separated from the outer
/// chain by a filter is deliberately NOT found, and the source spine reads
/// through restrictions.
fn trailing_dequalifying_access(
    expr: &resolved::Chain,
) -> Option<Vec<delightql_types::SqlIdentifier>> {
    // THE STEPS ONLY. A mention's own access is asked through `table.access`;
    // reading it here as well would find a dequalification the mention
    // already answered for, at a table this walk was handed for its pipes.
    let mut rest = expr.steps();
    while let Some((last, prefix)) = rest.split_last() {
        match last {
            resolved::Continuation::Access {
                access: resolved::Access::Dequalify(columns),
                ..
            } => return Some(columns.clone()),
            resolved::Continuation::Access { .. } | resolved::Continuation::Pipe { .. } => {}
            _ => return None,
        }
        rest = prefix;
    }
    None
}

/// The canonical spellings a table publishes, in heading order.
///
/// Order carries: the USING list decides which side of the merged column the
/// output takes, and two tables that share a name must agree on where it sits.
fn published_names(
    table: &FlatTable,
    identities: &crate::names::Registry,
) -> Vec<crate::names::Sym> {
    let scope = table.schema;
    let mut names = Vec::new();
    for column in identities.heading(scope).columns_seen() {
        if let Some(published) = identities.published_sym(column) {
            if !names.contains(&published) {
                names.push(published);
            }
        }
    }
    names
}
