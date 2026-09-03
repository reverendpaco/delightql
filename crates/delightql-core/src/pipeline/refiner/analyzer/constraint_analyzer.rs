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
    identities: &crate::relation::Planning,
) -> crate::error::Result<()> {
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
                    // `_` is the disregarded slot: consumed, constrained by
                    // nothing.
                    let Some(header) = item.term() else { continue };
                    log::debug!("Processing header {} : {:?}", col_idx, header);

                    let anon_column = crate::relation::published_ports(identities, &table.relation)
                        .expect("an anonymous table publishes what it was written with")
                        .into_iter()
                        .nth(col_idx)
                        .expect("anonymous headers and their structural heading agree");
                    let should_create_constraint = !matches!(
                        &header,
                        resolved::DomainExpression::Reference(Reference::Named(NamedReference(ColumnOccurrence { column, .. })))
                            if *column == anon_column
                    );

                    // Create constraint for any non-pure-Lvar expression
                    if should_create_constraint {
                        let left = resolved::DomainExpression::Reference(Reference::Named(
                            NamedReference(ColumnOccurrence::engine(anon_column)),
                        ));

                        let right = header.clone();
                        // MINTED AS THE LANGUAGE'S EQUALITY, and settled with
                        // every other leaf below: a ground header term
                        // narrows the anonymous relation's own cell, and only
                        // a term reading ANOTHER relation makes rows multiply.
                        // Minting the target's equality here said "join" for
                        // `_(x, null)`, which then matched nothing.
                        let predicate = resolved::TruthExpression::Comparison(Comparison {
                            operator: crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual,
                            left: Box::new(left),
                            right: Box::new(right),
                        });

                        let referenced_table =
                            foreign_owner(&header, table.relation.scope(), identities);

                        // A header term that reads the table's OWN scope —
                        // a repeated binder's equality — is a filter, not a
                        // join condition: both operands stand in one table.
                        let class = match referenced_table {
                            Some(referenced_table)
                                if referenced_table != table.relation.scope() =>
                            {
                                PredicateClass::FJC {
                                    left: referenced_table,
                                    right: table.relation.scope(),
                                }
                            }
                            _ => PredicateClass::F {
                                table: table.relation.scope(),
                            },
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
                            expr: crate::pipeline::refiner::settled::settle_equality_classes(
                                predicate, flat, identities,
                            )?,
                            operator_ref,
                            origin: resolved::FilterOrigin::Generated,
                        });
                    }
                }
            }
        }
    }
    Ok(())
}

/// The first table OUTSIDE `own` that `expr` reads, walking through the
/// scalar forms a header term can hold. A computed term reaches whatever
/// its interior references reach: `upper:(description)` constrains against
/// the table that owns `description`, so classifying by the outermost node
/// alone read a join condition as a local filter and stranded it below the
/// header's own narrowing.
fn foreign_owner(
    expr: &resolved::DomainExpression,
    own: crate::names::ScopeId,
    identities: &crate::relation::Planning,
) -> Option<crate::names::ScopeId> {
    use crate::pipeline::asts::core::FunctionApplication;
    match expr {
        resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence { column, .. },
        ))) => crate::relation::owner(identities, *column)
            .ok()
            .filter(|owner| *owner != own),
        resolved::DomainExpression::Reference(_) => None,
        resolved::DomainExpression::Application(FunctionApplication::Ground(_)) => None,
        resolved::DomainExpression::Application(FunctionApplication::Standard(application)) => {
            application
                .call()
                .arguments
                .scalar_members()
                .iter()
                .find_map(|member| {
                    member
                        .scalar_domain()
                        .and_then(|value| foreign_owner(value, own, identities))
                })
        }
        resolved::DomainExpression::Application(FunctionApplication::Infix(infix)) => {
            foreign_owner(&infix.left, own, identities)
                .or_else(|| foreign_owner(&infix.right, own, identities))
        }
        resolved::DomainExpression::Application(FunctionApplication::JsonAccess(access)) => {
            foreign_owner(&access.source, own, identities)
        }
        // Remaining scalar forms (records, subqueries, cases) keep the
        // conservative answer: unclassified stays a local filter, exactly
        // what every header term answered before this walk existed.
        resolved::DomainExpression::Application(_) => None,
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
    identities: &crate::relation::Planning,
) -> crate::error::Result<Vec<FlatOperator>> {
    crate::probe::probing!(using, {
        for (i, table) in tables.iter().enumerate() {
            crate::probe::probe!(
                using,
                "table {i} {:?} spec={:?} names={:?}",
                table.relation,
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
            tables[i].relation,
            using_cols
        );
        // The join at position i-1 joins table[i-1] to table[i], and a
        // dequalifying access on table[i] is about the join that brings it
        // in: `orders(*.(user_id))` means USING when joining TO orders.
        if i > 0 && i - 1 < operators.len() {
            log::debug!("Applying USING to join at position {}", i - 1);
            let left: Vec<_> = tables
                .iter()
                .filter(|table| {
                    operators[i - 1]
                        .left_tables
                        .contains(&table.relation.scope())
                })
                .flat_map(|table| {
                    crate::relation::published_ports(identities, &table.relation)
                        .unwrap_or_default()
                })
                .collect();
            let right: Vec<_> = tables
                .iter()
                .filter(|table| {
                    operators[i - 1]
                        .right_tables
                        .contains(&table.relation.scope())
                })
                .flat_map(|table| {
                    crate::relation::published_ports(identities, &table.relation)
                        .unwrap_or_default()
                })
                .collect();
            let names = using_cols.iter().map(|name| {
                let spelling = identities.intern(name.as_str(), name.is_stropped());
                identities.canonical(spelling)
            });
            let exact = resolved::Correspondence::between(names, &left, &right, identities)?;
            let FlatOperatorKind::Join {
                ref mut correlation,
            } = &mut operators[i - 1].kind;
            // THE SEAT IS ALREADY TAKEN OR IT IS NOT. A member that stated
            // its own condition holds the only correlation this join has;
            // a correspondence merges the heading and a condition does not,
            // so there is no combination of the two to write. Refuse rather
            // than pick one and lose the other in silence.
            if correlation.condition().is_some() {
                return Err(crate::error::DelightQLError::validation_error_categorized(
                    "join/using/stated_condition",
                    "this join already carries a stated correlation, and a \
dequalifying access asks it to merge headings as well",
                    "write one correlation: either the shared names or the explicit condition",
                ));
            }
            *correlation = resolved::MemberCorrelation::Correspond(exact);
        }
    }

    Ok(operators)
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
        match last.form() {
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
    identities: &crate::relation::Planning,
) -> Vec<crate::names::Sym> {
    let scope = table.relation;
    let mut names = Vec::new();
    for column in crate::relation::published_ports(identities, &scope)
        .expect("a flattened table retains its authority-issued interface")
        .into_iter()
        .map(|port| port.column())
    {
        if let Some(published) = identities.published_sym(column) {
            if !names.contains(&published) {
                names.push(published);
            }
        }
    }
    names
}
