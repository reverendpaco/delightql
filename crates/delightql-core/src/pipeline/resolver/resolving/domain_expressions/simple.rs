// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

use crate::error::{DelightQLError, Result};
use crate::names::{ColId, Registry, ScopeId};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;

use crate::pipeline::asts::core::{AuthoredColumn, ColumnOccurrence};
use crate::pipeline::asts::core::{NamedReference, Reference};
use delightql_types::SqlIdentifier;

/// What a correlated condition's names did, gathered as they resolve.
///
/// A reference reaching out of an interior relation is a correlation when
/// the condition also names a column of that relation, and a mistake when it
/// does not — the same act, judged by its company. No single reference knows
/// which it is, so the fact is accumulated over the whole condition and read
/// once at the end.
#[derive(Clone, Copy, Debug, Default)]
pub(in crate::pipeline::resolver) struct Witness {
    /// A name bound inside the interior relation.
    pub anchored: bool,
    /// A name found nothing there and was answered by the enclosing row.
    pub escaped: bool,
}

pub(in crate::pipeline::resolver) fn resolve_simple_expr(
    expr: ast_unresolved::DomainExpression,
    available: &[ColId],
    local_available: &[ColId],
    qualifier_scope: &[ScopeId],
    in_correlation: bool,
    witness: &mut Witness,
    registry: &Registry,
) -> Result<ast_resolved::DomainExpression> {
    match expr {
        ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
            AuthoredColumn {
                name,
                qualifier,
                namespace_path: _,
            },
        ))) => resolve_lvar(
            name,
            qualifier,
            available,
            local_available,
            qualifier_scope,
            in_correlation,
            witness,
            registry,
        ),
        ast_unresolved::DomainExpression::Reference(Reference::Ordinal(ordinal)) => {
            use super::super::super::unification::{
                unify_columns, ColumnReference, UnificationResult,
            };
            let result = unify_columns(
                vec![ColumnReference::Ordinal {
                    position: ordinal.position,
                    reverse: ordinal.reverse,
                    qualifier: ordinal.qualifier.clone(),
                }],
                available,
                qualifier_scope,
                registry,
            )
            .into_iter()
            .next()
            .expect("one ordinal reference produces one unification result");
            let column = match result {
                UnificationResult::Resolved(column) => column,
                UnificationResult::Unresolved(column) => {
                    if registry.any_heading_opaque(qualifier_scope) {
                        return Err(opaque_heading_refusal());
                    }
                    return Err(DelightQLError::column_not_found_error(
                        column,
                        "in ordinal predicate reference",
                    ));
                }
                UnificationResult::Opaque => {
                    return Err(crate::pipeline::resolver::opaque_reference_refusal())
                }
                UnificationResult::Refused(refusal) => return Err(refusal.into_error()),
                UnificationResult::Ambiguous { .. } => {
                    unreachable!("an ordinal selects by position, not by name")
                }
            };
            Ok(ast_resolved::DomainExpression::Reference(Reference::Named(
                NamedReference(ColumnOccurrence {
                    column,
                    explicit_qualifier: ordinal.qualifier.is_some(),
                    // As written: the alias PUBLISHES the name, so folding it
                    // here publishes one the author's own spelling cannot reach.
                }),
            )))
        }
        ast_unresolved::DomainExpression::Application(ast_unresolved::FunctionApplication::Ground(value)) => {
            Ok(ast_resolved::DomainExpression::Application(ast_resolved::FunctionApplication::Ground(super::super::super::helpers::converters::convert_literal_value(value))))
        }
        // The position that applies an open body spends its leaf during
        // resolution; a leaf reaching this road is outside every such
        // position and refuses BEFORE any closed resolved tree is minted.
        ast_unresolved::DomainExpression::Application(
            ast_unresolved::FunctionApplication::Open(_),
        ) => Err(crate::error::DelightQLError::validation_error_categorized(
            "value/open/unapplied",
            "a composition input stands outside any callable applying it",
            "the position that applies an open body spends its slot",
        )),
        _ => unreachable!("resolve_simple_expr called with non-simple expression"),
    }
}

fn resolve_lvar(
    name: SqlIdentifier,
    qualifier: Option<SqlIdentifier>,
    available: &[ColId],
    local_available: &[ColId],
    qualifier_scope: &[ScopeId],
    in_correlation: bool,
    witness: &mut Witness,
    registry: &Registry,
) -> Result<ast_resolved::DomainExpression> {
    use super::super::super::unification::{unify_columns, ColumnReference, UnificationResult};

    // An interior relation is the lexical scope under the reader's finger.
    // Search it before the enclosing context, including for a qualified
    // reference: a second `addresses` interior shadows an earlier sibling
    // named `addresses`. A different qualifier widens only after the local
    // heading proves it absent.
    //
    // Some complete EXISTS contexts disable deferred correlation validation,
    // so `in_correlation` alone does not identify this shape. The additional
    // occurrences in `available` are the structural evidence that an
    // enclosing context exists.
    let has_enclosing = available
        .iter()
        .any(|column| !local_available.contains(column));
    // `_` is exempt. Narrowing is LEXICAL SHADOWING — an inner relation
    // named `addresses` hides an outer one — and shadowing needs a name to
    // shadow. `_` has none: it points at the one unnamed pipe output in
    // view, and deciding whether there is exactly one means enumerating them
    // all. Searching the inner heading first would stop at the
    // nearest stage and answer where the law refuses, so a correlation
    // holding a pipe of its own would silently take it — which is the worse
    // of the two defects, because `_.x = _.x` would resolve and mean nothing.
    let points_at_a_pipe = qualifier.as_deref() == Some("_");
    let narrowed = !points_at_a_pipe && (has_enclosing || (in_correlation && qualifier.is_none()));
    let reference = ColumnReference::Named {
        name: name.clone(),
        qualifier: qualifier.clone(),
    };
    // A joined interior still needs the qualifiers of its own arms: its output
    // columns belong to the join scope, which answers to no arm name. A scope
    // is local when one of the local columns stands in it or republishes one
    // of its columns. This is occurrence evidence, so an earlier access to the
    // same base entity is not mistaken for the current relation.
    let local_qualifier_scope: Vec<ScopeId> = qualifier_scope
        .iter()
        .copied()
        .filter(|scope| {
            let heading = registry.heading(*scope).columns_seen();
            local_available.iter().any(|candidate| {
                registry.scope_of(*candidate) == *scope
                    || heading
                        .iter()
                        .any(|source| registry.republishes(*candidate, *source))
            })
        })
        .collect();
    // The local probe sees its own lexical qualifiers; the widened probe sees
    // the enclosing ones as well. Giving the local probe every qualifier would
    // let an earlier same-named relation defeat lexical shadowing.
    let address = |candidates: &[ColId], lexical_scopes: &[ScopeId]| {
        let mut offered_scopes: Vec<ScopeId> = candidates
            .iter()
            .map(|column| registry.scope_of(*column))
            .collect();
        offered_scopes.extend(lexical_scopes.iter().copied());
        let visible = offered_scopes
            .into_iter()
            .fold(Vec::new(), |mut scopes, scope| {
                if !scopes.contains(&scope) {
                    scopes.push(scope);
                }
                scopes
            });
        // What this site OFFERS. What is then searched is a different set — a
        // qualified reference reaches lexical scopes these candidates do not
        // own — and only the addressing road can enumerate it, so it does.
        crate::probe::probe!(
            resolve,
            "lvar {name:?} qualifier={qualifier:?} offered={} visible={visible:?}",
            candidates.len()
        );
        unify_columns(vec![reference.clone()], candidates, &visible, registry)
            .into_iter()
            .next()
            .expect("one reference produces one unification result")
    };
    let result = match address(
        if narrowed { local_available } else { available },
        if narrowed {
            &local_qualifier_scope
        } else {
            qualifier_scope
        },
    ) {
        // ABSENT from the inner relation is not a miss. A correlated subquery
        // stands inside a statement, and a name the subquery's own source does
        // not publish is what the enclosing row is there to answer — the inner
        // relation carrying that value under a DIFFERENT name is history, and
        // history does not shadow a name it no longer spells.
        //
        // Widen only on absence. A name the inner relation claims ambiguously
        // is still the inner relation's, and answering it with an outer column
        // would resolve, silently, a reference the query left undecided.
        UnificationResult::Unresolved(_) | UnificationResult::Refused(_) if narrowed => {
            // The one place a reference written inside an interior relation
            // comes to mean something outside it. Worth a topic of its own:
            // the escape is correct where the predicate also names a column
            // of its own source and is a mistake where it does not, and only
            // a reader looking at the whole predicate can tell which.
            crate::probe::probe!(escape, "{name:?} left the interior relation");
            if in_correlation {
                witness.escaped = true;
            }
            address(available, qualifier_scope)
        }
        settled => settled,
    };
    // Anchoring is decided by which relation the reference LANDED on, not by
    // how it was spelled: a qualified name may reach the interior relation,
    // so reading the spelling would call `employees.id == kid_id` an
    // uncorrelated predicate for having qualified the half that anchors it.
    //
    // Escaping is NOT the complement of this. Only the widening above is an
    // escape — a reference that found nothing here and was answered outside.
    // A name that simply resolves elsewhere may be an ordinary qualified
    // reference, or a reference belonging to an enclosing resolution
    // altogether, and counting those would refuse conditions that never
    // reached for anything missing.
    if in_correlation {
        if let UnificationResult::Resolved(column) = result {
            if local_available.contains(&column) {
                witness.anchored = true;
            }
        }
    }
    match result {
        UnificationResult::Resolved(column) => {
            Ok(ast_resolved::DomainExpression::Reference(Reference::Named(
                NamedReference(ColumnOccurrence {
                    column,
                    explicit_qualifier: qualifier.is_some(),
                    // As written: the alias PUBLISHES the name, so folding it
                    // here would publish a name the author's own spelling
                    // could not then reach.
                }),
            )))
        }
        UnificationResult::Unresolved(column) => {
            if registry.any_heading_opaque(qualifier_scope) {
                return Err(opaque_heading_refusal());
            }
            Err(DelightQLError::column_not_found_error(
                column,
                "in domain expression",
            ))
        }
        UnificationResult::Opaque => Err(crate::pipeline::resolver::opaque_reference_refusal()),
        UnificationResult::Refused(refusal) => Err(refusal.into_error()),
        UnificationResult::Ambiguous { column, tables } => {
            Err(DelightQLError::validation_error_categorized(
                "resolution/ambiguous",
                format!(
                    "Ambiguous column '{}' exists in scopes: {}",
                    column,
                    tables.join(", "),
                ),
                "in domain expression",
            ))
        }
    }
}

/// A name was used against a relation whose dimensions the target does not
/// publish. Nothing was enumerated, so nothing can be reported absent.
pub(crate) fn opaque_heading_refusal() -> DelightQLError {
    DelightQLError::validation_error_categorized(
        crate::uri_registry::subcat::RESOLUTION_SCHEMA,
        "this relation's heading is not published by the target, so its dimensions \
         cannot be named here",
        "declare the dimensions at the mention — `f(...)(a, b)` names one slot per \
         dimension of the full width",
    )
}
