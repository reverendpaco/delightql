// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Structural dependency analysis for interdependent EXISTS clauses.

use crate::error::Result;
use crate::pipeline::ast_visit::{walk_visit_relational, AstVisit, Descent};
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::asts::core::Existence;
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::resolved::{self, Resolved};
use crate::pipeline::refiner::flattener::FlatPredicate;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Default)]
pub struct ExistsDependencies {
    pub dependencies: HashMap<crate::names::ScopeId, HashSet<crate::names::ScopeId>>,
    pub roots: HashSet<crate::names::ScopeId>,
    pub exists_scopes: HashSet<crate::names::ScopeId>,
}

pub(super) fn detect_interdependent_exists(
    predicates: &[FlatPredicate],
    identities: &crate::relation::Planning,
) -> Result<ExistsDependencies> {
    let mut deps = ExistsDependencies::default();
    let mut relations = HashMap::new();

    for pred in predicates {
        if let resolved::TruthExpression::Existence(Existence {
            relation: subquery, ..
        }) = &pred.expr
        {
            let relation = subquery.semantic_relation();
            deps.exists_scopes.insert(relation.scope());
            relations.insert(relation.scope(), relation);
        }
    }

    for pred in predicates {
        let resolved::TruthExpression::Existence(Existence {
            relation: subquery, ..
        }) = &pred.expr
        else {
            continue;
        };
        let exists_relation = subquery.semantic_relation();
        let exists_scope = exists_relation.scope();
        let mut collector = ReferencedScopes {
            identities,
            scopes: HashSet::new(),
        };
        walk_visit_relational(&mut collector, subquery)?;
        collector.scopes.retain(|scope| {
            !crate::relation::contains_scope(identities, &exists_relation, *scope).unwrap_or(false)
        });

        let references = deps
            .exists_scopes
            .iter()
            .copied()
            .filter(|candidate| {
                collector.scopes.iter().any(|referenced| {
                    candidate == referenced
                        || relations.get(candidate).is_some_and(|relation| {
                            crate::relation::contains_scope(identities, relation, *referenced)
                                .unwrap_or(false)
                        })
                })
            })
            .collect::<HashSet<_>>();

        if references.is_empty() {
            deps.roots.insert(exists_scope);
        } else {
            deps.dependencies.insert(exists_scope, references);
        }
    }

    Ok(deps)
}

struct ReferencedScopes<'a> {
    identities: &'a crate::names::Registry,
    scopes: HashSet<crate::names::ScopeId>,
}

impl AstVisit<Resolved> for ReferencedScopes<'_> {
    fn enter_domain(&mut self, expr: &resolved::DomainExpression) -> Result<Descent> {
        match expr {
            resolved::DomainExpression::Reference(Reference::Named(NamedReference(
                ColumnOccurrence { column, .. },
            ))) => {
                self.scopes
                    .insert(crate::relation::owner(self.identities, *column)?);
            }
            _ => {}
        }
        Ok(Descent::Continue)
    }

    /// A whole-heading correlation references its two arms BY SCOPE — there
    /// is no column walk to find them, so the continuation that holds one is
    /// read directly.
    fn enter_continuation(&mut self, continuation: &resolved::Continuation) -> Result<Descent> {
        if let resolved::Continuation::Correlate { whole, .. } = continuation {
            let (left, right) = whole.arms();
            self.scopes.extend([left.scope(), right.scope()]);
        }
        Ok(Descent::Continue)
    }
}
