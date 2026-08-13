// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

use std::rc::Rc;

use crate::names::ScopeId;
use crate::pipeline::asts::refined::{self, JoinType};

#[stacksafe::stacksafe]
pub(super) fn extract_schema(
    expr: &refined::Chain,
    identities: &Rc<crate::names::Registry>,
) -> ScopeId {
    let Some((last, prefix)) = expr.split_last() else {
        return match &expr.head {
            refined::Grelex::Literal(anon) => anon.table.cpr_schema,
            refined::Grelex::Reference(rel) => match rel {
                refined::Relation::Ground { cpr_schema, .. }
                | refined::Relation::InnerRelation { cpr_schema, .. }
                | refined::Relation::FunctorCall { cpr_schema, .. } => *cpr_schema,
                refined::Relation::ConsultedView { scoped, .. } => *scoped,
            },
        };
    };
    match last {
        refined::Continuation::Access { cpr_schema, .. }
        | refined::Continuation::Member { cpr_schema, .. }
        | refined::Continuation::BagOp { cpr_schema, .. }
        | refined::Continuation::Pipe { cpr_schema, .. }
        | refined::Continuation::Structural(refined::StructuralStep { cpr_schema, .. }) => {
            *cpr_schema
        }
        // A restriction, a bound and a correlation all leave the heading.
        refined::Continuation::Restrict { .. }
        | refined::Continuation::Bound { .. }
        | refined::Continuation::Correlate { .. } => extract_schema(&prefix.to_chain(), identities),
        // A destructure publishes its own scope: it adds the pattern's columns
        // beside the heading it republished.
        refined::Continuation::Destructure { cpr_schema, .. } => *cpr_schema,
        refined::Continuation::ErJoin(_) => {
            unreachable!("ER joins are consumed by resolution")
        }
    }
}

pub(super) fn merge_schemas_for_join(
    left: ScopeId,
    right: ScopeId,
    identities: &Rc<crate::names::Registry>,
) -> ScopeId {
    let output = identities.mint_scope(
        crate::names::ScopeOrigin::Join { left, right },
        crate::names::Hint::None,
        None,
    );
    identities.republish_heading(left, output, crate::names::Republish::JoinArm);
    identities.republish_heading(right, output, crate::names::Republish::JoinArm);
    output
}

pub(super) fn compute_join_schema(
    left: &refined::Chain,
    right: &refined::Chain,
    _join_type: JoinType,
    identities: &Rc<crate::names::Registry>,
) -> ScopeId {
    merge_schemas_for_join(
        extract_schema(left, identities),
        extract_schema(right, identities),
        identities,
    )
}

pub(super) fn compute_filter_schema(
    source: &refined::Chain,
    identities: &Rc<crate::names::Registry>,
) -> ScopeId {
    extract_schema(source, identities)
}
