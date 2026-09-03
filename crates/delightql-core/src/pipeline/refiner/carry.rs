// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The refined form of a resolved subtree the refiner decides nothing about.
//!
//! Most of what crosses the resolved→refined edge is not a refinement. A
//! literal, a boolean leaf, an already-classified inner relation: the refiner
//! looks at them and has nothing to say. They still have to arrive in the
//! next phase, and the only road there is a fold.
//!
//! This is that fold, named, and it is fallible like every other. What it
//! does NOT have is a way to retag a payload: the payload methods it inherits
//! answer for one slot each, and a slot whose two phases hold different types
//! has to be answered here, in the open, or this file stops compiling.

use crate::error::Result;
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::asts::core::JoinType;
use crate::pipeline::asts::{refined, resolved};

/// The fold for a subtree the refiner is only carrying.
pub(crate) struct CarryToRefined;

impl AstTransform<resolved::Resolved, refined::Refined> for CarryToRefined {
    fn fold_correlation_arm(
        &mut self,
        arm: crate::relation::SemanticRelation,
    ) -> crate::error::Result<crate::relation::SemanticRelation> {
        Ok(arm)
    }


    crate::pipeline::ast_transform::uninhabited_payload_folds!(
        fold_column_ordinal,
        fold_column_range,
        fold_placeholder,
        fold_context_marker,
    );
    fn fold_open_leaf(
        &mut self,
        _: crate::pipeline::asts::core::FormalHole,
    ) -> crate::error::Result<crate::pipeline::asts::vocabulary::Never> {
        Err(crate::error::DelightQLError::validation_error_categorized(
            "value/open/unapplied",
            "a composition input stands outside any callable applying it",
            "the position that applies an open body spends its slot",
        ))
    }

    fn fold_cover_callable(&mut self, callable: ()) -> crate::error::Result<()> {
        Ok(callable)
    }

    fn fold_rename_target(
        &mut self,
        target: crate::names::Spelling,
    ) -> crate::error::Result<crate::names::Spelling> {
        Ok(target)
    }
    crate::pipeline::ast_transform::decided_payload_travels_forward!(
        fold_scope(crate::relation::SemanticRelation),
        fold_output(crate::relation::PortId),
        fold_scalar_output(crate::relation::PortId),
        fold_destructure(Vec<crate::pipeline::asts::core::DestructureMapping>),
        fold_drill(crate::pipeline::asts::core::operators::BoundDrill),
        fold_entity(crate::names::CallableId),
        fold_col(crate::pipeline::asts::core::ColumnOccurrence),
        fold_binder(crate::relation::PortId),
    );

    /// A comma with no decided orientation is an inner join, and the refined
    /// phase works with a decided one. This is the single narrowing on this
    /// edge, so it lives here rather than at each site that carries a member.
    fn transform_continuation(
        &mut self,
        continuation: resolved::Continuation,
    ) -> Result<refined::Continuation> {
        let continuation = match continuation {
            resolved::Continuation::Member {
                rhs,
                correlation,
                join_type,
            } => resolved::Continuation::Member {
                rhs,
                correlation,
                join_type: Some(join_type.unwrap_or(JoinType::Inner)),
            },
            other => other,
        };
        crate::pipeline::ast_transform::walk_transform_continuation(self, continuation)
    }
}

macro_rules! carry {
    ($($name:ident : $method:ident ($from:ty) -> $to:ty),+ $(,)?) => {
        $(
            pub(crate) fn $name(node: $from) -> Result<$to> {
                CarryToRefined.$method(node)
            }
        )+
    };
}

pub(crate) fn probe(
    probe: crate::pipeline::asts::core::Probe<resolved::Resolved>,
) -> Result<crate::pipeline::asts::core::Probe<refined::Refined>> {
    crate::pipeline::ast_transform::transform_probe(&mut CarryToRefined, probe)
}

pub(crate) fn inner_relation(
    pattern: crate::pipeline::asts::core::expressions::relational::InnerRelationPattern<
        resolved::Resolved,
    >,
) -> Result<
    crate::pipeline::asts::core::expressions::relational::InnerRelationPattern<refined::Refined>,
> {
    CarryToRefined.transform_inner_relation(pattern)
}

pub(crate) fn anon_table(table: resolved::AnonTable) -> Result<refined::AnonTable> {
    CarryToRefined.transform_anon_table(table)
}

carry! {
    domain: transform_domain (resolved::DomainExpression) -> refined::DomainExpression,
    boolean: transform_boolean (resolved::TruthExpression) -> refined::TruthExpression,
    access: transform_access (resolved::Access) -> refined::Access,
    tree_pattern: transform_tree_pattern (resolved::TreePattern) -> refined::TreePattern,
}
