// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The final lowering boundary for a mutation.
//!
//! This module owns the typed mutation boundary used by production DML
//! lowering and its focused contract tests.

use crate::names::{CallableCategory, CallableId, DmlVerb, Registry, ScopeId};
use crate::pipeline::asts::core::phases::Refined;
use crate::pipeline::asts::core::Access;
use crate::pipeline::sql_ast::statements::RelationTarget;

/// Why a mutation constructor refused a callable descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanBuildError {
    Uncategorized { callable: CallableId },
    NonDml { category: CallableCategory },
}

/// One lowering/effect-plan mutation value.
///
/// The source is the ordinary whole lowered operand — whatever the shared
/// lowering road produces, carried here as a black box. There is no source
/// or predicate fragment accessor and no proof wrapper: a reader may have
/// the operand entire or not at all, which is the same rule every other
/// continuation consumer lives under.
pub struct Mutation<S> {
    callable: CallableId,
    category: CallableCategory,
    source: S,
    target: RelationTarget,
    target_scope: ScopeId,
    receipt: Access<Refined>,
}

impl<S> Mutation<S> {
    /// Construct a mutation only from a registry descriptor whose category is
    /// one of the three DML verbs.
    pub fn try_new(
        registry: &Registry,
        callable: CallableId,
        source: S,
        target: RelationTarget,
        target_scope: ScopeId,
        receipt: Access<Refined>,
    ) -> Result<Self, PlanBuildError> {
        let category = registry
            .callable_category(callable)
            .ok_or(PlanBuildError::Uncategorized { callable })?;
        if !matches!(category, CallableCategory::Dml(_)) {
            return Err(PlanBuildError::NonDml { category });
        }
        Ok(Self {
            callable,
            category,
            source,
            target,
            target_scope,
            receipt,
        })
    }

    /// The verb is derived from the registry category, never supplied by a
    /// caller.
    pub fn verb(&self) -> DmlVerb {
        match self.category {
            CallableCategory::Dml(verb) => verb,
            _ => unreachable!("Mutation can only contain a DML category"),
        }
    }

    /// The relation being written.
    pub fn target(&self) -> &RelationTarget {
        &self.target
    }

    /// The scope of the relation being written.
    pub fn target_scope(&self) -> ScopeId {
        self.target_scope
    }

    /// The whole operand, borrowed. Nothing smaller is offered.
    pub fn source(&self) -> &S {
        &self.source
    }

    /// The whole operand, taken.
    pub fn into_source(self) -> S {
        self.source
    }

    /// Carry the same mutation over a transformed operand.
    ///
    /// The only door between operand representations, so the callable, the
    /// category, the target and the receipt cannot be restated — and a
    /// mutation cannot come to stand over a source that was decided
    /// somewhere other than the one lowering road.
    pub fn map_source<T, E>(
        self,
        transform: impl FnOnce(S) -> Result<T, E>,
    ) -> Result<Mutation<T>, E> {
        Ok(Mutation {
            callable: self.callable,
            category: self.category,
            source: transform(self.source)?,
            target: self.target,
            target_scope: self.target_scope,
            receipt: self.receipt,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::names::{CallableCategory, DmlVerb, Hint, ScopeOrigin};
    use crate::pipeline::sql_ast::QueryExpression;

    fn fixture(category: CallableCategory) -> Result<Mutation<QueryExpression>, PlanBuildError> {
        let registry = Registry::new(&[]);
        let spelling = registry.intern("mutation", false);
        let callable = registry.mint_callable(spelling, Vec::new(), category);
        let scope = registry.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
        Mutation::try_new(
            &registry,
            callable,
            QueryExpression::Values { rows: Vec::new() },
            RelationTarget::Scope(scope),
            scope,
            Access::All,
        )
    }

    #[test]
    fn accepts_each_dml_category_and_derives_its_verb() {
        for (category, expected) in [
            (CallableCategory::Dml(DmlVerb::Insert), DmlVerb::Insert),
            (CallableCategory::Dml(DmlVerb::Update), DmlVerb::Update),
            (CallableCategory::Dml(DmlVerb::Delete), DmlVerb::Delete),
        ] {
            let mutation = fixture(category).unwrap();
            assert_eq!(mutation.verb(), expected);
            assert!(matches!(mutation.receipt, Access::All));
        }
    }

    #[test]
    fn refuses_uncategorized_and_non_dml_callables() {
        for category in [
            CallableCategory::Scalar,
            CallableCategory::Relational,
            CallableCategory::Effect,
        ] {
            assert!(matches!(
                fixture(category),
                Err(PlanBuildError::NonDml { category: actual }) if actual == category
            ));
        }

        let registry = Registry::new(&[]);
        let spelling = registry.intern("compatibility", false);
        let callable = registry.mint_function(spelling, Vec::new());
        let scope = registry.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
        assert!(matches!(
            Mutation::try_new(
                &registry,
                callable,
                QueryExpression::Values { rows: Vec::new() },
                RelationTarget::Scope(scope),
                scope,
                Access::All,
            ),
            Err(PlanBuildError::Uncategorized { callable: actual }) if actual == callable
        ));
    }

    /// The operand may change representation; the mutation's identity may
    /// not. A road that could restate the verb while swapping the source is
    /// a second place for the verb to come from.
    #[test]
    fn mapping_the_source_carries_the_same_mutation() {
        let mutation = fixture(CallableCategory::Dml(DmlVerb::Update)).unwrap();
        let target_scope = mutation.target_scope();
        let mapped: Mutation<usize> = mutation
            .map_source(|source| {
                Ok::<_, PlanBuildError>(match source {
                    QueryExpression::Values { rows } => rows.len(),
                    _ => unreachable!("the fixture carries a VALUES body"),
                })
            })
            .unwrap();
        assert_eq!(mapped.verb(), DmlVerb::Update);
        assert_eq!(mapped.target_scope(), target_scope);
        assert_eq!(*mapped.source(), 0);
    }
}
