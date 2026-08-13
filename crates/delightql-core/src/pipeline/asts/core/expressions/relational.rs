// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Base relations and the derived-table patterns
//! Relation, InnerRelationPattern
//!
//! A relation is a chain HEAD (`Grelex::Reference`). The operators that
//! consume it live in `chain.rs`; nothing here nests a source.

use super::super::{Phase, Unresolved};
use super::access::Access;
use super::chain::{Chain, Continuation};
use super::domain::DomainExpression;
use super::functions::SealedCall;
use super::helpers::QualifiedName;
use super::truth::TruthExpression;
use crate::{lispy::ToLispy, ToLispy};
use delightql_types::SqlIdentifier;

/// Semantic patterns for INNER-RELATION
/// These capture the distinct compilation strategies for derived tables
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum InnerRelationPattern<P: Phase = Unresolved> {
    /// Indeterminate: Builder couldn't determine pattern yet
    /// Will be classified by refiner based on subquery structure
    #[lispy("pattern:indeterminate")]
    Indeterminate {
        identifier: QualifiedName,
        subquery: Box<Chain<P>>,
    },

    /// UDT: Uncorrelated Derived Table
    /// Simple projection/transformation with no correlation to outer query
    /// Compiles to: (SELECT ... FROM table) AS derived
    #[lispy("pattern:udt")]
    UncorrelatedDerivedTable {
        identifier: QualifiedName,
        subquery: Box<Chain<P>>,
        /// Whether this UDT wraps a consulted view (vs a regular table(|> pipeline)).
        /// When true and option://generation/rule/inlining/view is ON, the transformer
        /// lifts this to a CTE instead of inlining as a subquery.
        is_consulted_view: bool,
    },

    /// CDT-SJ: Correlated Derived Table - Scalar Join
    /// Has correlation predicate, no aggregation, no LIMIT
    /// Compiles to: JOIN with correlation predicate hoisted to ON clause
    #[lispy("pattern:cdt-sj")]
    CorrelatedScalarJoin {
        identifier: QualifiedName,
        correlation_filters: Vec<TruthExpression<P>>,
        subquery: Box<Chain<P>>,
    },

    /// CDT-GJ: Correlated Derived Table - Group Join
    /// Has correlation + aggregation
    /// Compiles to: JOIN with GROUP BY on correlation key
    #[lispy("pattern:cdt-gj")]
    CorrelatedGroupJoin {
        identifier: QualifiedName,
        correlation_filters: Vec<TruthExpression<P>>,
        aggregations: Vec<DomainExpression<P>>,
        subquery: Box<Chain<P>>,
    },
}

/// What an authored ground read SAYS about the relation it reads: how the
/// author addressed it, and the marks written on the mention itself.
///
/// `!!` and the passthrough slash live here rather than on the relation
/// because both are written ON the mention and both are read exactly where
/// the mention resolves — the first onto the occurrence's mutation evidence,
/// the second into the choice of lookup road. Neither survives that, so
/// neither is a property of the resolved relation.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum GroundMention {
    /// `users(*)`, `ns::orders(id, total)`, `ns/raw_table(*)` — a spelling
    /// resolution has to look up.
    #[lispy("mention:named")]
    Named {
        identifier: QualifiedName,
        alias: Option<SqlIdentifier>,
        /// DML mutation target marker: `!!` on source relation
        mutation_target: bool,
        /// Passthrough: skip entity catalog, use schema introspector directly.
        /// Syntax: `ns/raw_table(*)` — slash separates namespace from raw backend table name.
        passthrough: bool,
    },
    /// A compiler-owned relation selected by identity before resolution.
    /// Its heading is published on `scope` before the read resolves; no
    /// character-bearing lookup key participates. This covers plan-lifetime
    /// scratch and query-local higher-order carriers.
    ///
    /// It is a MENTION and not a relation of its own because what it names
    /// is a ground read like any other — it differs only in what resolution
    /// has to do to find it, which is nothing. Once resolution has run, the
    /// two are the same relation, and one carrier says so.
    #[lispy("mention:plan")]
    Plan {
        /// The compiler-owned physical or query-local relation.
        scope: crate::names::ScopeId,
        /// Authored relation vocabulary retained when a user-facing access
        /// is redirected through plan scratch. `None` is a compiler-only
        /// direct scratch read.
        authored_name: Option<SqlIdentifier>,
        alias: Option<SqlIdentifier>,
    },
}

impl GroundMention {
    /// A written name with no alias and no marks — what a compiler-built
    /// read of a user-visible relation says. The marks it fixes are not
    /// overridable defaults, they are the shape: only the parser can mark a
    /// mention, because only an author can write `!!` or the slash.
    pub fn named(identifier: QualifiedName) -> Self {
        GroundMention::Named {
            identifier,
            alias: None,
            mutation_target: false,
            passthrough: false,
        }
    }

    /// The same, carrying the `as` the caller wrote.
    pub fn aliased(identifier: QualifiedName, alias: Option<SqlIdentifier>) -> Self {
        GroundMention::Named {
            identifier,
            alias,
            mutation_target: false,
            passthrough: false,
        }
    }

    /// The spelling this mention addresses its relation by, when it
    /// addresses one by spelling at all.
    pub fn identifier(&self) -> Option<&QualifiedName> {
        match self {
            GroundMention::Named { identifier, .. } => Some(identifier),
            GroundMention::Plan { .. } => None,
        }
    }

    /// The `as` written on the mention, if any. Both alternatives can carry
    /// one: a redirected plan read keeps the user's own alias.
    pub fn alias(&self) -> Option<&SqlIdentifier> {
        match self {
            GroundMention::Named { alias, .. } | GroundMention::Plan { alias, .. } => {
                alias.as_ref()
            }
        }
    }
}

#[cfg(test)]
mod mention_tests {
    use super::super::super::metadata::NamespacePath;
    use super::*;

    fn name(text: &str) -> QualifiedName {
        QualifiedName {
            namespace_path: NamespacePath::empty(),
            name: text.into(),
        }
    }

    /// A scope only the registry can mint — the point of the plan mention is
    /// that its lookup key is one of these and not characters.
    fn scratch() -> crate::names::ScopeId {
        crate::names::Registry::new(&[]).mint_scope(
            crate::names::ScopeOrigin::AnonRelation,
            crate::names::Hint::None,
            None,
        )
    }

    /// A plan read addresses its relation by identity, so there is no
    /// spelling to hand back — and asking is how a caller learns that,
    /// rather than by matching the alternative itself.
    #[test]
    fn only_a_named_mention_answers_with_a_spelling() {
        assert_eq!(
            GroundMention::named(name("users"))
                .identifier()
                .map(|q| q.name.to_string()),
            Some("users".to_string())
        );
        assert!(GroundMention::Plan {
            scope: scratch(),
            authored_name: Some("valid".into()),
            alias: None,
        }
        .identifier()
        .is_none());
    }

    /// BOTH alternatives carry an alias: a user-facing access redirected
    /// through plan scratch keeps the `as` its author wrote, and the
    /// snapshot substitution relies on that.
    #[test]
    fn both_mentions_carry_the_authored_alias() {
        assert_eq!(
            GroundMention::aliased(name("users"), Some("u".into()))
                .alias()
                .map(ToString::to_string),
            Some("u".to_string())
        );
        assert_eq!(
            GroundMention::Plan {
                scope: scratch(),
                authored_name: Some("valid".into()),
                alias: Some("v".into()),
            }
            .alias()
            .map(ToString::to_string),
            Some("v".to_string())
        );
    }
}

/// Base relations - sources of data
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum Relation<P: Phase = Unresolved> {
    /// A ground read: `users(*)`, `orders(id, total)`.
    ///
    /// One carrier for the whole kind. Before resolution the mention says
    /// how the relation was addressed; after it, the mention is spent and
    /// `cpr_schema` is the only thing that answers for the relation — so a
    /// resolved ground read cannot be re-decided from characters, and there
    /// is no second variant for the post-resolution shape to drift into.
    #[lispy("relation:ground")]
    Ground {
        mention: P::Mention,
        outer: bool,
        cpr_schema: P::Scope,
    },
    /// Every named relational callable, including TVFs and higher-order
    /// applications, uses the same call payload as scalar positions.
    ///
    /// The relation it publishes is carried HERE, not on the call. A call
    /// in scalar position publishes no relation, and the shared payload is
    /// the same payload — so a field only one position can fill belongs to
    /// that position, where the surrounding node already draws the
    /// scalar/relational fence.
    #[lispy("relation:functor_call")]
    FunctorCall {
        call: SealedCall<P>,
        /// The name this relational read answers to. A callable relation is
        /// named where it STANDS, exactly as a ground read is; call identity
        /// is the same whether the site names its result or not.
        ///
        /// Spent at resolution, where the read's scope was minted answering
        /// to it — so a resolved tree carries no relational alias beside the
        /// scope that already answers for it.
        alias: P::StageName,
        cpr_schema: P::Scope,
    },
    /// INNER-RELATION (aka SNEAKY-PARENTHESES): table(|> pipeline) or table(, correlation |> pipeline)
    /// Derived tables with semantic pattern classification
    #[lispy("relation:inner")]
    InnerRelation {
        pattern: InnerRelationPattern<P>,
        /// A structural output occurrence allocated by an internal producer.
        /// Authored inner relations leave this empty and resolve normally.
        preminted_scope: Option<crate::names::ScopeId>,
        alias: Option<SqlIdentifier>,
        outer: bool,
        cpr_schema: P::Scope,
    },
    /// Consulted view expansion: view body inlined as a subquery.
    /// Holds a full Query (not just a chain) to support CTEs in view definitions.
    /// Created by the resolver when expanding `consult!`/`enlist!` view references.
    ///
    /// The expansion IS where the authored view name is spent: `scoped` is
    /// the boundary the body publishes, and every reference through the name
    /// was already answered against it. A second carrier holding the
    /// spelling beside that boundary is free to disagree with it.
    #[lispy("relation:consulted-view")]
    ConsultedView {
        body: Box<super::super::Query<P>>,
        scoped: P::Consulted,
        outer: bool,
    },
}

impl<P: Phase> Relation<P> {
    /// Whether this relation is a MENTION: a read whose own access stands as
    /// the first continuation of the chain it heads.
    ///
    /// A derived table and a consulted expansion spent their access where
    /// they were built — the interior said what the read asks for — so an
    /// access standing after one is a step on its result, not its read.
    pub fn takes_an_access(&self) -> bool {
        match self {
            Relation::Ground { .. } | Relation::FunctorCall { .. } => true,
            Relation::InnerRelation { .. } | Relation::ConsultedView { .. } => false,
        }
    }
}

/// The post-resolution phases: the mention is spent and the scope answers
/// for the relation.
impl<P: Phase<Mention = (), Scope = crate::names::ScopeId>> Relation<P> {
    /// A ground relation as the resolver produces one: a scope, addressed by
    /// nothing else.
    pub fn ground(outer: bool, cpr_schema: crate::names::ScopeId) -> Self {
        Relation::Ground {
            mention: (),
            outer,
            cpr_schema,
        }
    }

    /// A ground READ: the relation, and the access it was read under
    /// standing where every consumer looks for it.
    pub fn ground_read(
        access: Access<P>,
        outer: bool,
        cpr_schema: crate::names::ScopeId,
    ) -> Chain<P> {
        Chain::relation(Relation::ground(outer, cpr_schema))
            .then(Continuation::Access { access, cpr_schema })
    }
}
