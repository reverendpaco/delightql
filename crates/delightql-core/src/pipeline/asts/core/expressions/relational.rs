// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Base relations and the derived-table patterns
//! Relation, InnerRelationPattern
//!
//! A relation is a chain HEAD (`GroundForm::Reference`). The operators that
//! consume it live in `chain.rs`; nothing here nests a source.

use super::super::{Phase, Unresolved};
use super::chain::Chain;
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

impl<P: Phase> InnerRelationPattern<P> {
    /// REBUILD THE CHAIN THIS PATTERN IS A DERIVED TABLE OF.
    ///
    /// Every classification wraps exactly one, so a walk that rebuilds the
    /// subquery reaches it here rather than re-listing the four variants at
    /// each pass. The rewrite is handed the OPERAND alone and the
    /// classification is rebuilt around it, so a rebuild of the inside
    /// never moves what the derived table is.
    pub fn rebuilding_subquery(
        self,
        nested: impl FnOnce(Chain<P>) -> crate::error::Result<Chain<P>>,
    ) -> crate::error::Result<Self> {
        Ok(match self {
            InnerRelationPattern::Indeterminate {
                identifier,
                subquery,
            } => InnerRelationPattern::Indeterminate {
                identifier,
                subquery: Box::new(nested(*subquery)?),
            },
            InnerRelationPattern::UncorrelatedDerivedTable {
                identifier,
                subquery,
                is_consulted_view,
            } => InnerRelationPattern::UncorrelatedDerivedTable {
                identifier,
                subquery: Box::new(nested(*subquery)?),
                is_consulted_view,
            },
            InnerRelationPattern::CorrelatedScalarJoin {
                identifier,
                correlation_filters,
                subquery,
            } => InnerRelationPattern::CorrelatedScalarJoin {
                identifier,
                correlation_filters,
                subquery: Box::new(nested(*subquery)?),
            },
            InnerRelationPattern::CorrelatedGroupJoin {
                identifier,
                correlation_filters,
                aggregations,
                subquery,
            } => InnerRelationPattern::CorrelatedGroupJoin {
                identifier,
                correlation_filters,
                aggregations,
                subquery: Box::new(nested(*subquery)?),
            },
        })
    }
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
    /// A scratch row the plan allocated, read by the receipt of that
    /// allocation. Its heading is published before the read resolves and
    /// no character-bearing lookup key participates: it is a MENTION and
    /// not a relation of its own because what it names is a ground read
    /// like any other, differing only in what resolution has to do to find
    /// it, which is nothing. It carries no spelling — nothing authored
    /// addresses it, so nothing qualifies by it.
    #[lispy("mention:scratch")]
    Scratch { row: crate::relation::ScratchRow },
    /// A scratch row standing where the author wrote a name: a directive's
    /// receipt under its `as`, a mutation target read through its own
    /// snapshot. The name and the row were paired by the plan that placed
    /// the row there, and the read is an authored access under that name;
    /// the alias is the author's own on the same read.
    #[lispy("mention:receipt")]
    Receipt {
        receipt: crate::relation::NamedScratch,
        alias: Option<SqlIdentifier>,
    },
    /// A query-local structural landing whose body has not resolved yet.
    /// Resolution replaces this token with the authority-built carrier;
    /// it can never survive into a resolved phase.
    #[lispy("mention:structural")]
    Structural {
        pending: crate::relation::StructuralRelation,
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
            GroundMention::Scratch { .. }
            | GroundMention::Receipt { .. }
            | GroundMention::Structural { .. } => None,
        }
    }

    /// The `as` written on the mention, if any. A redirected receipt read
    /// keeps the user's own alias; a scratch read has nothing written on
    /// it.
    pub fn alias(&self) -> Option<&SqlIdentifier> {
        match self {
            GroundMention::Named { alias, .. }
            | GroundMention::Receipt { alias, .. }
            | GroundMention::Structural { alias, .. } => alias.as_ref(),
            GroundMention::Scratch { .. } => None,
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

    /// A scratch row only the authority can allocate — the point of the
    /// scratch and receipt mentions is that their lookup key is its
    /// receipt and not characters.
    fn scratch() -> crate::relation::ScratchRow {
        crate::relation::any_scratch(&crate::names::Registry::new(&[]))
    }

    /// A receipt read addresses its row by the receipt, so there is no
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
        assert!(GroundMention::Receipt {
            receipt: crate::relation::NamedScratch::for_test(scratch(), "valid".into()),
            alias: None,
        }
        .identifier()
        .is_none());
        assert!(GroundMention::Scratch { row: scratch() }
            .identifier()
            .is_none());
    }

    /// A named mention and a receipt read carry an alias: a user-facing
    /// access redirected through its snapshot keeps the `as` its author
    /// wrote, and the snapshot substitution relies on that. A scratch read
    /// has nothing written on it.
    #[test]
    fn named_and_receipt_mentions_carry_the_authored_alias() {
        assert_eq!(
            GroundMention::aliased(name("users"), Some("u".into()))
                .alias()
                .map(ToString::to_string),
            Some("u".to_string())
        );
        assert_eq!(
            GroundMention::Receipt {
                receipt: crate::relation::NamedScratch::for_test(scratch(), "valid".into()),
                alias: Some("v".into()),
            }
            .alias()
            .map(ToString::to_string),
            Some("v".to_string())
        );
        assert!(GroundMention::Scratch { row: scratch() }.alias().is_none());
    }
}

/// Base relations - sources of data
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum Relation<P: Phase = Unresolved> {
    /// A ground read: `users(*)`, `orders(id, total)`.
    ///
    /// One carrier for the whole kind. Before resolution the mention says
    /// how the relation was addressed; after it the mention is spent and the
    /// HEAD's own result is the only thing that answers for the relation —
    /// so a resolved ground read cannot be re-decided from characters, and
    /// there is no second variant for the post-resolution shape to drift
    /// into.
    #[lispy("relation:ground")]
    Ground { mention: P::Mention, outer: bool },
    /// Every named relational callable, including TVFs and higher-order
    /// applications, uses the same call payload as scalar positions.
    ///
    /// The relation it publishes is the HEAD's, not the call's. A call in
    /// scalar position publishes no relation, and the shared payload is the
    /// same payload — so the one position that publishes is where the pair
    /// lives.
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
    },
    /// INNER-RELATION (aka SNEAKY-PARENTHESES): table(|> pipeline) or table(, correlation |> pipeline)
    /// Derived tables with semantic pattern classification
    #[lispy("relation:inner")]
    InnerRelation {
        pattern: InnerRelationPattern<P>,
        alias: Option<SqlIdentifier>,
        outer: bool,
    },
    /// Consulted view expansion: view body inlined as a subquery.
    /// Holds a full Query (not just a chain) to support CTEs in view definitions.
    /// Created by the resolver when expanding `consult!`/`enlist!` view references.
    ///
    /// The expansion IS where the authored view name is spent: the head's
    /// result is the boundary the body publishes, and every reference
    /// through the name was already answered against it. A second carrier
    /// holding the spelling beside that boundary is free to disagree with
    /// it.
    #[lispy("relation:consulted-view")]
    ConsultedView {
        body: Box<super::super::Query<P>>,
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

    /// MARK THIS RELATION AN OUTER-JOIN OPERAND.
    ///
    /// Outerness says how the relation is JOINED, not what it publishes:
    /// every row it produces is a row it produced, and the join law decides
    /// which of them survive. So this reaches the ORIENTATION FIELD and has
    /// no spelling for anything else — a head cannot become a different
    /// relation through it.
    pub fn mark_outer(&mut self, orientation: bool) {
        match self {
            Relation::Ground { outer, .. }
            | Relation::InnerRelation { outer, .. }
            | Relation::ConsultedView { outer, .. } => *outer = orientation,
            Relation::FunctorCall { .. } => {}
        }
    }

    /// REBUILD THE RELATIONAL OPERAND STANDING INSIDE THIS RELATION.
    ///
    /// A derived table is a derived table OF a chain, and that chain is the
    /// only thing here a rebuild may reach. A ground read and a call name
    /// something rather than containing it; a consulted expansion carries a
    /// whole query, which travels the query road. Those three stand
    /// unchanged.
    pub fn rebuilding_nested(
        self,
        nested: impl FnOnce(Chain<P>) -> crate::error::Result<Chain<P>>,
    ) -> crate::error::Result<Self> {
        match self {
            Relation::InnerRelation {
                pattern,
                alias,
                outer,
            } => Ok(Relation::InnerRelation {
                pattern: pattern.rebuilding_subquery(nested)?,
                alias,
                outer,
            }),
            named @ (Relation::Ground { .. }
            | Relation::FunctorCall { .. }
            | Relation::ConsultedView { .. }) => Ok(named),
        }
    }
}

/// The post-resolution phases: the mention is spent and the head's own
/// result answers for the relation.
impl<P: Phase<Mention = (), Scope = crate::relation::SemanticRelation>> Relation<P> {
    /// A ground relation as the resolver produces one: addressed by nothing
    /// else, because the mention is spent.
    pub fn ground(outer: bool) -> Self {
        Relation::Ground { mention: (), outer }
    }
}
