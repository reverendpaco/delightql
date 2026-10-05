// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

use crate::diagnostic::Semantic;
use crate::error::{DelightQLError, Result};
use crate::lispy::ToLispy;
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::asts::core::{Chain, Phase};
use crate::pipeline::sql_ast::QueryExpression;
use crate::pipeline::ast_unresolved;

/// The fixpoint flavor a head AUTHORS.
///
/// THE BADGE CHOOSES THE UNION: an unbadged head authors the BAG fixpoint,
/// `%` authors the DEDUPLICATING one. Absence claims nothing, which is why
/// an unbadged non-recursive definition carries `Bag` and is lawful; a `%`
/// on a target with no self-reference is a false claim. On its own this type
/// is a claim, not a capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Fixpoint {
    /// Unbadged — clauses accumulate with `UNION ALL`; multiplicity
    /// accumulates and termination is the author's burden.
    Bag,
    /// `%` — clauses accumulate with `UNION`; THE FRONTIER IS THE NEW, and
    /// cyclic data terminates by construction.
    Deduplicating,
}

impl Fixpoint {
    /// The authored badge, from whether the head wore `%`.
    pub fn from_badge(badged: bool) -> Fixpoint {
        if badged {
            Fixpoint::Deduplicating
        } else {
            Fixpoint::Bag
        }
    }

    pub fn is_badged(self) -> bool {
        matches!(self, Fixpoint::Deduplicating)
    }

    /// How the badge is spelled, for a refusal that has to quote it.
    pub fn spelling(self) -> &'static str {
        match self {
            Fixpoint::Bag => "unbadged",
            Fixpoint::Deduplicating => "`%`-badged",
        }
    }
}

impl crate::lispy::ToLispy for Fixpoint {
    fn to_lispy(&self) -> String {
        match self {
            Fixpoint::Bag => "bag".to_string(),
            Fixpoint::Deduplicating => "deduplicating".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct DeduplicatingFixpoint(());

/// HOW A FIXPOINT'S CLAUSES ACCUMULATE.
///
/// The only thing that spells `UNION` anywhere in the compiler, and it never
/// leaves this module as a value: it is a private field of the fixpoint
/// bodies below, so there is no accumulation anyone can hold, place, move,
/// or attach to parts of their own choosing.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Accumulation {
    /// `UNION ALL` — multiplicity accumulates.
    Bag,
    /// `UNION` — THE FRONTIER IS THE NEW.
    Deduplicating(DeduplicatingFixpoint),
}

impl ToLispy for Accumulation {
    fn to_lispy(&self) -> String {
        match self {
            Accumulation::Bag => "bag".to_string(),
            Accumulation::Deduplicating(_) => "deduplicating".to_string(),
        }
    }
}

impl Accumulation {
    /// The keyword this accumulation joins its members with.
    fn keyword(&self) -> &'static str {
        match self {
            Accumulation::Bag => "UNION ALL",
            Accumulation::Deduplicating(_) => "UNION",
        }
    }
}

/// A CTE BINDING — the ONE atom a definition becomes.
///
/// An ordinary authored clause carries no decided relationship, so `authored`
/// builds that state freely.
#[derive(Debug, Clone, PartialEq)]
pub struct CteBinding<P: Phase = crate::pipeline::asts::core::Unresolved> {
    state: P::CteBindingState,
}

impl<P: Phase> CteBinding<P> {
    pub fn body(&self) -> &P::CteBody {
        self.state.body()
    }

    pub fn authority(&self) -> &P::CteAuthority {
        self.state.authority()
    }

    /// Every chain this binding holds, in emission order. READ-ONLY.
    pub fn parts(&self) -> Vec<&Chain<P>> {
        self.state.parts()
    }

    /// CROSS A PHASE BOUNDARY AS ONE BINDING.
    ///
    /// The chains fold and NOTHING ELSE IS ASKED. The walk is handed each
    /// chain and hands one back; it is never handed the subject, so it has
    /// nothing to answer about what this binding stands on and no way to
    /// return another binding's. The crossing is performed by the body's own
    /// carrier, which is the thing that knows which shape this phase has.
    pub(crate) fn folded<Q: Phase, F: AstTransform<P, Q> + ?Sized>(
        self,
        walk: &mut F,
    ) -> Result<CteBinding<Q>> {
        self.state.folded(walk)
    }
}

impl<P: Phase> ToLispy for CteBinding<P> {
    fn to_lispy(&self) -> String {
        format!(
            "(cte_binding (body {}) (subject {}) (authority {}))",
            self.body().to_lispy(),
            self.state.subject_lispy(),
            self.authority().to_lispy(),
        )
    }
}

pub trait CteBindingState<P: Phase>: Clone + std::fmt::Debug + PartialEq + Sized {
    fn body(&self) -> &P::CteBody;
    fn authority(&self) -> &P::CteAuthority;
    fn subject_lispy(&self) -> String;
    fn parts(&self) -> Vec<&Chain<P>>;
    fn folded<Q: Phase, F: AstTransform<P, Q> + ?Sized>(
        self,
        walk: &mut F,
    ) -> Result<CteBinding<Q>>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnresolvedBindingState(UnresolvedBinding);

#[derive(Debug, Clone, PartialEq)]
enum UnresolvedBinding {
    Ordinary {
        body: ast_unresolved::Chain,
        subject: crate::pipeline::asts::core::AuthoredCteSubject,
        authority: crate::pipeline::asts::core::CteAuthority,
    },
}

/// An authored binding: one clause, a spelling, and the judgments resolution
/// will spend. Nothing here has been decided, so nothing here needs a fence.
impl CteBinding<crate::pipeline::asts::core::Unresolved> {
    pub fn authored(
        body: ast_unresolved::Chain,
        subject: crate::pipeline::asts::core::AuthoredCteSubject,
        authority: crate::pipeline::asts::core::CteAuthority,
    ) -> Self {
        CteBinding {
            state: UnresolvedBindingState(UnresolvedBinding::Ordinary {
                body,
                subject,
                authority,
            }),
        }
    }

    /// Attach the authored declaration horizon while the let-block
    /// constructor still owns the binding.
    pub(crate) fn with_horizon(
        mut self,
        horizon: crate::pipeline::asts::core::LexicalHorizon,
    ) -> Self {
        let UnresolvedBinding::Ordinary { authority, .. } = &mut self.state.0;
        authority.horizon = horizon;
        self
    }

    pub fn subject(&self) -> crate::pipeline::asts::core::CteSubjectView<'_> {
        fn authored_view(
            subject: &crate::pipeline::asts::core::AuthoredCteSubject,
        ) -> crate::pipeline::asts::core::CteSubjectView<'_> {
            match subject {
                crate::pipeline::asts::core::AuthoredCteSubject::Authored { name, effect } => {
                    crate::pipeline::asts::core::CteSubjectView::Authored { name, effect }
                }
                crate::pipeline::asts::core::AuthoredCteSubject::Generated { name } => {
                    crate::pipeline::asts::core::CteSubjectView::Generated { name }
                }
            }
        }
        match &self.state.0 {
            UnresolvedBinding::Ordinary { subject, .. } => authored_view(subject),
        }
    }

    pub(in crate::pipeline) fn projected_through_head(
        self,
        items: &[crate::pipeline::asts::core::definitions::HeadItem],
        canonical_names: &[delightql_types::SqlIdentifier],
    ) -> Self {
        CteBinding {
            state: UnresolvedBindingState(match self.state.0 {
                UnresolvedBinding::Ordinary {
                    body,
                    subject,
                    mut authority,
                } => {
                    let body = crate::pipeline::asts::core::definitions::project_body_through_head(
                        body,
                        items,
                        canonical_names,
                    );
                    authority.head = authority.head.spent();
                    UnresolvedBinding::Ordinary {
                        body,
                        subject,
                        authority,
                    }
                }
            }),
        }
    }
}

/// One unresolved binding crossing a generic AST walk. Its fields stay
/// private, so the walk can replace only the body and cannot recover or move
/// frontier evidence.
pub struct AuthoredBinding<P: Phase> {
    body: Chain<P>,
    subject: crate::pipeline::asts::core::AuthoredCteSubject,
    authority: crate::pipeline::asts::core::CteAuthority,
}

impl AuthoredBinding<crate::pipeline::asts::core::Unresolved> {
    pub(in crate::pipeline) fn into_binding(
        self,
    ) -> CteBinding<crate::pipeline::asts::core::Unresolved> {
        CteBinding {
            state: UnresolvedBindingState(UnresolvedBinding::Ordinary {
                body: self.body,
                subject: self.subject,
                authority: self.authority,
            }),
        }
    }
}

impl CteBindingState<crate::pipeline::asts::core::Unresolved> for UnresolvedBindingState {
    fn body(&self) -> &ast_unresolved::Chain {
        match &self.0 {
            UnresolvedBinding::Ordinary { body, .. } => body,
        }
    }

    fn authority(&self) -> &crate::pipeline::asts::core::CteAuthority {
        match &self.0 {
            UnresolvedBinding::Ordinary { authority, .. } => authority,
        }
    }

    fn subject_lispy(&self) -> String {
        match &self.0 {
            UnresolvedBinding::Ordinary { subject, .. } => subject.to_lispy(),
        }
    }

    fn parts(&self) -> Vec<&ast_unresolved::Chain> {
        match &self.0 {
            UnresolvedBinding::Ordinary { body, .. } => vec![body],
        }
    }

    fn folded<Q: Phase, F: AstTransform<crate::pipeline::asts::core::Unresolved, Q> + ?Sized>(
        self,
        walk: &mut F,
    ) -> Result<CteBinding<Q>> {
        match self.0 {
            UnresolvedBinding::Ordinary {
                body,
                subject,
                authority,
            } => {
                let body = walk.transform_relational_action(body)?.into_inner();
                Q::cte_binding_of_authored(AuthoredBinding {
                    body,
                    subject,
                    authority,
                })
            }
        }
    }
}

/// A FIXPOINT'S BODY IN SQL — the same facts, lowered together, plus the
/// scope the binding they were decided for is bound at.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlFixpoint {
    /// The binding this body was decided FOR. It is carried, not chosen
    /// beside: `Cte::fixpoint` reads the scope off the body rather than
    /// taking one, so a recursive body cannot be bound anywhere else.
    scope: crate::names::ScopeId,
    accumulation: Accumulation,
    anchor: QueryExpression,
    members: Vec<QueryExpression>,
}

impl SqlFixpoint {
    /// The scope this fixpoint is bound at.
    pub fn scope(&self) -> crate::names::ScopeId {
        self.scope
    }

    /// The keyword this fixpoint joins its members with.
    pub fn keyword(&self) -> &'static str {
        self.accumulation.keyword()
    }

    /// Whether the accumulation DEDUPLICATES (`UNION`): every position of
    /// every part is then compared for equality across the whole fixpoint.
    pub fn is_deduplicating(&self) -> bool {
        matches!(self.accumulation, Accumulation::Deduplicating(_))
    }

    /// Every query this body holds, anchor first — the order it emits in.
    pub fn parts(&self) -> Vec<&QueryExpression> {
        std::iter::once(&self.anchor).chain(&self.members).collect()
    }

    /// The same, to rewrite in place. A pass that transforms a fixpoint
    /// transforms its parts; the accumulation is not something a rewrite
    /// gets to re-answer, and there is no field here for it to reach.
    pub fn parts_mut(&mut self) -> Vec<&mut QueryExpression> {
        std::iter::once(&mut self.anchor)
            .chain(self.members.iter_mut())
            .collect()
    }

    /// What this fixpoint unfolds FROM.
    pub fn anchor(&self) -> &QueryExpression {
        &self.anchor
    }

    /// The recursive members alone, for a reader that has already accounted
    /// for the anchor.
    pub fn members(&self) -> &[QueryExpression] {
        &self.members
    }

    /// A BAG fixpoint body, for fixtures that need a recursive SQL CTE
    /// without a compilation behind them. Test-only, and bag-only: there is
    /// no deduplicating road here either.
    #[cfg(test)]
    pub(crate) fn bag_fixture(
        scope: crate::names::ScopeId,
        anchor: QueryExpression,
        members: Vec<QueryExpression>,
    ) -> Self {
        SqlFixpoint {
            scope,
            accumulation: Accumulation::Bag,
            anchor,
            members,
        }
    }

    /// A fixpoint body from parts whose recursion verdict was decided
    /// elsewhere, together with its scope and accumulation.
    pub(crate) fn from_parts(
        scope: crate::names::ScopeId,
        deduplicating: bool,
        anchor: QueryExpression,
        members: Vec<QueryExpression>,
    ) -> Self {
        let accumulation = if deduplicating {
            Accumulation::Deduplicating(DeduplicatingFixpoint(()))
        } else {
            Accumulation::Bag
        };
        SqlFixpoint {
            scope,
            accumulation,
            anchor,
            members,
        }
    }
}

/// What a phase's CTE body can expose to read-only AST walks.
pub trait CteBodyCarrier<P: Phase>: Clone + std::fmt::Debug + PartialEq + ToLispy + Sized {}

impl CteBodyCarrier<crate::pipeline::asts::core::Unresolved>
    for Chain<crate::pipeline::asts::core::Unresolved>
{
}

/// The refusal of a name two query-local binding kinds both declare.
pub(crate) fn one_query_local_name(name: &delightql_types::SqlIdentifier) -> DelightQLError {
    DelightQLError::from(Semantic::ScopeDuplicate {
        message: format!(
            "'{name}' is declared twice in this query's bindings: a common table \
             expression, a common function expression and a common higher-order \
             expression share one query-local name space"
        ),
    })
}

