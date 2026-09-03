// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE BINDING AUTHORITY — what a CTE binding IS, at every phase, and the
//! recursion decision that makes one a fixpoint.
//!
//! ONE AUTHORED CLAUSE GROUP IN, ONE DECIDED BINDING OUT.
//! `resolve_definition_group` resolves each clause of a definition itself,
//! reads the badge off those same clauses, walks their resolved bodies for a
//! self-reference, and hands back the finished `CteBinding`. Nothing outside
//! supplies a body, a badge or a subject, so there is no pairing anything
//! else could get wrong.
//!
//! THE DECISION IS THE BODY, AND THE BODY IS THE BINDING'S. Neither is stored
//! beside something for a later phase to carry, re-read, or re-pair.
//! `RecursionState` never leaves this module; what leaves is a `CteBinding`
//! whose subject and `DefinitionBody` are private fields minted together.
//! Past resolution there is NO constructor for one at all — every road out of
//! this module preserves what a binding stands on and what its body is:
//!
//! - `folded`, `refined`, `map_chains` rewrite the chains and nothing else;
//! - `parts` / `parts_mut` reach the chains and never the variant;
//! - `into_sql` consumes the whole binding and takes the SQL scope from THIS
//!   binding's own subject, so no scope is ever chosen beside a body.
//!
//! `UNION` IS NOT A VALUE ANYTHING CAN HOLD. There is no deduplicating set
//! operator in either AST, and no accumulation value anywhere: `Accumulation`
//! is private here and lives only as a field of the two fixpoint bodies,
//! `FixpointBody` and `SqlFixpoint`. Both have private fields and one
//! producer each — `resolve_definition_group` and `CteBinding::into_sql` — so
//! outside this module a body cannot be told it is a fixpoint, a fixpoint
//! cannot be flattened, and one fixpoint's accumulation cannot be given
//! another's parts or another's scope. `SqlFixpoint` carries the scope it was
//! decided for and `sql_ast::Cte::fixpoint` reads it off the body rather than
//! taking one, so a recursive SQL CTE cannot be rebound either.
//!
//! Before resolution, ordinary authored bindings carry no decided
//! relationship and `CteBinding::authored` builds those freely. A recursive
//! frontier is different: its body and live instance evidence enter through
//! one opaque carrier and remain private until resolution spends them.
//!
//! The four lowering roots cannot even reach the decision — `relation::
//! fences::lowering_holds_no_construction_capability` holds that none of
//! them may so much as name `Planning`.

use crate::error::{DelightQLError, Result};
use crate::lispy::ToLispy;
use crate::names::ScopeId;
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::ast_visit::{walk_visit_relational, AstVisit, Descent};
use crate::pipeline::asts::core::{Chain, Phase};
use crate::pipeline::resolver::helpers::extraction::transform_schema_table_names;
use crate::pipeline::resolver::{apply_group_head, clauses_publish_one_heading, CteResolver};
use crate::pipeline::sql_ast::QueryExpression;
use crate::pipeline::{ast_resolved, ast_unresolved, asts::core::Resolved};
use crate::ToLispy;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CteGroupKey {
    Authored(delightql_types::SqlIdentifier),
    Generated(delightql_types::SqlIdentifier),
    Frontier(crate::defuse::FrontierGroup),
}

/// The fixpoint flavor a head AUTHORS.
///
/// THE BADGE CHOOSES THE UNION: an unbadged head authors the BAG fixpoint,
/// `%` authors the DEDUPLICATING one. Absence claims nothing, which is why
/// an unbadged non-recursive definition carries `Bag` and is lawful; a `%`
/// on a target with no self-reference is a false claim and `decide` refuses
/// it. On its own this type is a claim, not a capability.
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

/// Evidence that THIS module's walk found a self-reference under a
/// deduplicating badge.
///
/// The field is private and there is no constructor outside `decide`, so
/// every carrier that means `UNION` is unwritable without one. It carries
/// nothing further because there is nothing further to check: the
/// accumulation is not a token attached to some node that could be attached
/// to a different one — it is a private field of the body it belongs to, and
/// that body is built whole, once, from this decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct DeduplicatingFixpoint(());

/// The recursion decision, taken once and IMMEDIATELY SPENT into the body
/// it describes.
///
/// THREE OUTCOMES, each stated: a definition is not recursive, or it is a
/// fixpoint in one of the two flavors SQL offers. The flavor is decided at
/// `decide`, from the authored badge and the walk's finding together — no
/// later phase reads a character, a name, or a finished SQL tree to recover
/// it.
///
/// PRIVATE, and short-lived: this value never leaves the module and never
/// travels beside a body. `resolve_definition_group` turns it into the
/// body's own variant in the same breath, so from resolution onward the
/// decision has exactly one carrier and no second description to disagree
/// with.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RecursionState {
    NonRecursive,
    /// The BAG fixpoint: clause accumulation is `UNION ALL`.
    RecursiveBag,
    /// The DEDUPLICATING fixpoint (`%`): clause accumulation is `UNION`.
    /// Its payload is the evidence, so the variant cannot be written
    /// without one.
    RecursiveDeduplicating(DeduplicatingFixpoint),
}

impl RecursionState {
    /// HOW THIS TARGET'S CLAUSES ACCUMULATE — the one road to an
    /// [`Accumulation`], and `None` for a definition that is not a fixpoint
    /// at all.
    ///
    /// PRIVATE, like the type it answers with. The flavor has exactly ONE
    /// description in the compiler — the body built from this decision —
    /// and no phase can hold a second copy of it to disagree with.
    fn accumulation(&self) -> Option<Accumulation> {
        match self {
            RecursionState::NonRecursive => None,
            RecursionState::RecursiveBag => Some(Accumulation::Bag),
            RecursionState::RecursiveDeduplicating(evidence) => {
                Some(Accumulation::Deduplicating(*evidence))
            }
        }
    }
}

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
/// THE SUBJECT AND THE BODY ARE MINTED TOGETHER AND NEVER SEPARABLE. A
/// fixpoint's self-reference was judged against THIS subject, so a body
/// standing under another one is a valid pair of values describing something
/// that was never decided. The fields are private here and there is no bound
/// constructor at all: past resolution, the only way to obtain a binding is
/// to have been given one by `resolve_definition_group`, and the only things
/// that can be done to it preserve the subject and the body's variant.
///
/// An ordinary authored clause carries no decided relationship, so `authored`
/// builds that state freely. A recursive frontier already carries live
/// definition-use evidence and therefore has a separate atomic state.
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
    Frontier(crate::defuse::FrontierCte),
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
        if let UnresolvedBinding::Ordinary { authority, .. } = &mut self.state.0 {
            authority.horizon = horizon;
        }
        self
    }

    /// This binding read in a block whose positions all moved later by
    /// `offset`; only the block's own absorption calls it, in the same act
    /// that moved the claims.
    pub(crate) fn shifted(mut self, offset: usize) -> Self {
        if let UnresolvedBinding::Ordinary { authority, .. } = &mut self.state.0 {
            authority.horizon = authority.horizon.shifted(offset);
        }
        self
    }

    /// Admit the one atomic recursive-frontier value produced by defuse. No
    /// signature accepts a body beside frontier evidence.
    pub(crate) fn frontier(carrier: crate::defuse::FrontierCte) -> Self {
        CteBinding {
            state: UnresolvedBindingState(UnresolvedBinding::Frontier(carrier)),
        }
    }

    pub fn subject(&self) -> crate::pipeline::asts::core::CteSubjectView<'_> {
        match &self.state.0 {
            UnresolvedBinding::Ordinary { subject, .. } => match subject {
                crate::pipeline::asts::core::AuthoredCteSubject::Authored { name, effect } => {
                    crate::pipeline::asts::core::CteSubjectView::Authored { name, effect }
                }
                crate::pipeline::asts::core::AuthoredCteSubject::Generated { name } => {
                    crate::pipeline::asts::core::CteSubjectView::Generated { name }
                }
            },
            UnresolvedBinding::Frontier(_) => crate::pipeline::asts::core::CteSubjectView::Frontier,
        }
    }

    fn group_key(&self) -> CteGroupKey {
        match &self.state.0 {
            UnresolvedBinding::Ordinary { subject, .. } => match subject {
                crate::pipeline::asts::core::AuthoredCteSubject::Authored { name, .. } => {
                    CteGroupKey::Authored(name.clone())
                }
                crate::pipeline::asts::core::AuthoredCteSubject::Generated { name } => {
                    CteGroupKey::Generated(name.clone())
                }
            },
            UnresolvedBinding::Frontier(carrier) => CteGroupKey::Frontier(carrier.group()),
        }
    }

    fn into_resolution(
        self,
    ) -> (
        ast_unresolved::Chain,
        crate::pipeline::asts::core::CteAuthority,
    ) {
        match self.state.0 {
            UnresolvedBinding::Ordinary {
                body, authority, ..
            } => (body, authority),
            UnresolvedBinding::Frontier(carrier) => carrier.into_resolution(),
        }
    }

    /// Rewrite the body while preserving the subject, authority, and any live
    /// frontier evidence as one binding.
    pub(in crate::pipeline) fn map_body(
        self,
        map: impl FnOnce(ast_unresolved::Chain) -> Result<ast_unresolved::Chain>,
    ) -> Result<Self> {
        Ok(CteBinding {
            state: UnresolvedBindingState(match self.state.0 {
                UnresolvedBinding::Ordinary {
                    body,
                    subject,
                    authority,
                } => UnresolvedBinding::Ordinary {
                    body: map(body)?,
                    subject,
                    authority,
                },
                UnresolvedBinding::Frontier(carrier) => {
                    UnresolvedBinding::Frontier(carrier.map_body(map)?)
                }
            }),
        })
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
                    authority.head = crate::pipeline::asts::core::definitions::Head::glob();
                    UnresolvedBinding::Ordinary {
                        body,
                        subject,
                        authority,
                    }
                }
                UnresolvedBinding::Frontier(carrier) => UnresolvedBinding::Frontier(
                    carrier.projected_through_head(items, canonical_names),
                ),
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
            UnresolvedBinding::Frontier(carrier) => carrier.body(),
        }
    }

    fn authority(&self) -> &crate::pipeline::asts::core::CteAuthority {
        match &self.0 {
            UnresolvedBinding::Ordinary { authority, .. } => authority,
            UnresolvedBinding::Frontier(carrier) => carrier.authority(),
        }
    }

    fn subject_lispy(&self) -> String {
        match &self.0 {
            UnresolvedBinding::Ordinary { subject, .. } => subject.to_lispy(),
            UnresolvedBinding::Frontier(carrier) => carrier.subject_lispy(),
        }
    }

    fn parts(&self) -> Vec<&ast_unresolved::Chain> {
        vec![self.body()]
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
            UnresolvedBinding::Frontier(carrier) => {
                Q::cte_binding_of_frontier(carrier.folded(walk)?)
            }
        }
    }
}

/// The phases past resolution: a decided body, a bound subject, and an
/// authority resolution already spent.
pub trait BoundPhase:
    Phase<CteBody = DefinitionBody<Self>, CteAuthority = (), CteBindingState = BoundBindingState<Self>>
{
}

impl BoundPhase for Resolved {}
impl BoundPhase for crate::pipeline::asts::core::Refined {}

/// ONE DECIDED BINDING, IN TRANSIT.
///
/// The two halves of a single binding, taken off it together for a phase
/// crossing. Its fields are private here and its ONE producer is that
/// crossing, so `Phase::cte_binding_of_bound` — the admission that turns
/// halves back into a binding — cannot be reached with halves of two
/// different bindings, or with a body cloned off one and a subject read off
/// another.
pub struct BoundBinding<P: Phase> {
    body: DefinitionBody<P>,
    subject: crate::relation::SemanticRelation,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundBindingState<P: BoundPhase> {
    body: DefinitionBody<P>,
    subject: crate::relation::SemanticRelation,
    authority: (),
}

impl<P: BoundPhase> BoundBinding<P> {
    /// Put the halves back on, at the phase that admitted them.
    pub(in crate::pipeline) fn into_binding(self) -> CteBinding<P> {
        CteBinding {
            state: BoundBindingState {
                body: self.body,
                subject: self.subject,
                authority: (),
            },
        }
    }
}

impl<P: BoundPhase> CteBindingState<P> for BoundBindingState<P> {
    fn body(&self) -> &DefinitionBody<P> {
        &self.body
    }

    fn authority(&self) -> &() {
        &self.authority
    }

    fn subject_lispy(&self) -> String {
        self.subject.to_lispy()
    }

    fn parts(&self) -> Vec<&Chain<P>> {
        self.body.parts()
    }

    fn folded<Q: Phase, F: AstTransform<P, Q> + ?Sized>(
        self,
        walk: &mut F,
    ) -> Result<CteBinding<Q>> {
        let body = self
            .body
            .map_chains(|chain| Ok(walk.transform_relational_action(chain)?.into_inner()))?;
        Q::cte_binding_of_bound(BoundBinding {
            body,
            subject: self.subject,
        })
    }
}

/// A STRUCTURAL CARRIER, BOUND BY THE BINDING AUTHORITY: the row the
/// identity authority derived for it and the definition minted from the
/// same body under that row's relation, as one value. Nothing outside this
/// module pairs a body with a subject; the record that holds carriers
/// takes this value whole.
#[derive(Clone)]
pub(crate) struct BoundCarrier {
    row: crate::relation::CarrierRow,
    binding: ast_resolved::CteBinding,
}

impl BoundCarrier {
    /// The landing and the carrier, as the identity authority bound them.
    pub(crate) fn row(&self) -> crate::relation::CarrierRow {
        self.row
    }

    /// The definition, read where the record that holds it emits it.
    pub(crate) fn binding(&self) -> &ast_resolved::CteBinding {
        &self.binding
    }
}

/// BIND A RESOLVED BODY AS A STRUCTURAL CARRIER, for the carrier
/// authority, which alone constructs the witness. The body is spent: the
/// identity authority reserves the landing and instantiates the body under
/// it in one act, and the definition is minted here from that body under
/// the subject the act derived. There is no landing to supply and no
/// subject to choose, and the product is never taken apart.
pub(crate) fn bind_carrier(
    witness: crate::defuse::carriers::CarrierBind,
    part: crate::relation::form::HoPart,
    body: crate::pipeline::resolver::ResolvedRelation,
    identities: &crate::relation::Planning,
) -> Result<BoundCarrier> {
    let row = identities
        .authority()
        .bind_carrier(witness, part, &body.semantic_relation())?;
    let binding = ast_resolved::CteBinding::bound(
        DefinitionBody::Ordinary(body.into_body()),
        row.relation(),
    );
    Ok(BoundCarrier { row, binding })
}

impl<P: BoundPhase> CteBinding<P> {
    fn bound(body: DefinitionBody<P>, subject: crate::relation::SemanticRelation) -> Self {
        CteBinding {
            state: BoundBindingState {
                body,
                subject,
                authority: (),
            },
        }
    }

    pub fn subject(&self) -> &crate::relation::SemanticRelation {
        &self.state.subject
    }
}

/// A resolved binding, crossing into refinement or being rewritten in place.
impl CteBinding<Resolved> {
    /// Every chain, mutably, for a same-phase pass that edits in place.
    /// Neither the subject nor the body's variant is reachable from here.
    pub(in crate::pipeline) fn parts_mut(&mut self) -> Vec<&mut ast_resolved::Chain> {
        self.state.body.parts_mut()
    }

    /// REFINE EVERY CHAIN, KEEPING THE SUBJECT AND THE VARIANT.
    pub(in crate::pipeline) fn refined<F>(
        self,
        refine: F,
    ) -> Result<CteBinding<crate::pipeline::asts::core::Refined>>
    where
        F: FnMut(ast_resolved::Chain) -> Result<crate::pipeline::ast_refined::Chain>,
    {
        Ok(CteBinding {
            state: BoundBindingState {
                body: self.state.body.map_chains(refine)?,
                subject: self.state.subject,
                authority: (),
            },
        })
    }

    /// The same, staying in this phase.
    pub(in crate::pipeline) fn map_chains<F>(self, map: F) -> Result<Self>
    where
        F: FnMut(ast_resolved::Chain) -> Result<ast_resolved::Chain>,
    {
        Ok(CteBinding {
            state: BoundBindingState {
                body: self.state.body.map_chains(map)?,
                subject: self.state.subject,
                authority: (),
            },
        })
    }
}

/// A refined binding, becoming SQL.
impl CteBinding<crate::pipeline::asts::core::Refined> {
    /// THE ONE TRANSITION from a decided binding to a SQL binding.
    ///
    /// The whole binding is consumed and the scoped `Cte` leaves in one act:
    /// the scope is taken from THIS binding's own subject, and a fixpoint
    /// body carries it onward, so there is no moment at which a body and a
    /// scope are two arguments something could pair differently.
    ///
    /// The caller says only how a chain becomes SQL — never which chain is
    /// the anchor, never whether the result is a fixpoint. `anchor` answers
    /// first and hands `member` whatever the anchor decided the binding
    /// publishes.
    pub(in crate::pipeline) fn into_sql<S, A, M>(
        self,
        anchor: A,
        mut member: M,
    ) -> Result<crate::pipeline::sql_ast::Cte>
    where
        A: FnOnce(crate::pipeline::ast_refined::Chain) -> Result<(QueryExpression, S)>,
        M: FnMut(crate::pipeline::ast_refined::Chain, &S) -> Result<QueryExpression>,
    {
        use crate::pipeline::sql_ast::Cte;
        let scope = self.state.subject.scope();
        match self.state.body {
            DefinitionBody::Ordinary(chain) => Ok(Cte::ordinary(scope, anchor(chain)?.0)),
            DefinitionBody::Fixpoint(fixpoint) => {
                let (query, published) = anchor(fixpoint.anchor)?;
                let mut members = Vec::with_capacity(fixpoint.members.len());
                for clause in fixpoint.members {
                    members.push(member(clause, &published)?);
                }
                Ok(Cte::fixpoint(SqlFixpoint {
                    scope,
                    accumulation: fixpoint.accumulation,
                    anchor: query,
                    members,
                }))
            }
        }
    }
}

/// WHAT A DEFINITION'S BODY IS, at every phase that has one.
///
/// A BINDING IS ITS BODY. There is no decision stored beside a chain for
/// something to pair differently — the decision IS the variant, and the
/// variant travels through refinement and into SQL lowering as one value.
///
/// `Fixpoint`'s payload is [`FixpointBody`], whose fields are private to
/// this module, so no phase, rewrite or fold outside the recursion decision
/// can say a body is a fixpoint, change an ordinary body into one, or move
/// one fixpoint's accumulation onto another body's parts.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum DefinitionBody<P: Phase> {
    Ordinary(Chain<P>),
    Fixpoint(FixpointBody<P>),
}

/// A FIXPOINT'S BODY: what it unfolds FROM, what unfolds it, and how the
/// two accumulate — supplied together by the decision that judged them and
/// never separable back into independently chosen arguments.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub struct FixpointBody<P: Phase> {
    accumulation: Accumulation,
    /// Every base clause. At least one: a fixpoint with no anchor has
    /// nothing to unfold from.
    anchor: Chain<P>,
    /// The remaining clauses, in authored order.
    members: Vec<Chain<P>>,
}

impl<P: Phase> DefinitionBody<P> {
    /// Whether this body is a fixpoint — the decision, read.
    pub fn is_fixpoint(&self) -> bool {
        matches!(self, DefinitionBody::Fixpoint(_))
    }

    /// Every chain this body holds, anchor first.
    ///
    /// READ-ONLY, and that is what makes it harmless: a walk may look at
    /// every part, and looking cannot turn one body into another.
    pub fn parts(&self) -> Vec<&Chain<P>> {
        match self {
            DefinitionBody::Ordinary(chain) => vec![chain],
            DefinitionBody::Fixpoint(fixpoint) => std::iter::once(&fixpoint.anchor)
                .chain(&fixpoint.members)
                .collect(),
        }
    }

    /// The same parts, mutably. A pass may rewrite what a body holds; the
    /// variant and the accumulation are not reachable from here.
    pub(in crate::pipeline) fn parts_mut(&mut self) -> Vec<&mut Chain<P>> {
        match self {
            DefinitionBody::Ordinary(chain) => vec![chain],
            DefinitionBody::Fixpoint(fixpoint) => std::iter::once(&mut fixpoint.anchor)
                .chain(fixpoint.members.iter_mut())
                .collect(),
        }
    }

    /// REWRITE EVERY CHAIN, KEEPING THE VARIANT.
    ///
    /// The one shape-preserving map over a body. A caller says what to do
    /// with a chain, never which chain is the anchor and never whether the
    /// result is a fixpoint — so a rewrite cannot make an ordinary body
    /// recursive, cannot flatten a fixpoint, and cannot move one body's
    /// accumulation onto another's parts.
    pub(in crate::pipeline) fn map_chains<Q: Phase, F>(
        self,
        mut map: F,
    ) -> Result<DefinitionBody<Q>>
    where
        F: FnMut(Chain<P>) -> Result<Chain<Q>>,
    {
        Ok(match self {
            DefinitionBody::Ordinary(chain) => DefinitionBody::Ordinary(map(chain)?),
            DefinitionBody::Fixpoint(FixpointBody {
                accumulation,
                anchor,
                members,
            }) => DefinitionBody::Fixpoint(FixpointBody {
                accumulation,
                anchor: map(anchor)?,
                members: members.into_iter().map(map).collect::<Result<Vec<_>>>()?,
            }),
        })
    }
}

/// A FIXPOINT'S BODY IN SQL — the same facts, lowered together, plus the
/// scope the binding they were decided for is bound at.
///
/// Its fields are private here and its ONE producer is
/// [`CteBinding::into_sql`], so a recursive SQL CTE cannot be assembled from
/// an accumulation, parts and a scope a caller chose separately, and an
/// ordinary one cannot become recursive by assignment.
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
}

/// What a phase's CTE body can expose to read-only AST walks.
pub trait CteBodyCarrier<P: Phase>: Clone + std::fmt::Debug + PartialEq + ToLispy + Sized {}

impl CteBodyCarrier<crate::pipeline::asts::core::Unresolved>
    for Chain<crate::pipeline::asts::core::Unresolved>
{
}

impl<P: BoundPhase> CteBodyCarrier<P> for DefinitionBody<P> {}

/// Group authored clauses by their subject law and resolve each group in
/// first-appearance order. Both the grouping key and the operation that
/// spends it are private to this authority: a frontier binding cannot leave
/// here as separately movable body and instance evidence.
fn group_ctes(
    ctes: Vec<ast_unresolved::CteBinding>,
) -> Result<(
    HashMap<CteGroupKey, Vec<ast_unresolved::CteBinding>>,
    Vec<CteGroupKey>,
)> {
    use crate::pipeline::asts::core::CteSubjectView;

    let mut authored_groups: HashMap<
        delightql_types::SqlIdentifier,
        Vec<ast_unresolved::CteBinding>,
    > = HashMap::new();
    let mut authored_order = Vec::new();
    for cte in &ctes {
        let CteSubjectView::Authored { name, .. } = cte.subject() else {
            continue;
        };
        let is_new = !authored_groups.contains_key(name);
        authored_groups
            .entry(name.clone())
            .or_default()
            .push(cte.clone());
        if is_new {
            authored_order.push(name.clone());
        }
    }
    crate::pipeline::resolver::cte_validation::validate_grouped_cte_dependencies(
        &authored_groups,
        &authored_order,
    )?;

    let mut groups = HashMap::new();
    let mut order = Vec::new();
    for cte in ctes {
        let key = cte.group_key();
        let is_new = !groups.contains_key(&key);
        groups.entry(key.clone()).or_insert_with(Vec::new).push(cte);
        if is_new {
            order.push(key);
        }
    }
    Ok((groups, order))
}

/// The refusal of a name two query-local binding kinds both declare.
pub(crate) fn one_query_local_name(name: &delightql_types::SqlIdentifier) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        crate::uri_registry::subcat::SCOPE_DUPLICATE,
        format!(
            "'{name}' is declared twice in this query's bindings: a common table \
             expression, a common function expression and a common higher-order \
             expression share one query-local name space"
        ),
        "give each binding its own name",
    )
}

/// Resolve and register a flat CTE list without exposing the grouping carrier
/// or accepting a caller-selected frontier key beside a binding.
pub(crate) fn resolve_cte_bindings(
    ctes: Vec<ast_unresolved::CteBinding>,
    resolver: &mut dyn CteResolver,
) -> Result<Vec<ast_resolved::CteBinding>> {
    let (mut groups, order) = group_ctes(ctes)?;
    let mut resolved = Vec::with_capacity(order.len());
    for key in order {
        let group = groups
            .remove(&key)
            .expect("ordered CTE group was created by the exhaustive grouping pass");
        resolved.push(resolve_definition_group(&key, group, resolver)?);
    }
    Ok(resolved)
}

/// ONE AUTHORED CLAUSE GROUP IN, ONE RESOLVED BINDING OUT.
///
/// THE WHOLE TRANSITION IS HERE. The badge is read off the clauses this
/// function itself resolves, the self-reference walk runs over the bodies
/// those SAME clauses became, and the finished binding — body, subject and
/// decision — leaves as one value. Nothing outside supplies a body, a badge
/// or a subject, so there is no pairing for a caller to get wrong: what a
/// caller supplies is a resolution CAPABILITY (`CteResolver`) and the
/// authored clauses.
///
/// The scope interleaving that only resolution knows stays here too: the
/// anchor is registered after the first clause, so a later clause's
/// self-reference resolves to it and the walk below has something to find.
fn register_named_cte(
    resolver: &mut dyn CteResolver,
    key: &CteGroupKey,
    relation: crate::relation::SemanticRelation,
) {
    match key {
        CteGroupKey::Frontier(frontier) => resolver.register_frontier(frontier.clone(), relation),
        CteGroupKey::Authored(name) => resolver.register_query_local(
            crate::defuse::environment::QueryLocalRegistration::Relation {
                name: name.clone(),
                relation,
            },
        ),
        CteGroupKey::Generated(name) => resolver.register_query_local(
            crate::defuse::environment::QueryLocalRegistration::SyntheticRelation {
                name: name.clone(),
                relation,
            },
        ),
    }
}

#[derive(Debug, Clone, Copy)]
struct AuthoredClauseFacts {
    contains_union_family: bool,
}

struct AuthoredClause {
    binding: ast_unresolved::CteBinding,
    authored: AuthoredClauseFacts,
}

impl AuthoredClause {
    fn new(binding: ast_unresolved::CteBinding) -> Self {
        struct Collector {
            contains_union_family: bool,
        }
        impl AstVisit<crate::pipeline::asts::core::Unresolved> for Collector {
            fn enter_continuation(
                &mut self,
                continuation: &crate::pipeline::asts::core::Continuation<
                    crate::pipeline::asts::core::Unresolved,
                >,
            ) -> Result<Descent> {
                if matches!(
                    continuation,
                    crate::pipeline::asts::core::Continuation::BagOp { operator, .. }
                        if operator.accumulates_arm_rows()
                ) {
                    self.contains_union_family = true;
                }
                // Continue after finding one: this fact is derived by the
                // canonical exhaustive walk, never by a first-match search.
                Ok(Descent::Continue)
            }
        }

        let mut collector = Collector {
            contains_union_family: false,
        };
        walk_visit_relational(&mut collector, binding.body())
            .expect("authored set-form collection is infallible");
        AuthoredClause {
            binding,
            authored: AuthoredClauseFacts {
                contains_union_family: collector.contains_union_family,
            },
        }
    }
}

struct ResolvedClause {
    body: ast_resolved::Chain,
    authored: AuthoredClauseFacts,
}

fn resolve_definition_group(
    key: &CteGroupKey,
    group: Vec<ast_unresolved::CteBinding>,
    resolver: &mut dyn CteResolver,
) -> Result<ast_resolved::CteBinding> {
    // The spelling, for registration and teaching — the author's or the
    // compiler's. A structural group has none: its key is the carrier
    // scope, and the assembler below sees only glob heads, which never name
    // the subject.
    let name = match key {
        CteGroupKey::Authored(name) | CteGroupKey::Generated(name) => Some(name.clone()),
        CteGroupKey::Frontier(frontier) => Some(frontier.name().clone()),
    };
    let teaching_name = name
        .as_ref()
        .map(|name| name.as_str().to_string())
        .unwrap_or_default();

    // The subject's clauses meet HERE, at the one assembler, before any
    // scope is minted: mixed head forms, arity, the per-position
    // name-offer contest, the Ground-Position rule, and output-heading
    // collision are decided once for the whole group, and each clause
    // body then carries the projection its head declares. A per-clause
    // head applied at build time could not see its siblings, so a
    // disagreement had nowhere to be caught and became a NULL-padded
    // union instead.
    let group = apply_group_head(&teaching_name, group)?;
    let fixpoint = match key {
        CteGroupKey::Frontier(frontier) => frontier.fixpoint(),
        CteGroupKey::Authored(_) | CteGroupKey::Generated(_) => {
            agreed_badge(&group, &teaching_name)?
        }
    };
    let group: Vec<AuthoredClause> = group.into_iter().map(AuthoredClause::new).collect();

    if group.len() == 1 {
        let clause = group
            .into_iter()
            .next()
            .expect("a group of one has its clause");
        resolve_lone_clause(
            key,
            name.as_ref(),
            &teaching_name,
            fixpoint,
            clause,
            resolver,
        )
    } else {
        let (CteGroupKey::Authored(_) | CteGroupKey::Frontier(_), Some(name)) = (key, &name) else {
            return Err(DelightQLError::parse_error(
                "a compiler-built CTE binding was defined more than once",
            ));
        };
        resolve_clause_accumulation(key, name, &teaching_name, fixpoint, group, resolver)
    }
}

/// EVERY CLAUSE OF ONE TARGET WEARS THE SAME BADGE — a fixpoint flavor is
/// one claim about the target, so a mixed set is two claims about one thing
/// and refuses here. (The consulted surface decides the same law at
/// `DefinitionGroup::assemble`, where a subject's clauses meet instead.)
fn agreed_badge(group: &[ast_unresolved::CteBinding], teaching_name: &str) -> Result<Fixpoint> {
    let Some(first) = group.first() else {
        return Ok(Fixpoint::Bag);
    };
    let fixpoint = first.authority().fixpoint;
    for (idx, clause) in group.iter().enumerate().skip(1) {
        if clause.authority().fixpoint != fixpoint {
            return Err(DelightQLError::validation_error_categorized(
                crate::uri_registry::subcat::RECURSION_MIXED_BADGE,
                format!(
                    "binding '{teaching_name}': clause {} is {} and clause 1 is {}. \
                     A fixpoint flavor is one claim about the target — every clause \
                     wears the same badge.",
                    idx + 1,
                    clause.authority().fixpoint.spelling(),
                    fixpoint.spelling()
                ),
                "mixed fixpoint badges in one binding",
            ));
        }
    }
    Ok(fixpoint)
}

/// A definition of ONE clause. It accumulates nothing, so its body is
/// whatever the clause resolved to — and it is still asked the recursion
/// question, because a lone clause may well read its own name.
fn resolve_lone_clause(
    key: &CteGroupKey,
    name: Option<&delightql_types::SqlIdentifier>,
    teaching_name: &str,
    fixpoint: Fixpoint,
    clause: AuthoredClause,
    resolver: &mut dyn CteResolver,
) -> Result<ast_resolved::CteBinding> {
    let AuthoredClause { binding, authored } = clause;
    let (body, authority) = binding.into_resolution();
    let origin = authority.origin;
    let expression = resolver.resolve_cte_expression(body, authority.horizon)?;
    let crossing = resolver.crossing_carriers().to_vec();
    let expression = crate::pipeline::refiner::pattern_classifier::inject_crossing_carriers(
        expression,
        &crossing,
        resolver.identities(),
    )?;

    let subject = match name {
        Some(name) => transform_schema_table_names(
            &expression.semantic_relation(),
            name,
            origin,
            crate::names::CteRole::Materialize,
            resolver.identities(),
        )?,
        None => unreachable!("a named group key carries its spelling"),
    };
    if name.is_some() {
        register_named_cte(resolver, key, subject);
    }

    let resolved = ResolvedClause {
        body: expression,
        authored,
    };
    let recursion = decide(
        std::slice::from_ref(&resolved),
        subject.scope(),
        fixpoint,
        teaching_name,
        resolver.identities(),
    )?;

    Ok(ast_resolved::CteBinding::bound(
        // THE DECISION IS THE BODY. A lone clause accumulates nothing, so a
        // fixpoint one is its own anchor with no members — and an ordinary
        // one has no fixpoint shape to be given later.
        match recursion.accumulation() {
            None => DefinitionBody::Ordinary(resolved.body),
            Some(accumulation) => DefinitionBody::Fixpoint(FixpointBody {
                accumulation,
                anchor: resolved.body,
                members: Vec::new(),
            }),
        },
        // The authored spelling and effect declaration end here: the
        // binding IS its scope from this point on. The head and provenance
        // were spent above; the phase deletes their slot.
        subject,
    ))
}

/// A definition of SEVERAL clauses.
///
/// THE DECISION COMES FIRST. The clauses are asked for a self-reference
/// before anything is done with them, so the body below is BUILT FROM the
/// answer rather than assembled beside it — and a fixpoint's clauses are
/// never joined at all.
fn resolve_clause_accumulation(
    key: &CteGroupKey,
    name: &delightql_types::SqlIdentifier,
    teaching_name: &str,
    fixpoint: Fixpoint,
    group: Vec<AuthoredClause>,
    resolver: &mut dyn CteResolver,
) -> Result<ast_resolved::CteBinding> {
    let mut schemas: Vec<crate::relation::SemanticRelation> = Vec::with_capacity(group.len());
    let mut operands: Vec<ResolvedClause> = Vec::with_capacity(group.len());
    // The scope a later clause could already see, kept so the finished
    // group answers under it rather than under a second one.
    let mut anchor_scope: Option<crate::relation::SemanticRelation> = None;

    for (idx, clause) in group.into_iter().enumerate() {
        let AuthoredClause { binding, authored } = clause;
        let (body, authority) = binding.into_resolution();
        let origin = authority.origin;
        let expression = resolver.resolve_cte_expression(body, authority.horizon)?;
        let crossing = resolver.crossing_carriers().to_vec();
        let expression = crate::pipeline::refiner::pattern_classifier::inject_crossing_carriers(
            expression,
            &crossing,
            resolver.identities(),
        )?;
        let expr_schema = expression.semantic_relation();
        if idx == 0 {
            let base_schema = transform_schema_table_names(
                &expr_schema,
                name,
                origin,
                crate::names::CteRole::Recursive,
                resolver.identities(),
            )?;
            anchor_scope = Some(base_schema);
            register_named_cte(resolver, key, base_schema);
        }
        schemas.push(expr_schema);
        operands.push(ResolvedClause {
            body: expression,
            authored,
        });
    }

    // A later clause that referenced the name reached the anchor's scope.
    // The finished group answers under that same scope; a second scope
    // would strand the recursive member on a name no WITH clause binds.
    let published = anchor_scope.expect("a multi-clause group has an anchor");

    let recursion = decide(
        &operands,
        published.scope(),
        fixpoint,
        teaching_name,
        resolver.identities(),
    )?;

    // ACROSS CLAUSES THE BODIES PUBLISH ONE HEADING (heads-law: CLAUSE
    // AGREEMENT; effect-algebra-law: THE LEDGER IS A TAGGED SUM names
    // clause accumulation as the case where raggedness is NOT licensed). A
    // glob head declares nothing, so the disagreement first exists once the
    // bodies are resolved, and it is named HERE while the clauses are still
    // distinguishable. Left to the set law it arrives as two internal
    // widths and names neither the clause that disagrees nor what it
    // publishes.
    clauses_publish_one_heading(name, &schemas, resolver.identities())?;
    register_named_cte(resolver, key, published);

    let mut operands = operands.into_iter().map(|clause| clause.body);
    let anchor = operands.next().expect("a CTE group has a first clause");

    // THE DECISION IS THE BODY. A fixpoint's clauses STAY APART: the anchor
    // and the members are what the body is, so there is no accumulated tree
    // for a later phase to walk, no operator standing in for a boundary,
    // and nothing to re-split.
    if let Some(accumulation) = recursion.accumulation() {
        return Ok(ast_resolved::CteBinding::bound(
            DefinitionBody::Fixpoint(FixpointBody {
                accumulation,
                anchor,
                members: operands.collect(),
            }),
            published,
        ));
    }

    // A NON-FIXPOINT'S clauses are an ordinary positional stack. The
    // group's one heading was judged above, so accumulation is positional;
    // a corresponding fallback here would silently invent the NULL-padding
    // that HEADS forbids. Each step publishes the heading its own two
    // operands make, and for a positional stack that is the first clause's
    // throughout.
    let operator = crate::pipeline::asts::core::SetOperator::UnionAllPositional;
    let mut step_schemas = Vec::with_capacity(schemas.len());
    let mut accumulated = schemas[0];
    for schema in &schemas[1..] {
        let step = resolver
            .identities()
            .authority()
            .set_step(operator, &[accumulated, *schema])?;
        accumulated = step.result();
        step_schemas.push(step);
    }

    let identities = resolver.identities();
    let authority = identities.authority();
    let mut stacked = anchor;
    for (arm, step) in operands.zip(step_schemas) {
        stacked = authority.bag(stacked, step, arm, None);
    }

    Ok(ast_resolved::CteBinding::bound(
        DefinitionBody::Ordinary(stacked),
        // The authored spelling and effect declaration end here: the
        // binding IS its scope from this point on. The head and provenance
        // were spent above; the phase deletes their slot.
        published,
    ))
}

/// THE RECURSION DECISION, TAKEN WHERE THE SELF-REFERENCE BINDS.
///
/// A binding's own scope is registered before its later clauses resolve, so
/// a self-reference is a resolved reference to that scope and nothing else —
/// no name comparison, no shadowing question (a shadowed name resolves to
/// the inner scope by construction, which is why this is decided here and
/// not over spellings later). An ALIAS on the self-reference changes the
/// binding name and not the recursive character: the question asked is scope
/// membership, which an alias does not move.
///
/// The badge and the finding answer together: a self-reference under `%` is
/// the deduplicating fixpoint, a self-reference without one is the bag, and
/// `%` over nothing is a false fixpoint claim — a flavor asserted of a
/// non-fixpoint — which refuses rather than lowering as its unbadged twin.
///
/// The answer is stored, decided exactly once, here. Deriving it again
/// independently in a later pass — an advisory walk, a structural re-marking
/// over the SQL AST, the transformer's own walk — invites disagreement
/// between the passes.
///
/// The clauses are asked SEPARATELY: each is walked in full, so the nodes
/// visited are every node of the definition — and asking before anything is
/// built is what lets the ANSWER be the body rather than a second fact
/// standing next to one.
fn decide(
    clauses: &[ResolvedClause],
    binding: ScopeId,
    authored: Fixpoint,
    teaching_name: &str,
    identities: &crate::relation::Planning,
) -> Result<RecursionState> {
    let recursive_clauses = clauses
        .iter()
        .map(|clause| self_reference_count(clause, binding, identities))
        .collect::<Vec<_>>();
    let recursive = recursive_clauses.iter().copied().any(|count| count > 0);
    // One frontier read plus an authored set operation is the illegal
    // recursive-set shape. More frontier reads are already a stronger
    // nonlinear or self-subquery violation; preserve that settled diagnosis
    // instead of masking it with the enclosing operator.
    if clauses
        .iter()
        .zip(recursive_clauses)
        .any(|(clause, self_references)| {
            self_references == 1 && clause.authored.contains_union_family
        })
    {
        return Err(DelightQLError::validation_error_categorized(
            crate::uri_registry::subcat::RECURSION_SET_OPERATOR,
            "a union-family operator appears inside a recursive clause. Clause accumulation \
             is the fixpoint's own operation; finish the fixpoint before using a set operator. \
             SEMANTICS/recursion-contract-law.md.",
            "union-family operator inside a fixpoint",
        ));
    }
    match (recursive, authored) {
        (true, Fixpoint::Deduplicating) => Ok(RecursionState::RecursiveDeduplicating(
            DeduplicatingFixpoint(()),
        )),
        (true, Fixpoint::Bag) => Ok(RecursionState::RecursiveBag),
        (false, Fixpoint::Bag) => Ok(RecursionState::NonRecursive),
        (false, Fixpoint::Deduplicating) => Err(DelightQLError::validation_error_categorized(
            crate::uri_registry::subcat::RECURSION_FALSE_FIXPOINT,
            format!(
                "'{teaching_name}' wears the deduplicating fixpoint badge `%` and \
                 references nothing of itself, so there is no unfold for the badge to \
                 choose the union of. Drop the badge; to deduplicate an ordinary \
                 definition, spell the distinct view in the body (`|> %(*)`)"
            ),
            "a fixpoint badge on a non-fixpoint",
        )),
    }
}

/// How many times this clause references the binding.
///
/// PRIVATE, and the reason the door above cannot be talked past: the finding
/// is computed from the clauses THIS MODULE resolved, never supplied from
/// outside. There is one scope to ask about and no list of aliases beside
/// it: a group answers under the scope its anchor registered, which is the
/// scope a later clause's self-reference resolved to and the scope the
/// finished binding publishes.
fn self_reference_count(
    clause: &ResolvedClause,
    binding: ScopeId,
    identities: &crate::relation::Planning,
) -> usize {
    let mut finder = SelfReferenceFinder {
        own: binding,
        identities,
        count: 0,
    };
    walk_visit_relational(&mut finder, &clause.body)
        .expect("self-reference detection is infallible (hooks never return Err)");
    finder.count
}

/// Finds any resolved relation standing on the binding's own scope, anywhere
/// in the body — a predicate subquery, a pipe argument, a consulted view's
/// body, a nested binding. The shared whole-tree descent names every
/// recursive edge once, so a self-reference cannot hide in a field some
/// hand-rolled walker forgot.
struct SelfReferenceFinder<'a> {
    own: ScopeId,
    identities: &'a crate::names::Registry,
    count: usize,
}

impl AstVisit<Resolved> for SelfReferenceFinder<'_> {
    /// The HEAD is where a ground read lives, and the head is where the
    /// relation it publishes lives with it — so the question is asked at the
    /// one node that has both.
    fn enter_relational(&mut self, chain: &ast_resolved::Chain) -> Result<Descent> {
        use crate::pipeline::asts::core::{GroundForm, Relation};
        if let GroundForm::Reference(Relation::Ground { .. }) = chain.head().form() {
            let read = chain.head().result();
            if crate::relation::contains_scope(self.identities, read, self.own).unwrap_or(false) {
                self.count += 1;
            }
        }
        Ok(Descent::Continue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::sql_ast::SetOperator as Sql;

    fn named(name: &str) -> ast_unresolved::Chain {
        ast_unresolved::Chain::read(
            ast_unresolved::Relation::Ground {
                mention: ast_unresolved::GroundMention::Named {
                    identifier: ast_unresolved::QualifiedName {
                        namespace_path: ast_unresolved::NamespacePath::empty(),
                        name: name.into(),
                    },
                    alias: None,
                    mutation_target: false,
                    passthrough: false,
                },
                outer: false,
            },
            ast_unresolved::Access::All,
        )
    }

    fn authored(body: ast_unresolved::Chain) -> AuthoredClause {
        AuthoredClause::new(ast_unresolved::CteBinding::authored(
            body,
            crate::pipeline::asts::core::AuthoredCteSubject::Authored {
                name: "r".into(),
                effect: crate::pipeline::asts::core::CteEffectDeclaration::Pure,
            },
            crate::pipeline::asts::core::CteAuthority {
                horizon: crate::pipeline::asts::core::LexicalHorizon::all(),
                head: crate::pipeline::asts::core::definitions::Head::glob(),
                origin: crate::pipeline::asts::core::provenance::CteOrigin::UserDefined,
                fixpoint: Fixpoint::Bag,
            },
        ))
    }

    #[test]
    fn authored_union_fact_is_independent_of_lowered_recursive_shape() {
        use crate::pipeline::asts::core::SetOperator;

        for operator in [
            SetOperator::UnionAllPositional,
            SetOperator::UnionCorresponding,
            SetOperator::SmartUnionAll,
        ] {
            let clause = authored(named("left").bag_op(operator, named("right"), ()));
            assert!(clause.authored.contains_union_family);
        }
        let minus =
            authored(named("left").bag_op(SetOperator::MinusCorresponding, named("right"), ()));
        assert!(!minus.authored.contains_union_family);
    }

    /// THE KEYWORD EACH ACCUMULATION WRITES, and the whole SQL set-operator
    /// vocabulary beside it.
    ///
    /// The absence this module claims is a TYPE fact, not something a test
    /// inside the module could show: a child module sees its parent's
    /// private fields, so this test can build the deduplicating outcome and
    /// code outside cannot. What it does pin is that a badgeless decision
    /// never reaches `UNION`, and that the SQL vocabulary has exactly one
    /// member and it is ALL-flavored. `UNION` itself is pinned end to end —
    /// where a real badged fixpoint exists — by the five-dialect contract
    /// `R02_recursion--the_badge_chooses_the_union` and by
    /// `recursion_contract` P10/P11/P14.
    #[test]
    fn a_badgeless_decision_never_reaches_union() {
        for state in [RecursionState::NonRecursive, RecursionState::RecursiveBag] {
            match state.accumulation() {
                None | Some(Accumulation::Bag) => {}
                Some(Accumulation::Deduplicating(_)) => {
                    panic!("a badgeless decision must not deduplicate")
                }
            }
        }
        assert_eq!(Accumulation::Bag.keyword(), "UNION ALL");
        assert_eq!(
            Accumulation::Deduplicating(DeduplicatingFixpoint(())).keyword(),
            "UNION"
        );
        // The whole SQL set-operator vocabulary, exhaustively: the multiset
        // spellings are an ABSENT CAPABILITY, not a flag left false.
        match Sql::UnionAll {
            Sql::UnionAll => assert_eq!(Sql::UnionAll.keyword(), "UNION ALL"),
        }
    }
}
