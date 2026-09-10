// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! CORRELATION SUPPORT — the evaluation obligation a correlated interior
//! carries beside its heading.
//!
//! An interior relation in join position that reads the enclosing row is
//! realized by HOISTING: the restriction that reads the row is evaluated at
//! the enclosing join, so the interior occurrences it reads must still be
//! readable at the boundary that join sees. That is an obligation of the
//! interior's EVALUATION, not a position of its heading: no author can
//! address it, no heading shows it, and every operation standing between
//! the correlated restriction and the boundary either keeps it readable or
//! refuses — nothing drops it, and nothing later goes looking for it.
//!
//! The obligation is born in ONE act, [`super::SemanticBuilder::correlate`],
//! which DERIVES the occurrences from the condition and the relation the
//! restriction stands on, records what that relation owes, and writes the
//! step — all in the same act, from two operands and nothing else. Every
//! derivation afterwards re-judges the obligation under the form's own
//! [`SupportLaw`].

use super::PortId;
use crate::pipeline::asts::core::{Phase, TruthExpression};

/// A CORRELATED RESTRICTION: the condition the enclosing join evaluates and
/// the interior occurrences it reads — ONE value.
///
/// The fields are private to the relation authority and there is no
/// constructor outside [`super::SemanticBuilder::correlate`], which derives
/// the occurrences from the condition and the relation it is handed: a
/// caller supplies the restriction's two operands and nothing else, so no
/// condition can be paired with occurrences somebody else read, and no
/// occurrence list can be attached to a relation that does not publish it.
/// It inhabits the resolved phase alone ([`Phase::Correlated`]): the
/// resolver's act mints it, and the refiner's classification spends it at
/// the interior boundary — [`Correlated::into_condition`] is the one road
/// out, and what comes out is a plain predicate no interface takes back.
#[derive(Debug, PartialEq)]
pub struct Correlated<P: Phase> {
    condition: TruthExpression<P>,
    /// The occurrences the condition reads that the relation it stands on
    /// publishes, in the order the condition named them — what the
    /// relation owes.
    inner: Vec<PortId>,
    /// THE RELATION THE RESTRICTION STANDS ON — the one the act recorded
    /// the obligation on. Carried IN the value: every assembly of a step
    /// reads the owner off the payload and refuses a result that is not
    /// it, and a landing reads it too, so whoever holds the value holds
    /// its owner and no road pairs the correlation with another relation.
    standing: crate::relation::SemanticRelation,
}

impl<P: Phase> Clone for Correlated<P> {
    fn clone(&self) -> Self {
        Correlated {
            condition: self.condition.clone(),
            inner: self.inner.clone(),
            standing: self.standing,
        }
    }
}

impl<P: Phase> Correlated<P> {
    /// Minted by the correlation act alone, from what it derived, on the
    /// relation it stood on.
    pub(super) fn recorded(
        condition: TruthExpression<P>,
        inner: Vec<PortId>,
        standing: crate::relation::SemanticRelation,
    ) -> Self {
        Correlated {
            condition,
            inner,
            standing,
        }
    }

    /// The condition the enclosing join evaluates.
    pub(crate) fn condition(&self) -> &TruthExpression<P> {
        &self.condition
    }

    /// Whether the restriction reads this interior occurrence.
    pub(crate) fn reads(&self, port: PortId) -> bool {
        self.inner.contains(&port)
    }

    /// The relation the restriction stands on: the owner of the obligation.
    pub(crate) fn standing(&self) -> crate::relation::SemanticRelation {
        self.standing
    }

    /// SPEND the correlation at the boundary that evaluates it: the
    /// condition, as the plain predicate the enclosing join carries. Nothing
    /// takes the predicate back as a correlation.
    pub(crate) fn into_condition(self) -> TruthExpression<P> {
        self.condition
    }

    /// Cross a phase boundary, or a same-phase rewrite.
    ///
    /// The evidence the act derived — the occurrences owed, the owner — is
    /// kept only where it still describes the condition: the crossing reads
    /// the occurrences the condition names before and after the rewrite,
    /// through each phase's own reader, and REFUSES a rewrite that changed
    /// them. A walk may re-spell a condition (classify a relation nested in
    /// it, say); a walk that changes which occurrences it reads has changed
    /// what the relation owes, and only the correlation act may derive
    /// that. Whether the destination phase may hold the result at all is
    /// the phases' answer ([`Phase::admit_correlated`]), not the walker's.
    pub(crate) fn crossing<Q: Phase>(
        self,
        condition: impl FnOnce(TruthExpression<P>) -> crate::error::Result<TruthExpression<Q>>,
    ) -> crate::error::Result<Correlated<Q>> {
        let read = P::occurrences_read(&self.condition);
        let condition = condition(self.condition)?;
        let read_after = Q::occurrences_read(&condition);
        if !same_occurrences(&read, &read_after) {
            return Err(crate::diagnostic::Internal::invariant(
                "correlated",
                "a rewrite changed which occurrences a correlated restriction reads: the \
                 support its act derived no longer describes it, and only the act derives \
                 support",
            ));
        }
        Ok(Correlated {
            condition,
            inner: self.inner,
            standing: self.standing,
        })
    }
}

/// The same occurrences, as sets.
fn same_occurrences(before: &[PortId], after: &[PortId]) -> bool {
    before.len() == after.len()
        && before.iter().all(|port| after.contains(port))
        && after.iter().all(|port| before.contains(port))
}

impl<P: Phase> crate::lispy::ToLispy for Correlated<P> {
    fn to_lispy(&self) -> String {
        let inner: Vec<String> = self.inner.iter().map(|port| format!("{port:?}")).collect();
        format!(
            "(correlated {} [{}] {})",
            self.condition.to_lispy(),
            inner.join(" "),
            self.standing.to_lispy()
        )
    }
}

/// THE OCCURRENCES A CONDITION READS, at this level. Every named reference
/// the condition holds; a relation nested inside it — under a value or as a
/// probe — is its own scope and reads for itself. Defined for the bound
/// phases, whose references are occurrences.
pub(crate) fn ports_read_by<P>(condition: &TruthExpression<P>) -> Vec<PortId>
where
    P: Phase<Col = crate::pipeline::asts::core::ColumnOccurrence>,
{
    use crate::pipeline::ast_visit::walk_visit_boolean;
    let mut read = Read(Vec::new());
    walk_visit_boolean(&mut read, condition)
        .expect("a reference walk over a condition cannot fail");
    read.0
}

/// THE OCCURRENCES A CONDITION READS AT ANY DEPTH, for the correlation
/// act: a relation nested inside the condition — an existence's body, a
/// scalar subquery — is evaluated wherever the condition is, so a read it
/// makes of the interior row is one the hoisted condition makes. The
/// nested relation's own positions come along and are nobody's obligation;
/// the act keeps only what the relation it stands on publishes.
pub(crate) fn ports_read_by_deep<P>(condition: &TruthExpression<P>) -> Vec<PortId>
where
    P: Phase<Col = crate::pipeline::asts::core::ColumnOccurrence>,
{
    use crate::pipeline::ast_visit::walk_visit_boolean;
    let mut read = DeepRead(Vec::new());
    walk_visit_boolean(&mut read, condition)
        .expect("a reference walk over a condition cannot fail");
    read.0
}

/// THE OCCURRENCES A VALUE READS AT ANY DEPTH, for the publication that
/// states where the value is evaluated.
pub(crate) fn ports_read_deep<P>(
    value: &crate::pipeline::asts::core::DomainExpression<P>,
) -> Vec<PortId>
where
    P: Phase<Col = crate::pipeline::asts::core::ColumnOccurrence>,
{
    use crate::pipeline::ast_visit::walk_visit_domain;
    let mut read = DeepRead(Vec::new());
    walk_visit_domain(&mut read, value).expect("a reference walk over a value cannot fail");
    read.0
}

/// The reference walk that enters every nested relation.
struct DeepRead(Vec<PortId>);

impl<P> crate::pipeline::ast_visit::AstVisit<P> for DeepRead
where
    P: Phase<Col = crate::pipeline::asts::core::ColumnOccurrence>,
{
    fn enter_domain(
        &mut self,
        expr: &crate::pipeline::asts::core::DomainExpression<P>,
    ) -> crate::error::Result<crate::pipeline::ast_visit::Descent> {
        use crate::pipeline::asts::core::{ColumnOccurrence, NamedReference, Reference};
        if let crate::pipeline::asts::core::DomainExpression::Reference(Reference::Named(
            NamedReference(ColumnOccurrence { column, .. }),
        )) = expr
        {
            if !self.0.contains(column) {
                self.0.push(*column);
            }
        }
        Ok(crate::pipeline::ast_visit::Descent::Continue)
    }
}

/// THE OCCURRENCES A RUN OF VALUES READS, for the act that owes them — a
/// bounded ordering's specs. Every named reference the values hold, at this
/// level; a relation nested inside a value is its own scope and reads for
/// itself.
pub(crate) fn ports_read<'a, P>(
    values: impl Iterator<Item = &'a crate::pipeline::asts::core::DomainExpression<P>>,
) -> Vec<PortId>
where
    P: Phase<Col = crate::pipeline::asts::core::ColumnOccurrence>,
{
    use crate::pipeline::ast_visit::walk_visit_domain;
    let mut read = Read(Vec::new());
    for value in values {
        walk_visit_domain(&mut read, value).expect("a reference walk over a value cannot fail");
    }
    read.0
}

/// The one reference walk: named references at this level, in first-read
/// order, stopping at every nested relation.
struct Read(Vec<PortId>);

impl<P> crate::pipeline::ast_visit::AstVisit<P> for Read
where
    P: Phase<Col = crate::pipeline::asts::core::ColumnOccurrence>,
{
    fn enter_relational(
        &mut self,
        _: &crate::pipeline::asts::core::Chain<P>,
    ) -> crate::error::Result<crate::pipeline::ast_visit::Descent> {
        Ok(crate::pipeline::ast_visit::Descent::SkipSubtree)
    }

    fn enter_domain(
        &mut self,
        expr: &crate::pipeline::asts::core::DomainExpression<P>,
    ) -> crate::error::Result<crate::pipeline::ast_visit::Descent> {
        use crate::pipeline::asts::core::{ColumnOccurrence, NamedReference, Reference};
        if let crate::pipeline::asts::core::DomainExpression::Reference(Reference::Named(
            NamedReference(ColumnOccurrence { column, .. }),
        )) = expr
        {
            if !self.0.contains(column) {
                self.0.push(*column);
            }
        }
        Ok(if expr.nests_relation() {
            crate::pipeline::ast_visit::Descent::SkipSubtree
        } else {
            crate::pipeline::ast_visit::Descent::Continue
        })
    }
}

/// WHAT ONE FORM DOES WITH THE SUPPORT ITS OPERANDS OWE.
///
/// Stated by the form's derivation and stored with the relation, so a step
/// landed back on an operand re-judges its support under the law its own
/// derivation stated rather than under one somebody chose for it afterwards.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SupportLaw {
    /// The level republishes its operand's own dimensions, or adds beside
    /// them, and can emit any operand position it does not publish: an
    /// owed occurrence continues in the one published position standing on
    /// it, or rides beside the heading under its own identity.
    Carries,
    /// A join: each operand's level is a FROM item of this one, and the
    /// join's row re-emits every support column its operands emitted,
    /// after the semantic heading, left then right. What the operands OWE
    /// carries as under `Carries`; what their levels EMIT is re-emitted
    /// whether owed onward or not, because the physical row is the
    /// concatenation.
    Concatenates,
    /// A grouping publishes its keys and its reductions and nothing else,
    /// so an owed occurrence continues only in a KEY standing on it; a
    /// grouping that stands no key on the occurrence refuses. `keys` is the
    /// count of key positions at the front of the published heading.
    Translates { keys: usize },
    /// The operation cannot keep an operand position readable without
    /// changing what it means — a set operation merges arms the correlation
    /// does not filter, a witness collapses the rows, a binding is a
    /// complete statement of its own. It refuses while anything is owed,
    /// naming what it is.
    Refuses(&'static str),
}

#[cfg(test)]
mod tests {
    //! THE OBLIGATION IS INDIVISIBLE FROM CONSTRUCTION. Each test drives the
    //! one correlation act and the ordinary derivations over its result, and
    //! reads the support record the way lowering and the classifier do.

    use super::super::form::{
        AnonymousShape, AnonymousSlot, AnonymousSpec, GroupKind, GroupSpec, Naming, ProjectSlot,
        ProjectSpec, ProjectWhy, ReductionSlot,
    };
    use super::super::{Planning, RelForm, SemanticRelation};
    use crate::pipeline::asts::core::{ColumnOccurrence, Comparison, NamedReference, Reference};
    use crate::pipeline::asts::resolved;
    use crate::relation::PortId;

    /// A relation publishing two named dimensions, read as a chain.
    fn read(registry: &Planning, first: &str, second: &str) -> (resolved::Chain, PortId, PortId) {
        let slots = [
            AnonymousSlot::Declared {
                position: 0,
                named: Some(registry.intern(first, false)),
            },
            AnonymousSlot::Declared {
                position: 1,
                named: Some(registry.intern(second, false)),
            },
        ];
        let relation = registry
            .authority()
            .derive(RelForm::Anonymous(AnonymousSpec::plain(
                AnonymousShape::Tabular,
                &slots,
                None,
            )))
            .expect("an anonymous relation is built");
        let ports = crate::relation::published_ports(registry, &relation).expect("interface");
        let chain = registry
            .authority()
            .ground_read(resolved::Access::All, false, relation)
            .expect("a ground read");
        (chain, ports[0], ports[1])
    }

    fn equality(left: PortId, right: PortId) -> resolved::TruthExpression {
        resolved::TruthExpression::Comparison(Comparison {
            operator: crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual,
            left: Box::new(resolved::DomainExpression::Reference(Reference::Named(
                NamedReference(ColumnOccurrence::engine(left)),
            ))),
            right: Box::new(resolved::DomainExpression::Reference(Reference::Named(
                NamedReference(ColumnOccurrence::engine(right)),
            ))),
        })
    }

    fn proof() -> crate::pipeline::resolver::Terminal {
        crate::pipeline::resolver::Terminal::judged_for_test()
    }

    /// The correlation act over a read of `k, b`, reading `k` against an
    /// outer occurrence.
    fn correlated(registry: &Planning) -> (resolved::Chain, PortId, PortId, PortId) {
        let (chain, k, b) = read(registry, "k", "b");
        let (_, outer, _) = read(registry, "ok", "oa");
        let chain = registry
            .authority()
            .correlate(chain, equality(k, outer), proof())
            .expect("the correlation act");
        (chain, k, b, outer)
    }

    fn owes(registry: &Planning, relation: &SemanticRelation) -> Vec<PortId> {
        registry.relations().support_owed(relation.relation())
    }

    fn emits(registry: &Planning, relation: &SemanticRelation) -> Vec<PortId> {
        registry.relations().support_emitted(relation.relation())
    }

    fn project(
        registry: &Planning,
        input: SemanticRelation,
        slots: &[ProjectSlot],
    ) -> SemanticRelation {
        registry
            .authority()
            .derive(RelForm::Project(ProjectSpec {
                input,
                why: ProjectWhy::Stage,
                slots,
            }))
            .expect("a projection is built")
    }

    fn carried(source: PortId) -> ProjectSlot {
        ProjectSlot::Carried {
            source,
            naming: Naming::Inherited,
        }
    }

    fn reference(port: PortId) -> resolved::DomainExpression {
        resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence::engine(port),
        )))
    }

    fn is_support_refusal(result: &crate::error::Result<SemanticRelation>) -> bool {
        matches!(
            result,
            Err(crate::error::DelightQLError::Semantic(
                crate::diagnostic::Semantic::Interior(
                    crate::diagnostic::Interior::CorrelationSupport { .. }
                )
            ))
        )
    }

    #[test]
    fn the_correlated_read_owes_the_occurrence_and_publishes_it() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (chain, k, _, _) = correlated(&registry);
        let read = chain.semantic_relation();
        assert_eq!(owes(&registry, &read), vec![k]);
        assert!(
            emits(&registry, &read).is_empty(),
            "the read publishes k itself"
        );
        assert!(matches!(
            chain.continuations().last().map(resolved::Step::form),
            Some(resolved::Continuation::Correlated(_))
        ));
    }

    #[test]
    fn a_projection_dropping_the_occurrence_emits_it_beside_its_heading() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (chain, k, b, _) = correlated(&registry);
        let dropped = project(&registry, chain.semantic_relation(), &[carried(b)]);
        assert_eq!(owes(&registry, &dropped), vec![k]);
        assert_eq!(emits(&registry, &dropped), vec![k]);
        assert!(registry.authority().carries(&dropped, k).expect("carries"));
        let published = crate::relation::published_ports(&registry, &dropped).expect("ports");
        assert!(
            !published.contains(&k),
            "support is never a published position"
        );
        assert!(
            registry
                .relations()
                .dependencies(dropped.relation())
                .is_empty(),
            "no constraint dependency was stated: the support is its own record"
        );
    }

    #[test]
    fn a_projection_keeping_the_occurrence_continues_it_and_emits_nothing() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (chain, k, b, outer) = correlated(&registry);
        let kept = project(
            &registry,
            chain.semantic_relation(),
            &[carried(k), carried(b)],
        );
        let published = crate::relation::published_ports(&registry, &kept).expect("ports");
        assert_eq!(owes(&registry, &kept), vec![published[0]]);
        assert!(emits(&registry, &kept).is_empty());
        assert!(registry.authority().carries(&kept, k).expect("carries"));
        // Dropping the continued position one level up owes and emits IT,
        // under its own identity: the exact occurrence stays reachable
        // through the record, never through a name.
        let dropped = project(&registry, kept, &[carried(published[1])]);
        assert_eq!(owes(&registry, &dropped), vec![published[0]]);
        assert_eq!(emits(&registry, &dropped), vec![published[0]]);
        assert!(registry.authority().carries(&dropped, k).expect("carries"));
        assert!(
            !registry
                .authority()
                .carries(&dropped, outer)
                .expect("carries"),
            "the outer occurrence belongs to the enclosing row, not to the interior"
        );
    }

    #[test]
    fn two_positions_continuing_the_occurrence_leave_it_its_own_identity() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (chain, k, b, _) = correlated(&registry);
        let twice = project(
            &registry,
            chain.semantic_relation(),
            &[
                carried(k),
                ProjectSlot::Carried {
                    source: k,
                    naming: Naming::Anonymous,
                },
                carried(b),
            ],
        );
        assert_eq!(owes(&registry, &twice), vec![k], "no first-match winner");
        assert_eq!(emits(&registry, &twice), vec![k]);
    }

    #[test]
    fn a_set_over_an_obligated_arm_refuses() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (chain, _, _, _) = correlated(&registry);
        let (other, _, _) = read(&registry, "k", "b");
        let refused = registry
            .authority()
            .set_step(
                crate::pipeline::asts::core::SetOperator::UnionAllPositional,
                &[chain.semantic_relation(), other.semantic_relation()],
            )
            .map(|_| chain.semantic_relation());
        assert!(is_support_refusal(&refused));
    }

    #[test]
    fn a_grouping_translates_through_its_key_and_refuses_without_one() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (chain, k, b, _) = correlated(&registry);
        let reduction = [ReductionSlot::Value {
            slot: ProjectSlot::Computed {
                naming: Naming::Anonymous,
                shape: crate::names::ValueShape::Unknown,
            },
        }];
        let keyed = registry
            .authority()
            .derive(RelForm::Group(GroupSpec {
                input: chain.semantic_relation(),
                kind: GroupKind::Reduce,
                keys: &[carried(k)],
                reductions: &reduction,
            }))
            .expect("a grouping by the key is built");
        let published = crate::relation::published_ports(&registry, &keyed).expect("ports");
        assert_eq!(owes(&registry, &keyed), vec![published[0]]);
        assert!(emits(&registry, &keyed).is_empty());

        let refused = registry.authority().derive(RelForm::Group(GroupSpec {
            input: chain.semantic_relation(),
            kind: GroupKind::Reduce,
            keys: &[carried(b)],
            reductions: &reduction,
        }));
        assert!(is_support_refusal(&refused));

        let keyless = registry.authority().derive(RelForm::Group(GroupSpec {
            input: chain.semantic_relation(),
            kind: GroupKind::Reduce,
            keys: &[],
            reductions: &reduction,
        }));
        assert!(is_support_refusal(&keyless));
    }

    fn projection_step(
        registry: &Planning,
        input: SemanticRelation,
        port: PortId,
    ) -> resolved::Step {
        let (step, _) = registry
            .authority()
            .bind(crate::relation::pending::Pending::Publication {
                input,
                publishes: crate::relation::pending::Publishes::Anew,
                why: ProjectWhy::Stage,
                positions: vec![crate::relation::pending::Position::restating(port, None)],
            })
            .expect("the projection step");
        step
    }

    #[test]
    fn the_interior_boundary_spends_outward() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (chain, k, b, _) = correlated(&registry);
        let step = projection_step(&registry, chain.semantic_relation(), b);
        let body = registry
            .authority()
            .reland(chain, step)
            .expect("the projection lands on the correlated read");
        let identifier = resolved::QualifiedName {
            namespace_path: resolved::NamespacePath::empty(),
            name: delightql_types::SqlIdentifier::new("t"),
        };
        let head = registry
            .authority()
            .boundary_head(
                resolved::GroundForm::Reference(resolved::Relation::InnerRelation {
                    pattern: resolved::InnerRelationPattern::Indeterminate {
                        identifier,
                        subquery: Box::new(body),
                    },
                    alias: None,
                    outer: false,
                }),
                crate::relation::builder::Boundary::Interior {
                    answer: registry.intern("t", false),
                },
            )
            .expect("the interior boundary");
        let boundary = *head.result();
        assert!(
            owes(&registry, &boundary).is_empty(),
            "nothing over the boundary owes it"
        );
        assert_eq!(
            emits(&registry, &boundary),
            vec![k],
            "the boundary's level emits it"
        );
        assert!(registry.authority().carries(&boundary, k).expect("carries"));
    }

    #[test]
    fn a_relanded_step_rejudges_what_it_owes() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (chain, k, b) = read(&registry, "k", "b");
        let (_, outer, _) = read(&registry, "ok", "oa");
        // The projection is derived BEFORE the correlation exists.
        let step = projection_step(&registry, chain.semantic_relation(), b);
        assert!(owes(&registry, step.result()).is_empty());
        let correlated = registry
            .authority()
            .correlate(chain, equality(k, outer), proof())
            .expect("the correlation act");
        let relanded = registry
            .authority()
            .reland(correlated, step)
            .expect("the step lands back on its operand");
        let projection = relanded.semantic_relation();
        assert_eq!(owes(&registry, &projection), vec![k]);
        assert_eq!(emits(&registry, &projection), vec![k]);
    }

    // THE RELATIONSHIP IS OWNED BY THE ACT. The tests below drive the act
    // with the two operands it takes and read the one value it minted; each
    // of them is a road the packet forbids, refused by the act or by the
    // type rather than by a caller's discipline.

    fn is_uncorrelated_refusal(result: &crate::error::Result<resolved::Chain>) -> bool {
        matches!(
            result,
            Err(crate::error::DelightQLError::Semantic(
                crate::diagnostic::Semantic::Resolution(
                    crate::diagnostic::Resolution::CorrelationUncorrelatedPredicate { .. }
                )
            ))
        )
    }

    #[test]
    fn the_act_derives_the_occurrences_from_its_two_operands() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (chain, k, b, outer) = correlated(&registry);
        let Some(resolved::Continuation::Correlated(correlated)) =
            chain.continuations().last().map(resolved::Step::form)
        else {
            panic!("the act writes the correlated step")
        };
        assert!(
            correlated.reads(k),
            "the interior occurrence the condition read is owed"
        );
        assert!(
            !correlated.reads(b),
            "an interior occurrence the condition did not read is not owed"
        );
        assert!(
            !correlated.reads(outer),
            "the enclosing row's occurrence belongs to the enclosing row"
        );
        assert_eq!(owes(&registry, &chain.semantic_relation()), vec![k]);
    }

    #[test]
    fn a_condition_reading_no_interior_occurrence_refuses() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (chain, _, _) = read(&registry, "k", "b");
        let (_, first, second) = read(&registry, "ok", "oa");
        let refused = registry
            .authority()
            .correlate(chain, equality(first, second), proof());
        assert!(is_uncorrelated_refusal(&refused));
    }

    #[test]
    fn a_condition_reading_no_enclosing_occurrence_is_not_a_correlation() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (chain, k, b) = read(&registry, "k", "b");
        let refused = registry
            .authority()
            .correlate(chain, equality(k, b), proof());
        assert!(matches!(
            refused,
            Err(crate::error::DelightQLError::Internal(_))
        ));
    }

    #[test]
    fn a_correlated_step_lands_only_where_it_was_minted() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (read_chain, k, _) = read(&registry, "k", "b");
        let (_, outer, _) = read(&registry, "ok", "oa");
        // The correlation stands on a projection over the read.
        let step = projection_step(&registry, read_chain.semantic_relation(), k);
        let projected = registry
            .authority()
            .reland(read_chain.clone(), step)
            .expect("the projection lands on the read");
        let published = crate::relation::published_ports(&registry, &projected.semantic_relation())
            .expect("ports");
        let correlated = registry
            .authority()
            .correlate(projected, equality(published[0], outer), proof())
            .expect("the correlation act");
        // Taken off whole, the step's relation DESCENDS from the read the
        // projection stood on — and landing it there is refused anyway: the
        // obligation was recorded on the projection, not on its operand.
        let (_, step) = correlated
            .peel()
            .ok()
            .expect("the correlated step stands")
            .split();
        let refused = registry.authority().reland(read_chain, step);
        assert!(matches!(
            refused,
            Err(crate::error::DelightQLError::Internal(_))
        ));
    }

    #[test]
    fn a_correlated_step_is_not_a_transparent_form() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (chain, _, _, _) = correlated(&registry);
        let at = chain.continuations().len() - 1;
        assert!(!chain.continuations()[at].form().is_transparent());
        assert!(
            chain.clone().without(at).is_err(),
            "no pass takes a correlated restriction out of a chain"
        );
        // What comes off is not a payload `Chain::transparently` accepts, so
        // there is no road that puts it on another chain.
        let (_, step) = chain
            .peel()
            .ok()
            .expect("the correlated step stands")
            .split();
        assert!(resolved::Transparent::of(step.into_form()).is_err());
    }

    // THE RELATIONSHIP SURVIVES ITS REWRITE ENTRANCES. A crossing re-reads
    // what the condition names and refuses a change; a step assembly reads
    // the owner off the payload and refuses another step's result.

    fn correlated_payload(chain: resolved::Chain) -> super::Correlated<resolved::Resolved> {
        let (_, step) = chain
            .peel()
            .ok()
            .expect("the correlated step stands")
            .split();
        match step.into_form() {
            resolved::Continuation::Correlated(correlated) => correlated,
            _ => panic!("the outermost step is the correlated restriction"),
        }
    }

    #[test]
    fn a_crossing_that_changes_what_the_condition_reads_refuses() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (chain, _, b, outer) = correlated(&registry);
        let correlated = correlated_payload(chain);
        // The rewrite hands back a condition reading `b` where `k` was read:
        // the support the act derived no longer describes it.
        let changed = correlated.crossing::<resolved::Resolved>(|_| Ok(equality(b, outer)));
        assert!(matches!(
            changed,
            Err(crate::error::DelightQLError::Internal(_))
        ));
    }

    #[test]
    fn a_crossing_that_keeps_the_occurrences_carries_the_value_and_its_owner() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (chain, k, _, outer) = correlated(&registry);
        let standing = chain.semantic_relation();
        let correlated = correlated_payload(chain);
        // The same occurrences, spelled the other way round.
        let crossed = correlated
            .crossing::<resolved::Resolved>(|_| Ok(equality(outer, k)))
            .expect("a re-spelling keeps what the condition reads");
        assert!(crossed.reads(k));
        assert!(!crossed.reads(outer));
        assert_eq!(crossed.standing(), standing);
    }

    // THE RELATIONSHIP HOLDS TO THE ACTUAL OPERAND, AND THE KIND CANNOT BE
    // ERASED. A road that lands a step judges it against what the operand
    // it returns actually publishes, and a road that crosses a step crosses
    // the correlated kind itself, offering it to no hook.

    #[test]
    fn a_rebuild_that_replaces_the_operand_refuses() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (correlated_chain, _, _, _) = correlated(&registry);
        let (other, _, _) = read(&registry, "x", "y");
        let peeled = correlated_chain
            .peel()
            .ok()
            .expect("the correlated step stands");
        // The genuine payload and result are kept; the operand handed back
        // is another chain, whose relation the act never recorded.
        let rebuilt = peeled.rebuilding_arms(|_| Ok(other), Ok);
        assert!(matches!(
            rebuilt,
            Err(crate::error::DelightQLError::Internal(_))
        ));
        // The same rebuild over the operand it came off lands.
        let (correlated_chain, _, _, _) = correlated(&registry);
        let peeled = correlated_chain
            .peel()
            .ok()
            .expect("the correlated step stands");
        assert!(peeled.rebuilding_arms(Ok, Ok).is_ok());
    }

    // A REBUILD HANDS BACK WHAT IT WAS HANDED AT EVERY OPERAND POSITION —
    // the prefix above, and the operands nested INSIDE a node: a member's
    // arm, a bag step's arm, a derived table's subquery. Each road below
    // is handed a genuine chain and answers with another genuine chain; the
    // node above keeps its identity, so the carrier refuses the pairing.

    /// A join of the correlated read with another read, through the
    /// authority: the shape whose right arm is an operand nested in ONE
    /// node standing UNDER the correlation's owner.
    fn joined(registry: &Planning) -> resolved::Chain {
        let (left, _, _) = read(registry, "k", "b");
        let (right, _, _) = read(registry, "ok", "oa");
        let right_relation = right.semantic_relation();
        registry
            .authority()
            .extend(
                left,
                crate::relation::builder::StepOp::Join {
                    rhs: right,
                    correlation: resolved::MemberCorrelation::Cartesian(()),
                    join_type: None,
                    right: right_relation,
                    kind: super::super::form::JoinKind::Inner,
                    merged: &[],
                },
            )
            .expect("a join is built")
    }

    #[test]
    fn a_rebuild_that_replaces_a_members_arm_refuses() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (other, _, _) = read(&registry, "x", "y");
        let peeled = joined(&registry).peel().ok().expect("the member stands");
        // The prefix is handed back as it was; the ARM handed back is
        // another read, whose relation the join was never derived over.
        let rebuilt = peeled.rebuilding_arms(Ok, |_| Ok(other));
        assert!(matches!(
            rebuilt,
            Err(crate::error::DelightQLError::Internal(_))
        ));
        let peeled = joined(&registry).peel().ok().expect("the member stands");
        assert!(peeled.rebuilding_arms(Ok, Ok).is_ok());
    }

    #[test]
    fn a_rebuild_that_replaces_a_bag_arm_refuses() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (left, _, _) = read(&registry, "k", "b");
        let (right, _, _) = read(&registry, "ok", "oa");
        let (other, _, _) = read(&registry, "x", "y");
        let step = registry
            .authority()
            .set_step(
                crate::pipeline::asts::core::SetOperator::UnionAllPositional,
                &[left.semantic_relation(), right.semantic_relation()],
            )
            .expect("a set over two reads is built");
        let bagged = registry.authority().bag(left, step, right, None);
        let peeled = bagged.clone().peel().ok().expect("the bag step stands");
        let rebuilt = peeled.rebuilding_arms(Ok, |_| Ok(other));
        assert!(matches!(
            rebuilt,
            Err(crate::error::DelightQLError::Internal(_))
        ));
        let peeled = bagged.peel().ok().expect("the bag step stands");
        assert!(peeled.rebuilding_arms(Ok, Ok).is_ok());
    }

    /// A correlation standing on a join, through the authority: the shape
    /// whose obligated relation J was derived over an arm nested in the
    /// member step beneath the correlated restriction.
    fn correlated_join(registry: &Planning) -> resolved::Chain {
        let joined = joined(registry);
        let published = crate::relation::published_ports(registry, &joined.semantic_relation())
            .expect("the join's ports");
        let (_, outer, _) = read(registry, "x", "y");
        // The condition reads a position the RIGHT arm contributed.
        registry
            .authority()
            .correlate(joined, equality(published[2], outer), proof())
            .expect("the correlation act")
    }

    /// The review's walk: every payload identity, every relational subtree
    /// answered with one captured chain — the generic subtree hook, used
    /// to stand another operand where the join's arm stood.
    struct SubstituteOperand(resolved::Chain);

    impl crate::pipeline::ast_transform::AstTransform<resolved::Resolved, resolved::Resolved>
        for SubstituteOperand
    {
        crate::pipeline::ast_transform::same_phase_payload_folds!(resolved::Resolved);

        fn transform_relational_action(
            &mut self,
            _: resolved::Chain,
        ) -> crate::error::Result<crate::pipeline::ast_transform::FoldAction<resolved::Chain>>
        {
            Ok(crate::pipeline::ast_transform::FoldAction::Replaced(
                self.0.clone(),
            ))
        }
    }

    #[test]
    fn a_fold_that_substitutes_a_members_arm_under_a_correlation_refuses() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (other, _, _) = read(&registry, "c", "d");
        // The member keeps its result J and the correlation's owner is J:
        // what the walk replaced is the arm J was derived over, and the
        // carrier refuses the step before the correlation is ever judged.
        let changed = correlated_join(&registry).folded(&mut SubstituteOperand(other));
        assert!(matches!(
            changed,
            Err(crate::error::DelightQLError::Internal(_))
        ));
    }

    #[test]
    fn a_fold_that_substitutes_a_heads_body_refuses() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let boundary = interior_boundary(&registry);
        let (other, _, _) = read(&registry, "c", "d");
        // The same hook, reached through the head's payload: the derived
        // table's subquery comes back as another relation under the
        // boundary's kept identity.
        let changed = boundary.folded(&mut SubstituteOperand(other));
        assert!(matches!(
            changed,
            Err(crate::error::DelightQLError::Internal(_))
        ));
    }

    /// An interior boundary over the correlated read, through the
    /// authority.
    fn interior_boundary(registry: &Planning) -> resolved::Chain {
        let (body, _, _, _) = correlated(registry);
        boundary_over(registry, body)
    }

    fn boundary_over(registry: &Planning, body: resolved::Chain) -> resolved::Chain {
        let identifier = resolved::QualifiedName {
            namespace_path: resolved::NamespacePath::empty(),
            name: delightql_types::SqlIdentifier::new("t"),
        };
        let head = registry
            .authority()
            .boundary_head(
                resolved::GroundForm::Reference(resolved::Relation::InnerRelation {
                    pattern: resolved::InnerRelationPattern::Indeterminate {
                        identifier,
                        subquery: Box::new(body),
                    },
                    alias: None,
                    outer: false,
                }),
                crate::relation::builder::Boundary::Interior {
                    answer: registry.intern("t", false),
                },
            )
            .expect("the interior boundary");
        resolved::Chain::ground(head)
    }

    // THE REALIZATION ROAD. A walk stands a different body under a head's
    // kept identity only through the one hook that answers with an
    // interior, holding a replacement the authority judged FOR that body;
    // the carrier judges the answer against the body in hand.

    /// A walk whose realization hook answers every interior with one
    /// captured replacement — a realization judged for some other body.
    struct Realize(Option<crate::relation::Replacement<resolved::Resolved>>);

    impl crate::pipeline::ast_transform::AstTransform<resolved::Resolved, resolved::Resolved>
        for Realize
    {
        crate::pipeline::ast_transform::same_phase_payload_folds!(resolved::Resolved);

        fn realize_interior(
            &mut self,
            pattern: crate::pipeline::asts::core::expressions::relational::InnerRelationPattern<
                resolved::Resolved,
            >,
        ) -> crate::error::Result<
            crate::pipeline::asts::core::expressions::relational::Realized<resolved::Resolved>,
        > {
            use crate::pipeline::asts::core::expressions::relational::Realized;
            let resolved::InnerRelationPattern::Indeterminate { identifier, .. } = pattern else {
                panic!("the fixture's interior is indeterminate")
            };
            let replacement = self.0.take().expect("one interior is realized");
            Ok(Realized::replaced(replacement, |subquery| {
                resolved::InnerRelationPattern::Indeterminate {
                    identifier,
                    subquery,
                }
            }))
        }
    }

    #[test]
    fn a_realization_judged_for_another_body_refuses() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let boundary = interior_boundary(&registry);
        let (other, _, _) = read(&registry, "c", "d");
        // A genuine replacement, judged by the authority — for `other`.
        let of_other = registry
            .authority()
            .rebuilt(other, Ok)
            .expect("a rebuild handing its operand back is judged preserved");
        let changed = boundary.folded(&mut Realize(Some(of_other)));
        assert!(matches!(
            changed,
            Err(crate::error::DelightQLError::Internal(_))
        ));
    }

    #[test]
    fn a_realization_judged_for_the_body_it_stands_over_lands() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (body, _, _, _) = correlated(&registry);
        let of_body = registry
            .authority()
            .rebuilt(body.clone(), Ok)
            .expect("a rebuild handing its operand back is judged preserved");
        let boundary = boundary_over(&registry, body);
        let head_was = *boundary.head().result();
        let realized = boundary
            .folded(&mut Realize(Some(of_body)))
            .expect("a realization of the body the head stands over lands");
        assert_eq!(*realized.head().result(), head_was);
    }

    #[test]
    fn a_rebuild_that_publishes_beside_its_operand_refuses() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (chain, _, _) = read(&registry, "k", "b");
        let (other, _, _) = read(&registry, "c", "d");
        // The product is another read: no position of the operand landed
        // in it, so it is not a rebuild of the operand and mints no
        // replacement.
        let refused = registry.authority().rebuilt(chain, |_| Ok(other));
        assert!(matches!(
            refused,
            Err(crate::error::DelightQLError::Internal(_))
        ));
    }

    #[test]
    fn a_rebuild_that_replaces_a_heads_subquery_refuses() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let boundary = interior_boundary(&registry);
        let (other, _, _) = read(&registry, "x", "y");
        // The nested rewrite is handed the interior and answers with
        // another read; the boundary above was derived over no such thing.
        let rebuilt = boundary.clone().rebuilding(
            |_| Ok(other.clone()),
            |_, _| Ok(crate::pipeline::asts::core::Standing::Keep),
        );
        assert!(matches!(
            rebuilt,
            Err(crate::error::DelightQLError::Internal(_))
        ));
        assert!(boundary
            .rebuilding(Ok, |_, _| Ok(crate::pipeline::asts::core::Standing::Keep))
            .is_ok());
    }

    #[test]
    fn a_nodes_identity_crosses_by_the_phases_door_as_itself() {
        use crate::pipeline::asts::core::phases::carry_scope;
        use crate::pipeline::asts::core::{Refined, Resolved, Unresolved};
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (chain, _, _, _) = correlated(&registry);
        let owner = chain.semantic_relation();
        // The door takes the node's own identity and nothing else: within
        // the resolved phase and into the refined one it answers with that
        // identity, and there is no argument through which a caller could
        // offer another.
        assert_eq!(
            carry_scope::<Resolved, Resolved>(owner).expect("carried"),
            owner
        );
        assert_eq!(
            carry_scope::<Resolved, Refined>(owner).expect("carried"),
            owner
        );
        // A bound identity cannot enter the authored phase, and nothing
        // authored can cross into a bound phase: a relation is minted where
        // its node is resolved.
        assert!(carry_scope::<Resolved, Unresolved>(owner).is_err());
        assert!(carry_scope::<Unresolved, Resolved>(()).is_err());
    }

    // THE WALK HOLDS NO METHOD THAT ANSWERS WITH AN IDENTITY OR A STRUCTURE.
    // What a node publishes crosses through the phases' door
    // (`carry_scope`); what a node IS — its head form, its continuation, an
    // operator, a projection item, an ordering, a pattern — crosses by the
    // walk functions' own reconstruction, because the trait has no hook
    // that answers with any of them. The walks that once transplanted
    // another step's correlation, erased the correlated kind, demoted a
    // member to a restriction, or swapped a head's relation are not walks
    // anyone can write now. What a walk CAN do is rewrite a leaf, and the
    // witness below does the most a leaf rewrite can to a correlation —
    // re-spell its condition — and finds every identity, and the owner,
    // untouched.

    /// A walk whose truth hook swaps every comparison's sides.
    struct SwapSides;

    impl crate::pipeline::ast_transform::AstTransform<resolved::Resolved, resolved::Resolved>
        for SwapSides
    {
        crate::pipeline::ast_transform::same_phase_payload_folds!(resolved::Resolved);

        fn transform_boolean(
            &mut self,
            truth: resolved::TruthExpression,
        ) -> crate::error::Result<resolved::TruthExpression> {
            Ok(match truth {
                resolved::TruthExpression::Comparison(Comparison {
                    operator,
                    left,
                    right,
                }) => resolved::TruthExpression::Comparison(Comparison {
                    operator,
                    left: right,
                    right: left,
                }),
                other => crate::pipeline::ast_transform::walk_transform_boolean(self, other)?,
            })
        }
    }

    #[test]
    fn a_walk_rewrites_leaves_under_identities_and_structure_it_cannot_touch() {
        let registry = Planning::open(crate::names::Registry::new(&[]));
        let (chain, k, _, outer) = correlated(&registry);
        let head_was = *chain.head().result();
        let owner = chain.semantic_relation();
        let folded = chain
            .folded(&mut SwapSides)
            .expect("a leaf rewrite that keeps what the condition reads lands");
        assert_eq!(*folded.head().result(), head_was);
        assert_eq!(folded.semantic_relation(), owner);
        let Some(resolved::Continuation::Correlated(correlated)) =
            folded.continuations().last().map(resolved::Step::form)
        else {
            panic!("the correlated step stands")
        };
        assert!(correlated.reads(k));
        assert!(!correlated.reads(outer));
        assert_eq!(correlated.standing(), owner);
        // The re-spelling took: the outer occurrence now stands on the left.
        let resolved::TruthExpression::Comparison(Comparison { left, .. }) = correlated.condition()
        else {
            panic!("the condition is a comparison")
        };
        assert_eq!(**left, reference(outer));
    }
}
