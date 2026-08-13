// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! What a scope publishes, and the only road from that to a statement.
//!
//! A builder state and the statement it emits answer the same question —
//! which occurrences does this layer put on offer, in what order — and a
//! layer whose two answers differ emits SQL nothing can be checked against:
//! the alias claims a heading the body underneath does not output, and the
//! spelling is identical either way, so the text never shows it. One type
//! answers once. Entry and publication are the same value read twice, so
//! they cannot disagree.

use crate::error::{DelightQLError, Result};
use crate::names::{ColId, Registry, ScopeId};
use crate::pipeline::asts::core::ColumnMetadata;
use crate::pipeline::sql_ast::{
    DomainExpression, Publishes, QueryExpression, SelectBuilder, SelectItem, SelectStatement,
};

/// The publication a statement was proven to produce.
///
/// Not a badge saying a check happened somewhere — the fact itself, the
/// scope and the ordered outputs, so a reader can ask what was proven rather
/// than trust that something was. A token carrying nothing could be lifted
/// off one statement and stamped onto another; this one is re-checked
/// against whatever list it is applied to, so lifting it accomplishes
/// nothing.
///
/// The fields are private to this module, so the fact can only be stated by
/// the authority. Reading it is harmless and open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checked {
    at: ScopeId,
    outputs: Vec<ColId>,
}

impl Checked {
    /// The scope the statement carrying this was proven to produce.
    pub fn at(&self) -> ScopeId {
        self.at
    }

    /// Does this list produce exactly what was proven — the same
    /// occurrences, in the same order, no more and no fewer?
    ///
    /// The one comparison in the system. `Publication::check` asks it of a
    /// claim about to be proven, the statement doors ask it of a rebuild, and
    /// the answer means the same thing in both places because it is the same
    /// walk.
    pub fn verify(&self, items: &[SelectItem]) -> std::result::Result<(), String> {
        let mut produced = items.iter().flat_map(|item| match item.publishes() {
            Publishes::One(column) => vec![column],
            Publishes::Run(expansion) => expansion.to_vec(),
            Publishes::Nothing => Vec::new(),
        });
        for (slot, claimed) in self.outputs.iter().copied().enumerate() {
            match produced.next() {
                Some(published) if published == claimed => {}
                Some(published) => {
                    return Err(format!(
                        "a statement at {:?} publishes {published:?} in slot {slot}, where the \
                         publication carries {claimed:?}",
                        self.at
                    ))
                }
                None => {
                    return Err(format!(
                        "a statement at {:?} produces nothing for {claimed:?} in slot {slot}, \
                         which the publication carries",
                        self.at
                    ))
                }
            }
        }
        match produced.next() {
            None => Ok(()),
            Some(extra) => Err(format!(
                "a statement at {:?} publishes {extra:?} past every output the publication \
                 carries",
                self.at
            )),
        }
    }

    /// The fact an ordered heading states.
    pub(in crate::pipeline) fn stating(at: ScopeId, outputs: Vec<ColId>) -> Self {
        Self { at, outputs }
    }

    /// The fact a list states about itself, at `at`.
    ///
    /// Used by the doors that TRANSFORM a proven statement: what the
    /// transformed statement publishes is read off the transformed list, and
    /// what makes that a proof rather than a cache is the rule each door
    /// checks before recording it.
    pub(in crate::pipeline) fn of(at: ScopeId, items: &[SelectItem]) -> Self {
        Self {
            at,
            outputs: items
                .iter()
                .flat_map(|item| match item.publishes() {
                    Publishes::One(column) => vec![column],
                    Publishes::Run(expansion) => expansion.to_vec(),
                    Publishes::Nothing => Vec::new(),
                })
                .collect(),
        }
    }
}

/// Whether an intermediate wrap carries the hygienic columns it stands on.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) enum Hygiene {
    Drop,
    Carry,
}

/// Which scope owns what a publication lists.
///
/// One ordered representation of the column-bearing outputs a scope exposes.
///
/// Every builder state carries one; every statement is built from one. The
/// outputs are in heading order and include the hygienic ones — pruning is a
/// question asked of the view (`select_items`, `visible`), never a second
/// list kept beside this one.
#[derive(Debug, Clone)]
pub(in crate::pipeline) struct Publication {
    /// The scope this layer stands at. A parent names it by this — the
    /// subquery alias, the CTE reference — and a statement built from this
    /// publication is stamped with it.
    at: ScopeId,
    /// Everything on offer here, in heading order, hygienic included.
    outputs: Vec<ColumnMetadata>,
}

impl Publication {
    /// A layer publishing its own occurrences.
    ///
    /// Refuses an output owned elsewhere. That is the whole guarantee: a
    /// heading holding a foreign occurrence is a layer claiming to produce
    /// something it cannot name, and every later disagreement — the alias
    /// over a body that outputs different columns, the reference standing at
    /// a scope no FROM entry establishes — starts here. Refusing at
    /// construction means no such value exists to be passed on.
    pub(in crate::pipeline) fn at(
        at: ScopeId,
        outputs: Vec<ColumnMetadata>,
        identities: &Registry,
    ) -> Result<Self> {
        if let Some(stray) = outputs
            .iter()
            .map(ColumnMetadata::identity)
            .find(|column| identities.scope_of(*column) != at)
        {
            return Err(disagreement(format!(
                "{at:?} would publish {stray:?}, an occurrence of {:?}",
                identities.scope_of(stray)
            )));
        }
        Ok(Self { at, outputs })
    }

    /// A flat N-way join, whose visible outputs stay owned by the operand
    /// scopes its FROM tree publishes.
    ///
    /// Refuses an output owned by the join scope itself: the join carries no
    /// SQL alias of its own, so an occurrence minted into it is one no
    /// emitted FROM entry offers.
    pub(in crate::pipeline) fn over_operands(
        at: ScopeId,
        outputs: Vec<ColumnMetadata>,
        identities: &Registry,
    ) -> Result<Self> {
        if let Some(own) = outputs
            .iter()
            .map(ColumnMetadata::identity)
            .find(|column| identities.scope_of(*column) == at)
        {
            return Err(disagreement(format!(
                "the flat join at {at:?} would publish {own:?} as its own, but its FROM \
                 carries the operand aliases and nothing else"
            )));
        }
        Ok(Self { at, outputs })
    }

    /// The scope this layer stands at.
    pub(in crate::pipeline) fn at_scope(&self) -> ScopeId {
        self.at
    }

    /// Everything on offer, in heading order, hygienic included.
    pub(in crate::pipeline) fn outputs(&self) -> &[ColumnMetadata] {
        &self.outputs
    }

    /// The same list as bare occurrences — what a star standing over this
    /// heading expands to.
    pub(in crate::pipeline) fn identities_in_order(&self) -> Vec<ColId> {
        self.outputs.iter().map(ColumnMetadata::identity).collect()
    }

    /// Drop the hygienic outputs from the view.
    ///
    /// The prune answers the view, never the ownership heading: the Registry
    /// keeps the occurrence, because a reference the resolver bound to it has
    /// to reach it. A projection that emits the pruned list must publish the
    /// pruned heading, or the layer claims a column its own SELECT does not
    /// carry — the list and the view are pruned together or not at all.
    pub(in crate::pipeline) fn prune_hygienic(&mut self, identities: &Registry) {
        self.outputs
            .retain(|column| !is_hygienic(identities, column.identity()));
    }

    /// The outputs this view republished for `how`, each paired with what it
    /// stands for.
    ///
    /// The reason is the registry's, recorded once where the occurrence was
    /// minted. A carrier injected so a hoisted condition still names
    /// something is found by asking here, not by a list kept beside the
    /// tree: a second list is a second answer, and the two drift the moment
    /// a boundary republishes one and not the other.
    pub(in crate::pipeline) fn carriers(
        &self,
        how: crate::names::Republish,
        identities: &Registry,
    ) -> Vec<(ColId, ColId)> {
        self.outputs
            .iter()
            .map(ColumnMetadata::identity)
            .filter_map(|output| match identities.origin_of_col(output) {
                crate::names::ColumnOrigin::Republished { from, how: reason } if reason == how => {
                    Some((from, output))
                }
                _ => None,
            })
            .collect()
    }

    /// Explicit SELECT items for this view — every output named, nothing
    /// left to a star's expansion.
    ///
    /// Hygiene is dropped at a projection because no caller addresses it. An
    /// intermediate wrap is not that: something above still stands on the
    /// column — a JOIN ON, or a json expansion whose source IS the hygienic
    /// temp — and a wrap that drops it leaves that reader naming nothing.
    pub(in crate::pipeline) fn select_items(
        &self,
        identities: &Registry,
        hygiene: Hygiene,
    ) -> Vec<SelectItem> {
        self.outputs
            .iter()
            .map(ColumnMetadata::identity)
            .filter(|column| hygiene == Hygiene::Carry || !is_hygienic(identities, *column))
            .map(|column| SelectItem::Expression {
                expr: DomainExpression::Column(super::read_through_joins(identities, column)),
                alias: Some(column),
            })
            .collect()
    }

    /// Create a new publication and republish every output into it.
    pub(in crate::pipeline) fn requalified(
        &self,
        new_scope: ScopeId,
        identities: &Registry,
        how: crate::names::Republish,
    ) -> Result<Self> {
        let outputs = self
            .outputs
            .iter()
            .map(|column| {
                let source = column.identity();
                ColumnMetadata::new(identities.republish_column(
                    source,
                    new_scope,
                    how,
                    identities.published(source),
                    identities.addressing(source),
                    |_| {},
                ))
            })
            .collect();
        Self::at(new_scope, outputs, identities)
    }

    /// Republish across a subquery boundary, applying the SQL safety rename
    /// required for qualified references.
    pub(in crate::pipeline) fn requalified_for_subquery(
        &self,
        new_scope: ScopeId,
        identities: &Registry,
    ) -> Result<Self> {
        let outputs = self
            .outputs
            .iter()
            .map(|column| {
                let source = column.identity();
                ColumnMetadata::new(identities.republish_column(
                    source,
                    new_scope,
                    crate::names::Republish::Rename,
                    identities.qualified_safe_spelling(source),
                    identities.addressing(source),
                    |_| {},
                ))
            })
            .collect();
        Self::at(new_scope, outputs, identities)
    }

    /// Pair each output with the one the requalified publication minted for
    /// it, for rewriting the body that is about to sit underneath.
    pub(in crate::pipeline) fn pairs_with(&self, target: &Self) -> Vec<(ColId, ColId)> {
        self.outputs
            .iter()
            .zip(&target.outputs)
            .map(|(source, target)| (source.identity(), target.identity()))
            .collect()
    }

    /// Build the statement this publication stands for.
    ///
    /// The one road from a heading to a `SelectStatement`. It stamps the
    /// scope and checks what the list publishes against what this view
    /// claims, in one act — the two halves of the same fact, so a caller
    /// cannot perform one and forget the other.
    /// The list is compared against this view alone: which scope may own an
    /// output was settled when the publication was built, so there is no
    /// registry question left to ask here.
    pub(in crate::pipeline) fn publish(&self, select: SelectBuilder) -> Result<SelectStatement> {
        select
            .publishing(self.fact())
            .map_err(|message| disagreement(format!("statement finalization: {message}")))
    }

    /// The same check applied to a statement built elsewhere and adopted.
    ///
    /// A finished query reaching a builder is claimed to publish this view;
    /// the claim is checked here rather than believed.
    pub(in crate::pipeline) fn check_query(&self, query: &QueryExpression) -> Result<()> {
        match query {
            QueryExpression::Select(statement) => {
                if statement.at() != self.at {
                    return Err(disagreement(format!(
                        "a statement produced at {:?} is offered as the body of {:?}",
                        statement.at(),
                        self.at
                    )));
                }
                self.check(statement.select_list())
            }
            // A union publishes once. Its first branch carries the heading;
            // every branch after it fills those slots under the alignment
            // contract, which is a different obligation and not a weaker
            // reading of the same one.
            QueryExpression::SetOperation { left, right, .. } => {
                self.check_query(left)?;
                match right.as_ref() {
                    QueryExpression::Select(arm) => Alignment::with(self).check(arm.select_list()),
                    other => self.check_query(other),
                }
            }
            QueryExpression::WithCte { query, .. } => self.check_query(query),
            // A VALUES body names nothing, so it fills the claimed slots by
            // position — the alignment discipline, applied to rows instead of
            // a select list. Passing it unexamined let a body of any width
            // stand for a heading of any other.
            QueryExpression::Values { rows } => {
                for (index, row) in rows.iter().enumerate() {
                    if row.len() != self.outputs.len() {
                        return Err(disagreement(format!(
                            "row {index} of a VALUES body offers {} slots to fill the {} \
                             {:?} carries",
                            row.len(),
                            self.outputs.len(),
                            self.at
                        )));
                    }
                }
                // No row establishes no width, so nothing here can stand for
                // a heading that carries outputs.
                if rows.is_empty() && !self.outputs.is_empty() {
                    return Err(disagreement(format!(
                        "an empty VALUES body fills none of the {} slots {:?} carries",
                        self.outputs.len(),
                        self.at
                    )));
                }
                Ok(())
            }
        }
    }

    /// What a select list publishes must be what this view says it does —
    /// the same occurrences, in the same order, no more and no fewer.
    ///
    /// Ownership does not enter here. Which scope may own an output is
    /// settled when the publication is built; what is settled here is that
    /// the statement produces the outputs the publication carries. Testing
    /// ownership again would accept a same-scope occurrence this view never
    /// offered, and testing membership would accept the view's outputs
    /// dropped, reordered, or emitted twice.
    ///
    /// A star is compared like everything else, because it carries what it
    /// stands for. Deriving its run from the heading being checked — the
    /// outputs the named items happen to leave over — would let one `SELECT *`
    /// satisfy any claim at all, since the only fact consulted would be the
    /// claim.
    fn check(&self, items: &[SelectItem]) -> Result<()> {
        self.fact().verify(items).map_err(disagreement)
    }

    /// This view stated as the fact a proven statement carries.
    pub(in crate::pipeline) fn fact(&self) -> Checked {
        Checked::stating(self.at, self.identities_in_order())
    }
}

/// What a later set-operation arm owes the heading it joins.
///
/// SQL takes a union's output names from its first branch, so a later arm
/// names nothing of its own: it fills the same slots, in the same order, and
/// the publication belongs to the set expression's result rather than to the
/// arm. What it reads may be an occurrence of any scope in view — an outer
/// one under correlation included — so there is no heading here to check
/// against. The contract is alignment, and alignment is what is checked.
pub(in crate::pipeline) struct Alignment<'a> {
    /// The result's publication, whose slots this arm fills.
    with: &'a Publication,
}

impl<'a> Alignment<'a> {
    pub(in crate::pipeline) fn with(publication: &'a Publication) -> Self {
        Self { with: publication }
    }

    /// Build this arm, checked against the contract.
    pub(in crate::pipeline) fn align(&self, select: SelectBuilder) -> Result<SelectStatement> {
        // An arm does not publish the heading, so it is not built by proving
        // it. The contract is checked first; the fact the arm then carries is
        // what it produces at the result's scope, which is what a later
        // rebuild of this arm will be held to.
        self.check(select.items())?;
        let fact = Checked::of(self.with.at, select.items());
        select
            .publishing(fact)
            .map_err(|message| disagreement(format!("set-operation arm: {message}")))
    }

    /// What an arm owes, whichever way it is spelled.
    ///
    /// Two spellings satisfy the contract and both are correct SQL. An arm
    /// may republish the heading under the result's own occurrences — that is
    /// the exact-agreement check, unchanged — or it may spell nothing and
    /// fill the slots by position, which is what a row whose value READS an
    /// outer column has to do.
    ///
    /// Naming some slots and not others is neither. A union takes its output
    /// names from the first branch, so a partly-named arm has half a claim,
    /// and which half survives is the engine's business rather than the
    /// language's.
    pub(in crate::pipeline) fn check(&self, items: &[SelectItem]) -> Result<()> {
        let named = items
            .iter()
            .filter(|item| matches!(item, SelectItem::Expression { alias: Some(_), .. }))
            .count();
        if named == items.len() && !items.is_empty() {
            return self.with.check(items);
        }
        if named != 0 {
            return Err(disagreement(format!(
                "a set-operation arm names {named} of its {} slots; a union takes its output \
                 names from the first branch, so an arm names all of them or none",
                items.len()
            )));
        }
        // Width is the columns the arm EMITS, not how many items it holds: a
        // star is one item and any number of columns.
        let width: usize = items.iter().map(|item| item.publishes().slots()).sum();
        if width != self.with.outputs.len() {
            return Err(disagreement(format!(
                "a set-operation arm offers {width} slots to align with the {} its heading \
                 carries",
                self.with.outputs.len()
            )));
        }
        Ok(())
    }
}

/// Publish a statement for a heading assembled outside a builder state.
///
/// The lowering sites that stand a resolver-decided heading at its scope and
/// emit the statement for it take the road the builder's own states take:
/// state what is published, then check the list against it. A site whose list
/// has drifted from the heading it was lowered for is refused here rather
/// than emitted for a later reader to be confused by.
pub(in crate::pipeline) fn publish_at(
    at: ScopeId,
    outputs: impl IntoIterator<Item = ColId>,
    select: SelectBuilder,
    identities: &Registry,
) -> Result<SelectStatement> {
    let outputs = outputs.into_iter().map(ColumnMetadata::new).collect();
    Publication::at(at, outputs, identities)?.publish(select)
}

/// The carriers a scope publishes so a condition hoisted out of it still
/// names something, each paired with what it stands for.
///
/// The whole answer comes off the registry: a carrier is an occurrence of
/// this scope republished for [`crate::names::Republish::Correlation`]. The
/// refiner mints them and the transformer reads them, and because both ask
/// here neither carries a list the other could contradict.
pub(in crate::pipeline) fn correlation_carriers(
    scope: ScopeId,
    identities: &Registry,
) -> Result<Vec<(ColId, ColId)>> {
    let outputs = identities
        .known_heading(scope)
        .expect("a heading this test built is known")
        .iter()
        .copied()
        .map(ColumnMetadata::new)
        .collect();
    Ok(Publication::at(scope, outputs, identities)?
        .carriers(crate::names::Republish::Correlation, identities))
}

fn is_hygienic(identities: &Registry, column: ColId) -> bool {
    identities.addressing(column) == crate::names::Addressing::Hygienic
}

fn disagreement(message: String) -> DelightQLError {
    DelightQLError::ParseError {
        message: format!("publication: {message}"),
        source: None,
        subcategory: None,
    }
}

#[cfg(test)]
mod tests {
    //! What the authority guarantees, and that nothing goes around it.

    use super::*;
    use crate::names::{
        Addressing, ColumnOrigin, Hint, Registry, Republish, ScopeOrigin, ValueFacts,
    };

    fn registry() -> Registry {
        Registry::new(&[])
    }

    /// A base table with one column, and a scope standing over it.
    fn base(reg: &Registry, name: &str) -> (ScopeId, ColId) {
        let entity = reg.mint_entity(reg.intern(name, false));
        let scope = reg.mint_scope(ScopeOrigin::BaseTable { entity }, Hint::None, None);
        let column = reg.mint_column(
            scope,
            ColumnOrigin::CatalogColumn {
                entity,
                position: 0,
            },
            Some(reg.intern("id", false)),
            Addressing::Published,
            ValueFacts::default(),
        );
        (scope, column)
    }

    fn wrap(reg: &Registry, input: ScopeId) -> ScopeId {
        reg.mint_scope(ScopeOrigin::PipeStage { input }, Hint::None, None)
    }

    #[test]
    fn a_heading_holding_a_foreign_occurrence_refuses() {
        let reg = registry();
        let (base_scope, column) = base(&reg, "t");
        let over = wrap(&reg, base_scope);
        let error = Publication::at(over, vec![ColumnMetadata::new(column)], &reg)
            .expect_err("a layer cannot publish an occurrence it does not own");
        assert!(
            error.to_string().contains("an occurrence of"),
            "the refusal must name whose occurrence it is: {error}"
        );
    }

    #[test]
    fn a_flat_join_refuses_to_own_what_its_from_cannot_offer() {
        let reg = registry();
        let (base_scope, _) = base(&reg, "t");
        let join = wrap(&reg, base_scope);
        let minted = reg.mint_column(
            join,
            ColumnOrigin::Computed {
                via: crate::names::Computation::Operator,
            },
            None,
            Addressing::Published,
            ValueFacts::default(),
        );
        let error = Publication::over_operands(join, vec![ColumnMetadata::new(minted)], &reg)
            .expect_err("a flat join's FROM carries the operand aliases and nothing else");
        assert!(error.to_string().contains("as its own"), "{error}");
    }

    /// The entry and published laws are one law: what a publication claims is
    /// what its statement offers, and the second cannot drift from the first
    /// because the same value answers both.
    #[test]
    fn a_statement_publishing_an_occurrence_the_view_does_not_offer_refuses() {
        let reg = registry();
        let (base_scope, column) = base(&reg, "t");
        let over = wrap(&reg, base_scope);
        let mine = reg.republish_column(
            column,
            over,
            Republish::Passthrough,
            reg.published(column),
            Addressing::Published,
            |_| {},
        );
        let publication =
            Publication::at(over, vec![ColumnMetadata::new(mine)], &reg).expect("owned heading");

        // The list publishes the INNER occurrence — the same spelling, a
        // different column, and nothing in the SQL text tells them apart.
        let strayed = SelectBuilder::new().select_all(vec![SelectItem::Expression {
            expr: DomainExpression::Column(column),
            alias: Some(column),
        }]);
        let error = publication
            .publish(strayed)
            .expect_err("the statement offers what the view does not");
        assert!(error.to_string().contains("in slot 0"), "{error}");

        let agreeing = SelectBuilder::new().select_all(vec![SelectItem::Expression {
            expr: DomainExpression::Column(column),
            alias: Some(mine),
        }]);
        publication
            .publish(agreeing)
            .expect("the list and the view agree");
    }

    /// A publication carrying `[a, b]` is the claim that the statement
    /// produces a then b. Every way of failing that claim is a way of
    /// emitting SQL a consumer's qualification cannot be checked against.
    #[test]
    fn a_list_that_is_not_the_claimed_heading_refuses() {
        let reg = registry();
        let (base_scope, source) = base(&reg, "t");
        let over = wrap(&reg, base_scope);
        let slot = |name: &str| {
            reg.republish_column(
                source,
                over,
                Republish::Rename,
                Some(reg.intern(name, false)),
                Addressing::Published,
                |_| {},
            )
        };
        let a = slot("a");
        let b = slot("b");
        let stray = slot("c");
        let publication = Publication::at(
            over,
            vec![ColumnMetadata::new(a), ColumnMetadata::new(b)],
            &reg,
        )
        .expect("owned heading");

        let list = |columns: &[ColId]| {
            SelectBuilder::new().select_all(
                columns
                    .iter()
                    .map(|column| SelectItem::Expression {
                        expr: DomainExpression::Column(*column),
                        alias: Some(*column),
                    })
                    .collect(),
            )
        };

        publication
            .publish(list(&[a, b]))
            .expect("the list is the heading");

        for (name, columns, expected) in [
            ("missing", &[a][..], "produces nothing for"),
            ("reordered", &[b, a][..], "in slot 0"),
            ("duplicated", &[a, a][..], "in slot 1"),
            ("unexpected", &[a, b, stray][..], "past every output"),
            // Same scope, never offered: ownership would have let it pass.
            ("substituted", &[a, stray][..], "in slot 1"),
        ] {
            let Err(error) = publication.publish(list(columns)) else {
                panic!("{name} must refuse");
            };
            assert!(error.to_string().contains(expected), "{name}: {error}");
        }
    }

    /// An unnamed expression puts a value in the row under no name, so
    /// nothing can address the slot — but that is a stated contribution, not
    /// an item the comparison passes over. A claim of nothing still refuses a
    /// list that names something.
    #[test]
    fn an_empty_claim_takes_unnamed_values_and_refuses_named_ones() {
        let reg = registry();
        let (base_scope, column) = base(&reg, "t");
        let nothing = Publication::at(wrap(&reg, base_scope), Vec::new(), &reg)
            .expect("a layer may publish nothing");

        nothing
            .publish(SelectBuilder::new().select_all(vec![SelectItem::expression(
                DomainExpression::literal(crate::pipeline::asts::core::LiteralValue::Number(
                    "1".to_string(),
                )),
            )]))
            .expect("SELECT 1 offers no heading");

        let error = nothing
            .publish(
                SelectBuilder::new().select_all(vec![SelectItem::Expression {
                    expr: DomainExpression::Column(column),
                    alias: None,
                }]),
            )
            .expect_err("a bare column is output under its own name");
        assert!(error.to_string().contains("past every output"), "{error}");
    }

    /// A star is compared against what it carries, not against what the
    /// claim would need it to be.
    ///
    /// This is the difference between a proof and a restatement: a star whose
    /// run is inferred from "the outputs the named items leave over" agrees
    /// with every claim by construction, because the only fact consulted is
    /// the claim.
    #[test]
    fn a_star_whose_expansion_disagrees_with_the_claim_refuses() {
        let reg = registry();
        let (base_scope, source) = base(&reg, "t");
        let over = wrap(&reg, base_scope);
        let slot = |name: &str| {
            reg.republish_column(
                source,
                over,
                Republish::Rename,
                Some(reg.intern(name, false)),
                Addressing::Published,
                |_| {},
            )
        };
        let a = slot("a");
        let b = slot("b");
        let tail = slot("tail");
        let publication = Publication::at(
            over,
            vec![ColumnMetadata::new(a), ColumnMetadata::new(b)],
            &reg,
        )
        .expect("owned heading");

        let named = |column| SelectItem::Expression {
            expr: DomainExpression::Column(column),
            alias: Some(column),
        };
        let star_over =
            |columns: Vec<ColId>| SelectBuilder::new().select_all(vec![SelectItem::star(columns)]);

        publication
            .publish(star_over(vec![a, b]))
            .expect("the star stands for exactly the heading");

        // Every one of these is a `SELECT *` that the old arithmetic would
        // have accepted, because it never asked the star what it covered.
        for (name, expansion, expected) in [
            ("short", vec![a], "produces nothing for"),
            ("long", vec![a, b, tail], "past every output"),
            ("reordered", vec![b, a], "in slot 0"),
            ("foreign", vec![a, tail], "in slot 1"),
            ("empty", vec![], "produces nothing for"),
        ] {
            let Err(error) = publication.publish(star_over(expansion)) else {
                panic!("a star expanding to {name} must refuse");
            };
            assert!(error.to_string().contains(expected), "{name}: {error}");
        }

        // An empty claim is not a licence for a star that covers something.
        let nothing =
            Publication::at(wrap(&reg, base_scope), Vec::new(), &reg).expect("publishes nothing");
        let error = nothing
            .publish(star_over(vec![a]))
            .expect_err("a star covering an output cannot serve an empty claim");
        assert!(error.to_string().contains("past every output"), "{error}");
        nothing
            .publish(SelectBuilder::new().select_all(vec![SelectItem::star_over_nothing()]))
            .expect("a star over nothing serves a claim of nothing");

        // Nothing about the arithmetic remains: two stars are exact when both
        // carry what they cover.
        publication
            .publish(
                SelectBuilder::new()
                    .select_all(vec![SelectItem::star(vec![a]), SelectItem::star(vec![b])]),
            )
            .expect("two stars are as exact as one when each carries its run");
        let error = publication
            .publish(SelectBuilder::new().select_all(vec![SelectItem::star(vec![a]), named(a)]))
            .expect_err("the second slot is b");
        assert!(error.to_string().contains("in slot 1"), "{error}");
    }

    /// A VALUES body names nothing, so it fills the claimed slots by
    /// position. Passing it unexamined let a body of any width stand for a
    /// heading of any other.
    #[test]
    fn a_values_body_fills_the_claimed_slots_by_position() {
        let reg = registry();
        let (base_scope, source) = base(&reg, "t");
        let over = wrap(&reg, base_scope);
        let slot = |name: &str| {
            reg.republish_column(
                source,
                over,
                Republish::Rename,
                Some(reg.intern(name, false)),
                Addressing::Published,
                |_| {},
            )
        };
        let publication = Publication::at(
            over,
            vec![
                ColumnMetadata::new(slot("a")),
                ColumnMetadata::new(slot("b")),
            ],
            &reg,
        )
        .expect("owned heading");

        let value = || {
            DomainExpression::literal(crate::pipeline::asts::core::LiteralValue::Number(
                "1".to_string(),
            ))
        };
        publication
            .check_query(&QueryExpression::Values {
                rows: vec![vec![value(), value()], vec![value(), value()]],
            })
            .expect("every row fills both slots");

        let error = publication
            .check_query(&QueryExpression::Values {
                rows: vec![vec![value(), value()], vec![value()]],
            })
            .expect_err("a narrow row fills the wrong number of slots");
        assert!(error.to_string().contains("row 1"), "{error}");

        let error = publication
            .check_query(&QueryExpression::Values { rows: vec![] })
            .expect_err("no row establishes no width");
        assert!(error.to_string().contains("empty VALUES"), "{error}");
    }

    /// A later set-operation arm has an alignment contract, not a heading of
    /// its own: it fills the first branch's slots. A correlated fold's arm
    /// READS an outer column — the row IS that column — so there is nothing
    /// here to check ownership against, and the two obligations that remain
    /// are width and silence.
    #[test]
    fn a_later_arm_fills_the_slots_and_names_none_of_them() {
        let reg = registry();
        let (outer_scope, outer) = base(&reg, "users");
        let result = wrap(&reg, outer_scope);
        let published = reg.republish_column(
            outer,
            result,
            Republish::Passthrough,
            reg.published(outer),
            Addressing::Published,
            |_| {},
        );
        let publication = Publication::at(result, vec![ColumnMetadata::new(published)], &reg)
            .expect("owned heading");
        let arm = Alignment::with(&publication);

        let reading = |alias| {
            SelectBuilder::new().select_all(vec![SelectItem::Expression {
                expr: DomainExpression::Column(outer),
                alias,
            }])
        };

        arm.align(reading(None))
            .expect("an arm reading an outer column fills the slot");
        arm.align(reading(Some(published)))
            .expect("an arm may instead republish the heading under its own aliases");

        let error = arm
            .align(SelectBuilder::new().select_all(vec![
                SelectItem::expression(DomainExpression::Column(outer)),
                SelectItem::expression(DomainExpression::Column(outer)),
            ]))
            .expect_err("an arm of the wrong width aligns to nothing");
        assert!(error.to_string().contains("2 slots"), "{error}");

        // Naming the wrong occurrence is not saved by the arm being named:
        // a named arm answers to the exact-agreement check like any other.
        let error = arm
            .align(reading(Some(outer)))
            .expect_err("a named arm must name what the heading carries");
        assert!(error.to_string().contains("in slot 0"), "{error}");

        // Half a claim is neither spelling, and which half an engine honours
        // is not the language's to leave open.
        let two_wide = Publication::at(
            result,
            vec![
                ColumnMetadata::new(published),
                ColumnMetadata::new(reg.republish_column(
                    outer,
                    result,
                    Republish::Rename,
                    Some(reg.intern("second", false)),
                    Addressing::Published,
                    |_| {},
                )),
            ],
            &reg,
        )
        .expect("owned heading");
        let error = Alignment::with(&two_wide)
            .align(SelectBuilder::new().select_all(vec![
                SelectItem::Expression {
                    expr: DomainExpression::Column(outer),
                    alias: Some(published),
                },
                SelectItem::expression(DomainExpression::Column(outer)),
            ]))
            .expect_err("an arm names all of its slots or none");
        assert!(error.to_string().contains("names 1 of its 2"), "{error}");
    }

    /// Pruning hygiene answers the view, never the ownership heading: the
    /// occurrence stays in the Registry so a reference bound to it can still
    /// reach it.
    #[test]
    fn pruning_hygiene_leaves_the_registry_heading_alone() {
        let reg = registry();
        let (base_scope, column) = base(&reg, "t");
        let over = wrap(&reg, base_scope);
        let visible = reg.republish_column(
            column,
            over,
            Republish::Passthrough,
            reg.published(column),
            Addressing::Published,
            |_| {},
        );
        let carrier = reg.republish_column(
            column,
            over,
            Republish::Correlation,
            None,
            Addressing::Hygienic,
            |_| {},
        );
        let mut publication = Publication::at(
            over,
            vec![ColumnMetadata::new(visible), ColumnMetadata::new(carrier)],
            &reg,
        )
        .expect("owned heading");

        assert_eq!(
            publication.carriers(Republish::Correlation, &reg),
            vec![(column, carrier)],
            "the carrier's source and its reason are read off the registry, not a list \
             beside it"
        );
        assert_eq!(
            publication.select_items(&reg, Hygiene::Carry).len(),
            2,
            "an intermediate wrap carries what something above stands on"
        );

        publication.prune_hygienic(&reg);
        assert_eq!(publication.outputs().len(), 1);
        assert!(
            reg.known_heading(over)
                .expect("a heading this test built is known")
                .iter()
                .any(|column| *column == carrier),
            "the ownership heading keeps the carrier the view dropped"
        );
    }

    /// The carrier's reason is what tells it apart from every other hygienic
    /// republication, and it lives in the registry. A join's merged USING
    /// column is hygienic too; reading addressing alone would answer with
    /// both, which is why the sidecar existed.
    #[test]
    fn only_a_correlation_carrier_answers_as_one() {
        let reg = registry();
        let (base_scope, column) = base(&reg, "t");
        let over = wrap(&reg, base_scope);
        let carrier = reg.republish_column(
            column,
            over,
            Republish::Correlation,
            None,
            Addressing::Hygienic,
            |_| {},
        );
        let merged = reg.republish_column(
            column,
            over,
            Republish::Passthrough,
            reg.published(column),
            Addressing::Hygienic,
            |_| {},
        );
        assert_eq!(
            correlation_carriers(over, &reg).expect("owned heading"),
            vec![(column, carrier)],
            "the merged column is hygienic and is not a carrier: {merged:?}"
        );
    }

    /// Evidence is minted in exactly one place.
    ///
    /// `SelectBuilder::publishing` is the door that takes fresh evidence, and
    /// [`Checked`] can only be made here — so the compiler already stops a
    /// lowering site from stamping a scope unproven, and the reconstruction
    /// doors below the transformer carry evidence off the statement they are
    /// reshaping instead of making their own.
    ///
    /// A fact borrowed from another statement buys nothing.
    ///
    /// The old token was a unit badge: lift it off any statement, hand it to
    /// the build door, and an unrelated list was stamped as proven. The fact
    /// now names a scope and an ordered heading, and the door re-checks it
    /// against the list it is applied to, so borrowing one only means being
    /// refused in its terms.
    #[test]
    fn a_fact_lifted_from_elsewhere_does_not_stamp_a_statement() {
        let reg = registry();
        let (base_scope, source) = base(&reg, "t");
        let mine = wrap(&reg, base_scope);
        let theirs = wrap(&reg, base_scope);
        let republish = |into| {
            reg.republish_column(
                source,
                into,
                Republish::Passthrough,
                reg.published(source),
                Addressing::Published,
                |_| {},
            )
        };
        let ours = republish(mine);
        let elsewhere = Publication::at(theirs, vec![ColumnMetadata::new(republish(theirs))], &reg)
            .expect("owned heading");

        let list = || {
            SelectBuilder::new().select_all(vec![SelectItem::Expression {
                expr: DomainExpression::Column(source),
                alias: Some(ours),
            }])
        };

        Publication::at(mine, vec![ColumnMetadata::new(ours)], &reg)
            .expect("owned heading")
            .publish(list())
            .expect("our own fact describes our own list");

        let error = list()
            .publishing(elsewhere.fact())
            .expect_err("another statement's fact does not describe this list");
        assert!(error.contains("in slot 0"), "{error}");
    }

    /// A proven statement cannot be edited out from under its proof.
    ///
    /// `produced_at` and `select_list_mut` moved a statement's scope and
    /// outputs while the evidence sat still. The doors that replaced them
    /// move the fact in the same act, and each is held to a stated rule
    /// rather than to a badge.
    #[test]
    fn every_road_that_moves_a_statement_moves_its_fact() {
        let reg = registry();
        let (base_scope, source) = base(&reg, "t");
        let inner = wrap(&reg, base_scope);
        let outer = wrap(&reg, base_scope);
        let republish = |into| {
            reg.republish_column(
                source,
                into,
                Republish::Passthrough,
                reg.published(source),
                Addressing::Published,
                |_| {},
            )
        };
        let held = republish(inner);
        let moved = republish(outer);

        let mut statement = Publication::at(inner, vec![ColumnMetadata::new(held)], &reg)
            .expect("owned heading")
            .publish(
                SelectBuilder::new().select_all(vec![SelectItem::Expression {
                    expr: DomainExpression::Column(source),
                    alias: Some(held),
                }]),
            )
            .expect("proven at the inner scope");

        // Before: the statement answers to the inner view and to nothing else.
        let over =
            Publication::at(outer, vec![ColumnMetadata::new(moved)], &reg).expect("owned heading");
        over.check_query(&QueryExpression::Select(Box::new(statement.clone())))
            .expect_err("it has not been republished yet");

        statement
            .republish(outer, |output| {
                (output == held)
                    .then_some(moved)
                    .ok_or_else(|| "unpaired".to_string())
            })
            .expect("every slot is answered");

        // After: the fact moved with the scope and the outputs, so the view it
        // now belongs to accepts it and the one it left does not.
        over.check_query(&QueryExpression::Select(Box::new(statement.clone())))
            .expect("the statement now produces the outer heading");
        let mut rebuilt = SelectBuilder::new();
        for item in statement.select_list() {
            rebuilt = rebuilt.select(item.clone());
        }
        rebuilt
            .rebuilding(&statement)
            .expect("an untouched rebuild answers to the fact it carries");

        // A rebuild that moves an output is held to the fact, not to the list
        // it happens to be replacing.
        let error = SelectBuilder::new()
            .select_all(vec![SelectItem::Expression {
                expr: DomainExpression::Column(source),
                alias: Some(held),
            }])
            .rebuilding(&statement)
            .expect_err("the fact says this statement publishes the outer occurrence");
        assert!(error.contains("in slot 0"), "{error}");
    }

    /// A restructuring says what it may change, and is held to it.
    #[test]
    fn a_restructuring_may_not_rename_what_the_statement_named() {
        let reg = registry();
        let (base_scope, source) = base(&reg, "t");
        let above = wrap(&reg, base_scope);
        let below = wrap(&reg, base_scope);
        let named = reg.republish_column(
            source,
            above,
            Republish::Passthrough,
            reg.published(source),
            Addressing::Published,
            |_| {},
        );
        let statement = Publication::at(above, vec![ColumnMetadata::new(named)], &reg)
            .expect("owned heading")
            .publish(
                SelectBuilder::new().select_all(vec![SelectItem::Expression {
                    expr: DomainExpression::Column(source),
                    alias: Some(named),
                }]),
            )
            .expect("proven above");

        // Legitimate: the same slot, the same name, reading what is now below.
        SelectBuilder::new()
            .select_all(vec![SelectItem::Expression {
                expr: DomainExpression::Column(source),
                alias: Some(named),
            }])
            .restructuring(below, &statement)
            .expect("an unwrap keeps every name and every column");

        let error = SelectBuilder::new()
            .select_all(vec![SelectItem::expression(DomainExpression::Column(
                source,
            ))])
            .restructuring(below, &statement)
            .expect_err("dropping the name is not restructuring");
        assert!(error.contains("does not name what it named"), "{error}");

        let error = SelectBuilder::new()
            .select_all(vec![
                SelectItem::Expression {
                    expr: DomainExpression::Column(source),
                    alias: Some(named),
                },
                SelectItem::expression(DomainExpression::Column(source)),
            ])
            .restructuring(below, &statement)
            .expect_err("widening the row is not restructuring");
        assert!(error.contains("does not name what it named"), "{error}");
    }

    /// The fields of the fact are the authority's to write.
    ///
    /// A private field already stops another module from constructing one,
    /// and no statement hands one out. What a type cannot say is WHERE the
    /// stating lives, so the sweep says it, and no directory is exempt.
    #[test]
    fn nothing_outside_the_authority_states_a_fact() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/pipeline");
        let mut stating = Vec::new();
        let mut stack = vec![root.clone()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("pipeline source is readable") {
                let path = entry.expect("readable entry").path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().is_none_or(|ext| ext != "rs") {
                    continue;
                }
                let relative = path
                    .strip_prefix(&root)
                    .expect("under pipeline")
                    .display()
                    .to_string();
                if relative.ends_with("transformer/builder/publication.rs") {
                    continue;
                }
                let source = std::fs::read_to_string(&path).expect("readable source");
                // `Checked::of` recomputes a fact from a list a door has just
                // checked, so the two statement doors in `sql_ast` reach
                // it; `Checked::stating` writes a heading down as a fact, and
                // that is the authority's alone.
                if source.contains("Checked::stating") || source.contains("Checked {") {
                    stating.push(relative.clone());
                }
                if source.contains("Checked::of") && !relative.starts_with("sql_ast/") {
                    stating.push(format!("{relative} (recomputes a fact outside a door)"));
                }
            }
        }
        assert!(
            stating.is_empty(),
            "these state publication facts outside the authority: {stating:?}"
        );
    }
}
