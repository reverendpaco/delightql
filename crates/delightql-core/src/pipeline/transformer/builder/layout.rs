// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Physical SQL columns and their total semantic-site binding.

use crate::error::{DelightQLError, Result};
use crate::names::{ColId, Registry, ScopeId};
use crate::pipeline::asts::core::ColumnMetadata;
use crate::pipeline::sql_ast::{DomainExpression, SelectBuilder, SelectItem, SelectStatement};

/// Whether an intermediate wrap carries the hygienic columns it stands on.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) enum Hygiene {
    Drop,
    Carry,
}

/// WHAT A SQL LAYER DID TO THE SITE BENEATH IT.
///
/// One vocabulary, so a layer states the transformation and the binding
/// authority owns every one of them. A layer that added a transformation of
/// its own beside these would be a second physical road.
pub(in crate::pipeline) enum Resite<'a> {
    /// The layer emits everything beneath it and more.
    Extended,
    /// The layer emits the selected positions of what is beneath it.
    Projected(&'a [usize]),
    /// The layer emits positions taken from the ones named, in order.
    Reshaped(&'a [Option<usize>]),
    /// The layer emits the same positions under new spellings.
    Aliased(&'a [ColId]),
    /// The layer re-emits what is beneath it, position for position.
    Rebound,
}

#[derive(Debug, Clone)]
pub(in crate::pipeline) struct SqlLayout {
    /// The scope this layer stands at. A parent names it by this — the
    /// subquery alias, the CTE reference — and a statement built from this
    /// publication is stamped with it.
    at: ScopeId,
    /// Everything on offer here, in heading order, hygienic included.
    outputs: Vec<ColumnMetadata>,
    /// THE PHYSICAL SLOT IDENTITY OF EVERY POSITION THIS LAYER EMITS.
    ///
    /// TOTAL. A layout is bound where it is built, so a SQL position
    /// without a slot is not a state this type can be in; a semantic
    /// binding REPLACES the physical one where a relation is realized,
    /// and there is no absence for a later reader to check.
    site: crate::sql_binding::SqlSiteId,
}

impl SqlLayout {
    /// A layer's outputs, bound to physical slots in the same act.
    ///
    /// Every emitted position gets its slot identity here. Which SEMANTIC
    /// ports those slots carry is [`SqlLayout::bind`]'s answer, at the one
    /// place a relation is realized; until then the layer is physical and
    /// says so.
    pub(in crate::pipeline) fn new(
        at: ScopeId,
        outputs: Vec<ColumnMetadata>,
        identities: &Registry,
    ) -> Self {
        let columns: Vec<ColId> = outputs.iter().map(ColumnMetadata::identity).collect();
        let site = identities.bindings().bind_physical(&columns);
        Self { at, outputs, site }
    }

    /// The scope this layer stands at.
    pub(in crate::pipeline) fn at_scope(&self) -> ScopeId {
        self.at
    }

    /// Everything on offer, in heading order, hygienic included.
    pub(in crate::pipeline) fn outputs(&self) -> &[ColumnMetadata] {
        &self.outputs
    }

    /// Bind this complete emitted layout to the exact semantic relation it
    /// implements. Relation and interface remain construction authority
    /// facts; this records only their physical realization.
    pub(in crate::pipeline) fn bind(
        &mut self,
        relation: &crate::relation::SemanticRelation,
        sealed: &crate::relation::Relations,
        identities: &Registry,
    ) -> Result<()> {
        let columns = self.identities_in_order();
        // EVERY POSITION SAYS WHAT IT IS. A level emits the relation's
        // interface in order — that is what emitting an ordered interface
        // means — and may emit physical support past it: a published anchor
        // a predicate above names, a row-number witness. Those are stated
        // here, one at a time, rather than carved out of the row by a width
        // test that guessed whether the dependencies were present.
        let ports = sealed.interface(relation)?.ports().to_vec();
        let ports_len = ports.len();
        let dependencies = sealed.dependencies(relation)?;
        if columns.len() < ports_len {
            return Err(DelightQLError::transformation_error(
                "a SQL level emits fewer positions than the relation it realizes publishes",
                "sql binding",
            ));
        }
        let mut row = identities.bindings().emitting(sealed, relation)?;
        let mut emitted = columns.iter().copied();
        for port in ports {
            let slot = emitted.next().expect("the width was checked above");
            row.publishes(slot, port)?;
        }
        // THE SUPPORT IS THE NEXT RUN, and only where the level emitted it.
        // A level narrower than interface-plus-support emitted none: the
        // dependency was spent below it and nothing above names the slot.
        if columns.len() >= ports_len + dependencies.len() {
            for port in dependencies {
                let slot = emitted.next().expect("the width was checked above");
                row.supports(slot, port)?;
            }
        }
        for slot in emitted {
            row.scaffolds(slot);
        }
        self.site = row.close(sealed)?;
        Ok(())
    }

    pub(in crate::pipeline) fn site(&self) -> crate::sql_binding::SqlSiteId {
        self.site
    }

    /// Record the source occurrences a merge emits at retained positions.
    pub(in crate::pipeline) fn recognize_merge_aliases(
        &mut self,
        aliases: &[(ColId, ColId)],
        identities: &Registry,
    ) -> Result<()> {
        self.site = identities.bindings().merge_aliases(self.site, aliases)?;
        Ok(())
    }

    /// Carry operand sites' exact physical occurrence maps through an
    /// operation that retains their emitted positions.
    pub(in crate::pipeline) fn recognize_operand_aliases(
        &mut self,
        operands: &[crate::sql_binding::SqlSiteId],
        identities: &Registry,
    ) -> Result<()> {
        self.site = identities
            .bindings()
            .carry_physical_aliases(self.site, operands)?;
        Ok(())
    }

    /// Carry an already-bound semantic interface through a SQL layer that
    /// appends support columns such as `row_number()`. The added columns are
    /// physical-only and acquire no semantic port.
    /// Move this layout's site onto the one beneath it.
    pub(in crate::pipeline) fn resite(
        &mut self,
        prior: crate::sql_binding::SqlSiteId,
        how: Resite<'_>,
        identities: &Registry,
    ) -> Result<()> {
        let emitted = self.identities_in_order();
        let bindings = identities.bindings();
        self.site = match how {
            Resite::Extended => bindings.extend_site(prior, &emitted)?,
            Resite::Projected(selected) => bindings.project_site(prior, selected, &emitted)?,
            Resite::Reshaped(sources) => bindings.reshape_site(prior, sources, &emitted)?,
            Resite::Aliased(aliases) => bindings.alias_site(prior, aliases)?,
            Resite::Rebound => bindings.rebind_site(prior, &emitted)?,
        };
        Ok(())
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
    pub(in crate::pipeline) fn prune_hygienic(&mut self, identities: &Registry) -> Result<()> {
        // A HYGIENIC COLUMN THAT IS A PORT STAYS. A row-number witness a
        // bound filters on and a correlation carrier are positions the
        // relation publishes; pruning them would leave the interface and
        // the emitted list disagreeing by exactly those positions. What the
        // prune answers is the view a CALLER addresses, and the port's own
        // hygiene already keeps it out of that.
        let mut selected = Vec::with_capacity(self.outputs.len());
        for (position, column) in self.outputs.iter().enumerate() {
            let keep = !is_hygienic(identities, column.identity())
                || identities
                    .bindings()
                    .binds_a_port(self.site, column.identity())?;
            if keep {
                selected.push(position);
            }
        }
        if selected.len() != self.outputs.len() {
            let site = self.site;
            self.outputs = selected
                .iter()
                .map(|position| self.outputs[*position].clone())
                .collect();
            self.site =
                identities
                    .bindings()
                    .project_site(site, &selected, &self.identities_in_order())?;
        }
        Ok(())
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
            .map(|column| SelectItem::Publishing {
                expr: DomainExpression::Column(column),
                slot: column,
                printed: true,
            })
            .collect()
    }

    /// Create a new publication and republish every output into it.
    pub(in crate::pipeline) fn requalified(
        &self,
        new_scope: ScopeId,
        identities: &Registry,
    ) -> Result<Self> {
        let outputs = self
            .outputs
            .iter()
            .map(|column| {
                let source = column.identity();
                ColumnMetadata::new(identities.rebind_sql_column(
                    source,
                    new_scope,
                    identities.published(source),
                ))
            })
            .collect();
        let mut publication = Self::new(new_scope, outputs, identities);
        // A WRAP IS STILL AN EMISSION: it rebinds the site it stands over,
        // so a reference to what it wraps has a chain to be re-anchored
        // through.
        publication.site = identities
            .bindings()
            .rebind_site(self.site, &publication.identities_in_order())?;
        Ok(publication)
    }

    /// Requalify across a subquery boundary, applying the SQL safety rename
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
                // The wrap is the same relation re-aliased so it can stand
                // as one FROM entry — an EMISSION boundary, not a semantic
                // one — so ownership walks cross it and a named stage keeps
                // reporting its author's name through every SQL re-staging.
                ColumnMetadata::new(identities.rebind_sql_column(
                    source,
                    new_scope,
                    identities.qualified_safe_spelling(source),
                ))
            })
            .collect();
        let mut publication = Self::new(new_scope, outputs, identities);
        publication.site = identities
            .bindings()
            .rebind_site(self.site, &publication.identities_in_order())?;
        Ok(publication)
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
            .standing_at(self.at)
            .map_err(|message| disagreement(format!("statement finalization: {message}")))
    }
}

fn is_hygienic(identities: &Registry, column: ColId) -> bool {
    identities.addressing(column) == crate::names::Addressing::Hygienic
}

fn disagreement(message: String) -> DelightQLError {
    DelightQLError::ParseError {
        message: format!("SQL layout: {message}"),
        source: None,
        subcategory: None,
    }
}
