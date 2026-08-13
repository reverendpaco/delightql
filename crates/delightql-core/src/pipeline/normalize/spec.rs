// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Spec position — what directs an operator, and is never evaluated.
//!
//! SORTING LAW: everything after `|>` and every pipe-less postfix form is
//! exactly one of a CALL (substitution; never survives as a node), an OPERATOR
//! (anonymous, spec-directed), or CHAIN STRUCTURE. This module owns the second
//! and third; the call is `relex`'s, because a call publishes a relation and
//! the position it stands in is what says so.
//!
//! RETENTION DECIDES POSITION: the context-keeping expansion (`drill`) is
//! postfix — a postfix form extends the complete expression to its left — and
//! the payload-only ones are post-pipe, because a pipe publishes a fresh
//! heading. `R(*) |> .t(*)` ≡ `R(*).t(*) |> (t.*)`, and that equivalence is
//! spelled here rather than remembered downstream.

use super::{gap, Deferred, Normalizer};
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::operators::{ColumnAlias, ColumnNameTemplate};
use crate::pipeline::asts::core::operators::{EmbedMapCover, MapCover};
use crate::pipeline::asts::core::OutValue;
use crate::pipeline::asts::core::Reference;
use crate::pipeline::asts::core::TruthAsValue;
use crate::pipeline::asts::core::{
    DelegateSpec, DomainExpression, FunctionApplication, Glob, GroupSpec, MetadataGroup,
    MetadataOut, MetadataTarget, NameTarget, NamedOutItem, OneOut, OrderDirection, OrderingSpec,
    OutItem, PipeOp, ReductionItem, RegexSelector, RenameSource, RenameSpec, RepositionSpec,
    SelectorItem, Spread, TupleOrdinalClause, TupleOrdinalOperator, Unresolved,
};
use crate::pipeline::syntax::cst;
use delightql_types::SqlIdentifier;

type Operator = PipeOp<Unresolved>;
type Domex = DomainExpression<Unresolved>;

impl<'t> Normalizer<'t> {
    // -----------------------------------------------------------------
    // The pipe operations
    // -----------------------------------------------------------------

    #[stacksafe::stacksafe]
    pub(crate) fn pipe_operation(&mut self, node: cst::PipeOperation<'t>) -> Result<Operator> {
        match node {
            cst::PipeOperation::Project(project) => {
                let mut items = Vec::new();
                for child in project.children() {
                    match child {
                        cst::ProjectChild::OutItem(item) => items.push(self.out_item(item)?),
                        cst::ProjectChild::CommaSigil(_) => {}
                    }
                }
                Ok(PipeOp::Project(self.require(
                    crate::pipeline::asts::vocabulary::Vec1::try_from_vec(items),
                    "at least one projection item",
                )?))
            }
            // `+( … )` extends rather than replaces. The exact variant
            // carries the ADDED items alone; the operand's heading rides in
            // front of them inside the one shared projection algorithm.
            cst::PipeOperation::Embed(embed) => {
                let mut items = Vec::new();
                for child in embed.children() {
                    match child {
                        cst::EmbedChild::OutItem(item) => items.push(self.out_item(item)?),
                        cst::EmbedChild::CommaSigil(_) => {}
                    }
                }
                Ok(PipeOp::Embed(self.require(
                    crate::pipeline::asts::vocabulary::Vec1::try_from_vec(items),
                    "at least one embedded item",
                )?))
            }
            cst::PipeOperation::ProjectOut(project_out) => {
                let mut selector = Vec::new();
                for child in project_out.children() {
                    match child {
                        cst::ProjectOutChild::SelectorItem(item) => {
                            selector.push(self.selector_item(item)?)
                        }
                        cst::ProjectOutChild::CommaSigil(_) => {}
                    }
                }
                Ok(PipeOp::ProjectOut(selector))
            }
            cst::PipeOperation::Rename(rename) => {
                let mut specs = Vec::new();
                for child in rename.children() {
                    match child {
                        cst::RenameChild::RenamePair(pair) => specs.push(self.rename_pair(pair)?),
                        cst::RenameChild::CommaSigil(_) | cst::RenameChild::StarSigil(_) => {}
                    }
                }
                Ok(PipeOp::Rename(self.require(
                    crate::pipeline::asts::vocabulary::Vec1::try_from_vec(specs),
                    "at least one rename pair",
                )?))
            }
            cst::PipeOperation::MapCover(cover) => {
                let callable = self.require(cover.cover(), "a map cover has a callable")?;
                let function = self.callable(callable)?;
                let selector = self.require(cover.selector(), "a map cover has a selector")?;
                let columns = self.selector(selector)?;
                let conditioned_on = match cover.child() {
                    Some(guard) => Some(Box::new(self.guard(guard)?)),
                    None => None,
                };
                Ok(PipeOp::MapCover(MapCover {
                    callable: function,
                    selector: columns,
                    guard: conditioned_on,
                    cells: Vec::new(),
                }))
            }
            cst::PipeOperation::EmbedMapCover(cover) => {
                let callable = self.require(cover.cover(), "an embed cover has a callable")?;
                let function = self.callable(callable)?;
                let selector = self.require(cover.selector(), "an embed cover has a selector")?;
                let selector = self.selector(selector)?;
                let alias_template = cover
                    .child()
                    .map(|template| self.name_template(template))
                    .transpose()?
                    .map(ColumnAlias::Template);
                Ok(PipeOp::EmbedMapCover(EmbedMapCover {
                    callable: function,
                    naming: alias_template,
                    selector,
                    cells: Vec::new(),
                }))
            }
            // Naming is MANDATORY and addresses an EXISTING column: a
            // transform REDEFINES in place, and the name is the address it
            // redefines.
            cst::PipeOperation::Transform(transform) => {
                let mut transformations = Vec::new();
                let mut guard = None;
                for child in transform.children() {
                    match child {
                        cst::TransformChild::TransformItem(item) => {
                            transformations.push(self.transform_item(item)?)
                        }
                        cst::TransformChild::Guard(node) => guard = Some(node),
                        cst::TransformChild::CommaSigil(_) => {}
                    }
                }
                let conditioned_on = match guard {
                    Some(guard) => Some(Box::new(self.guard(guard)?)),
                    None => None,
                };
                Ok(PipeOp::Transform {
                    items: self.require(
                        crate::pipeline::asts::vocabulary::Vec1::try_from_vec(transformations),
                        "at least one transform item",
                    )?,
                    guard: conditioned_on,
                })
            }
            cst::PipeOperation::Group(group) => self.group(group),
        }
    }

    fn rename_pair(&mut self, node: cst::RenamePair<'t>) -> Result<RenameSpec<Unresolved>> {
        let source = self.require(node.source(), "a rename names its source")?;
        let from = match source {
            cst::RenameSource::Reference(reference) => {
                RenameSource::Reference(self.column_reference(reference)?)
            }
            cst::RenameSource::Glob(glob) => RenameSource::Glob(self.glob(glob)?),
            cst::RenameSource::Regex(regex) => RenameSource::Regex(RegexSelector::new(
                super::value::regex_interior(self.text(regex)).to_string(),
            )),
        };
        let target = self.require(node.target(), "a rename names its target")?;
        let to = match target {
            cst::NameTarget::Identifier(name) => {
                NameTarget::Identifier(self.identifier(name).as_str().to_string())
            }
            cst::NameTarget::AsNameTemplate(template) => {
                NameTarget::Template(ColumnAlias::Template(self.name_template(template)?))
            }
        };
        Ok(RenameSpec { from, to })
    }

    /// A spec-level NAME template, never the value template: `{@}` is the
    /// source column's name and `{#}` its ordinal, and neither is an
    /// expression to evaluate.
    fn name_template(&self, node: cst::AsNameTemplate<'t>) -> Result<ColumnNameTemplate> {
        let mut template = String::new();
        for child in node.children() {
            match child {
                cst::AsNameTemplateChild::NameTemplateText(text) => {
                    template.push_str(self.text(text))
                }
                cst::AsNameTemplateChild::NameTemplatePlaceholder(placeholder) => {
                    template.push_str(self.text(placeholder))
                }
            }
        }
        Ok(ColumnNameTemplate { template })
    }

    fn transform_item(&mut self, node: cst::TransformItem<'t>) -> Result<NamedOutItem<Unresolved>> {
        let mut value = None;
        let mut naming = None;
        for child in node.children() {
            match child {
                cst::TransformItemChild::OutValue(out) => value = Some(self.out_value(out)?),
                cst::TransformItemChild::TransformNaming(name) => naming = Some(name),
            }
        }
        let expr = self.require(value, "a transform item has a value")?;
        let naming = self.require(naming, "a transform item names the column it redefines")?;
        let target = self.require(naming.name(), "a naming carries a name")?;
        // The target is an ADDRESS. A qualifier says which live scope holds
        // the column being redefined, and a self-join is where the bare name
        // cannot say it. Both travel AS WRITTEN: a strop is what makes an
        // address case-sensitive, so folding either here would address a
        // column nobody named.
        let target = self.authored_column(target)?;
        Ok(NamedOutItem {
            expr,
            naming: target.name,
            qualifier: target.qualifier,
            output: (),
        })
    }

    /// Read `~>` as AND: the keys on the left are DISTINCTED ON, the right is
    /// reduced per group. Both halves empty is underivable, so a group
    /// without reductions is a distinct and needs no second operator.
    fn group(&mut self, node: cst::Group<'t>) -> Result<Operator> {
        let mut keys = Vec::new();
        let mut items = Vec::new();
        let mut reduces = false;
        for child in node.children() {
            match child {
                cst::GroupChild::GroupKey(key) => keys.push(self.group_key(key)?),
                cst::GroupChild::ReductionSigil(_) => reduces = true,
                cst::GroupChild::ReductionItem(item) => items.push(item),
                cst::GroupChild::CommaSigil(_) | cst::GroupChild::PercentSigil(_) => {}
            }
        }
        if !reduces {
            return Ok(PipeOp::Group(GroupSpec::Distinct {
                keys: self.require(
                    crate::pipeline::asts::vocabulary::Vec1::try_from_vec(keys),
                    "at least one group key",
                )?,
            }));
        }
        // IN SOURCE ORDER, every kind: a delegate is a reduction item like
        // any other, which is what makes the reduction nonempty by type.
        let mut reductions = Vec::new();
        for item in items {
            reductions.push(match self.reduction_item(item)? {
                Reduction::Value(item) => ReductionItem::Out(item),
                Reduction::Pivot(pivot) => ReductionItem::Pivot(pivot),
                Reduction::Metadata(metadata) => ReductionItem::Metadata(metadata),
                Reduction::Delegate(delegate) => ReductionItem::Delegate(delegate),
            });
        }
        Ok(PipeOp::Group(GroupSpec::Reduce {
            keys,
            reductions: self.require(
                crate::pipeline::asts::vocabulary::Vec1::try_from_vec(reductions),
                "at least one reduction item",
            )?,
            plan: crate::pipeline::asts::core::ReductionPlan::empty(),
        }))
    }

    /// A KEY PUBLISHES, so it names what it publishes the way an out item
    /// does — the same rename-versus-baptism classification, by operand kind.
    fn group_key(&mut self, node: cst::GroupKey<'t>) -> Result<OutItem<Unresolved>> {
        match node {
            cst::GroupKey::NamedGroupKey(key) => {
                let mut value = None;
                let mut naming = None;
                for child in key.children() {
                    match child {
                        cst::NamedGroupKeyChild::OutValue(out) => {
                            value = Some(self.out_value(out)?)
                        }
                        cst::NamedGroupKeyChild::Naming(name) => naming = Some(name),
                    }
                }
                let expr = self.require(value, "a group key has a value")?;
                let naming = match naming {
                    Some(naming) => {
                        let name = self.require(naming.name(), "a naming carries a name")?;
                        Some(self.identifier(name))
                    }
                    None => None,
                };
                Ok(OutItem::One(OneOut {
                    expr,
                    naming,
                    output: (),
                }))
            }
            cst::GroupKey::Spread(spread) => Ok(OutItem::Many(self.spread(spread)?)),
        }
    }

    fn reduction_item(&mut self, node: cst::ReductionItem<'t>) -> Result<Reduction> {
        match node {
            cst::ReductionItem::OutItem(item) => Ok(Reduction::Value(self.out_item(item)?)),
            // THE IN IS THE HEADING WITNESS: the pivot's published columns
            // come from an authored membership predicate on its key, which
            // resolution finds; nothing about the data decides them.
            cst::ReductionItem::Pivot(pivot) => {
                let value = self.require(pivot.value(), "a pivot has a value column")?;
                let key = self.require(pivot.key(), "a pivot has a key column")?;
                let value = self.operand(value)?;
                let key = self.operand(key)?;
                Ok(Reduction::Pivot(crate::pipeline::asts::core::PivotSpec {
                    value_column: Box::new(value),
                    pivot_key: Box::new(key),
                    values: Vec::new(),
                }))
            }
            // DISTINCT ON semantics: ordered consumption of the group's rows.
            // An absent ordering is the arbitrary delegate, spelled by its
            // absence rather than by a flag.
            cst::ReductionItem::GroupDelegate(delegate) => {
                let mut payload = Vec::new();
                let mut order = Vec::new();
                for child in delegate.children() {
                    match child {
                        cst::GroupDelegateChild::OutItem(item) => {
                            payload.push(self.out_item(item)?)
                        }
                        cst::GroupDelegateChild::Ordering(ordering) => {
                            order = self.ordering_specs(ordering)?
                        }
                        cst::GroupDelegateChild::CommaSigil(_)
                        | cst::GroupDelegateChild::WindowSigil(_) => {}
                    }
                }
                Ok(Reduction::Delegate(DelegateSpec { payload, order }))
            }
            // REDUCTION POSITION IS THE ONE DOOR. A metadata group publishes
            // one column here, and nowhere else does it stand at all.
            cst::ReductionItem::MetadataGroup(group) => {
                let (group, naming) = self.metadata_group(group)?;
                Ok(Reduction::Metadata(MetadataOut {
                    group,
                    naming,
                    output: (),
                }))
            }
        }
    }

    /// Data values become the KEYS; one metadata key per level. Reduction
    /// position only — this is the one door, and a value position never
    /// reaches it.
    pub(crate) fn metadata_group(
        &mut self,
        node: cst::MetadataGroup<'t>,
    ) -> Result<(MetadataGroup<Unresolved>, Option<SqlIdentifier>)> {
        let key_column =
            self.require(node.key_column(), "a metadata group names its key column")?;
        // The key is a reference: which live scope holds the column whose
        // VALUES become the record's keys is the qualifier's question.
        let key =
            self.authored_column(self.require(key_column.child(), "a key column is a reference")?)?;
        let mut target = None;
        let mut naming = None;
        for child in node.children() {
            match child {
                cst::MetadataGroupChild::MetaTarget(node) => target = Some(node),
                cst::MetadataGroupChild::Naming(name) => naming = Some(name),
                cst::MetadataGroupChild::MetadataSigil(_) => {}
            }
        }
        let target = self.require(target, "a metadata group has a target")?;
        let target = match target {
            cst::MetaTarget::EnclyphLike(enclyph) => {
                MetadataTarget::Enclyph(self.enclyph_like(enclyph)?)
            }
            // A nested group's own naming is the outer group's business only
            // when the outer position publishes it; a nested constructor
            // publishes nothing of its own.
            cst::MetaTarget::MetadataGroup(nested) => {
                MetadataTarget::Group(Box::new(self.metadata_group(nested)?.0))
            }
        };
        let naming = match naming {
            Some(naming) => {
                let name = self.require(naming.name(), "a naming carries a name")?;
                Some(self.identifier(name))
            }
            None => None,
        };
        Ok((
            MetadataGroup {
                key,
                target,
                cte_requirements: None,
            },
            naming,
        ))
    }

    // -----------------------------------------------------------------
    // Out items and selectors
    // -----------------------------------------------------------------

    /// ONE spelling for naming; rename versus baptism is a classification by
    /// OPERAND kind, not two syntaxes — a reference publishes its own name,
    /// an application mints one.
    pub(crate) fn out_item(&mut self, node: cst::OutItem<'t>) -> Result<OutItem<Unresolved>> {
        match node {
            cst::OutItem::NamedOutItem(item) => {
                let mut value = None;
                let mut naming = None;
                for child in item.children() {
                    match child {
                        cst::NamedOutItemChild::OutValue(out) => value = Some(self.out_value(out)?),
                        cst::NamedOutItemChild::Naming(name) => naming = Some(name),
                    }
                }
                let expr = self.require(value, "an out item has a value")?;
                let naming = match naming {
                    Some(naming) => {
                        let name = self.require(naming.name(), "a naming carries a name")?;
                        Some(self.identifier(name))
                    }
                    None => None,
                };
                Ok(OutItem::One(OneOut {
                    expr,
                    naming,
                    output: (),
                }))
            }
            // Naming on a spread refuses by construction, in the grammar and
            // again in the type: `Many` has no naming to set.
            cst::OutItem::Spread(spread) => Ok(OutItem::Many(self.spread(spread)?)),
        }
    }

    /// A PUBLISHED value, in the carrier the position admits: a domain
    /// value, or the licensed crossing.
    ///
    /// The crossing is BUILT here, where it is written. Reading it back as a
    /// domain expression and wrapping the result would put every published
    /// truth behind the broad value carrier again, and the position's type
    /// would be saying something the tree did not.
    pub(crate) fn out_value(&mut self, node: cst::OutValue<'t>) -> Result<OutValue<Unresolved>> {
        match node {
            cst::OutValue::DomainExpression(expression) => {
                // The pre-carved existence spelling is this position's
                // crossing under its own surface — one occurrence, one
                // carrier — so it is read as a crossing before the value
                // road is taken.
                if let Some(existence) = super::value::pre_carved_existence_value(expression) {
                    return Ok(OutValue::Truth(TruthAsValue(
                        self.exists_as_column(existence)?,
                    )));
                }
                Ok(OutValue::Domain(self.domain_expression(expression)?))
            }
            cst::OutValue::TruthAsValue(truth) => Ok(OutValue::Truth(TruthAsValue(
                self.truth_as_value_truth(truth)?,
            ))),
        }
    }

    /// WHAT A FORM COMPUTES, when the form names ONE value: a value rule's
    /// body, and a case arm's result. Both admit the crossing for the same
    /// reason an out item does — each names the one value its form denotes,
    /// and the pre-carved existence spelling is written there.
    ///
    /// `f:( … ) : +orders(, … )` is that spelling; the grammar carves it out
    /// precisely because these are value positions and a truth has no other
    /// way in.
    pub(crate) fn computed_value(
        &mut self,
        node: cst::DomainExpression<'t>,
    ) -> Result<OutValue<Unresolved>> {
        if let Some(existence) = super::value::pre_carved_existence_value(node) {
            return Ok(OutValue::Truth(TruthAsValue(
                self.exists_as_column(existence)?,
            )));
        }
        Ok(OutValue::Domain(self.domain_expression(node)?))
    }

    /// ONE selector carrier: the items as written, each already classified
    /// by the production it came from. No consumer re-reads a list to
    /// discover that it was "all", "a regex", or "a range".
    fn selector(&mut self, node: cst::Selector<'t>) -> Result<Vec<SelectorItem<Unresolved>>> {
        let mut items = Vec::new();
        for child in node.children() {
            match child {
                cst::SelectorChild::SelectorItem(item) => items.push(self.selector_item(item)?),
                cst::SelectorChild::CommaSigil(_) => {}
            }
        }
        Ok(items)
    }

    /// One enumerated addressing item: a reference, or a spread standing for
    /// the several it covers. Every enumerating operator reads it here.
    fn selector_item(&mut self, node: cst::SelectorItem<'t>) -> Result<SelectorItem<Unresolved>> {
        match node {
            cst::SelectorItem::Reference(reference) => {
                Ok(SelectorItem::Reference(self.column_reference(reference)?))
            }
            cst::SelectorItem::Spread(spread) => Ok(SelectorItem::Spread(self.spread(spread)?)),
        }
    }

    // -----------------------------------------------------------------
    // Chain structure borrowing operator spelling
    // -----------------------------------------------------------------

    pub(crate) fn pipe_structural(
        &mut self,
        node: cst::PipeStructural<'t>,
    ) -> Result<Vec<crate::pipeline::asts::core::Continuation<Unresolved>>> {
        use crate::pipeline::asts::core::{Continuation, StructuralForm, StructuralStep};
        match node {
            cst::PipeStructural::Ordering(ordering) => {
                Ok(vec![Continuation::Structural(StructuralStep {
                    form: StructuralForm::Ordering {
                        specs: self.ordering_specs(ordering)?,
                    },
                    named: Default::default(),
                    cpr_schema: (),
                })])
            }
            cst::PipeStructural::Reposition(reposition) => {
                let mut moves = Vec::new();
                for child in reposition.children() {
                    match child {
                        cst::RepositionChild::RepositionPair(pair) => {
                            moves.push(self.reposition_pair(pair)?)
                        }
                        cst::RepositionChild::CommaSigil(_)
                        | cst::RepositionChild::StarSigil(_) => {}
                    }
                }
                Ok(vec![Continuation::Structural(StructuralStep {
                    form: StructuralForm::Reposition { moves },
                    named: Default::default(),
                    cpr_schema: (),
                })])
            }
            // Payload only: the same expansion drill performs, then a
            // projection that keeps the interior and drops the context. ONE
            // expansion authority; the difference is what survives it.
            cst::PipeStructural::NarrowingAccess(narrow) => {
                let mut reference = None;
                let mut access = None;
                for child in narrow.children() {
                    match child {
                        cst::NarrowingAccessChild::Reference(node) => reference = Some(node),
                        cst::NarrowingAccessChild::Access(node) => access = Some(node),
                    }
                }
                let reference = self.require(reference, "a narrowing names a column")?;
                let access = self.require(access, "a narrowing has an access")?;
                let column = self.reference_name(reference)?;
                let expansion = self.expansion(column.clone(), access)?;
                Ok(vec![
                    expansion,
                    Continuation::Pipe {
                        operator: PipeOp::Project(crate::pipeline::asts::vocabulary::Vec1::new(
                            OutItem::Many(Spread::Glob(Glob::qualified(SqlIdentifier::new(
                                column,
                            )))),
                        )),
                        named: None,
                        cpr_schema: (),
                    },
                ])
            }
            cst::PipeStructural::NarrowingDestructure(narrow) => {
                let nest = self.require(narrow.column(), "a narrowing names a column")?;
                let pattern = self.require(narrow.pattern(), "a narrowing has a pattern")?;
                let pattern = self.narrowing_pattern(pattern)?;
                Ok(vec![Continuation::Structural(StructuralStep {
                    form: StructuralForm::Narrow {
                        nest: self.column_reference(nest)?,
                        pattern,
                        schema: (),
                    },
                    named: Default::default(),
                    cpr_schema: (),
                })])
            }
        }
    }

    pub(crate) fn postfix_operator(
        &mut self,
        node: cst::PostfixOperator<'t>,
    ) -> Result<crate::pipeline::asts::core::Continuation<Unresolved>> {
        use crate::pipeline::asts::core::{Continuation, StructuralForm, StructuralStep};
        match node {
            // `^^` is not a token: iteration is ordinary postfix stacking, so
            // two adjacent applications are two continuations.
            cst::PostfixOperator::Meta(_) => Ok(Continuation::Structural(StructuralStep {
                form: StructuralForm::Meta,
                named: Default::default(),
                cpr_schema: (),
            })),
            cst::PostfixOperator::Witness(witness) => {
                let polarity = self.require(witness.child(), "a witness carries a polarity")?;
                Ok(Continuation::Structural(StructuralStep {
                    form: StructuralForm::Witness {
                        polarity: self.polarity(polarity)?,
                    },
                    named: Default::default(),
                    cpr_schema: (),
                }))
            }
            cst::PostfixOperator::SignedWitness(_) => {
                Ok(Continuation::Structural(StructuralStep {
                    form: StructuralForm::SignedWitness,
                    named: Default::default(),
                    cpr_schema: (),
                }))
            }
            // `*` and `.(…)` are a dequalifying RUN, and a run is an access
            // wherever it stands — inside the parens or after them. The
            // access road claims them before this one is reached, so the
            // operator layer holds no second spelling of what a run means.
            cst::PostfixOperator::DomainActivate(_) | cst::PostfixOperator::Using(_) => {
                unreachable!("a dequalifying run is read as an access")
            }
            cst::PostfixOperator::Drill(drill) => {
                let mut reference = None;
                let mut access = None;
                for child in drill.children() {
                    match child {
                        cst::DrillChild::Reference(node) => reference = Some(node),
                        cst::DrillChild::Access(node) => access = Some(node),
                    }
                }
                let reference = self.require(reference, "a drill names a column")?;
                let access = self.require(access, "a drill has an access")?;
                let column = self.reference_name(reference)?;
                self.expansion(column, access)
            }
        }
    }

    /// The one expansion carrier drill and narrowing share. A glob access
    /// takes the interior whole; a slot row names the interior columns, and a
    /// ground slot pairs a position with the literal it is fixed to.
    fn expansion(
        &mut self,
        column: String,
        access: cst::Access<'t>,
    ) -> Result<crate::pipeline::asts::core::Continuation<Unresolved>> {
        let inner = self.require(access.child(), "an access has an interior")?;
        let mut glob = false;
        let mut columns = Vec::new();
        let mut groundings = Vec::new();
        match inner {
            cst::AccessChild::Interior(interior) => {
                // `.t(*)` — the whole interior. Anything else in the interior
                // is a shaping continuation the expansion has no place for.
                let mut seen = 0;
                for continuation in interior.children() {
                    seen += 1;
                    match continuation {
                        cst::Continuation::OperatorContinuation(
                            cst::OperatorContinuation::PostfixOperator(
                                cst::PostfixOperator::DomainActivate(_),
                            ),
                        ) => glob = true,
                        // An expansion's parens NAME the interior heading;
                        // they do not shape it. Shaping is the chain's, and
                        // the chain is where the expanded rows land.
                        _ => {
                            return Err(DelightQLError::validation_error_categorized(
                                "expansion/shaping_interior",
                                format!(
                                    "`.{column}( … )` names the interior columns to expand; \
                                     this one shapes them"
                                ),
                                "shape after the expansion: `.col(*) |> ( … )`",
                            ))
                        }
                    }
                }
                if seen == 0 {
                    return Err(DelightQLError::parse_error(
                        "an expansion interior names something",
                    ));
                }
            }
            // The interior's slots are POSITIONAL. A name binds its position,
            // the anaphor holds one without binding, and a ground term FIXES
            // one — a grounding pairs the position with the literal, and the
            // position still occupies a column slot so the list stays aligned.
            cst::AccessChild::ArgumentativeForm(form) => {
                for (position, slot) in self.slot_nodes(form).into_iter().enumerate() {
                    match slot {
                        cst::Slot::NamedReference(reference) => {
                            let name = self.require(reference.name(), "a slot names a column")?;
                            columns.push(self.identifier(name).as_str().to_string());
                        }
                        cst::Slot::Disregarded(_) => columns.push("_".to_string()),
                        cst::Slot::ConstraintTerm(cst::ConstraintTerm::FunctionApplication(
                            application,
                        )) => {
                            let term = self.function_application_expression(application)?;
                            let Some(value) = ground_value(&term) else {
                                return Err(interior_slot_refusal(&column));
                            };
                            columns.push("_".to_string());
                            groundings.push((position.to_string(), value));
                        }
                        cst::Slot::ConstraintTerm(cst::ConstraintTerm::TruthAsValue(_)) => {
                            return Err(interior_slot_refusal(&column))
                        }
                        cst::Slot::RenamedSlot(renamed) => {
                            return Err(self.renamed_slot_refusal(renamed))
                        }
                    }
                }
            }
        }
        Ok(crate::pipeline::asts::core::Continuation::Structural(
            crate::pipeline::asts::core::StructuralStep {
                form: crate::pipeline::asts::core::StructuralForm::Drill {
                    drill: crate::pipeline::asts::core::operators::AuthoredDrill {
                        column,
                        glob,
                        columns,
                        groundings,
                    },
                },
                named: Default::default(),
                cpr_schema: (),
            },
        ))
    }

    /// A pattern in narrowing position declares the heading: its members are
    /// the fields the payload publishes.
    /// A NARROWING'S PAYLOAD IS A TREE PATTERN — the same one a `~=`
    /// destructure declares, built by the same road.
    ///
    /// A narrowing publishes its fields and NOTHING else: no context rides
    /// through, so a member that nests, iterates, or keys has no output to
    /// attach to. Authored syntax is consumed or refused, never dropped.
    fn narrowing_pattern(
        &mut self,
        node: cst::RecordPattern<'t>,
    ) -> Result<crate::pipeline::asts::core::RecordPattern<Unresolved>> {
        for member in self.pattern_members(node) {
            if !matches!(
                member,
                cst::PatternMember::Binder(_) | cst::PatternMember::PathBinding(_)
            ) {
                return Err(DelightQLError::validation_error_categorized(
                    "narrowing/member",
                    format!(
                        "a narrowing names the fields its payload publishes; \
                         {} names none",
                        pattern_member_name(member)
                    ),
                    "write the fields: `|> .col{a, b}` or `|> .col{.a.b}`; \
                     destructure with `col ~= ~> { … }` for the pattern language",
                ));
            }
        }
        match self.tree_pattern(cst::TreePattern::RecordPattern(node))? {
            crate::pipeline::asts::core::TreePattern::Record(record) => Ok(record),
            other => unreachable!("a record pattern normalizes to a record pattern, got {other:?}"),
        }
    }

    pub(crate) fn ordering_specs(
        &mut self,
        node: cst::Ordering<'t>,
    ) -> Result<Vec<OrderingSpec<Unresolved>>> {
        let mut specs = Vec::new();
        for child in node.children() {
            match child {
                cst::OrderingChild::OrderItem(item) => {
                    let column = self.require(item.child(), "an order item names a column")?;
                    let direction = match item.direction().and_then(|node| node.child()) {
                        Some(cst::OrderDirectionChild::AscKeyword(_)) => {
                            Some(OrderDirection::Ascending)
                        }
                        Some(cst::OrderDirectionChild::DescKeyword(_)) => {
                            Some(OrderDirection::Descending)
                        }
                        None => None,
                    };
                    specs.push(OrderingSpec {
                        column: self.domain_expression(column)?,
                        direction,
                    });
                }
                cst::OrderingChild::CommaSigil(_) => {}
            }
        }
        Ok(specs)
    }

    fn reposition_pair(
        &mut self,
        node: cst::RepositionPair<'t>,
    ) -> Result<RepositionSpec<Unresolved>> {
        let source = self.require(node.source(), "a reposition names its column")?;
        let column = match source {
            cst::RepositionPairSource::Reference(reference) => self.column_reference(reference)?,
            cst::RepositionPairSource::Number(number) => {
                let text = self.text(number);
                let value: i64 = text.parse().map_err(|_| {
                    DelightQLError::parse_error(format!("'{text}' is not a column position"))
                })?;
                Reference::Ordinal(crate::pipeline::asts::core::ColumnOrdinal {
                    position: value.unsigned_abs() as u16,
                    reverse: value < 0,
                    qualifier: None,
                    namespace_path: crate::pipeline::asts::core::NamespacePath::empty(),
                    glob: false,
                })
            }
        };
        let position = self.require(node.position(), "a reposition names a position")?;
        let text = self.text(position);
        Ok(RepositionSpec {
            column,
            position: text.parse().map_err(|_| {
                DelightQLError::parse_error(format!("'{text}' is not a target position"))
            })?,
        })
    }

    /// A bound's ONE home is the comma member; `#<` and `#>` are the whole
    /// vocabulary.
    pub(crate) fn row_bound(&mut self, node: cst::RowBound<'t>) -> Result<TupleOrdinalClause> {
        let mut operator = None;
        let mut value = None;
        for child in node.children() {
            match child {
                cst::RowBoundChild::BoundOp(op) => operator = Some(op),
                cst::RowBoundChild::CompileTimeInteger(number) => value = Some(number),
            }
        }
        let operator = self.require(operator, "a bound has an operator")?;
        let text = self.text(operator).replace(char::is_whitespace, "");
        let operator = match text.as_str() {
            "#<" => TupleOrdinalOperator::LessThan,
            "#>" => TupleOrdinalOperator::GreaterThan,
            other => {
                return Err(DelightQLError::parse_error(format!(
                    "'{other}' is not a bound operator"
                )))
            }
        };
        let value = self.require(value, "a bound has a count")?;
        let value = self.compile_time_integer(value, "a row bound")?;
        Ok(TupleOrdinalClause {
            operator,
            value,
            offset: None,
        })
    }

    /// The name a reference addresses, for the operators that take a column
    /// by spelling. A qualified name in these positions reaches the same
    /// carrier the operator already stores.
    fn reference_name(&mut self, node: cst::Reference<'t>) -> Result<String> {
        match node {
            cst::Reference::NamedReference(reference) => {
                let name = self.require(reference.name(), "a reference has a name")?;
                Ok(self.identifier(name).as_str().to_string())
            }
            // The law admits a `reference` here, and an ordinal is one. The
            // carrier is a SPELLING, so the position is a build gap, not a
            // refusal: SEMANTICS admits it and no ruling removed it.
            cst::Reference::PositionalReference(ordinal) => Err(gap(
                Deferred::OperatorOrdinal,
                format!(
                    "the ordinal {} where an operator addresses a column by spelling",
                    self.text(ordinal)
                ),
            )),
        }
    }
}

/// What a reduction item contributes to its group.
enum Reduction {
    /// A metadata group and what it publishes.
    Metadata(MetadataOut<Unresolved>),
    Value(OutItem<Unresolved>),
    /// A pivot, which publishes one column per value its key's membership
    /// predicate names rather than one of its own.
    Pivot(crate::pipeline::asts::core::PivotSpec<Unresolved>),
    Delegate(DelegateSpec<Unresolved>),
}

fn pattern_member_name(member: cst::PatternMember<'_>) -> &'static str {
    match member {
        cst::PatternMember::Binder(_) => "a binder",
        cst::PatternMember::KeyedBinding(_) => "a keyed binding",
        cst::PatternMember::NestedPattern(_) => "a nested pattern",
        cst::PatternMember::PathBinding(_) => "a path binding",
        cst::PatternMember::MetadataBinding(_) => "a metadata binding",
        cst::PatternMember::Disregarded(_) => "the disregarded anaphor",
    }
}

/// An expansion interior's slots NAME columns or FIX them to constants. A
/// row-dependent term computes, and a truth term tests — neither is a heading,
/// and the interior is a heading.
fn interior_slot_refusal(column: &str) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "expansion/interior_slot",
        format!(
            "`.{column}( … )` names interior columns or fixes them to constants; \
             this slot does neither"
        ),
        "constrain after the expansion: `.col(a, b), a > 1`",
    )
}

/// The constant a ground interior slot fixes its position to. Parens are
/// admission, so a parenthesized constant is the same constant.
fn ground_value(term: &Domex) -> Option<String> {
    match term {
        DomainExpression::Application(FunctionApplication::Ground(value)) => {
            Some(value.to_string())
        }
        _ => None,
    }
}
