// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Values, truths and stage items. A comparison's equality class, a call's
//! grade and a scalar position's cardinality are their constructors'; this
//! module says where each stands.

use super::env::Address;
use super::integer::Site;
use super::Elaborator;
use crate::pipeline::middle::core::decide::grade::{self, CallPosition, Known};
use crate::pipeline::middle::core::graph::Arena;
use crate::pipeline::middle::core::heading::{Layout, Name};
use crate::pipeline::middle::core::ids::{ExprId, RelId, TruthId};
use crate::pipeline::middle::core::node::expr::CaseArm;
use crate::pipeline::middle::core::node::rel::ExpansionForm;
use crate::pipeline::middle::core::node::{
    Arg, Bind, BindAt, BindRole, Callee, CollectMember, Consumer, ExprKind, Frame, FrameEdge, Grade, Item,
    Level, Naming, PipeOp, Reach,
};
use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::facade::{
    self, AstPipeOp, CallArguments, CaseExpression, DomainExpression, Enclyph, FrameBound,
    FrameMode, FunctionApplication, GroupSpec, KnownCall, LiteralValue, NamedProof, NamedReference,
    OutItem, RecordMember, ReductionItem, Reference, ScalarArgument, ScalarRelation, Spread,
    StandardApplication, TruthExpression, TupleElement,
};

/// A window elaborated where its use stands: the partition, the order and
/// the frame.
pub(super) type Windowing = (Vec<ExprId>, Vec<crate::pipeline::middle::core::node::OrderKey>, Option<Frame>);

/// The window a call stands under: none, its own spec, or the window a
/// windowed use of the definition whose body the call is hands it.
pub(super) enum Under<'a> {
    None,
    Own(&'a facade::WindowSpec),
    Handed(Windowing),
}

impl Under<'_> {
    fn windowed(&self) -> bool {
        !matches!(self, Under::None)
    }
}

/// A stage as the elaborator writes it: an operation, or a removal or cover
/// whose references the stage's constructor resolves against its input.
pub(super) enum Stage {
    Op(PipeOp),
    Remove(Vec<ExprId>),
    Cover(Vec<(ExprId, ExprId)>),
}

impl Elaborator<'_, '_> {
    /// A value standing at `at`: the grade its position asks of the calls
    /// in it.
    #[stacksafe::stacksafe]
    pub(super) fn value(&mut self, expr: &DomainExpression, at: CallPosition) -> Result<ExprId, Refusal> {
        match expr {
            DomainExpression::Reference(reference) => self.reference(reference),
            DomainExpression::Application(application) => self.application(application, at),
        }
    }

    /// A written named reference: its address, resolved.
    pub(super) fn named_reference(&mut self, named: &NamedReference) -> Result<ExprId, Refusal> {
        let col = &named.0;
        if !col.namespace_path.is_empty() {
            return Err(refuse::outside("a namespace-qualified column"));
        }
        match &col.qualifier {
            Some(q) => self.resolve_ref(Address::Qualified(q, &col.name)),
            None => self.resolve_ref(Address::Bare(&col.name)),
        }
    }

    pub(super) fn reference(&mut self, reference: &Reference) -> Result<ExprId, Refusal> {
        match reference {
            Reference::Named(named) => self.named_reference(named),
            Reference::Ordinal(ordinal) => {
                if ordinal.glob || !ordinal.namespace_path.is_empty() {
                    return Err(refuse::outside("a glob or namespaced ordinal"));
                }
                let n = self.compile_time_integer(ordinal.position, Site::Ordinal)?;
                let position = u16::try_from(n.unsigned_abs())
                    .map_err(|_| refuse::elaboration_contract("an ordinal position past its range"))?;
                self.resolve_ref(Address::Ordinal {
                    position,
                    reverse: n < 0,
                    qualifier: ordinal.qualifier.as_ref(),
                })
            }
            Reference::Argument(selector) => {
                let actual = self.formal_value(selector.scope(), selector.position().index())?;
                match self.b.expr(actual).kind() {
                    ExprKind::Passenger(p) => {
                        let p = *p;
                        self.resolve_passenger(p)
                    }
                    ExprKind::Across(_) | ExprKind::Argument { .. } => Ok(actual),
                    ExprKind::Col(..)
                    | ExprKind::Merged(_)
                    | ExprKind::Const(_)
                    | ExprKind::Call { .. }
                    | ExprKind::Window { .. }
                    | ExprKind::Infix(..)
                    | ExprKind::Case { .. }
                    | ExprKind::Crossed(_)
                    | ExprKind::Scalar { .. }
                    | ExprKind::Collect { .. }
                    | ExprKind::Metadata { .. }
                    | ExprKind::Pick { .. }
                    | ExprKind::Construct { .. }
                    | ExprKind::Path { .. } => self.b.formal_read(actual),
                }
            }
            Reference::Physical(never) => match *never {},
        }
    }

    fn application(&mut self, application: &FunctionApplication, at: CallPosition) -> Result<ExprId, Refusal> {
        match application {
            FunctionApplication::Ground(literal) => Ok(self.b.constant(literal.clone())),
            FunctionApplication::Standard(standard) => self.standard(standard, at),
            FunctionApplication::Infix(infix) => {
                let l = self.value(&infix.left, at)?;
                let r = self.value(&infix.right, at)?;
                self.b.infix(infix.operator, l, r)
            }
            FunctionApplication::Case(case) => self.case(case, at),
            FunctionApplication::Crossed(crossing) => {
                // A crossed truth is a value: its values stand where it does.
                let t = self.truth_in(crossing.truth(), Consumer::Value, at)?;
                Ok(self.b.crossed(t))
            }
            FunctionApplication::Scalarized(scalar) => {
                let body = match scalar {
                    ScalarRelation::Named { body, .. } | ScalarRelation::Sourceless { body } => body,
                };
                let chain = (**body).clone().attached();
                let rel = self.chain(&chain)?;
                crate::pipeline::middle::core::decide::effect::fence(&self.b, rel, "a scalar subquery")?;
                Ok(self.b.scalar(rel))
            }
            FunctionApplication::Enclyph(Enclyph::Record(record)) if at == CallPosition::Reduction => {
                let members = self.collect_members(&record.members)?;
                self.b.collect(Layout::Record, members)
            }
            // A collected tuple's members are addressed by position: they
            // publish no name.
            FunctionApplication::Enclyph(Enclyph::Tuple(tuple)) if at == CallPosition::Reduction => {
                let members = self.collect_tuple(tuple)?;
                self.b.collect(Layout::Tuple, members)
            }
            // In value position an enclyph is one nested value; it declares
            // its top-level type (FN.19).
            FunctionApplication::Enclyph(Enclyph::Record(record)) => {
                let members = self.made_members(&record.members, at)?;
                self.b.construct(Layout::Record, members)
            }
            FunctionApplication::Enclyph(Enclyph::Tuple(tuple)) => {
                let mut members = Vec::with_capacity(tuple.elements.len());
                for element in &tuple.elements {
                    match element {
                        TupleElement::Value(value) => members.push(Item {
                            expr: self.value(value, at)?,
                            naming: Naming::Computed,
                        }),
                        TupleElement::Spread(spread) => self.spread_items(spread, false, &mut members)?,
                    }
                }
                self.b.construct(Layout::Tuple, members)
            }
            FunctionApplication::Enclyph(Enclyph::EmptyRecord(never)) => match *never {},
            FunctionApplication::JsonAccess(access) => {
                let source = self.value(&access.source, at)?;
                self.b.path(source, access.path.clone())
            }
            // `@` is what flows in: the covered cell a cover's callable is
            // applied to, else a companion cell's column.
            FunctionApplication::Open(facade::DomainHole::CompositionInput) => match (self.flowing, self.column_self) {
                (Some(flowing), _) => Ok(flowing),
                (None, super::ColumnSelf::Column(column)) => Ok(column),
                (None, super::ColumnSelf::Table) => Err(refuse::table_level_column_self()),
                (None, super::ColumnSelf::Outside) => Err(refuse::composition_input_unapplied()),
            },
            FunctionApplication::Open(facade::DomainHole::Disregarded) => {
                Err(refuse::outside("an open functor in value position"))
            }
            FunctionApplication::Template(template) => self.template(template, at),
            FunctionApplication::FieldSelect(select) => self.field_select(select, at),
            // A value definition's clauses are read by its call.
            FunctionApplication::ClauseSelection(_) => {
                Err(refuse::elaboration_contract("a clause selection outside a value definition's call"))
            }
        }
    }

    /// A string template: its text and interpolated values concatenated in
    /// order, so its value is text whatever it interpolates. A template that
    /// opens with an interpolation concatenates onto the empty text.
    fn template(&mut self, template: &facade::ValueTemplate, at: CallPosition) -> Result<ExprId, Refusal> {
        let mut parts = Vec::new();
        for part in template.parts() {
            parts.push(match part {
                facade::ValueTemplatePart::Text(text) => self.b.constant(LiteralValue::String(text.clone())),
                facade::ValueTemplatePart::Interpolation(value) => self.value(value, at)?,
            });
        }
        let mut parts = parts.into_iter();
        let first = parts
            .next()
            .ok_or_else(|| refuse::elaboration_contract("a template with no part"))?;
        let mut out = if matches!(template.parts().next(), Some(facade::ValueTemplatePart::Text(_))) {
            first
        } else {
            let empty = self.b.constant(LiteralValue::String(String::new()));
            self.b.infix(facade::BinOp::Concat, empty, first)?
        };
        for part in parts {
            out = self.b.infix(facade::BinOp::Concat, out, part)?;
        }
        Ok(out)
    }

    /// A record made in value position: its members, each a value of the
    /// row it stands in, named by its key.
    fn made_members(&mut self, members: &[RecordMember], at: CallPosition) -> Result<Vec<Item>, Refusal> {
        let mut out = Vec::with_capacity(members.len());
        for member in members {
            match member {
                RecordMember::SelfKeyed(named) => out.push(Item {
                    expr: self.reference(&Reference::Named(named.clone()))?,
                    naming: Naming::Reference,
                }),
                RecordMember::Keyed { key, value } => out.push(Item {
                    expr: self.value(value, at)?,
                    naming: Naming::As(Name::new(key.clone())),
                }),
                RecordMember::Spread(spread) => self.spread_items(spread, true, &mut out)?,
                RecordMember::Induced { .. } => {
                    return Err(refuse::outside("a nested collecting level in a record made in value position"))
                }
                RecordMember::Metadata { .. } => {
                    return Err(refuse::outside("a metadata group in a record made in value position"))
                }
            }
        }
        Ok(out)
    }

    /// The items a spread in a made value expands into. In a record, an
    /// unqualified glob's members are keyed as the run publishes its
    /// positions (THE SPREAD, keys per the MINTING LAW one level down): a
    /// position a collision poisoned answers to no key.
    fn spread_items(&mut self, spread: &Spread, record: bool, out: &mut Vec<Item>) -> Result<(), Refusal> {
        let Spread::Glob(glob) = spread else {
            // A regex or a span addresses columns by name or place: in a
            // record each is keyed by the name its column publishes.
            for (expr, name) in self.spread_values(spread)? {
                let naming = match (record, name) {
                    (true, Some(name)) => Naming::As(name),
                    (true, None) | (false, _) => Naming::Computed,
                };
                out.push(Item { expr, naming });
            }
            return Ok(());
        };
        if !glob.namespace_path.is_empty() {
            return Err(refuse::outside("a namespaced glob"));
        }
        let published: Vec<Option<Name>> = match (&glob.qualifier, self.scopes.last()) {
            (None, Some(top)) if record => top
                .run
                .outputs()
                .iter()
                .filter(|(p, _)| p.visibility == crate::pipeline::middle::core::heading::Visibility::Published)
                .map(|(p, _)| p.answering_name().cloned())
                .collect(),
            _ => Vec::new(),
        };
        let naming = glob_naming(glob.qualifier.is_some());
        for (k, expr) in self.glob(glob.qualifier.as_ref())?.into_iter().enumerate() {
            let naming = match (record, glob.qualifier.is_some(), published.get(k)) {
                (false, _, _) => Naming::Computed,
                (true, false, Some(Some(name))) => Naming::As(name.clone()),
                (true, false, _) => Naming::Computed,
                (true, true, _) => naming.clone(),
            };
            out.push(Item { expr, naming });
        }
        Ok(())
    }

    /// A NESTED TREE BOUNDARY IN A GROUPING KEY (tree-group-evaluation-law:
    /// nested tree boundaries remain explicit). A key record whose member
    /// collects (`{country, "people": ~> {…}} ~> …`) is decided as its
    /// staged equivalent: the group is keyed by the record's plain members
    /// beside the other keys, each nested level or metadata group collects
    /// within that group, and the record is then made from the group's
    /// columns, in the key's
    /// place. `None` when no key is such a record.
    pub(super) fn key_collector_stages(&mut self, operator: &AstPipeOp) -> Result<Option<RelId>, Refusal> {
        let AstPipeOp::Group(GroupSpec::Reduce { keys, reductions, .. }) = operator else {
            return Ok(None);
        };
        fn collecting(item: &OutItem) -> Option<&facade::Record> {
            match item {
                OutItem::One(one) => match &one.expr {
                    DomainExpression::Application(FunctionApplication::Enclyph(Enclyph::Record(record))) => record
                        .members
                        .iter()
                        .any(|m| matches!(m, RecordMember::Induced { .. } | RecordMember::Metadata { .. }))
                        .then_some(record),
                    _ => None,
                },
                OutItem::Many(_) | OutItem::Whole => None,
            }
        }
        if !keys.iter().any(|k| collecting(k).is_some()) {
            return Ok(None);
        }
        if reductions.iter().any(|r| !matches!(r, ReductionItem::Out(_) | ReductionItem::Metadata(_))) {
            return Err(refuse::outside("a grouping key record that collects beside a pivot or delegate item"));
        }
        /// Where a published column of the staged equivalent comes from:
        /// a key of the group, one of its reductions, or a record made from
        /// some of each.
        #[derive(Clone, Copy)]
        enum From {
            Key(usize),
            Reduced(usize),
        }
        enum Slot {
            One(From),
            Made(Vec<(From, Naming)>, Option<Name>),
        }
        self.stages.push(self.scopes.len() - 1);
        let built = (|| -> Result<(Vec<Item>, Vec<Item>, Vec<Slot>), Refusal> {
            let mut key_items = Vec::new();
            let mut red_items = Vec::new();
            let mut slots = Vec::new();
            for item in keys.iter() {
                match (collecting(item), item) {
                    (Some(record), OutItem::One(one)) => {
                        let mut members = Vec::new();
                        for member in &record.members {
                            match member {
                                RecordMember::Induced { key, value } => {
                                    let expr = match &**value {
                                        Enclyph::Record(inner) => {
                                            let collected = self.collect_members(&inner.members)?;
                                            self.b.collect(Layout::Record, collected)?
                                        }
                                        Enclyph::Tuple(tuple) => {
                                            let collected = self.collect_tuple(tuple)?;
                                            self.b.collect(Layout::Tuple, collected)?
                                        }
                                        Enclyph::EmptyRecord(never) => match *never {},
                                    };
                                    members.push((From::Reduced(red_items.len()), Naming::As(Name::new(key.clone()))));
                                    red_items.push(Item { expr, naming: Naming::Computed });
                                }
                                // A metadata child collects within the key's
                                // group, as a nested level does.
                                RecordMember::Metadata { key, group } => {
                                    let level = self.meta_level(group)?;
                                    let expr = self.b.metadata(level)?;
                                    members.push((From::Reduced(red_items.len()), Naming::As(Name::new(key.clone()))));
                                    red_items.push(Item { expr, naming: Naming::Computed });
                                }
                                other => {
                                    let made = self.made_members(std::slice::from_ref(other), CallPosition::Value)?;
                                    for item in made {
                                        // The member's key is decided before staging; the
                                        // staged group's column is the stage's own, so it
                                        // collides with no name the author publishes.
                                        let key = crate::pipeline::middle::core::node::expr::record_key(&self.b, &item);
                                        members.push((From::Key(key_items.len()), key.map_or(Naming::Computed, Naming::As)));
                                        key_items.push(Item { expr: item.expr, naming: Naming::Computed });
                                    }
                                }
                            }
                        }
                        slots.push(Slot::Made(members, one.naming.clone()));
                    }
                    _ => {
                        let start = key_items.len();
                        self.out_items(item, CallPosition::Value, &mut key_items)?;
                        slots.extend((start..key_items.len()).map(|i| Slot::One(From::Key(i))));
                    }
                }
            }
            for item in reductions.iter() {
                let start = red_items.len();
                match item {
                    ReductionItem::Out(out) => self.out_items(out, CallPosition::Reduction, &mut red_items)?,
                    ReductionItem::Metadata(out) => {
                        let level = self.meta_level(&out.group)?;
                        red_items.push(Item {
                            expr: self.b.metadata(level)?,
                            naming: out.naming.clone().map_or(Naming::Computed, Naming::As),
                        });
                    }
                    ReductionItem::Pivot(_) | ReductionItem::Delegate(_) => {
                        return Err(refuse::elaboration_contract("a reduction item the stage admitted"))
                    }
                }
                slots.extend((start..red_items.len()).map(|i| Slot::One(From::Reduced(i))));
            }
            Ok((key_items, red_items, slots))
        })();
        self.stages.pop();
        let (key_items, red_items, slots) = built?;
        let width = key_items.len();
        let namings: Vec<Naming> = key_items.iter().chain(red_items.iter()).map(|i| i.naming.clone()).collect();
        let input = self.close_top()?;
        let grouped = self.b.pipe(
            input,
            PipeOp::Group {
                keys: key_items,
                reductions: red_items,
            },
        )?;
        let binder = self.member_over(grouped)?;
        let column = |from: From| match from {
            From::Key(i) => i,
            From::Reduced(i) => width + i,
        };
        let mut items = Vec::new();
        for slot in slots {
            match slot {
                Slot::One(from) => {
                    let at = column(from);
                    items.push(Item {
                        expr: self.b.col(binder, at as u16),
                        naming: match &namings[at] {
                            Naming::Computed => Naming::Reference,
                            other => other.clone(),
                        },
                    });
                }
                Slot::Made(members, naming) => {
                    let members = members
                        .into_iter()
                        .map(|(from, naming)| Item {
                            expr: self.b.col(binder, column(from) as u16),
                            naming,
                        })
                        .collect();
                    let expr = self.b.construct(Layout::Record, members)?;
                    items.push(Item {
                        expr,
                        naming: naming.map_or(Naming::Computed, Naming::As),
                    });
                }
            }
        }
        let input = self.close_top()?;
        self.b.pipe(input, PipeOp::Project(items)).map(Some)
    }

    /// Open a run whose one member is `rel`, and its member's binder.
    pub(super) fn member_over(&mut self, rel: RelId) -> Result<crate::pipeline::middle::core::ids::BinderId, Refusal> {
        self.open_relation(rel)?;
        self.scopes
            .last()
            .and_then(|s| s.run.members().last().map(|m| m.binder()))
            .ok_or_else(|| refuse::elaboration_contract("a stage opened with no member"))
    }

    /// A collected tuple's members: values of the rows the collection
    /// reduces, addressed by position, so they publish no name.
    fn collect_tuple(&mut self, tuple: &facade::Tuple) -> Result<Vec<CollectMember>, Refusal> {
        let mut items = Vec::with_capacity(tuple.elements.len());
        for element in &tuple.elements {
            match element {
                TupleElement::Value(value) => items.push(Item {
                    expr: self.value(value, CallPosition::Reduced)?,
                    naming: Naming::Computed,
                }),
                TupleElement::Spread(spread) => self.spread_items(spread, false, &mut items)?,
            }
        }
        Ok(items
            .into_iter()
            .map(|item| {
                CollectMember::Item(Item {
                    expr: item.expr,
                    naming: Naming::Computed,
                })
            })
            .collect())
    }

    /// A collected record's members: values of the rows the collection
    /// reduces.
    fn collect_members(&mut self, members: &[RecordMember]) -> Result<Vec<CollectMember>, Refusal> {
        let mut out = Vec::new();
        for member in members {
            match member {
                RecordMember::SelfKeyed(named) => {
                    let expr = self.reference(&Reference::Named(named.clone()))?;
                    out.push(CollectMember::Item(Item {
                        expr,
                        naming: Naming::Reference,
                    }));
                }
                RecordMember::Keyed { key, value } => {
                    let expr = self.value(value, CallPosition::Reduced)?;
                    out.push(CollectMember::Item(Item {
                        expr,
                        naming: Naming::As(Name::new(key.clone())),
                    }));
                }
                RecordMember::Induced { key, value } => match &**value {
                    Enclyph::Record(record) => {
                        let inner = self.collect_members(&record.members)?;
                        out.push(CollectMember::Nested(Name::new(key.clone()), Layout::Record, inner));
                    }
                    Enclyph::EmptyRecord(never) => match *never {},
                    Enclyph::Tuple(tuple) => {
                        let inner = self.collect_tuple(tuple)?;
                        out.push(CollectMember::Nested(Name::new(key.clone()), Layout::Tuple, inner));
                    }
                },
                // A spread in a record keys its members as in a made record
                // (`spread_items`): a glob by the names the run publishes, a
                // regex or a span by its columns' names.
                RecordMember::Spread(spread) => {
                    let mut items = Vec::new();
                    self.spread_items(spread, true, &mut items)?;
                    out.extend(items.into_iter().map(CollectMember::Item));
                }
                RecordMember::Metadata { key, group } => {
                    let level = self.meta_level(group)?;
                    out.push(CollectMember::Metadata(Name::new(key.clone()), level));
                }
            }
        }
        // THE RECORD KEY LAW, by its one decider: the same occurrence
        // under one key is one member; one key over two values refuses.
        let items: Vec<Item> = out
            .iter()
            .filter_map(|m| match m {
                CollectMember::Item(item) => Some(item.clone()),
                CollectMember::Nested(..) | CollectMember::Metadata(..) => None,
            })
            .collect();
        let mut kept = crate::pipeline::middle::core::node::expr::record_keys(&self.b, items, &[])?.into_iter().peekable();
        out.retain(|m| match m {
            CollectMember::Item(item) => match kept.peek() {
                Some(next) if next.expr == item.expr => {
                    kept.next();
                    true
                }
                _ => false,
            },
            CollectMember::Nested(..) | CollectMember::Metadata(..) => true,
        });
        Ok(out)
    }

    /// THE METADATA TARGET IS A COLLECTOR: one level of a metadata group in
    /// a collected record, its key read from the rows the collection
    /// reduces and its target collected per key.
    fn meta_level(&mut self, group: &facade::MetadataGroup) -> Result<crate::pipeline::middle::core::node::MetaLevel, Refusal> {
        use crate::pipeline::middle::core::node::{MetaLevel, MetaTarget};
        let key = self.named_reference(&NamedReference(group.key.clone()))?;
        let target = match &group.target {
            facade::MetadataTarget::Group(inner) => MetaTarget::Group(Box::new(self.meta_level(inner)?)),
            facade::MetadataTarget::Enclyph(Enclyph::Record(record)) => {
                MetaTarget::Collect(Layout::Record, self.collect_members(&record.members)?)
            }
            facade::MetadataTarget::Enclyph(Enclyph::Tuple(tuple)) => {
                MetaTarget::Collect(Layout::Tuple, self.collect_tuple(tuple)?)
            }
            facade::MetadataTarget::Enclyph(Enclyph::EmptyRecord(never)) => match *never {},
        };
        Ok(MetaLevel { key, target })
    }

    fn standard(&mut self, standard: &StandardApplication, at: CallPosition) -> Result<ExprId, Refusal> {
        let under = match &standard.window {
            Some(window) => Under::Own(window),
            None => Under::None,
        };
        self.standard_under(standard, at, under)
    }

    /// A call under the window `under` names. A windowed use of a DQL value
    /// definition hands its window to the call the definition's body is
    /// (THE CALL POSITION ASCRIBES GRADE: a window position's contract
    /// flows inward through a DQL wrapper to the call in its body).
    pub(super) fn standard_under(
        &mut self,
        standard: &StandardApplication,
        at: CallPosition,
        under: Under<'_>,
    ) -> Result<ExprId, Refusal> {
        if let Some(guard) = &standard.guard {
            return self.guarded(standard, guard, at, under);
        }
        let call = standard.call.call();
        let (written, qualifier) = self.input.callee_name(&call.callee);
        let name = written.as_str().to_string();
        // A qualified callee selects only in the one namespace its route
        // reaches, or names the target provider; a miss refuses and never
        // reaches the target (THE TARGET SURFACE IS OPEN, steps 3 and 4).
        if let Some(q) = &qualifier {
            let local = written.clone();
            let referred = self.judge(&local, Some(q))?;
            if !matches!(referred, crate::pipeline::middle::select::Referred::Provider) {
                let spelled = format!("{}.{name}", self.input.qualifier_spelled(q));
                let family = match referred.referent()? {
                    Some(crate::pipeline::middle::select::Referent::Family(family)) => family,
                    Some(_) => return Err(refuse::outside("an engine-served callee")),
                    None => return Err(refuse::qualified_callee_unknown(&spelled)),
                };
                let window = self.windowing(under)?;
                return self.catalog_call(&family, &call.arguments, at, window);
            }
        }
        if name == "cast" && qualifier.is_none() {
            if under.windowed() {
                return Err(refuse::not_a_window(&name, "a scalar function"));
            }
            return self.cast(&call.arguments, at);
        }
        if matches!(&call.arguments, CallArguments::Scalar(args) if args.iter().any(is_star)) {
            if self.input.target_admits_star(&name, argument_count(&call.arguments)) == Some(false) {
                return Err(refuse::star_argument(&name, self.input.target_family()));
            }
        }
        if qualifier.is_none() {
            let local = written.clone();
            // A callable formal of the body being read is applied; under a
            // window, the callable the formal is bound to is windowed.
            if let Some(callable) = self.callable_formal(&local) {
                return match self.windowing(under)? {
                    Some(window) => self.windowed_callable(&local, callable, &call.arguments, window),
                    None => self.apply_callable(&local, callable, &call.arguments, at),
                };
            }
            if let Some((block, _)) = self.claim(&local, facade::QueryLocalDemand::Value)? {
                let window = self.windowing(under)?;
                let definition = self.local_value_definition(block, &local)?;
                return self.value_call(definition, &call.arguments, at, window);
            }
            // The callable lookup's first step: a visible DQL value function
            // of the catalog is selected before the target is called.
            if let Some(crate::pipeline::middle::select::Referent::Family(family)) = self.refer(&local, None)? {
                let window = self.windowing(under)?;
                return self.catalog_call(&family, &call.arguments, at, window);
            }
        }
        // A context marker selects a definition's context calling mode; a
        // target function declares none.
        if matches!(&call.arguments, CallArguments::Scalar(args) if args.iter().any(|a| matches!(a, ScalarArgument::Context(_)))) {
            return Err(refuse::context_marker_at_target(&name));
        }
        let callee = Callee { name: name.clone() };
        if under.windowed() {
            // A scalar built-in is no window function: a window cannot ride
            // it. Its arguments are judged first.
            let args = self.arguments(&call.arguments, CallPosition::Value)?;
            if self.input.known_call(&name, args.len()) == KnownCall::Scalar {
                return Err(refuse::not_a_window(&name, "a scalar function"));
            }
            let (partition, order, frame) = self
                .windowing(under)?
                .ok_or_else(|| refuse::elaboration_contract("a windowed call with no window"))?;
            return self.b.window(callee, args, partition, order, frame);
        }
        let arity = self.argument_arity(&call.arguments)?;
        let known = match self.input.known_call(&name, arity) {
            KnownCall::Scalar => Known::Scalar,
            KnownCall::Window => Known::Window,
            KnownCall::Aggregate => Known::Aggregate,
            KnownCall::Unknown => Known::Unknown,
        };
        let reduces = self.input.target_reduces(&name, arity);
        let args = self.arguments(&call.arguments, grade::argument_position(at, known, reduces))?;
        let e = match self.column {
            Some(column) => self.b.call_within(callee, args, at, known, reduces, column, &self.switches)?,
            None => self.b.call(callee, args, at, known, reduces)?,
        };
        // Outside an interior a contradicted grade refuses where it stands;
        // inside one it is judged where the interior's member is admitted.
        if let ExprKind::Call {
            grade: Grade::Contradicted(c),
            ..
        } = self.b.expr(e).kind()
        {
            if self.interiors == 0 {
                return Err(crate::pipeline::middle::core::decide::admission::grade_refusal(&name, *c));
            }
        }
        Ok(e)
    }

    /// A guarded call `f:(args | guard)` (domain-expressions FN.45): its
    /// guard a truth over the rows its arguments are read from. On a target
    /// function the call's constructor admits it (`Builder::guarded_call`,
    /// `Builder::guarded_window`): an aggregate's guard filters its
    /// contributions, a windowed aggregate's those of its frame, and a
    /// row-wise call's makes its value NULL where the guard fails. A DQL
    /// value definition, a callable formal or a cast is a row-wise call: its
    /// value is wrapped where it stands. A guard on any of them under a
    /// window, or on one whose value reads a population, is not built.
    fn guarded(&mut self, standard: &StandardApplication, guard: &TruthExpression, at: CallPosition, under: Under<'_>) -> Result<ExprId, Refusal> {
        let call = standard.call.call();
        let (local, qualifier) = self.input.callee_name(&call.callee);
        let name = local.as_str().to_string();
        let defined = qualifier.is_some()
            || name == "cast"
            || self.callable_formal(&local).is_some()
            || self.claim(&local, facade::QueryLocalDemand::Value)?.is_some()
            || matches!(self.refer(&local, None)?, Some(crate::pipeline::middle::select::Referent::Family(_)));
        if defined {
            if under.windowed() {
                return Err(refuse::outside("a guard on a windowed call of a DQL definition, a formal or a cast"));
            }
            let value = self.standard_under(&StandardApplication::plain(standard.call.clone()), at, Under::None)?;
            if crate::pipeline::middle::core::node::expr::population_sensitive(&self.b, value) {
                return Err(refuse::outside("a guard on a DQL definition whose value reads a population"));
            }
            let holds = self.truth_in(guard, Consumer::Value, at)?;
            return self.b.case(None, vec![(CaseArm::Truth(holds), value)], None, &self.switches);
        }
        let arity = self.argument_arity(&call.arguments)?;
        let known = match self.input.known_call(&name, arity) {
            KnownCall::Scalar => Known::Scalar,
            KnownCall::Window => Known::Window,
            KnownCall::Aggregate => Known::Aggregate,
            KnownCall::Unknown => Known::Unknown,
        };
        let reduces = self.input.target_reduces(&name, arity);
        let callee = Callee { name: name.clone() };
        if under.windowed() {
            match known {
                Known::Scalar => return Err(refuse::not_a_window(&name, "a scalar function")),
                Known::Window => return Err(refuse::guard_on_window_function(&name)),
                Known::Unknown if !reduces => {
                    return Err(refuse::outside("a guard on a windowed call of a function not known to aggregate"))
                }
                Known::Aggregate | Known::Unknown => {}
            }
            let args = self.arguments(&call.arguments, CallPosition::Value)?;
            let holds = self.truth_in(guard, Consumer::Value, CallPosition::Value)?;
            let (partition, order, frame) = self
                .windowing(under)?
                .ok_or_else(|| refuse::elaboration_contract("a windowed call with no window"))?;
            return self.b.guarded_window(callee, args, holds, partition, order, frame, &self.switches);
        }
        let position = grade::argument_position(at, known, reduces);
        let args = self.arguments(&call.arguments, position)?;
        let holds = self.truth_in(guard, Consumer::Value, position)?;
        let admitted = match self.column {
            Some(column) => self.b.and(vec![column, holds]),
            None => holds,
        };
        let e = self.b.guarded_call(callee, args, at, known, reduces, admitted, &self.switches)?;
        if let ExprKind::Call {
            grade: Grade::Contradicted(c),
            ..
        } = self.b.expr(e).kind()
        {
            if self.interiors == 0 {
                return Err(crate::pipeline::middle::core::decide::admission::grade_refusal(&name, *c));
            }
        }
        Ok(e)
    }

    /// A value definition's arguments: plain values, each elaborated at
    /// `at`; a distinct or star argument is no value of it.
    pub(super) fn plain_values(&mut self, arguments: &CallArguments, at: CallPosition, callee: &str) -> Result<Vec<ExprId>, Refusal> {
        let mut values = Vec::new();
        for arg in self.arguments(arguments, at)? {
            match arg {
                Arg::Value { expr, distinct: false } => values.push(expr),
                Arg::Value { distinct: true, .. } | Arg::Star => {
                    return Err(refuse::outside(&format!("a distinct or star argument to {callee}")))
                }
            }
        }
        Ok(values)
    }

    /// `cast:(value, ::type)`: the second argument is a type symbol the
    /// language names, never a string or a column; the value is converted
    /// to that type.
    fn cast(&mut self, arguments: &CallArguments, at: CallPosition) -> Result<ExprId, Refusal> {
        const TYPES: &[&str] = &["integer", "real", "text", "numeric", "boolean"];
        let types = TYPES.join("|");
        let members: Vec<&ScalarArgument> = match arguments {
            CallArguments::Scalar(args) => args.iter().collect(),
            CallArguments::None => Vec::new(),
            CallArguments::HigherOrder(_) => return Err(refuse::outside("a higher-order call in value position")),
        };
        if members.len() != 2 {
            return Err(refuse::cast(&format!(
                "cast: expects exactly 2 arguments: cast:(expr, type), got {}",
                members.len()
            )));
        }
        let symbol_expected = format!("cast: second argument must be a type symbol. Types: {types}");
        let ScalarArgument::Value(type_arg) = members[1] else {
            return Err(refuse::cast(&symbol_expected));
        };
        let type_name = match &type_arg.value {
            DomainExpression::Application(FunctionApplication::Ground(LiteralValue::Symbol(symbol))) => symbol.clone(),
            DomainExpression::Reference(Reference::Named(NamedReference(column))) if column.qualifier.is_none() => {
                return Err(refuse::cast(&format!(
                    "cast: a bare name is use; the type is a tag — write cast:(x, ::integer). Types: {types}"
                )))
            }
            DomainExpression::Application(FunctionApplication::Ground(_)) => {
                return Err(refuse::cast(&format!(
                    "cast: takes a type symbol, not a string — write cast:(x, ::integer). Types: {types}"
                )))
            }
            _ => return Err(refuse::cast(&symbol_expected)),
        };
        if !TYPES.contains(&type_name.as_str()) {
            return Err(refuse::cast(&format!(
                "cast: unknown type '{type_name}'. Types: {types} (date/timestamp and parameterized types are not \
                 yet supported)"
            )));
        }
        let ScalarArgument::Value(value) = members[0] else {
            return Err(refuse::outside("a cast of a non-value argument"));
        };
        let value = self.value(&value.value, CallPosition::Value)?;
        let ty = self.b.constant(LiteralValue::Symbol(type_name));
        let args = vec![
            Arg::Value { expr: value, distinct: false },
            Arg::Value { expr: ty, distinct: false },
        ];
        self.b.call(Callee { name: "cast".to_string() }, args, at, Known::Scalar, false)
    }

    pub(super) fn arguments(&mut self, arguments: &CallArguments, at: CallPosition) -> Result<Vec<Arg>, Refusal> {
        match arguments {
            CallArguments::None => Ok(Vec::new()),
            CallArguments::Scalar(args) => {
                let mut out = Vec::new();
                for arg in args {
                    out.push(match arg {
                        ScalarArgument::Value(value) => Arg::Value {
                            expr: self.value(&value.value, at)?,
                            distinct: value.distinct,
                        },
                        ScalarArgument::Star => Arg::Star,
                        ScalarArgument::Spread(Spread::Glob(glob))
                            if glob.qualifier.is_none() && glob.namespace_path.is_empty() =>
                        {
                            Arg::Star
                        }
                        // An addressing spread expands into the argument row
                        // (FN.35): each column it addresses is one argument.
                        ScalarArgument::Spread(spread @ (Spread::Regex(_) | Spread::PositionalSpan(_))) => {
                            for (expr, _) in self.spread_values(spread)? {
                                out.push(Arg::Value { expr, distinct: false });
                            }
                            continue;
                        }
                        ScalarArgument::Spread(_)
                        | ScalarArgument::Callable(_)
                        | ScalarArgument::Context(_) => {
                            return Err(refuse::outside("this call argument"))
                        }
                    });
                }
                Ok(out)
            }
            CallArguments::HigherOrder(_) => Err(refuse::outside("a higher-order call in value position")),
        }
    }

    /// The number of arguments a call's row holds once its addressing
    /// spreads are expanded: THE FINAL ROW decides which overload a target
    /// call is.
    fn argument_arity(&mut self, arguments: &CallArguments) -> Result<usize, Refusal> {
        let CallArguments::Scalar(args) = arguments else {
            return Ok(argument_count(arguments));
        };
        let mut count = 0;
        for arg in args {
            count += match arg {
                ScalarArgument::Spread(spread @ (Spread::Regex(_) | Spread::PositionalSpan(_))) => {
                    self.spread_values(spread)?.len()
                }
                _ => 1,
            };
        }
        Ok(count)
    }

    fn case(&mut self, case: &CaseExpression, at: CallPosition) -> Result<ExprId, Refusal> {
        match case {
            CaseExpression::Anchored {
                anchor,
                arms,
                default,
            } => {
                let anchor = self.value(anchor, at)?;
                let mut out = Vec::new();
                for arm in arms.iter() {
                    let result = self.value(&arm.result, at)?;
                    out.push((CaseArm::Literal(arm.term.clone()), result));
                }
                let default = match default {
                    Some(d) => Some(self.value(d, at)?),
                    None => None,
                };
                self.b.case(Some(anchor), out, default, &self.switches)
            }
            CaseExpression::Searched { arms, default } => {
                let mut out = Vec::new();
                for arm in arms.iter() {
                    let t = self.truth(&arm.condition, Consumer::Value)?;
                    let result = self.value(&arm.result, at)?;
                    out.push((CaseArm::Truth(t), result));
                }
                let default = match default {
                    Some(d) => Some(self.value(d, at)?),
                    None => None,
                };
                self.b.case(None, out, default, &self.switches)
            }
        }
    }

    pub(super) fn truth(&mut self, truth: &TruthExpression, consumer: Consumer) -> Result<TruthId, Refusal> {
        self.truth_in(truth, consumer, CallPosition::Value)
    }

    /// A truth whose values stand at `at`: a condition's at a row-wise
    /// position, a crossed truth's where the crossing stands.
    #[stacksafe::stacksafe]
    fn truth_in(&mut self, truth: &TruthExpression, consumer: Consumer, at: CallPosition) -> Result<TruthId, Refusal> {
        match truth {
            TruthExpression::Comparison(c) => {
                let l = self.value(&c.left, at)?;
                let r = self.value(&c.right, at)?;
                let own_row = self.reads_own_row(&[l, r]);
                self.b.cmp(c.operator, l, r, consumer, own_row, &self.switches)
            }
            TruthExpression::Conjunction(parts) => {
                let mut ids = Vec::new();
                for p in parts.iter() {
                    ids.push(self.truth_in(p, consumer, at)?);
                }
                Ok(self.b.and(ids))
            }
            TruthExpression::Disjunction(parts) => {
                let mut ids = Vec::new();
                for p in parts.iter() {
                    ids.push(self.truth_in(p, consumer, at)?);
                }
                Ok(self.b.or(ids))
            }
            TruthExpression::Not { expr } => {
                let t = self.truth_in(expr, consumer, at)?;
                Ok(self.b.not(t))
            }
            TruthExpression::Existence(existence) => {
                let rel = self.chain(&existence.relation)?;
                let positive = matches!(existence.polarity, facade::Polarity::Positive);
                crate::pipeline::middle::core::decide::effect::fence(&self.b, rel, "an existence")?;
                Ok(self.b.exists(positive, rel))
            }
            TruthExpression::Sigma(sigma) => {
                let NamedProof::Call(call) = &sigma.proof else {
                    return Err(refuse::outside("a bound sigma body"));
                };
                let call = call.call();
                let (name, qualifier) = self.input.callee_name(&call.callee);
                let args = self.arguments(&call.arguments, CallPosition::Value)?;
                let mut values = Vec::new();
                for arg in args {
                    match arg {
                        Arg::Value { expr, .. } => values.push(expr),
                        Arg::Star => return Err(refuse::outside("a star argument to a predicate")),
                    }
                }
                let positive = matches!(sigma.polarity, facade::Polarity::Positive);
                if qualifier.is_none() {
                    if let Some(rel) = self.cited_relation(&name, &values)? {
                        crate::pipeline::middle::core::decide::effect::fence(&self.b, rel, "an existence")?;
                        return Ok(self.b.exists(positive, rel));
                    }
                }
                let proof = self.sigma_proof(&name, qualifier.as_ref(), &values, consumer)?;
                self.b.sigma(proof, values, positive)
            }
            TruthExpression::Membership(membership) => match self.column_self {
                super::ColumnSelf::Outside => self.membership(membership, at),
                super::ColumnSelf::Column(_) | super::ColumnSelf::Table => self.check_membership(membership, consumer),
            },
            TruthExpression::RelationalMembership(membership) => self.relational_membership(membership, at),
        }
    }

    /// MEMBERSHIP (equality-law rows 5–6): an existence over the candidate
    /// rows, a row matching when it equals the probe component by component.
    /// Each match is judged by the probe's source, as every comparison is: a
    /// probe reading a row corresponds with a candidate, so NULL matches
    /// nothing and `not in` stays two-valued; a wholly ground probe is a
    /// local null-safe test. A literal list is an anonymous table whose
    /// header constrains each column to the probe's component.
    fn membership(&mut self, membership: &facade::Membership, at: CallPosition) -> Result<TruthId, Refusal> {
        let witness = membership.source == facade::MembershipSource::WitnessAnon;
        if witness {
            // An existence marker introduces no column: each header names one
            // the rows to its left already publish.
            for value in membership.probe.values() {
                if let Some(name) = facade::bare_name(value) {
                    if !self.publishes(&Name::new(&name)) {
                        return Err(refuse::witness_shape(&name));
                    }
                }
            }
        }
        let probe = self.probe(&membership.probe, at)?;
        let mut rows = Vec::with_capacity(membership.rows.len());
        for row in membership.rows.iter() {
            if row.width() != probe.len() {
                return Err(refuse::membership_arity(row.width(), probe.len()));
            }
            let mut cells = Vec::with_capacity(probe.len());
            for candidate in row.values() {
                cells.push(self.value(candidate, at)?);
            }
            rows.push(cells);
        }
        if witness {
            // A header's own lvar among the candidates makes the test vacuous.
            for (component, value) in probe.iter().zip(membership.probe.values()) {
                let cell = crate::pipeline::middle::core::node::rel::referenced_cell(&self.b, *component);
                if cell.is_some()
                    && rows.iter().flatten().any(|c| crate::pipeline::middle::core::node::rel::referenced_cell(&self.b, *c) == cell)
                {
                    return Err(refuse::header_row_lvar(&facade::bare_name(value).unwrap_or_default()));
                }
            }
        }
        let header = probe
            .into_iter()
            .map(crate::pipeline::middle::core::node::rel::HeaderSpec::Constraint)
            .collect();
        let candidates = self.b.lit(header, rows, false, &self.switches)?;
        Ok(self.b.exists(!membership.negated, candidates))
    }

    /// A membership in a relation's rows: an existence over the relation, its
    /// displayed positions each equal to the probe's component.
    fn relational_membership(&mut self, membership: &facade::RelationalMembership, at: CallPosition) -> Result<TruthId, Refusal> {
        let probe = self.probe(&membership.probe, at)?;
        // A probe some relation supplies without naming a column (a scalar
        // subquery) corresponds with each candidate; the comparison's class
        // counts named occurrences only.
        if probe.iter().any(|p| {
            self.b.expr(*p).occurrences().is_empty()
                && crate::pipeline::middle::core::node::expr::relation_supplied(&self.b, *p)
        }) {
            return Err(refuse::outside("a relational membership whose probe a relation supplies without naming a column"));
        }
        let candidates = self.chain(&membership.relation)?;
        crate::pipeline::middle::core::decide::effect::fence(&self.b, candidates, "a membership")?;
        let width = self.b.rel(candidates).heading().displayed().count();
        if width != probe.len() {
            return Err(refuse::membership_arity(width, probe.len()));
        }
        self.open_relation(candidates)?;
        if let Err(e) = self.match_probe(&probe) {
            self.scopes.pop();
            return Err(e);
        }
        let matched = self.close_top()?;
        Ok(self.b.exists(!membership.negated, matched))
    }

    /// The probe's components, in written order, each a value read where
    /// the membership stands. A component evaluated over a population (a
    /// window, an aggregate, a collection) is that population's value: it is
    /// evaluated over the members standing here, once, before the
    /// candidates' existence reads it (equality-law: judge the probe's
    /// source before matching), never over the candidates' own rows.
    fn probe(&mut self, probe: &facade::Probe, at: CallPosition) -> Result<Vec<ExprId>, Refusal> {
        let standing: Vec<crate::pipeline::middle::core::ids::BinderId> =
            self.scopes.last().map(|s| s.run.members().map(|m| m.binder()).collect()).unwrap_or_default();
        let mut out = Vec::new();
        for value in probe.values() {
            let component = self.value(value, at)?;
            let component = if crate::pipeline::middle::core::node::expr::population_sensitive(&self.b, component) {
                self.b.argument(component, &standing)?
            } else {
                component
            };
            out.push(component);
        }
        Ok(out)
    }

    /// Each displayed position of the open run equal to the probe's
    /// component at its place.
    fn match_probe(&mut self, probe: &[ExprId]) -> Result<(), Refusal> {
        let cells: Vec<crate::pipeline::middle::core::node::Cell> = self
            .scopes
            .last()
            .expect("the membership's run")
            .run
            .outputs()
            .iter()
            .filter(|(p, _)| !matches!(p.visibility, crate::pipeline::middle::core::heading::Visibility::Hidden(_)))
            .map(|(_, c)| *c)
            .collect();
        for (component, cell) in probe.iter().zip(cells) {
            let candidate = self.cell_value(cell);
            let own_row = self.reads_own_row(&[*component, candidate]);
            let equal = self.b.cmp(
                facade::CmpOp::NullSafeEqual,
                *component,
                candidate,
                Consumer::Filter,
                own_row,
                &self.switches,
            )?;
            let scope = self.scopes.last_mut().expect("the membership's run");
            scope.run.push_guard(&self.b, equal);
        }
        Ok(())
    }

    /// A membership written in a declared table's CHECK. Whether a NULL
    /// probe is in a set holding a NULL candidate is unruled; the switch
    /// carries today's reading. The probe is in the set when it equals some
    /// candidate row, component by component, each equality a verdict about
    /// the declared row.
    fn check_membership(&mut self, membership: &facade::Membership, consumer: Consumer) -> Result<TruthId, Refusal> {
        match self.switches.check_membership {
            crate::pipeline::middle::core::switches::CheckMembership::NullSafe => {}
        }
        let mut probe = Vec::new();
        for value in membership.probe.values() {
            probe.push(self.value(value, CallPosition::Value)?);
        }
        let mut found: Option<TruthId> = None;
        for row in membership.rows.iter() {
            if row.width() != probe.len() {
                return Err(refuse::membership_arity(row.width(), probe.len()));
            }
            let mut matched: Option<TruthId> = None;
            for (component, candidate) in probe.iter().zip(row.values()) {
                let candidate = self.value(candidate, CallPosition::Value)?;
                let own_row = self.reads_own_row(&[*component, candidate]);
                let equal = self.b.cmp(
                    facade::CmpOp::NullSafeEqual,
                    *component,
                    candidate,
                    consumer,
                    own_row,
                    &self.switches,
                )?;
                matched = Some(match matched {
                    None => equal,
                    Some(before) => self.b.and(vec![before, equal]),
                });
            }
            let matched = matched.ok_or_else(|| refuse::membership_arity(0, probe.len()))?;
            found = Some(match found {
                None => matched,
                Some(before) => self.b.or(vec![before, matched]),
            });
        }
        let found = found.ok_or_else(|| refuse::elaboration_contract("a membership with no candidate row"))?;
        Ok(match membership.negated {
            true => self.b.not(found),
            false => found,
        })
    }

    /// Whether the values read a row of the relation they stand in. Inside
    /// an interior standing in a pipe stage or an ordering, values that read
    /// only the stage's input (and what encloses it) read one formed row, not
    /// rows of their own relation; everywhere else they do.
    pub(super) fn reads_own_row(&self, values: &[ExprId]) -> bool {
        use crate::pipeline::middle::core::node::{Cell, Occurrence};
        let Some(&stage) = self.stages.last() else {
            return true;
        };
        if self.scopes.len() <= stage + 1 {
            return true;
        }
        let mut binders = Vec::new();
        let mut merges = Vec::new();
        for scope in &self.scopes[..=stage] {
            binders.extend(scope.run.members().map(|m| m.binder()));
            merges.extend(scope.run.outputs().iter().filter_map(|(_, c)| match c {
                Cell::Merged(m) => Some(*m),
                Cell::Col(..) => None,
            }));
        }
        let mut occurrences = values.iter().flat_map(|v| self.b.expr(*v).occurrences().iter().copied()).peekable();
        if occurrences.peek().is_none() {
            return true;
        }
        !occurrences.all(|o| match o {
            Occurrence::Binder(b) => binders.contains(&b),
            Occurrence::Merge(m) => merges.contains(&m),
        })
    }

    /// The window `under` names, elaborated where the call it is written
    /// on stands; a window handed in was elaborated where its use stands.
    fn windowing(&mut self, under: Under<'_>) -> Result<Option<Windowing>, Refusal> {
        let window = match under {
            Under::None => return Ok(None),
            Under::Handed(window) => return Ok(Some(window)),
            Under::Own(window) => window,
        };
        let mut partition = Vec::new();
        for p in &window.partition {
            partition.push(self.value(p, CallPosition::Value)?);
        }
        let order = self.order_keys_of(&window.ordering)?;
        let frame = match &window.frame {
            None => None,
            Some(frame) => Some(Frame {
                rows: matches!(frame.mode, FrameMode::Rows),
                start: frame_edge(&frame.start)?,
                end: frame_edge(&frame.end)?,
            }),
        };
        Ok(Some((partition, order, frame)))
    }

    pub(super) fn order_keys_of(
        &mut self,
        specs: &[facade::OrderingSpec],
    ) -> Result<Vec<crate::pipeline::middle::core::node::OrderKey>, Refusal> {
        use crate::pipeline::middle::core::node::{Direction, OrderKey};
        let mut keys = Vec::with_capacity(specs.len());
        for spec in specs {
            keys.push(OrderKey {
                expr: self.value(&spec.column, CallPosition::Value)?,
                direction: match spec.direction {
                    Some(facade::OrderDirection::Descending) => Direction::Descending,
                    Some(facade::OrderDirection::Ascending) | None => Direction::Ascending,
                },
            });
        }
        Ok(keys)
    }

    /// The items a stage publishes, each out item's in written order. A
    /// selecting spread (a regex, a positional span) selecting a named column
    /// another spread of the stage also selects — a selecting spread or a
    /// glob, or, in an embed, the input's own carried columns — authors that
    /// name twice and refuses (DUPLICATE AUTHORED NAMES REFUSE); a column no
    /// name answers to publishes again under a fresh mint. Two globs'
    /// collision mints both (COLLIDING OUTPUTS POISON BOTH), and an embed's
    /// glob over its carried columns is that collision.
    fn published_items<'i>(&mut self, items: impl Iterator<Item = &'i OutItem>, embed: bool) -> Result<Vec<Item>, Refusal> {
        use crate::pipeline::middle::core::node::rel::{referenced_cell, referenced_position};
        use crate::pipeline::middle::core::node::Cell;
        let mut out = Vec::new();
        let mut selected: Vec<Cell> = Vec::new();
        let mut globbed: Vec<Cell> = Vec::new();
        if embed {
            if let Some(top) = self.scopes.last() {
                globbed.extend(
                    top.run
                        .outputs()
                        .iter()
                        .filter(|(p, _)| !matches!(p.visibility, crate::pipeline::middle::core::heading::Visibility::Hidden(_)))
                        .map(|(_, c)| *c),
                );
            }
        }
        for item in items {
            let start = out.len();
            self.out_items(item, CallPosition::Value, &mut out)?;
            let OutItem::Many(spread) = item else {
                continue;
            };
            let selecting = matches!(spread, Spread::Regex(_) | Spread::PositionalSpan(_));
            for item in &out[start..] {
                let Some(cell) = referenced_cell(&self.b, item.expr) else {
                    continue;
                };
                if selected.contains(&cell) || (selecting && globbed.contains(&cell)) {
                    if let Some(name) = referenced_position(&self.b, item.expr).and_then(|p| p.answering_name().cloned()) {
                        return Err(refuse::duplicate_name(&name, true));
                    }
                }
                match selecting {
                    true => selected.push(cell),
                    false => globbed.push(cell),
                }
            }
        }
        Ok(out)
    }

    /// One out item, or the several a glob covers.
    pub(super) fn out_items(&mut self, item: &OutItem, at: CallPosition, out: &mut Vec<Item>) -> Result<(), Refusal> {
        match item {
            OutItem::One(one) => {
                let expr = self.value(&one.expr, at)?;
                let naming = match &one.naming {
                    Some(name) => Naming::As(name.clone()),
                    None => match &one.expr {
                        DomainExpression::Reference(_) => Naming::Reference,
                        DomainExpression::Application(_) => Naming::Computed,
                    },
                };
                out.push(Item { expr, naming });
            }
            OutItem::Many(Spread::Glob(glob)) => {
                if !glob.namespace_path.is_empty() {
                    return Err(refuse::outside("a namespaced glob"));
                }
                let naming = glob_naming(glob.qualifier.is_some());
                for expr in self.glob(glob.qualifier.as_ref())? {
                    out.push(Item {
                        expr,
                        naming: naming.clone(),
                    });
                }
            }
            OutItem::Many(spread @ (Spread::Regex(_) | Spread::PositionalSpan(_))) => {
                // A qualified span, as a qualified glob, publishes the
                // member's own positions.
                if let Spread::PositionalSpan(span) = spread {
                    if let (Some(q), true) = (&span.qualifier, span.namespace_path.is_empty()) {
                        for expr in self.qualified_span(q, span.start, span.end)? {
                            out.push(Item {
                                expr,
                                naming: Naming::QualifiedGlob,
                            });
                        }
                        return Ok(());
                    }
                }
                for (expr, _) in self.spread_values(spread)? {
                    out.push(Item {
                        expr,
                        naming: Naming::Glob,
                    });
                }
            }
            OutItem::Whole => return Err(refuse::outside("this projection item")),
        }
        Ok(())
    }

    /// A pipe operator's items, resolved against the run it consumes.
    pub(super) fn pipe_op(&mut self, operator: &AstPipeOp) -> Result<Stage, Refusal> {
        match operator {
            AstPipeOp::Project(items) => Ok(Stage::Op(PipeOp::Project(self.published_items(items.iter(), false)?))),
            AstPipeOp::Embed(items) => Ok(Stage::Op(PipeOp::Embed(self.published_items(items.iter(), true)?))),
            AstPipeOp::Group(GroupSpec::Distinct { keys }) => {
                Ok(Stage::Op(PipeOp::Distinct(self.published_items(keys.iter(), false)?)))
            }
            AstPipeOp::Group(GroupSpec::Reduce {
                keys, reductions, ..
            }) => {
                let key_items = self.published_items(keys.iter(), false)?;
                let mut red_items = Vec::new();
                let mut pivoted = Vec::new();
                for item in reductions.iter() {
                    match item {
                        ReductionItem::Out(out) => self.out_items(out, CallPosition::Reduction, &mut red_items)?,
                        ReductionItem::Pivot(pivot) => self.pivot(pivot, &key_items, &mut pivoted, &mut red_items)?,
                        ReductionItem::Delegate(spec) => self.delegate(spec, &key_items, &mut red_items)?,
                        ReductionItem::Metadata(out) => {
                            let level = self.meta_level(&out.group)?;
                            red_items.push(Item {
                                expr: self.b.metadata(level)?,
                                naming: out.naming.clone().map_or(Naming::Computed, Naming::As),
                            });
                        }
                    }
                }
                Ok(Stage::Op(PipeOp::Group {
                    keys: key_items,
                    reductions: red_items,
                }))
            }
            AstPipeOp::ProjectOut(selectors) => {
                let mut dropped = Vec::new();
                // A PROJECT-OUT TAKES SELECTORS: each removes what it addresses.
                for selector in selectors {
                    match selector {
                        facade::SelectorItem::Reference(reference) => dropped.push(self.reference(reference)?),
                        // A glob removes the columns of the input it covers:
                        // a qualifier's columns the input no longer carries
                        // (a drill's consumed collection) are not among them.
                        facade::SelectorItem::Spread(spread @ facade::Spread::Glob(glob)) => {
                            if !glob.namespace_path.is_empty() {
                                return Err(refuse::outside("a namespaced glob"));
                            }
                            let carried: Vec<_> = self
                                .scopes
                                .last()
                                .map(|top| top.run.outputs().iter().map(|(_, c)| *c).collect())
                                .unwrap_or_default();
                            let values: Vec<_> = self
                                .glob(glob.qualifier.as_ref())?
                                .into_iter()
                                .filter(|v| {
                                    crate::pipeline::middle::core::node::rel::referenced_cell(&self.b, *v)
                                        .is_some_and(|c| carried.contains(&c))
                                })
                                .collect();
                            if values.is_empty() {
                                return Err(refuse::selector_empty(&super::cover::spelling(spread), &[]));
                            }
                            dropped.extend(values);
                        }
                        facade::SelectorItem::Spread(spread) => {
                            for (value, _) in self.spread_values(spread)? {
                                dropped.push(value);
                            }
                        }
                    }
                }
                Ok(Stage::Remove(dropped))
            }
            AstPipeOp::Transform { items, guard } => {
                // THE GUARD IS PER-CELL: where it holds the cell is
                // redefined, where it fails the cell rides through.
                let holds = match guard {
                    Some(guard) => Some(self.truth(guard, Consumer::Value)?),
                    None => None,
                };
                let mut cover = Vec::new();
                for item in items.iter() {
                    let target = match &item.qualifier {
                        Some(q) => self.resolve_ref(Address::Qualified(q, &item.naming))?,
                        None => self.transform_target(&item.naming)?,
                    };
                    let expr = self.value(&item.expr, CallPosition::Value)?;
                    let expr = match holds {
                        Some(holds) => self.b.case(None, vec![(CaseArm::Truth(holds), expr)], Some(target), &self.switches)?,
                        None => expr,
                    };
                    cover.push((target, expr));
                }
                Ok(Stage::Cover(cover))
            }
            AstPipeOp::MapCover(cover) => self.map_cover(&cover.callable, &cover.selector, cover.guard.as_deref()),
            AstPipeOp::EmbedMapCover(cover) => {
                self.embed_map_cover(&cover.callable, cover.naming.as_ref(), &cover.selector)
            }
            AstPipeOp::Rename(pairs) => self.rename_cover(pairs.iter()),
        }
    }

    /// A drill: the rows of a structured position of the run so far, as a
    /// member reading that row, named by the drilled column. A receipt's
    /// payload is drilled as any interior: its heading carries it.
    pub(super) fn drill(
        &mut self,
        drill: &crate::pipeline::middle::facade::AuthoredDrill,
    ) -> Result<(RelId, Name), Refusal> {
        let name = Name::new(drill.column.clone());
        let top = self.scopes.last().ok_or_else(|| refuse::outside("a drill with no scope"))?;
        let Some(index) = crate::pipeline::middle::core::heading::correspondence::answers_to(top.run.heading(), &name)
            .first()
            .copied()
        else {
            // A name two positions lost by collision is ambiguous; a column
            // a receipt member does not publish is a payload its receipt
            // does not declare.
            use crate::pipeline::middle::core::heading::NameState;
            let poisoned = top
                .run
                .heading()
                .positions()
                .iter()
                .any(|p| matches!(&p.name, NameState::Lost(lost) if *lost == name));
            let receipt = top.run.members().any(|m| is_receipt(&self.b, m.rel()));
            return Err(if poisoned {
                refuse::ambiguous_column(&name)
            } else if receipt {
                refuse::no_payload(&drill.column)
            } else {
                refuse::column(&drill.column, "the drilled column is not published")
            });
        };
        let (position, cell) = top.run.outputs()[index].clone();
        let crate::pipeline::middle::core::node::Cell::Col(..) = cell else {
            return Err(refuse::outside("a drill of a merged key"));
        };
        let value = self.cell_value(cell);
        let width = match crate::pipeline::middle::core::node::rel::unnest_heading(&position.interior, &drill.column) {
            Ok(h) => h.len(),
            Err(e) => return Err(e),
        };
        // The drill's binds: every position of the known interior (a glob),
        // or one slot per position: a name binds it, `_` skips it, a ground
        // term constrains it and is consumed.
        let mut binds = Vec::with_capacity(width);
        if drill.glob && drill.columns.is_empty() && drill.groundings.is_empty() {
            for k in 0..width {
                binds.push(Bind {
                    at: BindAt::Position(k as u16),
                    role: BindRole::Publish(None),
                });
            }
        } else {
            if drill.columns.len() != width {
                return Err(refuse::drill_arity(&drill.column, width, drill.columns.len()));
            }
            for (k, slot) in drill.columns.iter().enumerate() {
                let ground = drill.groundings.iter().find(|(at, _)| *at == k);
                match (ground, slot.as_str()) {
                    (Some((_, text)), _) => {
                        let term = self.b.constant(text.clone());
                        let class = crate::pipeline::middle::core::node::rel::constraint_class(&self.b, term, &self.switches);
                        binds.push(Bind {
                            at: BindAt::Position(k as u16),
                            role: BindRole::Constrain { value: term, class },
                        });
                    }
                    (None, "_") => {}
                    (None, written) => {
                        binds.push(Bind {
                            at: BindAt::Position(k as u16),
                            role: BindRole::Publish(Some(Name::new(written))),
                        });
                    }
                }
            }
        }
        let level = Level {
            from: None,
            reach: Reach::Known,
            binds,
        };
        let rel = self.b.expand(value, ExpansionForm::Drill, vec![level], &drill.column)?;
        Ok((rel, name))
    }
}

/// Whether a member's relation is a receipt: an act's receipt, or the
/// positional access that reads one.
fn is_receipt(arena: &impl Arena, rel: RelId) -> bool {
    use crate::pipeline::middle::core::node::{ReadSource, RelKind};
    match arena.rel(rel).kind() {
        RelKind::Receipt { .. } => true,
        RelKind::Read {
            source: ReadSource::Local(body),
            ..
        } => matches!(arena.rel(*body).kind(), RelKind::Receipt { .. }),
        _ => false,
    }
}

/// The naming of a glob's items: an unqualified glob covers its input's
/// positions as the input publishes them; a qualified one, the member's own.
fn glob_naming(qualified: bool) -> Naming {
    if qualified {
        Naming::QualifiedGlob
    } else {
        Naming::Glob
    }
}

/// Whether an argument is a whole-operand `*`.
fn is_star(argument: &ScalarArgument) -> bool {
    match argument {
        ScalarArgument::Star => true,
        ScalarArgument::Spread(Spread::Glob(glob)) => glob.qualifier.is_none() && glob.namespace_path.is_empty(),
        _ => false,
    }
}

/// The number of arguments a call is written with: its arity.
fn argument_count(arguments: &CallArguments) -> usize {
    match arguments {
        CallArguments::None => 0,
        CallArguments::Scalar(args) => args.len(),
        CallArguments::HigherOrder(part) => part.members().len(),
    }
}

fn frame_edge(bound: &FrameBound) -> Result<FrameEdge, Refusal> {
    let number = |e: &DomainExpression| -> Result<i64, Refusal> {
        match e {
            DomainExpression::Application(FunctionApplication::Ground(LiteralValue::Number(n))) => n
                .spelling()
                .parse::<i64>()
                .map_err(|_| refuse::outside("a non-integer frame offset")),
            _ => Err(refuse::outside("a computed frame offset")),
        }
    };
    Ok(match bound {
        FrameBound::Unbounded => FrameEdge::Unbounded,
        FrameBound::CurrentRow => FrameEdge::CurrentRow,
        FrameBound::Preceding(e) => FrameEdge::Preceding(number(e)?),
        FrameBound::Following(e) => FrameEdge::Following(number(e)?),
    })
}
