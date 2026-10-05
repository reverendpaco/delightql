// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A built-in directive wherever a relation stands: its arguments read by
//! role, its source and target elaborated, the act built by the core's
//! effect constructor, and the receipt term that stands in its place. A
//! marked read (`!!`) births its row locator where it is read.

use super::instances::{call_term, Definition};
use super::{Elaborator, Term};
use crate::pipeline::middle::core::graph::Arena;
use crate::pipeline::middle::core::heading::Name;
use crate::pipeline::middle::core::ids::RelId;
use crate::pipeline::middle::core::instance::Actual;
use crate::pipeline::middle::core::node::effect::{
    CreatedSpec, EffectSpec, ReceiptCell, RowMode, Terminal, Verdict, WriteSpec,
};
use crate::pipeline::middle::core::node::rel::{AccessSpec, HeaderSpec, SlotSpec};
use crate::pipeline::middle::core::node::run::{MemberSpec, MergeRequest, OpenRun};
use crate::pipeline::middle::core::node::{Item, Naming, PipeOp, Route, SetOpKind};
use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::select::{BareChoice, Made, Referent};
use crate::pipeline::middle::facade::{
    self, Access, CallArguments, Chain, DefKind, Directive, DirectiveClass, FunctorCall, GroundForm,
    GroundMention, HoArgument, HoParam, LiteralValue, MutationKind, Payload, QualifiedName, QueryLocalDemand,
    QueryLocalKind, Relation, Slot,
};

impl Elaborator<'_, '_> {
    /// A read marked as a mutation's source: a stored table of the
    /// session, read with its row locator.
    pub(super) fn marked_read(
        &mut self,
        identifier: &QualifiedName,
        access: Option<&Access>,
    ) -> Result<RelId, Refusal> {
        let name = &identifier.name;
        let qualifier = (!identifier.namespace_path.is_empty()).then(|| identifier.namespace_path.qualifier());
        if qualifier.is_none() && self.claimed_kind(name).is_some() {
            return Err(refuse::marked_not_table(name.as_str()));
        }
        if qualifier.is_none() && self.formal_relation(name).is_some() {
            return Err(refuse::outside("a marked read of a relation formal"));
        }
        self.not_created(name)?;
        match self.refer(name, qualifier.as_ref())? {
            Some(Referent::Served(served)) if served.kind().is_database_object() => {
                // Only a stored table is a mutation target: the occurrence a
                // marker marks is the one a mutation reaches.
                if !stored_table(served.kind()) {
                    return Err(refuse::marked_not_table(name.as_str()));
                }
                let physical = self.physical(&served)?;
                let columns = self.catalog_columns(&served, &physical)?;
                let identity = self.input.row_identity(&served, physical.connection, physical.schema.as_deref())?;
                let typed = self.typed(&served, &physical);
                let access = self.access_spec(access, columns.len())?;
                self.b.marked_catalog_read(
                    served.name().clone(),
                    served.namespace().to_string(),
                    Some(served.entity_id()),
                    columns,
                    &identity,
                    physical,
                    typed,
                    access,
                    &self.switches,
                )
            }
            Some(Referent::Family(_)) => Err(refuse::marked_not_table(name.as_str())),
            Some(Referent::Served(_) | Referent::Declared(_)) => {
                Err(refuse::outside("a marked read of a relation that is not a stored table"))
            }
            None => Err(refuse::table(&self.mention_spelled(identifier))),
        }
    }

    /// A catalog name the statement's own creations make is not a mutation
    /// target in the same statement.
    pub(super) fn not_created(&self, name: &Name) -> Result<(), Refusal> {
        if self.created.iter().any(|c| c.name == *name) {
            return Err(refuse::outside("a mutation of an object the same statement creates"));
        }
        Ok(())
    }

    /// Record an object the statement creates: its columns are its source's
    /// published names, its storage where the placement puts it. A name
    /// created again after a read of the first object is not covered: the
    /// statement's later steps read objects by name.
    fn creating(&mut self, name: &Name, source: RelId, placement: &facade::Placement) -> Result<(), Refusal> {
        if self.created.iter().any(|c| c.name == *name && c.read) {
            return Err(refuse::outside("a name the statement creates again after reading the object it first created"));
        }
        let columns = self
            .b
            .rel(source)
            .heading()
            .displayed()
            .map(|(_, p)| {
                p.answering_name().map(|n| crate::pipeline::middle::core::node::CatalogColumn {
                    name: n.clone(),
                    declared: None,
                    class: None,
                    computed: Some(false),
                    stored: false,
                })
            })
            .collect();
        let creation = placement.creation();
        self.created.push(super::Created {
            name: name.clone(),
            namespace: placement.created_namespace(),
            owner: placement.namespace().to_string(),
            session: (creation.residence() == facade::ObjectResidence::SessionShadow).then(|| creation.shape()),
            receipt: None,
            columns,
            physical: crate::pipeline::middle::core::node::Physical {
                connection: placement.connection(),
                schema: placement.spelled_schema(),
            },
            read: false,
        });
        Ok(())
    }

    /// The namespace a written qualifier names, for a creation's target and
    /// for a read of what the statement creates alike: the one its route
    /// reaches (an alias's namespace, an exact path), or, where no catalog
    /// namespace answers it, its exact path as written.
    fn routed_namespace(&self, qualifier: &facade::Qualifier) -> String {
        match self.at.route(&self.world, qualifier) {
            Some(reached) => reached.fq,
            None => self.input.qualifier_spelled(qualifier),
        }
    }

    /// A read of an object the statement creates (EVERY OCCURRENCE
    /// EFFECTUATES: it is read after the act, `decide::effect` judging the
    /// order): through a qualifier naming the namespace the creation landed
    /// in, by the one identity a written qualifier is routed to
    /// (`routed_namespace`, as the creation's own target was), or by its
    /// bare name where BARE SELECTION, the statement's creations among its
    /// candidates, selects it (`World::bare_among`); `None` when no creation
    /// is selected and the catalog's judgment stands.
    pub(super) fn created_read(&mut self, identifier: &QualifiedName, access: Option<&Access>) -> Result<Option<RelId>, Refusal> {
        let named: Vec<usize> = (0..self.created.len()).filter(|k| self.created[*k].name == identifier.name).collect();
        if named.is_empty() {
            return Ok(None);
        }
        let at = if identifier.namespace_path.is_empty() {
            let made: Vec<Made<'_>> = named
                .iter()
                .map(|k| Made {
                    owner: &self.created[*k].owner,
                    session: self.created[*k].session.is_some(),
                })
                .collect();
            match self.world.bare_among(&self.at, &identifier.name, &made)? {
                BareChoice::Made(i) => named[i],
                BareChoice::Catalog => return Ok(None),
            }
        } else {
            let routed = self.routed_namespace(&identifier.namespace_path.qualifier());
            match named.iter().rev().find(|k| self.created[**k].namespace == routed) {
                Some(k) => *k,
                None => return Ok(None),
            }
        };
        let Some(receipt) = self.created[at].receipt else {
            return Err(refuse::table(&self.mention_spelled(identifier)));
        };
        let Some(columns) = self.created[at].columns.clone() else {
            return Err(refuse::outside("a read of a created object whose source publishes a position no name answers"));
        };
        self.created[at].read = true;
        let physical = self.created[at].physical.clone();
        let access = self.access_spec(access, columns.len())?;
        Ok(Some(self.b.created_read(receipt, identifier.name.clone(), columns, physical, access, &self.switches)?))
    }

    /// A built-in directive's term: its act built by the core's effect
    /// constructor from the descriptor's facts and the call's arguments,
    /// and the term its receipt stands as, under the receipt access the
    /// author wrote.
    pub(super) fn directive_term(
        &mut self,
        call: &FunctorCall,
        directive: Directive,
        receipt_access: Option<&Access>,
        alias: Option<Name>,
    ) -> Result<Term, Refusal> {
        let arguments = Arguments::of(&call.arguments, directive.formals_before_input)?;
        let operation = directive.operation.clone();
        let (source, write, verdict, prints, echoes, payload, witness) = match &directive.class {
            DirectiveClass::Unclaimed => {
                return Err(refuse::outside(&format!("{operation} in a statement the new middle compiles")))
            }
            DirectiveClass::Execution(kind) => {
                let kind = *kind;
                return self.execution_term(call, &directive, kind, receipt_access, alias);
            }
            DirectiveClass::Retraction => {
                Self::judge_receipt_access(&operation, directive.receipt.len(), receipt_access)?;
                let rel = self.retraction(&directive, &arguments)?;
                return self.receipt_access_term(rel, call, &operation, directive.receipt.len(), receipt_access, alias);
            }
            DirectiveClass::Terminal(kind) => {
                if !arguments.written.is_empty() || !arguments.rules.is_empty() {
                    return Err(refuse::landing_nowhere(&operation));
                }
                let label = match (kind, arguments.values.as_slice()) {
                    (facade::TerminalKind::Exit, []) => None,
                    (facade::TerminalKind::Abort, [value]) => Some(
                        literal_text(value).ok_or_else(|| refuse::outside("an abort label that is not a written string"))?,
                    ),
                    (facade::TerminalKind::Abort, []) => Some(operation.clone()),
                    _ => return Err(refuse::landing_nowhere(&operation)),
                };
                // Piped, it is reached when its input has a row; written in a
                // conjunction with no input of its own, when its step runs,
                // which the members before it gate (ETL MAPPING: a guard
                // before the directive).
                let source = match arguments.landed {
                    Some(chain) => Some(self.chain(chain)?),
                    None => None,
                };
                let terminal = match (kind, label.clone()) {
                    (facade::TerminalKind::Abort, Some(label)) => Terminal::Abort { label },
                    _ => Terminal::Exit,
                };
                (
                    source,
                    WriteSpec::Terminal(terminal),
                    source.map_or(Verdict::Reached, Verdict::Any),
                    false,
                    label.into_iter().collect(),
                    None,
                    None,
                )
            }
            DirectiveClass::Session(facts) => {
                Self::judge_receipt_access(&operation, directive.receipt.len(), receipt_access)?;
                let rel = self.session_act(&directive, facts, &arguments)?;
                return self.receipt_access_term(rel, call, &operation, directive.receipt.len(), receipt_access, alias);
            }
            DirectiveClass::Rows(kind) => {
                let kind = *kind;
                if !arguments.rules.is_empty() {
                    return Err(refuse::dml_rule_value());
                }
                let [target] = arguments.written.as_slice() else {
                    return Err(refuse::dml_target_count(arguments.written.len()));
                };
                if !arguments.values.is_empty() {
                    return Err(refuse::outside("a mutation argument that is not a relation"));
                }
                let source_chain = arguments.landed.ok_or_else(|| refuse::dml_source_count(0))?;
                let source = self.chain(source_chain)?;
                let mode = match kind {
                    MutationKind::Insert => RowMode::Insert,
                    MutationKind::Update => RowMode::Replace,
                    MutationKind::Delete => RowMode::Remove,
                };
                let target_rel = self.target(target, &operation)?;
                let echo = designator(target).map(|id| self.mention_spelled(id)).unwrap_or_default();
                (Some(source), WriteSpec::Rows { target: target_rel, mode }, Verdict::Affected, false, vec![echo], None, None)
            }
            DirectiveClass::Creation(creation) => {
                let creation = *creation;
                if !arguments.rules.is_empty() {
                    return Err(refuse::creation_designator(&operation));
                }
                let [target] = arguments.written.as_slice() else {
                    return Err(refuse::creation_designator(&operation));
                };
                if !arguments.values.is_empty() {
                    return Err(refuse::creation_designator(&operation));
                }
                let identifier = creation_designator(target).ok_or_else(|| refuse::creation_designator(&operation))?;
                // A qualified target selects its one namespace by the route
                // its qualifier takes; the namespace's backing is judged by
                // the placement, which never falls back to `main`.
                let namespace =
                    (!identifier.namespace_path.is_empty()).then(|| self.routed_namespace(&identifier.namespace_path.qualifier()));
                let source_chain = arguments.landed.ok_or_else(|| refuse::outside("a creation with no piped source"))?;
                let source = self.chain(source_chain)?;
                let name = identifier.name.clone();
                let mut placement = self.input.placement(namespace.as_deref(), name.as_str(), creation, &operation)?;
                // The statement's own earlier session objects hold their
                // temp names by the time this creation runs (NAME CLASH).
                if creation.residence() == facade::ObjectResidence::SessionShadow {
                    for earlier in &self.created {
                        if let Some(shape) = earlier.session.filter(|_| {
                            earlier.name == name && earlier.physical.connection == placement.connection()
                        }) {
                            placement.hold(facade::Holder {
                                shape,
                                owner: Some(earlier.owner.clone()),
                            });
                        }
                    }
                }
                self.creating(&name, source, &placement)?;
                let durable_exists = creation.residence() == facade::ObjectResidence::Durable && {
                    let owner = facade::Qualifier::exact(placement.namespace().split("::").map(str::to_string).collect());
                    self.refer(&name, Some(&owner))?.is_some()
                };
                let echoes = vec![placement.target_path(), placement.created_path()];
                let mut past_session = Vec::new();
                if creation.residence() == facade::ObjectResidence::SessionShadow {
                    // Every read of the durable namesake the statement holds so
                    // far, its source's included, is spelled past the pool.
                    let built: Vec<RelId> = self.b.built().collect();
                    let shadowed = crate::pipeline::middle::core::decide::effect::shadowed_reads(
                        &self.b,
                        built,
                        &name,
                        placement.connection(),
                    );
                    for (read, namespace) in shadowed {
                        // A query-local family's body is one read, demanded
                        // wherever the family is: before the act it reads
                        // the durable rows, after it the session object.
                        if self.block_bodies.iter().any(|body| body.contains(&read.index())) {
                            return Err(refuse::outside(
                                "a query-local definition, resolved where its block opens, reading a name a session \
                                 object the statement creates will shadow (whether each demand of it falls before or \
                                 after the creation is not followed)",
                            ));
                        }
                        let schema = self.input.past_session_schema(&namespace, placement.connection())?.ok_or_else(|| {
                            refuse::outside(
                                "a read the creation's own session object shadows, on a target with no schema \
                                 spelling past its session pool",
                            )
                        })?;
                        past_session.push((read, schema));
                    }
                }
                let created = CreatedSpec {
                    placement,
                    durable_exists,
                    past_session,
                };
                (Some(source), WriteSpec::Object(created), Verdict::Reached, false, echoes, None, None)
            }
            DirectiveClass::Utility => {
                let source_chain = arguments
                    .landed
                    .ok_or_else(|| refuse::outside("a utility directive with no piped input"))?;
                match directive.payload {
                    Payload::Input => {
                        if !arguments.written.is_empty() || !arguments.values.is_empty() || !arguments.rules.is_empty() {
                            return Err(refuse::landing_nowhere(&operation));
                        }
                        let source = self.chain(source_chain)?;
                        let prints = directive.side_effects;
                        (Some(source), WriteSpec::None, Verdict::Reached, prints, Vec::new(), Some(source), None)
                    }
                    Payload::Other => {
                        if !arguments.rules.is_empty() {
                            return Err(refuse::landing_nowhere(&operation));
                        }
                        let [other] = arguments.written.as_slice() else {
                            return Err(refuse::landing_nowhere(&operation));
                        };
                        let source = self.chain(source_chain)?;
                        let other = self.chain(other)?;
                        (Some(source), WriteSpec::None, Verdict::Reached, false, Vec::new(), Some(other), None)
                    }
                    Payload::Assertion => {
                        let properties: Vec<&Chain> = arguments.written.iter().chain(&arguments.rules).copied().collect();
                        // The property position takes a rule value: a value
                        // standing there names no rule.
                        if properties.is_empty() && !arguments.values.is_empty() {
                            return Err(refuse::rule_value_form("property", &operation));
                        }
                        let [property] = properties.as_slice() else {
                            return Err(refuse::outside("an assertion without exactly one property"));
                        };
                        let label = match arguments.values.as_slice() {
                            [] => None,
                            [value] => Some(
                                literal_text(value)
                                    .ok_or_else(|| refuse::outside("an assertion label that is not a written string"))?,
                            ),
                            _ => return Err(refuse::outside("an assertion with more than one label")),
                        };
                        let source = self.chain(source_chain)?;
                        let witness = self.property(property, source)?;
                        let verdict = Verdict::Witness {
                            witness,
                            label: label.clone(),
                        };
                        (Some(source), WriteSpec::None, verdict, false, label.into_iter().collect(), Some(source), Some(witness))
                    }
                    Payload::None => {
                        return Err(refuse::outside(&format!("{operation} in a statement the new middle compiles")))
                    }
                }
            }
        };
        let mut echoes = echoes.into_iter();
        let mut receipt = Vec::with_capacity(directive.receipt.len());
        for (i, (name, interior)) in directive.receipt.iter().enumerate() {
            let cell = match (i, *interior, name.as_str()) {
                (0, false, _) => ReceiptCell::Const(self.b.constant(LiteralValue::integer(1))),
                (1, false, _) => ReceiptCell::Const(self.b.constant(LiteralValue::String(operation.clone()))),
                (_, true, "witnesses") => ReceiptCell::Interior(
                    witness.ok_or_else(|| refuse::elaboration_contract("a witnesses column with no witness"))?,
                ),
                (_, true, _) => ReceiptCell::Interior(
                    payload.ok_or_else(|| refuse::elaboration_contract("an interior column with no payload"))?,
                ),
                (_, false, _) => ReceiptCell::Const(match echoes.next() {
                    Some(echo) => self.b.constant(LiteralValue::String(echo)),
                    None => self.b.constant(LiteralValue::Null),
                }),
            };
            receipt.push((name.clone(), cell));
        }
        let width = receipt.len();
        let creates = matches!(write, WriteSpec::Object(_));
        let rel = self.b.effect(EffectSpec {
            operation: operation.clone(),
            source,
            write,
            verdict,
            prints,
            inputs: Vec::new(),
            receipt,
        })?;
        if creates {
            if let Some(created) = self.created.last_mut() {
                created.receipt = Some(rel);
            }
        }
        self.receipt_access_term(rel, call, &operation, width, receipt_access, alias)
    }

    /// A session act's receipt access, judged before its arguments: a lone
    /// group is the receipt's, whatever the author meant by it.
    fn judge_receipt_access(operation: &str, width: usize, receipt_access: Option<&Access>) -> Result<(), Refusal> {
        if let Some(Access::Slots(slots)) = receipt_access {
            if slots.len() != width || slots.iter().any(|s| !matches!(s, Slot::Bind(_))) {
                return Err(refuse::receipt_access(operation, width));
            }
        } else if !matches!(receipt_access, None | Some(Access::All)) {
            return Err(refuse::receipt_access(operation, width));
        }
        Ok(())
    }

    /// RETRACTING A SESSION DEFINITION: the designator is selected at the
    /// statement's site exactly as a read of it would be, so a name the
    /// statement's own block declares is the selected identity and refuses
    /// as no session definition, and a served identity refuses likewise.
    /// The receipt names the selected family and what a later bare mention
    /// of its name selects with it gone (NULL when the block claims the name
    /// or nothing, or several, answer). The act removes the family it is
    /// told, and refuses what the world forbids: a definition the session
    /// did not author, one a grounding borrows, one another depends on.
    fn retraction(&mut self, directive: &Directive, arguments: &Arguments<'_>) -> Result<RelId, Refusal> {
        let operation = &directive.operation;
        if self.nested > 0 {
            return Err(refuse::session_position(operation));
        }
        let identifier = match (arguments.written.as_slice(), arguments.values.is_empty(), arguments.rules.is_empty(), arguments.landed) {
            ([target], true, true, None) => designator(target),
            _ => None,
        }
        .ok_or_else(refuse::retract_designator)?;
        let name = &identifier.name;
        let qualifier = (!identifier.namespace_path.is_empty()).then(|| identifier.namespace_path.qualifier());
        let spelled = self.mention_spelled(identifier);
        if qualifier.is_none() {
            if self.formal_relation(name).is_some() {
                return Err(refuse::retract_claimed(&spelled, "relation formal", name.as_str()));
            }
            if let Some(kind) = self.claimed_kind(name) {
                return Err(refuse::retract_claimed(&spelled, kind.description(), name.as_str()));
            }
        }
        let family = match self.refer(name, qualifier.as_ref())? {
            None => return Err(refuse::retract_missing(&spelled)),
            Some(Referent::Declared(declared)) => {
                return Err(refuse::retract_served(
                    &spelled,
                    &format!("{}.{}", declared.namespace(), declared.name()),
                    "a declared imprint object",
                ))
            }
            Some(Referent::Served(served)) => {
                return Err(refuse::retract_served(
                    &spelled,
                    &format!("{}.{}", served.namespace(), served.name()),
                    served.kind().variant_name(),
                ))
            }
            Some(Referent::Family(family)) => family,
        };
        // What a later bare mention of the name selects: the same judgment,
        // in the world as the act leaves it.
        let revealed = match self.claimed_kind(name) {
            Some(_) => None,
            None => match self.world.without(family.entity_id()).judge(&self.at, name, None)?.referent() {
                Ok(Some(found)) => Some(format!("{}.{}", found.namespace(), found.name())),
                Ok(None) | Err(_) => None,
            },
        };
        let told = [
            family.entity_id().to_string(),
            family.namespace().to_string(),
            family.name().as_str().to_string(),
        ];
        let values: Vec<_> = told.iter().map(|v| self.b.constant(LiteralValue::String(v.clone()))).collect();
        let header = ["entity_id", "namespace", "entity"].into_iter().map(|n| HeaderSpec::Bind(Name::new(n))).collect();
        let args = self.b.lit(header, vec![values], false, &self.switches)?;
        let echoes = [Some(family.name().as_str().to_string()), Some(family.namespace().to_string()), revealed];
        let mut receipt = Vec::with_capacity(directive.receipt.len());
        let mut echoes = echoes.into_iter();
        for (i, (column, _)) in directive.receipt.iter().enumerate() {
            let cell = match i {
                0 => self.b.constant(LiteralValue::integer(1)),
                1 => self.b.constant(LiteralValue::String(operation.clone())),
                _ => match echoes.next().flatten() {
                    Some(text) => self.b.constant(LiteralValue::String(text)),
                    None => self.b.constant(LiteralValue::Null),
                },
            };
            receipt.push((column.clone(), ReceiptCell::Const(cell)));
        }
        self.b.effect(EffectSpec {
            operation: operation.clone(),
            source: Some(args),
            write: WriteSpec::Session {
                directive: operation.trim_end_matches('!').to_string(),
                report: None,
            },
            verdict: Verdict::Reached,
            prints: false,
            inputs: Vec::new(),
            receipt,
        })
    }

    /// A built-in directive's receipt as the term standing where its call is
    /// written, under the receipt access the author wrote.
    fn receipt_access_term(
        &mut self,
        rel: RelId,
        call: &FunctorCall,
        operation: &str,
        width: usize,
        receipt_access: Option<&Access>,
        alias: Option<Name>,
    ) -> Result<Term, Refusal> {
        let rel = match receipt_access {
            None | Some(Access::All) => rel,
            Some(Access::Slots(slots)) => {
                let mut specs = Vec::with_capacity(slots.len());
                for slot in slots.iter() {
                    match slot {
                        Slot::Bind(binder) => specs.push(SlotSpec::Bind(binder.name.clone())),
                        Slot::Anon | Slot::Reuse(_) | Slot::Constraint(_) => {
                            return Err(refuse::receipt_access(operation, width))
                        }
                    }
                }
                if specs.len() != width {
                    return Err(refuse::receipt_access(operation, width));
                }
                self.b.local_read(rel, AccessSpec::Slots(specs), &self.switches)?
            }
            Some(_) => return Err(refuse::receipt_access(operation, width)),
        };
        let name = self.input.callee_name(&call.callee).0;
        Ok(call_term(rel, call, receipt_access, alias, name, Route::Plain, MergeRequest::None))
    }

    /// A session directive's act and receipt. Its arguments are one
    /// relation matched to its declared parameters by position: the values
    /// written in its argument group as one row, or a relation standing in
    /// it, written or piped, whose rows the act is applied to set-at-a-time
    /// (THE LIFT IS ORTHOGONAL TO SPELLING). The receipt is the law's: the
    /// core, the flat echoes of written values, the `input` echo of the
    /// argument rows and, for a consultation, the namespaces its arguments
    /// name.
    fn session_act(
        &mut self,
        directive: &Directive,
        facts: &facade::SessionFacts,
        arguments: &Arguments<'_>,
    ) -> Result<RelId, Refusal> {
        let operation = &directive.operation;
        if facts.liminal_only {
            return Err(refuse::liminal_only(operation));
        }
        // `doc!` annotates and never changes what is compiled, so an effect
        // body may demand it (THE SESSION'S SHAPE IS TEXT, NOT DATA).
        if self.nested > 0 && !facts.by_demand {
            return Err(refuse::session_position(operation));
        }
        if facts.returned == facade::SessionPayload::Discovered {
            return Err(refuse::outside(&format!(
                "{operation}: its `returned` payload holds what the act finds when it runs, which the receipt of a \
                 compiled statement does not carry"
            )));
        }
        if !arguments.rules.is_empty() {
            return Err(refuse::outside(&format!("{operation} with a rule value among its arguments")));
        }
        let declared = facts.params.len();
        let required = facts.params.iter().filter(|p| !p.optional).count();
        let lifted: Option<&Chain> = match (arguments.landed, arguments.written.as_slice(), arguments.values.is_empty()) {
            (Some(relation), [], true) => Some(relation),
            (None, [relation], true) => Some(*relation),
            (None, [], _) => None,
            _ => return Err(refuse::outside(&format!("{operation} with a relation beside written values"))),
        };
        let param_names: Vec<Name> = facts.params.iter().map(|p| p.name.clone()).collect();
        let (args, written) = match lifted {
            Some(relation) => {
                let rel = self.chain(relation)?;
                let width = self.displayed_width(rel);
                if width < required || width > declared {
                    if !crate::pipeline::middle::core::decide::effect::acts(&self.b, rel).is_empty() {
                        return Err(refuse::receipt_shape(operation, width, declared));
                    }
                    return Err(refuse::session_arity(operation, width, required, declared));
                }
                (rel, None)
            }
            None => {
                let count = arguments.values.len();
                if count < required || count > declared {
                    return Err(refuse::session_arity(operation, count, required, declared));
                }
                let mut values = Vec::with_capacity(count);
                for value in &arguments.values {
                    let e = self.value(value, crate::pipeline::middle::core::decide::grade::CallPosition::Value)?;
                    if !crate::pipeline::middle::core::graph::Arena::expr(&self.b, e).fv().is_empty() {
                        return Err(refuse::outside(&format!("{operation} with an argument read from a relation's rows")));
                    }
                    values.push(e);
                }
                let header = param_names[..count].iter().cloned().map(HeaderSpec::Bind).collect();
                let rel = self.b.lit(header, vec![values.clone()], false, &self.switches)?;
                (rel, Some(values))
            }
        };
        let width = self.displayed_width(args);
        let mut echoes = written.clone().unwrap_or_default().into_iter();
        // The relation the act reports into: its declared heading, standing
        // for the rows the act hands back when it runs.
        let report = match &facts.returned {
            facade::SessionPayload::Reported(columns) => {
                let header = columns.iter().cloned().map(HeaderSpec::Bind).collect();
                let row = columns.iter().map(|_| self.b.constant(LiteralValue::Null)).collect();
                Some(self.b.lit(header, vec![row], false, &self.switches)?)
            }
            _ => None,
        };
        let mut receipt = Vec::with_capacity(directive.receipt.len());
        for (i, (name, interior)) in directive.receipt.iter().enumerate() {
            let cell = match (i, *interior, name.as_str()) {
                (0, false, _) => ReceiptCell::Const(self.b.constant(LiteralValue::integer(1))),
                (1, false, _) => ReceiptCell::Const(self.b.constant(LiteralValue::String(operation.clone()))),
                (_, true, "input") => {
                    let columns: Vec<(Option<usize>, Name)> = facts
                        .input_echo
                        .iter()
                        .enumerate()
                        .map(|(k, n)| ((k < width).then_some(k), n.clone()))
                        .collect();
                    ReceiptCell::Interior(self.reshaped(args, &columns)?)
                }
                (_, true, _) if report.is_some() => ReceiptCell::Interior(report.expect("a report")),
                (_, true, _) => {
                    let columns: Vec<(Option<usize>, Name)> = facts
                        .params
                        .iter()
                        .enumerate()
                        .filter(|(_, p)| p.namespace)
                        .map(|(k, _)| ((k < width).then_some(k), Name::new("namespace")))
                        .collect();
                    ReceiptCell::Interior(self.reshaped(args, &columns)?)
                }
                (_, false, _) if written.is_none() => {
                    return Err(refuse::outside(&format!(
                        "a relation lifted into {operation}, whose receipt echoes its arguments as constants"
                    )))
                }
                (_, false, _) => ReceiptCell::Const(match echoes.next() {
                    Some(value) => value,
                    None => self.b.constant(LiteralValue::Null),
                }),
            };
            receipt.push((name.clone(), cell));
        }
        self.b.effect(EffectSpec {
            operation: operation.clone(),
            source: Some(args),
            write: WriteSpec::Session {
                directive: operation.trim_end_matches('!').to_string(),
                report,
            },
            verdict: Verdict::Reached,
            prints: false,
            inputs: Vec::new(),
            receipt,
        })
    }

    /// A relation's displayed positions taken by index, each under a new
    /// name; `None` stands a NULL in its place.
    fn reshaped(&mut self, rel: RelId, columns: &[(Option<usize>, Name)]) -> Result<RelId, Refusal> {
        let mut run = OpenRun::new();
        let binder = run.push_member(
            &mut self.b,
            rel,
            MemberSpec {
                marked: false,
                completes_marked_lead: false,
                route: Route::Plain,
                scope: None,
                names_scope: false,
                requalifies: false,
                born: crate::pipeline::middle::core::node::run::Born::Written,
                merge: MergeRequest::None,
            },
            &self.switches,
        )?;
        let input = run.close(&mut self.b)?;
        let positions: Vec<usize> = crate::pipeline::middle::core::graph::Arena::rel(&self.b, rel)
            .heading()
            .displayed()
            .map(|(i, _)| i)
            .collect();
        let mut items = Vec::with_capacity(columns.len());
        for (at, name) in columns {
            let expr = match at {
                Some(k) => self.b.col(binder, positions[*k] as u16),
                None => self.b.constant(LiteralValue::Null),
            };
            items.push(Item {
                expr,
                naming: Naming::As(name.clone()),
            });
        }
        self.b.pipe(input, PipeOp::Project(items))
    }

    /// An assertion's witness: the property, closed by the ordinary rule
    /// value judgment over its configured values, applied to the checked
    /// input as its one remaining relation formal.
    fn property(&mut self, property: &Chain, input: RelId) -> Result<RelId, Refusal> {
        if property.has_steps() {
            return Err(refuse::outside("an assertion property with continuations"));
        }
        self.assertion_property(property, input)
    }

    /// The target: a whole-table designator naming a stored table of the
    /// session outside the engine's own namespaces.
    fn target(&mut self, target: &Chain, verb: &str) -> Result<RelId, Refusal> {
        let identifier = designator(target).ok_or_else(|| refuse::target_designator(verb))?;
        let name = &identifier.name;
        let qualifier = (!identifier.namespace_path.is_empty()).then(|| identifier.namespace_path.qualifier());
        // A target names a stored table: a name the statement's query-local
        // blocks claim is a relation the statement computes, not a table.
        if qualifier.is_none() && (self.formal_relation(name).is_some() || self.claimed_kind(name).is_some()) {
            return Err(refuse::dml_target_local(verb, name.as_str()));
        }
        self.not_created(name)?;
        let found = self.refer(name, qualifier.as_ref())?;
        // A session object is the session's data, writable like any data
        // table; the engine's own namespaces are not, whatever they hold.
        let owner = match &found {
            Some(Referent::Served(served)) if !session_object(served.kind()) => Some(served.namespace().to_string()),
            Some(Referent::Served(_) | Referent::Declared(_)) | None => None,
            Some(Referent::Family(family)) => Some(family.namespace().to_string()),
        };
        if let Some(owner) = owner {
            if self.input.engine_owned(&owner)? {
                return Err(refuse::engine_owned(name.as_str(), &owner));
            }
        }
        match found {
            Some(Referent::Served(served)) if stored_table(served.kind()) => {
                let physical = self.physical(&served)?;
                let columns = self.catalog_columns(&served, &physical)?;
                self.b.catalog_read(
                    served.name().clone(),
                    served.namespace().to_string(),
                    Some(served.entity_id()),
                    columns,
                    physical,
                    true,
                    AccessSpec::All,
                    &self.switches,
                )
            }
            Some(Referent::Served(_) | Referent::Family(_) | Referent::Declared(_)) => {
                Err(refuse::dml_target_not_table(verb, name.as_str()))
            }
            None => Err(refuse::table(&self.mention_spelled(identifier))),
        }
    }
}

/// Whether a served entity is a stored table: the kinds whose rows a row
/// locator reaches. Whether its declared types are enforced is the serving
/// backend's answer (`Elaborator::typed`), not its kind: the catalog gives a
/// virtual table the kind of a table.
pub(super) fn stored_table(kind: facade::EntityType) -> bool {
    use facade::EntityType as K;
    matches!(
        kind,
        K::DbPermanentTable | K::DbTemporaryTable | K::DqlPermanentTableExpression | K::DqlTemporaryTableExpression
    )
}

/// Whether a served entity is a session object: a temp table in the
/// connection's temp schema.
fn session_object(kind: facade::EntityType) -> bool {
    use facade::EntityType as K;
    matches!(kind, K::DbTemporaryTable | K::DqlTemporaryTableExpression)
}

/// A call's arguments by role: the relations written in its argument
/// group, the relation a pipe landed, and the values written.
struct Arguments<'c> {
    written: Vec<&'c Chain>,
    landed: Option<&'c Chain>,
    values: Vec<&'c facade::DomainExpression>,
    rules: Vec<&'c Chain>,
}

impl<'c> Arguments<'c> {
    /// The arguments by role. PIPE SUBSTITUTION: piped and direct are one
    /// call, so a call written with one relation argument beyond the
    /// relational formals before its input, standing last, is the call with
    /// that relation piped in.
    fn of(arguments: &'c CallArguments, formals_before_input: Option<usize>) -> Result<Self, Refusal> {
        let mut out = Arguments {
            written: Vec::new(),
            landed: None,
            values: Vec::new(),
            rules: Vec::new(),
        };
        let CallArguments::HigherOrder(part) = arguments else {
            return Ok(out);
        };
        for member in part.members().iter() {
            match member {
                HoArgument::Relation(chain) => out.written.push(chain),
                HoArgument::Landed(chain) => out.landed = Some(chain),
                HoArgument::Value(value) => out.values.push(&value.value),
                HoArgument::Rule(chain) => out.rules.push(chain),
                HoArgument::Landing(_) | HoArgument::Skip => {
                    return Err(refuse::outside("a landing mark or skipped position in a directive's arguments"))
                }
            }
        }
        let last_is_relation = matches!(part.members().iter().last(), Some(HoArgument::Relation(_)));
        if out.landed.is_none()
            && last_is_relation
            && formals_before_input.is_some_and(|n| out.written.len() + out.rules.len() == n + 1)
        {
            out.landed = out.written.pop();
        }
        Ok(out)
    }
}

/// A written string literal's text.
fn literal_text(value: &facade::DomainExpression) -> Option<String> {
    match value {
        facade::DomainExpression::Application(facade::FunctionApplication::Ground(LiteralValue::String(s))) => {
            Some(s.clone())
        }
        _ => None,
    }
}

/// A whole-table designator's name: `name(*)`, optionally qualified.
fn designator(chain: &Chain) -> Option<&QualifiedName> {
    if chain.has_steps() {
        return None;
    }
    match (chain.head().form(), chain.head_access()) {
        (
            GroundForm::Reference(Relation::Ground {
                mention:
                    GroundMention::Named {
                        identifier,
                        mutation_target: false,
                        ..
                    },
            }),
            None | Some(Access::All),
        ) => Some(identifier),
        _ => None,
    }
}

/// A creation target's name: `name()` (inchoate: it has no columns yet) or
/// `name(*)`, optionally qualified.
fn creation_designator(chain: &Chain) -> Option<&QualifiedName> {
    if chain.has_steps() {
        return None;
    }
    match (chain.head().form(), chain.head_access()) {
        (
            GroundForm::Reference(Relation::Ground {
                mention:
                    GroundMention::Named {
                        identifier,
                        mutation_target: false,
                        alias: None,
                        ..
                    },
            }),
            None | Some(Access::All | Access::Unasked),
        ) => Some(identifier),
        _ => None,
    }
}

impl Elaborator<'_, '_> {
    /// Every query-local effect rule a statement declares, nested
    /// declarations included, judged where it is declared.
    pub(super) fn declared_effect_rules(&self, query: &facade::Query) -> Result<(), Refusal> {
        judge_declared_query_locals(query)
    }

    /// A user directive's term: an effect CTE's expression, or an effect
    /// rule's invocation, each elaborated anew at this demand (EVERY
    /// OCCURRENCE EFFECTUATES). A query-local declaration answers first; a
    /// consulted rule answers otherwise.
    pub(super) fn user_directive_term(
        &mut self,
        call: &FunctorCall,
        access: Option<&Access>,
        alias: Option<Name>,
    ) -> Result<Term, Refusal> {
        let (written_name, qualifier) = self.input.callee_name(&call.callee);
        let written = written_name.as_str().to_string();
        let operation = written.clone();
        let bare = self.input.callee_bare(&call.callee);
        if qualifier.is_none() {
            if let Some((block, kind)) = self.claim(&bare, QueryLocalDemand::Effect)? {
                let rel = match kind {
                    QueryLocalKind::EffectRelation => self.effect_label(block, &bare, call)?,
                    QueryLocalKind::EffectHigherOrder => {
                        let definition = self.local_effect_rule(block, &bare)?;
                        self.invoke_effect(definition, &call.arguments, &operation)?
                    }
                    QueryLocalKind::Relation
                    | QueryLocalKind::Value
                    | QueryLocalKind::HigherOrder
                    | QueryLocalKind::Sigma => return Err(refuse::outside("a demand of a pure query-local definition")),
                };
                return self.receipt_term(rel, call, &operation, access, alias);
            }
        }
        let definition = match self.refer(&written_name, qualifier.as_ref())? {
            Some(Referent::Family(family)) => self.catalog_definition(&family, &[DefKind::Effect])?,
            Some(Referent::Served(_) | Referent::Declared(_)) => return Err(refuse::outside("an engine-served directive")),
            None => {
                let spelled = match &qualifier {
                    Some(q) => format!("{}.{written}", self.input.qualifier_spelled(q)),
                    None => written.clone(),
                };
                return Err(refuse::directive_unknown(&spelled));
            }
        };
        let rel = self.invoke_effect(definition, &call.arguments, &operation)?;
        self.receipt_term(rel, call, &operation, access, alias)
    }

    /// A receipt as a term, under the receipt access the author wrote.
    fn receipt_term(
        &mut self,
        rel: RelId,
        call: &FunctorCall,
        operation: &str,
        access: Option<&Access>,
        alias: Option<Name>,
    ) -> Result<Term, Refusal> {
        let width = self.displayed_width(rel);
        let rel = match access {
            None | Some(Access::All) => rel,
            Some(Access::Slots(slots)) => {
                let mut specs = Vec::with_capacity(slots.len());
                for slot in slots.iter() {
                    match slot {
                        Slot::Bind(binder) => specs.push(SlotSpec::Bind(binder.name.clone())),
                        Slot::Anon | Slot::Reuse(_) | Slot::Constraint(_) => {
                            return Err(refuse::receipt_access(operation, width))
                        }
                    }
                }
                if specs.len() != width {
                    return Err(refuse::arity(specs.len(), width));
                }
                self.b.local_read(rel, AccessSpec::Slots(specs), &self.switches)?
            }
            Some(_) => return Err(refuse::receipt_access(operation, width)),
        };
        Ok(call_term(rel, call, access, alias, Name::new(operation), Route::Plain, MergeRequest::None))
    }

    /// An effect rule's invocation (THE USER RULE RECEIPT): its clauses
    /// elaborated with the formals bound to the call's actuals, each ending
    /// in a receipt; their union is the ledger; the invocation's receipt
    /// holds one row when the ledger has one, and carries the ledger.
    fn invoke_effect(&mut self, definition: Definition, arguments: &CallArguments, operation: &str) -> Result<RelId, Refusal> {
        let (ledger, inputs) = self.invocation(definition, arguments, operation)?;
        self.invocation_receipt(operation, Vec::new(), ledger, inputs)
    }

    /// An invocation's receipt: the core, the flat echoes, and the ledger it
    /// carries as `returned`; one row when the ledger has one.
    fn invocation_receipt(
        &mut self,
        operation: &str,
        echoes: Vec<(Name, LiteralValue)>,
        ledger: RelId,
        inputs: Vec<RelId>,
    ) -> Result<RelId, Refusal> {
        let mut receipt = vec![
            (Name::new("success"), ReceiptCell::Const(self.b.constant(LiteralValue::integer(1)))),
            (Name::new("operation"), ReceiptCell::Const(self.b.constant(LiteralValue::String(operation.to_string())))),
        ];
        for (name, value) in echoes {
            receipt.push((name, ReceiptCell::Const(self.b.constant(value))));
        }
        receipt.push((Name::new("returned"), ReceiptCell::Interior(ledger)));
        self.b.effect(EffectSpec {
            operation: operation.to_string(),
            source: None,
            write: WriteSpec::None,
            verdict: Verdict::Any(ledger),
            prints: false,
            inputs,
            receipt,
        })
    }

    /// An effect rule's clauses elaborated with its formals bound to the
    /// call's actuals, each ending in a receipt: their union is the ledger.
    /// Answers the ledger and the relations the formals are bound to.
    fn invocation(
        &mut self,
        mut definition: Definition,
        arguments: &CallArguments,
        operation: &str,
    ) -> Result<(RelId, Vec<RelId>), Refusal> {
        // AN EFFECT RULE HAS NO GROUND MEMBER: judged on the family's
        // complete signature before any clause is read.
        if definition
            .clauses
            .iter()
            .any(|c| c.params().iter().any(|p| matches!(p, HoParam::Ground { .. })))
        {
            return Err(refuse::effect_ground_member(operation));
        }
        // NO RECURSION: a rule reached again while it is being invoked.
        if self.building.iter().any(|b| b.definition == definition.key) {
            return Err(refuse::effect_recursion(operation));
        }
        let formals: Vec<Option<HoParam>> = definition
            .clauses
            .first()
            .map(|c| c.params().iter().cloned().map(Some).collect())
            .unwrap_or_default();
        if let Some(clause) = definition.clauses.first() {
            super::instances::landing(operation, clause.params(), arguments)?;
        }
        let actuals = self.actuals(operation, arguments, &formals)?;
        for actual in &actuals {
            // A landed relation is the chain's own operand; an authored
            // relation argument is an enclosed position.
            if let Actual::Relation { rel, landed: false } = actual {
                crate::pipeline::middle::core::decide::effect::fence(&self.b, *rel, "a higher-order argument")?;
            }
        }
        self.building.push(super::Building {
            definition: definition.key.clone(),
            display: operation.to_string(),
            local: definition.world.is_none(),
            form: super::Form::Relation,
            actuals: actuals.clone(),
            stands_in: false,
            frontier: None,
        });
        let clauses = definition.clauses.clone();
        let mut bodies = Vec::with_capacity(clauses.len());
        let mut result = Ok(());
        for clause in &clauses {
            match self.instance_clause(&mut definition, clause, &actuals) {
                Ok((None, body)) => bodies.push(body),
                Ok((Some(_), _)) => {
                    result = Err(refuse::effect_ground_member(operation));
                    break;
                }
                Err(e) => {
                    result = Err(e);
                    break;
                }
            }
        }
        self.building.pop();
        result?;
        let mut bodies = bodies.into_iter();
        let mut ledger = bodies.next().ok_or_else(|| refuse::outside("an effect rule with no clause"))?;
        for body in bodies {
            ledger = self.b.set_op(ledger, body, SetOpKind::Corresponding, None, crate::pipeline::middle::core::decide::setop::Gate::Closed)?;
        }
        let inputs = actuals
            .iter()
            .filter_map(|a| match a {
                Actual::Relation { rel, .. } => Some(*rel),
                Actual::Value(_) | Actual::Rule(_) => None,
            })
            .collect();
        Ok((ledger, inputs))
    }

    /// An execution directive (FILES AND THE RUN): the demand of a
    /// consulted namespace's `main!` with the arguments written after the
    /// namespace (or the file, whose consultation ran before the statement
    /// was compiled). At the prompt it is the whole statement; in an effect
    /// rule's body `run_namespace!` composes like any demand. Glob access
    /// releases the payload, the demanded `main!`'s receipt; the exact-arity
    /// binding list reads the run's receipt, which holds one row exactly
    /// when `main!`'s does.
    fn execution_term(
        &mut self,
        call: &FunctorCall,
        directive: &Directive,
        kind: facade::ExecutionKind,
        receipt_access: Option<&Access>,
        alias: Option<Name>,
    ) -> Result<Term, Refusal> {
        let operation = directive.operation.clone();
        if self.nested == 0 && !self.whole_run {
            return Err(refuse::run_position(&operation));
        }
        let CallArguments::HigherOrder(part) = &call.arguments else {
            return Err(refuse::session_arity(&operation, 0, 1, 1));
        };
        let mut members: Vec<HoArgument> = part.members().iter().cloned().collect();
        let first = members.remove(0);
        // A bare name stands for the namespace it spells, as a string does.
        let written = match &first {
            HoArgument::Value(value) => literal_text(&value.value).or_else(|| facade::bare_name(&value.value)),
            _ => None,
        }
        .ok_or_else(|| refuse::outside(&format!("{operation} whose first argument is not a written string")))?;
        let (echo, namespace) = match kind {
            facade::ExecutionKind::File => ("path", facade::run_namespace_of(&written)),
            facade::ExecutionKind::Namespace => ("namespace", written.clone()),
        };
        let qualifier = facade::Qualifier::exact(namespace.split("::").map(str::to_string).collect());
        let definition = match self.refer(&Name::new("main!"), Some(&qualifier))? {
            Some(Referent::Family(family)) => self.catalog_definition(&family, &[DefKind::Effect])?,
            _ => return Err(refuse::no_main(&namespace)),
        };
        let arguments = facade::call_arguments(members);
        // The run's payload is the demanded `main!`'s result: its receipt,
        // whose own payload is its ledger.
        let main = self.invoke_effect(definition, &arguments, "main!")?;
        let rel = match receipt_access {
            None | Some(Access::All) => main,
            Some(_) => {
                let receipt = self.invocation_receipt(
                    &operation,
                    vec![(Name::new(echo), LiteralValue::String(written))],
                    main,
                    Vec::new(),
                )?;
                let width = self.displayed_width(receipt);
                let Some(Access::Slots(slots)) = receipt_access else {
                    return Err(refuse::run_receipt_access(&operation, echo));
                };
                let mut specs = Vec::with_capacity(slots.len());
                for slot in slots.iter() {
                    match slot {
                        Slot::Bind(binder) => specs.push(SlotSpec::Bind(binder.name.clone())),
                        Slot::Anon | Slot::Reuse(_) | Slot::Constraint(_) => {
                            return Err(refuse::run_receipt_access(&operation, echo))
                        }
                    }
                }
                if specs.len() != width {
                    return Err(refuse::run_receipt_access(&operation, echo));
                }
                self.b.local_read(receipt, AccessSpec::Slots(specs), &self.switches)?
            }
        };
        let name = self.input.callee_name(&call.callee).0;
        Ok(call_term(rel, call, receipt_access, alias, name, Route::Plain, MergeRequest::None))
    }

    /// An effect CTE's expression, elaborated anew at this demand in its
    /// block's declaration environment.
    fn effect_label(&mut self, block: usize, name: &Name, call: &FunctorCall) -> Result<RelId, Refusal> {
        if !matches!(call.arguments, CallArguments::None) {
            return Err(refuse::outside("an effect CTE demanded with arguments"));
        }
        let key = self.local_key(block, name);
        if self.building.iter().any(|b| b.definition == key) {
            return Err(refuse::effect_recursion(&format!("{name}!")));
        }
        let bindings = self.pending_effect_family(block, name)?;
        self.building.push(super::Building {
            definition: key,
            display: format!("{name}!"),
            local: true,
            form: super::Form::Relation,
            actuals: Vec::new(),
            stands_in: false,
            frontier: None,
        });
        // CLAUSES ARE ARMS: each clause executes, in definition order, and
        // their rows accumulate as a rule's clauses do.
        let mut bodies = Vec::with_capacity(bindings.len());
        let mut result = Ok(());
        for binding in &bindings {
            match self.clause(block, binding) {
                Ok(body) => bodies.push(body),
                Err(e) => {
                    result = Err(e);
                    break;
                }
            }
        }
        self.building.pop();
        result?;
        let mut bodies = bodies.into_iter();
        let mut label = bodies.next().ok_or_else(|| refuse::elaboration_contract("an effect CTE with no clause"))?;
        for body in bodies {
            label = self.b.set_op(label, body, SetOpKind::Corresponding, None, crate::pipeline::middle::core::decide::setop::Gate::Closed)?;
        }
        Ok(label)
    }
}

/// How a written clause body ends, by THE ENDING LAW.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Ending {
    /// In a directive's receipt: the head is a directive call followed by
    /// nothing but its whole receipt access, or every arm of the union it
    /// ends in does.
    Receipt,
    /// In something other than a receipt.
    Other,
    /// In a signed witness over the receipt (`(+-)`): a ledger row, not a
    /// receipt (THE RECEIPT CORE).
    Witnessed,
    /// In a form whose ending the owner has not ruled: a positional receipt
    /// access that renames the receipt (`(s, o, t)`).
    Unruled(&'static str),
}

/// THE ENDING LAW, the new middle's one statement of it, judged on the
/// written body where the rule is declared. A witness over the receipt
/// (`(+)`, `(\+)`) is a structural step after it, so the body does not end
/// in the receipt.
pub(super) fn ending(chain: &Chain) -> Ending {
    let mut ends = if !matches!(
        chain.head().form(),
        GroundForm::Reference(Relation::FunctorCall { call, .. }) if call.is_effect()
    ) {
        Ending::Other
    } else if matches!(chain.head_access(), Some(Access::Slots(_))) {
        Ending::Unruled("whether a clause ending in a positional receipt access (`(s, o, t)`) ends in its receipt")
    } else {
        Ending::Receipt
    };
    for step in chain.steps() {
        let next = match step.form() {
            facade::Continuation::Access {
                access: Access::Slots(_), ..
            } => Ending::Unruled(
                "whether a clause ending in a positional receipt access (`(s, o, t)`) ends in its receipt",
            ),
            facade::Continuation::Access { .. } => Ending::Receipt,
            facade::Continuation::Structural(step) if matches!(step.form, facade::StructuralForm::SignedWitness) => {
                Ending::Witnessed
            }
            facade::Continuation::BagOp { arm, .. } => ending(arm),
            // A conjunction ends in its rightmost conjunct: a guard before a
            // directive leaves the clause ending in the directive.
            facade::Continuation::Member { rhs, .. } => {
                ends = ending(rhs);
                continue;
            }
            _ => Ending::Other,
        };
        ends = match (ends, next) {
            (Ending::Other, _) | (_, Ending::Other) => Ending::Other,
            (Ending::Unruled(q), _) | (_, Ending::Unruled(q)) => Ending::Unruled(q),
            (Ending::Witnessed, _) | (_, Ending::Witnessed) => Ending::Witnessed,
            (Ending::Receipt, Ending::Receipt) => Ending::Receipt,
        };
    }
    ends
}

/// THE ENDING LAW applied to one clause body of `rule`.
pub(super) fn judge_ending(rule: &str, chain: &Chain) -> Result<(), Refusal> {
    match ending(chain) {
        Ending::Receipt => Ok(()),
        Ending::Other => Err(refuse::effect_ending(rule)),
        Ending::Witnessed => Err(refuse::effect_ending_witnessed(rule)),
        Ending::Unruled(question) => Err(refuse::unruled(question)),
    }
}

/// THE BODY LAWS ARE JUDGED WHERE THE RULE IS DECLARED, the new middle's one
/// statement of them for both necks: a consulted family when it is
/// consulted, a query-local family at the statement that declares it,
/// whether or not anything demands it. An effect family's clauses end in a
/// directive and demand no session directive and no `run!`; it does not
/// demand itself; `main!` has one clause. Every query-local effect family a
/// clause body declares is judged the same way.
pub(crate) fn judge_declared_family(group: &facade::DefinitionGroup, effect: bool) -> Result<(), Refusal> {
    use facade::BodyDirective;
    let clauses = group.clauses();
    super::instances::judge_signatures(&group.name(), clauses.iter().map(|c| &c.head))?;
    if effect {
        let rule = format!("{}!", group.name().trim_end_matches('!'));
        let mut demands_itself = false;
        for clause in clauses {
            let facade::DdlBody::Relational(body) = &clause.body else {
                continue;
            };
            judge_ending(&rule, &body.body)?;
            super::integer::judge_read_once(body)?;
            // A demand of the body's own binding demands the binding, not a
            // rule of that name.
            let labels: Vec<String> = body
                .ctes()
                .iter()
                .filter_map(|c| match c.subject() {
                    facade::CteSubjectView::Authored { name, .. } | facade::CteSubjectView::Generated { name } => {
                        Some(format!("{name}!"))
                    }
                    facade::CteSubjectView::Frontier => None,
                })
                .collect();
            for demand in facade::body_demands(body)? {
                match demand.built_in {
                    Some(BodyDirective::Run) => return Err(refuse::effect_run_in_body(&rule)),
                    Some(BodyDirective::Session) => return Err(refuse::effect_session_directive(&rule, &demand.name)),
                    Some(BodyDirective::Doc | BodyDirective::Other) | None => {}
                }
                demands_itself |= !demand.qualified && demand.name == rule && !labels.contains(&demand.name);
            }
        }
        if demands_itself {
            return Err(refuse::effect_recursion(&rule));
        }
        if rule == "main!" && clauses.len() > 1 {
            return Err(refuse::effect_main_multi_clause(clauses.len()));
        }
    }
    for clause in clauses {
        if let facade::DdlBody::Relational(body) = &clause.body {
            judge_declared_query_locals(body)?;
        }
    }
    Ok(())
}

/// Every query-local effect family a query's block declares, nested
/// declarations included, by THE BODY LAWS.
pub(crate) fn judge_declared_query_locals(query: &facade::Query) -> Result<(), Refusal> {
    for ho in query.hos() {
        judge_declared_family(ho.group(), ho.declares_effect())?;
    }
    for sigma in query.sigmas() {
        judge_declared_family(sigma.group(), false)?;
    }
    for cfe in query.cfes() {
        super::instances::judge_signatures(cfe.name.as_str(), &cfe.clause_signatures.0)?;
    }
    Ok(())
}
