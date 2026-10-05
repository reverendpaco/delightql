// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Definition instances. An application's actuals are elaborated once, in
//! the caller; the definition's clauses are then elaborated in the
//! definition's own declaration environment with each formal bound to its
//! actual, before the application node exists (W5 #3, #5). Every
//! application is its own instance. The one place instance identity
//! decides anything is a self-reference inside the definition being built,
//! judged by the semantic identity of its actuals (recursion-contract-law,
//! MONOMORPHIC PARAMETERS).

use super::target::TargetCallee;
use super::{Building, Elaborator, Form, Frame, Term};
use crate::pipeline::middle::core::decide::grade::CallPosition;
use crate::pipeline::middle::core::heading::form::{self, Declared, Formed};
use crate::pipeline::middle::core::heading::Name;
use crate::pipeline::middle::core::ids::{ExprId, RelId, TruthId};
use crate::pipeline::middle::core::instance::{Actual, RuleValue};
use crate::pipeline::middle::core::node::rel::{AccessSpec, ClauseSpec};
use crate::pipeline::middle::core::node::run::MergeRequest;
use crate::pipeline::middle::core::node::{Consumer, Route, SigmaProof};
use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::select::Referent;
use crate::pipeline::middle::facade::{
    self, CallArguments, Clause, CmpOp, DdlBody, DefKind, DomainExpression, HoArgument,
    HoParam, LexicalHorizon, LiteralValue, Qualifier, QueryLocalDemand, QueryLocalKind,
};

/// A selected definition: its clauses and the declaration environment its
/// heads and bodies resolve in.
#[derive(Clone)]
pub(super) struct Definition {
    pub(super) key: String,
    display: String,
    /// The family's declared name, as a teaching names the family.
    name: String,
    pub(super) clauses: Vec<Clause>,
    /// Query-local blocks the body sees, and (for a query-local
    /// definition) the horizon it reads its declaring block at.
    blocks: usize,
    /// Instance frames the body sees: those open where it was declared.
    frames: usize,
    horizon: Option<LexicalHorizon>,
    /// Index of the catalog declaration world in `Elaborator::worlds`.
    pub(super) world: Option<usize>,
    /// Per clause, a fact clause's rows' heading offers (none for a stacked
    /// fact, whose header names its positions); `None` for a rule clause.
    facts: Vec<Option<facade::RowOffers>>,
    /// The danger settings a catalog definition's stored source declares
    /// beside its clauses; `None` for a definition of the statement's own
    /// text, whose settings are the statement's.
    declared: Option<facade::Settings>,
    /// The instance frames a closed rule value's spend reopens in place of
    /// the first `frames` open ones (an index of `Elaborator::closures`).
    captured: Option<usize>,
}

/// A definition's declaration environment, as a value definition's call
/// reads it.
pub(super) struct Parts {
    pub(super) key: String,
    pub(super) display: String,
    pub(super) blocks: usize,
    pub(super) frames: usize,
    pub(super) horizon: Option<LexicalHorizon>,
    pub(super) world: Option<usize>,
}

impl Definition {
    /// The declaration environment alone: no clause is read through it.
    pub(super) fn environment(
        key: String,
        display: String,
        blocks: usize,
        frames: usize,
        horizon: Option<LexicalHorizon>,
        world: Option<usize>,
    ) -> Self {
        Definition {
            name: display.clone(),
            key,
            display,
            clauses: Vec::new(),
            blocks,
            frames,
            horizon,
            world,
            facts: Vec::new(),
            declared: None,
            captured: None,
        }
    }

    pub(super) fn parts(&self) -> Parts {
        Parts {
            key: self.key.clone(),
            display: self.display.clone(),
            blocks: self.blocks,
            frames: self.frames,
            horizon: self.horizon,
            world: self.world,
        }
    }
}

/// The term a call stands as, for an application and a directive's receipt
/// alike. A call written in place is reached through its alias, else its
/// callee's name; a piped call is a pipe form, whose callee's name is no
/// scope and whose positions stay bare binders beside an authored alias. An
/// inchoate access names no scope, and an unaliased positional one only
/// binds its slots.
pub(super) fn call_term(
    rel: RelId,
    call: &facade::FunctorCall,
    access: Option<&facade::Access>,
    alias: Option<Name>,
    name: Name,
    route: Route,
    merge: MergeRequest,
) -> Term {
    let names_scope = match access {
        Some(facade::Access::Slots(_)) => alias.is_some(),
        Some(facade::Access::Unasked) => false,
        None | Some(facade::Access::All | facade::Access::Dequalify(_) | facade::Access::DequalifyAll) => true,
    };
    let piped = match &call.arguments {
        CallArguments::HigherOrder(part) => part.members().iter().any(|a| matches!(a, HoArgument::Landed(_))),
        CallArguments::None | CallArguments::Scalar(_) => false,
    };
    let scope = match (piped, alias) {
        (true, alias) => alias,
        (false, alias) => Some(alias.unwrap_or(name)),
    };
    let names_scope = names_scope && scope.is_some();
    // A landed call no `as` names is a pipe stage's unnamed output: what the
    // deictic `_` reaches (lvars-law `_` IS DEIXIS).
    let born = if piped && scope.is_none() {
        crate::pipeline::middle::core::node::run::Born::UnnamedStage
    } else {
        crate::pipeline::middle::core::node::run::Born::Written
    };
    Term {
        rel,
        scope,
        route,
        merge,
        names_scope,
        requalifies: !piped,
        born,
    }
}

impl Elaborator<'_, '_> {
    /// A relational application term: `f(actuals)(access)`.
    pub(super) fn application_term(
        &mut self,
        call: &facade::FunctorCall,
        access: Option<&facade::Access>,
        merge: MergeRequest,
        alias: Option<Name>,
    ) -> Result<Term, Refusal> {
        let (name, qualifier) = self.input.callee_name(&call.callee);
        // An engine route `main/f(…)` names the engine's own table function,
        // and `sys::target.f(…)` the target provider's, past any DQL identity
        // of the name (THE TARGET SURFACE IS OPEN, step 4).
        if let Some(q) = &qualifier {
            if self.input.callee_engine_routed(&call.callee) {
                return self.function_term(call, access, merge, alias, TargetCallee::Engine(q.clone()));
            }
            if matches!(self.judge(&name, Some(q))?, crate::pipeline::middle::select::Referred::Provider) {
                return self.function_term(call, access, merge, alias, TargetCallee::Provider);
            }
        }
        let (definition, configured, spent) = match self.callee(&name, qualifier.as_ref()) {
            // An unqualified name no DQL entity answers, applied to values
            // alone and not piped, is the target engine's table function.
            Err(error)
                if qualifier.is_none()
                    && error.error_uri().contains("semantic/resolution/callable_unknown")
                    && values_only(&call.arguments) =>
            {
                return self.function_term(call, access, merge, alias, TargetCallee::Open(error));
            }
            other => other?,
        };
        if configured.is_empty() {
            if let Some(clause) = definition.clauses.first() {
                landing(name.as_str(), clause.params(), &call.arguments)?;
            }
        }
        // The formal each supplied actual binds: in order, the positions a
        // configured rule leaves open, or every position.
        let formals: Vec<Option<HoParam>> = match definition.clauses.first() {
            Some(clause) if configured.is_empty() => clause.params().iter().cloned().map(Some).collect(),
            Some(clause) => clause
                .params()
                .iter()
                .zip(&configured)
                .filter(|(_, c)| c.is_none())
                .map(|(p, _)| Some(p.clone()))
                .collect(),
            None => Vec::new(),
        };
        let (mut supplied, knowable) = self.written_actuals(name.as_str(), &call.arguments, &formals)?;
        if let Some(capture) = spent.as_ref().and_then(|rule| rule.capture) {
            self.completed_over(&configured, capture, &mut supplied)?;
        }
        for actual in &supplied {
            // A landed relation is the chain's own operand; an authored
            // relation argument is an enclosed position.
            if let Actual::Relation { rel, landed: false } = actual {
                crate::pipeline::middle::core::decide::effect::fence(&self.b, *rel, "a higher-order argument")?;
            }
        }
        // A configured value is written where its designator stands, never
        // among this call's arguments.
        let (actuals, written) = if configured.is_empty() {
            (supplied, knowable)
        } else {
            let mut supplied = supplied.into_iter().zip(knowable);
            let mut out = Vec::with_capacity(configured.len());
            let mut written = Vec::with_capacity(configured.len());
            for c in configured {
                let (actual, knowable) = match c {
                    Some(a) => (a, false),
                    None => supplied
                        .next()
                        .ok_or_else(|| refuse::arity(out.len(), out.len() + 1))?,
                };
                out.push(actual);
                written.push(knowable);
            }
            if supplied.next().is_some() {
                return Err(refuse::outside("a spend with more actuals than its rule leaves open"));
            }
            (out, written)
        };
        // A closed rule value's spend is a new instance of its definition,
        // whatever is being built around it.
        let rel = match spent {
            Some(rule) if !rule.within_itself => self.new_instance(definition, actuals, &written, access)?,
            Some(_) | None => self.instantiate(definition, actuals, &written, access)?,
        };
        Ok(call_term(rel, call, access, alias, name, Route::Named, merge))
    }

    /// The relation a rule value's spend completes it over, as the core
    /// judges it against the rows its configured values were born over
    /// (`decide::rule::completion`): rows carrying every configured value
    /// complete with their own; a distinct relation joins the value's
    /// capture, so each of its rows meets every construction row's value.
    fn completed_over(&mut self, configured: &[Option<Actual>], capture: RelId, supplied: &mut [Actual]) -> Result<(), Refusal> {
        use crate::pipeline::middle::core::decide::rule::{completion, Completion};
        let passengers: Vec<crate::pipeline::middle::core::ids::PassengerId> = configured
            .iter()
            .filter_map(|c| match c {
                Some(Actual::Value(e)) => match crate::pipeline::middle::core::graph::Arena::expr(&self.b, *e).kind() {
                    crate::pipeline::middle::core::node::ExprKind::Passenger(p) => Some(*p),
                    _ => None,
                },
                Some(Actual::Relation { .. } | Actual::Rule(_)) | None => None,
            })
            .collect();
        if passengers.is_empty() {
            return Ok(());
        }
        let relations: Vec<usize> = supplied
            .iter()
            .enumerate()
            .filter(|(_, a)| matches!(a, Actual::Relation { .. }))
            .map(|(i, _)| i)
            .collect();
        let [at] = relations.as_slice() else {
            return Err(refuse::outside("a rule value configured from construction rows completed over several relations"));
        };
        let Actual::Relation { rel, landed } = supplied[*at] else {
            return Err(refuse::elaboration_contract("a relation actual that is none"));
        };
        if completion(&self.b, rel, &passengers, capture)? == Completion::Distinct {
            supplied[*at] = Actual::Relation {
                rel: self.joined_with_capture(rel, capture)?,
                landed,
            };
        }
        Ok(())
    }

    /// A distinct completion relation joined with a rule value's capture: its
    /// own rows each with every construction row's configured values,
    /// which ride hidden; it publishes only its own positions.
    fn joined_with_capture(&mut self, supplied: RelId, capture: RelId) -> Result<RelId, Refusal> {
        use crate::pipeline::middle::core::node::run::{MemberSpec, OpenRun};
        let spec = || MemberSpec {
            marked: false,
            completes_marked_lead: false,
            route: Route::Plain,
            scope: None,
            names_scope: false,
            requalifies: false,
            born: crate::pipeline::middle::core::node::run::Born::Written,
            merge: MergeRequest::None,
        };
        let mut over_capture = OpenRun::new();
        over_capture.push_member(&mut self.b, capture, spec(), &self.switches)?;
        let rows = over_capture.close(&mut self.b)?;
        let values = self.b.pipe(rows, crate::pipeline::middle::core::node::PipeOp::Project(Vec::new()))?;
        let mut joined = OpenRun::new();
        joined.push_member(&mut self.b, supplied, spec(), &self.switches)?;
        joined.push_member(&mut self.b, values, spec(), &self.switches)?;
        joined.close(&mut self.b)
    }

    /// A query-local effect rule (an effect mirror) of block `i`.
    pub(super) fn local_effect_rule(&mut self, i: usize, name: &Name) -> Result<Definition, Refusal> {
        let ho = self.blocks[i]
            .hos
            .iter()
            .find(|h| h.name() == name && h.declares_effect())
            .ok_or_else(|| refuse::elaboration_contract("an effect mirror its block does not declare"))?
            .clone();
        Ok(self.local_definition(i, &ho, format!("{name}!")))
    }

    /// A parameterized definition block `i` declares, in that block's
    /// declaration environment.
    pub(super) fn local_definition(&self, i: usize, ho: &facade::HoDefinition, display: String) -> Definition {
        Definition {
            key: self.local_key(i, ho.name()),
            display,
            name: ho.name().to_string(),
            clauses: ho.group().clauses().to_vec(),
            blocks: i + 1,
            frames: self.blocks[i].frames,
            horizon: Some(*ho.horizon()),
            world: None,
            facts: Vec::new(),
            declared: None,
            captured: None,
        }
    }

    /// The definition a relational callee names: a query-local
    /// parameterized definition, else the catalog's.
    pub(super) fn relational_definition(
        &mut self,
        name: &Name,
        qualifier: Option<&Qualifier>,
    ) -> Result<Definition, Refusal> {
        if let Some(local) = self.local_relational_definition(name, qualifier)? {
            return Ok(local);
        }
        let referent = self.refer(name, qualifier)?;
        self.catalog_relational_definition(name, referent)
    }

    /// The query-local parameterized definition an unqualified relational
    /// callee names, if its block claims the name.
    pub(super) fn local_relational_definition(
        &mut self,
        name: &Name,
        qualifier: Option<&Qualifier>,
    ) -> Result<Option<Definition>, Refusal> {
        if qualifier.is_none() {
            if let Some((i, kind)) = self.claim(name, QueryLocalDemand::HigherOrder)? {
                if kind != QueryLocalKind::HigherOrder {
                    return Err(refuse::outside("a query-local definition of another kind"));
                }
                let ho = self.blocks[i]
                    .hos
                    .iter()
                    .find(|h| h.name() == name)
                    .ok_or_else(|| refuse::outside("an effect mirror"))?
                    .clone();
                return Ok(Some(self.local_definition(i, &ho, name.to_string())));
            }
        }
        Ok(None)
    }

    /// The catalog definition a relational callee's referent is.
    pub(super) fn catalog_relational_definition(
        &mut self,
        name: &Name,
        referent: Option<Referent>,
    ) -> Result<Definition, Refusal> {
        match referent {
            Some(Referent::Family(family)) => self.catalog_definition(&family, &[DefKind::View, DefKind::HoView]),
            // A stored relation the name selects is no parameterized
            // relation: the call refuses, and the engine is never asked to
            // call a table.
            Some(Referent::Served(served)) if served.kind().is_database_object() => {
                Err(refuse::callable_of_another_kind(name.as_str(), "stored relation"))
            }
            Some(Referent::Served(_) | Referent::Declared(_)) => Err(refuse::outside("an engine-served callee")),
            None => Err(refuse::callable_unknown(name.as_str())),
        }
    }

    /// A catalog definition family, read from its stored source. Its clauses
    /// must declare one of `kinds`.
    pub(super) fn catalog_definition(
        &mut self,
        family: &crate::pipeline::middle::select::Family,
        kinds: &[DefKind],
    ) -> Result<Definition, Refusal> {
        let (declared, settings) = self.input.family_source(family)?;
        // A family none of whose clauses is of a kind the position takes is
        // the wrong kind for it: the name selected it, so the position
        // refuses. A relation read (the only position taking a fact) of a
        // value function has no relation face; every other position is a
        // call.
        if !declared.is_empty() && declared.iter().all(|(kind, _, _)| !kinds.contains(kind)) {
            let kind = facade::kind_name(declared[0].0);
            return Err(match kinds.contains(&DefKind::Fact) {
                true if matches!(declared[0].0, DefKind::Function) => refuse::function_read_as_relation(family.name().as_str(), kind),
                true => refuse::outside("a catalog definition of this kind in this position"),
                false if kinds.contains(&DefKind::Effect) || kinds.contains(&DefKind::Sigma) => {
                    refuse::outside("a catalog definition of this kind in this position")
                }
                false => refuse::callable_of_another_kind(family.name().as_str(), kind),
            });
        }
        if declared.iter().any(|(kind, _, _)| !kinds.contains(kind)) {
            return Err(refuse::outside("a catalog definition of this kind in this position"));
        }
        let site = self.world.body_of(family)?;
        let namespace = family.namespace().to_string();
        self.sites.push(site);
        let world = self.sites.len() - 1;
        Ok(Definition {
            key: format!("C{namespace}.{}", family.name()),
            display: format!("{namespace}.{}", family.name()),
            name: family.name().to_string(),
            facts: declared.iter().map(|(kind, _, offers)| (*kind == DefKind::Fact).then(|| offers.clone())).collect(),
            clauses: declared.into_iter().map(|(_, clause, _)| clause).collect(),
            blocks: 0,
            frames: 0,
            horizon: None,
            world: Some(world),
            declared: Some(settings),
            captured: None,
        })
    }

    /// What a relational callee names: a rule formal's configured value
    /// (its definition and configured prefix), else a definition.
    #[allow(clippy::type_complexity)]
    fn callee(
        &mut self,
        name: &Name,
        qualifier: Option<&Qualifier>,
    ) -> Result<(Definition, Vec<Option<Actual>>, Option<RuleValue>), Refusal> {
        if qualifier.is_none() {
            if let Some(rule) = self.held_rule(name) {
                let mut definition = self
                    .designated
                    .iter()
                    .find(|d| d.key == rule.definition)
                    .cloned()
                    .ok_or_else(|| refuse::outside("a rule value with no designated definition"))?;
                definition.captured = rule.closure;
                return Ok((definition, rule.configured.clone(), Some(rule)));
            }
        }
        Ok((self.relational_definition(name, qualifier)?, Vec::new(), None))
    }

    /// The actuals of a call, elaborated in the caller. The formal an actual
    /// binds decides what the actual is (FN.11: judged at build against the
    /// callee's descriptor, never by its spelling): a rule formal's actual is
    /// a designator; a relation formal's is a closed relation value, which an
    /// argumentative access is not; ground values at a relation formal
    /// declared with a column pattern lift into one row of it. A rule
    /// designator's configured values resolve against the construction rows:
    /// the relation landed at the call, which then carries each configured
    /// value as a passenger born over its row (FN.48, W5 #13).
    pub(super) fn actuals(
        &mut self,
        entity: &str,
        arguments: &CallArguments,
        formals: &[Option<HoParam>],
    ) -> Result<Vec<Actual>, Refusal> {
        Ok(self.written_actuals(entity, arguments, formals)?.0)
    }

    /// The actuals of a call, and by position whether each is a literal or
    /// mention written among the call's arguments: the knowable ground
    /// arguments of A PROVABLE MISS IS AN ERROR (grounding-and-mention-law).
    pub(super) fn written_actuals(
        &mut self,
        entity: &str,
        arguments: &CallArguments,
        formals: &[Option<HoParam>],
    ) -> Result<(Vec<Actual>, Vec<bool>), Refusal> {
        let part = match arguments {
            CallArguments::None => return Ok((Vec::new(), Vec::new())),
            CallArguments::HigherOrder(part) => part,
            CallArguments::Scalar(_) => {
                return Err(refuse::outside("a scalar argument row on a relation"))
            }
        };
        let written: Vec<&HoArgument> = part.members().iter().collect();
        let members = lifted(entity, &written, formals)?;
        let mut knowable: Vec<bool> = members
            .iter()
            .map(|m| matches!(m, Supplied::Written(HoArgument::Value(v)) if is_ground_literal(&v.value)))
            .collect();
        let mut landed: Option<RelId> = None;
        for member in &members {
            if let Supplied::Written(HoArgument::Landed(chain)) = member {
                if landed.is_some() {
                    return Err(refuse::outside("two landed relations"));
                }
                landed = Some(self.chain(chain)?);
            }
        }
        let formal = |i: usize| formals.get(i).and_then(|f| f.as_ref());
        let mut designated: Vec<bool> = Vec::with_capacity(members.len());
        for (i, member) in members.iter().enumerate() {
            designated.push(match (member, formal(i)) {
                // A name with one argument group at a relation formal is an
                // argumentative access, never a closed relation value.
                (Supplied::Written(HoArgument::Rule(_)), Some(HoParam::Relation { name, .. })) => {
                    return Err(refuse::relation_actual_form(name.as_str(), entity))
                }
                (Supplied::Written(HoArgument::Rule(_)), _) => true,
                (Supplied::Written(HoArgument::Relation(_)), Some(HoParam::Rule { .. })) => true,
                (Supplied::Written(HoArgument::Relation(chain)), _) if formals.is_empty() => self.designates(chain)?,
                (
                    Supplied::Lifted(_)
                    | Supplied::Written(
                        HoArgument::Value(_)
                        | HoArgument::Relation(_)
                        | HoArgument::Landed(_)
                        | HoArgument::Landing(_)
                        | HoArgument::Skip,
                    ),
                    _,
                ) => false,
            });
        }
        let mut designators: Vec<Designator<'_>> = Vec::new();
        for (i, (member, designator)) in members.iter().zip(&designated).enumerate() {
            match (designator, member) {
                (true, Supplied::Written(HoArgument::Rule(chain))) => designators.push(Designator {
                    chain,
                    formal: formal(i),
                    applied: false,
                }),
                (true, Supplied::Written(HoArgument::Relation(chain))) => designators.push(Designator {
                    chain,
                    formal: formal(i),
                    applied: true,
                }),
                _ => {}
            }
        }
        // The relations the call's other arguments supply, elaborated before
        // any designator's configured values are read: a configured value
        // naming one of their columns reads a sibling argument.
        let mut written_relations: Vec<Option<RelId>> = Vec::with_capacity(members.len());
        for (i, (member, designator)) in members.iter().zip(&designated).enumerate() {
            written_relations.push(match (designator, member) {
                (false, Supplied::Written(HoArgument::Relation(chain))) => {
                    let rel = self.chain(chain)?;
                    // An anonymous table written as the argument (the lift's
                    // rows included) is the call's own literal, unless it
                    // lifts into a scalar formal as the one value it holds.
                    if matches!(chain.head().form(), facade::GroundForm::Literal(_))
                        && !chain.has_steps()
                        && self.lifted_cell(rel, formal(i)).is_none()
                    {
                        self.call_literals.push(rel);
                    }
                    Some(rel)
                }
                _ => None,
            });
        }
        let siblings = Siblings {
            entity,
            relations: written_relations.iter().flatten().copied().collect(),
        };
        // A SCALAR ACTUAL KEEPS ITS OCCURRENCE (top-grammar.md): a bare name
        // the caller's rows do not answer and the landed relation publishes
        // is that relation's column, read over its row and carried with it.
        let column_bound: Vec<Option<&facade::DomainExpression>> = members
            .iter()
            .enumerate()
            .map(|(i, member)| match (member, formal(i)) {
                (Supplied::Written(HoArgument::Value(value)), None | Some(HoParam::Scalar { .. } | HoParam::Ground { .. }))
                    if is_bare_name(&value.value)
                        && landed.is_some_and(|r| {
                            publishes(crate::pipeline::middle::core::graph::Arena::rel(&self.b, r).heading(), &value.value)
                        })
                        && !bare_name(&value.value).is_some_and(|n| self.publishes(&n)) =>
                {
                    Some(&value.value)
                }
                _ => None,
            })
            .collect();
        let mut bound_values: Vec<ExprId> = Vec::new();
        let mut rules: Vec<RuleValue> = Vec::new();
        if !designators.is_empty() || column_bound.iter().any(Option::is_some) {
            match landed {
                // The construction rows are the relation landed at the call.
                Some(construction) => {
                    let bound: Vec<&facade::DomainExpression> = column_bound.iter().flatten().copied().collect();
                    let ((values, bound_now), carried) =
                        self.construction(construction, &designators, &bound, &siblings)?;
                    rules = values;
                    bound_values = bound_now;
                    landed = Some(carried);
                }
                // No relation stands at the call: each configured value is
                // read where the designator stands.
                None => {
                    let (values, passengers) = self.designate(&designators, None, &siblings)?;
                    if !passengers.is_empty() {
                        return Err(refuse::elaboration_contract("a passenger born with no construction row"));
                    }
                    rules = values;
                }
            }
        }
        let mut rules = rules.into_iter();
        let mut bound_values = bound_values.into_iter();
        let mut out = Vec::new();
        for (i, ((member, designator), written_relation)) in members.into_iter().zip(designated).zip(written_relations).enumerate() {
            if column_bound[i].is_some() {
                out.push(Actual::Value(
                    bound_values
                        .next()
                        .ok_or_else(|| refuse::elaboration_contract("a column-bound actual lost in construction"))?,
                ));
                continue;
            }
            if let (Supplied::Written(HoArgument::Value(_)), Some(HoParam::Rule { name, .. })) = (&member, formal(i)) {
                // A rule formal takes a designator; a value names no rule.
                return Err(refuse::rule_value_form(name.as_str(), entity));
            }
            if designator {
                out.push(Actual::Rule(
                    rules
                        .next()
                        .ok_or_else(|| refuse::elaboration_contract("a rule designator lost in construction"))?,
                ));
                continue;
            }
            let member = match member {
                Supplied::Lifted(cells) => {
                    out.push(Actual::Relation {
                        rel: self.lifted_row(&cells)?,
                        landed: false,
                    });
                    continue;
                }
                Supplied::Written(member) => member,
            };
            out.push(match member {
                HoArgument::Value(value) => match self.value(&value.value, CallPosition::Value) {
                    Ok(e) => Actual::Value(e),
                    // A bare name the landed relation publishes binds the
                    // formal to that column, row by row: not covered.
                    Err(error)
                        if is_bare_name(&value.value)
                            && error.error_uri().contains("semantic/resolution/column")
                            && landed.is_some_and(|r| publishes(crate::pipeline::middle::core::graph::Arena::rel(&self.b, r).heading(), &value.value)) =>
                    {
                        return Err(refuse::outside(
                            "a scalar actual naming a column of the landed relation (a column-bound formal)",
                        ));
                    }
                    // A bare name no row answers is no caller value: the
                    // parameter it would fill is left without input.
                    Err(error)
                        if is_bare_name(&value.value)
                            && error.error_uri().contains("semantic/resolution/column") =>
                    {
                        let parameter = formal(i)
                            .map(|p| p.name().to_string())
                            .unwrap_or_else(|| format!("{}", i + 1));
                        return Err(refuse::incomplete_application(&format!(
                            "parameter '{parameter}' of '{entity}' has no exact caller value — every member of a \
                             parameter row is required input before the body opens"
                        )));
                    }
                    Err(error) => return Err(error),
                },
                HoArgument::Relation(_) => {
                    let rel =
                        written_relation.ok_or_else(|| refuse::elaboration_contract("a written relation not elaborated"))?;
                    match self.lifted_cell(rel, formal(i)) {
                        Some(cell) => {
                            knowable[i] = matches!(
                                crate::pipeline::middle::core::graph::Arena::expr(&self.b, cell).kind(),
                                crate::pipeline::middle::core::node::ExprKind::Const(_)
                            );
                            Actual::Value(cell)
                        }
                        None => Actual::Relation { rel, landed: false },
                    }
                }
                HoArgument::Landed(_) => {
                    Actual::Relation {
                        rel: landed.ok_or_else(|| refuse::elaboration_contract("a lost landing"))?,
                        landed: true,
                    }
                }
                HoArgument::Rule(_) | HoArgument::Landing(_) | HoArgument::Skip => {
                    return Err(refuse::outside("a landing mark or skipped position"))
                }
            });
        }
        Ok((out, knowable))
    }

    /// THE LIFT AT A SCALAR FORMAL (FN.9, FN.11): an anonymous relation of
    /// one row and one column written where the callee declares a scalar is
    /// the one value it holds. A relation of any other degree there is not
    /// covered.
    fn lifted_cell(&self, rel: RelId, formal: Option<&HoParam>) -> Option<ExprId> {
        if !matches!(formal, Some(HoParam::Scalar { .. } | HoParam::Ground { .. })) {
            return None;
        }
        match crate::pipeline::middle::core::graph::Arena::rel(&self.b, rel).kind() {
            crate::pipeline::middle::core::node::RelKind::Lit { rows, .. } => match rows.as_slice() {
                [row] => match row.as_slice() {
                    [cell] => Some(*cell),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        }
    }

    /// A lifted row: the values of one headerless row, elaborated in the
    /// caller, as a one-row anonymous relation (FN.9: the lift spells bare
    /// rows inline; the formal's declared pattern then names its columns).
    fn lifted_row(&mut self, cells: &[&DomainExpression]) -> Result<RelId, Refusal> {
        let mut row = Vec::with_capacity(cells.len());
        for cell in cells {
            row.push(self.value(cell, CallPosition::Value)?);
        }
        let header = row.iter().map(|_| crate::pipeline::middle::core::node::rel::HeaderSpec::Anon).collect();
        let rel = self.b.lit(header, vec![row], false, &self.switches)?;
        self.call_literals.push(rel);
        Ok(rel)
    }

    /// Whether a relation-shaped actual of a callee whose formals are not
    /// known designates a parameterized definition: a bare mention, with at
    /// most a positional pattern, of a name the query-local blocks claim as
    /// a parameterized definition. Its pattern's slots are the configured
    /// values.
    fn designates(&mut self, chain: &facade::Chain) -> Result<bool, Refusal> {
        let facade::GroundForm::Reference(facade::Relation::Ground {
            mention: facade::GroundMention::Named { identifier, .. },
        }) = chain.head().form()
        else {
            return Ok(false);
        };
        if !identifier.namespace_path.is_empty() || chain.has_steps() {
            return Ok(false);
        }
        Ok(self.claimed_kind(&identifier.name) == Some(QueryLocalKind::HigherOrder))
    }

    /// A designator's callee and its configured prefix as written: a call's
    /// argument members, or a positional pattern's slots. `_` and `@` author
    /// no future hole (FN.48: the residual binds a complete left prefix); a
    /// relation that names no family (an anonymous relation) is no
    /// designator.
    fn designator_parts<'c>(&self, chain: &'c facade::Chain) -> Result<(Name, Option<Qualifier>, Vec<Prefix<'c>>), Refusal> {
        let mut parts = Vec::new();
        match chain.head().form() {
            facade::GroundForm::Reference(facade::Relation::FunctorCall { call, .. }) => {
                let call = call.call();
                let (name, qualifier) = self.input.callee_name(&call.callee);
                if let CallArguments::HigherOrder(part) = &call.arguments {
                    for member in part.members().iter() {
                        parts.push(match member {
                            HoArgument::Value(value) => Prefix::Value(&value.value),
                            HoArgument::Relation(relation) => Prefix::Relation(relation),
                            HoArgument::Rule(rule) => Prefix::Rule(rule),
                            HoArgument::Landing(_) | HoArgument::Skip => return Err(refuse::residual_prefix(&name)),
                            HoArgument::Landed(_) => return Err(refuse::outside("a configured actual landed by a pipe")),
                        });
                    }
                }
                Ok((name, qualifier, parts))
            }
            facade::GroundForm::Reference(facade::Relation::Ground {
                mention: facade::GroundMention::Named { identifier, .. },
            }) => {
                if let Some(facade::Access::Slots(slots)) = chain.head_access() {
                    for slot in slots.iter() {
                        if matches!(slot, facade::Slot::Anon) {
                            return Err(refuse::residual_prefix(identifier.name.as_str()));
                        }
                        parts.push(Prefix::Slot(slot));
                    }
                }
                let qualifier = (!identifier.namespace_path.is_empty()).then(|| identifier.namespace_path.qualifier());
                Ok((identifier.name.clone(), qualifier, parts))
            }
            facade::GroundForm::Literal(_) => Err(refuse::rule_value_form_of_relation()),
            facade::GroundForm::Reference(_) => Err(refuse::outside("a rule designator of this form")),
        }
    }

    /// A prefix member's value, elaborated where its designator stands. A
    /// column no row there answers is a sibling argument's column when one
    /// of the call's other arguments publishes it (FN.48: never over a
    /// sibling argument, `residual-capture`); otherwise it is a column of
    /// nothing in scope.
    fn prefix_value(&mut self, part: &Prefix<'_>, siblings: &Siblings<'_>) -> Result<ExprId, Refusal> {
        self.prefix_member(part).map_err(|error| match refuse::unanswered_column(&error) {
            Some(column) if siblings.publish(&self.b, column) => refuse::residual_capture(siblings.entity, column),
            Some(_) | None => error,
        })
    }

    fn prefix_member(&mut self, part: &Prefix<'_>) -> Result<ExprId, Refusal> {
        match part {
            Prefix::Value(expr) => self.value(expr, CallPosition::Value),
            Prefix::Slot(facade::Slot::Constraint(expr)) => self.value(expr, CallPosition::Value),
            Prefix::Slot(facade::Slot::Bind(binder)) => self.resolve_ref(super::env::Address::Bare(&binder.name)),
            Prefix::Slot(facade::Slot::Reuse(named)) => self.named_reference(named),
            Prefix::Slot(facade::Slot::Anon) | Prefix::Relation(_) | Prefix::Rule(_) => {
                Err(refuse::elaboration_contract("a configured value that is no value"))
            }
        }
    }

    /// An assertion's property (FN.48): a designator whose one remaining
    /// relation input is the act's staging of the asserted rows. Those rows
    /// stand at the designator, so they are its construction rows: its
    /// configured values are read over them exactly as over a relation
    /// landed at a call, each value riding its row.
    pub(super) fn assertion_property(&mut self, property: &facade::Chain, input: RelId) -> Result<RelId, Refusal> {
        let (name, qualifier, written) = self.designator_parts(property)?;
        // The property formal is rule-valued: a name no rule answers leaves
        // it without a rule value.
        let definition = match self.local_relational_definition(&name, qualifier.as_ref())? {
            Some(local) => local,
            None => {
                let referent = self.refer(&name, qualifier.as_ref())?;
                if referent.is_none() && (qualifier.is_some() || self.claimed_kind(&name).is_none()) {
                    return Err(refuse::rule_value_missing("assert!", name.as_str()));
                }
                self.catalog_relational_definition(&name, referent)?
            }
        };
        let params = definition.clauses.first().map(|c| c.params().to_vec()).unwrap_or_default();
        let parts = lifted_prefix(&definition.display, written, &params)?;
        if params.len() <= parts.len() {
            return Err(refuse::residual_none(name.as_str(), parts.len(), params.len()));
        }
        if params.len() > parts.len() + 1 {
            return Err(refuse::residual_contract(name.as_str(), "assert!'s property"));
        }
        let staged = self.b.staged(input)?;
        let (mut actuals, rows): (Vec<Actual>, RelId) = if parts.is_empty() {
            (Vec::new(), staged)
        } else {
            let siblings = Siblings {
                entity: "assert!",
                relations: Vec::new(),
            };
            let (configured, _, carried) =
                self.constructed(staged, |e, row| e.configure(&definition, parts, Some(row), &siblings))?;
            (configured.into_iter().flatten().collect(), carried)
        };
        for actual in &actuals {
            if let Actual::Relation { rel, landed: false } = actual {
                crate::pipeline::middle::core::decide::effect::fence(&self.b, *rel, "a higher-order argument")?;
            }
        }
        actuals.push(Actual::Relation { rel: rows, landed: true });
        self.instantiate(definition, actuals, &[], None)
    }

    /// The construction rows of a call's rule designators: each designator's
    /// configured actuals resolve over one member bound to the landed
    /// relation; each value becomes a passenger born over that member; the
    /// landed relation leaves carrying them.
    fn construction(
        &mut self,
        landed: RelId,
        designators: &[Designator<'_>],
        bound: &[&facade::DomainExpression],
        siblings: &Siblings<'_>,
    ) -> Result<((Vec<RuleValue>, Vec<ExprId>), RelId), Refusal> {
        let ((mut rules, bound_values), passengers, carried) = self.constructed(landed, |e, row| {
            let (rules, mut passengers) = e.designate(designators, Some(row), siblings)?;
            let mut values = Vec::with_capacity(bound.len());
            for value in bound {
                let expr = e.value(value, CallPosition::Value)?;
                let p = e.b.passenger(expr, row);
                passengers.push(p);
                e.column_bound.insert(p);
                values.push(e.b.passenger_value(p));
            }
            Ok(((rules, values), passengers))
        })?;
        if !passengers.is_empty() {
            fn captured(rule: &mut RuleValue, carried: RelId) {
                rule.capture = Some(carried);
                for c in rule.configured.iter_mut().flatten() {
                    if let Actual::Rule(nested) = c {
                        captured(nested, carried);
                    }
                }
            }
            for rule in &mut rules {
                captured(rule, carried);
            }
        }
        Ok(((rules, bound_values), carried))
    }

    /// `configure` run over construction rows: one member bound to `landed`,
    /// whose binder the configured values read; the landed relation leaves
    /// carrying the passengers they were born as.
    fn constructed<T>(
        &mut self,
        landed: RelId,
        configure: impl FnOnce(
            &mut Self,
            crate::pipeline::middle::core::ids::BinderId,
        ) -> Result<(T, Vec<crate::pipeline::middle::core::ids::PassengerId>), Refusal>,
    ) -> Result<(T, Vec<crate::pipeline::middle::core::ids::PassengerId>, RelId), Refusal> {
        self.scopes.push(super::Scope {
            run: crate::pipeline::middle::core::node::run::OpenRun::new(),
            arms: Vec::new(),
            reversed: false,
            qualified_only: false,
            pending: Some(Term {
                rel: landed,
                scope: None,
                route: Route::Plain,
                merge: MergeRequest::None,
                names_scope: false,
                requalifies: true,
                born: crate::pipeline::middle::core::node::run::Born::Written,
            }),
        });
        let result = self.materialize().and_then(|()| {
            let construction = self
                .scopes
                .last()
                .and_then(|s| s.run.members().next())
                .map(|m| m.binder())
                .ok_or_else(|| refuse::outside("a construction with no row"))?;
            configure(self, construction)
        });
        let (value, passengers) = match result {
            Ok(ok) => ok,
            Err(e) => {
                self.scopes.pop();
                return Err(e);
            }
        };
        let run = self.close_top()?;
        let carried = self.b.pipe(
            run,
            crate::pipeline::middle::core::node::PipeOp::Carry(passengers.clone()),
        )?;
        Ok((value, passengers, carried))
    }

    /// Each designator's rule value (FN.48): the family it designates, judged
    /// at the boundary against the contract of the formal it stands at, and
    /// its configured prefix; or, for a rule formal's name, the value it
    /// holds, passed on unchanged. Over construction rows, a configured
    /// value is a passenger born over them. A value read from a member
    /// written to the designator's left in the same run is that member's
    /// row's value, one per occurrence, as a scalar actual read from the
    /// caller's row is; a value that reads no row a scope here holds (a
    /// literal, a closed expression, a caller's value the enclosing instance
    /// received) is that value.
    fn designate(
        &mut self,
        designators: &[Designator<'_>],
        construction: Option<crate::pipeline::middle::core::ids::BinderId>,
        siblings: &Siblings<'_>,
    ) -> Result<(Vec<RuleValue>, Vec<crate::pipeline::middle::core::ids::PassengerId>), Refusal> {
        let mut rules = Vec::new();
        let mut passengers = Vec::new();
        for &Designator { chain, formal, applied } in designators {
            // A name with two groups at a rule-valued position is a completed
            // application, not a designator, whatever its first group holds.
            if let (true, facade::GroundForm::Reference(facade::Relation::FunctorCall { call, .. })) = (applied, chain.head().form()) {
                let (name, _) = self.input.callee_name(&call.call().callee);
                return Err(refuse::rule_value_applied(name.as_str()));
            }
            let (name, qualifier, written) = self.designator_parts(chain)?;
            if let Some(held) = qualifier.is_none().then(|| self.held_rule(&name)).flatten() {
                if !written.is_empty() {
                    return Err(refuse::unruled(
                        "whether a rule value a consumer received may be configured further where it is passed on",
                    ));
                }
                if let Some(HoParam::Rule { signature, .. }) = formal {
                    let definition = self
                        .designated
                        .iter()
                        .find(|d| d.key == held.definition)
                        .ok_or_else(|| refuse::elaboration_contract("a held rule value with no designated definition"))?;
                    let params = definition.clauses.first().map(|c| c.params().to_vec()).unwrap_or_default();
                    let head = definition.clauses.first().map(|c| head_names(&c.head.items));
                    crate::pipeline::middle::core::decide::rule::construct(
                        &definition.display,
                        &crate::pipeline::middle::core::decide::rule::Signature {
                            modes: params.iter().map(param_mode).collect(),
                            output: head.flatten(),
                        },
                        held.configured.iter().take_while(|c| c.is_some()).count(),
                        &contract(signature),
                    )?;
                }
                rules.push(held);
                continue;
            }
            let definition = self.rule_family(&name, qualifier.as_ref())?;
            let params = definition.clauses.first().map(|c| c.params().to_vec()).unwrap_or_default();
            let parts = lifted_prefix(&definition.display, written, &params)?;
            if let Some(HoParam::Rule { signature, .. }) = formal {
                let head = definition.clauses.first().map(|c| head_names(&c.head.items));
                crate::pipeline::middle::core::decide::rule::construct(
                    &definition.display,
                    &crate::pipeline::middle::core::decide::rule::Signature {
                        modes: params.iter().map(param_mode).collect(),
                        output: head.flatten(),
                    },
                    parts.len(),
                    &contract(signature),
                )?;
            }
            let (configured, born) = self.configure(&definition, parts, construction, siblings)?;
            passengers.extend(born);
            let key = definition.key.clone();
            let within_itself = self.building.iter().any(|b| b.definition == key);
            // A closed value nested in definition instances captures their
            // formals here, where it is designated.
            let closure = match (within_itself, definition.frames.min(self.frames.len())) {
                (false, open) if open > 0 => {
                    self.closures.push(self.frames[..open].to_vec());
                    Some(self.closures.len() - 1)
                }
                _ => None,
            };
            if !self.designated.iter().any(|d| d.key == key) {
                self.designated.push(definition);
            }
            rules.push(RuleValue {
                definition: key,
                configured,
                capture: None,
                within_itself,
                closure,
            });
        }
        Ok((rules, passengers))
    }

    /// A designated family's configured prefix (FN.48): each written member
    /// is what the family's formal at its position takes (FN.11). Over
    /// construction rows, a configured value read from them is a passenger
    /// born over them. A value read from a member written to the
    /// designator's left in the same run is that member's row's value, one
    /// per occurrence, as a scalar actual read from the caller's row is; a
    /// value that reads no row a scope here holds (a literal, a closed
    /// expression, a caller's value the enclosing instance received) is that
    /// value.
    fn configure(
        &mut self,
        definition: &Definition,
        parts: Vec<Sealed<'_>>,
        construction: Option<crate::pipeline::middle::core::ids::BinderId>,
        siblings: &Siblings<'_>,
    ) -> Result<(Vec<Option<Actual>>, Vec<crate::pipeline::middle::core::ids::PassengerId>), Refusal> {
        let params = definition.clauses.first().map(|c| c.params().to_vec()).unwrap_or_default();
        let mut passengers = Vec::new();
        // Each prefix member is what the designated family's formal at
        // its position takes (FN.11): a value, a closed relation, or a
        // rule value designated in turn.
        let mut configured: Vec<Option<Actual>> = Vec::with_capacity(params.len());
        for (i, part) in parts.iter().enumerate() {
            let part = match part {
                Sealed::Lifted(cells) => {
                    configured.push(Some(Actual::Relation {
                        rel: self.lifted_row(cells)?,
                        landed: false,
                    }));
                    continue;
                }
                Sealed::Part(part) => part,
            };
            let actual = match (part, params.get(i)) {
                (Prefix::Relation(rule), Some(param @ HoParam::Rule { .. })) => {
                    let nested = Designator { chain: rule, formal: Some(param), applied: true };
                    let (mut nested, born) = self.designate(&[nested], construction, siblings)?;
                    passengers.extend(born);
                    Actual::Rule(nested.pop().ok_or_else(|| refuse::elaboration_contract("a nested designator lost"))?)
                }
                (Prefix::Rule(rule), Some(param @ HoParam::Rule { .. })) => {
                    let nested = Designator { chain: rule, formal: Some(param), applied: false };
                    let (mut nested, born) = self.designate(&[nested], construction, siblings)?;
                    passengers.extend(born);
                    Actual::Rule(nested.pop().ok_or_else(|| refuse::elaboration_contract("a nested designator lost"))?)
                }
                (Prefix::Rule(_), Some(HoParam::Relation { name, .. })) => {
                    return Err(refuse::relation_actual_form(name.as_str(), &definition.display))
                }
                (Prefix::Relation(relation), _) => Actual::Relation {
                    rel: self.chain(relation)?,
                    landed: false,
                },
                (Prefix::Value(_) | Prefix::Slot(_), Some(HoParam::Rule { name, .. })) => {
                    return Err(refuse::rule_value_form(name.as_str(), &definition.display))
                }
                (Prefix::Rule(_), _) => return Err(refuse::outside("a configured rule value at a position of another kind")),
                (Prefix::Value(_) | Prefix::Slot(_), _) => {
                    let expr = self.prefix_value(part, siblings)?;
                    let fv = crate::pipeline::middle::core::graph::Arena::expr(&self.b, expr).fv().clone();
                    match construction {
                        // A value read from the construction row rides
                        // that row; one per row.
                        Some(row) if fv.contains(&row) => {
                            let p = self.b.passenger(expr, row);
                            passengers.push(p);
                            Actual::Value(self.b.passenger_value(p))
                        }
                        _ if self.reads_beyond_run(expr, construction.is_some()) => {
                            return Err(refuse::unruled(
                                "whether a rule value may be configured from a row beyond the run its designator \
                                 stands in (across an existence boundary)",
                            ))
                        }
                        // A value no construction row carries is evaluated
                        // at each spend: no once-only promise (FN.48, U7).
                        _ => Actual::Value(expr),
                    }
                }
            };
            configured.push(Some(actual));
        }
        while configured.len() < params.len() {
            configured.push(None);
        }
        Ok((configured, passengers))
    }

    /// The family a rule-valued position designates (FN.48): a query-local
    /// parameterized definition, else a consulted pure parameterized
    /// relational rule. A name the query-local blocks claim as another kind
    /// is never looked up further; any other name — a table, a view with no
    /// parameter row, a truth rule, none — names no rule.
    fn rule_family(&mut self, name: &Name, qualifier: Option<&Qualifier>) -> Result<Definition, Refusal> {
        if qualifier.is_none() {
            if let Some((_, kind)) = self.claim(name, QueryLocalDemand::HigherOrder)? {
                if kind != QueryLocalKind::HigherOrder {
                    return Err(refuse::callable_unknown(name.as_str()));
                }
            }
        }
        if let Some(local) = self.local_relational_definition(name, qualifier)? {
            return Ok(local);
        }
        let family = match self.refer(name, qualifier)? {
            Some(Referent::Family(family)) => family,
            Some(Referent::Served(_) | Referent::Declared(_)) | None => {
                return Err(refuse::rule_value_missing("a rule-valued parameter", name.as_str()))
            }
        };
        let declared = self.input.family_clauses(&family)?;
        let parameterized = declared.iter().all(|(kind, clause, _)| *kind == DefKind::HoView && !clause.params().is_empty());
        if declared.is_empty() || !parameterized {
            return Err(refuse::rule_value_missing("a rule-valued parameter", name.as_str()));
        }
        self.catalog_definition(&family, &[DefKind::HoView])
    }

    /// The rule value a rule formal of an enclosing instance holds, by name,
    /// innermost first.
    fn held_rule(&self, name: &Name) -> Option<RuleValue> {
        self.frames
            .iter()
            .rev()
            .find_map(|f| f.rules.iter().find(|(n, _)| n == name).map(|(_, r)| r.clone()))
    }

    /// Whether a configured value reads a row of an open scope other than
    /// the run its designator's call stands in (`construction`: the top
    /// scope holds the construction row, and the run is the one below it).
    fn reads_beyond_run(&self, expr: ExprId, construction: bool) -> bool {
        let fv = crate::pipeline::middle::core::graph::Arena::expr(&self.b, expr).fv();
        let n = self.scopes.len();
        let readable = |i: usize| i + 1 == n || (construction && i + 2 == n);
        self.scopes
            .iter()
            .enumerate()
            .filter(|(i, _)| !readable(*i))
            .any(|(_, scope)| scope.run.members().any(|m| fv.contains(&m.binder())))
    }

    /// Where a definition under construction is entered again: `None` when
    /// it is not under construction; otherwise its place in the stack. The
    /// one re-entry judgment of every definition kind — a query-local
    /// relation family, a parameterized or consulted relation definition,
    /// a clause-local one, a value or a truth definition. A value or truth
    /// definition entered again, directly or through definitions of its own
    /// form alone, is function- or truth-form recursion; a relation entered
    /// directly is its self-reference; any other return is a cycle through
    /// other definitions (recursion-contract-law).
    pub(super) fn reentry(&self, key: &str, display: &str) -> Result<Option<usize>, Refusal> {
        let Some(at) = self.building.iter().rposition(|b| b.definition == key) else {
            return Ok(None);
        };
        let through = &self.building[at + 1..];
        match self.building[at].form {
            Form::Value if through.iter().all(|b| b.form == Form::Value) => {
                return Err(refuse::recursion_function_form(display))
            }
            Form::Truth if through.iter().all(|b| b.form == Form::Truth) => {
                return Err(refuse::recursion_truth_form(display))
            }
            Form::Relation if through.is_empty() => return Ok(Some(at)),
            Form::Value | Form::Truth | Form::Relation => {}
        }
        let chain: Vec<&str> = self.building[at..]
            .iter()
            .map(|b| b.display.as_str())
            .chain(std::iter::once(display))
            .collect();
        Err(refuse::recursion_cycle(&chain.join(" -> "), self.building[at].local.then_some(display)))
    }

    /// The instance of `definition` over `actuals`, read under `access`: the
    /// frontier of the instance being built when this is its
    /// self-reference, else a new instance. A self-reference with other
    /// actuals, before the anchor, or under anything but the whole access
    /// refuses.
    pub(super) fn instantiate(
        &mut self,
        definition: Definition,
        actuals: Vec<Actual>,
        written: &[bool],
        access: Option<&facade::Access>,
    ) -> Result<RelId, Refusal> {
        if let Some(at) = self.reentry(&definition.key, &definition.display)? {
            use crate::pipeline::middle::core::decide::recursion::{reentry, Reentry};
            let open: Vec<Actual> = self
                .building
                .iter()
                .filter(|b| b.stands_in)
                .flat_map(|b| b.actuals.iter().cloned())
                .collect();
            match reentry(&self.b, &self.building[at].actuals, &actuals, &open) {
                Reentry::Widened => return Err(refuse::parameter_widening(&definition.display)),
                Reentry::Same | Reentry::Undecided => {}
            }
            if self.building[at].actuals.iter().any(|a| configured_from_rows(&self.b, a)) {
                return Err(refuse::unruled(
                    "whether a rule value configured from rows (a construction row, a member to the left) may be \
                     carried through a recursive definition",
                ));
            }
            return self.self_reference(at, &definition.display, access);
        }
        self.new_instance(definition, actuals, written, access)
    }

    /// A new instance of `definition` over `actuals` (`written`: by position,
    /// whether the call wrote a literal or mention there), read under
    /// `access`.
    fn new_instance(
        &mut self,
        definition: Definition,
        actuals: Vec<Actual>,
        written: &[bool],
        access: Option<&facade::Access>,
    ) -> Result<RelId, Refusal> {
        self.built(definition, actuals, written, access, false)
    }

    /// An instance of `definition` over `actuals`: supplied by a use, or
    /// the stand-ins of its declaration.
    fn built(
        &mut self,
        mut definition: Definition,
        actuals: Vec<Actual>,
        written: &[bool],
        access: Option<&facade::Access>,
        stands_in: bool,
    ) -> Result<RelId, Refusal> {
        let clauses = definition.clauses.clone();
        if clauses.is_empty() {
            return Err(refuse::outside("a definition with no clause"));
        }
        // A danger setting written inside a consulted body: whether the
        // body carries its own gate is not ruled, and no other danger is
        // covered.
        if let (false, Some(declared)) = (stands_in, definition.declared) {
            if !declared.covered {
                return Err(refuse::outside("a consulted definition whose body declares a danger setting"));
            }
            if declared.min_multiplicity {
                return Err(refuse::unruled(
                    "whether a min_multiplicity gate written inside a consulted definition's body changes that body \
                     (DOCKET: a danger gate that parses and does nothing)",
                ));
            }
        }
        self.building.push(Building {
            definition: definition.key.clone(),
            display: definition.display.clone(),
            local: definition.world.is_none(),
            form: Form::Relation,
            actuals: actuals.clone(),
            stands_in,
            frontier: None,
        });
        let result = self.instance_body(&mut definition, &clauses, &actuals, written);
        self.building.pop();
        let body = result?;
        let id = self.b.push_application(definition.key.clone(), actuals, body)?;
        let rel = self.b.apply(id)?;
        match self.access_spec(access, self.displayed_width(rel))? {
            AccessSpec::All => Ok(rel),
            access @ (AccessSpec::Unasked | AccessSpec::Slots(_)) => self.b.local_read(rel, access, &self.switches),
        }
    }

    fn instance_body(
        &mut self,
        definition: &mut Definition,
        clauses: &[Clause],
        actuals: &[Actual],
        written: &[bool],
    ) -> Result<RelId, Refusal> {
        self.dispatched(definition, clauses, actuals, written)?;
        let mut all = self.instance_specs(definition, 0, &clauses[0], actuals)?;
        let frontier = match clauses.len() > 1 || all.len() > 1 {
            true => Some(self.b.frontier(all[0].body)?),
            false => None,
        };
        if let Some(top) = self.building.last_mut() {
            top.frontier = frontier;
        }
        for (i, clause) in clauses.iter().enumerate().skip(1) {
            all.extend(self.instance_specs(definition, i, clause, actuals)?);
        }
        let display = definition.display.clone();
        self.b.close_family(&display, &definition.name, frontier, all)
    }

    /// THE PROVABLE MISS at each position where the call wrote a literal
    /// or mention (`decide::dispatch::provable_miss`), over every clause's
    /// ground member there. A value reaching the call any other way — a
    /// formal forwarded, a configured value — is no knowable argument of
    /// this call, and a miss is the empty relation.
    fn dispatched(&mut self, definition: &Definition, clauses: &[Clause], actuals: &[Actual], written: &[bool]) -> Result<(), Refusal> {
        for (position, actual) in actuals.iter().enumerate() {
            let (Actual::Value(e), Some(true)) = (actual, written.get(position)) else {
                continue;
            };
            let crate::pipeline::middle::core::node::ExprKind::Const(literal) =
                crate::pipeline::middle::core::graph::Arena::expr(&self.b, *e).kind()
            else {
                continue;
            };
            let literal = literal.clone();
            let grounds: Vec<Option<LiteralValue>> = clauses
                .iter()
                .map(|clause| match clause.params().get(position) {
                    Some(HoParam::Ground { text, .. }) => Some(ground_literal(text)),
                    Some(HoParam::Scalar { .. } | HoParam::Relation { .. } | HoParam::Rule { .. }) | None => None,
                })
                .collect();
            crate::pipeline::middle::core::decide::dispatch::provable_miss(&definition.name, position, &literal, &grounds)?;
        }
        Ok(())
    }

    /// The family clauses clause `i` of a definition contributes: a rule
    /// clause is one; a stacked fact is one, its table under its header; a
    /// headerless fact is one per row, each datum supplied to its position
    /// and named by the row's offer or abstaining (FACT ELABORATION; heads-law
    /// CLAUSE AGREEMENT judges the offers where the family closes).
    fn instance_specs(
        &mut self,
        definition: &mut Definition,
        i: usize,
        clause: &Clause,
        actuals: &[Actual],
    ) -> Result<Vec<ClauseSpec>, Refusal> {
        let badged = clause.head.fixpoint.is_badged();
        let fact = definition.facts.get(i).is_some_and(Option::is_some);
        let rows = match definition.facts.get(i).cloned().flatten() {
            Some(offers) if !offers.is_empty() => offers,
            Some(_) | None => {
                if fact {
                    fact_header(&definition.display, clause)?;
                }
                let (guard, body) = self.instance_clause(definition, clause, actuals)?;
                return Ok(vec![ClauseSpec {
                    guard,
                    body,
                    badged,
                    closed: fact || clause.head.items.listed().is_some(),
                    fact,
                }]);
            }
        };
        let DdlBody::Relational(query) = &clause.body else {
            return Err(refuse::elaboration_contract("a fact clause whose body is not its table"));
        };
        let facade::GroundForm::Literal(anon) = query.body.head().form() else {
            return Err(refuse::elaboration_contract("a fact clause whose body is not its table"));
        };
        let table = anon
            .table()
            .ok_or_else(|| refuse::elaboration_contract("a fact clause whose body is not its table"))?;
        let mut specs = Vec::with_capacity(rows.len());
        for (row, offers) in table.rows().iter().zip(&rows) {
            let positions: Vec<Declared<'_>> = offers
                .iter()
                .map(|offer| match offer {
                    Some(name) => Declared::Names(name),
                    None => Declared::Abstains,
                })
                .collect();
            form::form(Formed::Declared {
                name: &definition.display,
                positions: &positions,
            })?;
            let frame = Frame {
                marked: None,
                values: Vec::new(),
                relations: Vec::new(),
                named: Vec::new(),
                rules: Vec::new(),
                ..Frame::default()
            };
            let body = self.within_definition(definition, frame, |e| {
                let mut cells = Vec::new();
                for datum in row.iter() {
                    cells.push(e.value(&datum.value(), CallPosition::Value)?);
                }
                let header = cells.iter().map(|_| crate::pipeline::middle::core::node::rel::HeaderSpec::Anon).collect();
                let lit = e.b.lit(header, vec![cells], false, &e.switches)?;
                let binder = e.member_over(lit)?;
                let items = offers
                    .iter()
                    .enumerate()
                    .map(|(k, offer)| crate::pipeline::middle::core::node::Item {
                        expr: e.b.col(binder, k as u16),
                        naming: match offer {
                            Some(name) => crate::pipeline::middle::core::node::Naming::As(name.clone()),
                            None => crate::pipeline::middle::core::node::Naming::Abstain,
                        },
                    })
                    .collect();
                let input = e.close_top()?;
                e.b.pipe(input, crate::pipeline::middle::core::node::PipeOp::Project(items))
            })?;
            specs.push(ClauseSpec {
                guard: None,
                body,
                badged,
                closed: true,
                fact: true,
            });
        }
        Ok(specs)
    }

    /// One clause's binding of its formals to `actuals`: its dispatch guard
    /// (the ground positions matched against their actuals), the frame its
    /// body is elaborated in, and its body.
    fn clause_frame(
        &mut self,
        definition: &mut Definition,
        clause: &Clause,
        actuals: &[Actual],
    ) -> Result<(Option<TruthId>, Frame, facade::Query), Refusal> {
        let params = clause.params();
        if params.is_empty() && !actuals.is_empty() {
            return Err(refuse::not_parameterized(&definition.display));
        }
        if actuals.len() < params.len() {
            return Err(refuse::incomplete_application(&format!(
                "'{}' declares {} parameter(s), but this application supplies only {} — every parameter is \
                 required input",
                definition.display,
                params.len(),
                actuals.len()
            )));
        }
        if actuals.len() > params.len() {
            return Err(refuse::incomplete_application(&format!(
                "the application of '{}' leaves {} authored parameter-row member(s) without a declared parameter",
                definition.display,
                actuals.len() - params.len()
            )));
        }
        // A value standing at a relation formal is no relation: a kind
        // error wherever it stands in the row. A relation standing at a
        // scalar formal lifts (the callee lifts the slot), which this
        // binding does not cover.
        if let Some((param, _)) = params
            .iter()
            .zip(actuals.iter())
            .find(|(p, a)| matches!((p, a), (HoParam::Relation { .. }, Actual::Value(_))))
        {
            return Err(refuse::argument_kind(&definition.display, param.name().as_str()));
        }
        let mut values: Vec<Option<ExprId>> = vec![None; params.len()];
        let mut relations: Vec<(Name, RelId)> = Vec::new();
        let mut grounds: Vec<(ExprId, LiteralValue)> = Vec::new();
        let mut rules = Vec::new();
        for (i, (param, actual)) in params.iter().zip(actuals).enumerate() {
            match (param, actual) {
                (HoParam::Scalar { guard: None, .. }, Actual::Value(e)) => values[i] = Some(*e),
                (HoParam::Relation { name, cols }, Actual::Relation { rel: r, .. }) => {
                    let rel = match cols.listed() {
                        None => *r,
                        Some(declared) => self.bound_to_pattern(*r, declared)?,
                    };
                    relations.push((name.clone(), rel))
                }
                (HoParam::Ground { text, .. }, Actual::Value(e)) => grounds.push((*e, ground_literal(text))),
                (HoParam::Scalar { guard: Some(_), .. }, _) => {
                    return Err(refuse::outside("a guarded scalar formal"))
                }
                (HoParam::Rule { name, .. }, Actual::Rule(rule)) => rules.push((name.clone(), rule.clone())),
                (HoParam::Rule { .. }, _) => return Err(refuse::outside("a rule formal bound to something else")),
                (HoParam::Scalar { .. } | HoParam::Ground { .. }, Actual::Relation { .. }) => {
                    return Err(refuse::outside("a relation lifted into a scalar formal"))
                }
                (HoParam::Scalar { .. } | HoParam::Relation { .. } | HoParam::Ground { .. }, _) => {
                    return Err(refuse::outside("an actual of another kind than its formal"))
                }
            }
        }
        let mut guards: Vec<TruthId> = Vec::new();
        for (actual, literal) in grounds {
            let literal = self.b.constant(literal);
            guards.push(self.b.cmp(CmpOp::NullSafeEqual, actual, literal, Consumer::Value, true, &self.switches)?);
        }
        let query = match &clause.body_text {
            Some(_) => {
                self.input.analysis_body(clause)?
            }
            None => match &clause.body {
                DdlBody::Relational(query) => query.clone(),
                DdlBody::Scalar(_)
                | DdlBody::Truth(_)
                | DdlBody::FactFunction(_)
                | DdlBody::Deferred => return Err(refuse::outside("a non-relational definition body")),
            },
        };
        let frame = Frame {
            marked: self.input.clause_scope(&query),
            values,
            relations,
            named: Vec::new(),
            rules,
            ..Frame::default()
        };
        let guard = match guards.len() {
            0 => None,
            1 => Some(guards[0]),
            _ => Some(self.b.and(guards)),
        };
        Ok((guard, frame, query))
    }

    /// One clause of an instance: its dispatch guard (the ground positions
    /// matched against their actuals), and its body projected through its
    /// head, both elaborated in the definition's environment with the
    /// formals bound.
    pub(super) fn instance_clause(
        &mut self,
        definition: &mut Definition,
        clause: &Clause,
        actuals: &[Actual],
    ) -> Result<(Option<TruthId>, RelId), Refusal> {
        let (guard, frame, query) = self.clause_frame(definition, clause, actuals)?;
        let items = &clause.head.items;
        let body = self.within_definition(definition, frame, |e| {
            let body = e.query(&query)?;
            e.head(body, items)
        })?;
        Ok((guard, body))
    }

    /// A BODY IS JUDGED WHERE IT IS DECLARED (top-grammar): a definition is
    /// elaborated once in its own environment with each formal standing for
    /// what no caller supplies — a scalar or ground formal reads a
    /// declaration row (a scalar actual read from the caller's row is such a
    /// value, FN.51), and a relation formal declared with a column pattern
    /// binds a declaration relation of that width by the one positional
    /// binding every actual takes, so its heading is the declared one. The
    /// instance is never part of the statement. A query-local definition's
    /// whole family is elaborated (its bodies, heads, clause agreement,
    /// badge and recursion verdicts): the statement's world is the one its
    /// uses see. A consulted definition's heads are judged against bodies
    /// that elaborate; what the world binds later (a data hole) decides the
    /// rest at its uses. A definition with an open relation formal or a
    /// rule formal is judged at its uses, where an actual decides its body's
    /// heading; and a form this middle does not cover is judged where it is
    /// used.
    pub(super) fn declared_heads(&mut self, mut definition: Definition) -> Result<(), Refusal> {
        if definition.facts.iter().any(Option::is_some) {
            self.declared_agreement(&mut definition)?;
        }
        let clauses = definition.clauses.clone();
        let Some(first) = clauses.first() else {
            return Ok(());
        };
        let params = first.params().to_vec();
        let open = |p: &HoParam| match p {
            HoParam::Rule { .. } => true,
            HoParam::Relation { cols, .. } => cols.listed().is_none(),
            HoParam::Scalar { .. } | HoParam::Ground { .. } => false,
        };
        if params.iter().any(open) {
            return Ok(());
        }
        let actuals = self.declaration_actuals(&params)?;
        if definition.world.is_none() {
            return match self.built(definition, actuals, &[], None, true) {
                Ok(_) => Ok(()),
                Err(error) if refuse::is_uncovered(&error) => Ok(()),
                Err(error) => Err(error),
            };
        }
        self.building.push(Building {
            definition: definition.key.clone(),
            display: definition.display.clone(),
            local: definition.world.is_none(),
            form: Form::Relation,
            actuals: actuals.clone(),
            stands_in: true,
            frontier: None,
        });
        let judged = self.declared_clause_heads(&mut definition, &clauses, &actuals);
        self.building.pop();
        judged?;
        // A consulted body reads the catalog state of the statement that
        // uses it, so what it reads is judged there; its self-reference is
        // its own structure, malformed for every use where it is declared.
        match self.built(definition, actuals, &[], None, true) {
            Err(error) if refuse::is_self_reference(&error) => Err(error),
            Ok(_) | Err(_) => Ok(()),
        }
    }

    /// What each formal stands for where its definition is declared: a
    /// scalar or ground formal, a column of one declaration row; a relation
    /// formal declared with a column pattern, a one-row relation of that
    /// many unnamed columns, which the pattern names when it binds.
    fn declaration_actuals(&mut self, params: &[HoParam]) -> Result<Vec<Actual>, Refusal> {
        if params.is_empty() {
            return Ok(Vec::new());
        }
        let null = self.b.constant(LiteralValue::Null);
        let anon = |n: usize| -> Vec<crate::pipeline::middle::core::node::rel::HeaderSpec> {
            (0..n).map(|_| crate::pipeline::middle::core::node::rel::HeaderSpec::Anon).collect()
        };
        let shape = self.b.lit(anon(params.len()), vec![vec![null; params.len()]], false, &self.switches)?;
        let row = self.b.declaration_row(shape);
        let mut actuals = Vec::with_capacity(params.len());
        for (i, param) in params.iter().enumerate() {
            actuals.push(match param {
                HoParam::Relation { cols, .. } => {
                    let width = cols.listed().map_or(0, <[facade::HeadItem]>::len);
                    let rel = self.b.lit(anon(width), vec![vec![null; width]], false, &self.switches)?;
                    Actual::Relation { rel, landed: false }
                }
                HoParam::Scalar { .. } | HoParam::Ground { .. } | HoParam::Rule { .. } => {
                    Actual::Value(self.b.col(row, i as u16))
                }
            });
        }
        Ok(actuals)
    }

    /// CLAUSE AGREEMENT where a family holding facts is declared, by the one
    /// family judgment: a fact clause contributes the heading its rows or
    /// header publish, a closed rule head the positions its offers name (no
    /// body read), an open rule clause its body's heading where the body
    /// elaborates with no use (otherwise it is judged at its uses).
    fn declared_agreement(&mut self, definition: &mut Definition) -> Result<(), Refusal> {
        let clauses = definition.clauses.clone();
        let fact = |i: usize| definition.facts.get(i).is_some_and(Option::is_some);
        crate::pipeline::middle::core::node::rel::judge_forms(
            &definition.display,
            clauses.iter().enumerate().map(|(i, clause)| fact(i) || clause.head.items.listed().is_some()),
        )?;
        self.building.push(Building {
            definition: definition.key.clone(),
            display: definition.display.clone(),
            local: definition.world.is_none(),
            form: Form::Relation,
            actuals: Vec::new(),
            stands_in: true,
            frontier: None,
        });
        let mut headings: Vec<(crate::pipeline::middle::core::heading::Heading, bool, bool)> = Vec::new();
        let mut judged = Ok(());
        for (i, clause) in clauses.iter().enumerate() {
            let fact = definition.facts.get(i).is_some_and(Option::is_some);
            if let (false, Some(items)) = (fact, clause.head.items.listed()) {
                let positions: Vec<Declared<'_>> = items.iter().map(head_position).collect();
                match form::form(Formed::Declared {
                    name: &definition.display,
                    positions: &positions,
                }) {
                    Ok(heading) => headings.push((heading, true, false)),
                    Err(e) => {
                        judged = Err(e);
                        break;
                    }
                }
                continue;
            }
            match self.instance_specs(definition, i, clause, &[]) {
                Ok(specs) => headings.extend(specs.iter().map(|spec| {
                    let heading = crate::pipeline::middle::core::graph::Arena::rel(&self.b, spec.body).heading().clone();
                    (heading, spec.closed, spec.fact)
                })),
                Err(e) if fact => {
                    judged = Err(e);
                    break;
                }
                Err(_) => {}
            }
        }
        self.building.pop();
        judged?;
        if headings.is_empty() {
            return Ok(());
        }
        let clause_headings: Vec<crate::pipeline::middle::core::node::rel::ClauseHeading<'_>> = headings
            .iter()
            .map(|(heading, closed, fact)| crate::pipeline::middle::core::node::rel::ClauseHeading {
                heading,
                closed: *closed,
                fact: *fact,
            })
            .collect();
        crate::pipeline::middle::core::node::rel::judge_family(&definition.display, &definition.name, &clause_headings).map(|_| ())
    }

    fn declared_clause_heads(&mut self, definition: &mut Definition, clauses: &[Clause], actuals: &[Actual]) -> Result<(), Refusal> {
        for clause in clauses {
            let items = &clause.head.items;
            if items.listed().is_none() {
                continue;
            }
            let Ok((_, frame, query)) = self.clause_frame(definition, clause, actuals) else {
                continue;
            };
            self.within_definition(definition, frame, |e| match e.query(&query) {
                Ok(body) => e.head(body, items).map(|_| ()),
                Err(_) => Ok(()),
            })?;
        }
        Ok(())
    }

    /// A relation actual bound to its formal's declared column pattern: the
    /// pattern is POSITIONAL, so it is
    /// the actual read under the pattern as an argumentative access, by the
    /// core's one read constructor: each declared name binds the actual's
    /// position in order whatever that position's name, a repeated name
    /// unifies (lvars-law), and a width other than the pattern's refuses
    /// `semantic/arity`. The positions the actual carries hidden travel
    /// with its rows.
    fn bound_to_pattern(&mut self, actual: RelId, declared: &[facade::HeadItem]) -> Result<RelId, Refusal> {
        let mut slots = Vec::with_capacity(declared.len());
        for item in declared {
            match (&item.label, &item.supply) {
                (None, facade::Supply::Ref(name)) => slots.push(crate::pipeline::middle::core::node::rel::SlotSpec::Bind(name.clone())),
                (Some(_), _) | (None, facade::Supply::Ground(_)) => {
                    return Err(refuse::outside("a relation formal declaring a column other than by its bare name"))
                }
            }
        }
        self.b.local_read(actual, AccessSpec::Slots(slots), &self.switches)
    }

    /// Run `f` in a definition's declaration environment: its blocks,
    /// horizon, world and frames, with its own formals' frame open.
    pub(super) fn within_definition<T>(
        &mut self,
        definition: &mut Definition,
        frame: Frame,
        f: impl FnOnce(&mut Self) -> Result<T, Refusal>,
    ) -> Result<T, Refusal> {
        let blocks = definition.blocks;
        let horizon = definition.horizon;
        let world = definition.world;
        // A closed rule value's spend reopens the frames it captured; any
        // other body sees the first `frames` frames open here.
        let captured = definition.captured.and_then(|c| self.closures.get(c).cloned());
        let frames = if captured.is_some() { 0 } else { definition.frames };
        self.apart(blocks, frames, |e| {
            let saved_horizon = match (horizon, blocks.checked_sub(1)) {
                (Some(h), Some(i)) => Some((i, std::mem::replace(&mut e.blocks[i].horizon, h))),
                (Some(_) | None, _) => None,
            };
            if let Some(w) = world {
                std::mem::swap(&mut e.at, &mut e.sites[w]);
            }
            let base = e.frames.len();
            e.frames.extend(captured.into_iter().flatten());
            e.frames.push(frame);
            let out = f(e);
            e.frames.truncate(base);
            if let Some(w) = world {
                std::mem::swap(&mut e.at, &mut e.sites[w]);
            }
            if let Some((i, h)) = saved_horizon {
                e.blocks[i].horizon = h;
            }
            out
        })
    }

    /// A sigma citation: the truth rule or served predicate its name
    /// selects, with its arity judged. An authored rule is instantiated at
    /// the citation: each clause binds the arguments to its own formals by
    /// position and its truth is elaborated in the rule's environment, so
    /// its comparisons take their equality class at the invocation; the
    /// clauses are alternatives.
    pub(super) fn sigma_proof(
        &mut self,
        name: &Name,
        qualifier: Option<&Qualifier>,
        arguments: &[ExprId],
        consumer: Consumer,
    ) -> Result<SigmaProof, Refusal> {
        if qualifier.is_none() {
            if let Some((i, kind)) = self.claim(name, QueryLocalDemand::Sigma)? {
                if kind != QueryLocalKind::Sigma {
                    return Err(refuse::outside("a query-local definition of another kind"));
                }
                let sigma = self.blocks[i]
                    .sigmas
                    .iter()
                    .find(|s| s.name() == name)
                    .ok_or_else(|| refuse::outside("a sigma the block does not hold"))?
                    .clone();
                let definition = Definition {
                    key: self.local_key(i, sigma.name()),
                    display: name.to_string(),
                    name: name.to_string(),
                    clauses: sigma.group().clauses().to_vec(),
                    blocks: i + 1,
                    frames: self.blocks[i].frames,
                    horizon: Some(sigma.horizon()),
                    world: None,
                    facts: Vec::new(),
                    declared: None,
                    captured: None,
                };
                return self.truth_rule(definition, arguments, consumer);
            }
        }
        match self.refer(name, qualifier)? {
            Some(Referent::Family(family)) => {
                let definition = self.catalog_definition(&family, &[DefKind::Sigma])?;
                self.truth_rule(definition, arguments, consumer)
            }
            Some(Referent::Served(served)) if served.kind() == facade::EntityType::BinSigmaPredicate => {
                if let Some(width) = self.input.predicate_arity(&served) {
                    if width != arguments.len() {
                        return Err(refuse::sigma_arity(name.as_str(), width, arguments.len()));
                    }
                }
                Ok(SigmaProof::Served {
                    namespace: served.namespace().to_string(),
                    name: served.name().clone(),
                })
            }
            Some(Referent::Served(_) | Referent::Declared(_)) => {
                Err(refuse::outside("a relation's existence face in sigma position"))
            }
            None => Err(refuse::callable_unknown_sigma(name.as_str())),
        }
    }

    fn truth_rule(
        &mut self,
        mut definition: Definition,
        arguments: &[ExprId],
        consumer: Consumer,
    ) -> Result<SigmaProof, Refusal> {
        self.reentry(&definition.key, &definition.display)?;
        self.building.push(Building {
            definition: definition.key.clone(),
            display: definition.display.clone(),
            local: definition.world.is_none(),
            form: Form::Truth,
            actuals: Vec::new(),
            stands_in: false,
            frontier: None,
        });
        let result = self.truth_rule_clauses(&mut definition, arguments, consumer);
        self.building.pop();
        let truths = result?;
        let expansion = match truths.as_slice() {
            [one] => *one,
            _ => self.b.or(truths),
        };
        Ok(SigmaProof::Rule {
            definition: definition.display,
            expansion,
        })
    }

    fn truth_rule_clauses(
        &mut self,
        definition: &mut Definition,
        arguments: &[ExprId],
        consumer: Consumer,
    ) -> Result<Vec<TruthId>, Refusal> {
        let clauses = definition.clauses.clone();
        if clauses.is_empty() {
            return Err(refuse::outside("a truth rule with no clause"));
        }
        let mut truths = Vec::with_capacity(clauses.len());
        for clause in &clauses {
            let params = clause.params();
            if params.len() != arguments.len() {
                return Err(refuse::sigma_arity(&definition.display, params.len(), arguments.len()));
            }
            let mut named = Vec::with_capacity(params.len());
            for (param, argument) in params.iter().zip(arguments) {
                match param {
                    HoParam::Scalar { name, guard: None, .. } => named.push((name.clone(), *argument)),
                    HoParam::Scalar { guard: Some(_), .. }
                    | HoParam::Relation { .. }
                    | HoParam::Rule { .. }
                    | HoParam::Ground { .. } => {
                        return Err(refuse::outside("a truth rule formal that is not a plain value"))
                    }
                }
            }
            let DdlBody::Truth(truth) = &clause.body else {
                return Err(refuse::outside("a truth rule clause whose body is not a truth"));
            };
            let truth = truth.clone();
            let frame = Frame {
                marked: None,
                values: Vec::new(),
                relations: Vec::new(),
                named,
                rules: Vec::new(),
                ..Frame::default()
            };
            truths.push(self.within_definition(definition, frame, |e| e.truth(&truth, consumer))?);
        }
        Ok(truths)
    }

    /// The self-reference of the definition built at `at`: its frontier,
    /// read under `access`; before its anchor there is none.
    fn self_reference(&mut self, at: usize, display: &str, access: Option<&facade::Access>) -> Result<RelId, Refusal> {
        let built = &self.building[at];
        match built.frontier {
            Some(frontier) => {
                let access = self.access_spec(access, self.frontier_width(frontier))?;
                self.b.frontier_read(frontier, access)
            }
            None => Err(refuse::anchor_first(display, built.local)),
        }
    }

    /// Whether a family's clauses are all of the kinds a relation read
    /// takes ([`RELATION_READ`]).
    pub(super) fn reads_as_relation(&mut self, family: &crate::pipeline::middle::select::Family) -> Result<bool, Refusal> {
        let (declared, _) = self.input.family_source(family)?;
        Ok(!declared.is_empty() && declared.iter().all(|(kind, _, _)| RELATION_READ.contains(kind)))
    }

    /// A catalog view read by name: the instance of a definition with no
    /// formals.
    pub(super) fn view_read(
        &mut self,
        family: &crate::pipeline::middle::select::Family,
        access: Option<&facade::Access>,
    ) -> Result<RelId, Refusal> {
        let definition = self.catalog_definition(family, RELATION_READ)?;
        if definition.clauses.iter().any(|c| matches!(c.body, DdlBody::FactFunction(_))) {
            return self.fact_face(definition, access);
        }
        // A mention with no parameter row reads a relation: a definition
        // that declares parameters is no relation until applied. Inside its
        // own body the mention is its self-reference, bare or aliased, and
        // reads the instance being built (recursion-contract-law).
        if definition.clauses.iter().any(|c| !c.params().is_empty()) {
            return match self.reentry(&definition.key, &definition.display)? {
                Some(at) => self.self_reference(at, &definition.display, access),
                None => Err(refuse::not_a_relation(&definition.display)),
            };
        }
        self.instantiate(definition, Vec::new(), &[], access)
    }

    /// A formal's actual: the scalar at `position` of the instance whose
    /// clause reading owns `scope`.
    pub(super) fn formal_value(
        &self,
        scope: crate::pipeline::middle::facade::MarkedScopeId,
        position: usize,
    ) -> Result<ExprId, Refusal> {
        self.frames
            .iter()
            .rev()
            .find(|f| f.marked == Some(scope))
            .and_then(|f| f.values.get(position).copied().flatten())
            .ok_or_else(|| refuse::outside("a formal with no scalar actual in reach"))
    }

    /// A relation formal's actual, by name, innermost visible instance
    /// first.
    pub(super) fn formal_relation(&self, name: &Name) -> Option<RelId> {
        self.frames
            .iter()
            .rev()
            .find_map(|f| f.relations.iter().find(|(n, _)| n == name).map(|(_, r)| *r))
    }

    /// A value or truth definition's formal, by bare name.
    pub(super) fn named_formal(&self, name: &Name) -> Option<ExprId> {
        self.frames
            .iter()
            .rev()
            .find_map(|f| f.named.iter().find(|(n, _)| n == name).map(|(_, e)| *e))
    }
}

/// A rule designator standing at a call's position, the formal it binds,
/// and whether it was written with an access group after its arguments.
#[derive(Clone, Copy)]
struct Designator<'c> {
    chain: &'c facade::Chain,
    formal: Option<&'c HoParam>,
    applied: bool,
}

/// The call a designator stands in: the entity called, and the relations
/// its other arguments supply.
struct Siblings<'e> {
    entity: &'e str,
    relations: Vec<RelId>,
}

impl Siblings<'_> {
    /// Whether one of the call's other arguments publishes a column so named.
    fn publish(&self, arena: &impl crate::pipeline::middle::core::graph::Arena, column: &str) -> bool {
        let name = Name::new(column);
        self.relations.iter().any(|r| {
            arena
                .rel(*r)
                .heading()
                .displayed()
                .any(|(_, p)| p.answering_name() == Some(&name))
        })
    }
}

/// A sealed prefix position: a member as written, or values lifted into
/// one row of a relation formal declared with a column pattern.
enum Sealed<'c> {
    Part(Prefix<'c>),
    Lifted(Vec<&'c DomainExpression>),
}

/// THE LIFT in a designator's prefix (FN.9, FN.48): ground values at a
/// relation formal declared with a column pattern are one row of it, one
/// value per declared column. The prefix's own group states where the
/// lifted row ends; values beyond it at a later scalar formal leave the
/// split unstated, as in any application.
fn lifted_prefix<'c>(entity: &str, written: Vec<Prefix<'c>>, params: &[HoParam]) -> Result<Vec<Sealed<'c>>, Refusal> {
    let mut out = Vec::with_capacity(written.len());
    let mut parts = written.into_iter().peekable();
    let mut f = 0;
    while let Some(part) = parts.next() {
        if let (Prefix::Value(first), Some(HoParam::Relation { name, cols: facade::HeadItems::Listed(items) })) = (&part, params.get(f)) {
            if is_ground_literal(first) {
                let mut cells = vec![*first];
                while cells.len() < items.len() {
                    match parts.peek() {
                        Some(Prefix::Value(value)) => {
                            cells.push(*value);
                            parts.next();
                        }
                        Some(_) | None => break,
                    }
                }
                if cells.len() != items.len() {
                    return Err(refuse::arity(cells.len(), items.len()));
                }
                if parts.peek().is_some() && params[f + 1..].iter().any(|p| !matches!(p, HoParam::Relation { .. })) {
                    return Err(refuse::lifted_boundary(entity, name.as_str()));
                }
                out.push(Sealed::Lifted(cells));
                f += 1;
                continue;
            }
        }
        out.push(Sealed::Part(part));
        f += 1;
    }
    Ok(out)
}

/// Whether an actual is a rule value with a configured value read from a
/// row: a construction row's passenger, or a value of a member written to
/// its designator's left.
fn configured_from_rows(arena: &impl crate::pipeline::middle::core::graph::Arena, actual: &Actual) -> bool {
    let Actual::Rule(rule) = actual else {
        return false;
    };
    rule.configured.iter().flatten().any(|c| match c {
        Actual::Value(e) => {
            matches!(arena.expr(*e).kind(), crate::pipeline::middle::core::node::ExprKind::Passenger(_))
                || !arena.expr(*e).fv().is_empty()
        }
        Actual::Rule(_) => configured_from_rows(arena, c),
        Actual::Relation { .. } => false,
    })
}

/// A designator's configured prefix member as written.
enum Prefix<'c> {
    Value(&'c DomainExpression),
    Slot(&'c facade::Slot),
    Relation(&'c facade::Chain),
    Rule(&'c facade::Chain),
}


/// The columns a family whose every clause lists its head publishes, decided
/// from the heads alone by the one family judgment (heads-law CLAUSE
/// AGREEMENT over each head's declared positions); `None` when a head is
/// open, whose bodies decide its heading where it is read.
pub(crate) fn declared_columns<'h>(subject: &str, heads: impl IntoIterator<Item = &'h facade::Head>) -> Result<Option<Vec<Name>>, Refusal> {
    let mut headings = Vec::new();
    for head in heads {
        let Some(items) = head.items.listed() else {
            return Ok(None);
        };
        let positions: Vec<Declared<'_>> = items.iter().map(head_position).collect();
        headings.push(form::form(Formed::Declared { name: subject, positions: &positions })?);
    }
    let clauses: Vec<crate::pipeline::middle::core::node::rel::ClauseHeading<'_>> = headings
        .iter()
        .map(|heading| crate::pipeline::middle::core::node::rel::ClauseHeading { heading, closed: true, fact: false })
        .collect();
    if clauses.is_empty() {
        return Ok(None);
    }
    let heading = crate::pipeline::middle::core::node::rel::judge_family(subject, subject, &clauses)?;
    Ok(heading.displayed().map(|(_, p)| p.answering_name().cloned()).collect())
}

/// THE FAMILY'S ONE SIGNATURE, judged where a family is declared
/// (`decide::signature::judge`), over what each clause's head states.
pub(crate) fn judge_signatures<'h>(subject: &str, heads: impl IntoIterator<Item = &'h facade::Head>) -> Result<(), Refusal> {
    use crate::pipeline::middle::core::decide::signature::{judge, Capture, ClauseSignature, Receives};
    let receives = |param: &HoParam| match param {
        HoParam::Scalar { callable: false, .. } | HoParam::Ground { .. } => Receives::Value,
        HoParam::Scalar { callable: true, .. } => Receives::Function,
        HoParam::Relation { .. } => Receives::Relation,
        HoParam::Rule { signature, .. } => Receives::Rule(contract(signature)),
    };
    let clauses: Vec<ClauseSignature> = heads
        .into_iter()
        .map(|head| ClauseSignature {
            parameters: head.ho_params.as_ref().map(|params| params.iter().map(receives).collect()),
            capture: match &head.context {
                facade::ContextMode::None => Capture::Nothing,
                facade::ContextMode::Implicit => Capture::CallerRow,
                facade::ContextMode::Explicit(names) => Capture::Declared(names.clone()),
            },
            badged: head.fixpoint.is_badged(),
        })
        .collect();
    judge(subject, &clauses)
}

/// A declared parameter as a residual contract reads it.
fn param_mode(param: &HoParam) -> crate::pipeline::middle::core::decide::rule::Mode {
    use crate::pipeline::middle::core::decide::rule::Mode;
    match param {
        HoParam::Scalar { .. } | HoParam::Ground { .. } => Mode::Scalar,
        HoParam::Relation { cols, .. } => Mode::Relation(cols.listed().map(listed_names)),
        HoParam::Rule { .. } => Mode::Rule,
    }
}

/// A residual contract (`P(... T(*))(*)`) as the boundary reads it.
fn contract(signature: &facade::ResidualSignature) -> crate::pipeline::middle::core::decide::rule::Signature {
    use crate::pipeline::middle::core::decide::rule::Mode;
    crate::pipeline::middle::core::decide::rule::Signature {
        modes: signature
            .remaining
            .iter()
            .map(|mode| match mode {
                facade::ResidualMode::Scalar { .. } => Mode::Scalar,
                facade::ResidualMode::Relation { cols, .. } => Mode::Relation(cols.listed().map(listed_names)),
            })
            .collect(),
        output: head_names(&signature.output),
    }
}

/// The names a listed heading declares, position by position.
fn listed_names(items: &[facade::HeadItem]) -> Vec<Name> {
    items
        .iter()
        .map(|item| match head_position(item) {
            Declared::Names(name) => name.clone(),
            Declared::Abstains | Declared::NamesNothing => Name::new(String::new()),
        })
        .collect()
}

/// A head's published names, `None` for an open head.
fn head_names(items: &facade::HeadItems) -> Option<Vec<Name>> {
    items.listed().map(listed_names)
}

/// One member of a call's parameter row as its formal reads it: a written
/// argument, or values lifted into one row of a relation formal.
enum Supplied<'a> {
    Written(&'a HoArgument),
    Lifted(Vec<&'a DomainExpression>),
}

/// THE LIFT (top-grammar FN.9; FN.11, judged at build against the callee's
/// descriptor): ground values standing at a relation formal declared with a
/// column pattern are one row of that relation, one value per declared
/// column; a row of another width than the pattern refuses `semantic/arity`.
/// A scalar formal after it makes the
/// split between the lifted row and the arguments unstated, which only `&`
/// states.
fn lifted<'a>(entity: &str, members: &[&'a HoArgument], formals: &[Option<HoParam>]) -> Result<Vec<Supplied<'a>>, Refusal> {
    let mut out = Vec::with_capacity(members.len());
    let mut m = 0;
    let mut f = 0;
    while m < members.len() {
        if let (HoArgument::Value(first), Some(Some(HoParam::Relation { name, cols: facade::HeadItems::Listed(items) }))) =
            (members[m], formals.get(f))
        {
            if is_ground_literal(&first.value) {
                if formals[f + 1..].iter().flatten().any(|p| !matches!(p, HoParam::Relation { .. })) {
                    return Err(refuse::lifted_boundary(entity, name.as_str()));
                }
                let mut cells = Vec::with_capacity(items.len());
                while cells.len() < items.len() {
                    match members.get(m + cells.len()) {
                        Some(HoArgument::Value(value)) => cells.push(&value.value),
                        Some(_) | None => break,
                    }
                }
                if cells.len() != items.len() {
                    return Err(refuse::arity(cells.len(), items.len()));
                }
                m += cells.len();
                f += 1;
                out.push(Supplied::Lifted(cells));
                continue;
            }
        }
        out.push(Supplied::Written(members[m]));
        m += 1;
        f += 1;
    }
    Ok(out)
}

/// The literal a ground position stores: the AST's one stored-ground codec
/// (a mention's token is its canonical encoding, whose value is that
/// encoding: grounding-and-mention-law THE ENCODING IS THE VALUE).
fn ground_literal(text: &str) -> LiteralValue {
    LiteralValue::from_stored_ground(text.trim())
}

/// Whether a value is a bare, unqualified name.
/// Whether a heading answers to the bare name a value spells.
fn publishes(heading: &crate::pipeline::middle::core::heading::Heading, value: &facade::DomainExpression) -> bool {
    match value {
        facade::DomainExpression::Reference(facade::Reference::Named(facade::NamedReference(column))) => {
            !crate::pipeline::middle::core::heading::correspondence::answers_to(heading, &column.name).is_empty()
        }
        _ => false,
    }
}

/// Whether a value is a ground literal.
fn is_ground_literal(value: &facade::DomainExpression) -> bool {
    matches!(value, facade::DomainExpression::Application(facade::FunctionApplication::Ground(_)))
}

fn bare_name(value: &facade::DomainExpression) -> Option<Name> {
    match value {
        facade::DomainExpression::Reference(facade::Reference::Named(facade::NamedReference(column)))
            if column.qualifier.is_none() =>
        {
            Some(column.name.clone())
        }
        _ => None,
    }
}

fn is_bare_name(value: &facade::DomainExpression) -> bool {
    matches!(
        value,
        facade::DomainExpression::Reference(facade::Reference::Named(facade::NamedReference(column)))
            if column.qualifier.is_none()
    )
}

/// Whether a call's actuals are values alone, none of them landed.
fn values_only(arguments: &CallArguments) -> bool {
    match arguments {
        CallArguments::None | CallArguments::Scalar(_) => true,
        CallArguments::HigherOrder(part) => part.members().iter().all(|a| matches!(a, HoArgument::Value(_))),
    }
}

/// THE PIPE LANDS ONCE, AT A TABLE PARAMETER: a piped call needs a table
/// parameter, and its written row must leave exactly one formal for the
/// pipe, a table one; a written `@` needs a pipe.
pub(super) fn landing(entity: &str, params: &[HoParam], arguments: &CallArguments) -> Result<(), Refusal> {
    let members: Vec<&HoArgument> = match arguments {
        CallArguments::HigherOrder(part) => part.members().iter().collect(),
        CallArguments::None | CallArguments::Scalar(_) => Vec::new(),
    };
    let landed = members.iter().position(|m| matches!(m, HoArgument::Landed(_)));
    let Some(at) = landed else {
        if members.iter().any(|m| matches!(m, HoArgument::Landing(_))) {
            return Err(refuse::pipe_landing(&format!(
                "the call to '{entity}' writes @ but nothing is piped into it — @ names the landing of a piped \
                 relation; supply the argument directly, or pipe a relation in with |>"
            )));
        }
        return Ok(());
    };
    if !params.iter().any(|p| matches!(p, HoParam::Relation { .. })) {
        return Err(refuse::pipe_landing(&format!(
            "'{entity}' has no table-value parameter to receive pipe input (all parameters are scalar)"
        )));
    }
    let declared = params.len();
    let supplied = members.len() - 1;
    if supplied >= declared {
        return Err(refuse::pipe_landing(&format!(
            "'{entity}' declares {declared} parameter(s) and the call supplies {supplied} beside the piped \
             relation, so the pipe has no formal left to fill"
        )));
    }
    if supplied + 1 < declared {
        return Err(refuse::pipe_landing(&format!(
            "'{entity}' declares {declared} parameter(s) and the call supplies {supplied}, so {} remain and the \
             pipe can fill only one",
            declared - supplied
        )));
    }
    if let Some(param) = params.get(at) {
        if !matches!(param, HoParam::Relation { .. }) {
            let name = param.name();
            return Err(refuse::pipe_landing(&if at + 1 == declared {
                format!(
                    "the pipe lands at the final parameter of '{entity}', and '{name}' occupies it — a relation \
                     can land only at a table parameter (T(*) or T(cols)). write @ at the parameter that \
                     receives the pipe: {entity}(@, …)"
                )
            } else {
                format!(
                    "the pipe lands at '{name}', parameter {} of '{entity}', and '{name}' is scalar — a relation \
                     can land only at a table parameter (T(*) or T(cols)). Supply the scalar and write @ at a \
                     table parameter",
                    at + 1
                )
            }));
        }
    }
    Ok(())
}

/// What a listed rule head's position declares (heads-law HEAD `as` IS
/// RENAME AND BAPTISM): a label names it; an unlabeled reference names it
/// by the body column it plumbs; an unlabeled ground term abstains.
fn head_position(item: &facade::HeadItem) -> Declared<'_> {
    match (&item.label, &item.supply) {
        (Some(label), _) => Declared::Names(label),
        (None, facade::Supply::Ref(name)) => Declared::Names(name),
        (None, facade::Supply::Ground(_)) => Declared::Abstains,
    }
}

/// A stacked fact's header is its head (heads-law SUPPLY IS ELABORATION:
/// the head that remains is ordinary projection): each item names one
/// column, and no two name one identifier. Judged by the declared-head
/// form wherever the fact's clauses are formed, at consultation and at
/// every read.
fn fact_header(name: &str, clause: &Clause) -> Result<(), Refusal> {
    let DdlBody::Relational(query) = &clause.body else {
        return Ok(());
    };
    let facade::GroundForm::Literal(anon) = query.body.head().form() else {
        return Ok(());
    };
    let Some(header) = anon.table().and_then(|table| table.header()) else {
        return Ok(());
    };
    let positions: Vec<Declared<'_>> = header
        .iter()
        .map(|item| match &item.slot {
            facade::Slot::Bind(binder) => Declared::Names(&binder.name),
            facade::Slot::Anon | facade::Slot::Constraint(_) | facade::Slot::Reuse(_) => Declared::NamesNothing,
        })
        .collect();
    form::form(Formed::Declared { name, positions: &positions }).map(|_| ())
}

/// The kinds of definition a relation read by name takes.
const RELATION_READ: &[DefKind] = &[DefKind::View, DefKind::HoView, DefKind::Fact, DefKind::FactFunction];
