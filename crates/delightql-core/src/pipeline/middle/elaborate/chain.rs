// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Chains: a head, then continuations in authored order. Comma members,
//! conditions and bounds extend the open run; a pipe form consumes the run
//! (its items read the run's scope), closes it, and its output waits to be
//! the first member of the next run.

use super::locals::Local;
use super::{Elaborator, Scope, Term};
use crate::pipeline::middle::core::graph::Arena;
use crate::pipeline::middle::core::heading::Name;
use crate::pipeline::middle::core::ids::RelId;
use crate::pipeline::middle::core::decide::grade::CallPosition;
use crate::pipeline::middle::core::node::rel::{AccessSpec, HeaderSpec, SlotSpec};
use crate::pipeline::middle::core::node::run::{Born, MemberSpec, MergeRequest, OpenRun};
use crate::pipeline::middle::core::node::expr::truth_population_sensitive;
use crate::pipeline::middle::core::node::rel::ExpansionForm;
use crate::pipeline::middle::core::node::{Bound, Consumer, Item, Naming, PipeOp, Qual, RelKind, Route, SetOpKind};
use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::select::{Referent, Referred};
use crate::pipeline::middle::facade::{
    Access, Chain, Continuation, GroundForm, GroundMention, InnerRelationPattern, JoinRoles,
    MemberRole, Polarity, Relation, SetOperator, Slot, StructuralForm, TupleOrdinalClause,
    TupleOrdinalOperator,
};
use super::integer::Site;

impl Elaborator<'_, '_> {
    /// Elaborate a chain in relation position.
    #[stacksafe::stacksafe]
    pub(super) fn chain(&mut self, chain: &Chain) -> Result<RelId, Refusal> {
        // A relation nested in a pivot column's value reduces its own rows.
        let column = self.column.take();
        let rel = self.term(chain).and_then(|head| self.chain_from(chain, head));
        self.column = column;
        rel
    }

    /// Elaborate a chain's steps after its head's term.
    fn chain_from(&mut self, chain: &Chain, head: Term) -> Result<RelId, Refusal> {
        self.scopes.push(Scope {
            run: OpenRun::new(),
            arms: Vec::new(),
            reversed: false,
            qualified_only: false,
            pending: Some(head),
        });
        let result = self.steps(chain);
        match result {
            Ok(()) => self.finish_scope(),
            Err(e) => {
                self.scopes.pop();
                Err(e)
            }
        }
    }

    fn steps(&mut self, chain: &Chain) -> Result<(), Refusal> {
        let mut region: Option<super::bag::Region> = None;
        let mut edges: Option<super::edge::EdgeRun> = None;
        for (at, step) in chain.steps().iter().enumerate() {
            if !super::bag::continues(step.form()) {
                region = None;
            }
            if !matches!(step.form(), Continuation::ErJoin(_)) {
                edges = None;
            }
            match step.form() {
                Continuation::Restrict { condition, .. } => {
                    self.materialize()?;
                    let roles = region.as_ref().and_then(|r| r.conjuncts(at));
                    let written = [super::bag::Role::Filter(condition)];
                    for role in roles.unwrap_or(&written) {
                        let part = match role {
                            super::bag::Role::Filter(part) => part,
                            super::bag::Role::Moved => continue,
                        };
                        let truth = self.truth(part, Consumer::Filter)?;
                        let scope = self.scopes.last_mut().expect("an open scope");
                        scope.run.push_guard(&self.b, truth);
                    }
                }
                Continuation::Correlate { .. } => {
                    if region.is_none() {
                        return Err(refuse::outside("a whole-heading correlation outside a set operation"));
                    }
                }
                Continuation::Bound { bound } => {
                    self.materialize()?;
                    let bound = self.bound_of(bound)?;
                    self.scopes
                        .last_mut()
                        .expect("an open scope")
                        .run
                        .push_bound(bound);
                }
                Continuation::Member {
                    rhs,
                    correlation,
                    join,
                } => {
                    self.materialize()?;
                    let mut term = self.term(rhs)?;
                    // A member's own continuations constrain the member: it is
                    // the relation its chain denotes, written in place (its
                    // population meets the interior fences), padded as one,
                    // its merge still the member's with the run.
                    if rhs.has_steps() {
                        let merge = std::mem::replace(&mut term.merge, MergeRequest::None);
                        term = self.chain_term(rhs, term)?;
                        if !matches!(merge, MergeRequest::None) {
                            if !matches!(term.merge, MergeRequest::None) {
                                return Err(refuse::outside("a member merged by both its own access and a later one"));
                            }
                            term.merge = merge;
                        }
                    }
                    let (marked, completes) = match join {
                        JoinRoles::Member(role) => (*role == MemberRole::Optional, false),
                        JoinRoles::AfterOptionalLead(role) => (*role == MemberRole::Optional, true),
                    };
                    self.push_term(term, marked, completes)?;
                    if let Some(correlation) = correlation {
                        let Some(condition) = correlation.condition() else {
                            return Err(refuse::outside("a member correlation that is not a condition"));
                        };
                        let truth = self.truth(condition, Consumer::Filter)?;
                        let scope = self.scopes.last_mut().expect("an open scope");
                        scope.run.push_guard(&self.b, truth);
                    }
                }
                Continuation::Pipe { operator, named } => {
                    self.materialize()?;
                    if let Some(rel) = self.key_collector_stages(operator)? {
                        self.open_after(rel, named.clone(), true);
                        continue;
                    }
                    self.stages.push(self.scopes.len() - 1);
                    let op = self.pipe_op(operator);
                    self.stages.pop();
                    let op = op?;
                    // A stage naming every item it keeps publishes no
                    // column order of its input.
                    if matches!(
                        op,
                        super::value::Stage::Op(PipeOp::Project(_) | PipeOp::Group { .. } | PipeOp::Distinct(_))
                    ) {
                        self.scopes.last_mut().expect("the stage's input").reversed = false;
                    }
                    let input = self.close_top()?;
                    let rel = match op {
                        super::value::Stage::Op(op) => self.b.pipe(input, op)?,
                        super::value::Stage::Remove(selectors) => self.b.remove(input, selectors)?,
                        super::value::Stage::Cover(pairs) => self.b.cover(input, pairs)?,
                    };
                    self.open_after(rel, named.clone(), true);
                }
                Continuation::Structural(step) => {
                    self.materialize()?;
                    let rel = match &step.form {
                        StructuralForm::Ordering { specs, bound } => {
                            self.stages.push(self.scopes.len() - 1);
                            let keys = self.order_keys_of(specs);
                            self.stages.pop();
                            let keys = keys?;
                            let input = self.close_top()?;
                            let bound = bound.as_ref().map(|b| self.bound_of(b)).transpose()?;
                            self.b.order(input, keys, bound)?
                        }
                        StructuralForm::Meta => {
                            let input = self.close_top()?;
                            self.b.meta(input)?
                        }
                        StructuralForm::Drill { drill } => {
                            let (rel, name) = self.drill(drill)?;
                            let term = Term {
                                rel,
                                scope: Some(name),
                                route: Route::Plain,
                                merge: MergeRequest::None,
                                names_scope: true,
                                requalifies: true,
                                born: Born::Written,
                            };
                            self.push_term(term, false, false)?;
                            continue;
                        }
                        // The witness reifies the existence of the rows so
                        // far as one row `met`: whether they have a row
                        // (`+`), or none (`\+`). It takes no name: only an
                        // `as` names it. The `met` of a witness of a named
                        // relation is never a bare binder another member's
                        // `met` unifies with.
                        StructuralForm::Witness { polarity } => {
                            let qualified = self
                                .scopes
                                .last()
                                .filter(|s| s.run.members().count() == 1)
                                .and_then(|s| s.run.members().next())
                                .is_some_and(|m| m.scope().is_some());
                            let input = self.close_top()?;
                            let acting = !crate::pipeline::middle::core::decide::effect::acts(&self.b, input).is_empty();
                            let rel = if acting {
                                // THE TOTAL LEDGER: `+` over receipts
                                // totalizes and keeps only `met`, one row
                                // whether its rows hold a row; `\+` whether
                                // they hold none. The act stays its chain's
                                // operand, witnessed once.
                                let witnessed = self.b.witnessed(input)?;
                                let at = self.displayed_width(witnessed) - 1;
                                let binder = self.member_over(witnessed)?;
                                let met = self.b.col(binder, at as u16);
                                let (aggregate, value) = match polarity {
                                    Polarity::Positive => ("max", met),
                                    Polarity::Negative => {
                                        let one = self.b.constant(crate::pipeline::middle::facade::LiteralValue::integer(1));
                                        ("min", self.b.infix(crate::pipeline::middle::facade::BinOp::Sub, one, met)?)
                                    }
                                };
                                let verdict = self.b.call(
                                    crate::pipeline::middle::core::node::Callee { name: aggregate.to_string() },
                                    vec![crate::pipeline::middle::core::node::Arg::Value { expr: value, distinct: false }],
                                    crate::pipeline::middle::core::decide::grade::CallPosition::Reduction,
                                    crate::pipeline::middle::core::decide::grade::Known::Aggregate,
                                    false,
                                )?;
                                let run = self.close_top()?;
                                self.b.pipe(
                                    run,
                                    PipeOp::Group {
                                        keys: Vec::new(),
                                        reductions: vec![Item {
                                            expr: verdict,
                                            naming: Naming::As(Name::new("met")),
                                        }],
                                    },
                                )?
                            } else {
                                crate::pipeline::middle::core::decide::effect::fence(&self.b, input, "a witness")?;
                                let truth = self.b.exists(matches!(polarity, Polarity::Positive), input);
                                let met = self.b.crossed(truth);
                                self.b.lit(
                                    vec![HeaderSpec::Bind(Name::new("met"))],
                                    vec![vec![met]],
                                    qualified || step.named.is_some(),
                                    &self.switches,
                                )?
                            };
                            self.scopes.push(Scope {
                                run: OpenRun::new(),
                                arms: Vec::new(),
                                reversed: false,
                                qualified_only: false,
                                pending: Some(Term {
                                    rel,
                                    born: Born::Written,
                                    scope: step.named.clone(),
                                    route: Route::Plain,
                                    merge: MergeRequest::None,
                                    names_scope: step.named.is_some(),
                                    requalifies: true,
                                }),
                            });
                            continue;
                        }
                        // THE NARROWING DESTRUCTURE: the expansion the
                        // pattern declares, payload only — its binds are the
                        // next stage's input.
                        StructuralForm::Narrow { nest, pattern } => {
                            let value = self.reference(nest)?;
                            let planned = super::pattern::narrow(pattern)?;
                            let width = planned.width();
                            let column = reference_spelled(nest);
                            let rel = self.b.expand(value, ExpansionForm::Narrow, planned.levels, &column)?;
                            self.push_term(
                                Term {
                                    rel,
                                    scope: None,
                                    route: Route::Plain,
                                    merge: MergeRequest::None,
                                    names_scope: false,
                                    requalifies: false,
                                    born: Born::Written,
                                },
                                false,
                                false,
                            )?;
                            let binder = self
                                .scopes
                                .last()
                                .and_then(|s| s.run.members().last().map(|m| m.binder()))
                                .ok_or_else(|| refuse::elaboration_contract("a narrowing with no member"))?;
                            let items = (0..width)
                                .map(|k| Item {
                                    expr: self.b.col(binder, k as u16),
                                    naming: Naming::Reference,
                                })
                                .collect();
                            let input = self.close_top()?;
                            self.b.pipe(input, PipeOp::Project(items))?
                        }
                        // A reposition is a pipe form: a projection of its
                        // input in the places it decides.
                        StructuralForm::Reposition { moves } => {
                            self.stages.push(self.scopes.len() - 1);
                            let items = self.reposition(moves);
                            self.stages.pop();
                            let items = items?;
                            let input = self.close_top()?;
                            self.b.pipe(input, PipeOp::Project(items))?
                        }
                        // The signed witness takes no name, as the witness.
                        StructuralForm::SignedWitness => {
                            let input = self.close_top()?;
                            let rel = self.b.signed_witness(input)?;
                            self.scopes.push(Scope {
                                run: OpenRun::new(),
                                arms: Vec::new(),
                                reversed: false,
                                qualified_only: false,
                                pending: Some(Term {
                                    rel,
                                    born: Born::Written,
                                    scope: step.named.clone(),
                                    route: Route::Plain,
                                    merge: MergeRequest::None,
                                    names_scope: step.named.is_some(),
                                    requalifies: true,
                                }),
                            });
                            continue;
                        }
                    };
                    let pipe = matches!(
                        step.form,
                        StructuralForm::Ordering { .. } | StructuralForm::Narrow { .. } | StructuralForm::Reposition { .. }
                    );
                    self.open_after(rel, step.named.clone(), pipe);
                }
                Continuation::BagOp { operator, .. } => {
                    self.materialize()?;
                    let opens = region.is_none();
                    let left = if opens {
                        let members: Vec<Name> = self
                            .scopes
                            .last()
                            .map(|s| s.run.members().filter_map(|m| m.scope().cloned()).collect())
                            .unwrap_or_default();
                        let single = self.scopes.last().is_some_and(|s| s.run.members().count() == 1);
                        let (left, joined) = if single {
                            (members.into_iter().next(), Vec::new())
                        } else {
                            (None, members)
                        };
                        let opened = self.region(chain, at, left, joined)?;
                        let first = opened.first();
                        region = Some(opened);
                        first
                    } else {
                        self.close_top()?
                    };
                    let region_now = region.as_ref().expect("the region this step opened or continues");
                    let right = region_now.arm(at)?;
                    let arms = region_now.arm_qualifiers().to_vec();
                    let rel = if region_now.consumed(at) {
                        // The step's correlations subtracted from the arms
                        // they name; its arms still align as a minus's.
                        crate::pipeline::middle::core::node::rel::minus_pairs(
                            self.b.rel(left).heading(),
                            self.b.rel(right).heading(),
                        )?;
                        left
                    } else {
                        let written = if opens { region_now.gated() } else { None };
                        let gate = self.gate();
                        match operator {
                            SetOperator::UnionAllPositional => self.b.set_op(left, right, SetOpKind::Positional, written, gate)?,
                            SetOperator::UnionCorresponding => self.b.set_op(left, right, SetOpKind::Corresponding, written, gate)?,
                            SetOperator::SmartUnionAll => self.b.set_op(left, right, SetOpKind::Smart, written, gate)?,
                            SetOperator::MinusCorresponding => self.b.minus(left, right)?,
                        }
                    };
                    if matches!(self.b.rel(rel).kind(), RelKind::SetOp { correlation: Some(_), .. }) {
                        self.gate_spent = true;
                    }
                    self.open_after(rel, None, false);
                    self.scopes.last_mut().expect("the scope just opened").arms = arms;
                }
                // A destructure EXPANDS: a member reading the row its value
                // stands in, binding the pattern's columns.
                Continuation::Destructure { source, pattern } => {
                    self.materialize()?;
                    let value = self.value(source, CallPosition::Value)?;
                    let planned = super::pattern::destructure(pattern)?;
                    let rel = self.b.expand(value, ExpansionForm::Destructure, planned.levels, "the destructured value")?;
                    self.push_term(
                        Term {
                            rel,
                            scope: None,
                            route: Route::Plain,
                            merge: MergeRequest::None,
                            names_scope: false,
                            requalifies: false,
                            born: Born::Written,
                        },
                        false,
                        false,
                    )?;
                }
                Continuation::Access { access, named } => self.patterned(access, named.as_ref())?,
                Continuation::ErJoin(step) => {
                    self.materialize()?;
                    self.edge_step(chain, at, step, &mut edges)?;
                }
                Continuation::Correlated(_) => {
                    return Err(refuse::outside("this continuation"));
                }
            }
        }
        Ok(())
    }

    /// End the chain: the waiting relation, if nothing joined it, is the
    /// chain's value; otherwise the run closes.
    fn finish_scope(&mut self) -> Result<RelId, Refusal> {
        let enclosed = self.scopes.len() > 1;
        let top = self.scopes.last_mut().expect("an open scope");
        if top.run.is_empty() {
            let merging = top.pending.as_ref().is_some_and(|t| !matches!(t.merge, MergeRequest::None));
            if merging && !enclosed {
                let term = top.pending.take().expect("the merging term");
                if let Err(e) = self.unanchored_merge(term.rel, &term.merge) {
                    self.scopes.last_mut().expect("an open scope").pending = Some(term);
                    return Err(e);
                }
                self.scopes.pop();
                return Ok(term.rel);
            }
            if let Some(term) = if merging { None } else { top.pending.take() } {
                self.scopes.pop();
                return Ok(term.rel);
            }
        }
        self.materialize()?;
        self.close_top()
    }

    /// Bind the waiting relation as the open run's first member.
    pub(super) fn materialize(&mut self) -> Result<(), Refusal> {
        let top = self.scopes.last_mut().expect("an open scope");
        if let Some(term) = top.pending.take() {
            self.push_term(term, false, false)?;
        }
        Ok(())
    }

    pub(super) fn push_term(&mut self, term: Term, marked: bool, completes: bool) -> Result<(), Refusal> {
        let spec = MemberSpec {
            marked,
            completes_marked_lead: completes,
            route: term.route,
            scope: term.scope,
            names_scope: term.names_scope,
            requalifies: term.requalifies,
            born: term.born,
            merge: term.merge,
        };
        let enclosed = self.scopes.len() > 1;
        let first = self.scopes.last().expect("an open scope").run.is_empty();
        let mut spec = spec;
        if first && !enclosed && !matches!(spec.merge, MergeRequest::None) {
            self.unanchored_merge(term.rel, &spec.merge)?;
            spec.merge = MergeRequest::None;
        }
        let top = self.scopes.last_mut().expect("an open scope");
        if top.run.is_empty() && !matches!(spec.merge, MergeRequest::None) {
            // An interior's head merge searches left past the interior: the
            // keys correlate the head with the enclosing row.
            let mut spec = spec;
            let merge = std::mem::replace(&mut spec.merge, MergeRequest::None);
            top.run.push_member(&mut self.b, term.rel, spec, &self.switches)?;
            return self.correlate_head(merge);
        }
        top.run.push_member(&mut self.b, term.rel, spec, &self.switches)?;
        Ok(())
    }

    /// A merge on the first relation of a run with nothing to its left
    /// unifies nothing; each key it names must still name an accessed
    /// dimension of that relation (continuation-and-access-law: `.()`
    /// operates on dimensions already accessed). Whether `.*` there refuses
    /// or is identity is not decided.
    fn unanchored_merge(&self, rel: RelId, merge: &MergeRequest) -> Result<(), Refusal> {
        use crate::pipeline::middle::core::heading::correspondence::answers_to;
        match merge {
            MergeRequest::None => Ok(()),
            MergeRequest::Natural => Err(unanchored_correlation()),
            MergeRequest::Using(keys) => {
                let heading = crate::pipeline::middle::core::graph::Arena::rel(&self.b, rel).heading();
                for key in keys {
                    if answers_to(heading, key).len() != 1 {
                        return Err(refuse::column(key.as_str(), "the relation does not publish this name once"));
                    }
                }
                Ok(())
            }
        }
    }

    /// THE MERGE OF AN INTERIOR'S HEAD (continuation-and-access-law: an
    /// interior retains the unification operation): each key the head's
    /// merge names, or every name the head shares with the enclosing rows
    /// for `.*`, is the head's position equal to the binding the leftward
    /// search finds past the interior, a condition of the head's own run.
    fn correlate_head(&mut self, merge: MergeRequest) -> Result<(), Refusal> {
        use crate::pipeline::middle::core::heading::correspondence::answers_to;
        let top = self.scopes.last().expect("the interior's run");
        let head: Vec<(Name, crate::pipeline::middle::core::node::Cell)> = top
            .run
            .outputs()
            .iter()
            .filter_map(|(p, c)| p.answering_name().map(|n| (n.clone(), *c)))
            .collect();
        let interior = self.scopes.pop().expect("the interior's run");
        let keys: Vec<Name> = match &merge {
            MergeRequest::None => Vec::new(),
            MergeRequest::Using(names) => names.clone(),
            MergeRequest::Natural => head.iter().map(|(n, _)| n.clone()).filter(|n| self.publishes(n)).collect(),
        };
        let found: Result<Vec<_>, Refusal> = keys.iter().map(|k| self.resolve_ref(super::env::Address::Bare(k))).collect();
        self.scopes.push(interior);
        let found = found?;
        if matches!(merge, MergeRequest::Natural) && keys.is_empty() {
            return Err(refuse::using_no_shared());
        }
        for (key, outer) in keys.iter().zip(found) {
            let at = answers_to(self.scopes.last().expect("the interior's run").run.heading(), key);
            let [one] = at.as_slice() else {
                return Err(refuse::column(key.as_str(), "the interior's head does not publish this name once"));
            };
            let cell = self.scopes.last().expect("the interior's run").run.outputs()[*one].1;
            let own = self.cell_value(cell);
            let own_row = self.reads_own_row(&[own, outer]);
            let equal = self.b.cmp(
                crate::pipeline::middle::facade::CmpOp::NullSafeEqual,
                own,
                outer,
                Consumer::Filter,
                own_row,
                &self.switches,
            )?;
            self.scopes.last_mut().expect("the interior's run").run.push_guard(&self.b, equal);
        }
        Ok(())
    }

    /// A chain's steps after its head's term, answering the term the chain
    /// ends as when no step binds it (a patterned boundary names and
    /// patterns that term, a postfix access states its merge), else the term
    /// of its closed run under the head's naming.
    pub(super) fn chain_term(&mut self, chain: &Chain, head: Term) -> Result<Term, Refusal> {
        let (scope, names_scope, requalifies) = (head.scope.clone(), head.names_scope, head.requalifies);
        self.scopes.push(Scope {
            run: OpenRun::new(),
            arms: Vec::new(),
            reversed: false,
            qualified_only: false,
            pending: Some(head),
        });
        if let Err(e) = self.steps(chain) {
            self.scopes.pop();
            return Err(e);
        }
        let top = self.scopes.last_mut().expect("an open scope");
        if top.run.is_empty() {
            if let Some(term) = top.pending.take() {
                self.scopes.pop();
                return Ok(term);
            }
        }
        let rel = self.finish_scope()?;
        Ok(Term {
            rel,
            scope,
            route: Route::Inline,
            merge: MergeRequest::None,
            names_scope,
            requalifies,
            born: Born::Written,
        })
    }

    /// An access written after an occurrence (`R as u(p…)`, `R(p…)`,
    /// `R.(k)`, `R.*`): over the latest nameable occurrence, the one still
    /// waiting to bind. A slot pattern reads it under the total positional
    /// slot law, named by the alias (THE PATTERNED BOUNDARY); a dequalifying
    /// access is that occurrence's merge with the run.
    fn patterned(&mut self, access: &Access, named: Option<&Name>) -> Result<(), Refusal> {
        let top = self.scopes.last_mut().expect("an open scope");
        let Some(term) = top.pending.take() else {
            return Err(refuse::outside("an access after an occurrence already bound"));
        };
        let restore = |e: &mut Self, term: Term, error: Refusal| -> Refusal {
            e.scopes.last_mut().expect("an open scope").pending = Some(term);
            error
        };
        match (access, named) {
            // A slot pattern over a merging occurrence patterns the operand;
            // its merge is then judged on the patterned heading.
            (Access::Slots(_), _) => {
                let width = self.displayed_width(term.rel);
                let spec = match self.access_spec(Some(access), width) {
                    Ok(spec) => spec,
                    Err(e) => return Err(restore(self, term, e)),
                };
                let rel = match self.b.local_read(term.rel, spec, &self.switches) {
                    Ok(rel) => rel,
                    Err(e) => return Err(restore(self, term, e)),
                };
                self.scopes.last_mut().expect("an open scope").pending = Some(Term {
                    rel,
                    scope: named.cloned(),
                    route: term.route,
                    merge: term.merge,
                    names_scope: named.is_some(),
                    requalifies: true,
                    born: Born::Written,
                });
                Ok(())
            }
            // A dequalifying access states the occurrence's merge with the
            // run; a name after it names the occurrence, whose own cells its
            // qualifier still reads (THE MERGE IS OF THE HEADING).
            (Access::Dequalify(_) | Access::DequalifyAll, named) if matches!(term.merge, MergeRequest::None) => {
                let term = match named {
                    None => term,
                    Some(name) => Term {
                        scope: Some(name.clone()),
                        names_scope: true,
                        requalifies: true,
                        born: Born::Written,
                        ..term
                    },
                };
                self.scopes.last_mut().expect("an open scope").pending = Some(Term {
                    merge: merge_request(Some(access)),
                    ..term
                });
                Ok(())
            }
            // A star on a realised occurrence is identity; a name after it
            // names the occurrence, the last name written winning.
            (Access::All, named) => {
                let term = match named {
                    None => term,
                    Some(name) => Term {
                        scope: Some(name.clone()),
                        names_scope: true,
                        requalifies: true,
                        born: Born::Written,
                        ..term
                    },
                };
                self.scopes.last_mut().expect("an open scope").pending = Some(term);
                Ok(())
            }
            _ => Err(restore(self, term, refuse::outside("this access after an occurrence"))),
        }
    }

    /// Close the top run and leave its scope: a stage consumed it.
    pub(super) fn close_top(&mut self) -> Result<RelId, Refusal> {
        let scope = self.scopes.pop().expect("an open scope");
        if scope.reversed {
            return Err(super::edge::order_held());
        }
        scope.run.close(&mut self.b)
    }

    /// Open a run whose first member is `rel`, bound now.
    pub(super) fn open_relation(&mut self, rel: RelId) -> Result<(), Refusal> {
        self.open_after(rel, None, false);
        self.materialize()
    }

    /// Open the next run with a stage's output waiting as its first member;
    /// a pipe stage's output no `as` names is what `_` reaches.
    fn open_after(&mut self, rel: RelId, named: Option<Name>, pipe: bool) {
        let names_scope = named.is_some();
        let born = if pipe && named.is_none() { Born::UnnamedStage } else { Born::Written };
        self.scopes.push(Scope {
            run: OpenRun::new(),
            arms: Vec::new(),
            reversed: false,
            qualified_only: false,
            pending: Some(Term {
                rel,
                scope: named,
                route: Route::Plain,
                merge: MergeRequest::None,
                names_scope,
                requalifies: false,
                born,
            }),
        });
    }

    /// A relational term: the head of a chain and the access its own parens
    /// ask for.
    pub(super) fn term(&mut self, chain: &Chain) -> Result<Term, Refusal> {
        match chain.head().form() {
            GroundForm::Literal(anon) => {
                let table = anon
                    .table()
                    .ok_or_else(|| refuse::outside("a receipt table"))?;
                let header: Vec<HeaderSpec> = match table.header() {
                    Some(row) => row
                        .iter()
                        .map(|item| match &item.slot {
                            Slot::Bind(binder) => Ok(HeaderSpec::Bind(binder.name.clone())),
                            Slot::Anon => Ok(HeaderSpec::Disregard),
                            // A ground term fixes the column: it constrains it
                            // and publishes nothing.
                            Slot::Constraint(term) => Ok(HeaderSpec::Constraint(self.value(term, CallPosition::Value)?)),
                            Slot::Reuse(reference) => Ok(HeaderSpec::Reuse {
                                value: self.header_reuse(reference)?,
                                name: reference.0.name.clone(),
                            }),
                        })
                        .collect::<Result<_, Refusal>>()?,
                    None => (0..table.rows().first().len()).map(|_| HeaderSpec::Anon).collect(),
                };
                let mut rows = Vec::new();
                for row in table.rows().iter() {
                    let mut cells = Vec::new();
                    for datum in row.iter() {
                        cells.push(self.value(&datum.value(), CallPosition::Value)?);
                    }
                    rows.push(cells);
                }
                let alias = anon.authored_name().cloned();
                let rel = self.b.lit(header, rows, alias.is_some(), &self.switches)?;
                let names_scope = alias.is_some();
                Ok(Term {
                    rel,
                    scope: alias,
                    route: Route::Plain,
                    merge: MergeRequest::None,
                    names_scope,
                    requalifies: true,
                    born: Born::Written,
                })
            }
            GroundForm::Reference(Relation::Ground { mention }) => {
                let GroundMention::Named {
                    identifier,
                    alias,
                    mutation_target,
                    passthrough,
                } = mention
                else {
                    return Err(refuse::outside("a compiler-built mention"));
                };
                let access = chain.head_access();
                let merge = merge_request(access);
                let (scope, names_scope) = match (access, alias) {
                    (Some(Access::Slots(_)), None) => (None, false),
                    (Some(Access::Unasked), alias) => {
                        (Some(alias.clone().unwrap_or_else(|| identifier.name.clone())), false)
                    }
                    (_, alias) => (Some(alias.clone().unwrap_or_else(|| identifier.name.clone())), true),
                };
                let rel = if *mutation_target {
                    self.marked_read(identifier, access)?
                } else {
                    self.named_read(identifier, access, *passthrough)?
                };
                Ok(Term {
                    rel,
                    scope,
                    route: Route::Plain,
                    merge,
                    names_scope,
                    requalifies: true,
                    born: Born::Written,
                })
            }
            GroundForm::Reference(Relation::InnerRelation { pattern, alias }) => {
                let InnerRelationPattern::Indeterminate {
                    identifier,
                    subquery,
                } = pattern
                else {
                    return Err(refuse::outside("a derived interior"));
                };
                self.interiors += 1;
                let built = self.interior(subquery);
                self.interiors -= 1;
                let (rel, merge) = built?;
                crate::pipeline::middle::core::decide::effect::fence(&self.b, rel, "a functor interior")?;
                // `r(+)` is the witness of `r`, which takes no name: a later
                // `r(*)` is the first scope named `r`.
                let witness = alias.is_none()
                    && subquery.steps().last().is_some_and(|step| {
                        matches!(step.form(), Continuation::Structural(s) if matches!(s.form, StructuralForm::Witness { .. }))
                    });
                Ok(Term {
                    rel,
                    scope: (!witness).then(|| alias.clone().unwrap_or_else(|| identifier.name.clone())),
                    route: Route::Inline,
                    merge,
                    names_scope: !witness,
                    requalifies: true,
                    born: Born::Written,
                })
            }
            GroundForm::Reference(Relation::FunctorCall { call, alias }) => {
                if call.is_effect() {
                    return match self.input.directive(&call.call().callee) {
                        Some(directive) => self.directive_term(call.call(), directive, chain.head_access(), alias.clone()),
                        None => self.user_directive_term(call.call(), chain.head_access(), alias.clone()),
                    };
                }
                let access = chain.head_access();
                self.application_term(call.call(), access, merge_request(access), alias.clone())
            }
            GroundForm::Reference(Relation::ConsultedView { .. }) => {
                Err(refuse::outside("a consulted view"))
            }
        }
    }

    /// A qualified anonymous-table header: the position it reuses, of a
    /// member of the run the table joins. A qualifier no member of that run
    /// answers names no owner, and a header cannot create one.
    fn header_reuse(
        &mut self,
        reference: &crate::pipeline::middle::facade::NamedReference,
    ) -> Result<crate::pipeline::middle::core::ids::ExprId, Refusal> {
        let column = &reference.0;
        let spelled = match &column.qualifier {
            Some(q) => format!("{q}.{}", column.name),
            None => column.name.to_string(),
        };
        let owned = match (&column.qualifier, self.scopes.last()) {
            (Some(qualifier), Some(scope)) if column.namespace_path.is_empty() => scope.run.answering(qualifier)?.is_some(),
            _ => false,
        };
        if !owned {
            return Err(refuse::header_qualifier(&spelled));
        }
        self.named_reference(reference)
    }

    /// A served entity's columns as the core reads them: each with the
    /// class the target's type vocabulary gives its declaration, and whether
    /// the storage serving it at `physical` computes it.
    pub(super) fn catalog_columns(
        &self,
        served: &crate::pipeline::middle::select::Served,
        physical: &crate::pipeline::middle::core::node::Physical,
    ) -> Result<Vec<crate::pipeline::middle::core::node::CatalogColumn>, Refusal> {
        let computed = self.input.computed_columns(served, physical.connection, physical.schema.as_deref())?;
        Ok(self
            .input
            .served_columns(served)?
            .into_iter()
            .map(|(name, declared, stored)| crate::pipeline::middle::core::node::CatalogColumn {
                class: self.input.declared_class(declared.as_deref()),
                computed: computed.as_ref().map(|c| c.contains(&name)),
                name,
                declared,
                stored,
            })
            .collect())
    }

    /// An interior's relation, and the merge its head requests. That merge
    /// reaches left out of the interior (continuation-and-access-law: an
    /// interior retains the unification operation): it is the interior
    /// member's merge with the run the interior stands in, and the
    /// continuations after the head narrow the member. Read so, the merge
    /// keeps its meaning only while each of them commutes with the match on
    /// the merged key: a row-wise condition does, so one is all an interior
    /// that merges may hold after its head.
    #[stacksafe::stacksafe]
    fn interior(&mut self, chain: &Chain) -> Result<(RelId, MergeRequest), Refusal> {
        let mut head = self.term(chain)?;
        let merge = std::mem::replace(&mut head.merge, MergeRequest::None);
        if matches!(merge, MergeRequest::None) {
            return Ok((self.chain_from(chain, head)?, merge));
        }
        let steps = chain.steps();
        // Later continuations act on the unified rows: the head's merge is
        // its match with the enclosing row (the head correlates as the
        // interior's first member), and what the interior publishes is what
        // its pipe publishes.
        if steps.iter().any(|step| matches!(step.form(), Continuation::Pipe { .. })) {
            head.merge = merge;
            return Ok((self.chain_from(chain, head)?, MergeRequest::None));
        }
        if steps.iter().any(|step| !matches!(step.form(), Continuation::Restrict { .. })) {
            return Err(refuse::outside("a step other than a condition inside an interior whose head merges"));
        }
        let rel = self.chain_from(chain, head)?;
        let guards: Vec<_> = match self.b.rel(rel).kind() {
            RelKind::Run(run) => run
                .quals()
                .iter()
                .filter_map(|q| match q {
                    Qual::Guard(g) => Some(g.truth()),
                    Qual::Member(_) | Qual::Bound(_) => None,
                })
                .collect(),
            _ => Vec::new(),
        };
        if guards.iter().any(|t| truth_population_sensitive(&self.b, *t)) {
            return Err(refuse::outside(
                "a condition evaluating a window or an aggregate inside an interior whose head merges",
            ));
        }
        Ok((rel, merge))
    }

    /// What a read's parens ask for, over a relation displaying `width`
    /// positions. The width is judged first: a positional pattern of the
    /// wrong width refuses by arity before any of its slots is resolved.
    pub(super) fn access_spec(&mut self, access: Option<&Access>, width: usize) -> Result<AccessSpec, Refusal> {
        Ok(match access {
            None | Some(Access::All | Access::Dequalify(_) | Access::DequalifyAll) => AccessSpec::All,
            Some(Access::Unasked) => AccessSpec::Unasked,
            Some(Access::Slots(slots)) => {
                if slots.len() != width {
                    return Err(refuse::arity(slots.len(), width));
                }
                let mut specs = Vec::with_capacity(slots.len());
                for slot in slots.iter() {
                    specs.push(match slot {
                        Slot::Bind(binder) => SlotSpec::Bind(binder.name.clone()),
                        Slot::Anon => SlotSpec::Anon,
                        // A qualified name in a slot is a reference to the
                        // column it addresses, never a new binder: the slot
                        // is constrained equal to it and publishes nothing.
                        Slot::Reuse(reference) => SlotSpec::Constraint(self.named_reference(reference)?),
                        Slot::Constraint(expr) => {
                            SlotSpec::Constraint(self.value(expr, CallPosition::Value)?)
                        }
                    });
                }
                AccessSpec::Slots(specs)
            }
        })
    }

    /// The displayed width of a relation built in this graph.
    pub(super) fn displayed_width(&self, rel: RelId) -> usize {
        self.b.rel(rel).heading().displayed().count()
    }

    /// The displayed width of a fixpoint's frontier.
    pub(super) fn frontier_width(&self, frontier: crate::pipeline::middle::core::ids::BinderId) -> usize {
        self.b.binder(frontier).heading().displayed().count()
    }

    /// ARGUMENTATIVE EXISTENCE: a polarized application `+r(a, …)` whose
    /// unqualified name selects a relation — a query-local relation, a
    /// catalog relation or fact, a stored relation — is the existence of
    /// that relation read under its arguments as slots, each constraining
    /// its position (FN.39: polarity reinterprets the application). `None`
    /// where the name selects anything else, which the sigma road reads.
    pub(super) fn cited_relation(&mut self, name: &Name, arguments: &[crate::pipeline::middle::core::ids::ExprId]) -> Result<Option<RelId>, Refusal> {
        let relation = match self.claim(name, crate::pipeline::middle::facade::QueryLocalDemand::Sigma)? {
            Some((_, kind)) => kind == crate::pipeline::middle::facade::QueryLocalKind::Relation,
            None => match self.refer(name, None)? {
                Some(Referent::Family(family)) => self.reads_as_relation(&family)?,
                Some(Referent::Served(served)) => served.kind().is_database_object(),
                Some(Referent::Declared(_)) | None => false,
            },
        };
        if !relation {
            return Ok(None);
        }
        let identifier = crate::pipeline::middle::facade::QualifiedName {
            namespace_path: crate::pipeline::middle::facade::NamespacePath::empty(),
            name: name.clone(),
        };
        let rel = self.named_read(&identifier, None, false)?;
        // THE SLOT LAW: slot count must equal the entity's column count.
        let width = self.displayed_width(rel);
        if arguments.len() != width {
            return Err(refuse::arity(arguments.len(), width));
        }
        let slots = arguments.iter().map(|a| SlotSpec::Constraint(*a)).collect();
        self.b.local_read(rel, AccessSpec::Slots(slots), &self.switches).map(Some)
    }

    /// A named relation: a query-local binding or frontier, else the
    /// catalog. A passthrough (`ns/table`) reads the target's own relation:
    /// the catalog's stored table when it holds one, and otherwise the
    /// engine's own relation (`engine_read`).
    fn named_read(
        &mut self,
        identifier: &crate::pipeline::middle::facade::QualifiedName,
        access: Option<&Access>,
        passthrough: bool,
    ) -> Result<RelId, Refusal> {
        let name = &identifier.name;
        let qualified = !identifier.namespace_path.is_empty();
        if !qualified {
            if let Some(rel) = self.formal_relation(name) {
                let access = self.access_spec(access, self.displayed_width(rel))?;
                return self.b.local_read(rel, access, &self.switches);
            }
            match self.local_relation(name)? {
                Some(Local::Body(rel)) => {
                    let access = self.access_spec(access, self.displayed_width(rel))?;
                    return self.b.local_read(rel, access, &self.switches);
                }
                Some(Local::Frontier(frontier)) => {
                    let access = self.access_spec(access, self.frontier_width(frontier))?;
                    return self.b.frontier_read(frontier, access);
                }
                None => {}
            }
        }
        let qualifier = qualified.then(|| identifier.namespace_path.qualifier());
        if let Some(rel) = self.created_read(identifier, access)? {
            return Ok(rel);
        }
        let referred = self.judge(name, qualifier.as_ref())?;
        let free = match &referred {
            Referred::Unanswered { free } => free.clone(),
            _ => None,
        };
        match referred.referent()? {
            Some(Referent::Declared(_)) if passthrough => Err(refuse::outside(
                "a passthrough read of a relation the catalog holds no stored table for (the target's own schema)",
            )),
            Some(Referent::Declared(declared)) => {
                let columns: Vec<crate::pipeline::middle::core::node::CatalogColumn> = declared
                    .columns()
                    .iter()
                    .map(|(column, declared_type)| crate::pipeline::middle::core::node::CatalogColumn {
                        class: self.input.declared_class(declared_type.as_deref()),
                        name: Name::new(column.clone()),
                        declared: declared_type.clone(),
                        computed: Some(false),
                        stored: false,
                    })
                    .collect();
                let physical = crate::pipeline::middle::core::node::Physical {
                    connection: declared.connection(),
                    schema: declared.schema().map(str::to_string),
                };
                let access = self.access_spec(access, columns.len())?;
                self.b.catalog_read(
                    declared.name().clone(),
                    declared.namespace().to_string(),
                    None,
                    columns,
                    physical,
                    false,
                    access,
                    &self.switches,
                )
            }
            Some(Referent::Served(served)) if served.kind().is_database_object() => {
                let mut physical = self.physical(&served)?;
                // A session object the statement has created under this
                // name would answer the unqualified spelling: the durable
                // read is spelled past the session pool.
                if physical.schema.is_none()
                    && self
                        .created
                        .iter()
                        .any(|c| c.session.is_some() && c.name == *served.name() && c.physical.connection == physical.connection)
                {
                    let past = self.input.past_session_schema(served.namespace(), physical.connection)?;
                    physical.schema = Some(past.ok_or_else(|| {
                        refuse::outside(
                            "a read the statement's own session object shadows, on a target with no schema spelling \
                             past its session pool",
                        )
                    })?);
                }
                let columns = self.catalog_columns(&served, &physical)?;
                let typed = self.typed(&served, &physical);
                let access = self.access_spec(access, columns.len())?;
                self.b.catalog_read(
                    served.name().clone(),
                    served.namespace().to_string(),
                    Some(served.entity_id()),
                    columns,
                    physical,
                    typed,
                    access,
                    &self.switches,
                )
            }
            Some(Referent::Served(_)) => Err(refuse::outside("an engine-served relation")),
            Some(Referent::Family(_)) | None if passthrough => self.engine_read(identifier, access),
            Some(Referent::Family(family)) => self.view_read(&family, access),
            None if self.before_acts > 0 => Err(refuse::outside(
                "a query-local definition, resolved where its block opens, reading a name the catalog does not hold \
                 while the statement's acts, which may create it, are elaborated later",
            )),
            None => match free {
                Some(world) => Err(refuse::data_hole(name.as_str(), &world)),
                None => Err(refuse::table(&self.mention_spelled(identifier))),
            },
        }
    }

}

impl Elaborator<'_, '_> {
    /// The qualifier a chain's head relation answers to, as `term` names it.
    pub(super) fn head_scope(&self, chain: &Chain) -> Option<Name> {
        match chain.head().form() {
            GroundForm::Literal(anon) => anon.authored_name().cloned(),
            GroundForm::Reference(Relation::Ground {
                mention: GroundMention::Named { identifier, alias, .. },
            })
            | GroundForm::Reference(Relation::InnerRelation {
                pattern: InnerRelationPattern::Indeterminate { identifier, .. },
                alias,
            }) => Some(alias.clone().unwrap_or_else(|| identifier.name.clone())),
            GroundForm::Reference(Relation::FunctorCall { call, alias }) => Some(
                alias
                    .clone()
                    .unwrap_or_else(|| self.input.callee_name(&call.call().callee).0),
            ),
            GroundForm::Reference(_) => None,
        }
    }

    /// A mention as its author wrote it: the qualifier, then the name.
    pub(super) fn mention_spelled(&self, identifier: &crate::pipeline::middle::facade::QualifiedName) -> String {
        match identifier.namespace_path.is_empty() {
            true => identifier.name.to_string(),
            false => format!(
                "{}.{}",
                self.input.qualifier_spelled(&identifier.namespace_path.qualifier()),
                identifier.name
            ),
        }
    }
}

/// A name correlation (`.(…)`, `.*`) written on the first relation of its
/// own run with no run to its left: what it correlates with stands outside
/// (the row of an enclosing scope, an existence's caller).
fn unanchored_correlation() -> Refusal {
    refuse::outside("a name correlation on the first relation of its run (an existence's head, or an interior standing first in its own run)")
}

/// A reference as its author wrote it, for a refusal's wording.
fn reference_spelled(reference: &crate::pipeline::middle::facade::Reference) -> String {
    match reference {
        crate::pipeline::middle::facade::Reference::Named(named) => named.0.name.to_string(),
        _ => "the narrowed value".to_string(),
    }
}

/// The merge a read's parens request.
fn merge_request(access: Option<&Access>) -> MergeRequest {
    match access {
        Some(Access::Dequalify(names)) => MergeRequest::Using(names.clone()),
        Some(Access::DequalifyAll) => MergeRequest::Natural,
        None | Some(Access::All | Access::Unasked | Access::Slots(_)) => MergeRequest::None,
    }
}

impl Elaborator<'_, '_> {
    /// A bound's row clause, each of its positions the integer the one
    /// compile-time integer decision answers for it.
    fn bound_of(&self, clause: &TupleOrdinalClause) -> Result<Bound, Refusal> {
        let offset = clause
            .offset
            .map(|offset| self.compile_time_integer(offset, Site::Offset))
            .transpose()?;
        Ok(match clause.operator {
            TupleOrdinalOperator::LessThan => Bound {
                count: Some(self.compile_time_integer(clause.value, Site::Count)?),
                offset,
            },
            TupleOrdinalOperator::GreaterThan => Bound {
                count: None,
                offset: Some(self.compile_time_integer(clause.value, Site::Offset)?),
            },
            TupleOrdinalOperator::Exactly => Bound {
                count: Some(1),
                offset: Some(self.compile_time_integer(clause.value, Site::Count)? - 1),
            },
        })
    }
}
