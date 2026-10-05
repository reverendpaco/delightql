// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Value definition calls, on both necks. A call's arguments are read
//! where it stands; each value argument is the caller's value evaluated
//! there (`Builder::argument`), each callable argument the callable written
//! there with the environment it was written in, a context marker the row
//! the call stands in. The definition's clauses are then read in its own
//! declaration environment, each binding the arguments to its own formals,
//! and the call is the first clause whose guards hold.

use super::value::{Under, Windowing};
use super::{Building, Elaborator, Form, Frame};
use crate::pipeline::middle::core::decide::grade::CallPosition;
use crate::pipeline::middle::core::heading::Name;
use crate::pipeline::middle::core::ids::{ExprId, TruthId};
use crate::pipeline::middle::core::node::expr::CaseArm;
use crate::pipeline::middle::core::node::Consumer;
use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::facade::{
    self, CallArguments, ClauseFormals, ContextMode, DdlBody, DomainExpression, FunctionApplication, HoParam,
    LexicalHorizon, ScalarArgument, SelectorItem, TruthExpression,
};

/// A callable a value definition's callable formal designates: the
/// callable as written, landed with its composition input open, and the
/// environment it was written in (an index of `Elaborator::suspended`).
#[derive(Clone)]
pub(super) struct Callable {
    landed: DomainExpression,
    env: usize,
    /// Whether the callable is a function written with no argument.
    functor: bool,
}

/// One clause of a value definition: its formals by position, its guards,
/// its body.
struct ValueClause {
    formals: Vec<Name>,
    guards: Vec<TruthExpression>,
    body: DomainExpression,
}

/// A value definition as a call reads it, whichever neck declared it.
pub(super) struct ValueDefinition {
    key: String,
    display: String,
    blocks: usize,
    frames: usize,
    horizon: Option<LexicalHorizon>,
    world: Option<usize>,
    local: bool,
    /// By position: whether the formal takes a callable.
    callable: Vec<bool>,
    context: ContextMode,
    clauses: Vec<ValueClause>,
}

/// One argument as the call supplies it.
enum Supplied {
    Value(ExprId),
    Callable(Callable),
}

impl Elaborator<'_, '_> {
    /// The query-local value definition block `block` declares under
    /// `name`.
    pub(super) fn local_value_definition(&mut self, block: usize, name: &Name) -> Result<ValueDefinition, Refusal> {
        let cfe = self.blocks[block]
            .cfes
            .iter()
            .find(|c| &c.name == name)
            .ok_or_else(|| refuse::outside("a value definition the block does not hold"))?
            .clone();
        let callable: Vec<bool> = cfe.formals.iter().map(|f| f.role == facade::CfeFormalRole::Callable).collect();
        let positional: Vec<Name> = cfe.formals.iter().map(|f| f.name.clone()).collect();
        let clauses = match &cfe.body {
            DomainExpression::Application(FunctionApplication::ClauseSelection(selection)) => selection
                .arms()
                .iter()
                .map(|arm| {
                    let ClauseFormals::Bare(entries) = &arm.formals else {
                        return Err(refuse::outside("a clause selection over marked formals"));
                    };
                    let mut formals = positional.clone();
                    for (formal, position) in entries {
                        let slot = formals
                            .get_mut(*position)
                            .ok_or_else(|| refuse::elaboration_contract("a clause formal past the definition's arguments"))?;
                        *slot = formal.clone();
                    }
                    Ok(ValueClause {
                        formals,
                        guards: arm.guard.iter().cloned().collect(),
                        body: arm.result.clone(),
                    })
                })
                .collect::<Result<Vec<_>, Refusal>>()?,
            body => vec![ValueClause {
                formals: positional,
                guards: Vec::new(),
                body: body.clone(),
            }],
        };
        Ok(ValueDefinition {
            key: self.local_key(block, &cfe.name),
            display: name.to_string(),
            blocks: block + 1,
            frames: self.blocks[block].frames,
            horizon: Some(cfe.horizon()),
            world: None,
            local: true,
            callable,
            context: cfe.context_mode.clone(),
            clauses,
        })
    }

    /// A catalog value function's clauses, each formal a plain or callable
    /// value, its guards the guards its formals carry.
    pub(super) fn catalog_value_definition(&mut self, definition: super::instances::Definition) -> Result<ValueDefinition, Refusal> {
        let mut callable: Option<Vec<bool>> = None;
        let mut context: Option<ContextMode> = None;
        let mut clauses = Vec::with_capacity(definition.clauses.len());
        for clause in &definition.clauses {
            // A consulted function captures as a query-local one does
            // (FN.38); its clauses state one capture.
            if context.as_ref().is_some_and(|known| *known != clause.head.context) {
                return Err(refuse::elaboration_contract("a value family whose clauses disagree on their capture"));
            }
            context = Some(clause.head.context.clone());
            let mut formals = Vec::new();
            let mut guards = Vec::new();
            let mut roles = Vec::new();
            for param in clause.params() {
                match param {
                    HoParam::Scalar { name, guard, callable } => {
                        formals.push(name.clone());
                        guards.extend(guard.iter().cloned());
                        roles.push(*callable);
                    }
                    HoParam::Relation { .. } | HoParam::Rule { .. } | HoParam::Ground { .. } => {
                        return Err(refuse::outside("a value function formal that is not a plain value"))
                    }
                }
            }
            if callable.as_ref().is_some_and(|known| *known != roles) {
                return Err(refuse::elaboration_contract("a value family whose clauses disagree on a formal's role"));
            }
            callable = Some(roles);
            let DdlBody::Scalar(body) = &clause.body else {
                return Err(refuse::outside("a value function whose body is not a value"));
            };
            clauses.push(ValueClause {
                formals,
                guards,
                body: body.clone(),
            });
        }
        let parts = definition.parts();
        Ok(ValueDefinition {
            key: parts.key,
            display: parts.display,
            blocks: parts.blocks,
            frames: parts.frames,
            horizon: parts.horizon,
            world: parts.world,
            local: parts.world.is_none(),
            callable: callable.unwrap_or_default(),
            context: context.unwrap_or(ContextMode::None),
            clauses,
        })
    }

    /// A catalog value function or fact function called at `at`.
    pub(super) fn catalog_call(
        &mut self,
        family: &crate::pipeline::middle::select::Family,
        arguments: &CallArguments,
        at: CallPosition,
        window: Option<Windowing>,
    ) -> Result<ExprId, Refusal> {
        let definition = self.catalog_definition(family, &[facade::DefKind::Function, facade::DefKind::FactFunction])?;
        if fact_mode(&definition).is_ok() {
            if window.is_some() {
                return Err(refuse::not_a_window(family.name().as_str(), "a DQL fact function"));
            }
            return self.fact_call(definition, arguments, None, at);
        }
        let definition = self.catalog_value_definition(definition)?;
        self.value_call(definition, arguments, at, window)
    }

    /// A value definition called at `at`: its arguments read where the call
    /// stands, then the first of its clauses whose guards hold, read in the
    /// definition's own environment with the arguments bound to that
    /// clause's formals. With no clause selected the call is NULL.
    pub(super) fn value_call(
        &mut self,
        definition: ValueDefinition,
        arguments: &CallArguments,
        at: CallPosition,
        window: Option<Windowing>,
    ) -> Result<ExprId, Refusal> {
        self.reentry(&definition.key, &definition.display)?;
        // A windowed use hands its window to the one call the body is; a
        // body that is no call computes per row and takes no window.
        let handed = match window {
            None => None,
            Some(window) => {
                let [clause] = definition.clauses.as_slice() else {
                    return Err(refuse::unruled("where a window lands in a value definition of several clauses"));
                };
                if !clause.guards.is_empty() {
                    return Err(refuse::unruled("where a window lands in a value definition of a guarded clause"));
                }
                match &clause.body {
                    DomainExpression::Application(FunctionApplication::Standard(standard)) if standard.window.is_none() => {
                        Some((standard.clone(), window))
                    }
                    _ => return Err(refuse::not_a_window(&definition.display, "a DQL value definition")),
                }
            }
        };
        let (captured, supplied) = self.call_arguments(&definition, arguments, at)?;
        let declared = definition.callable.len();
        if supplied.len() != declared {
            return Err(refuse::value_arity(&definition.display, declared + captured.named(), supplied.len() + captured.named()));
        }
        let standing: Vec<crate::pipeline::middle::core::ids::BinderId> =
            self.scopes.last().map(|s| s.run.members().map(|m| m.binder()).collect()).unwrap_or_default();
        let mut values: Vec<Option<ExprId>> = Vec::with_capacity(declared);
        let mut callables: Vec<Option<Callable>> = Vec::with_capacity(declared);
        for (position, (argument, takes_callable)) in supplied.into_iter().zip(&definition.callable).enumerate() {
            match (argument, *takes_callable) {
                (Supplied::Value(value), false) => {
                    values.push(Some(self.b.argument(value, &standing)?));
                    callables.push(None);
                }
                (Supplied::Callable(callable), true) => {
                    values.push(None);
                    callables.push(Some(callable));
                }
                (Supplied::Value(_), true) => return Err(refuse::callable_expected(&definition.display, position + 1)),
                (Supplied::Callable(_), false) => return Err(refuse::code_argument(&definition.display)),
            }
        }
        let env = self.suspended.len();
        let context = matches!(captured, Captured::Row).then_some(env);
        let named_captures = match captured {
            Captured::Named(named) => named,
            Captured::None | Captured::Row => Vec::new(),
        };
        let mut arms: Vec<(CaseArm, ExprId)> = Vec::new();
        let mut fallback = None;
        self.building.push(Building {
            definition: definition.key.clone(),
            display: definition.display.clone(),
            local: definition.local,
            form: Form::Value,
            actuals: Vec::new(),
            stands_in: false,
            frontier: None,
        });
        let read = (|| -> Result<(), Refusal> {
            for clause in &definition.clauses {
                let mut frame = Frame {
                    named: named_captures.clone(),
                    context,
                    captures: definition.context != ContextMode::None,
                    ..Frame::default()
                };
                for (k, formal) in clause.formals.iter().enumerate() {
                    if let Some(Some(value)) = values.get(k) {
                        frame.named.push((formal.clone(), *value));
                    }
                    if let Some(Some(callable)) = callables.get(k) {
                        frame.callables.push((formal.clone(), callable.clone()));
                    }
                }
                let (guard, result) = self.within_value_definition(&definition, frame, |e| {
                    let mut truths: Vec<TruthId> = Vec::with_capacity(clause.guards.len());
                    for guard in &clause.guards {
                        truths.push(e.truth(guard, Consumer::Value)?);
                    }
                    let guard = match truths.len() {
                        0 => None,
                        1 => Some(truths[0]),
                        _ => Some(e.b.and(truths)),
                    };
                    let result = match &handed {
                        Some((standard, window)) => e.standard_under(standard, at, Under::Handed(window.clone()))?,
                        None => e.value(&clause.body, at)?,
                    };
                    Ok((guard, result))
                })?;
                match guard {
                    Some(guard) => arms.push((CaseArm::Truth(guard), result)),
                    None => {
                        fallback = Some(result);
                        break;
                    }
                }
            }
            Ok(())
        })();
        self.building.pop();
        read?;
        match (arms.is_empty(), fallback) {
            (true, Some(only)) => Ok(only),
            (_, fallback) => self.b.case(None, arms, fallback, &self.switches),
        }
    }

    /// Run `f` in a value definition's declaration environment with
    /// `frame` open.
    fn within_value_definition<T>(
        &mut self,
        definition: &ValueDefinition,
        frame: Frame,
        f: impl FnOnce(&mut Self) -> Result<T, Refusal>,
    ) -> Result<T, Refusal> {
        let mut environment = super::instances::Definition::environment(
            definition.key.clone(),
            definition.display.clone(),
            definition.blocks,
            definition.frames,
            definition.horizon,
            definition.world,
        );
        self.within_definition(&mut environment, frame, f)
    }

    /// A call's arguments as the definition's formals receive them, after
    /// its capture: a context marker captures the row the call stands in
    /// (implicitly, or the declared columns by name); a declared capture
    /// written without one takes the call's first arguments by position.
    fn call_arguments(
        &mut self,
        definition: &ValueDefinition,
        arguments: &CallArguments,
        at: CallPosition,
    ) -> Result<(Captured, Vec<Supplied>), Refusal> {
        let written: Vec<&ScalarArgument> = match arguments {
            CallArguments::None => Vec::new(),
            CallArguments::Scalar(args) => args.iter().collect(),
            CallArguments::HigherOrder(_) => return Err(refuse::outside("a higher-order call in value position")),
        };
        let marked = matches!(written.first(), Some(ScalarArgument::Context(_)));
        if written.iter().skip(1).any(|a| matches!(a, ScalarArgument::Context(_))) {
            return Err(refuse::context_marker_position(&definition.display));
        }
        // A function body has no row of its own: a `..` call written
        // anywhere in a context function's body refuses, nested relations
        // included, as one directly in a value body does.
        if marked && (self.scopes.is_empty() || self.frames.last().is_some_and(|f| f.captures)) {
            return Err(refuse::context_without_row(&definition.display));
        }
        let rest = if marked { &written[1..] } else { &written[..] };
        // A declared capture written without `..` takes the first arguments.
        let offset = match (&definition.context, marked) {
            (ContextMode::Explicit(names), false) => names.len(),
            _ => 0,
        };
        let mut supplied = Vec::with_capacity(rest.len());
        for argument in rest {
            let takes_callable = supplied
                .len()
                .checked_sub(offset)
                .and_then(|k| definition.callable.get(k))
                .copied()
                .unwrap_or(false);
            self.supplied(argument, takes_callable, at, &mut supplied)?;
        }
        let captured = match (&definition.context, marked) {
            (ContextMode::Explicit(names), _) if names.is_empty() => return Err(refuse::empty_capture(&definition.display)),
            (ContextMode::None, false) => Captured::None,
            (ContextMode::None, true) => return Err(refuse::context_marker_position(&definition.display)),
            (ContextMode::Implicit, true) => Captured::Row,
            (ContextMode::Implicit, false) => return Err(refuse::implicit_capture_positional(&definition.display)),
            (ContextMode::Explicit(names), true) => {
                let mut named = Vec::with_capacity(names.len());
                for name in names {
                    named.push((name.clone(), self.resolve_ref(super::env::Address::Bare(name))?));
                }
                Captured::Named(named)
            }
            (ContextMode::Explicit(names), false) => {
                if supplied.len() < names.len() {
                    return Err(refuse::value_arity(
                        &definition.display,
                        names.len() + definition.callable.len(),
                        supplied.len(),
                    ));
                }
                let mut named = Vec::with_capacity(names.len());
                for (name, argument) in names.iter().zip(supplied.drain(..names.len())) {
                    let Supplied::Value(value) = argument else {
                        return Err(refuse::code_argument(&definition.display));
                    };
                    named.push((name.clone(), value));
                }
                Captured::Named(named)
            }
        };
        Ok((captured, supplied))
    }

    /// One written argument, as what it supplies: a value, the values a
    /// spread covers in the heading the call sees, or a callable. Where the
    /// formal takes a callable, a function written with no arguments
    /// (`upper:()`) and an open template (`:"<{@}>"`) are callables (FN.4).
    fn supplied(
        &mut self,
        argument: &ScalarArgument,
        takes_callable: bool,
        at: CallPosition,
        out: &mut Vec<Supplied>,
    ) -> Result<(), Refusal> {
        match argument {
            ScalarArgument::Value(value) if takes_callable && !value.distinct => match &value.value {
                DomainExpression::Application(FunctionApplication::Standard(standard))
                    if no_arguments(&standard.call.call().arguments)
                        && standard.window.is_none()
                        && standard.guard.is_none() =>
                {
                    out.push(Supplied::Callable(self.callable_actual(&facade::Callable::Functor(standard.clone()))?))
                }
                DomainExpression::Application(FunctionApplication::Template(template)) => {
                    out.push(Supplied::Callable(self.callable_actual(&facade::Callable::String(template.clone()))?))
                }
                other => out.push(Supplied::Value(self.value(other, at)?)),
            },
            ScalarArgument::Value(value) if !value.distinct => out.push(Supplied::Value(self.value(&value.value, at)?)),
            ScalarArgument::Value(_) => return Err(refuse::outside("a distinct argument to a value definition")),
            ScalarArgument::Callable(callable) => out.push(Supplied::Callable(self.callable_actual(callable)?)),
            ScalarArgument::Spread(facade::Spread::Glob(glob)) if glob.qualifier.is_none() => {
                return Err(refuse::unruled("what a bare glob supplies to a value definition's arguments"))
            }
            ScalarArgument::Spread(spread) => {
                for column in self.addressed(&[SelectorItem::Spread(spread.clone())])? {
                    let value = self.cell_value(column.cell);
                    out.push(Supplied::Value(value));
                }
            }
            ScalarArgument::Star => return Err(refuse::unruled("what a bare glob supplies to a value definition's arguments")),
            ScalarArgument::Context(_) => return Err(refuse::outside("a context marker past the first argument")),
        }
        Ok(())
    }

    /// The callable a callable argument designates: a callable formal of
    /// the body the call stands in, written bare, forwards the callable it
    /// holds; any other is the callable as written, in the environment it
    /// is written in.
    fn callable_actual(&mut self, callable: &facade::Callable) -> Result<Callable, Refusal> {
        if let facade::Callable::Functor(application) = callable {
            let call = application.call.call();
            let (name, qualifier) = self.input.callee_name(&call.callee);
            if qualifier.is_none() && no_arguments(&call.arguments) {
                if let Some(held) = self.callable_formal(&name) {
                    return Ok(held);
                }
            }
        }
        Ok(Callable {
            landed: self.input.cover_application(callable)?,
            env: self.suspended.len(),
            functor: matches!(callable, facade::Callable::Functor(application) if no_arguments(&application.call.call().arguments)),
        })
    }

    /// The callable a callable formal of the body being read designates.
    pub(super) fn callable_formal(&self, name: &Name) -> Option<Callable> {
        self.frames
            .iter()
            .rev()
            .find_map(|f| f.callables.iter().find(|(n, _)| n == name).map(|(_, c)| c.clone()))
    }

    /// A callable formal applied in a body: its one argument read where
    /// the application stands, then the callable read where it was written,
    /// with that argument flowing into its composition input.
    pub(super) fn apply_callable(
        &mut self,
        name: &Name,
        callable: Callable,
        arguments: &CallArguments,
        at: CallPosition,
    ) -> Result<ExprId, Refusal> {
        let mut values = Vec::new();
        for argument in self.plain_values(arguments, at, "a callable formal")? {
            values.push(argument);
        }
        let [flowing] = values.as_slice() else {
            if callable.functor {
                return Err(refuse::outside("a function passed as a callable applied to several values"));
            }
            return Err(refuse::lambda_arity(name.as_str(), values.len()));
        };
        let flowing = *flowing;
        self.in_suspended(callable.env, Some(flowing), |e| e.value(&callable.landed, at))
    }

    /// A callable formal applied under a window spec its body writes
    /// (`partition`, `order` and `frame`, values of the body): the callable
    /// must be a target function written with no argument, applied where
    /// it was written with the call's one value flowing in, and windowed
    /// there.
    pub(super) fn windowed_callable(
        &mut self,
        name: &Name,
        callable: Callable,
        arguments: &CallArguments,
        (partition, order, frame): Windowing,
    ) -> Result<crate::pipeline::middle::core::ids::ExprId, Refusal> {
        let values = self.plain_values(arguments, CallPosition::Value, "a callable formal")?;
        let [flowing] = values.as_slice() else {
            if callable.functor {
                return Err(refuse::outside("a function passed as a callable applied to several values"));
            }
            return Err(refuse::lambda_arity(name.as_str(), values.len()));
        };
        let DomainExpression::Application(facade::FunctionApplication::Standard(standard)) = &callable.landed else {
            return Err(refuse::outside("a callable that is not a target function, applied under a window"));
        };
        if standard.window.is_some() || standard.guard.is_some() {
            return Err(refuse::outside("a windowed or guarded callable applied under a window"));
        }
        let call = standard.call.call();
        let (callee, qualifier) = self.input.callee_name(&call.callee);
        let callee = callee.into_inner();
        if qualifier.is_some() {
            return Err(refuse::outside("a qualified callable applied under a window"));
        }
        let args = self.in_suspended(callable.env, Some(*flowing), |e| e.arguments(&call.arguments, CallPosition::Value))?;
        if self.input.known_call(&callee, args.len()) == facade::KnownCall::Scalar {
            return Err(refuse::not_a_window(&callee, "a scalar function"));
        }
        self.b.window(
            crate::pipeline::middle::core::node::Callee { name: callee },
            args,
            partition,
            order,
            frame,
        )
    }

    /// Run `f` in the environment a body suspended (`env`), with `flowing`
    /// standing for the composition input.
    pub(super) fn in_suspended<T>(
        &mut self,
        env: usize,
        flowing: Option<ExprId>,
        f: impl FnOnce(&mut Self) -> Result<T, Refusal>,
    ) -> Result<T, Refusal> {
        let Some(entry) = self.suspended.get_mut(env) else {
            return Err(refuse::elaboration_contract("a callable whose environment is no longer suspended"));
        };
        let mut scopes = std::mem::take(&mut entry.scopes);
        let mut interiors = entry.interiors;
        let mut stages = std::mem::take(&mut entry.stages);
        let blocks = entry.blocks.clone();
        let kept = entry.frames_kept;
        let mut hidden = std::mem::take(&mut entry.hidden_frames);
        let mut column_self = entry.column_self;
        let mut at = entry.at.clone();
        std::mem::swap(&mut self.scopes, &mut scopes);
        std::mem::swap(&mut self.interiors, &mut interiors);
        std::mem::swap(&mut self.stages, &mut stages);
        let body_blocks: Vec<(bool, LexicalHorizon)> = self.blocks.iter().map(|b| (b.hidden, b.horizon)).collect();
        for (block, (h, horizon)) in self.blocks.iter_mut().zip(&blocks) {
            block.hidden = *h;
            block.horizon = *horizon;
        }
        let body_frames = self.frames.split_off(kept.min(self.frames.len()));
        self.frames.append(&mut hidden);
        std::mem::swap(&mut self.column_self, &mut column_self);
        let body_flowing = std::mem::replace(&mut self.flowing, flowing);
        std::mem::swap(&mut self.at, &mut at);
        let out = f(self);
        std::mem::swap(&mut self.at, &mut at);
        self.flowing = body_flowing;
        std::mem::swap(&mut self.column_self, &mut column_self);
        let mut hidden = self.frames.split_off(kept.min(self.frames.len()));
        self.frames.extend(body_frames);
        for (block, (h, horizon)) in self.blocks.iter_mut().zip(body_blocks) {
            block.hidden = h;
            block.horizon = horizon;
        }
        std::mem::swap(&mut self.stages, &mut stages);
        std::mem::swap(&mut self.interiors, &mut interiors);
        std::mem::swap(&mut self.scopes, &mut scopes);
        let entry = &mut self.suspended[env];
        entry.scopes = scopes;
        entry.interiors = interiors;
        entry.stages = stages;
        entry.hidden_frames = std::mem::take(&mut hidden);
        let _ = column_self;
        out
    }

    /// A bare name a context function's body reads that no formal and no
    /// scope of the body answers: the row its call stands in, as captured.
    pub(super) fn captured_name(
        &mut self,
        name: &Name,
        ambiguous: fn(&Name) -> Refusal,
    ) -> Result<Option<ExprId>, Refusal> {
        let Some(env) = self.frames.last().and_then(|f| f.context) else {
            return Ok(None);
        };
        let Some(entry) = self.suspended.get_mut(env) else {
            return Err(refuse::elaboration_contract("a capture whose row is no longer suspended"));
        };
        let mut scopes = std::mem::take(&mut entry.scopes);
        std::mem::swap(&mut self.scopes, &mut scopes);
        let found = self.resolve_in_scopes(name, ambiguous);
        std::mem::swap(&mut self.scopes, &mut scopes);
        self.suspended[env].scopes = scopes;
        found
    }
}

/// Whether a call is written with no argument: `f:()`.
fn no_arguments(arguments: &CallArguments) -> bool {
    match arguments {
        CallArguments::None => true,
        CallArguments::Scalar(args) => args.is_empty(),
        CallArguments::HigherOrder(_) => false,
    }
}

/// What a call's context marker captured.
enum Captured {
    None,
    /// The row the call stands in: the body's free names read it.
    Row,
    /// The declared columns, each by name.
    Named(Vec<(Name, ExprId)>),
}

impl Captured {
    fn named(&self) -> usize {
        match self {
            Captured::Named(named) => named.len(),
            Captured::None | Captured::Row => 0,
        }
    }
}

impl Elaborator<'_, '_> {
    /// A fact function called at `at`: its arms in authored order as one
    /// case over its inputs, each input met null-safely by the arm's ground
    /// term (a mention by its encoding), its default the case's default; a
    /// call no arm and no default answers is NULL. `field` selects one
    /// declared output; with none, the function must declare exactly one.
    pub(super) fn fact_call(
        &mut self,
        definition: super::instances::Definition,
        arguments: &CallArguments,
        field: Option<&Name>,
        at: CallPosition,
    ) -> Result<ExprId, Refusal> {
        let mode = fact_mode(&definition)?;
        let display = definition.parts().display;
        let outputs: Vec<String> = mode.outputs.iter().map(|o| o.to_string()).collect();
        let output = match field {
            Some(name) => {
                mode.output_position(name).ok_or_else(|| refuse::mode_unknown_output(&display, name.as_str(), &outputs))?
            }
            None if mode.outputs.len() == 1 => 0,
            None => return Err(refuse::mode_degree(&display, &outputs)),
        };
        let written: Vec<&ScalarArgument> = match arguments {
            CallArguments::None => Vec::new(),
            CallArguments::Scalar(args) => args.iter().collect(),
            CallArguments::HigherOrder(_) => return Err(refuse::outside("a higher-order call in value position")),
        };
        let mut supplied = Vec::with_capacity(written.len());
        for argument in written {
            self.supplied(argument, false, at, &mut supplied)?;
        }
        if supplied.len() != mode.inputs.len() {
            return Err(refuse::fact_arity(&display, mode.inputs.len(), supplied.len()));
        }
        let standing: Vec<crate::pipeline::middle::core::ids::BinderId> =
            self.scopes.last().map(|s| s.run.members().map(|m| m.binder()).collect()).unwrap_or_default();
        let mut inputs = Vec::with_capacity(supplied.len());
        for argument in supplied {
            let Supplied::Value(value) = argument else {
                return Err(refuse::code_argument(&display));
            };
            inputs.push(self.b.argument(value, &standing)?);
        }
        let parts = definition.parts();
        let mut environment =
            super::instances::Definition::environment(parts.key, parts.display, parts.blocks, parts.frames, parts.horizon, parts.world);
        // An arm's outputs, and the default's, read the inputs by their
        // declared names.
        let named: Vec<(Name, ExprId)> = mode.inputs.iter().cloned().zip(inputs.iter().copied()).collect();
        let frame = || Frame {
            named: named.clone(),
            ..Frame::default()
        };
        let mut arms: Vec<(CaseArm, ExprId)> = Vec::with_capacity(mode.arms.len());
        for arm in mode.arms.iter() {
            let mut matched = Vec::with_capacity(inputs.len());
            for (input, term) in inputs.iter().zip(arm.inputs.iter()) {
                let term = self.b.constant(term.clone());
                matched.push(self.b.cmp(facade::CmpOp::NullSafeEqual, *input, term, Consumer::Value, true, &self.switches)?);
            }
            let matched = match matched.len() {
                1 => matched[0],
                _ => self.b.and(matched),
            };
            let result = arm
                .outputs
                .get(output)
                .ok_or_else(|| refuse::elaboration_contract("an arm with fewer outputs than its mode"))?
                .clone();
            let result = self.within_definition(&mut environment, frame(), |e| e.value(&result, at))?;
            arms.push((CaseArm::Truth(matched), result));
        }
        let default = match &mode.default {
            Some(outputs) => {
                let result = outputs
                    .get(output)
                    .ok_or_else(|| refuse::elaboration_contract("a default with fewer outputs than its mode"))?
                    .clone();
                Some(self.within_definition(&mut environment, frame(), |e| e.value(&result, at))?)
            }
            None => None,
        };
        self.b.case(None, arms, default, &self.switches)
    }

    /// A fact function read as a relation: a family with no default has a
    /// finite face, its arms as rows of its inputs then its outputs; one
    /// with a default has none.
    pub(super) fn fact_face(
        &mut self,
        definition: super::instances::Definition,
        access: Option<&facade::Access>,
    ) -> Result<crate::pipeline::middle::core::ids::RelId, Refusal> {
        use crate::pipeline::middle::core::node::rel::{AccessSpec, HeaderSpec};
        let mode = fact_mode(&definition)?;
        let parts = definition.parts();
        if mode.default.is_some() {
            return Err(refuse::fact_relational_face(&parts.display));
        }
        let mut environment = super::instances::Definition::environment(
            parts.key,
            parts.display,
            parts.blocks,
            parts.frames,
            parts.horizon,
            parts.world,
        );
        let header: Vec<HeaderSpec> = mode.heading().into_iter().map(HeaderSpec::Bind).collect();
        let mut rows = Vec::with_capacity(mode.arms.len());
        for arm in mode.arms.iter() {
            let mut row: Vec<ExprId> = arm.inputs.iter().map(|t| self.b.constant(t.clone())).collect();
            for output in arm.outputs.iter() {
                let output = output.clone();
                row.push(self.within_definition(&mut environment, Frame::default(), |e| e.value(&output, CallPosition::Value))?);
            }
            rows.push(row);
        }
        let rel = self.b.lit(header, rows, false, &self.switches)?;
        let width = mode.inputs.len() + mode.outputs.len();
        match self.access_spec(access, width)? {
            AccessSpec::All => Ok(rel),
            access @ (AccessSpec::Unasked | AccessSpec::Slots(_)) => self.b.local_read(rel, access, &self.switches),
        }
    }

    /// `f:(…).out`: one declared output of a fact function's call. A
    /// function that declares no mode has no output to select.
    pub(super) fn field_select(&mut self, select: &facade::FieldSelect, at: CallPosition) -> Result<ExprId, Refusal> {
        let call = select.application.call.call();
        let (local, qualifier) = self.input.callee_name(&call.callee);
        let name = local.as_str().to_string();
        let referent = match &qualifier {
            Some(q) => self.refer(&local, Some(q))?,
            None if self.claim(&local, facade::QueryLocalDemand::Value)?.is_some() || self.callable_formal(&local).is_some() => {
                return Err(refuse::mode_undeclared(&name))
            }
            None => self.refer(&local, None)?,
        };
        match referent {
            Some(crate::pipeline::middle::select::Referent::Family(family)) => {
                let definition = self.catalog_definition(&family, &[facade::DefKind::Function, facade::DefKind::FactFunction])?;
                if fact_mode(&definition).is_err() {
                    return Err(refuse::mode_undeclared(&name));
                }
                self.fact_call(definition, &call.arguments, Some(&select.field.name), at)
            }
            _ => Err(refuse::mode_undeclared(&name)),
        }
    }
}

/// The declared mode of a fact function's one clause.
fn fact_mode(definition: &super::instances::Definition) -> Result<facade::FactFunctionMode, Refusal> {
    match definition.clauses.as_slice() {
        [clause] => match &clause.body {
            DdlBody::FactFunction(declared) => Ok(declared.mode().clone()),
            _ => Err(refuse::outside("a function declaring no mode")),
        },
        _ => Err(refuse::outside("a fact function of several declarations")),
    }
}
