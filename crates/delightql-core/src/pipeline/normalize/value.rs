// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Value position — what evaluates to one value per tuple.
//!
//! Position is grammatical here, never re-decided from content: a `_` reaching
//! this module is the disregarded anaphor because the grammar put it under
//! `domain_hole`, and the deictic stage is a different member under
//! `qualifier`. The glyph never classifies.
//!
//! Two normalizations that happen HERE and nowhere else:
//!
//! - **ONE TEMPLATE PARSE.** The CST has one template form. It is classified
//!   once by CONTENT — zero interpolations is a ground string, interpolations
//!   without the flowing value are a template, one with it is an open string —
//!   and no downstream consumer rescans the parts.
//! - **The citation.** `:f` is the OTHER nullary; it normalizes to the
//!   zero-argument application and is never ground.

use super::landing;
use super::Normalizer;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::expressions::ValueTemplatePart;
use crate::pipeline::asts::core::ArgumentValue;
use crate::pipeline::asts::core::Callable;
use crate::pipeline::asts::core::{
    AuthoredColumn, ColumnOrdinal, ColumnRange, DomainExpression, Enclyph, FunctionApplication,
    FunctorCall, Glob, LiteralValue, NamespacePath, Path, PathStep, PureCall, Record, RecordMember,
    RegexSelector, Spread, StandardApplication, Tuple, Unresolved,
};
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::vocabulary::Vec1;
use crate::pipeline::syntax::cst;
use delightql_types::SqlIdentifier;

type Domex = DomainExpression<Unresolved>;
type Argument = crate::pipeline::asts::core::operators::ScalarArgument<Unresolved>;

impl<'t> Normalizer<'t> {
    // -----------------------------------------------------------------
    // The value spine
    // -----------------------------------------------------------------

    #[stacksafe::stacksafe]
    pub(crate) fn domain_expression(&mut self, node: cst::DomainExpression<'t>) -> Result<Domex> {
        match node {
            cst::DomainExpression::Reference(reference) => self.reference_expression(reference),
            cst::DomainExpression::FunctionApplication(application) => {
                self.function_application(application)
            }
        }
    }

    /// A binder interior: the body of a lambda, a function-pipe step, a sparse
    /// fill, a slot list. The two holes are DIFFERENT carriers per level, and
    /// this is the value level's.
    pub(crate) fn open_expression(&mut self, node: cst::OpenExpression<'t>) -> Result<Domex> {
        match node {
            cst::OpenExpression::DomainExpression(expression) => self.domain_expression(expression),
            cst::OpenExpression::DomainHole(hole) => Ok(self.domain_hole(hole)),
        }
    }

    /// `@` is what flows in; `_` is the disregarded. Both live to
    /// instantiation — the value the composition input stands for is
    /// supplied when the open body is applied, not before: by the function
    /// pipe at build, by a definition at its call site, by a cover per
    /// cell, by a companion cell at the column it constrains.
    fn domain_hole(&self, node: cst::DomainHole<'t>) -> Domex {
        match node {
            cst::DomainHole::CompositionInput(_) => {
                DomainExpression::Application(FunctionApplication::Open(
                    crate::pipeline::asts::core::DomainHole::CompositionInput,
                ))
            }
            cst::DomainHole::Disregarded(_) => DomainExpression::Application(
                FunctionApplication::Open(crate::pipeline::asts::core::DomainHole::Disregarded),
            ),
        }
    }

    /// A reference in VALUE position. A scalar FORMAL standing here stays a
    /// reference: the body's formal frame — the caller-resolved actual —
    /// answers it at resolution, so no caller syntax is spliced into the
    /// body. Everywhere a reference addresses a column and nothing else, the
    /// position reads `reference` instead.
    pub(crate) fn reference_expression(&mut self, node: cst::Reference<'t>) -> Result<Domex> {
        if let cst::Reference::NamedReference(reference) = node {
            let column = self.authored_column(reference)?;
            return Ok(DomainExpression::Reference(Reference::Named(
                NamedReference(column),
            )));
        }
        Ok(DomainExpression::Reference(self.column_reference(node)?))
    }

    /// The one reading of an authored reference: a name or a position, asking
    /// the same addressing question.
    pub(crate) fn column_reference(
        &mut self,
        node: cst::Reference<'t>,
    ) -> Result<Reference<Unresolved>> {
        match node {
            cst::Reference::NamedReference(reference) => Ok(Reference::Named(NamedReference(
                self.authored_column(reference)?,
            ))),
            cst::Reference::PositionalReference(ordinal) => {
                let ordinal =
                    self.require(ordinal.child(), "a positional reference has an ordinal")?;
                Ok(Reference::Ordinal(self.column_ordinal(ordinal)?))
            }
        }
    }

    /// A written column reference: characters, and nothing else. Whether a
    /// qualifier names a table, an alias, or the unnamed stage is a
    /// resolution question; what is decided here is that a qualifier was
    /// written at all.
    pub(crate) fn authored_column(&self, node: cst::NamedReference<'t>) -> Result<AuthoredColumn> {
        let name = self.require(node.name(), "a named reference has a name")?;
        let qualifier = node
            .qualifier()
            .map(|qualifier| self.qualifier(qualifier).map(|q| q.spelling()))
            .transpose()?;
        // ONE reader, so the refusal is the same wherever a qualified column
        // may be written.
        if let Some(refused) = node.refused_segment() {
            let refused = self.text(refused);
            let qualifier = qualifier.as_ref().map_or("", |q| q.as_str());
            let name = self.text(name);
            return Err(DelightQLError::validation_error_categorized(
                "reference/multi_segment_qualifier",
                format!(
                    "'{refused}.{qualifier}.{name}' carries two qualifiers — a column \
                     reference takes at most one, and only the segment next to the column \
                     name is ever read"
                ),
                format!(
                    "write '{qualifier}.{name}'; to reach another namespace, qualify the \
                     relation instead"
                ),
            ));
        }
        Ok(AuthoredColumn {
            name: self.identifier(name),
            qualifier,
            namespace_path: NamespacePath::empty(),
        })
    }

    fn column_ordinal(&mut self, node: cst::Ordinal<'t>) -> Result<ColumnOrdinal> {
        let inner = self.require(node.child(), "an ordinal has a position")?;
        let position = self.compile_time_integer(inner, "a column ordinal")?;
        let reverse = position < 0;
        Ok(ColumnOrdinal {
            position: position.unsigned_abs().try_into().map_err(|_| {
                DelightQLError::parse_error(format!("column ordinal |{position}| is out of range"))
            })?,
            reverse,
            qualifier: self.written_qualifier(node.qualifier())?,
            namespace_path: NamespacePath::empty(),
            // The whole-heading spelling `q|*|` is `positional_heading`, a
            // truth form of its own; nothing here can produce one.
            glob: false,
        })
    }

    /// The scope a POSITIONAL addressing form was written against, if one
    /// was. A qualifier is an ADDRESS, so position carries the same optional
    /// one a name does and reads it in the same place. The positional
    /// carriers hold a spelling rather than an identity, so the strop is
    /// resolved here — the interior, never the backticks.
    /// The qualifier a reference wrote, as written. The spelling carries
    /// its own strop, and flattening it to characters here is what makes a
    /// case-sensitive scope unreachable through an ordinal.
    fn written_qualifier(
        &mut self,
        node: Option<cst::Qualifier<'t>>,
    ) -> Result<Option<SqlIdentifier>> {
        node.map(|qualifier| self.qualifier(qualifier).map(|q| q.spelling().clone()))
            .transpose()
    }

    // -----------------------------------------------------------------
    // Spreads — the multi-domex
    // -----------------------------------------------------------------

    /// THE SPREAD IS A MULTI-DOMEX: an authored multi-reference that EXPANDS
    /// at resolution into the columns it addresses. It computes no value, so
    /// there is no scalar node under it to hang a name on.
    pub(crate) fn spread(&mut self, node: cst::Spread<'t>) -> Result<Spread<Unresolved>> {
        match node {
            cst::Spread::Glob(glob) => Ok(Spread::Glob(self.glob(glob)?)),
            cst::Spread::Regex(regex) => Ok(Spread::Regex(RegexSelector::new(
                regex_interior(self.text(regex)).to_string(),
            ))),
            cst::Spread::PositionalSpan(span) => {
                Ok(Spread::PositionalSpan(self.column_range(span)?))
            }
        }
    }

    pub(crate) fn glob(&mut self, node: cst::Glob<'t>) -> Result<Glob> {
        let qualifier = node
            .qualifier()
            .map(|qualifier| self.qualifier(qualifier).map(|q| q.spelling()))
            .transpose()?;
        Ok(Glob {
            qualifier,
            namespace_path: NamespacePath::empty(),
            authored: (),
        })
    }

    fn column_range(&mut self, node: cst::PositionalSpan<'t>) -> Result<ColumnRange> {
        let endpoint =
            |normalizer: &Self, slot: Option<cst::Number<'t>>| -> Result<Option<(u16, bool)>> {
                slot.map(|number| {
                    let text = normalizer.text(number);
                    let (digits, reverse) = match text.strip_prefix('-') {
                        Some(rest) => (rest, true),
                        None => (text, false),
                    };
                    digits
                        .parse::<u16>()
                        .map(|position| (position, reverse))
                        .map_err(|_| {
                            DelightQLError::parse_error(format!("invalid span endpoint: {text}"))
                        })
                })
                .transpose()
            };
        Ok(ColumnRange {
            start: endpoint(self, node.start())?,
            end: endpoint(self, node.end())?,
            qualifier: self.written_qualifier(node.qualifier())?,
            namespace_path: NamespacePath::empty(),
        })
    }

    // -----------------------------------------------------------------
    // Applications
    // -----------------------------------------------------------------

    fn function_application(&mut self, node: cst::FunctionApplication<'t>) -> Result<Domex> {
        match node {
            cst::FunctionApplication::InfixOperator(infix) => self.infix(infix),
            cst::FunctionApplication::NonInfixApplication(application) => {
                self.non_infix_application(application)
            }
            // The whole-value stratum of the crossing: an infix truth read
            // as a value. Same mint as the operand stratum.
            cst::FunctionApplication::CrossedTruth(crossed) => self.crossed_truth(crossed),
        }
    }

    /// NO PEMDAS, structurally: an operand derives no infix form, so there is
    /// no precedence to apply and no associativity to remember.
    fn infix(&mut self, node: cst::InfixOperator<'t>) -> Result<Domex> {
        let mut operands = Vec::new();
        let mut operator = None;
        for child in node.children() {
            match child {
                cst::InfixOperatorChild::Operand(operand) => operands.push(operand),
                cst::InfixOperatorChild::BinaryOp(op) => operator = Some(op),
            }
        }
        let operator = self.require(operator, "an infix form has an operator")?;
        let text = self.text(operator);
        let operator = binary_operator(text).ok_or_else(|| {
            DelightQLError::parse_error(format!("'{text}' is not a binary operator"))
        })?;
        let mut operands = operands.into_iter();
        let left = self.require(operands.next(), "an infix form has a left operand")?;
        let right = self.require(operands.next(), "an infix form has a right operand")?;
        let left = self.operand(left)?;
        let right = self.operand(right)?;
        Ok(DomainExpression::Application(FunctionApplication::Infix(
            crate::pipeline::asts::core::InfixApplication {
                operator,
                left: Box::new(left),
                right: Box::new(right),
            },
        )))
    }

    pub(crate) fn operand(&mut self, node: cst::Operand<'t>) -> Result<Domex> {
        match node {
            cst::Operand::Reference(reference) => self.reference_expression(reference),
            cst::Operand::NonInfixApplication(application) => {
                self.non_infix_application(application)
            }
        }
    }

    fn non_infix_application(&mut self, node: cst::NonInfixApplication<'t>) -> Result<Domex> {
        match node {
            // PARENS ARE ADMISSION, NOT MEANING. They decide which expression
            // is nested inside which and are spent here; nothing downstream
            // re-decides grouping, and no receipt survives for a later pass
            // to consult. An infix operand derives no infix form, so a nested
            // infix IS the authored grouping and lowering reads it back from
            // the structure.
            cst::NonInfixApplication::ParenthesizedOperand(parens) => {
                let inner = self.require(parens.child(), "parentheses enclose an expression")?;
                self.domain_expression(inner)
            }
            // The operand stratum of the crossing: a non-infix truth read as
            // a value, standing wherever an operand stands.
            cst::NonInfixApplication::CrossedTruth(crossed) => self.crossed_truth(crossed),
            // THE TWO-ANAPHOR LAW, at the application position: `@` is what
            // flows in, whatever root it stands under; the position that
            // applies the enclosing body spends it.
            cst::NonInfixApplication::CompositionInput(_) => {
                Ok(DomainExpression::Application(FunctionApplication::Open(
                    crate::pipeline::asts::core::DomainHole::CompositionInput,
                )))
            }
            cst::NonInfixApplication::Ground(ground) => self.ground_expression(ground),
            cst::NonInfixApplication::Template(template) => self.template(template),
            cst::NonInfixApplication::FunctorLike(functor) => self.functor_like(functor),
            cst::NonInfixApplication::FunctionPipe(pipe) => self.function_pipe(pipe),
            cst::NonInfixApplication::CaseLike(case) => self.case_like(case),
            cst::NonInfixApplication::RelationLike(relation) => self.relation_like(relation),
            cst::NonInfixApplication::EnclyphLike(enclyph) => Ok(DomainExpression::Application(
                FunctionApplication::Enclyph(self.enclyph_like(enclyph)?),
            )),
            cst::NonInfixApplication::JsonAccess(access) => self.json_access(access),
        }
    }

    // -----------------------------------------------------------------
    // Calls
    // -----------------------------------------------------------------

    fn functor_like(&mut self, node: cst::FunctorLike<'t>) -> Result<Domex> {
        let application = match node {
            cst::FunctorLike::StandardApplication(application) => {
                self.standard_application(application)?
            }
            // The nullary consumer. `:f` IS `f:()`, and after this line
            // nothing can tell them apart — which is the point.
            cst::FunctorLike::Citation(citation) => {
                let callee = self.require(citation.callee(), "a citation names a callee")?;
                let callee = self.require(callee.child(), "a callee is a predicate identifier")?;
                let call = FunctorCall::scalar(self.plain_reference(callee)?, Vec::new());
                StandardApplication::plain(self.seal_pure(call)?)
            }
            cst::FunctorLike::WindowApplication(window) => self.window_application(window)?,
        };
        Ok(DomainExpression::Application(
            FunctionApplication::Standard(application),
        ))
    }

    /// THE APPLICATION IS THE CALL PLUS WHAT THE POSITION ADDS. The guard is
    /// read here because it filters the rows THIS application sees, not
    /// because the callee knows about it.
    fn standard_application(
        &mut self,
        node: cst::StandardApplication<'t>,
    ) -> Result<StandardApplication<Unresolved>> {
        let callee = self.require(node.callee(), "an application names a callee")?;
        let mut arguments = Vec::new();
        let mut guard = None;
        for child in node.children() {
            match child {
                cst::StandardApplicationChild::Argument(argument) => arguments.push(argument),
                cst::StandardApplicationChild::Guard(node) => guard = Some(node),
                cst::StandardApplicationChild::CommaSigil(_) => {}
            }
        }
        self.application(callee, arguments, guard, None)
    }

    /// A windowed application is the same application with the spec its
    /// enclosure carries: the parens close over arguments, guard and window
    /// together, so the spec can belong to no other call.
    fn window_application(
        &mut self,
        node: cst::WindowApplication<'t>,
    ) -> Result<StandardApplication<Unresolved>> {
        let callee = self.require(node.callee(), "an application names a callee")?;
        let mut arguments = Vec::new();
        let mut guard = None;
        for child in node.children() {
            match child {
                cst::WindowApplicationChild::Argument(argument) => arguments.push(argument),
                cst::WindowApplicationChild::Guard(node) => guard = Some(node),
                cst::WindowApplicationChild::CommaSigil(_) => {}
            }
        }
        let window = self.require(node.window(), "a window application has a spec")?;
        self.application(callee, arguments, guard, Some(window))
    }

    /// The one builder behind both spellings: with or without a window, the
    /// callee, the argument row and the guard are read the same way.
    fn application(
        &mut self,
        callee: cst::Callee<'t>,
        arguments: Vec<cst::Argument<'t>>,
        guard: Option<cst::Guard<'t>>,
        window: Option<cst::WindowSpec<'t>>,
    ) -> Result<StandardApplication<Unresolved>> {
        let callee = self.require(callee.child(), "a callee is a predicate identifier")?;
        let reference = self.plain_reference(callee)?;
        let arguments = arguments
            .into_iter()
            .map(|argument| self.argument(argument))
            .collect::<Result<Vec<_>>>()?;
        let guard = match guard {
            Some(guard) => Some(Box::new(self.guard(guard)?)),
            None => None,
        };
        let window = match window {
            Some(spec) => Some(self.window_spec(spec)?),
            None => None,
        };
        let call = self.seal_pure(FunctorCall::scalar_application(reference, arguments))?;
        Ok(StandardApplication {
            call,
            guard,
            window,
        })
    }

    /// A call's argument row. The `%` prefix is an argument MODIFIER — the
    /// values dedupe before the function sees them — not a call to something
    /// named DISTINCT.
    pub(crate) fn argument(&mut self, node: cst::Argument<'t>) -> Result<Argument> {
        match node {
            cst::Argument::ValueArgument(argument) => {
                let mut distinct = false;
                let mut value = None;
                for child in argument.children() {
                    match child {
                        cst::ValueArgumentChild::DistinctMark(_) => distinct = true,
                        cst::ValueArgumentChild::DomainExpression(expression) => {
                            value = Some(expression)
                        }
                    }
                }
                let value = self.require(value, "an argument has a value")?;
                let value = self.domain_expression(value)?;
                // `%` MODIFIES THIS ARGUMENT. It is argument data, so it
                // rides on the argument's value and cannot be manufactured
                // anywhere a domain expression stands.
                Ok(Argument::Value(ArgumentValue { distinct, value }))
            }
            // AN ARGUMENT MAY ENUMERATE: `count:(*)`, `f:(/re/)`. It stands
            // for the several values it covers and computes none.
            cst::Argument::Spread(spread) => Ok(Argument::Spread(self.spread(spread)?)),
            // `..` selects the call's context mode. An argument-row position
            // of its own: instantiation consumes it, and no value carrier
            // exists for it to hide in.
            cst::Argument::ContextMarker(_) => Ok(Argument::Context(
                crate::pipeline::asts::core::ContextMarker,
            )),
            // A CALLABLE STANDS WHERE THE POSITION SUPPLIES ITS SLOT. The
            // curried parameter is that position; whether the callee declares
            // one is the descriptor's judgment at resolution.
            // A CALLABLE STANDS WHERE THE POSITION SUPPLIES ITS SLOT. The
            // curried parameter is that position; whether the callee declares
            // one is the descriptor's judgment at resolution.
            cst::Argument::Lambda(lambda) => Ok(Argument::Callable(
                crate::pipeline::asts::core::Callable::Lambda(self.lambda(lambda)?),
            )),
        }
    }

    pub(crate) fn guard(
        &mut self,
        node: cst::Guard<'t>,
    ) -> Result<crate::pipeline::asts::core::TruthExpression<Unresolved>> {
        let condition = self.require(node.child(), "a guard has a condition")?;
        self.truth_expression(condition)
    }

    /// A call in VALUE position is pure by construction: the effect fence is
    /// a mark on the authored name, and no effect name derives here.
    pub(crate) fn seal_pure(&self, call: FunctorCall<Unresolved>) -> Result<PureCall<Unresolved>> {
        PureCall::seal(call).map_err(|call| {
            DelightQLError::validation_error_categorized(
                "effect/position",
                format!(
                    "'{}!' is a directive and cannot stand where a pure call stands",
                    call.callee.name_text()
                ),
                "effects are legal only as direct operands of a chain join",
            )
        })
    }

    // -----------------------------------------------------------------
    // Windows
    // -----------------------------------------------------------------

    fn window_spec(
        &mut self,
        node: cst::WindowSpec<'t>,
    ) -> Result<crate::pipeline::asts::core::WindowSpec<Unresolved>> {
        let mut partition = Vec::new();
        if let Some(node) = node.partition() {
            for child in node.children() {
                match child {
                    cst::PartitionChild::DomainExpression(expression) => {
                        partition.push(self.domain_expression(expression)?)
                    }
                    cst::PartitionChild::CommaSigil(_) | cst::PartitionChild::PercentSigil(_) => {}
                }
            }
        }
        let ordering = match node.order() {
            Some(ordering) => self.ordering_specs(ordering)?,
            None => Vec::new(),
        };
        let frame = node
            .frame()
            .map(|frame| self.window_frame(frame))
            .transpose()?;
        Ok(crate::pipeline::asts::core::WindowSpec {
            partition,
            ordering,
            frame,
        })
    }

    fn window_frame(
        &mut self,
        node: cst::Frame<'t>,
    ) -> Result<crate::pipeline::asts::core::operators::WindowFrame<Unresolved>> {
        use crate::pipeline::asts::core::operators::FrameMode;

        let kind = self.require(node.kind_field(), "a frame names its mode")?;
        let mode = match self.text(kind) {
            "rows" => FrameMode::Rows,
            "range" => FrameMode::Range,
            "groups" => FrameMode::Groups,
            other => {
                return Err(DelightQLError::parse_error(format!(
                    "'{other}' is not a frame mode"
                )))
            }
        };
        let mut bounds = node.children();
        let start = self.require(bounds.next(), "a frame has a start bound")?;
        let end = self.require(bounds.next(), "a frame has an end bound")?;
        Ok(crate::pipeline::asts::core::operators::WindowFrame {
            mode,
            start: self.frame_bound(start)?,
            end: self.frame_bound(end)?,
        })
    }

    fn frame_bound(
        &mut self,
        node: cst::FrameBound<'t>,
    ) -> Result<crate::pipeline::asts::core::operators::FrameBound<Unresolved>> {
        use crate::pipeline::asts::core::operators::FrameBound;

        match node {
            cst::FrameBound::FrameUnbounded(_) => Ok(FrameBound::Unbounded),
            cst::FrameBound::FrameCurrentRow(_) => Ok(FrameBound::CurrentRow),
            cst::FrameBound::FramePreceding(bound) => {
                let offset = self.require(bound.child(), "a preceding bound has an offset")?;
                Ok(FrameBound::Preceding(Box::new(
                    self.domain_expression(offset)?,
                )))
            }
            cst::FrameBound::FrameFollowing(bound) => {
                let offset = self.require(bound.child(), "a following bound has an offset")?;
                Ok(FrameBound::Following(Box::new(
                    self.domain_expression(offset)?,
                )))
            }
        }
    }

    // -----------------------------------------------------------------
    // Templates
    // -----------------------------------------------------------------

    /// ONE TEMPLATE PARSE, classified once by content. The carrier's one
    /// door refuses a template with nothing to interpolate, which is how the
    /// ground string becomes the only representation of that form.
    fn template(&mut self, node: cst::Template<'t>) -> Result<Domex> {
        let parts = self.template_parts(node)?;
        Ok(DomainExpression::Application(
            match crate::pipeline::asts::core::ValueTemplate::interpolating(parts) {
                Some(template) => FunctionApplication::Template(template),
                None => {
                    FunctionApplication::Ground(LiteralValue::String(self.template_text(node)?))
                }
            },
        ))
    }

    /// The authored text of a template that interpolates nothing.
    fn template_text(&mut self, node: cst::Template<'t>) -> Result<String> {
        Ok(self
            .template_parts(node)?
            .into_iter()
            .map(|part| match part {
                ValueTemplatePart::Text(text) => text,
                // Unreachable: the caller reached here only because no part
                // interpolates.
                ValueTemplatePart::Interpolation(_) => String::new(),
            })
            .collect())
    }

    pub(crate) fn template_parts(
        &mut self,
        node: cst::Template<'t>,
    ) -> Result<Vec<ValueTemplatePart<Unresolved>>> {
        let mut parts = Vec::new();
        for child in node.children() {
            match child {
                cst::TemplateChild::TemplatePart(part) => match part {
                    cst::TemplatePart::TemplateText(text) => {
                        parts.push(ValueTemplatePart::Text(template_escapes(self.text(text))?))
                    }
                    cst::TemplatePart::Interpolation(interpolation) => {
                        parts.push(self.interpolation(interpolation)?)
                    }
                },
                cst::TemplateChild::TripleTemplatePart(part) => {
                    let part = self.require(part.child(), "a template part has content")?;
                    match part {
                        cst::TripleTemplatePartChild::TripleTemplateText(text) => {
                            parts.push(ValueTemplatePart::Text(template_escapes(self.text(text))?))
                        }
                        cst::TripleTemplatePartChild::Interpolation(interpolation) => {
                            parts.push(self.interpolation(interpolation)?)
                        }
                    }
                }
            }
        }
        Ok(parts)
    }

    fn interpolation(
        &mut self,
        node: cst::Interpolation<'t>,
    ) -> Result<ValueTemplatePart<Unresolved>> {
        let inner = self.require(node.child(), "an interpolation encloses an expression")?;
        Ok(ValueTemplatePart::Interpolation(Box::new(
            self.domain_expression(inner)?,
        )))
    }

    // -----------------------------------------------------------------
    // Callables
    // -----------------------------------------------------------------

    /// THE CALLABLE, BUILT — AND ITS SLOT JUDGED, HERE.
    ///
    /// THE SLOT IS ONE. The grammar admits zero or many holes and the count
    /// is judged in this one place, so a cover and a function pipe cannot
    /// disagree about what a callable is. A form with an argument row may
    /// leave the slot unwritten and take the implicit landing its position
    /// supplies; a form without one must write it. A second bare hole would
    /// spell the flowing value twice under a glyph that names nothing, so it
    /// refuses toward the binder that names it.
    pub(crate) fn callable(&mut self, node: cst::Callable<'t>) -> Result<Callable<Unresolved>> {
        let written = self.text(node).to_string();
        match node {
            cst::Callable::OpenFunctor(functor) => {
                let application = self.open_functor(functor)?;
                self.one_slot_with_a_row(application, &written)
            }
            // A window function is a function, and its spec is enclosed where
            // it is enclosed everywhere else: inside the application's parens.
            cst::Callable::OpenWindowFunctor(window) => {
                let application = self.open_window_functor(window)?;
                self.one_slot_with_a_row(application, &written)
            }
            // A template in callable position is the open string: it carries
            // the flowing value inside an interpolation, so it needs no
            // argument row and takes no implicit landing.
            cst::Callable::Template(template) => {
                let parts = self.template_parts(template)?;
                let Some(template) =
                    crate::pipeline::asts::core::ValueTemplate::interpolating(parts)
                else {
                    return Err(landing::nothing_receives_it(&written));
                };
                let interpolated = template
                    .parts()
                    .filter_map(|part| match part {
                        ValueTemplatePart::Interpolation(value) => Some((**value).clone()),
                        ValueTemplatePart::Text(_) => None,
                    })
                    .collect::<Vec<_>>();
                self.one_slot_without_a_row(&interpolated, &written)?;
                Ok(Callable::String(template))
            }
            cst::Callable::Lambda(lambda) => Ok(Callable::Lambda(self.lambda(lambda)?)),
        }
    }

    /// A form WITH an argument row: the position may supply the slot, so
    /// zero is the implicit landing and only a second written one refuses.
    fn one_slot_with_a_row(
        &self,
        application: StandardApplication<Unresolved>,
        written: &str,
    ) -> Result<Callable<Unresolved>> {
        match landing::holes_in_application(&application)? {
            0 | 1 => Ok(Callable::Functor(application)),
            slots => Err(landing::the_slot_is_one(written, slots)),
        }
    }

    /// A form with NO argument row: the slot is written, once.
    fn one_slot_without_a_row(&self, values: &[Domex], written: &str) -> Result<()> {
        match landing::holes_in(values)? {
            0 => Err(landing::nothing_receives_it(written)),
            1 => Ok(()),
            slots => Err(landing::the_slot_is_one(written, slots)),
        }
    }

    fn open_functor(
        &mut self,
        node: cst::OpenFunctor<'t>,
    ) -> Result<StandardApplication<Unresolved>> {
        let callee = self.require(node.callee(), "an open functor names a callee")?;
        let mut expressions = Vec::new();
        let mut guard = None;
        for child in node.children() {
            match child {
                cst::OpenFunctorChild::OpenExpression(expression) => expressions.push(expression),
                cst::OpenFunctorChild::Guard(node) => guard = Some(node),
                cst::OpenFunctorChild::CommaSigil(_) => {}
            }
        }
        self.open_application(callee, expressions, guard, None)
    }

    /// The windowed open functor: the same open row, with the spec its
    /// enclosure carries.
    fn open_window_functor(
        &mut self,
        node: cst::OpenWindowFunctor<'t>,
    ) -> Result<StandardApplication<Unresolved>> {
        let callee = self.require(node.callee(), "an open functor names a callee")?;
        let mut expressions = Vec::new();
        let mut guard = None;
        for child in node.children() {
            match child {
                cst::OpenWindowFunctorChild::OpenExpression(expression) => {
                    expressions.push(expression)
                }
                cst::OpenWindowFunctorChild::Guard(node) => guard = Some(node),
                cst::OpenWindowFunctorChild::CommaSigil(_) => {}
            }
        }
        let window = self.require(node.window(), "a windowed callable has a spec")?;
        self.open_application(callee, expressions, guard, Some(window))
    }

    /// The one builder behind both open spellings.
    ///
    /// Zero holes means implicit landing: `x /-> upper:(y)` is `upper(x, y)`.
    /// The position that applies the callable supplies the slot, so the
    /// elision is honoured by leaving the argument row as written.
    fn open_application(
        &mut self,
        callee: cst::Callee<'t>,
        expressions: Vec<cst::OpenExpression<'t>>,
        guard: Option<cst::Guard<'t>>,
        window: Option<cst::WindowSpec<'t>>,
    ) -> Result<StandardApplication<Unresolved>> {
        let callee = self.require(callee.child(), "a callee is a predicate identifier")?;
        let reference = self.plain_reference(callee)?;
        let mut arguments = Vec::new();
        for expression in expressions {
            arguments.push(Argument::plain(self.open_expression(expression)?));
        }
        let guard = match guard {
            Some(guard) => Some(Box::new(self.guard(guard)?)),
            None => None,
        };
        let window = match window {
            Some(spec) => Some(self.window_spec(spec)?),
            None => None,
        };
        let call = self.seal_pure(FunctorCall::scalar_application(reference, arguments))?;
        Ok(StandardApplication {
            call,
            guard,
            window,
        })
    }

    /// THE BINDER NAMES THE FLOW, and the name is spent HERE.
    ///
    /// `:(|x| …)` says the flowing value may stand at more than one place,
    /// so the binder's uses become the slot and the name goes no further:
    /// nothing downstream carries a binding environment, and no resolver
    /// meets a reference that addresses no column. A binder used nowhere
    /// receives nothing, and a binder written beside `@` spells the flow
    /// twice.
    fn lambda(
        &mut self,
        node: cst::Lambda<'t>,
    ) -> Result<crate::pipeline::asts::core::Lambda<Unresolved>> {
        let mut binder = None;
        let mut body = None;
        for child in node.children() {
            match child {
                cst::LambdaChild::LambdaBinder(node) => binder = Some(node),
                cst::LambdaChild::OpenExpression(expression) => body = Some(expression),
            }
        }
        let written = self.text(node).to_string();
        let body = self.require(body, "a lambda has a body")?;
        let body = self.open_expression(body)?;
        let Some(binder) = binder else {
            self.one_slot_without_a_row(std::slice::from_ref(&body), &written)?;
            return Ok(crate::pipeline::asts::core::Lambda {
                body: Box::new(body),
            });
        };
        let name = self.require(binder.child(), "a lambda binder names the flow")?;
        let name = self.identifier(name);
        if landing::holes_in(std::slice::from_ref(&body))? > 0 {
            return Err(landing::binder_beside_a_hole(&written, name.as_str()));
        }
        let (body, uses) = landing::bind_the_binder(body, &name)?;
        if uses == 0 {
            return Err(landing::binder_receives_nothing(&written, name.as_str()));
        }
        Ok(crate::pipeline::asts::core::Lambda {
            body: Box::new(body),
        })
    }

    // -----------------------------------------------------------------
    // The function pipe
    // -----------------------------------------------------------------

    /// THE SUBSTITUTION LAW at value level, SPENT HERE. `/->` lands the
    /// flowing value at the argument row's final place, and a written `@`
    /// overrides that.
    ///
    /// The pipe does not survive this boundary. Each step becomes the
    /// ordinary application it denotes, so a piped call and a directly
    /// written call are one shape and no later pass branches on how the
    /// call arrived.
    fn function_pipe(&mut self, node: cst::FunctionPipe<'t>) -> Result<Domex> {
        let mut value = None;
        let mut steps = Vec::new();
        for child in node.children() {
            match child {
                cst::FunctionPipeChild::DomainExpression(expression) if value.is_none() => {
                    // NO PRECEDENCE. An infix composition standing as the
                    // pipe's source has two readings — `(a ++ b) /-> f` and
                    // `a ++ (b /-> f)` — and the language picks neither.
                    if let cst::DomainExpression::FunctionApplication(
                        cst::FunctionApplication::InfixOperator(_),
                    ) = expression
                    {
                        let written = self.text(expression);
                        return Err(DelightQLError::parse_error_categorized(
                            crate::uri_registry::subcat::PARSE_PONY,
                            format!(
                                "'{written}' composes an infix operator with a function pipe and \
nothing groups them: DelightQL has NO operator precedence, so the expression \
has no reading. Parenthesize the operand the pipe receives."
                            ),
                        ));
                    }
                    value = Some(self.domain_expression(expression)?)
                }
                cst::FunctionPipeChild::DomainExpression(expression) => {
                    return Err(DelightQLError::parse_error(format!(
                        "a function pipe has one source; '{}' is a second",
                        self.text(expression)
                    )))
                }
                cst::FunctionPipeChild::FunctionPipeStep(step) => steps.push(step),
            }
        }
        let mut value = self.require(value, "a function pipe has a source")?;
        for step in steps {
            value = self.function_pipe_step(step, value)?;
        }
        Ok(value)
    }

    /// One step of the pipe: the callable, applied to what flows in.
    fn function_pipe_step(
        &mut self,
        node: cst::FunctionPipeStep<'t>,
        flowing: Domex,
    ) -> Result<Domex> {
        let mut callable = None;
        for child in node.children() {
            match child {
                cst::FunctionPipeStepChild::FunctionPipeOperator(_) => {}
                cst::FunctionPipeStepChild::Callable(node) => callable = Some(node),
            }
        }
        let callable = self.require(callable, "a function-pipe step has a callable")?;
        self.apply_callable(callable, flowing)
    }

    /// A CALLABLE, APPLIED. The one place the value level spends a landing.
    ///
    /// The slot was judged where the callable was built, so this only spends
    /// it: a form with an argument row and no written slot takes the default
    /// landing, and everything else receives the value where the author
    /// wrote it.
    pub(crate) fn apply_callable(
        &mut self,
        node: cst::Callable<'t>,
        flowing: Domex,
    ) -> Result<Domex> {
        match self.callable(node)? {
            Callable::Functor(application) => self.land_in_application(application, flowing),
            Callable::String(template) => {
                let parts = template.into_parts();
                let spent = landing::spend(
                    parts
                        .iter()
                        .filter_map(|part| match part {
                            ValueTemplatePart::Interpolation(value) => Some((**value).clone()),
                            ValueTemplatePart::Text(_) => None,
                        })
                        .collect(),
                    &flowing,
                )?;
                let mut spent = spent.into_iter();
                let parts = parts
                    .into_iter()
                    .map(|part| match part {
                        ValueTemplatePart::Interpolation(_) => ValueTemplatePart::Interpolation(
                            Box::new(spent.next().expect("one spent value per interpolation")),
                        ),
                        text => text,
                    })
                    .collect();
                Ok(DomainExpression::Application(
                    FunctionApplication::Template(
                        crate::pipeline::asts::core::ValueTemplate::interpolating(parts)
                            .expect("the build already proved an interpolation is present"),
                    ),
                ))
            }
            // A binder was already spent into its uses, so what stands here
            // is a body with slots — one if the author wrote the bare hole,
            // however many the binder named.
            Callable::Lambda(lambda) => {
                let mut spent = landing::spend(vec![*lambda.body], &flowing)?;
                Ok(spent.pop().expect("one value in, one value out"))
            }
        }
    }

    /// The landing a form WITH an argument row takes.
    fn land_in_application(
        &mut self,
        mut application: StandardApplication<Unresolved>,
        flowing: Domex,
    ) -> Result<Domex> {
        let application = match landing::holes_in_application(&application)? {
            // ZERO HOLES: the default landing, the row's final place. This
            // is why `x /-> upper:(y)` means `upper(y, x)`.
            0 => {
                use crate::pipeline::asts::core::operators::CallArguments;
                let call = application.call_mut();
                let arguments = match std::mem::replace(&mut call.arguments, CallArguments::None) {
                    CallArguments::Scalar(members) => members,
                    CallArguments::None => Vec::new(),
                    other @ CallArguments::HigherOrder(_) => {
                        call.arguments = other;
                        return Err(DelightQLError::parse_error(
                            "a scalar application carries a scalar argument row",
                        ));
                    }
                };
                call.arguments = CallArguments::Scalar(landing::land_final(
                    Argument::plain(flowing),
                    arguments,
                ));
                application
            }
            _ => landing::spend_in_application(application, &flowing)?,
        };
        Ok(DomainExpression::Application(
            FunctionApplication::Standard(application),
        ))
    }

    // -----------------------------------------------------------------
    // Cases
    // -----------------------------------------------------------------

    /// THE HEADER CLASSIFIES: the `@` header decides anchored versus searched
    /// at PARSE, so arm content never reclassifies a case. The anchor is read
    /// once and stored once — it belongs to the case, not to each arm.
    fn case_like(&mut self, node: cst::CaseLike<'t>) -> Result<Domex> {
        use crate::pipeline::asts::core::{CaseExpression, MatchArm, SearchedArm};

        let inner = self.require(node.child(), "a case has a body")?;
        let case = match inner {
            cst::CaseLikeChild::AnchoredCase(anchored) => {
                let anchor = self.require(anchored.anchor(), "an anchored case has an anchor")?;
                let anchor = self.domain_expression(anchor)?;
                let mut arms = Vec::new();
                let mut default = None;
                for child in anchored.children() {
                    match child {
                        cst::AnchoredCaseChild::MatchArm(arm) => {
                            let (term, result) = self.match_arm(arm)?;
                            arms.push(MatchArm {
                                term,
                                result: Box::new(result),
                            });
                        }
                        cst::AnchoredCaseChild::DefaultArm(arm) => {
                            default = Some(Box::new(self.default_arm(arm)?))
                        }
                        cst::AnchoredCaseChild::Separator(_) => {}
                    }
                }
                CaseExpression::Anchored {
                    anchor: Box::new(anchor),
                    arms: self.require(Vec1::try_from_vec(arms), "a case has a match arm")?,
                    default,
                }
            }
            cst::CaseLikeChild::SearchedCase(searched) => {
                let mut arms = Vec::new();
                let mut default = None;
                for child in searched.children() {
                    match child {
                        cst::SearchedCaseChild::SearchedArm(arm) => {
                            let (condition, result) = self.searched_arm(arm)?;
                            arms.push(SearchedArm {
                                condition: Box::new(condition),
                                result: Box::new(result),
                            });
                        }
                        cst::SearchedCaseChild::DefaultArm(arm) => {
                            default = Some(Box::new(self.default_arm(arm)?))
                        }
                    }
                }
                CaseExpression::Searched {
                    arms: self.require(Vec1::try_from_vec(arms), "a case has a condition arm")?,
                    default,
                }
            }
        };
        Ok(DomainExpression::Application(FunctionApplication::Case(
            case,
        )))
    }

    /// Matching is NULL-SAFE equality: a `null` match arm MATCHES a null
    /// anchor, which is why the term is a ground VALUE and not a comparison.
    fn match_arm(&mut self, node: cst::MatchArm<'t>) -> Result<(LiteralValue, Domex)> {
        // FIELDED, because every ground IS a domain expression: `null -> null`
        // is one node kind on both sides of the arrow and only the field says
        // which is the match term.
        let value = self.require(node.value(), "a match arm has a ground term")?;
        let value = self.ground(value)?;
        let result = self.require(node.result(), "a match arm has a result")?;
        Ok((value, self.domain_expression(result)?))
    }

    fn searched_arm(
        &mut self,
        node: cst::SearchedArm<'t>,
    ) -> Result<(
        crate::pipeline::asts::core::TruthExpression<Unresolved>,
        Domex,
    )> {
        let condition = self.require(node.condition(), "a searched arm has a condition")?;
        let condition = self.arm_condition(condition)?;
        let result = self.require(node.result(), "a searched arm has a result")?;
        Ok((condition, self.domain_expression(result)?))
    }

    /// `,` as `and` is scoped to case-arm conditions and nowhere else.
    fn arm_condition(
        &mut self,
        node: cst::ArmCondition<'t>,
    ) -> Result<crate::pipeline::asts::core::TruthExpression<Unresolved>> {
        use crate::pipeline::asts::core::TruthExpression;

        let mut conditions = Vec::new();
        for child in node.children() {
            match child {
                cst::ArmConditionChild::TruthExpression(truth) => {
                    conditions.push(self.truth_expression(truth)?)
                }
                cst::ArmConditionChild::CommaSigil(_) => {}
            }
        }
        self.require(
            TruthExpression::all(conditions),
            "an arm condition has a test",
        )
    }

    fn default_arm(&mut self, node: cst::DefaultArm<'t>) -> Result<Domex> {
        let result = self.require(node.result(), "a default arm has a result")?;
        self.domain_expression(result)
    }

    // -----------------------------------------------------------------
    // Enclyphs and JSON
    // -----------------------------------------------------------------

    /// An enclyph in value position is ONE nested value; in reduction
    /// position, a table of them. There is no tree-group kind: a tree group
    /// IS an enclyph whose position compresses it.
    pub(crate) fn enclyph_like(
        &mut self,
        node: cst::EnclyphLike<'t>,
    ) -> Result<Enclyph<Unresolved>> {
        match node {
            cst::EnclyphLike::Record(record) => {
                let mut members = Vec::new();
                for child in record.children() {
                    match child {
                        cst::RecordChild::RecordMember(member) => {
                            members.push(self.record_member(member)?)
                        }
                        cst::RecordChild::CommaSigil(_) => {}
                    }
                }
                Ok(Enclyph::Record(Record::plain(self.require(
                    Vec1::try_from_vec(members),
                    "a record has at least one member",
                )?)))
            }
            cst::EnclyphLike::Tuple(tuple) => {
                let mut elements = Vec::new();
                for child in tuple.children() {
                    match child {
                        cst::TupleChild::DomainExpression(expression) => {
                            elements.push(crate::pipeline::asts::core::TupleElement::Value(
                                self.domain_expression(expression)?,
                            ))
                        }
                        cst::TupleChild::Spread(spread) => elements.push(
                            crate::pipeline::asts::core::TupleElement::Spread(self.spread(spread)?),
                        ),
                        cst::TupleChild::CommaSigil(_) => {}
                    }
                }
                Ok(Enclyph::Tuple(Box::new(Tuple {
                    elements: self.require(
                        Vec1::try_from_vec(elements),
                        "a tuple has at least one element",
                    )?,
                })))
            }
        }
    }

    /// A CONSTRUCTION member builds a value. The four forms are the grammar's,
    /// and a pattern's binders, reaches and anaphor have no derivation here.
    fn record_member(&mut self, node: cst::RecordMember<'t>) -> Result<RecordMember<Unresolved>> {
        match node {
            // FN.22 (amended): a metadata group may stand as an induced
            // member's body, under a fixed key. The key is the name; the
            // group's own naming has no place here by grammar.
            cst::RecordMember::KeyedMetadata(member) => {
                let mut key = None;
                let mut group = None;
                for child in member.children() {
                    match child {
                        cst::KeyedMetadataChild::Key(node) => key = Some(self.key(node)?),
                        cst::KeyedMetadataChild::MetadataGroup(node) => {
                            let (built, _naming) = self.metadata_group(node)?;
                            group = Some(built);
                        }
                    }
                }
                Ok(RecordMember::Metadata {
                    key: self.require(key, "a keyed metadata member has a key")?,
                    group: Box::new(self.require(group, "a keyed metadata member has a group")?),
                })
            }
            cst::RecordMember::KeyedValue(member) => {
                let mut key = None;
                let mut value = None;
                for child in member.children() {
                    match child {
                        cst::KeyedValueChild::Key(node) => key = Some(self.key(node)?),
                        cst::KeyedValueChild::DomainExpression(expression) => {
                            value = Some(self.domain_expression(expression)?)
                        }
                    }
                }
                Ok(RecordMember::Keyed {
                    key: self.require(key, "a keyed value has a key")?,
                    value: Box::new(self.require(value, "a keyed value has a value")?),
                })
            }
            // A nested level, re-entering reduction in the parent's group:
            // the induction IS the marker plus the position.
            cst::RecordMember::InducedMember(member) => {
                let mut key = None;
                let mut value = None;
                for child in member.children() {
                    match child {
                        cst::InducedMemberChild::Key(node) => key = Some(self.key(node)?),
                        cst::InducedMemberChild::EnclyphLike(enclyph) => {
                            value = Some(self.enclyph_like(enclyph)?)
                        }
                        cst::InducedMemberChild::ReductionSigil(_) => {}
                    }
                }
                Ok(RecordMember::Induced {
                    key: self.require(key, "an induced member has a key")?,
                    value: Box::new(self.require(value, "an induced member has a constructor")?),
                })
            }
            // A reference donates its own unqualified name as the key; only
            // references qualify, because nothing else has a name to donate.
            cst::RecordMember::SelfKeyedReference(member) => {
                let reference =
                    self.require(member.child(), "a self-keyed member is a reference")?;
                Ok(RecordMember::SelfKeyed(NamedReference(
                    self.authored_column(reference)?,
                )))
            }
            cst::RecordMember::Spread(spread) => Ok(RecordMember::Spread(self.spread(spread)?)),
        }
    }

    fn key(&self, node: cst::Key<'t>) -> Result<String> {
        let string = self.require(node.child(), "a key is a string")?;
        Ok(super::ground::string_interior(self.text(string)).to_string())
    }

    /// THE one accessor — exactly ONE path, a scalar reach.
    fn json_access(&mut self, node: cst::JsonAccess<'t>) -> Result<Domex> {
        let mut source = None;
        let mut accessor = None;
        for child in node.children() {
            match child {
                cst::JsonAccessChild::NamedReference(reference) => source = Some(reference),
                cst::JsonAccessChild::JsonAccessor(node) => accessor = Some(node),
            }
        }
        let source = self.require(source, "a json access has a source")?;
        let accessor = self.require(accessor, "a json access has an accessor")?;
        let path = self.require(accessor.child(), "an accessor has a path")?;
        Ok(DomainExpression::Application(
            FunctionApplication::JsonAccess(crate::pipeline::asts::core::JsonAccess {
                source: Box::new(DomainExpression::Reference(Reference::Named(
                    NamedReference(self.authored_column(source)?),
                ))),
                path: self.path(path)?,
            }),
        ))
    }

    // -----------------------------------------------------------------
    // Relations in value position
    // -----------------------------------------------------------------

    /// CARDINALITY IS AUTHORED: a relation enters value position only through
    /// an inner form ending in a declared COMPRESSION. The compression closes
    /// the interior — nothing may follow it and reopen the relation — so an
    /// uncompressed inner form has no derivation and the refusal is
    /// structural rather than a check a consumer could forget.
    fn relation_like(&mut self, node: cst::RelationLike<'t>) -> Result<Domex> {
        use crate::pipeline::asts::core::Access;

        match node {
            cst::RelationLike::ScalarSubquery(subquery) => {
                let callee = self.require(subquery.callee(), "an inner form names a relation")?;
                let (identifier, passthrough) = self.relation_identifier(callee)?;
                let interior =
                    self.require(subquery.interior(), "an inner form has an interior")?;
                // COLON-FIRST: the relation the inner form names may be one
                // the caller parameterizes — `foo:(args)(, continuation)` —
                // and it is the same relation.
                let base = match subquery.arguments() {
                    Some(row) => {
                        let arguments = row
                            .children()
                            .filter_map(|child| match child {
                                cst::InnerArgumentRowChild::HoArgument(argument) => {
                                    Some(self.one_ho_argument(argument))
                                }
                                cst::InnerArgumentRowChild::CommaSigil(_) => None,
                            })
                            .collect::<Result<Vec<_>>>()?;
                        let reference = self.relation_reference(callee)?;
                        self.higher_order_call_with(
                            reference,
                            arguments,
                            Access::All,
                            Vec::new(),
                            crate::pipeline::asts::vocabulary::FunctorMarks::default(),
                        )?
                    }
                    None => self.mention_read(identifier.clone(), passthrough, Access::All, false),
                };
                let body = self.compressed_interior(interior, base)?;
                Ok(DomainExpression::Application(
                    FunctionApplication::Scalarized(
                        crate::pipeline::asts::core::ScalarRelation::Named {
                            identifier,
                            body: Box::new(body),
                        },
                    ),
                ))
            }
            // THE SOURCELESS INNER FORM: no base relation. The leading comma
            // is the no-op base made visible, so the first member IS the
            // head.
            cst::RelationLike::AnonScalarSubquery(subquery) => {
                let interior =
                    self.require(subquery.interior(), "an inner form has an interior")?;
                let mut continuations = interior.continuation();
                let first = self.require(
                    continuations.next(),
                    "a sourceless inner form names its first relation",
                )?;
                let base = match first {
                    cst::Continuation::BinaryContinuation(
                        cst::BinaryContinuation::CommaContinuation(comma),
                    ) => {
                        let member =
                            self.require(comma.member(), "a comma continuation has a member")?;
                        match member {
                            cst::CommaContinuationMember::GrelexLikeMember(
                                cst::GrelexLikeMember::Grelex(grelex),
                            ) => self.grelex(grelex)?,
                            _ => return Err(sourceless_needs_a_base()),
                        }
                    }
                    _ => return Err(sourceless_needs_a_base()),
                };
                let mut chain = base;
                for continuation in continuations {
                    chain = self.continuation(continuation, chain)?;
                }
                let compression = self.require(
                    interior.compression(),
                    "an inner form ends in a compression",
                )?;
                let body = self.compression(compression, chain)?;
                Ok(DomainExpression::Application(
                    FunctionApplication::Scalarized(
                        crate::pipeline::asts::core::ScalarRelation::Sourceless {
                            body: Box::new(body),
                        },
                    ),
                ))
            }
            // THE MODE IS THE COMPRESSION — a column pick on a call that is
            // one row by the callee's declared functional dependency. The
            // declaration lives in the catalog, so what is built here is the
            // pick and the call it picks from; which output the name reaches,
            // and whether the callee declares a mode at all, is resolution's
            // one question.
            cst::RelationLike::FieldSelect(select) => {
                let call = self.require(select.call(), "a field select picks from a call")?;
                let application = self.standard_application(call)?;
                let column = self.require(select.column(), "a field select names a column")?;
                let column = crate::pipeline::asts::core::AuthoredColumn {
                    name: self.identifier(column),
                    qualifier: None,
                    namespace_path: crate::pipeline::asts::core::NamespacePath::empty(),
                };
                Ok(DomainExpression::Application(
                    FunctionApplication::FieldSelect(crate::pipeline::asts::core::FieldSelect {
                        application,
                        field: column,
                        dependency: (),
                    }),
                ))
            }
        }
    }

    /// The compression CLOSES the interior. Its placement is part of the
    /// surface: what stands before it shapes the relation, and what it
    /// declares is the one-row guarantee.
    /// The interior, read as a BODY and the compression that closes it.
    ///
    /// The compression is removed from the chain exactly once, here, and
    /// returned as the proof it is. Nothing downstream re-derives cardinality
    /// by inspecting how the interior happened to end.
    fn compressed_interior(
        &mut self,
        node: cst::CompressedInterior<'t>,
        base: crate::pipeline::asts::core::Chain<Unresolved>,
    ) -> Result<crate::pipeline::asts::core::ScalarizedRelation<Unresolved>> {
        let mut chain = base;
        for continuation in node.continuation() {
            chain = self.continuation(continuation, chain)?;
        }
        let compression =
            self.require(node.compression(), "an inner form ends in a compression")?;
        self.compression(compression, chain)
    }

    /// A BOUND-TO-ONE OWNS THE ORDERING IT CONSUMES: the ordering that
    /// decides WHICH row is part of the proof, not a step standing loose
    /// before it.
    fn compression(
        &mut self,
        node: cst::Compression<'t>,
        chain: crate::pipeline::asts::core::Chain<Unresolved>,
    ) -> Result<crate::pipeline::asts::core::ScalarizedRelation<Unresolved>> {
        use crate::pipeline::asts::core::{
            Continuation, GroupSpec, PipeOp, Scalarization, ScalarizedRelation,
        };

        let (body, scalarization) = match node {
            cst::Compression::SingletonReduction(reduction) => {
                let PipeOp::Group(GroupSpec::Reduce { reductions, .. }) =
                    self.singleton_reduction(reduction)?
                else {
                    return Err(DelightQLError::parse_error(
                        "a singleton reduction is the zero-key group".to_string(),
                    ));
                };
                (chain, Scalarization::ZeroKeyReduction(reductions))
            }
            // The ordering the bound consumes is the last one written, and it
            // travels WITH the bound rather than beside it.
            cst::Compression::BoundToOne(_) => {
                let mut chain = chain;
                let ordering = match chain
                    .continuations()
                    .last()
                    .map(crate::pipeline::asts::core::Step::form)
                {
                    // An ordering already carrying a bound is a finished
                    // membership act; the compression then selects one of
                    // ITS members and owns no ordering.
                    Some(Continuation::Structural(
                        crate::pipeline::asts::core::StructuralStep {
                            form:
                                crate::pipeline::asts::core::StructuralForm::Ordering {
                                    bound: None,
                                    ..
                                },
                            ..
                        },
                    )) => match chain.continuations_mut().pop().map(|step| step.into_form()) {
                        Some(Continuation::Structural(
                            crate::pipeline::asts::core::StructuralStep {
                                form:
                                    crate::pipeline::asts::core::StructuralForm::Ordering {
                                        specs, ..
                                    },
                                ..
                            },
                        )) => specs,
                        _ => unreachable!("the peek and the pop read the same step"),
                    },
                    _ => Vec::new(),
                };
                (chain, Scalarization::BoundToOne { ordering })
            }
        };
        Ok(ScalarizedRelation::authored(body, scalarization))
    }

    /// A path never evaluates alone: it travels only into the positions that
    /// APPLY it to a source, and it arrives there as a `Path`.
    pub(crate) fn path(&self, node: cst::Path<'t>) -> Result<Path> {
        let steps = self.path_steps(node)?;
        Path::try_from_steps(steps).ok_or_else(|| {
            DelightQLError::parse_error("a path reaches at least one key".to_string())
        })
    }

    /// A path's keys as the extraction reads them. One reader, so a path
    /// written where an index already chose the container reaches the same
    /// way as one written on its own.
    pub(crate) fn path_steps(&self, node: cst::Path<'t>) -> Result<Vec<PathStep>> {
        let mut steps = Vec::new();
        for key in node.children() {
            let step = match key {
                cst::PathKey::PathName(name) => {
                    let name = self.require(name.child(), "a path name is an identifier")?;
                    PathStep::Key(self.identifier(name).as_str().to_string())
                }
                cst::PathKey::StringNode(string) => {
                    PathStep::Key(super::ground::string_interior(self.text(string)).to_string())
                }
                cst::PathKey::Number(number) => {
                    let text = self.text(number);
                    let index_value = text.parse::<i64>().map_err(|_| {
                        DelightQLError::parse_error(format!("'{text}' is not a path index"))
                    })?;
                    PathStep::Index(index_value)
                }
            };
            steps.push(step);
        }
        Ok(steps)
    }
}

/// THE SOURCELESS INNER FORM has no OUTER base relation — the body resolves
/// against the enclosing row — but its interior still supplies one of its own:
/// the leading comma is the no-op base made visible, and the member after it
/// is what the compression reduces. An interior that names none reduces
/// nothing.
fn sourceless_needs_a_base() -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "compression/sourceless_base",
        "a sourceless inner form's interior supplies its own base relation; \
         this one names none, so the compression has nothing to reduce",
        "write the relation the reduction consumes: `_:(, _(1; 2) ~> sum:(|1|))`",
    )
}

/// The closed binary vocabulary, decoded at the one boundary that sees the
/// authored glyph.
fn binary_operator(text: &str) -> Option<crate::pipeline::asts::vocabulary::BinOp> {
    use crate::pipeline::asts::vocabulary::BinOp;

    Some(match text {
        "+" => BinOp::Add,
        "-" => BinOp::Sub,
        "*" => BinOp::Mul,
        "/" => BinOp::Div,
        "%" => BinOp::Mod,
        // `++` concatenates. `||` is SQL's spelling for it and is DelightQL's
        // positional union, so the two cannot share a reader.
        "++" => BinOp::Concat,
        _ => return None,
    })
}

/// The AUTHORED comparison vocabulary. `=` and `!=` are the language's
/// null-safe pair. The engine's own three-valued equality (`CmpOp::Equal`,
/// `CmpOp::NotEqual`) has no authored glyph: it is constructed by the
/// compiler for correspondence and by the prelude predicates `sql_eq` /
/// `sql_ne`, and a source spelling `==` / `!==` is a retired token the parse
/// diagnoses, never one this decoder admits. `<>` is a TARGET's spelling of
/// the traditional inequality, written by the generator on the dialects that
/// want it and never read from a source.
pub(crate) fn comparison_operator(text: &str) -> Option<crate::pipeline::asts::vocabulary::CmpOp> {
    use crate::pipeline::asts::vocabulary::CmpOp;

    Some(match text {
        "=" => CmpOp::NullSafeEqual,
        "!=" => CmpOp::NullSafeNotEqual,
        "<" => CmpOp::LessThan,
        "<=" => CmpOp::LessThanOrEqual,
        ">" => CmpOp::GreaterThan,
        ">=" => CmpOp::GreaterThanOrEqual,
        _ => return None,
    })
}

/// The characters between a regex's slashes.
pub(super) fn regex_interior(text: &str) -> &str {
    text.strip_prefix('/')
        .and_then(|rest| rest.strip_suffix('/'))
        .unwrap_or(text)
}

/// Escapes belong to template text and to nothing else: a plain string
/// literal carries its bytes as written.
fn template_escapes(text: &str) -> Result<String> {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(character) = chars.next() {
        if character != '\\' {
            out.push(character);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('\\') => out.push('\\'),
            Some('q') => out.push('\''),
            Some('Q') => out.push('"'),
            Some(other) => {
                return Err(DelightQLError::parse_error_categorized(
                    "template/escape",
                    format!(
                        "unrecognized escape '\\{other}' in a template; the escapes are \\n \\t \\\\ \\q \\Q"
                    ),
                ))
            }
            None => {
                return Err(DelightQLError::parse_error_categorized(
                    "template/escape",
                    "a template ends in a trailing backslash",
                ))
            }
        }
    }
    Ok(out)
}
