// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The chain: continuations, and the two decisions that give a query its
//! shape — where a pipe breaks, and whether the comma clauses cascade.

use anyhow::Result;
use delightql_cst::cst::{self, TypedNode};

use super::core::Formatter;
use crate::rules::{BreakMode, ClauseBreak};

impl<'t> Formatter<'t> {
    /// One continuation. The two families are the grammar's own, and Rust
    /// keeps the match complete against them.
    pub(super) fn continuation(&mut self, continuation: cst::Continuation<'t>) -> Result<()> {
        match continuation {
            cst::Continuation::BinaryContinuation(binary) => self.binary_continuation(binary),
            cst::Continuation::OperatorContinuation(operator) => {
                self.operator_continuation(operator)
            }
        }
    }

    fn binary_continuation(&mut self, binary: cst::BinaryContinuation<'t>) -> Result<()> {
        match binary {
            cst::BinaryContinuation::CommaContinuation(comma) => self.comma_continuation(comma),
            // A peer join: two relations meet, the operator spelled between
            // them.
            cst::BinaryContinuation::MinusContinuation(minus) => {
                for child in minus.children() {
                    match child {
                        cst::MinusContinuationChild::MinusSigil(sigil) => self.sigil(sigil),
                        cst::MinusContinuationChild::Grelex(peer) => self.echo(peer),
                    }
                }
                Ok(())
            }
            cst::BinaryContinuation::UnionLikeContinuation(union) => {
                match union {
                    cst::UnionLikeContinuation::CorrespondingUnionContinuation(c) => {
                        for child in c.children() {
                            match child {
                                cst::CorrespondingUnionContinuationChild::CorrespondingUnionSigil(
                                    sigil,
                                ) => self.sigil(sigil),
                                cst::CorrespondingUnionContinuationChild::Grelex(peer) => {
                                    self.echo(peer)
                                }
                            }
                        }
                    }
                    cst::UnionLikeContinuation::PositionalUnionContinuation(c) => {
                        for child in c.children() {
                            match child {
                                cst::PositionalUnionContinuationChild::PositionalUnionSigil(
                                    sigil,
                                ) => self.sigil(sigil),
                                cst::PositionalUnionContinuationChild::Grelex(peer) => {
                                    self.echo(peer)
                                }
                            }
                        }
                    }
                    cst::UnionLikeContinuation::SmartUnionContinuation(c) => {
                        for child in c.children() {
                            match child {
                                cst::SmartUnionContinuationChild::SmartUnionSigil(sigil) => {
                                    self.sigil(sigil)
                                }
                                cst::SmartUnionContinuationChild::Grelex(peer) => self.echo(peer),
                            }
                        }
                    }
                }
                Ok(())
            }
            // `A(*) &(::ctx) B(*)` — the sigil, its optional context, and the
            // peer term. The context binds to the operator, not to a space.
            cst::BinaryContinuation::EdgeContinuation(edge) => {
                self.output.write(" ");
                if let Some(operator) = edge.operator() {
                    self.echo(operator);
                }
                if let Some(context) = edge.context() {
                    self.echo(context);
                }
                self.output.write(" ");
                if let Some(term) = edge.term() {
                    self.echo(term);
                }
                Ok(())
            }
        }
    }

    /// A peer-join sigil: spaces on both sides, so the two relations stay
    /// legible as peers.
    fn sigil<T: TypedNode<'t>>(&mut self, sigil: T) {
        self.output.write(" ");
        self.echo(sigil);
        self.output.write(" ");
    }

    fn operator_continuation(&mut self, operator: cst::OperatorContinuation<'t>) -> Result<()> {
        match operator {
            cst::OperatorContinuation::PipeContinuation(pipe) => self.pipe_continuation(pipe),
            // A postfix operator EXTENDS the expression to its left, so it
            // rides that line with no space: `users(*)*`, `users(*).items(*)`.
            cst::OperatorContinuation::PostfixOperator(postfix) => {
                match postfix {
                    cst::PostfixOperator::DomainActivate(n) => self.echo(n),
                    cst::PostfixOperator::Drill(n) => self.echo(n),
                    cst::PostfixOperator::Meta(n) => self.echo(n),
                    cst::PostfixOperator::SignedWitness(n) => self.echo(n),
                    cst::PostfixOperator::Using(n) => self.echo(n),
                    cst::PostfixOperator::Witness(n) => self.echo(n),
                }
                Ok(())
            }
            // `~> {…}` — the one reduction position outside a group.
            cst::OperatorContinuation::SingletonReduction(reduction) => {
                self.output.write(" ");
                self.echo(reduction);
                Ok(())
            }
            // `as name` carries its own keyword; the space is ours.
            cst::OperatorContinuation::StageName(name) => {
                self.output.write(" ");
                self.echo(name);
                Ok(())
            }
            // `as name(slots)` — the same keyword, the same space.
            cst::OperatorContinuation::ArgumentativeStage(stage) => {
                self.output.write(" ");
                self.echo(stage);
                Ok(())
            }
        }
    }

    /// A pipe: break before it, or keep it inline while the segment fits.
    ///
    /// THE CHAIN DECIDES ONCE. Under `fit`, the first pipe that does not fit
    /// breaks and every later pipe in the same query breaks with it — a chain
    /// half inline and half broken reads as two chains.
    fn pipe_continuation(&mut self, pipe: cst::PipeContinuation<'t>) -> Result<()> {
        let mut operator = None;
        let mut form = None;
        for child in pipe.children() {
            match child {
                cst::PipeContinuationChild::PipeOperator(op) => operator = Some(op),
                cst::PipeContinuationChild::PostPipeForm(f) => form = Some(f),
            }
        }

        // A comment standing before the pipe ends the line by itself; the
        // decision below then has nothing left to fit onto.
        self.flush_before(pipe);
        let operator_width = operator.map_or(2, |op| self.echo_width(op));
        let inline = self.output.current_line_length() > 0
            && self.config.pipe_break == BreakMode::Fit
            && !self.pipe_chain_broken
            && match form {
                Some(f) => {
                    let segment = self.measure(|this| this.post_pipe_form(f));
                    self.output.current_line_length() + 1 + operator_width + 1 + segment
                        <= self.config.pipe_break_width
                }
                None => true,
            };

        if inline {
            self.output.write(" ");
        } else {
            self.pipe_chain_broken = true;
            self.output
                .newline_with_indent(self.base_indent + self.config.pipe_indent);
        }
        if let Some(op) = operator {
            self.echo(op);
        }
        self.output.write(" ");
        if let Some(f) = form {
            self.post_pipe_form(f)?;
        }
        Ok(())
    }

    /// A comma clause. `each` breaks every clause; `cascade` measures the
    /// whole remaining chain ONCE and then treats it as one unit; `fit` is
    /// greedy, clause by clause.
    fn comma_continuation(&mut self, comma: cst::CommaContinuation<'t>) -> Result<()> {
        self.flush_before(comma);
        let member = comma.member();
        let would_exceed = match self.config.comma_clause_break {
            ClauseBreak::Each => true,
            ClauseBreak::Cascade => {
                if self.comma_chain_broken.is_none() {
                    // Probe the JOINED rendering with the decision forced, so
                    // the measure depends on the CST and the config alone and
                    // never on the previous pass's layout.
                    self.comma_chain_broken = Some(false);
                    let width = self.measure(|this| this.comma_member(member));
                    let fits = self.output.current_line_length() + 2 + width
                        <= self.config.pipe_break_width;
                    self.comma_chain_broken = Some(!fits);
                }
                self.comma_chain_broken == Some(true)
            }
            ClauseBreak::Fit => {
                let width = self.measure(|this| this.comma_member(member));
                self.output.current_line_length() + 2 + width > self.config.continuation_length
            }
        };

        if self.output.current_line_length() == 0 {
            // A comment already broke the line; the comma leads its clause.
            self.output
                .write(&" ".repeat(self.base_indent + self.config.continuation_indent));
            self.output.write(", ");
        } else if would_exceed
            && self.output.current_line_length() > self.config.continuation_indent
        {
            self.output.write(",");
            self.output
                .newline_with_indent(self.base_indent + self.config.continuation_indent);
        } else {
            self.output.write(", ");
        }
        self.comma_member(member)
    }

    fn comma_member(&mut self, member: Option<cst::CommaContinuationMember<'t>>) -> Result<()> {
        // FN.6 — the comma admits a relation, a truth expression, a
        // destructure, an ordering, or a bound. None of the five needs a
        // layout of its own; what needed deciding was the break above.
        match member {
            Some(cst::CommaContinuationMember::GrelexLikeMember(n)) => self.echo(n),
            Some(cst::CommaContinuationMember::TruthExpression(n)) => self.echo(n),
            Some(cst::CommaContinuationMember::DestructureRelex(n)) => self.echo(n),
            Some(cst::CommaContinuationMember::Ordering(n)) => self.echo(n),
            Some(cst::CommaContinuationMember::RowBound(n)) => self.echo(n),
            None => {}
        }
        Ok(())
    }

    pub(super) fn effrelex(&mut self, effrelex: cst::Effrelex<'t>) -> Result<()> {
        if let Some(block) = effrelex.let_block() {
            self.let_block(block)?;
        }
        if let Some(chain) = effrelex.chain() {
            self.effect_chain(chain)?;
        }
        Ok(())
    }

    /// An effect chain.
    ///
    /// Its alternatives share fields — a pipe's source and a join's left both
    /// stand left of their operator — so the faithful reading is the authored
    /// ORDER. Each child is claimed by a typed cast, in the order the
    /// alternatives can produce; the chain's shape is the pipe's and the
    /// connective's, and every other member is a form to echo.
    pub(super) fn effect_chain(&mut self, chain: cst::EffectChain<'t>) -> Result<()> {
        let node = chain.node();
        let mut cursor = node.walk();
        let children: Vec<_> = node.children(&mut cursor).filter(|c| c.is_named()).collect();
        for child in children {
            if let Some(inner) = cst::EffectChain::cast(child) {
                self.effect_chain(inner)?;
            } else if let Some(body) = cst::LetFreeRelex::cast(child) {
                self.let_free_relex(body)?;
            } else if let Some(continuation) = cst::Continuation::cast(child) {
                self.continuation(continuation)?;
            } else if let Some(annotation) = cst::Annotation::cast(child) {
                self.annotation(annotation);
            } else if let Some(op) = cst::PipeOperator::cast(child) {
                // The effect pipe takes the same break decision as the pure
                // one: it is the same link in the same chain.
                self.effect_pipe(op);
            } else if let Some(op) = cst::UnwrapPipeOperator::cast(child) {
                self.effect_pipe(op);
            } else if let Some(connective) = cst::BinaryConnective::cast(child) {
                self.sigil(connective);
            } else if let Some(any) = cst::AnyNode::cast(child) {
                self.echo(any);
            }
        }
        Ok(())
    }

    fn effect_pipe<T: TypedNode<'t>>(&mut self, operator: T) {
        let width = self.echo_width(operator);
        let inline = self.config.pipe_break == BreakMode::Fit
            && !self.pipe_chain_broken
            && self.output.current_line_length() + 1 + width <= self.config.pipe_break_width;
        if inline {
            self.output.write(" ");
        } else {
            self.pipe_chain_broken = true;
            self.output
                .newline_with_indent(self.base_indent + self.config.pipe_indent);
        }
        self.echo(operator);
        self.output.write(" ");
    }
}
