// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use anyhow::Result;
use delightql_cst::cst::{self, TypedNode};
use delightql_cst::{SyntaxTree, Token};

use crate::builder::OutputBuilder;
use crate::rules::FormatConfig;

pub struct Formatter<'t> {
    pub(super) tree: &'t SyntaxTree,
    /// The authored token stream, once. Every echoed region is a window into
    /// it, found by authored span.
    pub(super) tokens: Vec<Token>,
    /// The EXTRAS, in authored order. The grammar skips them before any
    /// token, so no typed accessor hands one over and no arm can be given
    /// one; position is the only thing that finds them.
    pub(super) extras: Vec<Extra>,
    /// Authored spans of the INFIX operators, so the echo writer can space
    /// them whatever the author did. Found by kind — `a>=b` and `a >= b` are
    /// the same term, and a canonical spelling that kept the difference
    /// would make two.
    pub(super) infix: Vec<(usize, usize)>,
    /// Authored spans of the word-shaped keywords. An opening paren binds
    /// TIGHT to a value it accesses and loosely to a keyword before it:
    /// `orders(*)`, but `x in ("a"; "b")`.
    pub(super) keywords: Vec<(usize, usize)>,
    pub(super) output: OutputBuilder,
    pub(super) config: FormatConfig,
    /// Indentation the enclosing context established — a CTE body's, so a
    /// pipe inside it breaks relative to the binding rather than the margin.
    pub(super) base_indent: usize,
    /// Under `pipe_break=fit`, set once a pipe in the current query's chain
    /// breaks; the rest then cascade. A chain half inline and half broken
    /// reads as two chains.
    pub(super) pipe_chain_broken: bool,
    /// Under `comma_clause_break=cascade`, the current query's ONE group
    /// decision. `None` until the first comma measures the chain.
    pub(super) comma_chain_broken: Option<bool>,
    /// The authored byte through which output has been produced. Extras
    /// attach to no production, so what finds them is the gap between this
    /// and the next claimed span.
    pub(super) emitted: usize,
    /// Set when a node reached the echo writer that the registry says has a
    /// layout arm, or that the registry does not place at all.
    pub(crate) hit_unknown: bool,
    pub(crate) unknown_kind: Option<String>,
}

impl<'t> Formatter<'t> {
    pub fn new(tree: &'t SyntaxTree, config: FormatConfig) -> Self {
        Self {
            tokens: tree.tokens(),
            extras: collect_extras(tree),
            infix: collect_spans(tree, |kind| {
                matches!(kind, cst::Kind::CmpOp | cst::Kind::BinaryOp)
            }),
            keywords: collect_spans(tree, |kind| {
                matches!(
                    kind,
                    cst::Kind::AndKeyword
                        | cst::Kind::AsKeyword
                        | cst::Kind::AscKeyword
                        | cst::Kind::DescKeyword
                        | cst::Kind::InKeyword
                        | cst::Kind::NotKeyword
                        | cst::Kind::OfKeyword
                        | cst::Kind::OrKeyword
                )
            }),
            tree,
            output: OutputBuilder::new(),
            config,
            base_indent: 0,
            pipe_chain_broken: false,
            comma_chain_broken: None,
            emitted: 0,
            hit_unknown: false,
            unknown_kind: None,
        }
    }

    pub fn output(self) -> String {
        self.output.build()
    }

    /// Record that a node arrived somewhere the registry says it should not
    /// have. The output may now be missing layout the registry promised, so
    /// the caller returns the input unchanged and names the kind.
    pub(super) fn flag_unhandled(&mut self, kind: &str) {
        if !self.hit_unknown {
            self.unknown_kind = Some(kind.to_string());
        }
        self.hit_unknown = true;
    }

    /// Trial-format into a scratch buffer and return the widest line.
    ///
    /// A break decision must measure what the formatter WILL emit. Measuring
    /// the SOURCE measures the previous pass's layout, and a decision that
    /// reads its own last output is not a fixed point.
    pub(super) fn measure<F>(&mut self, f: F) -> usize
    where
        F: FnOnce(&mut Self) -> Result<()>,
    {
        let saved = std::mem::replace(&mut self.output, OutputBuilder::new());
        let pipe = self.pipe_chain_broken;
        let comma = self.comma_chain_broken;
        let emitted = self.emitted;
        let _ = f(self);
        self.pipe_chain_broken = pipe;
        self.comma_chain_broken = comma;
        self.emitted = emitted;
        let probe = std::mem::replace(&mut self.output, saved);
        probe.build().lines().map(str::len).max().unwrap_or(0)
    }

    /// The whole submission.
    ///
    /// The entrance is NAMED by the caller, never inferred from which parse
    /// succeeded, so the branch here is a check on the tree the entrance
    /// produced rather than a classification of the text.
    pub fn format_root(&mut self) -> Result<Branch> {
        let Some(branch) = self.tree.root_branch() else {
            // A source declaring nothing is lawful — but comments are still
            // the author's, and a comments-only file must come back whole.
            self.flush_extras(self.tree.source().len());
            return Ok(Branch::Formatted);
        };
        match branch {
            cst::SourceFileChild::QuerySequenceRoot(root) => {
                self.query_sequence_root(root)?;
                self.flush_extras(self.tree.source().len());
                Ok(Branch::Formatted)
            }
            cst::SourceFileChild::DefinitionFile(_) => Ok(Branch::DefinitionFile),
            cst::SourceFileChild::CompanionCellRoot(_) => Ok(Branch::CompanionCell),
        }
    }

    fn query_sequence_root(&mut self, root: cst::QuerySequenceRoot<'t>) -> Result<()> {
        let mut prev_end: Option<usize> = None;
        for child in root.children() {
            let range = self.tree.byte_range(child);
            if let (Some(prev), Some(range)) = (prev_end, range.as_ref()) {
                self.blank_line_between(prev, range.start);
            }
            prev_end = range.map(|r| r.end);
            match child {
                // The utility file's own header. It is AUTHORED vocabulary,
                // so it survives formatting like any other token; a synthetic
                // one has no authored span and never reaches here.
                cst::QuerySequenceRootChild::QuerySequenceHeader(header) => {
                    self.echo(header);
                    self.output.newline();
                }
                cst::QuerySequenceRootChild::QuerySequence(sequence) => {
                    self.query_sequence(sequence)?;
                }
            }
        }
        Ok(())
    }

    fn query_sequence(&mut self, sequence: cst::QuerySequence<'t>) -> Result<()> {
        let mut prev_end: Option<usize> = None;
        for form in sequence.children() {
            let range = self.tree.byte_range(form);
            if let (Some(prev), Some(range)) = (prev_end, range.as_ref()) {
                self.blank_line_between(prev, range.start);
            }
            prev_end = range.map(|r| r.end);
            // Each form starts its own chain: the cascade decisions belong to
            // one query and must not leak into the next.
            self.pipe_chain_broken = false;
            self.comma_chain_broken = None;
            match form {
                cst::QuerySequenceChild::Relex(relex) => self.relex(relex)?,
                cst::QuerySequenceChild::Effrelex(effrelex) => self.effrelex(effrelex)?,
            }
            self.output.newline();
        }
        Ok(())
    }

    /// One blank line where the author left one or more — their grouping of a
    /// sequence, which no other signal carries.
    fn blank_line_between(&mut self, prev_end: usize, next_start: usize) {
        if self.config.blank_lines != crate::rules::BlankLines::Preserve {
            return;
        }
        let gap = &self.tree.source()[prev_end..next_start];
        if gap.matches('\n').count() >= 2 {
            self.output.blank_line();
        }
    }

    pub(super) fn relex(&mut self, relex: cst::Relex<'t>) -> Result<()> {
        if let Some(block) = relex.let_block() {
            self.let_block(block)?;
        }
        if let Some(body) = relex.body() {
            self.let_free_relex(body)?;
        }
        Ok(())
    }

    /// A chain: its head, then its continuations and annotations in authored
    /// order.
    pub(super) fn let_free_relex(&mut self, body: cst::LetFreeRelex<'t>) -> Result<()> {
        // FN.41 — a leading outer waits for its peer. The three head fields
        // are alternatives, and the peer follows the leading form directly.
        if let Some(outer) = body.leading_outer() {
            self.verbatim(outer);
            if let Some(name) = body.leading_outer_name() {
                self.output.write(" ");
                self.verbatim(name);
            }
            if let Some(peer) = body.peer() {
                self.output.write(", ");
                self.verbatim(peer);
            }
        } else if let Some(grelex) = body.grelex() {
            self.verbatim(grelex);
        }
        for child in body.children() {
            match child {
                cst::LetFreeRelexChild::Continuation(continuation) => {
                    self.continuation(continuation)?;
                }
                cst::LetFreeRelexChild::Annotation(annotation) => {
                    self.annotation(annotation);
                }
            }
        }
        Ok(())
    }

    /// An annotation decorates a POSITION and never changes the relex around
    /// it, so its placement is layout and nothing else.
    pub(super) fn annotation<T: TypedNode<'t>>(&mut self, annotation: T) {
        if self.config.annotation_placement == crate::rules::Placement::OwnLine {
            self.output
                .newline_with_indent(self.base_indent + self.config.pipe_indent);
        } else {
            self.output.write(" ");
        }
        self.verbatim(annotation);
    }
}

/// One comment or session tool, with its authored span.
pub(super) struct Extra {
    pub(super) text: String,
    pub(super) start: usize,
    pub(super) end: usize,
    /// Whether the spelling runs to end of line, so nothing may follow it on
    /// the line the formatter emits it on.
    pub(super) runs_to_end_of_line: bool,
}

/// The extras a tree carries, found by KIND rather than by spelling.
fn collect_extras(tree: &SyntaxTree) -> Vec<Extra> {
    let mut extras: Vec<Extra> = delightql_cst::walk(tree)
        .filter_map(|node| {
            let kind = node.typed_kind()?;
            // The grammar's own `extras` set. `comment` is the one that runs
            // to end of line; the session tools are delimited.
            let runs_to_end_of_line = match kind {
                cst::Kind::Comment => true,
                cst::Kind::SmartComment | cst::Kind::StopPoint | cst::Kind::DebugPoint => false,
                _ => return None,
            };
            let range = tree.byte_range(node)?;
            Some(Extra {
                text: tree.text(node).to_string(),
                start: range.start,
                end: range.end,
                runs_to_end_of_line,
            })
        })
        .collect();
    extras.sort_by_key(|extra| extra.start);
    extras
}

/// The authored spans of every node whose kind the predicate names.
fn collect_spans(tree: &SyntaxTree, want: impl Fn(cst::Kind) -> bool) -> Vec<(usize, usize)> {
    let mut spans: Vec<(usize, usize)> = delightql_cst::walk(tree)
        .filter(|node| node.typed_kind().is_some_and(&want))
        .filter_map(|node| tree.byte_range(node).map(|r| (r.start, r.end)))
        .collect();
    spans.sort_unstable();
    spans
}

/// What the named entrance's tree turned out to hold.
///
/// `dql format` speaks the utility form. The other two branches are not
/// failures of the input — they are forms this tool does not lay out yet, and
/// saying which one keeps the caller's diagnostic honest.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Branch {
    Formatted,
    DefinitionFile,
    CompanionCell,
}
