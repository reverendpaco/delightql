// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The echo writer.
//!
//! What the formatter takes no position on is reproduced from the AUTHOR'S
//! tokens. Token identity is then a property of the writer rather than a
//! hope: the texts come from the authored stream in authored order, and the
//! only thing invented is the whitespace between them.

use delightql_cst::cst::{Kind, TypedNode};
use delightql_cst::Token;

use super::core::Formatter;
use crate::registry::{policy, Policy};
use crate::rules::CommaJoin;

impl<'t> Formatter<'t> {
    /// Echo one node's authored tokens.
    pub(super) fn verbatim<T: TypedNode<'t>>(&mut self, node: T) {
        if let Some(kind) = Kind::from_str(node.node().kind()) {
            match policy(kind) {
                Policy::Verbatim => {}
                // The registry promised this kind an arm and it arrived here
                // instead: the layout the registry advertises is missing, so
                // say so rather than emit a silently flatter query.
                Policy::LaidOut | Policy::Unplaced => self.flag_unhandled(kind.as_str()),
            }
        } else {
            self.flag_unhandled(node.node().kind());
        }
        self.echo_range(node);
    }

    /// Echo a node without consulting the registry — for a node an arm has
    /// already claimed and chosen to reproduce.
    pub(super) fn echo<T: TypedNode<'t>>(&mut self, node: T) {
        self.echo_range(node);
    }

    fn echo_range<T: TypedNode<'t>>(&mut self, node: T) {
        let Some(range) = self.tree.byte_range(node) else {
            // Wholly inside host framing: not the author's, and not text any
            // coordinate could describe.
            return;
        };
        self.claim(range.start, range.end);
        let text = self.rendered(range.start, range.end);
        self.output.write(&text);
    }

    /// Emit whatever the author wrote between the last emitted byte and this
    /// node, then record the node as emitted.
    ///
    /// EXTRAS RIDE THE GAPS. A comment attaches to no production — the grammar
    /// skips it before any token — so no typed accessor hands one over. What
    /// finds it is its POSITION: text standing between two claimed spans is
    /// text the author wrote and the visitor has not yet emitted.
    pub(super) fn claim(&mut self, start: usize, end: usize) {
        self.flush_extras(start);
        self.emitted = self.emitted.max(end);
    }

    pub(super) fn flush_extras(&mut self, upto: usize) {
        if upto <= self.emitted {
            return;
        }
        let from = self.emitted;
        let first = self.extras.partition_point(|e| e.start < from);
        let last = self.extras.partition_point(|e| e.end <= upto);
        for i in first..last {
            let (text, line_ended) = {
                let extra = &self.extras[i];
                (extra.text.clone(), extra.runs_to_end_of_line)
            };
            if self.output.current_line_length() > 0 {
                self.output.write("  ");
            }
            self.output.write(&text);
            // A comment that runs to end of line takes whatever follows it
            // with it unless the line ends here.
            if line_ended {
                self.output.newline();
            }
        }
        // Everything else in the gap is punctuation the arm around it writes
        // itself. A token an arm FORGOT is not lost quietly: the token law
        // reparses the output and the whole format becomes a pass-through.
        self.emitted = self.emitted.max(upto);
    }

    /// Emit anything standing before a node the visitor is about to lay out,
    /// so the layout's own decision is made at the position the author's text
    /// actually leaves it at.
    pub(super) fn flush_before<T: TypedNode<'t>>(&mut self, node: T) {
        if let Some(range) = self.tree.byte_range(node) {
            self.flush_extras(range.start);
        }
    }

    /// Claim a whole node for an arm that builds its own text from the node's
    /// parts rather than writing it through the echo path.
    pub(super) fn claim_node<T: TypedNode<'t>>(&mut self, node: T) {
        if let Some(range) = self.tree.byte_range(node) {
            self.claim(range.start, range.end);
        }
    }

    /// Whether an authored span holds a comment that runs to end of line. A
    /// list layout that re-joined its items around one would bury everything
    /// after it.
    pub(super) fn holds_line_comment<T: TypedNode<'t>>(&self, node: T) -> bool {
        match self.tree.byte_range(node) {
            Some(range) => self.extras.iter().any(|e| {
                e.runs_to_end_of_line && e.start >= range.start && e.end <= range.end
            }),
            None => false,
        }
    }

    /// The tokens of an authored span, laid out on one line where that is
    /// safe and reproduced byte-for-byte where it is not.
    ///
    /// TWO BRANCHES, ONE FIXED POINT. A span whose authored text is already
    /// one line and carries no line comment is re-spaced; anything else is
    /// echoed as the author wrote it. Either way the second pass sees text of
    /// the same shape and takes the same branch, which is what idempotence
    /// costs here.
    pub(super) fn rendered(&self, start: usize, end: usize) -> String {
        let source = self.tree.source();
        let slice = &source[start..end];
        let window = self.window(start, end);
        if window.is_empty() {
            return String::new();
        }
        // A comment running to end of line pulls whatever follows it into
        // itself when a span is re-spaced onto one line — a different query,
        // which the token law would catch, but as a pass-through rather than
        // as output.
        if slice.contains('\n')
            || self
                .extras
                .iter()
                .any(|e| e.runs_to_end_of_line && e.start >= start && e.end <= end)
        {
            return slice.to_string();
        }

        let mut out = String::new();
        let mut prev: Option<&Token> = None;
        for token in window {
            if let Some(previous) = prev {
                if self.wants_space(previous, token) {
                    out.push(' ');
                }
            }
            out.push_str(&token.text);
            prev = Some(token);
        }
        out
    }

    /// Whether two adjacent authored tokens are separated in the output.
    ///
    /// WHITESPACE IS SPELLING, NOT IDENTITY. `people( * )` and `people(*)`
    /// are one term, and the canonicalizer compares canonical spellings
    /// byte-for-byte — so the answer here cannot be "whatever the author
    /// did" wherever the language settles the question. Where it does not,
    /// the author's own gap stands.
    fn wants_space(&self, prev: &Token, next: &Token) -> bool {
        // A separator hugs what it follows and is followed by the knob's
        // spelling. Removing a gap around one can fuse nothing: no token
        // begins or ends with a comma or semicolon.
        if is_separator(&next.text) {
            return false;
        }
        if is_separator(&prev.text) {
            return self.config.comma_join_args != CommaJoin::Tight;
        }
        // Brackets hug their contents.
        if opens(&prev.text) || closes(&next.text) {
            return false;
        }
        // An opener that ACCESSES the value before it hugs it — `orders(*)`,
        // `sum:(total)`, `users(*).(id)`. After a word-shaped keyword the
        // same paren is a separate operand: `x in ("a"; "b")`.
        if opens(&next.text) && value_like(&prev.text) && !self.is_keyword(prev) {
            return false;
        }
        // An infix operator takes room on both sides whatever the author
        // wrote: `age>=30` and `age >= 30` are the same term.
        if self.is_infix(prev) || self.is_infix(next) {
            return true;
        }
        // Everything else is the author's. Preserving gap EXISTENCE is what
        // keeps `token.immediate` boundaries — the effect mark, the mutation
        // mark, the outer mark — attached to what they modify.
        prev.end < next.start
    }

    fn is_infix(&self, token: &Token) -> bool {
        self.infix
            .binary_search(&(token.start, token.end))
            .is_ok()
    }

    fn is_keyword(&self, token: &Token) -> bool {
        self.keywords
            .binary_search(&(token.start, token.end))
            .is_ok()
    }

    /// The authored tokens lying inside a span.
    pub(super) fn window(&self, start: usize, end: usize) -> &[Token] {
        let first = self.tokens.partition_point(|t| t.start < start);
        let last = self.tokens.partition_point(|t| t.end <= end);
        if last <= first {
            return &[];
        }
        &self.tokens[first..last]
    }

    /// The width an echoed span would occupy on one line, for a fit decision
    /// that must not consult the previous pass's layout.
    pub(super) fn echo_width<T: TypedNode<'t>>(&self, node: T) -> usize {
        match self.tree.byte_range(node) {
            Some(range) => self
                .rendered(range.start, range.end)
                .lines()
                .map(str::len)
                .max()
                .unwrap_or(0),
            None => 0,
        }
    }
}

fn is_separator(text: &str) -> bool {
    text == "," || text == ";"
}

/// A token that OPENS a bracketed region — the bare brackets and every
/// compound spelling the grammar gives them (`_(`, `$(`, `.(`, `:(`, `+_(`…).
fn opens(text: &str) -> bool {
    matches!(text.chars().next_back(), Some('(' | '[' | '{'))
}

/// A token that CLOSES one. The annotation and comment terminators (`~~)`,
/// `*/)`) are deliberately not here: they stand off from their body.
fn closes(text: &str) -> bool {
    matches!(text, ")" | "]" | "}")
}

/// A token an access can attach to: a name, a literal, a glob, or a closing
/// bracket ending the expression the access applies to.
fn value_like(text: &str) -> bool {
    match text.chars().next_back() {
        Some(c) => c.is_alphanumeric() || matches!(c, '_' | '`' | '"' | '\'' | ')' | ']' | '}' | '*'),
        None => false,
    }
}
