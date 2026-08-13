// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! What stands on the right of a pipe.
//!
//! Four of these forms carry a list wide enough to need a decision — the
//! projection, the group's two halves, the rename, and the map cover. The
//! rest are echoed: their layout is already the author's.

use anyhow::Result;
use delightql_cst::cst::{self, TypedNode};

use super::core::Formatter;

impl<'t> Formatter<'t> {
    pub(super) fn post_pipe_form(&mut self, form: cst::PostPipeForm<'t>) -> Result<()> {
        match form {
            cst::PostPipeForm::PipeOperation(operation) => self.pipe_operation(operation),
            // Ordering, bounds, narrowing, reposition — one line each.
            cst::PostPipeForm::PipeStructural(structural) => {
                match structural {
                    cst::PipeStructural::NarrowingAccess(n) => self.echo(n),
                    cst::PipeStructural::NarrowingDestructure(n) => self.echo(n),
                    cst::PipeStructural::Ordering(n) => self.echo(n),
                    cst::PipeStructural::Reposition(n) => self.echo(n),
                }
                Ok(())
            }
            cst::PostPipeForm::PureInvocation(invocation) => {
                self.echo(invocation);
                Ok(())
            }
        }
    }

    fn pipe_operation(&mut self, operation: cst::PipeOperation<'t>) -> Result<()> {
        match operation {
            cst::PipeOperation::Project(project) => {
                if self.holds_line_comment(project) {
                    self.echo(project);
                    return Ok(());
                }
                self.projection(project, "(", ")")
            }
            cst::PipeOperation::Embed(embed) => {
                self.echo(embed);
                Ok(())
            }
            cst::PipeOperation::Group(group) => self.group(group),
            cst::PipeOperation::Rename(rename) => self.rename(rename),
            cst::PipeOperation::MapCover(cover) => self.map_cover(cover),
            cst::PipeOperation::EmbedMapCover(cover) => {
                self.echo(cover);
                Ok(())
            }
            cst::PipeOperation::ProjectOut(out) => {
                self.echo(out);
                Ok(())
            }
            cst::PipeOperation::Transform(transform) => {
                self.echo(transform);
                Ok(())
            }
        }
    }

    /// A projection list, ALIGNED UNDER ITS OPENING PAREN when it does not
    /// fit. The alignment column is where the paren landed, so a broken list
    /// still reads as one list rather than as a new clause.
    fn projection(&mut self, project: cst::Project<'t>, open: &str, close: &str) -> Result<()> {
        self.claim_node(project);
        let items: Vec<_> = project
            .children()
            .filter_map(|child| match child {
                cst::ProjectChild::OutItem(item) => Some(item),
                // The separators are the layout's to place.
                cst::ProjectChild::CommaSigil(_) => None,
            })
            .collect();
        let rendered: Vec<String> = items.iter().map(|item| self.echoed(*item)).collect();
        self.aligned_list(&rendered, open, close);
        Ok(())
    }

    /// `%(keys ~> aggregates)` — the group reduction.
    ///
    /// Broken, the arrow stands on its own line between the two halves: the
    /// keys are what the rows collapse BY and the reductions are what
    /// survives, and the shape should show which is which.
    fn group(&mut self, group: cst::Group<'t>) -> Result<()> {
        // A list re-joined around a `//` comment buries everything after it.
        // The author's own layout is the only safe one there.
        if self.holds_line_comment(group) {
            self.echo(group);
            return Ok(());
        }
        self.claim_node(group);
        let mut keys: Vec<String> = Vec::new();
        let mut reductions: Vec<String> = Vec::new();
        let mut arrow: Option<String> = None;
        let mut percent: Option<String> = None;
        for child in group.children() {
            match child {
                cst::GroupChild::PercentSigil(sigil) => percent = Some(self.echoed(sigil)),
                cst::GroupChild::ReductionSigil(sigil) => arrow = Some(self.echoed(sigil)),
                cst::GroupChild::GroupKey(key) => keys.push(self.echoed(key)),
                cst::GroupChild::ReductionItem(item) => reductions.push(self.echoed(item)),
                cst::GroupChild::CommaSigil(_) => {}
            }
        }
        if let Some(sigil) = percent {
            self.output.write(&sigil);
        }

        let inline_width: usize = keys.iter().chain(&reductions).map(String::len).sum::<usize>()
            + 2 * keys.len().saturating_sub(1)
            + 2 * reductions.len().saturating_sub(1)
            + arrow.as_ref().map_or(0, |a| a.len() + 2)
            + 2;
        if arrow.is_none() || inline_width <= self.config.projection_length {
            self.output.write("(");
            self.output.write(&keys.join(", "));
            if let Some(a) = &arrow {
                if !keys.is_empty() {
                    self.output.write(" ");
                }
                self.output.write(a);
                self.output.write(" ");
            }
            self.output.write(&reductions.join(", "));
            self.output.write(")");
            return Ok(());
        }

        self.output.write("(");
        let column = self.output.current_line_length();
        self.write_items(&keys, column);
        self.output
            .newline_with_indent(column + self.config.aggregation_arrow_indent);
        self.output.write(arrow.as_deref().unwrap_or("~>"));
        self.output.newline_with_indent(column);
        self.write_items(&reductions, column);
        self.output.write(")");
        Ok(())
    }

    fn rename(&mut self, rename: cst::Rename<'t>) -> Result<()> {
        // A list re-joined around a `//` comment buries everything after it.
        // The author's own layout is the only safe one there.
        if self.holds_line_comment(rename) {
            self.echo(rename);
            return Ok(());
        }
        self.claim_node(rename);
        let mut star: Option<String> = None;
        let mut pairs: Vec<String> = Vec::new();
        for child in rename.children() {
            match child {
                cst::RenameChild::StarSigil(sigil) => star = Some(self.echoed(sigil)),
                cst::RenameChild::RenamePair(pair) => pairs.push(self.echoed(pair)),
                cst::RenameChild::CommaSigil(_) => {}
            }
        }
        if let Some(sigil) = star {
            self.output.write(&sigil);
        }
        self.aligned_list(&pairs, "(", ")");
        Ok(())
    }

    /// `$(f:())(cols)` — the cover, then its column list. Broken, the list
    /// lands under an indent of its own: the cover is one thing and the
    /// columns it is applied to are another.
    fn map_cover(&mut self, cover: cst::MapCover<'t>) -> Result<()> {
        // A list re-joined around a `//` comment buries everything after it.
        // The author's own layout is the only safe one there.
        if self.holds_line_comment(cover) {
            self.echo(cover);
            return Ok(());
        }
        self.claim_node(cover);
        let whole = self.echo_width(cover);
        if whole <= self.config.projection_length {
            self.echo(cover);
            return Ok(());
        }
        self.output.write("$(");
        if let Some(callable) = cover.cover() {
            self.output.write(&self.echoed(callable));
        }
        self.output.write(")");
        self.output.newline_with_indent(
            self.base_indent + self.config.pipe_indent + self.config.map_cover_extra_indent,
        );
        match cover.selector() {
            Some(selector) => {
                let items: Vec<String> = selector
                    .children()
                    .filter_map(|child| match child {
                        cst::SelectorChild::SelectorItem(item) => Some(self.echoed(item)),
                        cst::SelectorChild::CommaSigil(_) => None,
                    })
                    .collect();
                self.aligned_list(&items, "(", ")");
            }
            None => {}
        }
        // An if-only filter shares the selector's parens; it has no place in
        // the itemized layout, so it rides the end as the author wrote it.
        if let Some(guard) = cover.child() {
            self.output.write(" ");
            self.echo(guard);
        }
        Ok(())
    }

    /// A comma list, joined while it fits and aligned under its opener when
    /// it does not.
    fn aligned_list(&mut self, items: &[String], open: &str, close: &str) {
        self.output.write(open);
        let column = self.output.current_line_length();
        let joined = items.join(", ");
        if joined.len() + column <= self.config.projection_length {
            self.output.write(&joined);
        } else {
            self.write_items(items, column);
        }
        self.output.write(close);
    }

    /// Items one per line at a fixed column, the comma trailing its item.
    fn write_items(&mut self, items: &[String], column: usize) {
        let joined = items.join(", ");
        if joined.len() + column <= self.config.projection_length {
            self.output.write(&joined);
            return;
        }
        for (i, item) in items.iter().enumerate() {
            if i > 0 {
                self.output.write(",");
                self.output.newline_with_indent(column);
            }
            self.output.write(item);
        }
    }

    /// One node's echoed text, without writing it.
    pub(super) fn echoed<T: TypedNode<'t>>(&self, node: T) -> String {
        match self.tree.byte_range(node) {
            Some(range) => self.rendered(range.start, range.end),
            None => String::new(),
        }
    }
}
