// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The let-block preamble: query-scoped bindings, and the four styles that
//! decide where a binding's name stands relative to its body.

use anyhow::Result;
use delightql_cst::cst;

use super::core::Formatter;
use crate::rules::CteStyle;

impl<'t> Formatter<'t> {
    pub(super) fn let_block(&mut self, block: cst::LetBlock<'t>) -> Result<()> {
        for child in block.children() {
            match child {
                cst::LetBlockChild::Cte(cte) => match cte {
                    cst::Cte::StandardCte(standard) => self.standard_cte(standard)?,
                    cst::Cte::LabelCte(label) => self.label_cte(label)?,
                },
                cst::LetBlockChild::EffectCte(effect) => {
                    match effect {
                        cst::EffectCte::EffectStandardCte(n) => self.echo(n),
                        cst::EffectCte::EffectLabelCte(n) => self.echo(n),
                    }
                    self.output.newline();
                }
                cst::LetBlockChild::Cfe(cfe) => self.cfe(cfe)?,
                // A DDL block's body has its own layout, in its own language.
                cst::LetBlockChild::DdlAnnotation(ddl) => {
                    self.echo(ddl);
                    self.output.newline();
                }
            }
        }
        Ok(())
    }

    /// `name(head): body` — the head names the binding's columns, so it comes
    /// first whatever the style does with the body.
    fn standard_cte(&mut self, cte: cst::StandardCte<'t>) -> Result<()> {
        if let Some(name) = cte.name() {
            self.echo(name);
        }
        if let Some(head) = cte.head() {
            self.echo(head);
        }
        self.output.write(": ");
        let outer = self.base_indent;
        if self.config.cte_style == CteStyle::Traditional {
            self.base_indent = self.config.cte_indent;
            self.output.newline_with_indent(self.config.cte_indent);
        }
        if let Some(body) = cte.body() {
            self.let_free_relex(body)?;
        }
        self.base_indent = outer;
        self.output.newline();
        Ok(())
    }

    /// `body: name` — the label shorthand. The four styles differ in where
    /// the name comes to rest.
    fn label_cte(&mut self, cte: cst::LabelCte<'t>) -> Result<()> {
        let name = cte.name().map(|n| self.echoed(n)).unwrap_or_default();
        let badge = cte.child().map(|b| self.echoed(b)).unwrap_or_default();

        match self.config.cte_style {
            CteStyle::Traditional => {
                self.output.write(&name);
                self.output.write(&badge);
                self.output.write("(*): ");
                let outer = self.base_indent;
                self.base_indent = self.config.cte_indent;
                self.output.newline_with_indent(self.config.cte_indent);
                self.label_body(cte)?;
                self.base_indent = outer;
            }
            CteStyle::Centric => {
                let outer = self.base_indent;
                self.base_indent = self.config.cte_indent;
                self.output.newline_with_indent(self.config.cte_indent);
                self.label_body(cte)?;
                self.base_indent = outer;
                self.output.newline();
                self.output.write(": ");
                self.output.write(&name);
                self.output.write(&badge);
            }
            CteStyle::Columnar => {
                self.label_body(cte)?;
                let target = std::cmp::max(
                    self.config.projection_length,
                    self.config.continuation_length,
                ) + self.config.cte_columnar_padding;
                let current = self.output.current_line_length();
                if current + 2 + name.len() > target {
                    self.output.newline();
                }
                let current = self.output.current_line_length();
                if current < target {
                    self.output.write(&" ".repeat(target - current));
                }
                self.output.write(": ");
                self.output.write(&name);
                self.output.write(&badge);
            }
            CteStyle::Subordinate => {
                self.label_body(cte)?;
                self.output.newline_with_indent(self.config.cte_indent);
                self.output.write(": ");
                self.output.write(&name);
                self.output.write(&badge);
            }
        }
        self.output.newline();
        Ok(())
    }

    fn label_body(&mut self, cte: cst::LabelCte<'t>) -> Result<()> {
        match cte.body() {
            Some(cst::LabelCteBody::LetFreeRelex(body)) => self.let_free_relex(body),
            // A mutation source exists to be fed to its terminal; it carries
            // no chain of its own to lay out.
            Some(cst::LabelCteBody::MutationSource(source)) => {
                self.echo(source);
                Ok(())
            }
            None => Ok(()),
        }
    }

    /// `f:(params): body` — a query-scoped function. Two parameter lists make
    /// it higher-order; the first holds the curried params.
    fn cfe(&mut self, cfe: cst::Cfe<'t>) -> Result<()> {
        if let Some(name) = cfe.name() {
            self.echo(name);
        }
        self.output.write(":(");
        if let Some(params) = cfe.first_params() {
            self.output.write(&self.echoed(params));
        }
        self.output.write(")");
        if let Some(params) = cfe.second_params() {
            self.output.write("(");
            self.output.write(&self.echoed(params));
            self.output.write(")");
        }
        self.output.write(":");
        self.output.newline_with_indent(self.config.cte_indent);
        if let Some(body) = cfe.body() {
            self.echo(body);
        }
        self.output.newline();
        Ok(())
    }
}
