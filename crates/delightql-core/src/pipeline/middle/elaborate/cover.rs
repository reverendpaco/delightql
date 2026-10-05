// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The covers: the map cover redefines selected columns in place, the
//! embed-map cover appends one transformed column per selected column, the
//! rename cover republishes selected columns under new names in place. Each
//! addresses the stage input's columns through a selector, decided once
//! against that input's heading.

use super::value::Stage;
use super::Elaborator;
use crate::pipeline::middle::core::decide::grade::CallPosition;
use crate::pipeline::middle::core::heading::{selector, Name};
use crate::pipeline::middle::core::ids::ExprId;
use crate::pipeline::middle::core::node::expr::CaseArm;
use crate::pipeline::middle::core::node::{Cell, Consumer, Item, Naming, PipeOp};
use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::core::heading::Visibility;
use crate::pipeline::middle::facade::{
    Callable, ColumnAlias, DomainExpression, NameTarget, RegexCase, RenameSource, RenameSpec, RepositionSpec,
    SelectorItem, Spread, TruthExpression,
};

/// One column a selector addresses: the stage input's cell, its position in
/// the input's heading, and the name it publishes there.
pub(super) struct Addressed {
    pub(super) cell: Cell,
    pub(super) position: usize,
    pub(super) name: Option<Name>,
    /// The position's 1-based place among the input's displayed positions.
    pub(super) displayed: usize,
}

impl Elaborator<'_, '_> {
    /// The columns a selector addresses, in written order, each a position of
    /// the stage input it reads. A column addressed twice has no reading the
    /// law states.
    pub(super) fn addressed(&mut self, selector: &[SelectorItem]) -> Result<Vec<Addressed>, Refusal> {
        let mut cells: Vec<Cell> = Vec::new();
        for item in selector {
            let before = cells.len();
            let mut lost: Vec<Name> = Vec::new();
            match item {
                SelectorItem::Reference(reference) => {
                    let value = self.reference(reference)?;
                    cells.push(self.cell_of(value)?);
                }
                SelectorItem::Spread(Spread::Glob(glob)) => {
                    if !glob.namespace_path.is_empty() {
                        return Err(refuse::outside("a namespaced glob"));
                    }
                    for value in self.glob(glob.qualifier.as_ref())? {
                        cells.push(self.cell_of(value)?);
                    }
                }
                SelectorItem::Spread(Spread::Regex(regex)) => {
                    let top = self.scopes.last().ok_or_else(|| refuse::column("/regex/", "no scope"))?;
                    let positions =
                        selector::regex(top.run.heading(), &regex.pattern, regex.case == RegexCase::Exact)?;
                    // Matches from more than one member publish in the run's
                    // column order; one member's publish in its own.
                    if top.reversed {
                        let mut members = positions.iter().map(|p| match top.run.outputs()[*p].1 {
                            Cell::Col(b, _) => Some(b),
                            Cell::Merged(_) => None,
                        });
                        let first = members.next();
                        if first.is_some_and(|f| f.is_none() || members.any(|m| m != f)) {
                            return Err(super::edge::order_held());
                        }
                    }
                    lost.extend(selector::regex_lost(top.run.heading(), &regex.pattern, regex.case == RegexCase::Exact)?);
                    cells.extend(positions.into_iter().map(|p| top.run.outputs()[p].1));
                }
                SelectorItem::Spread(Spread::PositionalSpan(span)) => {
                    if !span.namespace_path.is_empty() {
                        return Err(refuse::outside("a namespaced positional span"));
                    }
                    if let Some(q) = &span.qualifier {
                        for value in self.qualified_span(q, span.start, span.end)? {
                            cells.push(self.cell_of(value)?);
                        }
                        continue;
                    }
                    let top = self.scopes.last().ok_or_else(|| refuse::column("|:|", "no scope"))?;
                    // A span counts positions in the run's column order.
                    if top.reversed {
                        return Err(super::edge::order_held());
                    }
                    let positions = selector::span(top.run.heading(), span.start, span.end)?;
                    cells.extend(positions.into_iter().map(|p| top.run.outputs()[p].1));
                }
            }
            // A SELECTOR MUST ADDRESS A COLUMN, each spread of it alike.
            if let SelectorItem::Spread(spread) = item {
                if cells.len() == before {
                    let names: Vec<String> = lost.iter().map(|n| n.to_string()).collect();
                    return Err(refuse::selector_empty(&spelling(spread), &names));
                }
            }
        }
        let top = self.scopes.last().ok_or_else(|| refuse::column("?", "no scope"))?;
        let mut out: Vec<Addressed> = Vec::with_capacity(cells.len());
        for cell in cells {
            let position = top
                .run
                .outputs()
                .iter()
                .position(|(_, c)| *c == cell)
                .ok_or_else(|| refuse::column("?", "the selector names no output of the stage's input"))?;
            if out.iter().any(|a| a.position == position) {
                return Err(refuse::unruled(
                    "whether a selector that addresses one column twice covers it once, twice, or refuses",
                ));
            }
            let displayed = top.run.heading().displayed().position(|(i, _)| i == position).map_or(0, |k| k + 1);
            out.push(Addressed {
                cell,
                position,
                name: top.run.outputs()[position].0.answering_name().cloned(),
                displayed,
            });
        }
        Ok(out)
    }

    /// THE REPOSITION over the stage input: each written move's source (a
    /// reference or an ordinal) is a published position, the final places
    /// decided by `selector::reposition`; a projection of the whole input in
    /// that order, every position carried as the input publishes it.
    pub(super) fn reposition(&mut self, moves: &[RepositionSpec]) -> Result<Vec<Item>, Refusal> {
        let top = self.scopes.last().ok_or_else(|| refuse::column("*[", "no scope"))?;
        if top.run.outputs().iter().any(|(p, _)| p.visibility == Visibility::Latent) {
            return Err(refuse::unruled(
                "whether a latent dimension a reposition does not move stays latent or is activated",
            ));
        }
        let published: Vec<Cell> = top
            .run
            .outputs()
            .iter()
            .filter(|(p, _)| p.visibility == Visibility::Published)
            .map(|(_, c)| *c)
            .collect();
        let mut pairs = Vec::with_capacity(moves.len());
        for spec in moves {
            let value = self.reference(&spec.column)?;
            let cell = self.cell_of(value)?;
            let source = published
                .iter()
                .position(|c| *c == cell)
                .ok_or_else(|| refuse::column("?", "the reposition names no published column of the stage's input"))?;
            pairs.push((source, spec.position));
        }
        let order = selector::reposition(published.len(), &pairs)?;
        Ok(order
            .into_iter()
            .map(|k| Item {
                expr: self.cell_value(published[k]),
                naming: Naming::Glob,
            })
            .collect())
    }

    /// The columns one addressing spread (a regex, a positional span, a
    /// qualified glob) addresses in the stage input, in its order: each
    /// column's value and the name it publishes there (FN.35).
    pub(super) fn spread_values(&mut self, spread: &Spread) -> Result<Vec<(ExprId, Option<Name>)>, Refusal> {
        let addressed = self.addressed(&[SelectorItem::Spread(spread.clone())])?;
        Ok(addressed.into_iter().map(|a| (self.cell_value(a.cell), a.name)).collect())
    }

    /// The run cell a reference reads.
    fn cell_of(&self, value: ExprId) -> Result<Cell, Refusal> {
        crate::pipeline::middle::core::node::rel::referenced_cell(&self.b, value)
            .ok_or_else(|| refuse::column("?", "the selector names no output of the stage's input"))
    }

    /// The callable applied to one cell: its application, with the cell as
    /// what flows in, elaborated in value position.
    fn applied(&mut self, application: &DomainExpression, cell: Cell) -> Result<ExprId, Refusal> {
        let flowing = self.cell_value(cell);
        let outer = self.flowing.replace(flowing);
        let out = self.value(application, CallPosition::Value);
        self.flowing = outer;
        out
    }

    /// THE MAP COVER: each selected cell redefined in place by the callable
    /// applied to it. A guard is conditional application, judged per row on
    /// the cover's input: where it holds the cell is redefined, otherwise
    /// its own value rides through (THE GUARD IS PER-CELL).
    pub(super) fn map_cover(
        &mut self,
        callable: &Callable,
        selector: &[SelectorItem],
        guard: Option<&TruthExpression>,
    ) -> Result<Stage, Refusal> {
        let application = self.input.cover_application(callable)?;
        let mut pairs = Vec::new();
        for addressed in self.addressed(selector)? {
            let target = self.cell_value(addressed.cell);
            let applied = self.applied(&application, addressed.cell)?;
            let value = match guard {
                None => applied,
                Some(guard) => {
                    let holds = self.truth(guard, Consumer::Value)?;
                    let riding = self.cell_value(addressed.cell);
                    self.b.case(None, vec![(CaseArm::Truth(holds), applied)], Some(riding), &self.switches)?
                }
            };
            pairs.push((target, value));
        }
        Ok(Stage::Cover(pairs))
    }

    /// THE EMBED-MAP COVER: one column appended per selected column, the
    /// callable applied to it, named by the name template or, unnamed,
    /// minted. Appended after the input's whole heading, as every embed
    /// appends, in the selector's order.
    pub(super) fn embed_map_cover(
        &mut self,
        callable: &Callable,
        naming: Option<&ColumnAlias>,
        selector: &[SelectorItem],
    ) -> Result<Stage, Refusal> {
        let application = self.input.cover_application(callable)?;
        let addressed = self.addressed(selector)?;
        let mut items = Vec::with_capacity(addressed.len());
        for column in &addressed {
            let naming = match naming {
                None => Naming::Computed,
                Some(alias) => {
                    let template = match alias {
                        ColumnAlias::Template(template) => template.template.as_str(),
                        ColumnAlias::Literal(literal) => literal.as_str(),
                    };
                    let name = selector::template_name(template, column.name.as_ref(), column.displayed)?;
                    self.not_ridden_through(&name, None)?;
                    Naming::As(name)
                }
            };
            items.push(Item {
                expr: self.applied(&application, column.cell)?,
                naming,
            });
        }
        Ok(Stage::Op(PipeOp::Embed(items)))
    }

    /// THE RENAME COVER: every position of the stage input in place, each
    /// renamed one republished under its target (RENAME AND BAPTISM: the
    /// source name is gone), every other riding through under its own name
    /// state; a projection of the whole input.
    pub(super) fn rename_cover<'p>(&mut self, pairs: impl Iterator<Item = &'p RenameSpec>) -> Result<Stage, Refusal> {
        let top = self.scopes.last().ok_or_else(|| refuse::column("*", "no scope"))?;
        if top.run.outputs().iter().any(|(p, _)| p.visibility == Visibility::Latent) {
            return Err(refuse::unruled(
                "whether a latent dimension a rename cover does not rename stays latent or is activated",
            ));
        }
        let mut renamed: Vec<(usize, Name)> = Vec::new();
        for pair in pairs {
            let source = match &pair.from {
                RenameSource::Reference(reference) => SelectorItem::Reference(reference.clone()),
                RenameSource::Regex(regex) => SelectorItem::Spread(Spread::Regex(regex.clone())),
                RenameSource::Glob(glob) => SelectorItem::Spread(Spread::Glob(glob.clone())),
            };
            let addressed = self.addressed(std::slice::from_ref(&source))?;
            for column in &addressed {
                let target = match &pair.to {
                    NameTarget::Identifier(name) if addressed.len() == 1 => Name::new(name.clone()),
                    NameTarget::Identifier(name) => return Err(refuse::duplicate_name(name, false)),
                    NameTarget::Template(ColumnAlias::Template(template)) => {
                        selector::template_name(&template.template, column.name.as_ref(), column.displayed)?
                    }
                    NameTarget::Template(ColumnAlias::Literal(literal)) => {
                        selector::template_name(literal, column.name.as_ref(), column.displayed)?
                    }
                };
                if renamed.iter().any(|(at, _)| *at == column.position) {
                    return Err(refuse::unruled("whether one column renamed twice in a rename cover refuses or publishes once"));
                }
                renamed.push((column.position, target));
            }
        }
        let riding: Vec<usize> = (0..self.scopes.last().map_or(0, |s| s.run.outputs().len()))
            .filter(|i| !renamed.iter().any(|(at, _)| at == i))
            .collect();
        for (_, target) in &renamed {
            self.not_ridden_through(target, Some(&riding))?;
        }
        let top = self.scopes.last().ok_or_else(|| refuse::column("*", "no scope"))?;
        let positions: Vec<(usize, Cell)> = top
            .run
            .outputs()
            .iter()
            .enumerate()
            .filter(|(_, (p, _))| p.visibility == Visibility::Published)
            .map(|(i, (_, c))| (i, *c))
            .collect();
        let mut items = Vec::with_capacity(positions.len());
        for (at, cell) in positions {
            items.push(Item {
                expr: self.cell_value(cell),
                naming: match renamed.iter().find(|(r, _)| *r == at) {
                    Some((_, target)) => Naming::As(target.clone()),
                    None => Naming::Glob,
                },
            });
        }
        Ok(Stage::Op(PipeOp::Project(items)))
    }

    /// A name a new or renamed column publishes, against the stage input's
    /// columns that ride through (`riding`, every one when `None`): one of
    /// them publishing it, or having lost it to a collision, meets two
    /// articles that do not agree (an alias pre-empts a mint; colliding
    /// outputs poison both).
    pub(super) fn not_ridden_through(&self, name: &Name, riding: Option<&[usize]>) -> Result<(), Refusal> {
        use crate::pipeline::middle::core::heading::NameState;
        let top = self.scopes.last().ok_or_else(|| refuse::column("?", "no scope"))?;
        let clashes = top.run.outputs().iter().enumerate().any(|(i, (p, _))| {
            riding.is_none_or(|r| r.contains(&i))
                && (p.answering_name() == Some(name) || matches!(&p.name, NameState::Lost(lost) if lost == name))
        });
        if clashes {
            return Err(refuse::riding_name(name));
        }
        Ok(())
    }
}

/// A spread as written, for a refusal that names it.
pub(super) fn spelling(spread: &Spread) -> String {
    match spread {
        Spread::Glob(glob) => match &glob.qualifier {
            Some(q) => format!("the glob {q}.*"),
            None => "the glob *".to_string(),
        },
        Spread::Regex(regex) => {
            format!("the pattern /{}/{}", regex.pattern, if regex.case == RegexCase::Exact { "c" } else { "" })
        }
        Spread::PositionalSpan(span) => {
            let side = |b: Option<(u16, bool)>| match b {
                Some((p, true)) => format!("-{p}"),
                Some((p, false)) => p.to_string(),
                None => String::new(),
            };
            format!("the range |{}:{}|", side(span.start), side(span.end))
        }
    }
}
