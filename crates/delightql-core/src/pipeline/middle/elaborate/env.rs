// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Reference binding (W5 #2): what a name, qualified name or ordinal
//! denotes, decided against the scopes open at its position, innermost
//! first. The answer is the reference's own value node; nothing records it
//! beside the graph.

use super::Elaborator;
use crate::pipeline::middle::core::graph::Arena;
use crate::pipeline::middle::core::heading::correspondence::{answers_to, lost};
use crate::pipeline::middle::core::heading::{Name, NameState, Visibility};
use crate::pipeline::middle::core::heading::selector;
use crate::pipeline::middle::core::ids::{BinderId, ExprId};
use crate::pipeline::middle::core::node::Cell;
use crate::pipeline::middle::core::refuse::{self, Refusal};

/// A column reference as written: a bare name, a qualified name, or a
/// position of the displayed heading (of the innermost scope, or of a
/// qualified member).
pub(super) enum Address<'a> {
    Bare(&'a Name),
    Qualified(&'a Name, &'a Name),
    Ordinal {
        position: u16,
        reverse: bool,
        qualifier: Option<&'a Name>,
    },
}

impl Elaborator<'_, '_> {
    /// What a column reference denotes at its position (W5 #2): the one
    /// entry every reference position resolves through — projection and
    /// stage items, slots, conditions, keys, cover targets, arguments,
    /// actuals and configured values.
    pub(super) fn resolve_ref(&mut self, address: Address<'_>) -> Result<ExprId, Refusal> {
        if let Some(ends) = self.edge_scope.clone() {
            return self.resolve_in_edge(&ends, address);
        }
        match address {
            Address::Bare(name) => self.resolve_bare(name),
            Address::Qualified(qualifier, name) => self.resolve_qualified(qualifier, name),
            Address::Ordinal {
                position,
                reverse,
                qualifier,
            } => self.resolve_ordinal(position, reverse, qualifier),
        }
    }

    /// A reference in an edge's condition: a column of one of the edge's
    /// two endpoints, qualified by the name the body reads it by or bare
    /// and answered by exactly one of them (EACH EDGE KEEPS ITS OWN SCOPE).
    fn resolve_in_edge(&mut self, ends: &[(Name, crate::pipeline::middle::core::ids::BinderId); 2], address: Address<'_>) -> Result<ExprId, Refusal> {
        let (candidates, spelled): (Vec<_>, String) = match address {
            Address::Bare(name) => (ends.iter().map(|(_, b)| (*b, name)).collect(), name.to_string()),
            Address::Qualified(q, name) => (
                ends.iter().filter(|(n, _)| n == q).map(|(_, b)| (*b, name)).collect(),
                format!("{q}.{name}"),
            ),
            Address::Ordinal { .. } => return Err(refuse::outside("an ordinal in an edge's condition")),
        };
        let found: Vec<(crate::pipeline::middle::core::ids::BinderId, usize)> = candidates
            .into_iter()
            .flat_map(|(b, name)| answers_to(self.b.binder(b).heading(), name).into_iter().map(move |k| (b, k)))
            .collect();
        match found.as_slice() {
            [(b, k)] => Ok(self.b.col(*b, *k as u16)),
            [] => Err(refuse::column(&spelled, "an edge's condition reads a column of its two endpoints only")),
            [_, _, ..] => Err(refuse::ambiguous_column(&spelled)),
        }
    }

    pub(super) fn cell_value(&mut self, cell: Cell) -> ExprId {
        match cell {
            Cell::Col(binder, position) => self.b.col(binder, position),
            Cell::Merged(merge) => self.b.merged(merge),
        }
    }

    /// A bare name: the published heading of the innermost scope that
    /// answers to it, outward. A name lost by collision in a scope is
    /// ambiguous there; a latent dimension answers to no name.
    fn resolve_bare(&mut self, name: &Name) -> Result<ExprId, Refusal> {
        self.resolve_bare_as(name, |n| refuse::ambiguous_column(n))
    }

    /// A transform's bare target, resolved as any bare name; an ambiguous
    /// one refuses as a target.
    pub(super) fn transform_target(&mut self, name: &Name) -> Result<ExprId, Refusal> {
        self.resolve_bare_as(name, refuse::ambiguous_transform_target)
    }

    fn resolve_bare_as(&mut self, name: &Name, ambiguous: fn(&Name) -> Refusal) -> Result<ExprId, Refusal> {
        if let Some(formal) = self.named_formal(name) {
            return Ok(formal);
        }
        if let Some(found) = self.resolve_in_scopes(name, ambiguous)? {
            return Ok(found);
        }
        if let Some(found) = self.captured_name(name, ambiguous)? {
            return Ok(found);
        }
        Err(refuse::column(name.as_str(), "no live scope publishes this name"))
    }

    /// A bare name in the open scopes, innermost first: `None` when no
    /// scope answers to it.
    pub(super) fn resolve_in_scopes(&mut self, name: &Name, ambiguous: fn(&Name) -> Refusal) -> Result<Option<ExprId>, Refusal> {
        for i in (0..self.scopes.len()).rev() {
            if self.scopes[i].qualified_only {
                continue;
            }
            let heading = self.scopes[i].run.heading();
            let answering = answers_to(heading, name);
            match answering.as_slice() {
                [one] => {
                    let cell = self.scopes[i].run.outputs()[*one].1;
                    return Ok(Some(self.cell_value(cell)));
                }
                [_, _, ..] => return Err(ambiguous(name)),
                [] => {}
            }
            if !lost(heading, name).is_empty() {
                return Err(ambiguous(name));
            }
            let latent = heading.positions().iter().any(|p| {
                p.visibility == Visibility::Latent
                    && matches!(&p.name, NameState::Authored(n) | NameState::Catalog(n) if n == name)
            });
            if latent {
                return Err(refuse::latent_name(name.as_str()));
            }
        }
        Ok(None)
    }

    /// Whether a bare name answers to something where it stands: a formal,
    /// or a position (published, lost to a collision, or latent) of a live
    /// scope.
    pub(super) fn publishes(&self, name: &Name) -> bool {
        self.named_formal(name).is_some()
            || self.scopes.iter().filter(|scope| !scope.qualified_only).any(|scope| {
                let heading = scope.run.heading();
                !answers_to(heading, name).is_empty()
                    || !lost(heading, name).is_empty()
                    || heading.positions().iter().any(|p| {
                        p.visibility == Visibility::Latent
                            && matches!(&p.name, NameState::Authored(n) | NameState::Catalog(n) if n == name)
                    })
            })
    }

    /// A qualified name: the named member of the innermost scope that has
    /// one, and the position of its own heading carrying the name. The
    /// qualifier reaches a name its member lost by collision.
    fn resolve_qualified(&mut self, qualifier: &Name, name: &Name) -> Result<ExprId, Refusal> {
        if qualifier.as_str() == "_" && !qualifier.is_stropped() {
            return self.resolve_deictic(name);
        }
        for i in (0..self.scopes.len()).rev() {
            let member = self.scopes[i].run.answering(qualifier)?.map(|m| m.binder());
            let Some(binder) = member else {
                continue;
            };
            let heading = self.b.binder(binder).heading();
            let found: Vec<(usize, bool)> = heading
                .positions()
                .iter()
                .enumerate()
                .filter_map(|(k, p)| match &p.name {
                    NameState::Authored(n) | NameState::Catalog(n) | NameState::Lost(n)
                        if n == name && !matches!(p.visibility, Visibility::Hidden(_)) =>
                    {
                        Some((k, p.visibility == Visibility::Latent))
                    }
                    NameState::Authored(_)
                    | NameState::Catalog(_)
                    | NameState::Lost(_)
                    | NameState::Minted(_) => None,
                })
                .collect();
            return match found.as_slice() {
                [(_, true)] => Err(refuse::latent_name(&format!("{qualifier}.{name}"))),
                [(k, false)] => Ok(self.b.col(binder, *k as u16)),
                [] => Err(refuse::column(
                    &format!("{qualifier}.{name}"),
                    "the qualified scope does not publish this name",
                )),
                [_, _, ..] => Err(refuse::ambiguous_column(&format!("{qualifier}.{name}"))),
            };
        }
        if self.scopes.iter().any(|s| s.arms.contains(qualifier)) {
            return Err(refuse::arm_name_after(&format!("{qualifier}.{name}")));
        }
        Err(refuse::not_a_scope(&qualifier.to_string(), &format!("{qualifier}.{name}"), &self.live_scopes()))
    }

    /// THE DEICTIC `_` (lvars-law): the one visible pipe output no `as`
    /// named, found by enumerating every open scope, never the nearest;
    /// `_.x` is that member's position published as `x`.
    fn resolve_deictic(&mut self, name: &Name) -> Result<ExprId, Refusal> {
        let spelled = format!("_.{name}");
        let candidates: Vec<crate::pipeline::middle::core::ids::BinderId> = self
            .scopes
            .iter()
            .flat_map(|s| s.run.members().filter(|m| m.deictic()).map(|m| m.binder()))
            .collect();
        let binder = match candidates.as_slice() {
            [] => return Err(refuse::no_unnamed_pipe(&spelled)),
            [one] => *one,
            [_, _, ..] => return Err(refuse::two_unnamed_pipes(&spelled)),
        };
        let heading = self.b.binder(binder).heading();
        let found: Vec<usize> = heading
            .positions()
            .iter()
            .enumerate()
            .filter(|(_, p)| match &p.name {
                NameState::Authored(n) | NameState::Catalog(n) | NameState::Lost(n) => {
                    n == name && p.visibility == Visibility::Published
                }
                NameState::Minted(_) => false,
            })
            .map(|(k, _)| k)
            .collect();
        match found.as_slice() {
            [k] => Ok(self.b.col(binder, *k as u16)),
            [] => Err(refuse::column(&spelled, "the unnamed pipe output does not publish this name")),
            [_, _, ..] => Err(refuse::ambiguous_column(&spelled)),
        }
    }

    /// An ordinal: a position of the displayed heading (published and
    /// latent) of the innermost scope, or of a qualified member's own.
    fn resolve_ordinal(
        &mut self,
        position: u16,
        reverse: bool,
        qualifier: Option<&Name>,
    ) -> Result<ExprId, Refusal> {
        let pick = |len: usize| -> Option<usize> {
            let p = position as usize;
            if p == 0 || p > len {
                return None;
            }
            Some(if reverse { len - p } else { p - 1 })
        };
        match qualifier {
            None => {
                let top = self
                    .scopes
                    .last()
                    .ok_or_else(|| refuse::column(&format!("|{position}|"), "no scope"))?;
                if top.reversed {
                    return Err(super::edge::order_held());
                }
                let displayed: Vec<usize> = top.run.heading().displayed().map(|(i, _)| i).collect();
                let index = pick(displayed.len())
                    .map(|k| displayed[k])
                    .ok_or_else(|| refuse::column(&format!("|{position}|"), "past the displayed heading"))?;
                let cell = top.run.outputs()[index].1;
                Ok(self.cell_value(cell))
            }
            Some(q) => {
                let Some(binder) = self.answering_member(q)? else {
                    return Err(refuse::not_a_scope(&q.to_string(), &format!("{q}|{position}|"), &self.live_scopes()));
                };
                let displayed: Vec<usize> = self.b.binder(binder).heading().displayed().map(|(i, _)| i).collect();
                let index = pick(displayed.len())
                    .map(|k| displayed[k])
                    .ok_or_else(|| refuse::column(&format!("{q}|{position}|"), "past the member's heading"))?;
                Ok(self.b.col(binder, index as u16))
            }
        }
    }

    /// A qualified span `q|a:b|`: the member's own positions the span
    /// counts over its displayed heading, as values.
    pub(super) fn qualified_span(
        &mut self,
        q: &Name,
        start: Option<(u16, bool)>,
        end: Option<(u16, bool)>,
    ) -> Result<Vec<ExprId>, Refusal> {
        let Some(binder) = self.answering_member(q)? else {
            return Err(refuse::not_a_scope(&q.to_string(), &format!("{q}|:|"), &self.live_scopes()));
        };
        let positions = selector::span(self.b.binder(binder).heading(), start, end)?;
        Ok(positions.into_iter().map(|k| self.b.col(binder, k as u16)).collect())
    }

    /// The member a qualifier answers, in the innermost scope that has one.
    fn answering_member(&self, q: &Name) -> Result<Option<BinderId>, Refusal> {
        for scope in self.scopes.iter().rev() {
            if let Some(member) = scope.run.answering(q)? {
                return Ok(Some(member.binder()));
            }
        }
        Ok(None)
    }

    /// Every displayed position of a qualified member, or of the innermost
    /// scope, as values: what a glob covers. A glob does not skip a cell it
    /// covers, and no name reaches a latent dimension: a glob over one
    /// takes the latent-under-glob switch.
    pub(super) fn glob(&mut self, qualifier: Option<&Name>) -> Result<Vec<ExprId>, Refusal> {
        match qualifier {
            None => {
                let top = self
                    .scopes
                    .last()
                    .ok_or_else(|| refuse::column("*", "no scope"))?;
                if top.reversed {
                    return Err(super::edge::order_held());
                }
                if top.run.outputs().iter().any(|(p, _)| p.visibility == Visibility::Latent) {
                    return match self.switches.latent_under_glob {
                        crate::pipeline::middle::core::switches::LatentUnderGlob::Refuse => {
                            Err(refuse::latent_under_glob())
                        }
                    };
                }
                let cells: Vec<Cell> = top
                    .run
                    .outputs()
                    .iter()
                    .filter(|(p, _)| p.visibility == Visibility::Published)
                    .map(|(_, c)| *c)
                    .collect();
                Ok(cells.into_iter().map(|c| self.cell_value(c)).collect())
            }
            Some(q) => {
                let Some(binder) = self.answering_member(q)? else {
                    return Err(refuse::not_a_scope(&q.to_string(), &format!("{q}.*"), &self.live_scopes()));
                };
                if self.b.binder(binder).heading().positions().iter().any(|p| p.visibility == Visibility::Latent) {
                    return match self.switches.latent_under_glob {
                        crate::pipeline::middle::core::switches::LatentUnderGlob::Refuse => Err(refuse::latent_under_glob()),
                    };
                }
                let positions: Vec<usize> = self
                    .b
                    .binder(binder)
                    .heading()
                    .positions()
                    .iter()
                    .enumerate()
                    .filter(|(_, p)| p.visibility == Visibility::Published)
                    .map(|(k, _)| k)
                    .collect();
                Ok(positions.into_iter().map(|k| self.b.col(binder, k as u16)).collect())
            }
        }
    }

    /// The qualifiers the open scopes answer to, innermost first, each once.
    fn live_scopes(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for scope in self.scopes.iter().rev() {
            for name in scope.run.members().filter_map(|m| m.scope()) {
                let spelled = name.to_string();
                if !out.contains(&spelled) {
                    out.push(spelled);
                }
            }
        }
        out
    }

    /// A configured value's reference: the one position of the innermost
    /// scope that carries its passenger. None carrying it means a reduction
    /// dropped its construction rows (O-OCC-8, today's refusal).
    pub(super) fn resolve_passenger(
        &mut self,
        passenger: crate::pipeline::middle::core::ids::PassengerId,
    ) -> Result<ExprId, Refusal> {
        for i in (0..self.scopes.len()).rev() {
            let found: Vec<Cell> = self.scopes[i]
                .run
                .outputs()
                .iter()
                .filter(|(p, _)| p.visibility == Visibility::Hidden(passenger))
                .map(|(_, c)| *c)
                .collect();
            match found.as_slice() {
                [one] => return Ok(self.cell_value(*one)),
                [_, _, ..] => return Err(refuse::completion_pairing()),
                [] => {}
            }
        }
        if self.column_bound.contains(&passenger) {
            return Err(refuse::outside(
                "a column-bound scalar actual read where the relation carrying it does not stand (passed on to \
                 another application)",
            ));
        }
        match self.switches.lineage_drop {
            crate::pipeline::middle::core::switches::LineageDrop::Refuse => {
                Err(refuse::residual_completion())
            }
        }
    }
}
