// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Structured values as SQL: a made record or tuple as the target's
//! document constructor, a path as the target's document read, and every
//! member a constructor takes admitted by its decided structure. The
//! structure is the core's; this file only spells it, through the canonical
//! functions and intrinsics the shared dialect data respells per target.

use super::value::At;
use super::{Realizer, Result};
use crate::pipeline::middle::core::graph::Arena;
use crate::pipeline::middle::core::heading::{Evidence, Known, Layout, Made};
use crate::pipeline::middle::core::ids::ExprId;
use crate::pipeline::middle::core::node::rel::NodeRoad;
use crate::pipeline::middle::core::node::{ExprKind, Item};
use crate::pipeline::middle::facade::{Intrinsic, LiteralValue, Path, SqlExpr};

impl Realizer<'_, '_> {
    /// A made record (`JSON_OBJECT`, keyed by the decided member heading; a
    /// member no name answers to under a minted key) or tuple
    /// (`JSON_ARRAY`).
    pub(super) fn construct(&mut self, e: ExprId, layout: Layout, members: &[Item], at: At<'_>) -> Result<SqlExpr> {
        let graph = self.graph;
        let heading = match &graph.structure(e)?.known {
            Known::One(heading, _) => heading.clone(),
            _ => return Err(self.contract("a made value with no member heading")),
        };
        let mut args = Vec::with_capacity(members.len() * 2);
        for (member, position) in members.iter().zip(heading.positions()) {
            if layout == Layout::Record {
                args.push(match position.answering_name() {
                    Some(key) => SqlExpr::Literal(LiteralValue::String(key.to_string())),
                    None => {
                        let scope = self.out.scope(None);
                        SqlExpr::PublishedNameLiteral(self.out.column(scope, None))
                    }
                });
            }
            args.push(self.admit(member.expr, at)?);
        }
        Ok(match layout {
            Layout::Record => self.out.function("JSON_OBJECT", args),
            Layout::Tuple => self.out.function("JSON_ARRAY", args),
        })
    }

    /// A value a made structure takes as a member, by its decided
    /// structure: a made document nests, an ordinary value (a bound member
    /// included: both readings of the member-binding question write it alike) is the value it is,
    /// and an extracted value is the document's node with its kind, read in
    /// the same expression as its path or carried beside it.
    pub(super) fn admit(&mut self, member: ExprId, at: At<'_>) -> Result<SqlExpr> {
        let graph = self.graph;
        let structure = graph.structure(member)?;
        match (structure.evidence, structure.made) {
            (Evidence::Severed, _) => Err(self.contract("a member past the core's read-boundary refusal")),
            (Evidence::Extracted, _) => match graph.expr(member).kind() {
                ExprKind::Path { source, path } => {
                    let node = self.node(*source, path, at)?;
                    Ok(self.out.intrinsic(Intrinsic::JsonScalar, vec![node]))
                }
                _ => {
                    let node = self.reached_node(member, at)?;
                    Ok(self.out.intrinsic(Intrinsic::JsonSplice, vec![node]))
                }
            },
            (Evidence::Decided | Evidence::Member | Evidence::Reached, Made::Always) => {
                let value = self.value(member, at)?;
                Ok(self.out.intrinsic(Intrinsic::JsonSplice, vec![value]))
            }
            (Evidence::Decided | Evidence::Member | Evidence::Reached, Made::Sometimes) => {
                Err(self.uncovered("a constructor member whose rows hold made documents beside other values"))
            }
            (Evidence::Decided | Evidence::Member | Evidence::Reached, Made::Never) => {
                let value = self.value(member, at)?;
                Ok(self.out.intrinsic(Intrinsic::JsonScalar, vec![value]))
            }
        }
    }

    /// A path: the target's document read of the source's document. A path
    /// over an extraction in the same expression is the composed path over
    /// the first source, so a string node stays a string node.
    pub(super) fn path(&mut self, source: ExprId, path: &Path, at: At<'_>) -> Result<SqlExpr> {
        let (document, path) = self.composed(source, path, at)?;
        Ok(self.out.function("json_extract", vec![document, SqlExpr::JsonPathLiteral(path)]))
    }

    /// The node a path addresses, as a document value keeping its kind.
    fn node(&mut self, source: ExprId, path: &Path, at: At<'_>) -> Result<SqlExpr> {
        let (document, path) = self.composed(source, path, at)?;
        Ok(self.out.intrinsic(Intrinsic::JsonExtractRaw, vec![document, SqlExpr::JsonPathLiteral(path)]))
    }

    /// An extraction made in this expression, as a document that keeps its
    /// kind across a stage: the node the path addresses, with its kind read
    /// as a value (a container is its document; an atom is quoted into
    /// one).
    pub(super) fn node_document(&mut self, extraction: ExprId, at: At<'_>) -> Result<SqlExpr> {
        let graph = self.graph;
        match graph.expr(extraction).kind() {
            ExprKind::Path { source, path } => {
                let (document, path) = self.composed(*source, path, at)?;
                Ok(self.node_at(document, &path))
            }
            ExprKind::Case { anchor, arms, default } => {
                self.case_of(*anchor, arms, *default, at, |this, result, at| this.node_document(result, at))
            }
            ExprKind::Const(LiteralValue::Null) => Ok(SqlExpr::Literal(LiteralValue::Null)),
            ExprKind::Col(..) if graph.node_road(extraction) == Some(NodeRoad::Bound) => self.reached_node(extraction, at),
            _ => {
                if graph.structure(extraction)?.always_made() {
                    self.value(extraction, at)
                } else {
                    Err(self.contract("a node document of a value the core gave no node"))
                }
            }
        }
    }

    /// The node `path` addresses in `document`, as a document that keeps
    /// its kind across a stage.
    pub(super) fn node_at(&mut self, document: SqlExpr, path: &Path) -> SqlExpr {
        let node = self
            .out
            .intrinsic(Intrinsic::JsonExtractRaw, vec![document.clone(), SqlExpr::JsonPathLiteral(path.clone())]);
        let kind = self.out.function("json_type", vec![document, SqlExpr::JsonPathLiteral(path.clone())]);
        self.out.intrinsic(Intrinsic::JsonEachDocument, vec![node, kind])
    }

    /// The node a published interior key names in `document`, as a
    /// document that keeps its kind.
    pub(super) fn node_at_key(&mut self, document: SqlExpr, key: crate::pipeline::middle::facade::ColId) -> SqlExpr {
        let node = self
            .out
            .intrinsic(Intrinsic::JsonExtractRaw, vec![document.clone(), SqlExpr::PublishedJsonPathLiteral(key)]);
        let kind = self.out.function("json_type", vec![document, SqlExpr::PublishedJsonPathLiteral(key)]);
        self.out.intrinsic(Intrinsic::JsonEachDocument, vec![node, kind])
    }

    /// The document a path reads and the whole path from it.
    fn composed(&mut self, source: ExprId, path: &Path, at: At<'_>) -> Result<(SqlExpr, Path)> {
        let graph = self.graph;
        if let ExprKind::Path { source: inner, path: first } = graph.expr(source).kind() {
            let mut whole = first.clone();
            for step in path.steps() {
                whole = whole.then(step.clone());
            }
            return self.composed(*inner, &whole, at);
        }
        let structure = graph.structure(source)?;
        if structure.evidence != Evidence::Extracted {
            return Ok((self.value(source, at)?, path.clone()));
        }
        Ok((self.reached_node(source, at)?, path.clone()))
    }

    /// The node of an extracted value that is no path in this expression,
    /// by the road the core decided: the passenger its stage carries, or
    /// the node an expansion bound beside it in the stage being written.
    pub(super) fn reached_node(&self, value: ExprId, at: At<'_>) -> Result<SqlExpr> {
        match self.graph.node_road(value) {
            Some(NodeRoad::Carried(carrier)) => self.carried_node(carrier, at),
            Some(NodeRoad::Bound) => {
                let ExprKind::Col(b, i) = self.graph.expr(value).kind() else {
                    return Err(self.contract("a bound node road of a value that reads no position"));
                };
                at.get(super::frame::Key::ColNode(*b, *i))
                    .ok_or_else(|| self.contract("a node the core found bound, absent from the stage"))
            }
            Some(NodeRoad::InPlace) | None => Err(self.contract("an extracted value the core gave no node road")),
        }
    }

    /// The node a stage carries beside an extracted value it published.
    pub(super) fn carried_node(&self, carrier: crate::pipeline::middle::core::ids::PassengerId, at: At<'_>) -> Result<SqlExpr> {
        at.get(super::frame::Key::Passenger(carrier))
            .ok_or_else(|| self.unsupplied("the node a stage carries beside an extracted value"))
    }
}
