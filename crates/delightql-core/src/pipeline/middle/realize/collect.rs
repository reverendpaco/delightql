// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! G3a: a collection is its aggregate: one document per group, holding one
//! object per contributing row, a row contributing when one of its members
//! is present; over no row, the empty collection. A document's keys are the
//! names of the collection's interior heading, one column per position of
//! that heading, so a drill reading the document spells the same keys.

use super::frame::Key;
use super::value::{integer, At};
use super::{Realizer, Result};
use crate::pipeline::middle::core::graph::{Arena, Graph};
use crate::pipeline::middle::core::heading::{Heading, Known, Origin};
use crate::pipeline::middle::core::ids::ExprId;
use crate::pipeline::middle::core::node::{CollectMember, ExprKind};
use crate::pipeline::middle::facade::{
    BinaryOperator, ColId, Intrinsic, LiteralValue, SqlExpr, WhenClause,
};
use std::collections::BTreeMap;

/// Every interior heading a statement's relations hold, by the collection
/// that formed it.
pub(super) fn interior_headings(
    graph: &Graph,
    rels: impl Iterator<Item = crate::pipeline::middle::core::ids::RelId>,
    values: impl Iterator<Item = ExprId>,
) -> BTreeMap<ExprId, Heading> {
    fn walk(h: &Heading, out: &mut BTreeMap<ExprId, Heading>) {
        for p in h.positions() {
            match &p.interior.known {
                Known::None => {}
                Known::One(inner, _) => walk(inner, out),
                Known::Keyed(inner) => {
                    // A chain of keyed levels holds its collection at the
                    // bottom.
                    let mut held = &inner.known;
                    while let Known::Keyed(next) = held {
                        held = &next.known;
                    }
                    if let Known::Shape(h, Origin::Collection(e) | Origin::Tuples(e)) = held {
                        out.entry(*e).or_insert_with(|| (**h).clone());
                        walk(h, out);
                    }
                }
                Known::Shape(inner, origin) => {
                    if let Origin::Collection(e) | Origin::Tuples(e) = origin {
                        out.entry(*e).or_insert_with(|| (**inner).clone());
                    }
                    walk(inner, out);
                }
                Known::PerArm(arms) => {
                    for (inner, origin) in arms {
                        if let Some(Origin::Collection(e) | Origin::Tuples(e)) = origin {
                            out.entry(*e).or_insert_with(|| inner.clone());
                        }
                        walk(inner, out);
                    }
                }
            }
        }
    }
    let mut out = BTreeMap::new();
    for r in rels {
        walk(graph.rel(r).heading(), &mut out);
    }
    // A collection no heading publishes (one a call receives and reads)
    // has the interior its argument's structure states.
    for e in values {
        if let crate::pipeline::middle::core::node::ExprKind::Argument { structure, .. } = graph.expr(e).kind() {
            if let Known::Shape(h, Origin::Collection(origin) | Origin::Tuples(origin)) = &structure.known {
                out.entry(*origin).or_insert_with(|| (**h).clone());
                walk(h, &mut out);
            }
        }
    }
    out
}

impl Realizer<'_, '_> {
    /// The column naming position `k` of an interior heading, by what
    /// formed it: a collection (its own, or one of its nested levels'), or
    /// the relation a receipt carries. A document and a drill of it spell
    /// the same keys.
    pub(super) fn interior_col(&mut self, origin: Origin, heading: &Heading, k: usize) -> Result<ColId> {
        let at = match self.interior_scopes.iter().position(|(o, h, _)| *o == origin && h == heading) {
            Some(at) => at,
            None => {
                let scope = self.out.scope(None);
                let cols = heading
                    .positions()
                    .iter()
                    .map(|p| self.out.column(scope, p.answering_name()))
                    .collect();
                self.interior_scopes.push((origin, heading.clone(), cols));
                self.interior_scopes.len() - 1
            }
        };
        self.interior_scopes[at]
            .2
            .get(k)
            .copied()
            .ok_or_else(|| self.uncovered("a position past a collection's interior heading"))
    }

    /// The top interior heading of a collection.
    fn top_heading(&self, origin: ExprId) -> Result<Heading> {
        self.interior_headings
            .get(&origin)
            .cloned()
            .ok_or_else(|| self.uncovered("a collection with no interior heading"))
    }

    /// G3a: a collection as the aggregate of its group. A nested level
    /// reads the document a stage below computed for it (G3b).
    pub(super) fn collect(&mut self, e: ExprId, at: At<'_>) -> Result<SqlExpr> {
        let tree = self.level_tree(e)?;
        let pick = at.get(Key::Pick(e, 0));
        let (object, present) = self.level_object(e, &tree[0], at)?;
        let mut presence = vec![any_present(present)];
        if let Some(pick) = pick {
            presence.push(is_one(pick));
        }
        if let Some(flag) = super::value::keep_flag(at.local) {
            presence.push(is_one(flag));
        }
        let row = SqlExpr::Case {
            expr: None,
            when_clauses: vec![WhenClause::new(SqlExpr::and(presence), object)],
            else_clause: None,
        };
        let concat = self.out.function("GROUP_CONCAT", vec![row, text(",")]);
        let array = self.document(concat);
        let empty = self.out.function("JSON", vec![text("[]")]);
        Ok(self.out.function("COALESCE", vec![array, empty]))
    }

    /// A collection's levels: the top level first, then every nested level
    /// breadth first (a level's nested levels after it), each holding its
    /// members, its interior heading, its enclosing level and the levels
    /// nested in it by member position. A metadata member's levels are keyed:
    /// one per level of its chain, the last holding the collected target.
    fn level_tree(&self, e: ExprId) -> Result<Vec<Level>> {
        let mut tree = match self.graph.expr(e).kind() {
            ExprKind::Collect { layout, members } => vec![Level {
                layout: *layout,
                members: members.clone(),
                heading: self.top_heading(e)?,
                parent: None,
                children: Vec::new(),
                key: None,
                chain: false,
            }],
            // A metadata collector's top level is keyed.
            ExprKind::Metadata { level } => {
                let interior = self.graph.structure(e)?;
                let Known::Keyed(held) = &interior.known else {
                    return Err(self.contract("a metadata collector whose structure is no keyed object"));
                };
                match &level.target {
                    crate::pipeline::middle::core::node::MetaTarget::Collect(layout, members) => vec![Level {
                        layout: *layout,
                        members: members.clone(),
                        heading: self.top_heading(e)?,
                        parent: None,
                        children: Vec::new(),
                        key: Some(level.key),
                        chain: false,
                    }],
                    crate::pipeline::middle::core::node::MetaTarget::Group(inner) => {
                        let mut tree = vec![Level {
                            layout: crate::pipeline::middle::core::heading::Layout::Record,
                            members: Vec::new(),
                            heading: Heading::default(),
                            parent: None,
                            children: Vec::new(),
                            key: Some(level.key),
                            chain: true,
                        }];
                        self.keyed_levels(&mut tree, 0, 0, inner, held)?;
                        tree
                    }
                }
            }
            _ => return Err(self.contract("a collection level of a value that is no collection")),
        };
        let mut at = 0;
        while at < tree.len() {
            let members = tree[at].members.clone();
            for (k, member) in members.iter().enumerate() {
                let interior = match tree[at].heading.positions().get(k) {
                    Some(p) => p.interior.clone(),
                    None if tree[at].chain => continue,
                    None => return Err(self.uncovered("a nested level with no interior heading")),
                };
                match member {
                    CollectMember::Item(_) => {}
                    CollectMember::Nested(_, layout, inner) => {
                        let Known::Shape(h, _) = &interior.known else {
                            return Err(self.uncovered("a nested level with no interior heading"));
                        };
                        let id = self.level_id(&tree)?;
                        tree[at].children.push((k, id));
                        tree.push(Level {
                            layout: *layout,
                            members: inner.clone(),
                            heading: (**h).clone(),
                            parent: Some(at),
                            children: Vec::new(),
                            key: None,
                            chain: false,
                        });
                    }
                    CollectMember::Metadata(_, level) => self.keyed_levels(&mut tree, at, k, level, &interior)?,
                }
            }
            at += 1;
        }
        Ok(tree)
    }

    /// A metadata level under member `k` of level `parent`, and its chain.
    fn keyed_levels(
        &self,
        tree: &mut Vec<Level>,
        parent: usize,
        k: usize,
        level: &crate::pipeline::middle::core::node::MetaLevel,
        interior: &crate::pipeline::middle::core::heading::Interior,
    ) -> Result<()> {
        use crate::pipeline::middle::core::node::MetaTarget;
        let Known::Keyed(held) = &interior.known else {
            return Err(self.contract("a metadata level whose interior is no keyed object"));
        };
        let id = self.level_id(tree)?;
        tree[parent].children.push((k, id));
        match &level.target {
            MetaTarget::Collect(layout, members) => {
                let Known::Shape(h, _) = &held.known else {
                    return Err(self.uncovered("a metadata level with no interior heading"));
                };
                tree.push(Level {
                    layout: *layout,
                    members: members.clone(),
                    heading: (**h).clone(),
                    parent: Some(parent),
                    children: Vec::new(),
                    key: Some(level.key),
                    chain: false,
                });
            }
            MetaTarget::Group(inner) => {
                tree.push(Level {
                    layout: crate::pipeline::middle::core::heading::Layout::Record,
                    members: Vec::new(),
                    heading: Heading::default(),
                    parent: Some(parent),
                    children: Vec::new(),
                    key: Some(level.key),
                    chain: true,
                });
                self.keyed_levels(tree, id as usize, 0, inner, held)?;
            }
        }
        Ok(())
    }

    /// The id the next level of a collection takes.
    fn level_id(&self, tree: &[Level]) -> Result<u16> {
        u16::try_from(tree.len()).map_err(|_| self.uncovered("a collection of that many levels"))
    }

    /// One level's object for a row, and the presence tests of its members.
    fn level_object(&mut self, e: ExprId, level: &Level, at: At<'_>) -> Result<(SqlExpr, Vec<SqlExpr>)> {
        let layout = level.layout;
        let mut args = Vec::new();
        let mut present = Vec::new();
        for (k, m) in level.members.iter().enumerate() {
            let (value, admitted) = match m {
                CollectMember::Item(item) => {
                    let value = self.value(item.expr, at)?;
                    let admitted = self.admit(item.expr, at)?;
                    (value, admitted)
                }
                CollectMember::Nested(..) | CollectMember::Metadata(..) => {
                    let child = level
                        .children
                        .iter()
                        .find(|(member, _)| *member == k)
                        .map(|(_, id)| *id)
                        .ok_or_else(|| self.contract("a nested member with no level"))?;
                    let value = at
                        .get(Key::Level(e, child))
                        .ok_or_else(|| self.unreached("a nested level's document"))?;
                    let spliced = self.out.intrinsic(Intrinsic::JsonSplice, vec![value.clone()]);
                    let empty = match m {
                        CollectMember::Metadata(..) => self.out.function("JSON", vec![text("{}")]),
                        _ => self.out.function("JSON", vec![text("[]")]),
                    };
                    (value, self.out.function("COALESCE", vec![spliced, empty]))
                }
            };
            present.push(SqlExpr::Binary {
                left: Box::new(value),
                op: BinaryOperator::IsNot,
                right: Box::new(SqlExpr::Literal(LiteralValue::Null)),
            });
            if layout == crate::pipeline::middle::core::heading::Layout::Record {
                let key = self.interior_col(Origin::Collection(e), &level.heading, k)?;
                args.push(SqlExpr::PublishedNameLiteral(key));
            }
            args.push(admitted);
        }
        // The object constructor is the function the dialect pack spells
        // per target (PostgreSQL's `json_build_object`); the intrinsic form
        // has no per-target spelling and reaches PostgreSQL as `json_object`.
        Ok(match layout {
            crate::pipeline::middle::core::heading::Layout::Record => (self.out.function("JSON_OBJECT", args), present),
            crate::pipeline::middle::core::heading::Layout::Tuple => (self.out.function("JSON_ARRAY", args), present),
        })
    }

    /// The partition the levels nested in `parent` collect over: the
    /// population, the group's keys, and every enclosing level's members, a
    /// keyed level's key among them.
    fn level_partition(&mut self, tree: &[Level], parent: usize, keys: &[ExprId], at: At<'_>) -> Result<Vec<SqlExpr>> {
        let mut partition = self.population(at.local)?;
        for k in keys {
            partition.push(self.value(*k, at)?);
        }
        let mut enclosing: Vec<usize> = Vec::new();
        let mut ancestor = Some(parent);
        while let Some(a) = ancestor {
            enclosing.push(a);
            ancestor = tree[a].parent;
        }
        for a in enclosing.into_iter().rev() {
            for m in &tree[a].members {
                if let CollectMember::Item(item) = m {
                    partition.push(self.value(item.expr, at)?);
                }
            }
            if let Some(key) = tree[a].key {
                partition.push(self.value(key, at)?);
            }
        }
        Ok(partition.into_iter().map(super::rel::set_key).collect())
    }

    /// A metadata level's object over its enclosing partition: one entry
    /// per key, its label the key's value as an object key, holding what the
    /// level holds for that key. A row with no key takes no entry.
    fn keyed_object(&mut self, e: ExprId, j: u16, key: ExprId, partition: Option<&[SqlExpr]>, at: At<'_>) -> Result<SqlExpr> {
        // A key whose rows contribute no object holds the empty collection.
        let held = at
            .get(Key::KeyedRows(e, j))
            .ok_or_else(|| self.unreached("what a metadata level holds for a key"))?;
        let held = self.out.function("COALESCE", vec![held, text("[]")]);
        let first = at.get(Key::KeyPick(e, j)).ok_or_else(|| self.unreached("a metadata level's key pick"))?;
        let value = self.value(key, at)?;
        // An object key is text, as `JSON_GROUP_OBJECT` takes its label.
        let label = self.out.intrinsic(Intrinsic::JsonLabel, vec![value.clone()]);
        let text_label = SqlExpr::Binary {
            left: Box::new(label),
            op: BinaryOperator::Concatenate,
            right: Box::new(text("")),
        };
        let quoted = self.out.function("json_quote", vec![text_label]);
        let entry = SqlExpr::Binary {
            left: Box::new(SqlExpr::Binary {
                left: Box::new(quoted),
                op: BinaryOperator::Concatenate,
                right: Box::new(text(":")),
            }),
            op: BinaryOperator::Concatenate,
            right: Box::new(held),
        };
        let row = SqlExpr::Case {
            expr: None,
            when_clauses: vec![WhenClause::new(
                SqlExpr::and(vec![
                    is_one(first),
                    SqlExpr::Binary {
                        left: Box::new(value),
                        op: BinaryOperator::IsNot,
                        right: Box::new(SqlExpr::Literal(LiteralValue::Null)),
                    },
                ]),
                entry,
            )],
            else_clause: None,
        };
        // Over the enclosing level's partition, or, for a metadata
        // collector's own value, as the group's aggregate.
        let concat = match partition {
            Some(partition) => SqlExpr::WindowFunction {
                name: "GROUP_CONCAT".to_string(),
                args: vec![row, text(",")],
                distinct: false,
                partition_by: partition.to_vec(),
                order_by: Vec::new(),
                frame: None,
            },
            None => self.out.function("GROUP_CONCAT", vec![row, text(",")]),
        };
        let parts = self.out.function("COALESCE", vec![concat, text("")]);
        Ok(self.out.function(
            "JSON",
            vec![SqlExpr::Binary {
                left: Box::new(SqlExpr::Binary {
                    left: Box::new(text("{")),
                    op: BinaryOperator::Concatenate,
                    right: Box::new(parts),
                }),
                op: BinaryOperator::Concatenate,
                right: Box::new(text("}")),
            }],
        ))
    }

    /// `JSON('[' || parts || ']')`.
    fn document(&mut self, parts: SqlExpr) -> SqlExpr {
        self.out.function(
            "JSON",
            vec![SqlExpr::Binary {
                left: Box::new(SqlExpr::Binary {
                    left: Box::new(text("[")),
                    op: BinaryOperator::Concatenate,
                    right: Box::new(parts),
                }),
                op: BinaryOperator::Concatenate,
                right: Box::new(text("]")),
            }],
        )
    }

    /// G3b: the nested levels of a group's collections, each one more stage
    /// below the group: a level's document is the aggregate of its rows over
    /// the partition its enclosing levels' members make (the window form of
    /// a keyed reduction), with one row picked per such partition for the
    /// enclosing level to collect. Sibling levels share their enclosing
    /// level's partition and its one pick. The group's input is read once.
    pub(super) fn nested_levels(
        &mut self,
        mut rows: super::run::Rows,
        keys: &[ExprId],
        reductions: &[ExprId],
        outer: &super::frame::Enclosing,
    ) -> Result<super::run::Rows> {
        let graph = self.graph;
        for red in reductions {
            let keyed_root = match graph.expr(*red).kind() {
                ExprKind::Collect { .. } => false,
                ExprKind::Metadata { .. } => true,
                _ => continue,
            };
            let e = *red;
            let tree = self.level_tree(e)?;
            if tree.len() < 2 && !keyed_root {
                continue;
            }
            // DECISION(grouping): a nested level is a window aggregate over
            // its enclosing levels' partition, with one picked row per
            // partition.
            // Every level after the levels nested in it: breadth first,
            // reversed.
            for j in (1..tree.len()).rev() {
                let parent = tree[j].parent.ok_or_else(|| self.contract("a nested level with no enclosing level"))?;
                let env = rows.frame.env.clone();
                let at = At { local: &env, outer };
                let partition = self.level_partition(&tree, parent, keys, at)?;
                // A keyed level collects within each key of its partition,
                // then takes one entry per key into its object.
                let held_partition = match tree[j].key {
                    Some(key) => {
                        let mut within = partition.clone();
                        within.push(super::rel::set_key(self.value(key, at)?));
                        within
                    }
                    None => partition.clone(),
                };
                let held = match tree[j].key {
                    Some(_) => self.level_rows(e, &tree, j, &held_partition, at)?,
                    None => self.level_rows(e, &tree, j, &partition, at)?,
                };
                let document = match tree[j].key {
                    None => held,
                    Some(key) => {
                        let first = SqlExpr::WindowFunction {
                            name: "row_number".to_string(),
                            args: Vec::new(),
                            distinct: false,
                            partition_by: held_partition,
                            order_by: Vec::new(),
                            frame: None,
                        };
                        let frame = self.stage_with(
                            rows,
                            vec![(Key::KeyedRows(e, j as u16), held), (Key::KeyPick(e, j as u16), first)],
                            outer,
                        )?;
                        rows = super::run::Rows::of(frame);
                        let env = rows.frame.env.clone();
                        let at = At { local: &env, outer };
                        let partition = self.level_partition(&tree, parent, keys, at)?;
                        self.keyed_object(e, j as u16, key, Some(&partition), at)?
                    }
                };
                let env = rows.frame.env.clone();
                let at = At { local: &env, outer };
                let partition = self.level_partition(&tree, parent, keys, at)?;
                let mut staged = vec![(Key::Level(e, j as u16), document)];
                let parent_pick = Key::Pick(e, parent as u16);
                if at.get(parent_pick).is_none() {
                    staged.push((
                        parent_pick,
                        SqlExpr::WindowFunction {
                            name: "row_number".to_string(),
                            args: Vec::new(),
                            distinct: false,
                            partition_by: partition,
                            order_by: Vec::new(),
                            frame: None,
                        },
                    ));
                }
                let frame = self.stage_with(rows, staged, outer)?;
                rows = super::run::Rows::of(frame);
            }
            // A metadata collector's top level collects within each key of
            // the group; the collector's value takes one entry per key.
            if let (true, Some(key)) = (keyed_root, tree[0].key) {
                let env = rows.frame.env.clone();
                let at = At { local: &env, outer };
                let mut within = self.population(&env)?;
                for k in keys {
                    within.push(self.value(*k, at)?);
                }
                within.push(self.value(key, at)?);
                let within: Vec<SqlExpr> = within.into_iter().map(super::rel::set_key).collect();
                let held = self.level_rows(e, &tree, 0, &within, at)?;
                let first = SqlExpr::WindowFunction {
                    name: "row_number".to_string(),
                    args: Vec::new(),
                    distinct: false,
                    partition_by: within,
                    order_by: Vec::new(),
                    frame: None,
                };
                let frame = self.stage_with(rows, vec![(Key::KeyedRows(e, 0), held), (Key::KeyPick(e, 0), first)], outer)?;
                rows = super::run::Rows::of(frame);
            }
        }
        Ok(rows)
    }

    /// What a keyed level holds for each row's key: the further level's
    /// object, or its own rows of the key collected over `within`.
    fn level_rows(&mut self, e: ExprId, tree: &[Level], j: usize, within: &[SqlExpr], at: At<'_>) -> Result<SqlExpr> {
        if tree[j].chain {
            let child = tree[j]
                .children
                .first()
                .map(|(_, id)| *id)
                .ok_or_else(|| self.contract("a chained metadata level with no further level"))?;
            return at.get(Key::Level(e, child)).ok_or_else(|| self.unreached("a chained metadata level's object"));
        }
        let (object, present) = self.level_object(e, &tree[j], at)?;
        let row = SqlExpr::Case {
            expr: None,
            when_clauses: vec![WhenClause::new(
                SqlExpr::and(vec![
                    any_present(present),
                    match at.get(Key::Pick(e, j as u16)) {
                        Some(p) => is_one(p),
                        None => SqlExpr::Literal(LiteralValue::Boolean(true)),
                    },
                ]),
                object,
            )],
            else_clause: None,
        };
        let concat = SqlExpr::WindowFunction {
            name: "GROUP_CONCAT".to_string(),
            args: vec![row, text(",")],
            distinct: false,
            partition_by: within.to_vec(),
            order_by: Vec::new(),
            frame: None,
        };
        Ok(self.document(concat))
    }

    /// A metadata collector's value: its group's object, one entry per key.
    pub(super) fn metadata_object(&mut self, e: ExprId, at: At<'_>) -> Result<SqlExpr> {
        let ExprKind::Metadata { level } = self.graph.expr(e).kind() else {
            return Err(self.contract("a metadata object of a value that is no metadata collector"));
        };
        self.keyed_object(e, 0, level.key, None, at)
    }
}

/// One level of a collection: its members, its interior heading, the level
/// it is nested in, and each nested member's level by member position.
struct Level {
    /// How the level's rows address their members.
    layout: crate::pipeline::middle::core::heading::Layout,
    members: Vec<CollectMember>,
    heading: Heading,
    parent: Option<usize>,
    children: Vec<(usize, u16)>,
    /// A metadata level's key: its rows are partitioned by the key's
    /// values within its enclosing level's partition, and it holds an
    /// object keyed by them.
    key: Option<ExprId>,
    /// Whether the keyed level holds a further keyed level rather than its
    /// own collected rows.
    chain: bool,
}

fn text(s: &str) -> SqlExpr {
    SqlExpr::Literal(LiteralValue::String(s.to_string()))
}

fn is_one(e: SqlExpr) -> SqlExpr {
    SqlExpr::Binary {
        left: Box::new(e),
        op: BinaryOperator::Equal,
        right: Box::new(SqlExpr::Literal(integer(1))),
    }
}

/// Whether a row contributes an object: some member of it is present. An
/// object with no members is present on every row it stands on.
fn any_present(present: Vec<SqlExpr>) -> SqlExpr {
    if present.is_empty() {
        SqlExpr::Literal(LiteralValue::Boolean(true))
    } else {
        SqlExpr::Parens(Box::new(SqlExpr::or(present)))
    }
}
