// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Tree-group lowering: `%(keys ~> {record})` with nested reductions.
//!
//! Handles data-oriented tree groups (`~> {cols}`), metadata tree groups
//! (`country:~> {cols}`), and mixed aggregates. Produces CTE chains via
//! `push_cte` for nested reductions.
//!
//! Entry points called from `relational::r_lower_group_by_spec`:
//! - `r_lower_tree_group_cte` — nested tree groups requiring CTEs
//! - `s_lower_reduction_item` — single reductions item dispatch

use super::builder::{Builder, CteBody, Projected, Qualify, Unprojected};
use super::relational::ReductionPayload;
use super::scalar;
use super::TransformCtx;
use crate::diagnostic::{Constraint, Internal};
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::expressions::{Enclyph, MetadataTarget, RecordMember};
use crate::pipeline::asts::core::literals::LiteralValue;
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::asts::core::Refined;
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::refined as ast_refined;
use crate::pipeline::sql_ast::{
    BinaryOperator, DomainExpression as SqlExpr, SelectBuilder, SelectItem, TableExpression,
    WhenClause,
};

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

#[derive(Clone)]
enum TreeJsonKey {
    Published(crate::names::ColId),
    Literal(String),
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum TreeColumn {
    Semantic(crate::relation::PortId),
    Physical(crate::names::ColId),
}

impl TreeColumn {
    fn column(self) -> crate::names::ColId {
        match self {
            TreeColumn::Semantic(port) => port.column(),
            TreeColumn::Physical(column) => column,
        }
    }
}

/// The physical identities realized levels have emitted for the columns
/// the levels above them read: each level's grouping keys and its output.
///
/// Once a level is realized, everything above it reads the emitted
/// identity rather than asking a semantic binding, so an input that binds
/// no semantic port — a join of sibling branches publishes physical slots
/// only — reads like any other input. A column absent from the carry is
/// read the ordinary way, which is what the first level over the source
/// does.
#[derive(Clone, Default)]
struct Carry(std::collections::HashMap<TreeColumn, crate::names::ColId>);

impl Carry {
    /// What a realized level emitted, in the order `build_tree_level`
    /// states: its grouping keys, then its outputs.
    fn record(
        &mut self,
        level: &TreeLevel,
        emitted: &[crate::pipeline::asts::core::ColumnMetadata],
    ) -> Result<()> {
        let outputs: Vec<crate::names::ColId> = if level.siblings.is_empty() {
            vec![level.output]
        } else {
            level
                .siblings
                .iter()
                .map(|sibling| sibling.output)
                .collect()
        };
        if emitted.len() != level.group_keys.len() + outputs.len() {
            return Err(Internal::invariant(
                "transformer::tree_group",
                "a realized level emitted a different width than it stated".to_string(),
            ));
        }
        for (key, column) in level.group_keys.iter().zip(emitted) {
            self.0.insert(*key, column.identity());
        }
        for (output, column) in outputs.iter().zip(&emitted[level.group_keys.len()..]) {
            self.0
                .insert(TreeColumn::Physical(*output), column.identity());
        }
        Ok(())
    }
}

/// An input read through the carry: a column a level below emitted is
/// read by that emission's identity; anything else by the input's own
/// answer.
struct Carried<'a> {
    input: &'a dyn Qualify,
    carry: &'a Carry,
}

impl Qualify for Carried<'_> {
    fn identities(&self) -> &crate::names::Registry {
        self.input.identities()
    }

    fn sql_sites(&self) -> Vec<crate::sql_binding::SqlSiteId> {
        self.input.sql_sites()
    }

    fn rebind_port(&self, port: crate::relation::PortId) -> Result<crate::names::ColId> {
        match self.carry.0.get(&TreeColumn::Semantic(port)) {
            Some(column) => self.input.rebind_physical(*column),
            None => self.input.rebind_port(port),
        }
    }

    fn slot_of_port(&self, port: crate::relation::PortId) -> Result<usize> {
        match self.carry.0.get(&TreeColumn::Semantic(port)) {
            Some(column) => self.input.slot_of_physical(*column),
            None => self.input.slot_of_port(port),
        }
    }

    fn slot_of_physical(&self, column: crate::names::ColId) -> Result<usize> {
        let column = self
            .carry
            .0
            .get(&TreeColumn::Physical(column))
            .copied()
            .unwrap_or(column);
        self.input.slot_of_physical(column)
    }

    fn rebind_physical(&self, column: crate::names::ColId) -> Result<crate::names::ColId> {
        let column = self
            .carry
            .0
            .get(&TreeColumn::Physical(column))
            .copied()
            .unwrap_or(column);
        self.input.rebind_physical(column)
    }

    fn scope_columns(&self) -> Vec<crate::pipeline::asts::core::ColumnMetadata> {
        self.input.scope_columns()
    }

    fn tree_valued(&self, column: crate::names::ColId) -> bool {
        self.input.tree_valued(column)
    }
}

/// A level and the levels whose outputs it reads. Each child collects the
/// same input the parent reads — never the parent's other children's
/// outputs — so two children are two branches over one input, joined on
/// the keys they share, and one child is the linear chain.
struct LevelPlan {
    level: TreeLevel,
    children: Vec<LevelPlan>,
}

/// One published cell of a level, as the LOWERING needs it: a key and the
/// value under it. A record's member says both; a tuple's element publishes
/// by position and says only the value.
///
/// This is the transformer's own working carrier, not a second construction
/// vocabulary: the AST's members are read into it exactly once, here.
#[derive(Clone)]
enum TreeLeaf {
    /// A self-keyed member: the key IS the column's published name.
    Published(crate::relation::PortId),
    /// A keyed member: an authored key and the value under it.
    Keyed {
        key: String,
        value: Box<ast_refined::DomainExpression>,
    },
    /// A tuple element: a value with no key of its own.
    Positional(Box<ast_refined::DomainExpression>),
}

#[derive(Clone)]
struct TreeLevel {
    leaves: Vec<TreeLeaf>,
    group_keys: Vec<TreeColumn>,
    output: crate::names::ColId,
    inner: Vec<(TreeJsonKey, crate::names::ColId)>,
    metadata_key: Option<TreeColumn>,
    siblings: Vec<TreeSibling>,
    /// A tuple level renders `JSON_ARRAY` rows; a record level `JSON_OBJECT`.
    positional: bool,
    /// The level whose output IS the tree: an empty collection is `[]`
    /// there. Every inner level answers NULL when it collects nothing, so
    /// the level above can tell an absent child from a present one with an
    /// empty collection, and elides the row accordingly.
    root: bool,
}

/// What a collecting level answers when every row was elided.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Empty {
    /// The released tree: `[]`.
    Array,
    /// An inner level: NULL, which the level above reads as absence.
    Null,
}

#[derive(Clone)]
struct TreeSibling {
    output: crate::names::ColId,
    leaves: Vec<TreeLeaf>,
    /// A tuple level renders `JSON_ARRAY`; a record level `JSON_OBJECT`.
    positional: bool,
}

fn resolved_heading(
    relation: &crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Vec<crate::names::ColId>> {
    Ok(ctx
        .relations
        .interface(relation)?
        .ports()
        .iter()
        .map(|port| port.column())
        .collect())
}

fn source_column(expression: &ast_refined::DomainExpression) -> Result<TreeColumn> {
    match expression {
        ast_refined::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence { column, .. },
        ))) => Ok(TreeColumn::Semantic(*column)),
        ast_refined::DomainExpression::Reference(Reference::Physical(column)) => {
            Ok(TreeColumn::Physical(*column))
        }
        _ => Err(Internal::invariant(
            "transformer::tree_group",
            "tree group key is not a resolved column".to_string(),
        )),
    }
}

fn current_column(source: TreeColumn, qualify: &dyn Qualify) -> Result<crate::names::ColId> {
    match source {
        TreeColumn::Semantic(port) => qualify.rebind_port(port),
        TreeColumn::Physical(column) => qualify.rebind_physical(column),
    }
}

fn current_slot(source: TreeColumn, qualify: &dyn Qualify) -> Result<usize> {
    match source {
        TreeColumn::Semantic(port) => qualify.slot_of_port(port),
        TreeColumn::Physical(column) => qualify.slot_of_physical(column),
    }
}

fn internal_tree_column(key: Option<&str>, ctx: &TransformCtx) -> crate::names::ColId {
    let scope = ctx.identities.anonymous_scope(None);
    let published = key.map(|text| ctx.identities.intern(text, false));
    ctx.identities.sql_column(
        scope,
        published,
        if published.is_some() {
            crate::names::Addressing::Bare
        } else {
            crate::names::Addressing::Hygienic
        },
    )
}

fn rebind_tree_expression(mut expression: SqlExpr, input: &dyn Qualify) -> Result<SqlExpr> {
    struct Rebind<'a> {
        input: &'a dyn Qualify,
        error: Option<&'static str>,
    }
    impl crate::pipeline::sql_ast::walk::SqlVisitorMut for Rebind<'_> {
        fn expr(&mut self, expression: &mut SqlExpr) {
            let SqlExpr::Column(source) = expression else {
                return;
            };
            match current_column(TreeColumn::Physical(*source), self.input) {
                Ok(column) => *expression = SqlExpr::Column(column),
                Err(_) => self.error = Some("tree group expression cannot be rebound"),
            }
        }
    }
    let mut visitor = Rebind { input, error: None };
    crate::pipeline::sql_ast::walk::visit_expression_mut(&mut expression, &mut visitor);
    match visitor.error {
        Some(message) => Err(Internal::invariant(
            "transformer::tree_group",
            message.to_string(),
        )),
        None => Ok(expression),
    }
}

/// A reduction item's PUBLISHED value.
pub(super) fn s_lower_out_reduction_item(
    value: ast_refined::DomainExpression,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SelectItem> {
    s_lower_reduction_item(ReductionPayload::Value(value), qualify, ctx)
}

/// One reduction, lowered where it publishes.
///
/// A metadata level reaching here is one the CTE road did not take, which
/// only happens when its own analysis said it needed no CTE — and there is
/// no straight rendering of a data-keyed record.
pub(super) fn s_lower_reduction_item(
    payload: ReductionPayload,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SelectItem> {
    let expr = match payload {
        ReductionPayload::Value(expr) => expr,
        ReductionPayload::Metadata(_) => {
            return Err(DelightQLError::from(Constraint::General {
                message: "a metadata group reduces through its own CTE chain".to_string(),
            }))
        }
    };
    match expr {
        ast_refined::DomainExpression::Application(ast_refined::FunctionApplication::Enclyph(
            Enclyph::Record(record),
        )) => Ok(SelectItem::Scaffolding {
            slot: ctx.identities.scaffolding_slot(),
            expr: s_lower_record_aggregate(record_leaves(record.members.into_vec()), qualify, ctx)?,
        }),
        ast_refined::DomainExpression::Application(ast_refined::FunctionApplication::Enclyph(
            Enclyph::Tuple(tuple),
        )) => {
            let lowered = tuple
                .elements
                .into_vec()
                .into_iter()
                .map(|element| scalar::s_lower_expression(element.into_value(), qualify, ctx))
                .collect::<Result<Vec<_>>>()?;
            Ok(SelectItem::Scaffolding {
                slot: ctx.identities.scaffolding_slot(),
                expr: tree_aggregate(
                    SqlExpr::function("JSON_ARRAY", lowered.clone()),
                    lowered,
                    Empty::Array,
                ),
            })
        }
        other => scalar::s_lower_select_item(other, qualify, ctx),
    }
}

/// Read a record's members as the lowering's leaves. An induced member is a
/// LEVEL, not a leaf; the level walk takes it.
fn record_leaves(members: Vec<RecordMember<Refined>>) -> Vec<TreeLeaf> {
    members
        .into_iter()
        .filter_map(|member| match member {
            RecordMember::SelfKeyed(NamedReference(ColumnOccurrence { column, .. })) => {
                Some(TreeLeaf::Published(column))
            }
            RecordMember::Keyed { key, value } => Some(TreeLeaf::Keyed { key, value }),
            RecordMember::Induced { .. } | RecordMember::Metadata { .. } => None,
            RecordMember::Spread(spread) => spread.expanded(),
        })
        .collect()
}

fn s_lower_record_aggregate(
    leaves: Vec<TreeLeaf>,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlExpr> {
    let mut arguments = Vec::new();
    let mut checks = Vec::new();
    for leaf in &leaves {
        if let Some((key, value, tree)) = lower_tree_leaf(leaf, qualify, ctx)? {
            arguments.push(json_key_expression(leaf_key(key)));
            arguments.push(if tree {
                SqlExpr::function("json", vec![value.clone()])
            } else {
                value.clone()
            });
            checks.push(value);
        }
    }
    Ok(tree_aggregate(
        SqlExpr::function("JSON_OBJECT", arguments),
        checks,
        Empty::Array,
    ))
}

/// The key a leaf publishes under. A positional leaf has none of its own —
/// where an object is being built around it, it publishes under the empty
/// name rather than inventing one.
fn leaf_key(key: Option<TreeJsonKey>) -> TreeJsonKey {
    key.unwrap_or(TreeJsonKey::Literal(String::new()))
}

fn json_key_expression(key: TreeJsonKey) -> SqlExpr {
    match key {
        TreeJsonKey::Published(column) => SqlExpr::PublishedNameLiteral(column),
        TreeJsonKey::Literal(text) => SqlExpr::literal(LiteralValue::String(text)),
    }
}

/// The key a leaf publishes under. Settled by the leaf alone — a level, a
/// qualifier and a lowering have no say in it. Reading the key back at a
/// layer ABOVE the one that emitted the value has to go through here: the
/// enclosing CTE emits its own outputs, and the source ports it grouped are
/// not bound there.
fn leaf_json_key(leaf: &TreeLeaf) -> Option<TreeJsonKey> {
    match leaf {
        TreeLeaf::Published(column) => Some(TreeJsonKey::Published(column.column())),
        TreeLeaf::Keyed { key, .. } => Some(TreeJsonKey::Literal(key.clone())),
        TreeLeaf::Positional(_) => None,
    }
}

fn lower_tree_leaf(
    leaf: &TreeLeaf,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<Option<(Option<TreeJsonKey>, SqlExpr, bool)>> {
    let value = match leaf {
        TreeLeaf::Published(column) => {
            let current = current_column(TreeColumn::Semantic(*column), qualify)?;
            return Ok(Some((
                Some(TreeJsonKey::Published(column.column())),
                SqlExpr::Column(current),
                qualify.tree_valued(current),
            )));
        }
        TreeLeaf::Keyed { value, .. } | TreeLeaf::Positional(value) => value,
    };
    let lowered = scalar::s_lower_expression((**value).clone(), qualify, ctx)?;
    let lowered = rebind_tree_expression(lowered, qualify)?;
    let tree = match &**value {
        ast_refined::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence { column, .. },
        ))) => qualify.tree_valued(current_column(TreeColumn::Semantic(*column), qualify)?),
        _ => false,
    };
    Ok(Some((leaf_json_key(leaf), lowered, tree)))
}

/// The embedding of an inner level's value in the row above it: the JSON
/// is parsed back so it nests, and an inner level that collected nothing
/// (NULL) prints as the empty collection it is. The ROW's presence is
/// judged on the raw value, before this reading, which is what keeps an
/// absent child from surfacing as a phantom member.
fn embed_inner(value: SqlExpr) -> SqlExpr {
    SqlExpr::function(
        "COALESCE",
        vec![
            SqlExpr::function("json", vec![value]),
            SqlExpr::function(
                "JSON",
                vec![SqlExpr::literal(LiteralValue::String("[]".to_string()))],
            ),
        ],
    )
}

fn tree_aggregate(value: SqlExpr, checks: Vec<SqlExpr>, empty: Empty) -> SqlExpr {
    let predicate = match checks.len() {
        0 => SqlExpr::literal(LiteralValue::Boolean(true)),
        _ => SqlExpr::or(
            checks
                .into_iter()
                .map(|expression| SqlExpr::Binary {
                    left: Box::new(expression),
                    op: BinaryOperator::IsNot,
                    right: Box::new(SqlExpr::literal(LiteralValue::Null)),
                })
                .collect(),
        ),
    };
    let row = SqlExpr::Case {
        expr: None,
        when_clauses: vec![WhenClause::new(predicate, value)],
        else_clause: None,
    };
    let concat = SqlExpr::function(
        "GROUP_CONCAT",
        vec![row, SqlExpr::literal(LiteralValue::String(",".to_string()))],
    );
    let array = SqlExpr::function(
        "JSON",
        vec![SqlExpr::concat(
            SqlExpr::concat(
                SqlExpr::literal(LiteralValue::String("[".to_string())),
                concat,
            ),
            SqlExpr::literal(LiteralValue::String("]".to_string())),
        )],
    );
    match empty {
        // `'[' || NULL || ']'` is NULL: a level that collected nothing
        // answers NULL without a case of its own.
        Empty::Null => array,
        Empty::Array => SqlExpr::function(
            "COALESCE",
            vec![
                array,
                SqlExpr::function(
                    "JSON",
                    vec![SqlExpr::literal(LiteralValue::String("[]".to_string()))],
                ),
            ],
        ),
    }
}

/// A level induced under a key: what it publishes, and by what geometry.
///
/// A record level keeps its MEMBERS, because a member may induce a level of
/// its own; a tuple's elements are values by construction, so it is already
/// its leaves.
#[derive(Clone)]
enum NestedLevel {
    Record(Vec<RecordMember<Refined>>),
    Tuple(Vec<TreeLeaf>),
    /// A metadata group under the member's key: its own level chain.
    Metadata(ast_refined::MetadataGroup),
}

impl NestedLevel {
    /// The cells this level publishes.
    fn leaves(&self) -> Vec<TreeLeaf> {
        match self {
            Self::Record(members) => record_leaves(members.clone()),
            Self::Tuple(leaves) => leaves.clone(),
            Self::Metadata(_) => Vec::new(),
        }
    }

    /// A tuple never induces; a record does when a member says so, and a
    /// metadata group always builds levels of its own.
    fn induces(&self) -> bool {
        match self {
            Self::Record(members) => members
                .iter()
                .any(|member| matches!(member, RecordMember::Induced { .. })),
            Self::Tuple(_) => false,
            Self::Metadata(_) => true,
        }
    }

    fn positional(&self) -> bool {
        matches!(self, Self::Tuple(_))
    }
}

/// A level's own leaves, and the levels induced beneath it.
fn split_tree_members(
    members: Vec<RecordMember<Refined>>,
) -> (Vec<TreeLeaf>, Vec<(String, NestedLevel)>) {
    let mut leaves = Vec::new();
    let mut nested = Vec::new();
    for member in members {
        match member {
            RecordMember::Induced { key, value } => match *value {
                Enclyph::Record(record) => {
                    nested.push((key, NestedLevel::Record(record.members.into_vec())))
                }
                Enclyph::EmptyRecord(_) => nested.push((key, NestedLevel::Record(Vec::new()))),
                Enclyph::Tuple(tuple) => {
                    let elements = tuple
                        .elements
                        .into_vec()
                        .into_iter()
                        .map(|element| match element.into_value() {
                            ast_refined::DomainExpression::Reference(Reference::Named(
                                NamedReference(ColumnOccurrence { column, .. }),
                            )) => TreeLeaf::Published(column),
                            expression => TreeLeaf::Positional(Box::new(expression)),
                        })
                        .collect();
                    nested.push((key, NestedLevel::Tuple(elements)));
                }
            },
            RecordMember::Metadata { key, group } => {
                nested.push((key, NestedLevel::Metadata(*group)))
            }
            RecordMember::SelfKeyed(NamedReference(ColumnOccurrence { column, .. })) => {
                leaves.push(TreeLeaf::Published(column))
            }
            RecordMember::Keyed { key, value } => leaves.push(TreeLeaf::Keyed { key, value }),
            RecordMember::Spread(spread) => spread.expanded(),
        }
    }
    (leaves, nested)
}

fn leaf_column(leaf: &TreeLeaf) -> Option<crate::names::ColId> {
    leaf_tree_column(leaf).map(TreeColumn::column)
}

/// The structural column a leaf reads, when it reads exactly one.
fn leaf_tree_column(leaf: &TreeLeaf) -> Option<TreeColumn> {
    match leaf {
        TreeLeaf::Published(column) => Some(TreeColumn::Semantic(*column)),
        TreeLeaf::Keyed { value, .. } | TreeLeaf::Positional(value) => source_column(value).ok(),
    }
}

fn leaf_dependencies(leaf: &TreeLeaf, qualify: &dyn Qualify) -> Result<Vec<TreeColumn>> {
    struct Columns<'a> {
        qualify: &'a dyn Qualify,
        found: Vec<TreeColumn>,
    }

    impl Columns<'_> {
        fn record(&mut self, column: TreeColumn) {
            if current_column(column, self.qualify).is_ok() && !self.found.contains(&column) {
                self.found.push(column);
            }
        }
    }

    impl crate::pipeline::ast_visit::AstVisit<Refined> for Columns<'_> {
        fn enter_domain(
            &mut self,
            expression: &ast_refined::DomainExpression,
        ) -> Result<crate::pipeline::ast_visit::Descent> {
            use crate::pipeline::ast_visit::Descent;

            match expression {
                ast_refined::DomainExpression::Reference(Reference::Named(NamedReference(
                    ColumnOccurrence { column, .. },
                ))) => {
                    self.record(TreeColumn::Semantic(*column));
                }
                // A staged window value is a column of this level's input
                // like any other, and a level beneath must carry it.
                ast_refined::DomainExpression::Reference(Reference::Physical(column)) => {
                    self.record(TreeColumn::Physical(*column));
                }
                _ => {}
            }
            Ok(Descent::Continue)
        }

        fn enter_function(
            &mut self,
            function: &ast_refined::FunctionApplication,
        ) -> Result<crate::pipeline::ast_visit::Descent> {
            use crate::pipeline::ast_visit::Descent;

            match function {
                ast_refined::FunctionApplication::Enclyph(Enclyph::Record(record)) => {
                    for member in record.members.iter() {
                        if let RecordMember::SelfKeyed(NamedReference(ColumnOccurrence {
                            column,
                            ..
                        })) = member
                        {
                            self.record(TreeColumn::Semantic(*column));
                        }
                    }
                }
                _ => {}
            }
            Ok(Descent::Continue)
        }
    }

    match leaf {
        TreeLeaf::Published(column) => Ok(vec![TreeColumn::Semantic(*column)]),
        TreeLeaf::Keyed { value, .. } | TreeLeaf::Positional(value) => {
            let mut columns = Columns {
                qualify,
                found: Vec::new(),
            };
            let _ = crate::pipeline::ast_visit::walk_visit_domain(&mut columns, value)?;
            Ok(columns.found)
        }
    }
}

fn plan_tree_levels(
    members: Vec<RecordMember<Refined>>,
    group_keys: Vec<TreeColumn>,
    output: crate::names::ColId,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<LevelPlan> {
    let (leaves, nested) = split_tree_members(members);
    if nested.is_empty() {
        return Ok(LevelPlan {
            level: TreeLevel {
                leaves,
                group_keys,
                output,
                inner: Vec::new(),
                metadata_key: None,
                siblings: Vec::new(),
                positional: false,
                root: false,
            },
            children: Vec::new(),
        });
    }
    let mut inner_keys = group_keys.clone();
    for member in &leaves {
        for column in leaf_dependencies(member, qualify)? {
            if !inner_keys.contains(&column) {
                inner_keys.push(column);
            }
        }
    }
    let all_flat = nested.len() > 1 && nested.iter().all(|(_, level)| !level.induces());
    let mut inner = Vec::new();
    let mut children = Vec::new();
    if all_flat {
        // Flat siblings collect the same rows at the same granularity: one
        // level computes them side by side.
        let siblings = nested
            .into_iter()
            .map(|(key, level)| {
                let alias = internal_tree_column(Some(&key), ctx);
                inner.push((TreeJsonKey::Literal(key.clone()), alias));
                TreeSibling {
                    output: alias,
                    leaves: level.leaves(),
                    positional: level.positional(),
                }
            })
            .collect();
        children.push(LevelPlan {
            level: TreeLevel {
                leaves: Vec::new(),
                group_keys: inner_keys,
                output: internal_tree_column(None, ctx),
                inner: Vec::new(),
                metadata_key: None,
                siblings,
                positional: false,
                root: false,
            },
            children: Vec::new(),
        });
    } else {
        for (key, level) in nested {
            let alias = internal_tree_column(Some(&key), ctx);
            children.push(match level {
                NestedLevel::Record(members) => {
                    plan_tree_levels(members, inner_keys.clone(), alias, qualify, ctx)?
                }
                // A metadata member builds its own chain: the target's
                // levels grouped one level finer, then one JSON_GROUP_OBJECT
                // per metadata key back up to this member's column.
                NestedLevel::Metadata(group) => {
                    plan_metadata_member(group, inner_keys.clone(), alias, qualify, ctx)?
                }
                // A tuple level collects its elements by position: one
                // array per row, duplicates and all.
                NestedLevel::Tuple(leaves) => LevelPlan {
                    level: TreeLevel {
                        leaves,
                        group_keys: inner_keys.clone(),
                        output: alias,
                        inner: Vec::new(),
                        metadata_key: None,
                        siblings: Vec::new(),
                        positional: true,
                        root: false,
                    },
                    children: Vec::new(),
                },
            });
            inner.push((TreeJsonKey::Literal(key), alias));
        }
    }
    // The level COLLECTS, whatever its nested members are: a metadata
    // member keeps its own key law beneath, and does not make the record
    // around it a scalar summary of the group. Its plain members key the
    // levels beneath, so the level holds one record per set of those keys
    // within its group.
    Ok(LevelPlan {
        level: TreeLevel {
            leaves,
            group_keys,
            output,
            inner,
            metadata_key: None,
            siblings: Vec::new(),
            positional: false,
            root: false,
        },
        children,
    })
}

/// Peel a reduction down to the record it ends in, collecting the metadata
/// keys it passed through on the way.
fn unwrap_tree_item(
    item: ReductionPayload,
) -> Result<(Vec<RecordMember<Refined>>, Vec<TreeColumn>)> {
    fn record_of(enclyph: Enclyph<Refined>) -> Result<Vec<RecordMember<Refined>>> {
        match enclyph {
            Enclyph::Record(record) => Ok(record.members.into_vec()),
            Enclyph::EmptyRecord(_) => Ok(Vec::new()),
            Enclyph::Tuple(_) => Err(DelightQLError::from(Constraint::General {
                message: "tree group constructor must be a record".to_string(),
            })),
        }
    }
    fn unwrap_group(
        group: ast_refined::MetadataGroup,
        metadata: &mut Vec<TreeColumn>,
    ) -> Result<Vec<RecordMember<Refined>>> {
        metadata.push(TreeColumn::Semantic(group.key.column));
        match group.target {
            MetadataTarget::Enclyph(enclyph) => record_of(enclyph),
            MetadataTarget::Group(nested) => unwrap_group(*nested, metadata),
        }
    }
    let mut metadata = Vec::new();
    let members = match item {
        ReductionPayload::Metadata(group) => unwrap_group(group, &mut metadata)?,
        ReductionPayload::Value(ast_refined::DomainExpression::Application(
            ast_refined::FunctionApplication::Enclyph(enclyph),
        )) => record_of(enclyph)?,
        ReductionPayload::Value(_) => {
            return Err(DelightQLError::from(Constraint::General {
                message: "tree group reduction is not a constructor".to_string(),
            }))
        }
    };
    Ok((members, metadata))
}

/// A metadata member's own level chain: the target's levels grouped by the
/// parent's keys plus every metadata key, then one JSON_GROUP_OBJECT level
/// per metadata key back up to the member's output. The same stacking the
/// top-level metadata road performs, standing under a record member.
fn plan_metadata_member(
    group: ast_refined::MetadataGroup,
    parent_keys: Vec<TreeColumn>,
    output: crate::names::ColId,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<LevelPlan> {
    fn unwrap(
        group: ast_refined::MetadataGroup,
        metadata: &mut Vec<TreeColumn>,
    ) -> Result<Vec<RecordMember<Refined>>> {
        metadata.push(TreeColumn::Semantic(group.key.column));
        match group.target {
            MetadataTarget::Enclyph(Enclyph::Record(record)) => Ok(record.members.into_vec()),
            MetadataTarget::Enclyph(Enclyph::EmptyRecord(_)) => Ok(Vec::new()),
            MetadataTarget::Enclyph(Enclyph::Tuple(_)) => {
                Err(DelightQLError::from(Constraint::General {
                    message: "metadata tree group constructor must be a record".to_string(),
                }))
            }
            MetadataTarget::Group(nested) => unwrap(*nested, metadata),
        }
    }
    let mut metadata = Vec::new();
    let members = unwrap(group, &mut metadata)?;
    let mut initial_keys = parent_keys.clone();
    initial_keys.extend(metadata.iter().copied());
    let innermost = internal_tree_column(None, ctx);
    let target = plan_tree_levels(members, initial_keys, innermost, qualify, ctx)?;
    Ok(wrap_in_metadata(
        target,
        &metadata,
        &parent_keys,
        output,
        ctx,
    ))
}

/// One JSON_GROUP_OBJECT level per metadata key, innermost first, each
/// grouped by the parent's keys plus the metadata keys above it, the
/// outermost publishing `output`.
fn wrap_in_metadata(
    mut plan: LevelPlan,
    metadata: &[TreeColumn],
    parent_keys: &[TreeColumn],
    output: crate::names::ColId,
    ctx: &TransformCtx,
) -> LevelPlan {
    for index in (0..metadata.len()).rev() {
        let inner = plan.level.output;
        let level_output = if index == 0 {
            output
        } else {
            internal_tree_column(None, ctx)
        };
        let mut keys = parent_keys.to_vec();
        keys.extend(metadata[..index].iter().copied());
        plan = LevelPlan {
            level: TreeLevel {
                leaves: Vec::new(),
                group_keys: keys,
                output: level_output,
                inner: vec![(TreeJsonKey::Literal(String::new()), inner)],
                metadata_key: Some(metadata[index]),
                siblings: Vec::new(),
                positional: false,
                root: false,
            },
            children: vec![plan],
        };
    }
    plan
}

/// Realize a plan over its input: the children first, each over the same
/// input, then the level over what they emitted.
///
/// One child is the linear chain: its output carries the level's keys,
/// and the level reads them from it. Several children are branches: the
/// input is frozen once, each branch is realized over its own copy, the
/// branches are joined NULL-safely on the keys they all emit first, and
/// the level reads the join. A deeper branch therefore never consumes the
/// rows its sibling still needs.
fn realize(
    plan: &LevelPlan,
    input: Builder<Projected>,
    carry: Carry,
    ctx: &TransformCtx,
) -> Result<(Builder<Projected>, Carry)> {
    let (input, mut carry) = match plan.children.as_slice() {
        [] => (input, carry),
        [child] => realize(child, input, carry, ctx)?,
        children => {
            // Every branch emits the level's inner keys first, then its own
            // output: the keys are the join, the outputs are what the level
            // reads.
            let key_count = children[0].level.group_keys.len();
            if children
                .iter()
                .any(|child| child.level.group_keys.len() != key_count)
            {
                return Err(Internal::invariant(
                    "transformer::tree_group",
                    "sibling branches group by different keys".to_string(),
                ));
            }
            let arms: Vec<Arm<'_>> = children
                .iter()
                .map(|child| {
                    let carry = carry.clone();
                    Box::new(move |arm: Builder<Projected>| {
                        realize(child, arm, carry, ctx).map(|(realized, _)| realized)
                    }) as Arm<'_>
                })
                .collect();
            let (joined, emitted) = join_arms(input, key_count, arms, ctx)?;
            let mut branch_carry = carry.clone();
            for (key, column) in children[0].level.group_keys.iter().zip(&emitted[0]) {
                branch_carry.0.insert(*key, *column);
            }
            for (child, columns) in children.iter().zip(&emitted) {
                if columns.len() != key_count + 1 {
                    return Err(Internal::invariant(
                        "transformer::tree_group",
                        "a branch emitted a different width than its level stated".to_string(),
                    ));
                }
                branch_carry
                    .0
                    .insert(TreeColumn::Physical(child.level.output), columns[key_count]);
            }
            (joined.project_all_carrying_hygiene()?, branch_carry)
        }
    };
    let realized = input.push_cte(|input| build_tree_level(&plan.level, input, &carry, ctx))?;
    carry.record(&plan.level, realized.columns())?;
    Ok((realized, carry))
}

/// One arm over a shared input: what it emits, over the builder it is
/// handed.
type Arm<'a> = Box<dyn FnOnce(Builder<Projected>) -> Result<Builder<Projected>> + 'a>;

/// Realize several arms over ONE evaluated input and join them NULL-safely
/// on the first `key_count` columns each emits; answer the join and, per
/// arm, the identities it emitted.
///
/// The input is materialized once, as a named CTE, and every arm reads
/// that name: an expression computed before the branch — `random:()`, a
/// window — is evaluated once and every arm sees the same rows. Copying
/// the input's SQL into each arm would evaluate it once per arm, and a
/// key computed differently per copy would drop the parent from the join.
/// The first arm runs on the input builder itself and carries every CTE
/// before it; the others re-read the CTE by name and carry only their own.
fn join_arms(
    input: Builder<Projected>,
    key_count: usize,
    arms: Vec<Arm<'_>>,
    _ctx: &TransformCtx,
) -> Result<(Builder<Unprojected>, Vec<Vec<crate::names::ColId>>)> {
    use crate::pipeline::sql_ast::{JoinCondition, JoinType};

    let input = input.push_cte(passthrough_body)?;
    let shared = input.publication().at_scope();
    let cols = input.columns().to_vec();
    let names = input.names().clone();
    let identities = std::rc::Rc::clone(input.identities());
    let site = input.publication().site();
    let mut arms = arms.into_iter();
    let first = arms.next().ok_or_else(|| {
        Internal::invariant(
            "transformer::tree_group",
            "a branch point with no arms".to_string(),
        )
    })?;
    let mut operands: Vec<super::builder::JoinOperand> = Vec::new();
    let mut emitted: Vec<Vec<crate::names::ColId>> = Vec::new();
    let mut admit = |realized: Builder<Projected>| -> Result<()> {
        let operand = realized.demote()?.into_join_operand()?;
        let columns: Vec<crate::names::ColId> = operand
            .columns()
            .iter()
            .map(|column| column.identity())
            .collect();
        if columns.len() < key_count {
            return Err(Internal::invariant(
                "transformer::tree_group",
                "an arm emitted fewer columns than the keys it joins on".to_string(),
            ));
        }
        emitted.push(columns);
        operands.push(operand);
        Ok(())
    };
    admit(first(input)?)?;
    for arm in arms {
        let reread = SelectBuilder::new()
            .from_tables(vec![TableExpression::Scope(shared)])
            .set_select(vec![SelectItem::star(
                cols.iter().map(|column| column.identity()).collect(),
            )])
            .standing_at(shared)
            .map_err(|e| Internal::invariant("transformer::tree_group", e))?;
        let arm_input = Builder::from_frozen_at_site(
            crate::pipeline::sql_ast::QueryExpression::Select(Box::new(reread)),
            super::builder::ScopeName::Fresh(names.fresh(super::builder::wrap_origin(
                &cols,
                &identities,
                crate::names::WrapReason::Projection,
            ))),
            cols.clone(),
            names.clone(),
            std::rc::Rc::clone(&identities),
            site,
        )?
        .project_all_carrying_hygiene()?;
        admit(arm(arm_input)?)?;
    }
    let anchor = emitted[0][..key_count].to_vec();
    let conditions: Vec<(JoinType, JoinCondition)> = emitted[1..]
        .iter()
        .map(|columns| {
            let condition = if key_count == 0 {
                SqlExpr::literal(LiteralValue::Boolean(true))
            } else {
                SqlExpr::and(
                    (0..key_count)
                        .map(|i| SqlExpr::Binary {
                            left: Box::new(SqlExpr::Column(anchor[i])),
                            op: BinaryOperator::IsNotDistinctFrom,
                            right: Box::new(SqlExpr::Column(columns[i])),
                        })
                        .collect(),
                )
            };
            (JoinType::Inner, JoinCondition::On(condition))
        })
        .collect();
    Ok((Builder::from_joins(operands, conditions)?, emitted))
}

/// The input, republished as a CTE of its own: every column carried in
/// place, so the arms above read one evaluated relation by name.
fn passthrough_body(input: &super::builder::CteInput) -> Result<CteBody> {
    let columns = input.scope_columns();
    let items = columns
        .iter()
        .map(|column| {
            SelectItem::expression_with_alias(SqlExpr::Column(column.identity()), column.identity())
        })
        .collect();
    let input_slots = (0..columns.len()).map(Some).collect();
    let mut body = assemble_tree_cte(input, items, Vec::new(), input_slots)?;
    // Every arm reads this binding, so it is evaluated into storage once: a
    // planner that inlined it would evaluate a volatile value once per arm,
    // and a key computed that way would drop its own row from the join.
    body.materialized_once = true;
    Ok(body)
}

/// Build one level's body, standing it at a scope of its own.
///
/// A level's select list is assembled from two sources that answer to nobody
/// in common: its grouping keys are occurrences of the input's heading, and
/// its aggregate is a column minted for the level. Neither is the level's
/// output, so the level mints the scope it stands at and republishes every
/// slot into it — the same one act a wrap performs, since aliasing the select
/// list and stating what the statement outputs are not separable.
fn assemble_tree_cte(
    input: &super::builder::CteInput,
    mut items: Vec<SelectItem>,
    group_by: Vec<SqlExpr>,
    input_slots: Vec<Option<usize>>,
) -> Result<CteBody> {
    let (at, outputs, physical_aliases) = super::builder::stand_cte_body_at(
        &mut items,
        input.scope(),
        crate::names::WrapReason::Aggregate,
        input.identities(),
    )?;
    let mut select = SelectBuilder::new()
        .from_tables(vec![TableExpression::Scope(input.scope())])
        .set_select(items);
    if !group_by.is_empty() {
        select = select.group_by(group_by);
    }
    let select = (select)
        .standing_at(at)
        .map_err(|e| Internal::invariant("transformer::tree_group", e))?;
    Ok(CteBody {
        query: crate::pipeline::sql_ast::QueryExpression::Select(Box::new(select)),
        output_columns: outputs,
        input_slots,
        physical_aliases,
        materialized_once: false,
    })
}

fn build_tree_level(
    level: &TreeLevel,
    cte_input: &super::builder::CteInput,
    carry: &Carry,
    ctx: &TransformCtx,
) -> Result<CteBody> {
    let input = &Carried {
        input: cte_input,
        carry,
    };
    let mut items = Vec::new();
    let mut group_by = Vec::new();
    let mut input_slots = Vec::new();
    for key in &level.group_keys {
        let current = current_column(*key, input)?;
        let expression = SqlExpr::Column(current);
        group_by.push(expression.clone());
        items.push(SelectItem::expression_with_alias(expression, key.column()));
        input_slots.push(Some(current_slot(*key, input)?));
    }
    if !level.siblings.is_empty() {
        for sibling in &level.siblings {
            let mut values = Vec::new();
            let mut checks = Vec::new();
            for leaf in &sibling.leaves {
                if let Some((key, value, tree)) = lower_tree_leaf(leaf, input, ctx)? {
                    checks.push(value.clone());
                    if !sibling.positional {
                        values.push(json_key_expression(leaf_key(key)));
                    }
                    values.push(if tree {
                        SqlExpr::function("json", vec![value])
                    } else {
                        value
                    });
                }
            }
            let row = SqlExpr::function(
                if sibling.positional {
                    "JSON_ARRAY"
                } else {
                    "JSON_OBJECT"
                },
                values,
            );
            items.push(SelectItem::expression_with_alias(
                tree_aggregate(row, checks, Empty::Null),
                sibling.output,
            ));
        }
    } else if let Some(metadata) = level.metadata_key {
        let key = SqlExpr::Column(current_column(metadata, input)?);
        let inner = level.inner.first().ok_or_else(|| {
            Internal::invariant(
                "transformer::tree_group",
                "metadata tree group has no constructor".to_string(),
            )
        })?;
        let value = SqlExpr::Column(current_column(TreeColumn::Physical(inner.1), input)?);
        items.push(SelectItem::expression_with_alias(
            SqlExpr::function("JSON_GROUP_OBJECT", vec![key, embed_inner(value)]),
            level.output,
        ));
    } else {
        let mut values = Vec::new();
        let mut checks = Vec::new();
        for leaf in &level.leaves {
            if let Some((key, value, tree)) = lower_tree_leaf(leaf, input, ctx)? {
                if !level.positional {
                    values.push(json_key_expression(leaf_key(key)));
                }
                values.push(if tree {
                    SqlExpr::function("json", vec![value.clone()])
                } else {
                    value.clone()
                });
                checks.push(value);
            }
        }
        for (key, source) in &level.inner {
            let value = SqlExpr::Column(current_column(TreeColumn::Physical(*source), input)?);
            values.push(json_key_expression(key.clone()));
            values.push(embed_inner(value.clone()));
            checks.push(value);
        }
        let row = SqlExpr::function(
            if level.positional {
                "JSON_ARRAY"
            } else {
                "JSON_OBJECT"
            },
            values,
        );
        items.push(SelectItem::expression_with_alias(
            tree_aggregate(
                row,
                checks,
                if level.root {
                    Empty::Array
                } else {
                    Empty::Null
                },
            ),
            level.output,
        ));
    }
    input_slots.resize(items.len(), None);
    assemble_tree_cte(cte_input, items, group_by, input_slots)
}

pub(super) fn r_lower_tree_group_cte(
    builder: Builder<Unprojected>,
    keys: Vec<ast_refined::DomainExpression>,
    reductions: Vec<ReductionPayload>,
    result: crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    let item = reductions.into_iter().next().ok_or_else(|| {
        Internal::invariant(
            "transformer::tree_group",
            "tree group has no reduction".to_string(),
        )
    })?;
    let outputs = resolved_heading(&result, ctx)?;
    if outputs.len() != keys.len() + 1 {
        return Err(Internal::invariant(
            "transformer::tree_group",
            "tree group output heading does not match its keys".to_string(),
        ));
    }
    let tree_output = outputs[keys.len()];
    let key_aliases = outputs[..keys.len()].to_vec();
    lower_tree_cte_chain(builder, &keys, item, &key_aliases, tree_output, ctx)
}

/// Build the CTE chain for one tree reduction: the source, one grouping
/// level per nesting depth, and a final projection of `keys + tree`. The
/// key items alias `key_aliases` slot by slot; the tree aliases
/// `tree_output`.
fn lower_tree_cte_chain(
    builder: Builder<Unprojected>,
    keys: &[ast_refined::DomainExpression],
    item: ReductionPayload,
    key_aliases: &[crate::names::ColId],
    tree_output: crate::names::ColId,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    let group_keys = keys
        .iter()
        .map(|expression| source_column(expression))
        .collect::<Result<Vec<_>>>()?;
    let (members, metadata) = unwrap_tree_item(item)?;
    let mut initial_keys = group_keys.clone();
    initial_keys.extend(metadata.iter().copied());
    // With no metadata key above it, the record level IS the released
    // tree; under one, it is the innermost level of the key chain.
    let record_output = if metadata.is_empty() {
        tree_output
    } else {
        internal_tree_column(None, ctx)
    };
    let mut plan = plan_tree_levels(members, initial_keys, record_output, &builder, ctx)?;
    plan.level.root = metadata.is_empty();
    let plan = wrap_in_metadata(plan, &metadata, &group_keys, tree_output, ctx);
    // Carrying hygiene: a staged window value is the input's own hygienic
    // column and the levels above read it.
    let projected = builder.project_all_carrying_hygiene()?;
    let (projected, carry) = realize(&plan, projected, Carry::default(), ctx)?;
    let read = Carried {
        input: &projected,
        carry: &carry,
    };
    let mut items = group_keys
        .iter()
        .zip(key_aliases.iter())
        .map(|(source, output)| {
            current_column(*source, &read)
                .map(|current| SelectItem::expression_with_alias(SqlExpr::Column(current), *output))
        })
        .collect::<Result<Vec<_>>>()?;
    let aggregate = current_column(TreeColumn::Physical(plan.level.output), &read)?;
    items.push(SelectItem::expression_with_alias(
        SqlExpr::Column(aggregate),
        tree_output,
    ));
    projected.add_projection(items)
}

/// The same question, asked of a reduction item. A metadata level always
/// reduces, analyzed or not.
pub(super) fn reduction_item_needs_cte(
    item: &ast_refined::ReductionItem,
    item_index: usize,
    plan: &ast_refined::ReductionPlan,
) -> bool {
    match item {
        ast_refined::ReductionItem::Out(_) => plan.needs_cte(
            crate::pipeline::asts::core::TreeGroupLocation::InReductions,
            item_index,
        ),
        // A delegate selects a representative row; it builds no tree.
        ast_refined::ReductionItem::Delegate(_) => false,
        ast_refined::ReductionItem::Metadata(metadata) => metadata
            .group
            .cte_requirements
            .as_ref()
            .is_none_or(|req| req.needs_cte),
        // A group holding a pivot takes the pivot road, which builds no
        // tree-group CTEs.
        ast_refined::ReductionItem::Pivot(_) => false,
    }
}

/// Lower a grouped reduction that MIXES CTE-needing tree groups with other
/// reductions (aggregates, simple trees, arbitrary delegates).
///
/// Each CTE-needing tree gets its own chain — a chain reduces at interior
/// granularities, so a sibling reduction cannot ride inside it without
/// counting groups instead of rows. Everything else shares one straight
/// grouped arm. Every arm reads the ONE input `join_arms` materializes,
/// groups by the same keys, joins on them NULL-safely, and the final
/// projection publishes the resolver's heading in its order.
pub(super) fn r_lower_tree_group_mixed(
    builder: Builder<Unprojected>,
    keys: Vec<ast_refined::OutItem>,
    reductions: Vec<ast_refined::ReductionItem>,
    plan: ast_refined::ReductionPlan,
    arbitrary: Vec<ast_refined::OutItem>,
    result: crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    let outputs = resolved_heading(&result, ctx)?;
    let key_count = keys.len();
    // An arbitrary payload that duplicates a group key is stamped `None` by
    // the resolver and publishes nothing — it is already emitted in group
    // position. Counting the syntactic payload instead refuses the whole
    // query, and carrying a slot for it would later emit some other arm's
    // first aggregate under this payload's output name.
    let published_arbitrary = arbitrary
        .iter()
        .filter(|item| item.output().is_some())
        .count();
    if outputs.len() != key_count + reductions.len() + published_arbitrary {
        return Err(Internal::invariant(
            "transformer::tree_group",
            "tree group output heading does not match its reductions".to_string(),
        ));
    }
    let key_exprs: Vec<ast_refined::DomainExpression> = super::relational::published_values(keys);

    // Freeze the source once; every arm reads its own copy. EVERY ARM IS
    // STILL THE SAME RELATION: the arms re-emit the operand's own
    // occurrences, so each carries the operand's binding and a reduction
    // item's references reach the positions they were addressed against.
    // Partition: CTE-needing trees each take an arm; the rest share arm 0.
    // `slot[i]` is where reduction i lands — (arm, offset past keys) — and
    // `None` marks a payload that publishes nothing, so the ledger holds
    // exactly the slots the heading has. The layout is decided here, before
    // any arm is built, so the arms need no shared mutable state.
    let mut plain: Vec<(usize, ast_refined::ReductionItem)> = Vec::new();
    let mut trees: Vec<(usize, ReductionPayload)> = Vec::new();
    for (index, item) in reductions.into_iter().enumerate() {
        if !reduction_item_needs_cte(&item, index, &plan) {
            plain.push((index, item));
            continue;
        }
        let payload = match item {
            ast_refined::ReductionItem::Out(item) => {
                match super::relational::into_published_value(item) {
                    Some(value) => ReductionPayload::Value(value),
                    None => continue,
                }
            }
            ast_refined::ReductionItem::Metadata(metadata) => {
                ReductionPayload::Metadata(metadata.group)
            }
            // `needs_cte` answers `false` for a pivot and a delegate, so
            // neither reaches the tree arms.
            ast_refined::ReductionItem::Pivot(_) | ast_refined::ReductionItem::Delegate(_) => {
                continue
            }
        };
        trees.push((index, payload));
    }
    let arbitrary_base = plain.len() + trees.len();
    let mut slot: Vec<Option<(usize, usize)>> = vec![None; arbitrary_base + arbitrary.len()];
    let mut offset = 0usize;
    for (index, entry) in &plain {
        if entry
            .out_item()
            .and_then(ast_refined::OutItem::value)
            .is_some()
        {
            slot[*index] = Some((0, offset));
            offset += 1;
        }
    }
    for (position, entry) in arbitrary.iter().enumerate() {
        if entry.value().is_some() && entry.output().is_some() {
            slot[arbitrary_base + position] = Some((0, offset));
            offset += 1;
        }
    }
    for (arm, (index, _)) in trees.iter().enumerate() {
        slot[*index] = Some((arm + 1, 0));
    }

    // EVERY ARM READS ONE EVALUATED INPUT: the arms re-emit the operand's
    // own occurrences over the input `join_arms` materializes once, so a
    // value computed before the reduction is computed once, and a reduction
    // item's references reach the positions they were addressed against.
    let key_aliases = key_exprs
        .iter()
        .map(|expression| source_column(expression))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(TreeColumn::column)
        .collect::<Vec<_>>();
    let mut arms: Vec<Arm<'_>> = Vec::new();
    // Arm 0: keys + every non-CTE reduction, one straight GROUP BY. With
    // no plain reductions this is the distinct-keys anchor the tree arms
    // join back to.
    {
        let key_exprs = key_exprs.clone();
        arms.push(Box::new(move |input: Builder<Projected>| {
            let arm = input.demote()?;
            let keys: Vec<SelectItem> = key_exprs
                .iter()
                .map(|expression| scalar::s_lower_select_item(expression.clone(), &arm, ctx))
                .collect::<Result<_>>()?;
            let mut aggregates: Vec<SelectItem> = Vec::new();
            for (_, entry) in plain.iter() {
                let Some(expr) = entry.out_item().and_then(ast_refined::OutItem::value) else {
                    continue;
                };
                let mut item = s_lower_out_reduction_item(expr.clone(), &arm, ctx)?;
                super::relational::alias_unaliased(&mut item, entry.output().column());
                aggregates.push(item);
            }
            for entry in arbitrary.into_iter() {
                let output = entry.output().map(crate::relation::PortId::column);
                let Some(expr) = super::relational::into_published_value(entry) else {
                    continue;
                };
                let Some(col) = output else {
                    continue; // resolver stamped None — no output column
                };
                let lowered = scalar::s_lower_select_item(expr, &arm, ctx)?;
                let mut item = match lowered.expr() {
                    Some(expr) => lowered.with_expr(SqlExpr::intrinsic(
                        crate::names::Intrinsic::Arbitrary,
                        vec![expr.clone()],
                    )),
                    None => lowered,
                };
                super::relational::alias_unaliased(&mut item, col);
                aggregates.push(item);
            }
            arm.add_group_by(super::builder::GroupBySpec { keys, aggregates })
        }));
    }
    // One arm per CTE-needing tree. The arm's interior aliases are its
    // own: the composite's final projection is where the resolver's
    // occurrences are published.
    for (_, payload) in trees {
        let key_exprs = key_exprs.clone();
        let key_aliases = key_aliases.clone();
        arms.push(Box::new(move |input: Builder<Projected>| {
            let arm = input.demote()?;
            let tree_output = internal_tree_column(None, ctx);
            lower_tree_cte_chain(arm, &key_exprs, payload, &key_aliases, tree_output, ctx)
        }));
    }
    let input = builder.project_all_carrying_hygiene()?;
    let (joined, emitted) = join_arms(input, key_count, arms, ctx)?;

    let mut final_items: Vec<SelectItem> = Vec::new();
    for (i, anchor) in emitted[0][..key_count].iter().enumerate() {
        final_items.push(SelectItem::expression_with_alias(
            SqlExpr::Column(*anchor),
            outputs[i],
        ));
    }
    // Published slots only, in heading order — a `None` payload contributes
    // no output, so it must not consume one either.
    for ((arm, offset), output) in slot.iter().flatten().zip(&outputs[key_count..]) {
        let column = emitted[*arm]
            .get(key_count + offset)
            .copied()
            .ok_or_else(|| {
                Internal::invariant(
                    "transformer::tree_group",
                    "a reduction arm emitted fewer columns than its slots".to_string(),
                )
            })?;
        final_items.push(SelectItem::expression_with_alias(
            SqlExpr::Column(column),
            *output,
        ));
    }

    joined.add_projection_publishing(
        final_items,
        result.scope(),
        super::relational::columns_from_relation(&result, ctx)?,
    )
}

pub(super) fn r_lower_tree_group_in_keys(
    builder: Builder<Unprojected>,
    keys: Vec<ast_refined::DomainExpression>,
    reductions: Vec<ast_refined::DomainExpression>,
    result: crate::relation::SemanticRelation,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    let mut tree = None;
    let mut tree_at = 0usize;
    let mut plain = Vec::new();
    // WHERE the tree stood among the keys is part of the answer: the result
    // publishes the reducing-by items in the order they were written, and a
    // key beside the tree holds its own slot in that order.
    for (position, expression) in keys.into_iter().enumerate() {
        if tree.is_none() && matches!(expression, ast_refined::DomainExpression::Application(_)) {
            tree_at = position;
            tree = Some(expression);
        } else {
            plain.push(expression);
        }
    }
    let tree = tree.ok_or_else(|| {
        Internal::invariant(
            "transformer::tree_group",
            "reducing-by tree group is missing".to_string(),
        )
    })?;
    let ast_refined::DomainExpression::Application(ast_refined::FunctionApplication::Enclyph(
        Enclyph::Record(record),
    )) = tree
    else {
        return Err(DelightQLError::from(Constraint::General {
            message: "reducing-by tree group must be a record".to_string(),
        }));
    };
    let members = record.members.into_vec();
    let outputs = resolved_heading(&result, ctx)?;
    let plain_columns = plain
        .iter()
        .map(|expression| source_column(expression))
        .collect::<Result<Vec<_>>>()?;
    // Every reducing-by item publishes one output and every reduction
    // publishes one. A width that disagrees means this lowering and the
    // heading the resolver published are describing different relations.
    // Emitting under that disagreement drops keys from the result while
    // still grouping by them, so the width is checked instead.
    let key_count = plain_columns.len() + 1;
    if outputs.len() != key_count + reductions.len() {
        return Err(Internal::invariant(
            "transformer::tree_group",
            format!(
                "reducing-by tree group publishes {} outputs for {} key(s) and {} \
                 reduction(s)",
                outputs.len(),
                key_count,
                reductions.len()
            ),
        ));
    }
    let (leaves, nested) = split_tree_members(members);
    // The keys this record groups by, in the order its CTE emits them: the
    // keys beside it, then its own plain members' columns.
    let mut inner_keys = plain_columns.clone();
    for leaf in &leaves {
        inner_keys.push(leaf_tree_column(leaf).ok_or_else(|| {
            Internal::invariant(
                "transformer::tree_group",
                "tree group leaf has no structural column".to_string(),
            )
        })?);
    }
    // A flat nested member collects inside the record's own grouping CTE;
    // a member that nests again is a level plan of its own, realized as a
    // branch over the same input and joined back on the record's keys.
    enum Placement {
        Flat(usize),
        Deep(usize),
    }
    let mut placements: Vec<(String, Placement)> = Vec::new();
    let mut flat: Vec<(String, NestedLevel)> = Vec::new();
    let mut deep: Vec<LevelPlan> = Vec::new();
    for (key, level) in nested {
        if level.induces() {
            let alias = internal_tree_column(Some(&key), ctx);
            let plan = match level {
                NestedLevel::Record(members) => {
                    plan_tree_levels(members, inner_keys.clone(), alias, &builder, ctx)?
                }
                NestedLevel::Metadata(group) => {
                    plan_metadata_member(group, inner_keys.clone(), alias, &builder, ctx)?
                }
                NestedLevel::Tuple(_) => unreachable!("a tuple never induces"),
            };
            placements.push((key, Placement::Deep(deep.len())));
            deep.push(plan);
        } else {
            placements.push((key.clone(), Placement::Flat(flat.len())));
            flat.push((key, level));
        }
    }
    let flat_aliases = flat
        .iter()
        .map(|(key, _)| internal_tree_column(Some(key), ctx))
        .collect::<Vec<_>>();
    let leaf_count = leaves.len();
    let flat_count = flat.len();
    let extra_outputs = outputs[key_count..].to_vec();
    // The record's own grouping CTE: its keys, its flat nested members,
    // and the reductions beside it, one GROUP BY.
    let main_body = |input: &super::builder::CteInput| -> Result<CteBody> {
        let mut items = Vec::new();
        let mut group_by = Vec::new();
        let mut input_slots = Vec::new();
        for source in &plain_columns {
            let current = current_column(*source, input)?;
            let expression = SqlExpr::Column(current);
            group_by.push(expression.clone());
            items.push(SelectItem::expression_with_alias(
                expression,
                source.column(),
            ));
            input_slots.push(Some(current_slot(*source, input)?));
        }
        for leaf in &leaves {
            if let Some((_, value, _)) = lower_tree_leaf(leaf, input, ctx)? {
                let alias = leaf_column(leaf).ok_or_else(|| {
                    Internal::invariant(
                        "transformer::tree_group",
                        "tree group leaf has no structural column".to_string(),
                    )
                })?;
                group_by.push(value.clone());
                items.push(SelectItem::expression_with_alias(value, alias));
            }
        }
        for ((_, level), alias) in flat.iter().zip(flat_aliases.iter()) {
            let positional = level.positional();
            let mut values = Vec::new();
            let mut checks = Vec::new();
            for leaf in &level.leaves() {
                if let Some((key, value, tree)) = lower_tree_leaf(leaf, input, ctx)? {
                    checks.push(value.clone());
                    if positional {
                        values.push(value);
                    } else {
                        values.push(json_key_expression(leaf_key(key)));
                        values.push(if tree {
                            SqlExpr::function("json", vec![value])
                        } else {
                            value
                        });
                    }
                }
            }
            items.push(SelectItem::expression_with_alias(
                tree_aggregate(
                    SqlExpr::function(
                        if positional {
                            "JSON_ARRAY"
                        } else {
                            "JSON_OBJECT"
                        },
                        values,
                    ),
                    checks,
                    Empty::Array,
                ),
                *alias,
            ));
        }
        for (expression, output) in reductions.iter().zip(extra_outputs.iter()) {
            let mut item =
                s_lower_reduction_item(ReductionPayload::Value(expression.clone()), input, ctx)?;
            if let Some(realized) = item.realizing(*output) {
                item = realized;
            }
            items.push(item);
        }
        input_slots.resize(items.len(), None);
        assemble_tree_cte(input, items, group_by, input_slots)
    };
    // What the final projection reads: the record CTE's columns in their
    // emitted order, and each deep member's output.
    let value_start = plain_columns.len();
    let items_over = |main: &[crate::names::ColId], deep_out: &[crate::names::ColId]| {
        let mut json = Vec::new();
        for (index, leaf) in leaves.iter().enumerate() {
            json.push(json_key_expression(leaf_key(leaf_json_key(leaf))));
            json.push(SqlExpr::Column(main[value_start + index]));
        }
        for (key, placement) in &placements {
            json.push(SqlExpr::literal(LiteralValue::String(key.clone())));
            json.push(match placement {
                Placement::Flat(index) => SqlExpr::function(
                    "json",
                    vec![SqlExpr::Column(main[value_start + leaf_count + index])],
                ),
                Placement::Deep(index) => embed_inner(SqlExpr::Column(deep_out[*index])),
            });
        }
        // The keys go out in the order they were written, the tree in its
        // own place among them. A key beside the tree is an ordinary
        // grouping key and an ordinary output; the CTE already grouped by
        // it and carried it, and this is where it stops being dropped.
        let mut items = Vec::with_capacity(outputs.len());
        for (position, output) in outputs[..key_count].iter().enumerate() {
            if position == tree_at {
                items.push(SelectItem::expression_with_alias(
                    SqlExpr::function("JSON_OBJECT", json.clone()),
                    *output,
                ));
                continue;
            }
            let plain_index = if position < tree_at {
                position
            } else {
                position - 1
            };
            items.push(SelectItem::expression_with_alias(
                SqlExpr::Column(main[plain_index]),
                *output,
            ));
        }
        let extras_start = value_start + leaf_count + flat_count;
        items.extend(
            main[extras_start..]
                .iter()
                .zip(outputs[key_count..].iter())
                .map(|(source, output)| {
                    SelectItem::expression_with_alias(SqlExpr::Column(*source), *output)
                }),
        );
        items
    };
    // Carrying hygiene: a window staged for a nested level is the input's
    // own hygienic column and these CTEs read it.
    let input = builder.project_all_carrying_hygiene()?;
    if deep.is_empty() {
        let projected = input.push_cte(main_body)?;
        let main: Vec<crate::names::ColId> = projected
            .columns()
            .iter()
            .map(|column| column.identity())
            .collect();
        let items = items_over(&main, &[]);
        return projected.add_projection(items);
    }
    let inner_key_count = inner_keys.len();
    let mut arms: Vec<Arm<'_>> = Vec::new();
    arms.push(Box::new(|arm: Builder<Projected>| arm.push_cte(main_body)));
    for plan in &deep {
        arms.push(Box::new(move |arm: Builder<Projected>| {
            realize(plan, arm, Carry::default(), ctx).map(|(realized, _)| realized)
        }));
    }
    let (joined, emitted) = join_arms(input, inner_key_count, arms, ctx)?;
    let deep_out = emitted[1..]
        .iter()
        .map(|columns| {
            columns.get(inner_key_count).copied().ok_or_else(|| {
                Internal::invariant(
                    "transformer::tree_group",
                    "a deep member of a grouping-key record emitted no output".to_string(),
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let items = items_over(&emitted[0], &deep_out);
    joined.add_projection(items)
}
