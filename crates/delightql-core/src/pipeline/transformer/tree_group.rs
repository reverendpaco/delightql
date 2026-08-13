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

/// One published cell of a level, as the LOWERING needs it: a key and the
/// value under it. A record's member says both; a tuple's element publishes
/// by position and says only the value.
///
/// This is the transformer's own working carrier, not a second construction
/// vocabulary: the AST's members are read into it exactly once, here.
#[derive(Clone)]
enum TreeLeaf {
    /// A self-keyed member: the key IS the column's published name.
    Published(crate::names::ColId),
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
    group_keys: Vec<crate::names::ColId>,
    output: crate::names::ColId,
    inner: Vec<(TreeJsonKey, crate::names::ColId)>,
    metadata_key: Option<crate::names::ColId>,
    siblings: Vec<TreeSibling>,
}

#[derive(Clone)]
struct TreeSibling {
    output: crate::names::ColId,
    leaves: Vec<TreeLeaf>,
    /// A tuple level renders `JSON_ARRAY`; a record level `JSON_OBJECT`.
    positional: bool,
}

fn resolved_heading(
    schema: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<Vec<crate::names::ColId>> {
    Ok(ctx.identities.known_heading(schema)?.to_vec())
}

fn source_column(expression: &ast_refined::DomainExpression) -> Result<crate::names::ColId> {
    match expression {
        ast_refined::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence { column, .. },
        ))) => Ok(*column),
        _ => Err(DelightQLError::ParseError {
            message: "tree group key is not a resolved column".to_string(),
            source: None,
            subcategory: None,
        }),
    }
}

fn current_column(
    source: crate::names::ColId,
    qualify: &dyn Qualify,
) -> Result<crate::names::ColId> {
    let identities = qualify.identities();
    let columns = qualify.scope_columns();
    // The rebind tiers, in their order: identity, republication chain,
    // then value. A value-only match is too loose to lead — a prepend
    // stage leaves the original column and its rename both in scope, one
    // value carried twice, while the chain names exactly the occurrence
    // the tree's reference was resolved to.
    if columns.iter().any(|column| column.identity() == source) {
        return Ok(qualify.read_through_joins(source));
    }
    let mut chained = columns
        .iter()
        .map(|column| column.identity())
        .filter(|candidate| identities.republishes(*candidate, source));
    match (chained.next(), chained.next()) {
        (Some(column), None) => return Ok(qualify.read_through_joins(column)),
        (Some(_), Some(_)) => {
            return Err(DelightQLError::ParseError {
                message: "tree group column is ambiguous at this CTE level".to_string(),
                source: None,
                subcategory: None,
            })
        }
        (None, _) => {}
    }
    let matches = columns
        .into_iter()
        .map(|column| column.identity())
        .filter(|candidate| identities.same_value(*candidate, source))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        // What a leaf emits is read, not published — the key beside it carries
        // the published name — so the occurrence wanted here is the one a FROM
        // entry actually offers. A join publishes a heading under no alias, and
        // stopping at its occurrence qualifies the leaf by a table the
        // statement does not contain.
        [column] => Ok(qualify.read_through_joins(*column)),
        [] => Err(DelightQLError::ParseError {
            message: "tree group column is not present at this CTE level".to_string(),
            source: None,
            subcategory: None,
        }),
        _ => Err(DelightQLError::ParseError {
            message: "tree group column is ambiguous at this CTE level".to_string(),
            source: None,
            subcategory: None,
        }),
    }
}

fn internal_tree_column(key: Option<&str>, ctx: &TransformCtx) -> crate::names::ColId {
    let scope = ctx.identities.mint_scope(
        crate::names::ScopeOrigin::AnonRelation,
        crate::names::Hint::None,
        None,
    );
    let published = key.map(|text| ctx.identities.intern(text, false));
    ctx.identities.mint_column(
        scope,
        crate::names::ColumnOrigin::Computed {
            via: crate::names::Computation::Aggregate,
        },
        published,
        if published.is_some() {
            crate::names::Addressing::Bare
        } else {
            crate::names::Addressing::Hygienic
        },
        crate::names::ValueFacts::default(),
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
            match current_column(*source, self.input) {
                Ok(column) => *expression = SqlExpr::Column(column),
                Err(_) => self.error = Some("tree group expression cannot be rebound"),
            }
        }
    }
    let mut visitor = Rebind { input, error: None };
    crate::pipeline::sql_ast::walk::visit_expression_mut(&mut expression, &mut visitor);
    match visitor.error {
        Some(message) => Err(DelightQLError::ParseError {
            message: message.to_string(),
            source: None,
            subcategory: None,
        }),
        None => Ok(expression),
    }
}

/// A reduction item's PUBLISHED value. A crossing is never a tree group, so
/// it lowers as the ordinary published value.
pub(super) fn s_lower_out_reduction_item(
    value: ast_refined::OutValue,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SelectItem> {
    match value {
        ast_refined::OutValue::Domain(domain) => {
            s_lower_reduction_item(ReductionPayload::Value(domain), qualify, ctx)
        }
        crossing @ ast_refined::OutValue::Truth(_) => Ok(SelectItem::Expression {
            expr: super::scalar::s_lower_out_value(crossing, qualify, ctx)?,
            alias: None,
        }),
    }
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
            return Err(DelightQLError::ParseError {
                message: "a metadata group reduces through its own CTE chain".to_string(),
                source: None,
                subcategory: None,
            })
        }
    };
    match expr {
        ast_refined::DomainExpression::Application(ast_refined::FunctionApplication::Enclyph(
            Enclyph::Record(record),
        )) => Ok(SelectItem::Expression {
            expr: s_lower_record_aggregate(record_leaves(record.members.into_vec()), qualify, ctx)?,
            alias: None,
        }),
        ast_refined::DomainExpression::Application(ast_refined::FunctionApplication::Enclyph(
            Enclyph::Tuple(tuple),
        )) => {
            let lowered = tuple
                .elements
                .into_vec()
                .into_iter()
                .map(|element| scalar::s_lower_expression(element, qualify, ctx))
                .collect::<Result<Vec<_>>>()?;
            Ok(SelectItem::Expression {
                expr: tree_aggregate(SqlExpr::function("JSON_ARRAY", lowered.clone()), lowered),
                alias: None,
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
            RecordMember::Induced { .. } => None,
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

fn lower_tree_leaf(
    leaf: &TreeLeaf,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<Option<(Option<TreeJsonKey>, SqlExpr, bool)>> {
    let value = match leaf {
        TreeLeaf::Published(column) => {
            let current = current_column(*column, qualify)?;
            return Ok(Some((
                Some(TreeJsonKey::Published(*column)),
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
        ))) => qualify.tree_valued(current_column(*column, qualify)?),
        _ => false,
    };
    let key = match leaf {
        TreeLeaf::Keyed { key, .. } => Some(TreeJsonKey::Literal(key.clone())),
        TreeLeaf::Positional(_) => None,
        TreeLeaf::Published(_) => unreachable!("answered above"),
    };
    Ok(Some((key, lowered, tree)))
}

fn tree_aggregate(value: SqlExpr, checks: Vec<SqlExpr>) -> SqlExpr {
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
    SqlExpr::function(
        "COALESCE",
        vec![
            array,
            SqlExpr::function(
                "JSON",
                vec![SqlExpr::literal(LiteralValue::String("[]".to_string()))],
            ),
        ],
    )
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
}

impl NestedLevel {
    /// The cells this level publishes.
    fn leaves(&self) -> Vec<TreeLeaf> {
        match self {
            Self::Record(members) => record_leaves(members.clone()),
            Self::Tuple(leaves) => leaves.clone(),
        }
    }

    /// A tuple never induces; a record does when a member says so.
    fn induces(&self) -> bool {
        match self {
            Self::Record(members) => members
                .iter()
                .any(|member| matches!(member, RecordMember::Induced { .. })),
            Self::Tuple(_) => false,
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
                        .map(|element| match element {
                            ast_refined::DomainExpression::Reference(Reference::Named(
                                NamedReference(ColumnOccurrence { column, .. }),
                            )) => TreeLeaf::Published(column),
                            expression => TreeLeaf::Positional(Box::new(expression)),
                        })
                        .collect();
                    nested.push((key, NestedLevel::Tuple(elements)));
                }
            },
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
    match leaf {
        TreeLeaf::Published(column) => Some(*column),
        TreeLeaf::Keyed { value, .. } | TreeLeaf::Positional(value) => {
            source_column(value).ok()
        }
    }
}

fn leaf_dependencies(leaf: &TreeLeaf, qualify: &dyn Qualify) -> Result<Vec<crate::names::ColId>> {
    struct Columns<'a> {
        qualify: &'a dyn Qualify,
        found: Vec<crate::names::ColId>,
    }

    impl Columns<'_> {
        fn record(&mut self, column: crate::names::ColId) {
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
                    self.record(*column);
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
                            self.record(*column);
                        }
                    }
                }
                _ => {}
            }
            Ok(Descent::Continue)
        }
    }

    match leaf {
        TreeLeaf::Published(column) => Ok(vec![*column]),
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

fn collect_tree_levels(
    members: Vec<RecordMember<Refined>>,
    group_keys: Vec<crate::names::ColId>,
    output: crate::names::ColId,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
    levels: &mut Vec<TreeLevel>,
) -> Result<()> {
    let (leaves, nested) = split_tree_members(members);
    if nested.is_empty() {
        levels.push(TreeLevel {
            leaves,
            group_keys,
            output,
            inner: Vec::new(),
            metadata_key: None,
            siblings: Vec::new(),
        });
        return Ok(());
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
    if all_flat {
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
        levels.push(TreeLevel {
            leaves: Vec::new(),
            group_keys: inner_keys,
            output: internal_tree_column(None, ctx),
            inner: Vec::new(),
            metadata_key: None,
            siblings,
        });
    } else {
        for (key, level) in nested {
            let alias = internal_tree_column(Some(&key), ctx);
            match level {
                NestedLevel::Record(members) => {
                    collect_tree_levels(members, inner_keys.clone(), alias, qualify, ctx, levels)?
                }
                // A tuple induced alone reaches this road with no geometry of
                // its own to state: the level publishes its elements as an
                // object whose keys are empty.
                NestedLevel::Tuple(leaves) => levels.push(TreeLevel {
                    leaves,
                    group_keys: inner_keys.clone(),
                    output: alias,
                    inner: Vec::new(),
                    metadata_key: None,
                    siblings: Vec::new(),
                }),
            }
            inner.push((TreeJsonKey::Literal(key), alias));
        }
    }
    levels.push(TreeLevel {
        leaves,
        group_keys,
        output,
        inner,
        metadata_key: None,
        siblings: Vec::new(),
    });
    Ok(())
}

/// Peel a reduction down to the record it ends in, collecting the metadata
/// keys it passed through on the way.
fn unwrap_tree_item(
    item: ReductionPayload,
) -> Result<(Vec<RecordMember<Refined>>, Vec<crate::names::ColId>)> {
    fn record_of(enclyph: Enclyph<Refined>) -> Result<Vec<RecordMember<Refined>>> {
        match enclyph {
            Enclyph::Record(record) => Ok(record.members.into_vec()),
            Enclyph::EmptyRecord(_) => Ok(Vec::new()),
            Enclyph::Tuple(_) => Err(DelightQLError::ParseError {
                message: "tree group constructor must be a record".to_string(),
                source: None,
                subcategory: None,
            }),
        }
    }
    fn unwrap_group(
        group: ast_refined::MetadataGroup,
        metadata: &mut Vec<crate::names::ColId>,
    ) -> Result<Vec<RecordMember<Refined>>> {
        metadata.push(group.key.column);
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
            return Err(DelightQLError::ParseError {
                message: "tree group reduction is not a constructor".to_string(),
                source: None,
                subcategory: None,
            })
        }
    };
    Ok((members, metadata))
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
) -> Result<CteBody> {
    let (at, outputs) = super::builder::stand_cte_body_at(
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
    let select =
        super::builder::publish_at(at, outputs.iter().copied(), select, input.identities())?;
    Ok(CteBody {
        query: crate::pipeline::sql_ast::QueryExpression::Select(Box::new(select)),
        output_columns: outputs,
    })
}

fn build_tree_level(
    level: &TreeLevel,
    input: &super::builder::CteInput,
    ctx: &TransformCtx,
) -> Result<CteBody> {
    let mut items = Vec::new();
    let mut group_by = Vec::new();
    for key in &level.group_keys {
        let current = current_column(*key, input)?;
        let expression = SqlExpr::Column(current);
        group_by.push(expression.clone());
        items.push(SelectItem::expression_with_alias(expression, *key));
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
                tree_aggregate(row, checks),
                sibling.output,
            ));
        }
    } else if let Some(metadata) = level.metadata_key {
        let key = SqlExpr::Column(current_column(metadata, input)?);
        let inner = level
            .inner
            .first()
            .ok_or_else(|| DelightQLError::ParseError {
                message: "metadata tree group has no constructor".to_string(),
                source: None,
                subcategory: None,
            })?;
        let value = SqlExpr::Column(current_column(inner.1, input)?);
        items.push(SelectItem::expression_with_alias(
            SqlExpr::function(
                "JSON_GROUP_OBJECT",
                vec![key, SqlExpr::function("json", vec![value])],
            ),
            level.output,
        ));
    } else {
        let mut values = Vec::new();
        let mut checks = Vec::new();
        for leaf in &level.leaves {
            if let Some((key, value, tree)) = lower_tree_leaf(leaf, input, ctx)? {
                values.push(json_key_expression(leaf_key(key)));
                values.push(if tree {
                    SqlExpr::function("json", vec![value.clone()])
                } else {
                    value.clone()
                });
                checks.push(value);
            }
        }
        for (key, source) in &level.inner {
            let value = SqlExpr::Column(current_column(*source, input)?);
            values.push(json_key_expression(key.clone()));
            values.push(SqlExpr::function("json", vec![value.clone()]));
            checks.push(value);
        }
        items.push(SelectItem::expression_with_alias(
            tree_aggregate(SqlExpr::function("JSON_OBJECT", values), checks),
            level.output,
        ));
    }
    assemble_tree_cte(input, items, group_by)
}

pub(super) fn r_lower_tree_group_cte(
    builder: Builder<Unprojected>,
    keys: Vec<ast_refined::DomainExpression>,
    reductions: Vec<ReductionPayload>,
    cpr_schema: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    let item = reductions
        .into_iter()
        .next()
        .ok_or_else(|| DelightQLError::ParseError {
            message: "tree group has no reduction".to_string(),
            source: None,
            subcategory: None,
        })?;
    let outputs = resolved_heading(cpr_schema, ctx)?;
    if outputs.len() != keys.len() + 1 {
        return Err(DelightQLError::ParseError {
            message: "tree group output heading does not match its keys".to_string(),
            source: None,
            subcategory: None,
        });
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
    let mut levels = Vec::new();
    collect_tree_levels(
        members,
        initial_keys,
        tree_output,
        &builder,
        ctx,
        &mut levels,
    )?;
    for index in (0..metadata.len()).rev() {
        let inner =
            levels
                .last()
                .map(|level| level.output)
                .ok_or_else(|| DelightQLError::ParseError {
                    message: "metadata tree group has no inner level".to_string(),
                    source: None,
                    subcategory: None,
                })?;
        let output = if index == 0 {
            tree_output
        } else {
            internal_tree_column(None, ctx)
        };
        let mut keys = group_keys.clone();
        keys.extend(metadata[..index].iter().copied());
        levels.push(TreeLevel {
            leaves: Vec::new(),
            group_keys: keys,
            output,
            inner: vec![(TreeJsonKey::Literal(String::new()), inner)],
            metadata_key: Some(metadata[index]),
            siblings: Vec::new(),
        });
    }
    let mut projected = builder.project_all()?;
    for level in &levels {
        projected = projected.push_cte(|input| build_tree_level(level, input, ctx))?;
    }
    let mut items = group_keys
        .iter()
        .zip(key_aliases.iter())
        .map(|(source, output)| {
            current_column(*source, &projected)
                .map(|current| SelectItem::expression_with_alias(SqlExpr::Column(current), *output))
        })
        .collect::<Result<Vec<_>>>()?;
    let aggregate = current_column(
        levels
            .last()
            .expect("tree group always creates a level")
            .output,
        &projected,
    )?;
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
/// Each CTE-needing tree gets its own chain over a frozen copy of the
/// source — a chain reduces at interior granularities, so a sibling
/// reduction cannot ride inside it without counting groups instead of
/// rows. Everything else shares one straight grouped arm. The arms all
/// group by the same keys, join on them NULL-safely, and the final
/// projection publishes the resolver's heading in its order.
pub(super) fn r_lower_tree_group_mixed(
    builder: Builder<Unprojected>,
    keys: Vec<ast_refined::OutItem>,
    reductions: Vec<ast_refined::ReductionItem>,
    plan: ast_refined::ReductionPlan,
    arbitrary: Vec<ast_refined::OutItem>,
    cpr_schema: crate::names::ScopeId,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast::{BinaryOperator, JoinCondition, JoinType};

    let outputs = resolved_heading(cpr_schema, ctx)?;
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
        return Err(DelightQLError::ParseError {
            message: "tree group output heading does not match its reductions".to_string(),
            source: None,
            subcategory: None,
        });
    }
    let key_exprs: Vec<ast_refined::DomainExpression> =
        super::relational::published_values(keys);

    // Freeze the source once; every arm reads its own copy.
    let cols = builder.columns().to_vec();
    let names = builder.names().clone();
    let identities = std::rc::Rc::clone(builder.identities());
    let src = builder.project_all()?.to_sql()?;
    let fresh_source = || -> Result<Builder<Unprojected>> {
        Builder::from_frozen(
            src.clone(),
            super::builder::ScopeName::Fresh(names.fresh(super::builder::wrap_origin(
                &cols,
                &identities,
                crate::names::WrapReason::Projection,
            ))),
            cols.clone(),
            names.clone(),
            std::rc::Rc::clone(&identities),
        )
    };

    // Partition: CTE-needing trees each take an arm; the rest share arm 0.
    // `slot[i]` is where reduction i landed — (arm, offset past keys) — and
    // `None` marks a payload that publishes nothing, so the ledger holds
    // exactly the slots the heading has.
    let mut plain: Vec<(usize, ast_refined::ReductionItem)> = Vec::new();
    let mut trees: Vec<(usize, ast_refined::ReductionItem)> = Vec::new();
    for (index, item) in reductions.into_iter().enumerate() {
        if reduction_item_needs_cte(&item, index, &plan) {
            trees.push((index, item));
        } else {
            plain.push((index, item));
        }
    }
    let arbitrary_base = plain.len() + trees.len();
    let mut slot: Vec<Option<(usize, usize)>> = vec![None; arbitrary_base + arbitrary.len()];

    let mut operands: Vec<super::builder::JoinOperand> = Vec::new();

    // Arm 0: keys + every non-CTE reduction, one straight GROUP BY. With
    // no plain reductions this is the distinct-keys anchor the tree arms
    // join back to.
    {
        let arm = fresh_source()?;
        let keys: Vec<SelectItem> = key_exprs
            .iter()
            .map(|expression| scalar::s_lower_select_item(expression.clone(), &arm, ctx))
            .collect::<Result<_>>()?;
        let mut aggregates: Vec<SelectItem> = Vec::new();
        for (offset, (index, entry)) in plain.iter().enumerate() {
            let Some(expr) = entry.out_item().and_then(ast_refined::OutItem::value) else {
                continue;
            };
            let mut item = s_lower_out_reduction_item(expr.clone(), &arm, ctx)?;
            if let Some(col) = *entry.output() {
                super::relational::alias_unaliased(&mut item, col);
            }
            slot[*index] = Some((0, offset));
            aggregates.push(item);
        }
        for (position, entry) in arbitrary.into_iter().enumerate() {
            let output = entry.output();
            let Some(expr) = super::relational::into_published_value(entry) else {
                continue;
            };
            let Some(col) = output else {
                continue; // resolver stamped None — no output column
            };
            let mut item = match scalar::s_lower_select_item(expr, &arm, ctx)? {
                SelectItem::Expression { expr, alias } => SelectItem::Expression {
                    expr: SqlExpr::intrinsic(crate::names::Intrinsic::Arbitrary, vec![expr]),
                    alias,
                },
                other => other,
            };
            super::relational::alias_unaliased(&mut item, col);
            slot[arbitrary_base + position] = Some((0, aggregates.len()));
            aggregates.push(item);
        }
        let projected = arm.add_group_by(super::builder::GroupBySpec { keys, aggregates })?;
        operands.push(projected.demote()?.into_join_operand()?);
    }

    // One arm per CTE-needing tree. The arm's interior aliases are its
    // own: the composite's final projection is where the resolver's
    // occurrences are published.
    for (index, entry) in trees {
        let payload = match entry {
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
            // neither is collected into the tree entries this walks.
            ast_refined::ReductionItem::Pivot(_) | ast_refined::ReductionItem::Delegate(_) => {
                continue
            }
        };
        let arm = fresh_source()?;
        let tree_output = internal_tree_column(None, ctx);
        let key_aliases = key_exprs
            .iter()
            .map(|expression| source_column(expression))
            .collect::<Result<Vec<_>>>()?;
        let projected =
            lower_tree_cte_chain(arm, &key_exprs, payload, &key_aliases, tree_output, ctx)?;
        slot[index] = Some((operands.len(), 0));
        operands.push(projected.demote()?.into_join_operand()?);
    }

    let key_anchor: Vec<crate::names::ColId> = operands[0].columns[..key_count]
        .iter()
        .map(|column| column.identity())
        .collect();
    let conditions: Vec<(JoinType, JoinCondition)> = operands[1..]
        .iter()
        .map(|operand| {
            let condition = if key_count == 0 {
                SqlExpr::literal(LiteralValue::Boolean(true))
            } else {
                SqlExpr::and(
                    (0..key_count)
                        .map(|i| SqlExpr::Binary {
                            left: Box::new(SqlExpr::Column(key_anchor[i])),
                            op: BinaryOperator::IsNotDistinctFrom,
                            right: Box::new(SqlExpr::Column(operand.columns[i].identity())),
                        })
                        .collect(),
                )
            };
            (JoinType::Inner, JoinCondition::On(condition))
        })
        .collect();

    let mut final_items: Vec<SelectItem> = Vec::new();
    for (i, anchor) in key_anchor.iter().enumerate() {
        final_items.push(SelectItem::expression_with_alias(
            SqlExpr::Column(*anchor),
            outputs[i],
        ));
    }
    // Published slots only, in heading order — a `None` payload contributes
    // no output, so it must not consume one either.
    for ((arm, offset), output) in slot.iter().flatten().zip(&outputs[key_count..]) {
        let column = operands[*arm].columns[key_count + offset].identity();
        final_items.push(SelectItem::expression_with_alias(
            SqlExpr::Column(column),
            *output,
        ));
    }

    Builder::from_joins(operands, conditions)?.add_projection_publishing(
        final_items,
        cpr_schema,
        super::relational::columns_from_cpr_schema(cpr_schema, &ctx.identities),
    )
}

pub(super) fn r_lower_tree_group_in_keys(
    builder: Builder<Unprojected>,
    keys: Vec<ast_refined::DomainExpression>,
    reductions: Vec<ast_refined::DomainExpression>,
    cpr_schema: crate::names::ScopeId,
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
    let tree = tree.ok_or_else(|| DelightQLError::ParseError {
        message: "reducing-by tree group is missing".to_string(),
        source: None,
        subcategory: None,
    })?;
    let ast_refined::DomainExpression::Application(ast_refined::FunctionApplication::Enclyph(
        Enclyph::Record(record),
    )) = tree
    else {
        return Err(DelightQLError::ParseError {
            message: "reducing-by tree group must be a record".to_string(),
            source: None,
            subcategory: None,
        });
    };
    let members = record.members.into_vec();
    let outputs = resolved_heading(cpr_schema, ctx)?;
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
        return Err(DelightQLError::ParseError {
            message: format!(
                "reducing-by tree group publishes {} outputs for {} key(s) and {} \
                 reduction(s)",
                outputs.len(),
                key_count,
                reductions.len()
            ),
            source: None,
            subcategory: None,
        });
    }
    let (leaves, nested) = split_tree_members(members);
    let nested_aliases = nested
        .iter()
        .map(|(key, _)| internal_tree_column(Some(key), ctx))
        .collect::<Vec<_>>();
    let leaf_count = leaves.len();
    let nested_count = nested.len();
    let extra_outputs = outputs[key_count..].to_vec();
    let projected = builder.project_all()?.push_cte(|input| {
        let mut items = Vec::new();
        let mut group_by = Vec::new();
        for source in &plain_columns {
            let current = current_column(*source, input)?;
            let expression = SqlExpr::Column(current);
            group_by.push(expression.clone());
            items.push(SelectItem::expression_with_alias(expression, *source));
        }
        for leaf in &leaves {
            if let Some((_, value, _)) = lower_tree_leaf(leaf, input, ctx)? {
                let alias = leaf_column(leaf).ok_or_else(|| DelightQLError::ParseError {
                    message: "tree group leaf has no structural column".to_string(),
                    source: None,
                    subcategory: None,
                })?;
                group_by.push(value.clone());
                items.push(SelectItem::expression_with_alias(value, alias));
            }
        }
        for ((_, level), alias) in nested.iter().zip(nested_aliases.iter()) {
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
                ),
                *alias,
            ));
        }
        for (expression, output) in reductions.iter().zip(extra_outputs.iter()) {
            let mut item =
                s_lower_reduction_item(ReductionPayload::Value(expression.clone()), input, ctx)?;
            if let SelectItem::Expression { alias, .. } = &mut item {
                *alias = Some(*output);
            }
            items.push(item);
        }
        assemble_tree_cte(input, items, group_by)
    })?;
    let columns = projected.columns();
    let value_start = plain_columns.len();
    let mut json = Vec::new();
    for (index, leaf) in leaves.iter().enumerate() {
        if let Some((key, _, _)) = lower_tree_leaf(leaf, &projected, ctx)? {
            json.push(json_key_expression(leaf_key(key)));
            json.push(SqlExpr::Column(columns[value_start + index].identity()));
        }
    }
    for (index, (key, _)) in nested.iter().enumerate() {
        json.push(SqlExpr::literal(LiteralValue::String(key.clone())));
        json.push(SqlExpr::function(
            "json",
            vec![SqlExpr::Column(
                columns[value_start + leaf_count + index].identity(),
            )],
        ));
    }
    // The keys go out in the order they were written, the tree in its own
    // place among them. A key beside the tree is an ordinary grouping key
    // and an ordinary output; the CTE already grouped by it and carried it,
    // and this is where it stops being dropped.
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
            SqlExpr::Column(columns[plain_index].identity()),
            *output,
        ));
    }
    let extras_start = value_start + leaf_count + nested_count;
    items.extend(
        columns[extras_start..]
            .iter()
            .zip(outputs[key_count..].iter())
            .map(|(source, output)| {
                SelectItem::expression_with_alias(SqlExpr::Column(source.identity()), *output)
            }),
    );
    projected.add_projection(items)
}
