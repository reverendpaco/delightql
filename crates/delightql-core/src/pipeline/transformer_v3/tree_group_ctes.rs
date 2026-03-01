//! CTE generation for nested tree group reductions
//!
//! When tree groups contain nested reductions (`"key": ~> {...}`), we generate
//! CTEs to handle the multiple aggregation levels. Each nesting level becomes
//! a CTE, with grouping keys accumulating as we descend.
//!
//! Pattern:
//! - Innermost CTE: GROUP BY all accumulated keys
//! - Middle CTEs: GROUP BY fewer keys (dropping inner keys)
//! - Outermost: Final aggregation
//!
//! Example:
//! ```delightql
//! users(*) ~> {country, "people": ~> {first_name, last_name}}
//! ```
//! Becomes:
//! ```sql
//! WITH
//!   people_by_country AS (
//!     SELECT country,
//!            JSON_GROUP_ARRAY(JSON_OBJECT('first_name', first_name, 'last_name', last_name)) as people
//!     FROM users
//!     GROUP BY country
//!   )
//! SELECT JSON_GROUP_ARRAY(JSON_OBJECT('country', country, 'people', JSON(people)))
//! FROM people_by_country
//! ```

use super::QualifierScope;
use crate::error::Result;
use crate::pipeline::asts::addressed as ast;
use crate::pipeline::asts::addressed::NamespacePath;
use crate::pipeline::sql_ast_v3::{
    Cte, DomainExpression, QueryExpression, SelectBuilder, SelectItem, TableExpression,
};

use super::context::TransformContext;
use super::expression_transformer::transform_domain_expression;
use super::helpers::alias_generator::next_alias;

/// Result of generating nested reduction CTEs.
///
/// Replaces the loose tuple (Vec<Cte>, String, Vec<String>, Vec<(usize, String)>).
/// Private fields, accessor methods. Consumers cannot destructure to primitives.
pub(in crate::pipeline::transformer_v3) struct NestedCteResult {
    ctes: Vec<Cte>,
    cte_name: String,
    column_aliases: Vec<String>,
    grouping_dress_keys: Vec<(usize, String)>,
}

impl NestedCteResult {
    pub fn take_ctes(&mut self) -> Vec<Cte> {
        std::mem::take(&mut self.ctes)
    }

    pub fn cte_name(&self) -> &str {
        &self.cte_name
    }

    pub fn column_aliases(&self) -> &[String] {
        &self.column_aliases
    }

    pub fn grouping_dress_keys(&self) -> &[(usize, String)] {
        &self.grouping_dress_keys
    }
}

/// Tracks the current data source during nested CTE generation.
///
/// Non-Clone, non-Copy: must be explicitly consumed via `descend_into_cte`
/// when the source changes. The compiler prevents stale qualifier references.
struct SourceBinding {
    from_expr: TableExpression,
    scope: super::QualifierScope,
}

impl SourceBinding {
    fn from_original(source: TableExpression) -> Self {
        let scope = if matches!(
            &source,
            TableExpression::Table { .. } | TableExpression::Subquery { .. }
        ) {
            super::QualifierScope::Unqualified
        } else {
            let base_alias = extract_base_alias(&source);
            super::QualifierScope::JoinSource { base_alias }
        };
        SourceBinding {
            from_expr: source,
            scope,
        }
    }

    /// Consume the old binding and transition to a new CTE source.
    fn descend_into_cte(self, cte_name: String, col_aliases: Vec<String>) -> Self {
        SourceBinding {
            from_expr: TableExpression::table(&cte_name),
            scope: self.scope.descend_into_cte(cte_name, col_aliases),
        }
    }

    /// Read-only accessor for the qualification scope.
    fn scope(&self) -> &super::QualifierScope {
        &self.scope
    }

    /// Build a SQL column expression, deriving qualification from `self.scope`.
    fn qualify_column(
        &self,
        name: &str,
        qualifier: Option<&delightql_types::SqlIdentifier>,
    ) -> DomainExpression {
        self.scope.qualify_column(name, qualifier)
    }

    /// Rewrite accumulated keys to reference CTE columns if we recursed into an inner CTE.
    fn rewrite_accumulated_keys(
        &self,
        base_keys: &[(Option<String>, ast::DomainExpression)],
    ) -> Vec<(Option<String>, ast::DomainExpression)> {
        if let Some((cte_name, col_aliases)) = self.scope.cte_identity() {
            base_keys
                .iter()
                .enumerate()
                .map(|(idx, (key_name, _key_expr))| {
                    if let Some(col_name) = col_aliases.get(idx) {
                        (
                            key_name.clone(),
                            ast::DomainExpression::Lvar {
                                name: col_name.clone().into(),
                                qualifier: Some(cte_name.to_string().into()),
                                namespace_path: NamespacePath::empty(),
                                alias: None,
                                provenance: crate::pipeline::asts::addressed::PhaseBox::phantom(),
                            },
                        )
                    } else {
                        (key_name.clone(), _key_expr.clone())
                    }
                })
                .collect()
        } else {
            base_keys.to_vec()
        }
    }

    /// Deduplicate keys by key name, keeping only the first occurrence.
    fn dedup_keys(
        keys: Vec<(Option<String>, ast::DomainExpression)>,
    ) -> Vec<(Option<String>, ast::DomainExpression)> {
        let mut deduped = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (key_name_opt, key_expr) in keys {
            if let Some(key_name) = &key_name_opt {
                if seen.contains(key_name) {
                    continue;
                }
                seen.insert(key_name.clone());
            }
            deduped.push((key_name_opt, key_expr));
        }
        deduped
    }

    /// Resolve the FROM expression for the final CTE SELECT, updating `from_expr`
    /// and `scope` to reflect the resolved state.
    ///
    /// When we recursed into an inner CTE, `from_expr` is already the CTE table —
    /// no change needed. Otherwise, unwraps subquery sources when name collisions
    /// require access to inner join qualifiers.
    fn resolve_from_expression(&mut self, keys: &[(Option<String>, ast::DomainExpression)]) {
        if self.scope.did_recurse() {
            return;
        }

        if matches!(self.from_expr, TableExpression::Table { .. }) {
            return;
        }

        // Check if there are name collisions requiring unwrapping
        let mut name_counts = std::collections::HashMap::new();
        for (_key_name, key_expr) in keys {
            if let ast::DomainExpression::Lvar { name, .. } = key_expr {
                *name_counts.entry(name.clone()).or_insert(0) += 1;
            }
        }
        let needs_explicit_select = name_counts.values().any(|&count| count > 1);

        if needs_explicit_select {
            if let TableExpression::Subquery { query, .. } = &self.from_expr {
                if let QueryExpression::Select(select_box) = &***query {
                    if let Some(from_tables) = select_box.from() {
                        let inner = from_tables[0].clone();
                        let base_alias = extract_base_alias(&inner);
                        self.from_expr = inner;
                        // Explicit scope transition
                        let old =
                            std::mem::replace(&mut self.scope, super::QualifierScope::Unqualified);
                        self.scope = old.unwrap_to_join(base_alias);
                    }
                }
            }
        }
    }

    fn from_expression(&self) -> &TableExpression {
        &self.from_expr
    }

    /// Whether we recursed into an inner CTE (source is no longer the original).
    fn did_recurse(&self) -> bool {
        self.scope.did_recurse()
    }

    /// Whether column qualifiers should be dropped for the current source.
    fn should_drop_qualifiers(&self) -> bool {
        self.scope.should_drop_qualifiers()
    }
}

/// Extract a base alias from a TableExpression for join qualification.
fn extract_base_alias(table: &TableExpression) -> String {
    match table {
        TableExpression::Table { alias: Some(a), .. } => a.clone(),
        TableExpression::Table { name, .. } => name.clone(),
        TableExpression::Subquery { alias, .. } => alias.clone(),
        TableExpression::Join { left, .. } => extract_base_alias(left),
        _ => "base".to_string(),
    }
}

/// Check if a tree group contains any nested reductions
pub fn has_nested_reductions(expr: &ast::DomainExpression) -> bool {
    match expr {
        ast::DomainExpression::Function(ast::FunctionExpression::Curly { members, .. }) => {
            members.iter().any(|m| {
                matches!(
                    m,
                    ast::CurlyMember::KeyValue {
                        nested_reduction: true,
                        ..
                    }
                )
            })
        }
        // Metadata tree groups ARE always nested reductions (they need two-level CTE handling)
        ast::DomainExpression::Function(ast::FunctionExpression::MetadataTreeGroup { .. }) => true,
        // Non-tree-group function expressions.
        ast::DomainExpression::Function(ast::FunctionExpression::Regular { .. })
        | ast::DomainExpression::Function(ast::FunctionExpression::Curried { .. })
        | ast::DomainExpression::Function(ast::FunctionExpression::HigherOrder { .. })
        | ast::DomainExpression::Function(ast::FunctionExpression::Bracket { .. })
        | ast::DomainExpression::Function(ast::FunctionExpression::Infix { .. })
        | ast::DomainExpression::Function(ast::FunctionExpression::Lambda { .. })
        | ast::DomainExpression::Function(ast::FunctionExpression::StringTemplate { .. })
        | ast::DomainExpression::Function(ast::FunctionExpression::CaseExpression { .. })
        | ast::DomainExpression::Function(ast::FunctionExpression::Window { .. })
        | ast::DomainExpression::Function(ast::FunctionExpression::Array { .. })
        | ast::DomainExpression::Function(ast::FunctionExpression::JsonPath { .. }) => false,
        // Non-function domain expressions: not tree groups
        ast::DomainExpression::Lvar { .. }
        | ast::DomainExpression::Literal { .. }
        | ast::DomainExpression::Projection(_)
        | ast::DomainExpression::NonUnifiyingUnderscore
        | ast::DomainExpression::ValuePlaceholder { .. }
        | ast::DomainExpression::Predicate { .. }
        | ast::DomainExpression::PipedExpression { .. }
        | ast::DomainExpression::Parenthesized { .. }
        | ast::DomainExpression::Tuple { .. }
        | ast::DomainExpression::ScalarSubquery { .. }
        | ast::DomainExpression::PivotOf { .. } => false,
        // Pipeline violations: should not survive to Addressed phase
        ast::DomainExpression::Substitution(_) | ast::DomainExpression::ColumnOrdinal(_) => {
            unreachable!("Substitution/ColumnOrdinal should not survive to Addressed phase")
        }
    }
}

/// Extract nested reduction members from a tree group
/// Returns (nested_members, non_nested_members)
pub fn extract_nested_members(
    members: Vec<ast::CurlyMember>,
) -> (
    Vec<(String, Box<ast::DomainExpression>)>,
    Vec<ast::CurlyMember>,
) {
    let mut nested = Vec::new();
    let mut non_nested = Vec::new();

    for member in members {
        match member {
            ast::CurlyMember::KeyValue {
                key,
                nested_reduction: true,
                value,
            } => {
                nested.push((key, value));
            }
            other => {
                non_nested.push(other);
            }
        }
    }

    (nested, non_nested)
}

/// Extract non-nested members (grouping columns) from a tree group
/// Used to get the grouping keys when a tree group appears in reducing_by
pub fn extract_grouping_members(tree_group: &ast::DomainExpression) -> Vec<ast::DomainExpression> {
    match tree_group {
        ast::DomainExpression::Function(ast::FunctionExpression::Curly { members, .. }) => members
            .iter()
            .filter_map(|m| match m {
                ast::CurlyMember::Shorthand {
                    column,
                    qualifier,
                    schema,
                } => Some(ast::DomainExpression::Lvar {
                    name: column.clone(),
                    qualifier: qualifier.clone(),
                    namespace_path: schema
                        .as_ref()
                        .map(|s| NamespacePath::single(s.as_str()))
                        .unwrap_or_else(|| NamespacePath::empty()),
                    alias: None,
                    provenance: crate::pipeline::asts::addressed::PhaseBox::phantom(),
                }),
                ast::CurlyMember::KeyValue {
                    nested_reduction: false,
                    value,
                    ..
                } => Some(*value.clone()),
                // KeyValue with nested_reduction=true: skip (nested reduction, not grouping member)
                ast::CurlyMember::KeyValue {
                    nested_reduction: true,
                    ..
                } => None,
                // Other CurlyMember variants: skip (not grouping columns)
                ast::CurlyMember::Comparison { .. }
                | ast::CurlyMember::Glob { .. }
                | ast::CurlyMember::Pattern { .. }
                | ast::CurlyMember::OrdinalRange { .. }
                | ast::CurlyMember::Placeholder { .. }
                | ast::CurlyMember::PathLiteral { .. } => None,
            })
            .collect(),
        // Non-Curly expressions: no grouping members to extract
        _ => vec![],
    }
}

/// Extract GROUPING KERNEL (leaf fields) from GROUPING DRESS structures
///
/// GROUPING DRESS: The nested JSON structure used as grouping key (e.g., `{"key": {country, age}}`)
/// GROUPING KERNEL: The actual leaf field values (e.g., `[country, age]`)
///
/// This function recursively extracts leaf fields for efficient GROUP BY.
/// Instead of `GROUP BY JSON_OBJECT('key', JSON_OBJECT('country', country, 'age', age))`,
/// we can do `GROUP BY country, age` which is semantically identical but more efficient.
///
/// Examples:
/// - `{country}` → `[country]`
/// - `{"key": {country, age}}` → `[country, age]`
/// - `{country, "nested": {city}}` → `[country, city]`
pub fn extract_grouping_kernel(expr: &ast::DomainExpression) -> Vec<ast::DomainExpression> {
    match expr {
        // Leaf field - this is part of the kernel
        ast::DomainExpression::Lvar { .. } => {
            vec![expr.clone()]
        }

        // Nested object - recurse to find leaves
        ast::DomainExpression::Function(ast::FunctionExpression::Curly { members, .. }) => {
            let mut kernel = Vec::new();

            for member in members {
                match member {
                    // Shorthand member: {country} → extract 'country'
                    ast::CurlyMember::Shorthand {
                        column,
                        qualifier,
                        schema,
                    } => {
                        kernel.push(ast::DomainExpression::Lvar {
                            name: column.clone(),
                            qualifier: qualifier.clone(),
                            namespace_path: schema
                                .as_ref()
                                .map(|s| NamespacePath::single(s.as_str()))
                                .unwrap_or_else(|| NamespacePath::empty()),
                            alias: None,
                            provenance: crate::pipeline::asts::addressed::PhaseBox::phantom(),
                        });
                    }

                    // KeyValue with scalar (non-reduction) - recurse on value
                    ast::CurlyMember::KeyValue {
                        nested_reduction: false,
                        value,
                        ..
                    } => {
                        // Recursively extract kernel from the value
                        kernel.extend(extract_grouping_kernel(value));
                    }

                    // Skip aggregate reductions - they don't contribute to grouping
                    other => panic!("catch-all hit in tree_group_ctes.rs extract_grouping_kernel (CurlyMember): {:?}", other),
                }
            }

            kernel
        }

        // Other expressions - use as-is (shouldn't happen in GROUPING DRESS, but be defensive)
        _ => vec![expr.clone()],
    }
}

/// Prepare accumulated keys by adding GROUPING DRESS from non_nested_members.
fn prepare_accumulated_keys(
    accumulated_grouping_keys: &[(Option<String>, ast::DomainExpression)],
    non_nested_members: &[ast::CurlyMember],
) -> Vec<(Option<String>, ast::DomainExpression)> {
    let mut base_keys = accumulated_grouping_keys.to_vec();
    for member in non_nested_members {
        if let ast::CurlyMember::KeyValue {
            key,
            nested_reduction: false,
            value,
        } = member
        {
            if matches!(
                value.as_ref(),
                ast::DomainExpression::Function(ast::FunctionExpression::Curly { .. })
            ) {
                base_keys.push((Some(key.clone()), *value.clone()));
            }
        }
    }
    base_keys
}

/// Build aggregate items from modified nested members.
fn build_aggregate_items(
    modified_members: Vec<(String, Box<ast::DomainExpression>)>,
    ctx: &TransformContext,
) -> Result<(Vec<SelectItem>, Vec<String>)> {
    let agg_ctx = ctx.set_aggregate(true);
    let mut items = Vec::new();
    let mut aliases = Vec::new();
    for (key_name, tree_group_expr) in modified_members {
        // unknown() OK: tree-group aggregate expressions reference CTE columns, not user provenance
        let transformed = transform_domain_expression(
            *tree_group_expr,
            &agg_ctx,
            &mut crate::pipeline::transformer_v3::SchemaContext::unknown(),
        )?;
        items.push(SelectItem::expression_with_alias(
            transformed,
            key_name.clone(),
        ));
        aliases.push(key_name);
    }
    Ok((items, aliases))
}

/// Process nested members, recursing into inner tree groups and transitioning the source
/// binding via `descend_into_cte` on each recursion.
fn process_nested_members(
    binding: SourceBinding,
    nested_members: Vec<(String, Box<ast::DomainExpression>)>,
    base_accumulated_keys: &[(Option<String>, ast::DomainExpression)],
    where_clause: Option<&DomainExpression>,
    ctx: &TransformContext,
) -> Result<(
    SourceBinding,
    Vec<Cte>,
    Vec<(String, Box<ast::DomainExpression>)>,
)> {
    let mut binding = binding;
    let mut all_ctes = Vec::new();
    let mut modified_nested_members = Vec::new();

    for (key_name, tree_group_expr) in nested_members {
        if has_nested_reductions(&tree_group_expr) {
            if let ast::DomainExpression::Function(ast::FunctionExpression::MetadataTreeGroup {
                key_column,
                key_qualifier,
                key_schema,
                constructor,
                ..
            }) = tree_group_expr.as_ref()
            {
                let key_expr = ast::DomainExpression::Lvar {
                    name: key_column.clone(),
                    qualifier: key_qualifier.clone(),
                    namespace_path: key_schema
                        .as_ref()
                        .map(|s| NamespacePath::single(s.as_str()))
                        .unwrap_or_else(|| NamespacePath::empty()),
                    alias: None,
                    provenance: crate::pipeline::asts::addressed::PhaseBox::phantom(),
                };

                let mut new_accumulated_keys = base_accumulated_keys.to_vec();
                new_accumulated_keys.push((None, key_expr));

                let nested_members_inner = vec![(
                    "constructor".to_string(),
                    Box::new(ast::DomainExpression::Function(*constructor.clone())),
                )];

                let inner_cte_name_preassigned = extract_cte_name_from_function(constructor);

                let mut inner_result = generate_nested_reduction_cte(
                    &new_accumulated_keys,
                    nested_members_inner,
                    Vec::new(),
                    binding.from_expression().clone(),
                    where_clause,
                    ctx,
                    inner_cte_name_preassigned,
                )?;

                all_ctes.extend(inner_result.take_ctes());
                let inner_cte_name = inner_result.cte_name().to_string();
                let inner_col_aliases = inner_result.column_aliases().to_vec();

                if let Some(cte_col) = inner_col_aliases.last() {
                    let wrapped_expr = ast::FunctionExpression::Regular {
                        name: "JSON".into(),
                        namespace: None,
                        arguments: vec![ast::DomainExpression::Lvar {
                            name: cte_col.clone().into(),
                            qualifier: None,
                            namespace_path: NamespacePath::empty(),
                            alias: None,
                            provenance: crate::pipeline::asts::addressed::PhaseBox::phantom(),
                        }],
                        alias: None,
                        conditioned_on: None,
                    };

                    let final_mtg = ast::DomainExpression::Function(
                        ast::FunctionExpression::MetadataTreeGroup {
                            key_column: key_column.clone(),
                            key_qualifier: key_qualifier.clone(),
                            key_schema: key_schema.clone(),
                            constructor: Box::new(wrapped_expr),
                            keys_only: false,
                            cte_requirements: None,
                            alias: None,
                        },
                    );

                    modified_nested_members.push((key_name, Box::new(final_mtg)));
                }

                binding = binding.descend_into_cte(inner_cte_name, inner_col_aliases);
            } else if let ast::DomainExpression::Function(ast::FunctionExpression::Curly {
                members,
                ..
            }) = tree_group_expr.as_ref()
            {
                let mut inner_grouping_cols_with_names: Vec<(
                    Option<String>,
                    ast::DomainExpression,
                )> = Vec::new();

                for member in members.iter() {
                    match member {
                        ast::CurlyMember::Shorthand {
                            column,
                            qualifier,
                            schema,
                        } => {
                            inner_grouping_cols_with_names.push((
                                None,
                                ast::DomainExpression::Lvar {
                                    name: column.clone(),
                                    qualifier: qualifier.clone(),
                                    namespace_path: schema
                                        .as_ref()
                                        .map(|s| NamespacePath::single(s.as_str()))
                                        .unwrap_or_else(|| NamespacePath::empty()),
                                    alias: None,
                                    provenance: crate::pipeline::asts::addressed::PhaseBox::phantom(
                                    ),
                                },
                            ));
                        }
                        ast::CurlyMember::KeyValue {
                            key,
                            nested_reduction: false,
                            value,
                        } => {
                            if matches!(
                                value.as_ref(),
                                ast::DomainExpression::Function(
                                    ast::FunctionExpression::Curly { .. }
                                )
                            ) {
                                inner_grouping_cols_with_names
                                    .push((Some(key.clone()), *value.clone()));
                            } else {
                                inner_grouping_cols_with_names.push((None, *value.clone()));
                            }
                        }
                        ast::CurlyMember::KeyValue {
                            nested_reduction: true,
                            ..
                        } => {}
                        ast::CurlyMember::Comparison { .. }
                        | ast::CurlyMember::Glob { .. }
                        | ast::CurlyMember::Pattern { .. }
                        | ast::CurlyMember::OrdinalRange { .. }
                        | ast::CurlyMember::Placeholder { .. }
                        | ast::CurlyMember::PathLiteral { .. } => {}
                    }
                }

                let mut new_accumulated_keys = base_accumulated_keys.to_vec();
                new_accumulated_keys.extend(inner_grouping_cols_with_names);

                let (inner_nested, inner_non_nested) =
                    if let ast::DomainExpression::Function(ast::FunctionExpression::Curly {
                        members,
                        ..
                    }) = tree_group_expr.as_ref()
                    {
                        extract_nested_members(members.clone())
                    } else {
                        (Vec::new(), Vec::new())
                    };

                let inner_cte_name_preassigned = extract_cte_name_from_domain(&tree_group_expr);

                let mut inner_result = generate_nested_reduction_cte(
                    &new_accumulated_keys,
                    inner_nested.clone(),
                    inner_non_nested.clone(),
                    binding.from_expression().clone(),
                    where_clause,
                    ctx,
                    inner_cte_name_preassigned,
                )?;

                all_ctes.extend(inner_result.take_ctes());
                let inner_cte_name = inner_result.cte_name().to_string();
                let inner_col_aliases = inner_result.column_aliases().to_vec();
                let inner_grouping_dress = inner_result.grouping_dress_keys().to_vec();

                // Build modified tree group that references CTE columns with JSON()
                let mut new_inner_members = Vec::new();

                let mut inner_level_dress_keys = std::collections::HashSet::new();
                for member in &inner_non_nested {
                    if let ast::CurlyMember::KeyValue {
                        key,
                        nested_reduction: false,
                        value,
                    } = member
                    {
                        if matches!(
                            value.as_ref(),
                            ast::DomainExpression::Function(ast::FunctionExpression::Curly { .. })
                        ) {
                            inner_level_dress_keys.insert(key.clone());
                        }
                    }
                }

                for (col_idx, key_name) in &inner_grouping_dress {
                    if inner_level_dress_keys.contains(key_name) {
                        if let Some(cte_col_name) = inner_col_aliases.get(*col_idx) {
                            new_inner_members.push(ast::CurlyMember::KeyValue {
                                key: key_name.clone(),
                                nested_reduction: false,
                                value: Box::new(ast::DomainExpression::Function(
                                    ast::FunctionExpression::Regular {
                                        name: "JSON".into(),
                                        namespace: None,
                                        arguments: vec![ast::DomainExpression::Lvar {
                                            name: cte_col_name.clone().into(),
                                            qualifier: Some(inner_cte_name.clone().into()),
                                            namespace_path: NamespacePath::empty(),
                                            alias: None,
                                            provenance:
                                                crate::pipeline::asts::addressed::PhaseBox::phantom(
                                                ),
                                        }],
                                        alias: None,
                                        conditioned_on: None,
                                    },
                                )),
                            });
                        }
                    }
                }

                for member in &inner_non_nested {
                    match member {
                        ast::CurlyMember::Shorthand { column, .. } => {
                            new_inner_members.push(ast::CurlyMember::Shorthand {
                                column: column.clone(),
                                qualifier: Some(inner_cte_name.clone().into()),
                                schema: None,
                            });
                        }
                        ast::CurlyMember::KeyValue {
                            nested_reduction: false,
                            value,
                            ..
                        } if !matches!(
                            value.as_ref(),
                            ast::DomainExpression::Function(ast::FunctionExpression::Curly { .. })
                        ) =>
                        {
                            new_inner_members.push(member.clone());
                        }
                        _ => {}
                    }
                }

                let grouping_key_count = inner_col_aliases.len() - inner_nested.len();
                for (idx, (inner_key, _)) in inner_nested.iter().enumerate() {
                    let cte_col_idx = grouping_key_count + idx;
                    if let Some(cte_col_name) = inner_col_aliases.get(cte_col_idx) {
                        new_inner_members.push(ast::CurlyMember::KeyValue {
                            key: inner_key.clone(),
                            nested_reduction: false,
                            value: Box::new(ast::DomainExpression::Function(
                                ast::FunctionExpression::Regular {
                                    name: "JSON".into(),
                                    namespace: None,
                                    arguments: vec![ast::DomainExpression::Lvar {
                                        name: cte_col_name.clone().into(),
                                        qualifier: None,
                                        namespace_path: NamespacePath::empty(),
                                        alias: None,
                                        provenance: crate::pipeline::asts::addressed::PhaseBox::new(
                                            None,
                                        ),
                                    }],
                                    alias: None,
                                    conditioned_on: None,
                                },
                            )),
                        });
                    }
                }

                let modified_tree_group = Box::new(ast::DomainExpression::Function(
                    ast::FunctionExpression::Curly {
                        members: new_inner_members,
                        inner_grouping_keys: vec![],
                        cte_requirements: None,
                        alias: None,
                    },
                ));

                modified_nested_members.push((key_name, modified_tree_group));

                binding = binding.descend_into_cte(inner_cte_name, inner_col_aliases);
            }
        } else {
            modified_nested_members.push((key_name, tree_group_expr));
        }
    }

    Ok((binding, all_ctes, modified_nested_members))
}

/// Build SELECT items, column aliases, GROUP BY expressions, and grouping dress keys
/// from accumulated keys.
///
/// The `binding` determines how columns are qualified: CTE/subquery sources use
/// unqualified names; original table sources use qualified names.
fn build_select_items(
    binding: &SourceBinding,
    keys: &[(Option<String>, ast::DomainExpression)],
    ctx: &TransformContext,
) -> Result<(
    Vec<SelectItem>,
    Vec<String>,
    Vec<DomainExpression>,
    Vec<(usize, String)>,
)> {
    let mut select_items = Vec::new();
    let mut column_aliases = Vec::new();
    let mut grouping_dress_keys = Vec::new();
    let mut used_names = std::collections::HashSet::new();
    let mut group_by_source_exprs = Vec::new();

    log::debug!("build_select_items: keys.len()={}", keys.len());
    for (idx, (key_name_opt, key_expr)) in keys.iter().enumerate() {
        log::debug!(
            "  [{}]: key_name={:?}, key_expr={:?}",
            idx,
            key_name_opt,
            key_expr
        );
    }

    for (key_name_opt, key_expr) in keys {
        if let Some(key_name) = key_name_opt {
            if let ast::DomainExpression::Lvar {
                name, qualifier, ..
            } = key_expr
            {
                log::debug!(
                    "GROUPING DRESS is Lvar (rewritten): name={}, qualifier={:?}",
                    name,
                    qualifier
                );
                let json_expr = binding.qualify_column(name, qualifier.as_ref());

                let json_alias = next_alias();
                select_items.push(SelectItem::expression_with_alias(
                    json_expr.clone(),
                    &json_alias,
                ));
                column_aliases.push(json_alias.clone());

                let col_idx = column_aliases.len() - 1;
                grouping_dress_keys.push((col_idx, key_name.clone()));

                group_by_source_exprs.push(json_expr);
            } else {
                log::debug!(
                    "GROUPING DRESS is NOT Lvar (transforming): key_expr={:?}",
                    key_expr
                );
                // unknown() OK: tree-group key expressions are constructed JSON_OBJECT calls, not user columns
                let json_object_expr = transform_domain_expression(
                    key_expr.clone(),
                    ctx,
                    &mut crate::pipeline::transformer_v3::SchemaContext::unknown(),
                )?;

                let json_alias = next_alias();
                select_items.push(SelectItem::expression_with_alias(
                    json_object_expr.clone(),
                    &json_alias,
                ));
                column_aliases.push(json_alias.clone());

                let col_idx = column_aliases.len() - 1;
                grouping_dress_keys.push((col_idx, key_name.clone()));

                let kernel_fields = extract_grouping_kernel(key_expr);
                for kernel_field in kernel_fields {
                    if let ast::DomainExpression::Lvar {
                        name, qualifier, ..
                    } = kernel_field
                    {
                        let raw_expr = binding.qualify_column(&name, qualifier.as_ref());
                        group_by_source_exprs.push(raw_expr);
                    }
                }
            }
        } else {
            // Simple scalar field (no GROUPING DRESS)
            let (source_expr, base_name) = match key_expr {
                ast::DomainExpression::Lvar {
                    name, qualifier, ..
                } => {
                    let col_name = name.to_string();

                    let (source_col_name, source_qual_opt, final_alias) =
                        if binding.should_drop_qualifiers() {
                            if used_names.contains(&col_name) {
                                if let Some(qual) = qualifier {
                                    let aliased_name = format!("{}_{}", qual, name);
                                    (aliased_name.clone(), None, aliased_name)
                                } else {
                                    let mut counter = 1;
                                    while used_names.contains(&format!("{}_{}", name, counter)) {
                                        counter += 1;
                                    }
                                    let aliased = format!("{}_{}", name, counter);
                                    (aliased.clone(), None, aliased)
                                }
                            } else {
                                (col_name.clone(), None, col_name.clone())
                            }
                        } else {
                            let mut source_qual = qualifier.as_deref().map(|s| s.to_string());
                            let mut alias = col_name.clone();

                            if used_names.contains(&col_name) {
                                if let Some(qual) = qualifier {
                                    alias = format!("{}_{}", qual, name);
                                    source_qual = Some(qual.to_string());
                                } else {
                                    let mut counter = 1;
                                    while used_names.contains(&format!("{}_{}", name, counter)) {
                                        counter += 1;
                                    }
                                    alias = format!("{}_{}", name, counter);
                                }
                            }
                            (name.to_string(), source_qual, alias)
                        };

                    let source = if let Some(qual) = source_qual_opt {
                        DomainExpression::Column {
                            name: source_col_name,
                            qualifier: Some(QualifierScope::structural(qual)),
                        }
                    } else {
                        DomainExpression::Column {
                            name: source_col_name,
                            qualifier: None,
                        }
                    };

                    (source, final_alias)
                }
                _ => {
                    let alias = next_alias();
                    // unknown() OK: constructed key expression, not a user column
                    let transformed = transform_domain_expression(
                        key_expr.clone(),
                        ctx,
                        &mut crate::pipeline::transformer_v3::SchemaContext::unknown(),
                    )?;
                    (transformed, alias)
                }
            };

            used_names.insert(base_name.clone());
            select_items.push(SelectItem::expression_with_alias(
                source_expr.clone(),
                base_name.clone(),
            ));
            column_aliases.push(base_name);
            group_by_source_exprs.push(source_expr);
        }
    }

    Ok((
        select_items,
        column_aliases,
        group_by_source_exprs,
        grouping_dress_keys,
    ))
}

/// Assemble the final CTE SELECT statement.
fn assemble_cte_query(
    from: TableExpression,
    select_items: Vec<SelectItem>,
    group_by_exprs: Vec<DomainExpression>,
    where_clause: Option<&DomainExpression>,
    apply_where: bool,
) -> Result<crate::pipeline::sql_ast_v3::SelectStatement> {
    let should_apply_where = apply_where && where_clause.is_some();
    let mut builder = SelectBuilder::new()
        .from_tables(vec![from])
        .set_select(select_items);

    if !group_by_exprs.is_empty() {
        builder = builder.group_by(group_by_exprs);
    }

    if should_apply_where {
        builder = builder.where_clause(where_clause.unwrap().clone());
    }

    builder
        .build()
        .map_err(|e| crate::error::DelightQLError::ParseError {
            message: format!("Failed to build CTE: {}", e),
            source: None,
            subcategory: None,
        })
}

/// Generate CTEs for nested tree group reductions (RECURSIVE)
///
/// Takes:
/// - accumulated_grouping_keys: (key_name, expression) pairs for ALL outer levels
///   - key_name is Some("level1") for GROUPING DRESS members
///   - key_name is None for simple scalar fields
/// - nested_members: list of (key_name, tree_group_expr) for nested reductions
/// - non_nested_members: GROUPING DRESS - scalar nested objects used as grouping keys
/// - source: the FROM clause (table or subquery)
/// - ctx: transform context
///
/// Returns: (all_ctes, final_cte_name, final_column_aliases, grouping_dress_keys)
///
/// grouping_dress_keys: Maps column index to original key name for GROUPING DRESS members
///
/// This function is RECURSIVE - if nested_members contain tree groups with
/// their own nested reductions, it will recursively generate CTEs for those
/// inner levels first, then generate a CTE for this level.
pub fn generate_nested_reduction_cte(
    accumulated_grouping_keys: &[(Option<String>, ast::DomainExpression)],
    nested_members: Vec<(String, Box<ast::DomainExpression>)>,
    non_nested_members: Vec<ast::CurlyMember>,
    source: TableExpression,
    where_clause: Option<&DomainExpression>,
    ctx: &TransformContext,
    cte_name: Option<String>,
) -> Result<NestedCteResult> {
    // Phase 1: prepare accumulated keys (add GROUPING DRESS from non_nested_members)
    let base_keys = prepare_accumulated_keys(accumulated_grouping_keys, &non_nested_members);

    // Phase 2: recurse into nested members (may transition source via descend_into_cte)
    let binding = SourceBinding::from_original(source);
    let (mut binding, mut all_ctes, modified_nested) =
        process_nested_members(binding, nested_members, &base_keys, where_clause, ctx)?;

    // Phase 3: rewrite keys for current source + dedup
    let keys = SourceBinding::dedup_keys(binding.rewrite_accumulated_keys(&base_keys));

    // Phase 4a: resolve FROM expression (may unwrap subquery, updating from_expr
    // so that should_drop_qualifiers() reflects the resolved state)
    binding.resolve_from_expression(&keys);

    // Phase 4b: build SELECT + GROUP BY
    let transform_ctx = ctx.with_qualifier_scope(binding.scope().clone());
    let (mut select_items, mut column_aliases, group_by_source_exprs, grouping_dress_keys) =
        build_select_items(&binding, &keys, &transform_ctx)?;

    // Phase 5: aggregate items
    let (agg_items, agg_aliases) = build_aggregate_items(modified_nested, &transform_ctx)?;
    select_items.extend(agg_items);
    column_aliases.extend(agg_aliases);

    // Phase 6: assemble CTE
    let this_cte_name = cte_name.unwrap_or_else(|| next_alias());
    let cte_query = assemble_cte_query(
        binding.from_expression().clone(),
        select_items,
        group_by_source_exprs,
        where_clause,
        !binding.did_recurse(),
    )?;

    let this_cte = Cte::new(
        this_cte_name.clone(),
        QueryExpression::Select(Box::new(cte_query)),
    );

    // Add this level's CTE to the list
    all_ctes.push(this_cte);

    // Return all CTEs (from innermost to outermost), this CTE's name, and column aliases
    log::debug!(
        "generate_nested_reduction_cte: returning cte_name={}, column_aliases={:?}, grouping_dress_keys={:?}",
        this_cte_name,
        column_aliases,
        grouping_dress_keys
    );
    Ok(NestedCteResult {
        ctes: all_ctes,
        cte_name: this_cte_name,
        column_aliases,
        grouping_dress_keys,
    })
}

/// Extract pre-assigned CTE name from a DomainExpression's cte_requirements
fn extract_cte_name_from_domain(expr: &ast::DomainExpression) -> Option<String> {
    match expr {
        ast::DomainExpression::Function(func) => extract_cte_name_from_function(func),
        ast::DomainExpression::Lvar { .. }
        | ast::DomainExpression::Literal { .. }
        | ast::DomainExpression::Predicate { .. }
        | ast::DomainExpression::PipedExpression { .. }
        | ast::DomainExpression::Parenthesized { .. }
        | ast::DomainExpression::Tuple { .. }
        | ast::DomainExpression::ScalarSubquery { .. }
        | ast::DomainExpression::PivotOf { .. }
        | ast::DomainExpression::Projection(_)
        | ast::DomainExpression::ValuePlaceholder { .. }
        | ast::DomainExpression::NonUnifiyingUnderscore => None,
        ast::DomainExpression::Substitution(_) | ast::DomainExpression::ColumnOrdinal(_) => {
            unreachable!("Substitution/ColumnOrdinal should not survive to Addressed phase")
        }
    }
}

/// Extract pre-assigned CTE name from a FunctionExpression's cte_requirements
fn extract_cte_name_from_function(func: &ast::FunctionExpression) -> Option<String> {
    match func {
        ast::FunctionExpression::Curly {
            cte_requirements: Some(req),
            ..
        } => req.cte_name.get().clone(),
        ast::FunctionExpression::MetadataTreeGroup {
            cte_requirements: Some(req),
            ..
        } => req.cte_name.get().clone(),
        // Non-tree-group functions or those without cte_requirements
        _ => None,
    }
}
