//! Table access, joins, anonymous tables parsing

use super::expressions::*;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::asts::unresolved::{GroundedPath, NamespacePath};
use crate::pipeline::cst::CstNode;
use crate::pipeline::query_features::FeatureCollector;
use delightql_types::SqlIdentifier;

pub(super) fn parse_column_spec(
    node: CstNode,
    features: &mut crate::pipeline::query_features::FeatureCollector,
) -> Result<DomainSpec> {
    if node.has_child("glob_spec") {
        return Ok(DomainSpec::Glob);
    }

    // Note: glob_with_using (*{cols}) removed from grammar - USING is now .(cols) operator
    // The .(cols) syntax is handled via try_collapse_to_domain_spec -> DomainSpec::GlobWithUsing

    let mut columns = Vec::new();

    if let Some(list_node) = node.find_child("column_list") {
        for item in list_node.children() {
            if item.kind() == "column_spec_item" {
                // Extract alias if present
                let alias = item.field_text("alias");

                // Parse the expression
                // IMPORTANT: Check for complex expressions first before identifier, since
                // identifier will match ANY identifier (including those inside complex expressions)
                let mut expr = if item.has_child("placeholder") {
                    DomainExpression::placeholder_builder().build()
                } else if let Some(scalar) = item.find_child("scalar_subquery") {
                    super::expressions::parse_scalar_subquery(scalar, features)?
                } else if let Some(func) = item.find_child("function_call") {
                    parse_function_call(func)?
                } else if let Some(lit) = item.find_child("literal") {
                    parse_literal(lit)?
                } else if let Some(paren) = item.find_child("parenthesized_expression") {
                    super::expressions::parse_expression(paren, &mut FeatureCollector::new())?
                } else if let Some(id) = item.find_child("identifier") {
                    // Simple identifier - must be last since identifiers appear in other constructs
                    DomainExpression::lvar_builder(crate::pipeline::cst::unstrop(id.text())).build()
                } else {
                    continue;
                };

                // Reject aliases on non-lvar expressions in positional binding.
                // Non-lvar expressions (literals, functions) become WHERE filters —
                // aliases are meaningless in that context.
                if let Some(ref alias_str) = alias {
                    let is_lvar = matches!(expr, DomainExpression::Lvar { .. });
                    if !is_lvar {
                        return Err(DelightQLError::validation_error(
                            format!(
                                "Alias '{}' is not allowed in positional binding — \
                                 non-column expressions become WHERE filters, not named columns",
                                alias_str
                            ),
                            "positional_alias_validation".to_string(),
                        ));
                    }
                }

                // Apply alias using existing helper function
                super::helpers::apply_alias_to_expression(&mut expr, alias);

                columns.push(expr);
            }
        }
    }

    if columns.is_empty() {
        Ok(DomainSpec::Glob)
    } else {
        Ok(DomainSpec::Positional(columns))
    }
}

/// Parse catalog functor: ns::(*) or `ns::`(*)
///
/// Produces a Relation::Ground pointing to sys::meta with the catalog name as entity name.
/// The resolver looks up the wrapper view in sys::meta, which expands via the generator HO view.
pub(super) fn parse_catalog_functor(
    node: CstNode,
    features: &mut crate::pipeline::query_features::FeatureCollector,
) -> Result<RelationalExpression> {
    // Extract catalog name from either bare or stropped form
    let catalog_name_node = node
        .field("catalog_name")
        .ok_or_else(|| DelightQLError::parse_error("No catalog_name in catalog_functor"))?;

    let catalog_name = if catalog_name_node.kind() == "stropped_identifier" {
        // Stropped: `main::` or `sys::entities::` — strip backticks
        let raw = catalog_name_node.text();
        let trimmed = raw.trim_matches('`');
        if !trimmed.ends_with("::") {
            return Err(DelightQLError::parse_error(format!(
                "Stropped catalog functor must end with '::', got: {}",
                trimmed
            )));
        }
        trimmed.to_string()
    } else if catalog_name_node.kind() == "namespace_path" {
        // Multi-segment bare: sys::entities → append "::"
        let mut parts = Vec::new();
        for child in catalog_name_node.children() {
            if child.kind() == "identifier" {
                parts.push(crate::pipeline::cst::unstrop(child.text()));
            }
        }
        format!("{}::", parts.join("::"))
    } else if catalog_name_node.kind() == "identifier" {
        // Single-segment bare: main → append "::"
        format!(
            "{}::",
            crate::pipeline::cst::unstrop(catalog_name_node.text())
        )
    } else {
        return Err(DelightQLError::parse_error(format!(
            "Unexpected catalog_name node kind: {}",
            catalog_name_node.kind()
        )));
    };

    // Check for alias
    let alias = node
        .find_child("table_alias")
        .and_then(|n| n.field_text("name"));

    let identifier = QualifiedName {
        namespace_path: NamespacePath::from_parts(vec!["sys".to_string(), "meta".to_string()])
            .map_err(|e| DelightQLError::parse_error(format!("Invalid namespace path: {:?}", e)))?,
        name: catalog_name.into(),
        grounding: None,
    };

    // Check for continuation (handles * as qualify_operator, etc.)
    if let Some(continuation_node) = node.field("continuation") {
        let base = RelationalExpression::Relation(Relation::Ground {
            identifier: identifier.clone(),
            canonical_name: PhaseBox::phantom(),
            domain_spec: DomainSpec::Bare,
            alias: alias.clone().map(|s| s.into()),
            outer: false,
            mutation_target: false,
            passthrough: false,
            cpr_schema: PhaseBox::phantom(),
            hygienic_injections: Vec::new(),
        });

        let subquery = super::continuation::handle_continuation(continuation_node, base, features)?;

        // Try to collapse to a DomainSpec (like table_access does)
        if let Some(domain_spec) = try_collapse_to_domain_spec(&subquery) {
            return Ok(RelationalExpression::Relation(Relation::Ground {
                identifier,
                canonical_name: PhaseBox::phantom(),
                domain_spec,
                alias: alias.map(|s| s.into()),
                outer: false,
                mutation_target: false,
                passthrough: false,
                cpr_schema: PhaseBox::phantom(),
                hygienic_injections: Vec::new(),
            }));
        }

        // Return as InnerRelation with Indeterminate pattern
        Ok(RelationalExpression::Relation(Relation::InnerRelation {
            pattern: InnerRelationPattern::Indeterminate {
                identifier,
                subquery: Box::new(subquery),
            },
            alias: alias.map(|s| s.into()),
            outer: false,
            cpr_schema: PhaseBox::phantom(),
        }))
    } else if let Some(columns_node) = node.field("columns") {
        let domain_spec = parse_column_spec(columns_node, features)?;
        Ok(RelationalExpression::Relation(Relation::Ground {
            identifier,
            canonical_name: PhaseBox::phantom(),
            domain_spec,
            alias: alias.map(|s| s.into()),
            outer: false,
            mutation_target: false,
            passthrough: false,
            cpr_schema: PhaseBox::phantom(),
            hygienic_injections: Vec::new(),
        }))
    } else {
        // Empty parens or no column spec
        Ok(RelationalExpression::Relation(Relation::Ground {
            identifier,
            canonical_name: PhaseBox::phantom(),
            domain_spec: DomainSpec::Glob,
            alias: alias.map(|s| s.into()),
            outer: false,
            mutation_target: false,
            passthrough: false,
            cpr_schema: PhaseBox::phantom(),
            hygienic_injections: Vec::new(),
        }))
    }
}

/// Parse pseudo-predicate call (e.g., mount!(), enlist!())
pub(super) fn parse_pseudo_predicate_call(
    node: CstNode,
    features: &mut crate::pipeline::query_features::FeatureCollector,
) -> Result<Relation> {
    // Get the pseudo-predicate name (includes the ! suffix)
    let name = node
        .field_text("name")
        .ok_or_else(|| DelightQLError::parse_error("No name in pseudo_predicate_call"))?;

    // Pseudo-predicate names must end with '!'
    let full_name = format!("{}!", name);

    // Parse arguments (literal expressions in MVP)
    let mut arguments = Vec::new();
    if let Some(args_list) = node.field("arguments") {
        // args_list is pseudo_predicate_argument_list
        for child in args_list.children() {
            if child.kind() == "domain_expression" {
                arguments.push(parse_expression(child, features)?);
            }
        }
    }

    // Check for alias
    let alias = node
        .find_child("table_alias")
        .and_then(|n| n.field_text("name"));

    Ok(Relation::PseudoPredicate {
        name: full_name,
        arguments,
        alias,
        cpr_schema: PhaseBox::phantom(),
    })
}

/// Parse TVF (Table-Valued Function) call
pub(super) fn parse_tvf_call(
    node: CstNode,
    features: &mut crate::pipeline::query_features::FeatureCollector,
) -> Result<Relation> {
    let function = node
        .field_text("function")
        .ok_or_else(|| DelightQLError::parse_error("No function in TVF"))?;

    // Parse namespace qualification (same as table_access)
    let (namespace_path, grounding) = if let Some(ns_node) = node.field("namespace_path") {
        let (ns, gr) = parse_namespace_qualification(ns_node)?;
        (Some(ns), gr)
    } else {
        (None, None)
    };

    // Collect arguments as flat strings AND structured groups.
    let mut arguments = Vec::new();
    let mut argument_groups = None;
    if let Some(args_node) = node.field("arguments") {
        collect_tvf_arguments_recursive(args_node, &mut arguments);
        // Also extract structured groups (preserves & and ; boundaries)
        let groups = super::operators::parse_ho_argument_list(args_node);
        if !groups.is_empty() {
            // Store groups whenever present (even single group with ; rows)
            argument_groups = Some(groups);
        }
    }

    // HO param substitution: replace table param names in TVF arguments
    if let Some(ref bindings) = features.ho_bindings {
        for arg in &mut arguments {
            if let Some(actual_name) = bindings.table_params.get(arg.as_str()) {
                *arg = actual_name.clone();
            }
        }
        if let Some(ref mut groups) = argument_groups {
            for group in groups.iter_mut() {
                for row in group.rows.iter_mut() {
                    for arg in row.iter_mut() {
                        if let Some(actual_name) = bindings.table_params.get(arg.as_str()) {
                            *arg = actual_name.clone();
                        }
                    }
                }
            }
        }
    }

    let alias = node
        .find_child("table_alias")
        .and_then(|n| n.field_text("name"));

    let domain_spec = if let Some(columns_node) = node.field("columns") {
        parse_column_spec(columns_node, features)?
    } else {
        DomainSpec::Glob
    };

    // Parse first-parens as DomainSpec for PatternResolver unification.
    // Each tvf_argument CST child becomes a DomainExpression.
    let mut first_parens_spec = if let Some(args_node) = node.field("arguments") {
        Some(parse_first_parens_as_domain_spec(args_node)?)
    } else {
        None
    };

    // HO param substitution: replace param names in first_parens_spec Lvars.
    // Table params: Lvar("T") → Lvar("actual_table_name")
    // Scalar params: Lvar("n") → the bound DomainExpression (e.g., Literal(5))
    if let Some(ref bindings) = features.ho_bindings {
        if let Some(DomainSpec::Positional(ref mut exprs)) = first_parens_spec {
            for expr in exprs.iter_mut() {
                if let DomainExpression::Lvar { name, .. } = expr {
                    if let Some(actual_name) = bindings.table_params.get(name.as_str()) {
                        *name = actual_name.clone().into();
                    } else if let Some(bound_expr) = bindings.scalar_params.get(name.as_str()) {
                        *expr = bound_expr.clone();
                    }
                }
            }
        }
    }

    Ok(Relation::TVF {
        function: function.into(),
        arguments,
        domain_spec,
        alias: alias.map(|s| s.into()),
        namespace: namespace_path,
        grounding,
        cpr_schema: PhaseBox::phantom(),
        argument_groups,
        first_parens_spec,
    })
}

/// Extract the logical name from a tvf_argument CST node.
/// For functor args (table_access child): returns just the table name (with optional namespace).
/// For b64 string_literal args: decodes the base64 content.
/// For other scalar/literal args: returns full text (including quotes for regular strings,
/// since the SQL transformer needs them for proper quoting).
pub(super) fn extract_tvf_argument_text(node: CstNode) -> String {
    for child in node.children() {
        match child.kind() {
            "table_access" => {
                let table = child.field_text("table").unwrap_or_default();
                if let Some(ns) = child.field("namespace_path") {
                    return format!("{}.{}", ns.text(), table);
                }
                return table;
            }
            "string_literal" => {
                let text = child.text();
                // Only decode b64-prefixed strings; regular strings keep their quotes
                // for downstream SQL generation.
                if text.starts_with("b64:") {
                    if let Some(decoded) =
                        super::expressions::literals::decode_string_literal_text(text)
                    {
                        return decoded;
                    }
                }
            }
            _ => {}
        }
    }
    node.text().to_string()
}

/// Parse a tvf_argument CST node into a DomainExpression for PatternResolver unification.
///
/// Maps grammar alternatives to DomainExpression variants:
/// - `string_literal` → Literal(String)
/// - `number_literal` → Literal(Number)
/// - `identifier` → Lvar (could be table name or scalar — Step 4 disambiguates)
/// - `qualified_column` → Lvar with qualifier
/// - `table_access` → Lvar (table reference — Step 4 separates table params)
/// - `value_placeholder` (@) → ValuePlaceholder
/// - `*` → Projection(Glob)
fn parse_tvf_argument_as_domain_expression(node: CstNode) -> Result<DomainExpression> {
    for child in node.children() {
        match child.kind() {
            "string_literal" => {
                let text = child.text();
                let value = if text.starts_with("b64:") {
                    super::expressions::literals::decode_string_literal_text(text)
                        .unwrap_or_else(|| text.to_string())
                } else {
                    super::expressions::literals::strip_string_quotes(text).to_string()
                };
                return Ok(DomainExpression::literal_builder(LiteralValue::String(value)).build());
            }
            "number_literal" => {
                return Ok(DomainExpression::literal_builder(LiteralValue::Number(
                    child.text().to_string(),
                ))
                .build());
            }
            "identifier" => {
                return Ok(DomainExpression::lvar_builder(crate::pipeline::cst::unstrop(
                    child.text(),
                ))
                .build());
            }
            "qualified_column" => {
                let qualifier = if let Some(table_field) = child.field("table") {
                    Some(crate::pipeline::cst::unstrop(table_field.text()))
                } else {
                    child.field_text("qualifier")
                };
                let name = child
                    .field_text("column")
                    .unwrap_or_else(|| child.text().to_string());
                return Ok(DomainExpression::lvar_builder(name)
                    .with_qualifier(qualifier)
                    .build());
            }
            "table_access" => {
                // Table reference — represented as Lvar; Step 4 will separate table params
                let table_name = child.field_text("table").unwrap_or_default();
                return Ok(DomainExpression::lvar_builder(table_name).build());
            }
            "value_placeholder" => {
                return Ok(DomainExpression::ValuePlaceholder { alias: None });
            }
            "placeholder" => {
                return Ok(DomainExpression::NonUnifiyingUnderscore);
            }
            _ => {}
        }
    }
    // `*` appears as a bare token child, not as a named child kind
    let text = node.text().trim();
    if text == "*" {
        return Ok(DomainExpression::glob_builder().build());
    }
    Err(DelightQLError::parse_error(format!(
        "Cannot parse tvf_argument as DomainExpression: '{}'",
        node.text()
    )))
}

/// Parse the first-parens arguments node into a DomainSpec.
///
/// Handles all CST structures: ho_argument_list, argument_list, and direct tvf_argument.
/// Each tvf_argument becomes a DomainExpression. Glob (*) alone produces DomainSpec::Glob.
/// The `&` group separator is flattened — Step 4 uses entity param types to separate.
pub(super) fn parse_first_parens_as_domain_spec(args_node: CstNode) -> Result<DomainSpec> {
    let mut exprs = Vec::new();
    collect_first_parens_exprs(args_node, &mut exprs)?;

    // Single glob → DomainSpec::Glob (enumerate all PureGround values)
    if exprs.len() == 1 {
        if let DomainExpression::Projection(ProjectionExpr::Glob { .. }) = &exprs[0] {
            return Ok(DomainSpec::Glob);
        }
    }

    if exprs.is_empty() {
        Ok(DomainSpec::Bare)
    } else {
        Ok(DomainSpec::Positional(exprs))
    }
}

/// Recursively collect DomainExpressions from first-parens CST nodes.
fn collect_first_parens_exprs(
    node: CstNode,
    out: &mut Vec<DomainExpression>,
) -> Result<()> {
    for child in node.children() {
        match child.kind() {
            "tvf_argument" => out.push(parse_tvf_argument_as_domain_expression(child)?),
            "ho_argument_list" | "ho_argument_group" | "ho_argument_row" | "argument_list" => {
                collect_first_parens_exprs(child, out)?;
            }
            _ => {} // skip separators (&, ;, ,)
        }
    }
    Ok(())
}

/// Recursively collect tvf_argument text from potentially nested node structures.
/// Handles ho_argument_list → ho_argument_group → ho_argument_row → tvf_argument.
fn collect_tvf_arguments_recursive(node: CstNode, out: &mut Vec<String>) {
    for child in node.children() {
        match child.kind() {
            "tvf_argument" => out.push(extract_tvf_argument_text(child)),
            "ho_argument_list" | "ho_argument_group" | "ho_argument_row" | "argument_list" => {
                collect_tvf_arguments_recursive(child, out);
            }
            other => {
                log::warn!("Ignoring unknown node kind in TVF arguments: {:?}", other);
            }
        }
    }
}

fn parse_single_ns(node: CstNode) -> Result<NamespacePath> {
    match node.kind() {
        "identifier" => Ok(NamespacePath::single(crate::pipeline::cst::unstrop(
            node.text(),
        ))),
        "namespace_path" => {
            let parts: Vec<String> = node
                .children()
                .filter(|child| child.kind() == "identifier")
                .map(|child| crate::pipeline::cst::unstrop(child.text()))
                .collect();
            NamespacePath::from_parts(parts).map_err(|e| {
                DelightQLError::parse_error(format!("Invalid namespace path: {:?}", e))
            })
        }
        other => Err(DelightQLError::parse_error(format!(
            "Expected identifier or namespace_path, got: {other}"
        ))),
    }
}

/// Parse namespace qualification from a CST node. Returns (namespace_path, optional grounding).
///
/// Handles three cases:
/// - `identifier` → single-level namespace, no grounding
/// - `namespace_path` → multi-level namespace, no grounding
/// - `grounded_namespace` → data_ns^lib_ns with grounding context
pub(super) fn parse_namespace_qualification(
    ns_node: CstNode,
) -> Result<(NamespacePath, Option<GroundedPath>)> {
    match ns_node.kind() {
        "identifier" | "namespace_path" => {
            let ns = parse_single_ns(ns_node)?;
            Ok((ns, None))
        }
        "grounded_namespace" => {
            // Children are: ns_or_id, ^, ns_or_id, ^, ns_or_id, ...
            // First non-^ child is the data namespace, rest are grounded namespaces
            let ns_children: Vec<CstNode> = ns_node
                .children()
                .filter(|c| c.kind() == "namespace_path" || c.kind() == "identifier")
                .collect();

            if ns_children.len() < 2 {
                return Err(DelightQLError::parse_error(
                    "Grounded namespace requires at least data_ns^lib_ns",
                ));
            }

            let data_ns = parse_single_ns(ns_children[0].clone())?;
            let grounded_ns: Vec<NamespacePath> = ns_children[1..]
                .iter()
                .map(|c| parse_single_ns(c.clone()))
                .collect::<Result<_>>()?;

            Ok((
                data_ns.clone(),
                Some(GroundedPath {
                    data_ns,
                    grounded_ns,
                }),
            ))
        }
        other => panic!("catch-all hit in builder_v2/relations.rs parse_namespace_path: unexpected node kind {:?}", other),
    }
}

pub(super) fn parse_table_access(
    node: CstNode,
    features: &mut crate::pipeline::query_features::FeatureCollector,
) -> Result<RelationalExpression> {
    let outer = node.field("outer").is_some(); // Postfix ? on table name: table?(*)
    let mutation_target = node.field("mutation_target").is_some(); // Postfix !! for DML mutation target
    let passthrough = node.has_child("passthrough_separator");

    // Parse namespace_path field (can be identifier, namespace_path, or grounded_namespace)
    let (namespace_path, grounding) = if let Some(ns_node) = node.field("namespace_path") {
        parse_namespace_qualification(ns_node)?
    } else {
        (NamespacePath::empty(), None)
    };

    // Get table name (required)
    let raw_table_name = node
        .field_text("table")
        .ok_or_else(|| DelightQLError::parse_error("No table name in table_access"))?;

    // Glob binding: substitute table name with bound actual name (e.g., T → users)
    let table_name = if namespace_path.is_empty() && grounding.is_none() {
        if let Some(ref bindings) = features.ho_bindings {
            if let Some(actual_name) = bindings.table_params.get(raw_table_name.as_str()) {
                actual_name.clone()
            } else {
                raw_table_name.to_string()
            }
        } else {
            raw_table_name.to_string()
        }
    } else {
        raw_table_name.to_string()
    };

    // Check for alias
    let alias = node
        .find_child("table_alias")
        .and_then(|n| n.field_text("name"));

    // Check if this is INNER-RELATION (has continuation) or regular table access (has columns)
    if let Some(continuation_node) = node.field("continuation") {
        // INNER-RELATION: table(|> pipeline) or table(, correlation |> pipeline)
        // This is similar to inner_exists but returns a Relation instead of a BooleanExpression

        let identifier = QualifiedName {
            namespace_path: namespace_path.clone(),
            name: table_name.into(),
            grounding: grounding.clone(),
        };

        // Create base relation (will be used as starting point for continuation)
        // HO table_expr substitution: if the table name matches a bound expression
        // (e.g., V → depts(v) for argumentative table refs), use it as the base.
        // This handles V(|> ...) where V is an HO param with interior operators.
        let base = if namespace_path.is_empty() && grounding.is_none() {
            if let Some(ref bindings) = features.ho_bindings {
                if let Some(bound_expr) = bindings.table_expr_params.get(raw_table_name.as_str()) {
                    let mut expr = bound_expr.clone();
                    if let Some(ref alias_str) = alias {
                        apply_alias_to_relational_expr(&mut expr, alias_str.clone());
                    }
                    expr
                } else {
                    RelationalExpression::Relation(Relation::Ground {
                        identifier: identifier.clone(),
                        canonical_name: PhaseBox::phantom(),
                        domain_spec: DomainSpec::Bare,
                        alias: alias.clone().map(|s| s.into()),
                        outer,
                        mutation_target,
                        passthrough,
                        cpr_schema: PhaseBox::phantom(),
                        hygienic_injections: Vec::new(),
                    })
                }
            } else {
                RelationalExpression::Relation(Relation::Ground {
                    identifier: identifier.clone(),
                    canonical_name: PhaseBox::phantom(),
                    domain_spec: DomainSpec::Bare,
                    alias: alias.clone().map(|s| s.into()),
                    outer,
                    mutation_target,
                    passthrough,
                    cpr_schema: PhaseBox::phantom(),
                    hygienic_injections: Vec::new(),
                })
            }
        } else {
            RelationalExpression::Relation(Relation::Ground {
                identifier: identifier.clone(),
                canonical_name: PhaseBox::phantom(),
                domain_spec: DomainSpec::Bare,
                alias: alias.clone().map(|s| s.into()),
                outer,
                mutation_target,
                passthrough,
                cpr_schema: PhaseBox::phantom(),
                hygienic_injections: Vec::new(),
            })
        };

        // Parse the continuation to get the full subquery
        let subquery = super::continuation::handle_continuation(continuation_node, base, features)?;

        // Check if this is a simple pattern that can be collapsed to a DomainSpec
        // instead of creating a full inner relation. This handles:
        // - table(*) → DomainSpec::Glob
        // - table(.(cols)) → DomainSpec::GlobWithUsing
        // - table(*.(cols)) → DomainSpec::GlobWithUsing
        if let Some(domain_spec) = try_collapse_to_domain_spec(&subquery) {
            // Argumentative binding: if T(*) collapses to Glob and T matches a table_expr param,
            // replace the entire expression with the bound anonymous table
            if domain_spec == DomainSpec::Glob && namespace_path.is_empty() && grounding.is_none() {
                if let Some(ref bindings) = features.ho_bindings {
                    if let Some(bound_expr) =
                        bindings.table_expr_params.get(raw_table_name.as_str())
                    {
                        let mut expr = bound_expr.clone();
                        // Apply alias from call site (e.g., Tags(*) as tg → anon_table as tg)
                        if let Some(ref alias_str) = alias {
                            apply_alias_to_relational_expr(&mut expr, alias_str.clone());
                        }
                        return Ok(expr);
                    }
                }
            }

            return Ok(RelationalExpression::Relation(Relation::Ground {
                identifier,
                canonical_name: PhaseBox::phantom(),
                domain_spec,
                alias: alias.map(|s| s.into()),
                outer,
                mutation_target,
                passthrough,
                cpr_schema: PhaseBox::phantom(),
                hygienic_injections: Vec::new(),
            }));
        }

        // Return as InnerRelation with Indeterminate pattern
        // The refiner will classify this into UDT/CDT-SJ/CDT-GJ/CDT-WJ
        Ok(RelationalExpression::Relation(Relation::InnerRelation {
            pattern: InnerRelationPattern::Indeterminate {
                identifier: identifier.clone(),
                subquery: Box::new(subquery),
            },
            alias: alias.map(|s| s.into()),
            outer,
            cpr_schema: PhaseBox::phantom(),
        }))
    } else {
        // Regular table access with column_spec (or empty parens)
        let domain_spec = if let Some(columns_node) = node.field("columns") {
            parse_column_spec(columns_node, features)?
        } else {
            // Empty parens: () introduces unqualified names (natural join candidate)
            DomainSpec::Bare
        };

        Ok(RelationalExpression::Relation(Relation::Ground {
            identifier: QualifiedName {
                namespace_path,
                name: table_name.into(),
                grounding,
            },
            canonical_name: PhaseBox::phantom(),
            domain_spec,
            alias: alias.map(|s| s.into()),
            outer,
            mutation_target,
            passthrough,
            cpr_schema: PhaseBox::phantom(),
            hygienic_injections: Vec::new(),
        }))
    }
}

pub(super) fn parse_anonymous_table(node: CstNode, features: &mut crate::pipeline::query_features::FeatureCollector) -> Result<Relation> {
    let outer = node.has_child("outer_marker");

    // Check for exists_marker prefix (+_() or \+_())
    let exists_mode = if let Some(exists_marker_node) = node.find_child("exists_marker") {
        let marker_text = exists_marker_node.text();
        marker_text == "+" || marker_text == "\\+"
    } else {
        false
    };

    // Check for qua target: _(cols @ data) qua target_table
    let qua_target = node
        .field_text("qua_target")
        .map(|s| SqlIdentifier::new(&s));

    // Check for alias at the end
    let alias = node
        .find_child("table_alias")
        .and_then(|n| n.field_text("name"));

    // Parse headers and data
    let mut column_headers = None;
    let mut rows = Vec::new();
    let mut sparse_indices: Vec<usize> = Vec::new();
    let mut sparse_names: Vec<String> = Vec::new();

    // Look for column_headers
    if let Some(headers_node) = node.find_child("column_headers") {
        let mut headers = Vec::new();
        let mut col_idx = 0usize;
        for child in headers_node.children() {
            match child.kind() {
                "column_header_item" => {
                    // Check for sparse marker
                    let is_sparse = child.find_child("sparse_marker").is_some();

                    // Parse the header expression from the item's inner content
                    let header_expr = if let Some(id_node) = child.find_child("identifier") {
                        let name = crate::pipeline::cst::unstrop(id_node.text());
                        if is_sparse {
                            sparse_names.push(name.to_lowercase());
                            sparse_indices.push(col_idx);
                        }
                        DomainExpression::lvar_builder(name).build()
                    } else if let Some(qc_node) = child.find_child("qualified_column") {
                        super::expressions::parse_expression(qc_node, &mut FeatureCollector::new())?
                    } else if let Some(fc_node) = child.find_child("function_call") {
                        parse_function_call(fc_node)?
                    } else {
                        continue; // Skip unknown children
                    };
                    headers.push(header_expr);
                    col_idx += 1;
                }
                // Legacy path: direct identifier/qualified_column/function_call
                // (for grammars without column_header_item wrapping)
                "identifier" => {
                    headers.push(
                        DomainExpression::lvar_builder(crate::pipeline::cst::unstrop(child.text()))
                            .build(),
                    );
                    col_idx += 1;
                }
                "qualified_column" => {
                    headers.push(super::expressions::parse_expression(
                        child,
                        &mut FeatureCollector::new(),
                    )?);
                    col_idx += 1;
                }
                "function_call" => {
                    headers.push(parse_function_call(child)?);
                    col_idx += 1;
                }
                other => panic!(
                    "unexpected node kind in anonymous table column headers: {:?}",
                    other
                ),
            }
        }
        column_headers = Some(headers);
    }

    // Parse data_rows
    if let Some(data_rows_node) = node.find_child("data_rows") {
        if sparse_indices.is_empty() {
            // Standard path: no sparse columns
            for child in data_rows_node.children() {
                if child.kind() == "data_row" {
                    rows.push(parse_data_row(child, features)?);
                }
            }
        } else {
            // Sparse path: parse rows with sparse fill awareness
            let total_width = column_headers.as_ref().map_or(0, |h| h.len());
            let n_required = total_width - sparse_indices.len();

            for (row_idx, child) in data_rows_node
                .children()
                .filter(|c| c.kind() == "data_row")
                .enumerate()
            {
                let row = parse_sparse_data_row(
                    child,
                    &sparse_indices,
                    &sparse_names,
                    total_width,
                    n_required,
                    row_idx,
                    features,
                )?;
                rows.push(row);
            }
        }
    }

    // Validate anonymous table dimensions
    if !rows.is_empty() {
        let first_row_width = rows[0].values.len();

        // Check header count matches row width
        if let Some(ref headers) = column_headers {
            if headers.len() != first_row_width {
                return Err(DelightQLError::parse_error_categorized(
                    "anon",
                    format!(
                        "Anonymous table has {} column header(s) but rows have {} value(s)",
                        headers.len(),
                        first_row_width
                    ),
                ));
            }
        }

        // Check all rows have the same width
        for (i, row) in rows.iter().enumerate().skip(1) {
            if row.values.len() != first_row_width {
                return Err(DelightQLError::parse_error_categorized(
                    "anon",
                    format!(
                        "Anonymous table row {} has {} value(s) but row 1 has {}",
                        i + 1,
                        row.values.len(),
                        first_row_width
                    ),
                ));
            }
        }
    }

    Ok(Relation::Anonymous {
        column_headers,
        rows,
        alias: alias.map(|s| s.into()),
        outer,
        exists_mode,
        qua_target,
        cpr_schema: PhaseBox::phantom(),
    })
}

/// Parse a data row
/// EPOCH 7: Updated to handle domain_expression for melt/unpivot (including binary expressions)
pub(super) fn parse_data_row(node: CstNode, features: &mut crate::pipeline::query_features::FeatureCollector) -> Result<Row> {
    let mut values = Vec::new();
    for child in node.children().filter(|child| child.kind() == "domain_expression") {
        let inner = child.children().next().ok_or_else(|| {
            DelightQLError::parse_error("Empty domain_expression in data row")
        })?;
        values.push(parse_expression(inner, features)?);
    }
    Ok(Row { values })
}

/// Parse a data row in a sparse anonymous table.
///
/// Separates positional values from sparse fills, then builds
/// a full-width row with NULLs for unfilled sparse columns.
fn parse_sparse_data_row(
    node: CstNode,
    sparse_indices: &[usize],
    sparse_names: &[String],
    total_width: usize,
    n_required: usize,
    row_idx: usize,
    features: &mut crate::pipeline::query_features::FeatureCollector,
) -> Result<Row> {
    let mut positional = Vec::new();
    // Map from sparse column name (lowercase) -> value
    let mut fills: Vec<(String, DomainExpression)> = Vec::new();

    for child in node.children().filter(|c| c.kind() == "domain_expression") {
        let inner = child
            .children()
            .next()
            .ok_or_else(|| DelightQLError::parse_error("Empty domain_expression in data row"))?;

        if inner.kind() == "sparse_fill" {
            // Parse sparse fill: _(col1, col2 @ val1, val2)
            let fill_pairs = parse_sparse_fill_node(inner, sparse_names, features)?;
            fills.extend(fill_pairs);
        } else {
            // Regular positional value
            positional.push(parse_expression(inner, features)?);
        }
    }

    if positional.len() != n_required {
        return Err(DelightQLError::parse_error_categorized(
            "anon",
            format!(
                "Sparse anonymous table row {} has {} positional value(s) but expected {} \
                 (total columns: {}, sparse columns: {})",
                row_idx + 1,
                positional.len(),
                n_required,
                total_width,
                sparse_indices.len(),
            ),
        ));
    }

    // Build full-width row: interleave positional values + sparse fills + NULLs
    let mut full_row = Vec::with_capacity(total_width);
    let mut pos_iter = positional.into_iter();

    for i in 0..total_width {
        if let Some(sparse_pos) = sparse_indices.iter().position(|&idx| idx == i) {
            let col_name = &sparse_names[sparse_pos];
            if let Some((_name, val)) = fills.iter().find(|(name, _)| name == col_name) {
                full_row.push(val.clone());
            } else {
                // Unfilled sparse column → NULL
                full_row.push(DomainExpression::Literal {
                    value: LiteralValue::Null,
                    alias: None,
                });
            }
        } else {
            full_row.push(pos_iter.next().unwrap());
        }
    }

    Ok(Row { values: full_row })
}

/// Parse a sparse_fill CST node: _(col1, col2 @ val1, val2)
///
/// Returns pairs of (column_name, value_expression).
fn parse_sparse_fill_node(
    node: CstNode,
    sparse_names: &[String],
    features: &mut crate::pipeline::query_features::FeatureCollector,
) -> Result<Vec<(String, DomainExpression)>> {
    // Extract column names from the column_headers child
    let headers_node = node
        .find_child("column_headers")
        .ok_or_else(|| DelightQLError::parse_error("Sparse fill missing column headers"))?;

    let mut col_names = Vec::new();
    for child in headers_node.children() {
        if child.kind() == "column_header_item" {
            if let Some(id_node) = child.find_child("identifier") {
                col_names.push(crate::pipeline::cst::unstrop(id_node.text()).to_lowercase());
            }
        } else if child.kind() == "identifier" {
            col_names.push(crate::pipeline::cst::unstrop(child.text()).to_lowercase());
        }
    }

    // Validate that all referenced columns are actually sparse
    for name in &col_names {
        if !sparse_names.contains(name) {
            return Err(DelightQLError::parse_error_categorized(
                "anon",
                format!(
                    "Sparse fill references column '{}' which is not a sparse (?) column. \
                     Sparse columns are: {}",
                    name,
                    sparse_names.join(", "),
                ),
            ));
        }
    }

    // Extract values from the data_row child
    let data_row_node = node
        .find_child("data_row")
        .ok_or_else(|| DelightQLError::parse_error("Sparse fill missing data row"))?;

    let mut values = Vec::new();
    for child in data_row_node
        .children()
        .filter(|c| c.kind() == "domain_expression")
    {
        let inner = child
            .children()
            .next()
            .ok_or_else(|| DelightQLError::parse_error("Empty domain_expression in sparse fill"))?;
        values.push(parse_expression(inner, features)?);
    }

    if col_names.len() != values.len() {
        return Err(DelightQLError::parse_error_categorized(
            "anon",
            format!(
                "Sparse fill has {} column name(s) but {} value(s)",
                col_names.len(),
                values.len(),
            ),
        ));
    }

    Ok(col_names.into_iter().zip(values).collect())
}

/// Try to collapse a simple continuation pattern to a DomainSpec.
///
/// This handles the common cases where interior continuations can be
/// collapsed back to a simple table access with a DomainSpec:
///
/// - `table() *` → DomainSpec::Glob
/// - `table() *.(cols)` → DomainSpec::GlobWithUsing(cols)
/// - `table() .(cols)` → DomainSpec::GlobWithUsing(cols)
///
/// Returns None if the subquery is too complex to collapse (e.g., pipes,
/// filters, projections, meta-ize).
fn try_collapse_to_domain_spec(subquery: &RelationalExpression) -> Option<DomainSpec> {
    match subquery {
        RelationalExpression::Pipe(pipe) => {
            match (&pipe.source, &pipe.operator) {
                // Pattern 1: table() * → Glob
                // A single Qualify operator on a bare Ground relation
                (
                    RelationalExpression::Relation(Relation::Ground {
                        domain_spec: DomainSpec::Bare,
                        ..
                    }),
                    UnaryRelationalOperator::Qualify,
                ) => Some(DomainSpec::Glob),

                // Pattern 2: table() .(cols) → GlobWithUsing(cols)
                // A single Using operator on a bare Ground relation
                (
                    RelationalExpression::Relation(Relation::Ground {
                        domain_spec: DomainSpec::Bare,
                        ..
                    }),
                    UnaryRelationalOperator::Using { columns },
                ) => Some(DomainSpec::GlobWithUsing(columns.clone())),

                // Pattern 3: table() *.(cols) → GlobWithUsing(cols)
                // Using operator on top of Qualify on a bare Ground relation
                (
                    RelationalExpression::Pipe(inner_pipe),
                    UnaryRelationalOperator::Using { columns },
                ) => {
                    match (&inner_pipe.source, &inner_pipe.operator) {
                        (
                            RelationalExpression::Relation(Relation::Ground {
                                domain_spec: DomainSpec::Bare,
                                ..
                            }),
                            UnaryRelationalOperator::Qualify,
                        ) => Some(DomainSpec::GlobWithUsing(columns.clone())),
                        _ => {
                            // Try collapsing the inner pipe first, then extend with these columns
                            // Handles: table() *.(id).(name) → GlobWithUsing([id, name])
                            if let Some(inner_spec) = try_collapse_to_domain_spec(
                                &RelationalExpression::Pipe(inner_pipe.clone()),
                            ) {
                                match inner_spec {
                                    DomainSpec::GlobWithUsing(mut existing) => {
                                        existing.extend(columns.clone());
                                        Some(DomainSpec::GlobWithUsing(existing))
                                    }
                                    // Glob, Positional, Bare can't be extended with USING columns
                                    DomainSpec::Glob
                                    | DomainSpec::Positional(_)
                                    | DomainSpec::Bare => None,
                                }
                            } else {
                                None
                            }
                        }
                    }
                }

                // Pattern 4: table() * * → Glob (idempotent)
                // Qualify on top of Qualify collapses
                (inner_source, UnaryRelationalOperator::Qualify) => {
                    try_collapse_to_domain_spec(inner_source)
                }

                // Any other pattern cannot be collapsed
                // Any other pipe pattern cannot be collapsed to a DomainSpec
                // (e.g., MetaIze, General projection, Modulo, etc.)
                _ => None,
            }
        }
        // Non-pipe expressions cannot be collapsed to DomainSpec
        // Non-pipe expressions cannot be collapsed to DomainSpec
        // (Filter, Join, SetOperation, Relation, etc.)
        _ => None,
    }
}

/// Apply an alias to a `RelationalExpression` by setting the alias field
/// on the innermost `Relation` variant.
fn apply_alias_to_relational_expr(expr: &mut RelationalExpression, alias: String) {
    match expr {
        RelationalExpression::Relation(rel) => match rel {
            Relation::Ground {
                alias: ref mut a, ..
            }
            | Relation::Anonymous {
                alias: ref mut a, ..
            }
            | Relation::TVF {
                alias: ref mut a, ..
            } => {
                *a = Some(alias.into());
            }
            Relation::InnerRelation {
                alias: ref mut a, ..
            } => {
                *a = Some(alias.into());
            }
            // ConsultedView/PseudoPredicate: only exists post-resolution, not reachable in builder
            Relation::ConsultedView { .. } | Relation::PseudoPredicate { .. } => {
                unreachable!("ConsultedView/PseudoPredicate not present in builder phase")
            }
        },
        // Pipe/Filter: recurse to innermost relation
        RelationalExpression::Pipe(pipe) => {
            apply_alias_to_relational_expr(&mut pipe.source, alias);
        }
        RelationalExpression::Filter { source, .. } => {
            apply_alias_to_relational_expr(source, alias);
        }
        // Join/SetOperation: can't meaningfully alias composite expressions
        RelationalExpression::Join { .. } | RelationalExpression::SetOperation { .. } => {
            log::warn!("Cannot apply alias to composite RelationalExpression");
        }
        // ER chains consumed before builder
        RelationalExpression::ErJoinChain { .. }
        | RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains consumed before apply_alias_to_relational_expr")
        }
    }
}
