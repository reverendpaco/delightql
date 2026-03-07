//! Piped invocations: HO view application, DML terminals, directive terminals

use super::super::expressions::*;
use super::super::relations;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::cst::CstNode;
use crate::pipeline::query_features::FeatureCollector;

/// Parse piped higher-order view invocation: source |> ho_view(cols) or source |> ho_view(args)(cols)
pub(in crate::pipeline::builder_v2) fn parse_piped_invocation(
    node: CstNode,
    input: RelationalExpression,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    let function_node = node
        .field("function")
        .ok_or_else(|| DelightQLError::parse_error("No function in piped invocation"))?;
    let function = function_node.text().to_string();

    // Assertion views (exists/notexists/forall/equals) are no-ops during building —
    // the predicate is extracted by detect_assertion_view in continuation.rs.
    // The view pipe is consumed as metadata, not as a relational operation.
    // Unicode aliases (∃, ∄, ∀, ≡) are accepted as equivalent.
    match function.as_str() {
        "exists" | "∃" | "notexists" | "∄" | "forall" | "∀" | "equals" | "≡" => {
            return Ok(input)
        }
        _ => {}
    }

    // Extract optional namespace qualification
    let namespace = if let Some(ns_node) = node.field("namespace_path") {
        let (ns, _grounding) = relations::parse_namespace_qualification(ns_node)?;
        Some(ns)
    } else {
        None
    };

    // Collect arguments if present (for multi-param HO views: |> mask_ssn("***")(*))
    let (arguments, mut first_parens_spec) = if let Some(args_node) = node.field("arguments") {
        let groups = parse_ho_argument_list(args_node);
        let spec = relations::parse_first_parens_as_domain_spec(args_node)?;
        (groups, Some(spec))
    } else {
        (Vec::new(), None)
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

    // Parse column spec (output columns)
    let domain_spec = if let Some(columns_node) = node.field("columns") {
        relations::parse_column_spec(columns_node, features)?
    } else {
        DomainSpec::Glob
    };

    Ok(RelationalExpression::Pipe(Box::new(
        stacksafe::StackSafe::new(PipeExpression {
            source: input,
            operator: UnaryRelationalOperator::HoViewApplication {
                function,
                arguments,
                first_parens_spec,
                domain_spec,
                namespace,
            },
            cpr_schema: PhaseBox::phantom(),
        }),
    )))
}

/// Parse bang pipe operation: unified DML + directive pipe.
/// Dispatches to DML or directive based on which CST fields are present.
pub(in crate::pipeline::builder_v2) fn parse_bang_pipe_operation(
    node: CstNode,
    input: RelationalExpression,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    if node.field("target").is_some() || node.field("anon_target").is_some() {
        // DML path: has 'target' (named table) or 'anon_target' (anonymous table)
        parse_dml_pipe_target(node, input, features)
    } else {
        // Directive path: has 'name' and optional 'arguments'
        features.mark(crate::pipeline::query_features::QueryFeature::PseudoPredicates);
        let name = node
            .field_text("name")
            .ok_or_else(|| DelightQLError::parse_error("No name in directive pipe terminal"))?;
        let full_name = format!("{}!", name);

        let mut arguments = Vec::new();
        if let Some(args_node) = node.field("arguments") {
            for child in args_node.children() {
                if child.kind() == "domain_expression" {
                    arguments.push(parse_expression(child, features)?);
                }
            }
        }

        Ok(RelationalExpression::Pipe(Box::new(
            stacksafe::StackSafe::new(PipeExpression {
                source: input,
                operator: UnaryRelationalOperator::DirectiveTerminal {
                    name: full_name,
                    arguments,
                },
                cpr_schema: PhaseBox::phantom(),
            }),
        )))
    }
}

/// Parse DML pipe target: delete!(table)(*), update!(ns.table)(*), etc.
fn parse_dml_pipe_target(
    node: CstNode,
    input: RelationalExpression,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    use crate::pipeline::asts::core::operators::DmlKind;

    let operation = node
        .field_text("operation")
        .ok_or_else(|| DelightQLError::parse_error("No operation in DML pipe target"))?;

    let kind = match operation.as_str() {
        "update" => DmlKind::Update,
        "delete" => DmlKind::Delete,
        "insert" => DmlKind::Insert,
        "keep" => DmlKind::Keep,
        _ => {
            return Err(DelightQLError::parse_error(format!(
                "Unknown DML operation: {}!. Expected update!, delete!, insert!, or keep!",
                operation
            )))
        }
    };

    // Extract target: either named table_access or anonymous_table
    let (target, target_namespace) = if let Some(target_node) = node.field("target") {
        let target = target_node
            .field_text("table")
            .unwrap_or_else(|| target_node.text().to_string());
        let target_namespace = target_node
            .field("namespace_path")
            .map(|ns| ns.text().to_string());
        (target, target_namespace)
    } else if node.field("anon_target").is_some() {
        // Anonymous table target: _(*)
        ("_".to_string(), None)
    } else {
        return Err(DelightQLError::parse_error("No target in DML pipe target"));
    };

    // Parse domain spec (column selection)
    let domain_spec = if let Some(columns_node) = node.field("columns") {
        relations::parse_column_spec(columns_node, features)?
    } else {
        DomainSpec::Glob
    };

    Ok(RelationalExpression::Pipe(Box::new(
        stacksafe::StackSafe::new(PipeExpression {
            source: input,
            operator: UnaryRelationalOperator::DmlTerminal {
                kind,
                target,
                target_namespace,
                domain_spec,
            },
            cpr_schema: PhaseBox::phantom(),
        }),
    )))
}

/// Parse HO argument list from CST node.
///
/// Supports three structures:
/// - `ho_argument_list` → contains `ho_argument_group` nodes separated by `&`
/// - `argument_list` → legacy flat list (backward compat)
/// - Anything else → fall back to extracting tvf_argument children
pub(in crate::pipeline::builder_v2) fn parse_ho_argument_list(
    node: CstNode,
) -> Vec<crate::pipeline::asts::core::operators::HoCallGroup> {
    use crate::pipeline::asts::core::operators::HoCallGroup;

    match node.kind() {
        "ho_argument_list" => {
            // New structured form: & separates groups, ; separates rows within groups
            let mut groups = Vec::new();
            for child in node.children() {
                if child.kind() == "ho_argument_group" {
                    groups.push(parse_ho_argument_group(child));
                }
            }
            if groups.is_empty() {
                // Fallback: single group from direct children
                groups.push(parse_ho_argument_group(node));
            }
            groups
        }
        "argument_list" => {
            // Legacy flat list: all args in one group, one row
            let mut values = Vec::new();
            for child in node.children() {
                if child.kind() == "tvf_argument" {
                    values.push(relations::extract_tvf_argument_text(child));
                }
            }
            if values.is_empty() {
                Vec::new()
            } else {
                vec![HoCallGroup::single_row(values)]
            }
        }
        _ => {
            // Direct tvf_arguments at this level
            let mut values = Vec::new();
            for child in node.children() {
                if child.kind() == "tvf_argument" {
                    values.push(relations::extract_tvf_argument_text(child));
                }
            }
            if values.is_empty() {
                Vec::new()
            } else {
                vec![HoCallGroup::single_row(values)]
            }
        }
    }
}

/// Parse a single &-separated group from an ho_argument_group CST node.
fn parse_ho_argument_group(node: CstNode) -> crate::pipeline::asts::core::operators::HoCallGroup {
    use crate::pipeline::asts::core::operators::HoCallGroup;

    let mut rows = Vec::new();
    for child in node.children() {
        if child.kind() == "ho_argument_row" {
            let values: Vec<String> = child
                .children()
                .filter(|c| c.kind() == "tvf_argument")
                .map(|c| relations::extract_tvf_argument_text(c))
                .collect();
            if !values.is_empty() {
                rows.push(values);
            }
        }
    }

    if rows.is_empty() {
        // Fallback: extract tvf_arguments directly from this node
        let values: Vec<String> = node
            .children()
            .filter(|c| c.kind() == "tvf_argument")
            .map(|c| relations::extract_tvf_argument_text(c))
            .collect();
        HoCallGroup {
            rows: if values.is_empty() {
                Vec::new()
            } else {
                vec![values]
            },
        }
    } else {
        HoCallGroup { rows }
    }
}
