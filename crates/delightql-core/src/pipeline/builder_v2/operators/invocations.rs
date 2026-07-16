// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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
    let (namespace, grounding) = if let Some(ns_node) = node.field("namespace_path") {
        let (ns, grounding) = relations::parse_namespace_qualification(ns_node)?;
        (Some(ns), grounding)
    } else {
        (None, None)
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
                grounding,
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
                } else if child.kind() == "namespace_path" {
                    // Bare ::-qualified argument (REPORT-1.5 F2): carried as
                    // an Lvar with the :: text intact.
                    arguments.push(
                        DomainExpression::lvar_builder(child.text().to_string()).build(),
                    );
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
    let operation = node
        .field_text("operation")
        .ok_or_else(|| DelightQLError::parse_error("No operation in DML pipe target"))?;

    match operation.as_str() {
        // DML directives take their TARGET as a relational designator —
        // a preserved higher-order term, never a string (Phase 6 slice 5,
        // the mzynmnok item-4 residue): the effect transformer interprets
        // it as a whole-table designator or refuses with a teaching
        // diagnostic, exactly like the DDL path below.
        "delete" | "insert" | "update" => {
            return parse_directive_pipe_invocation(node, input, features)
        }
        // Piped two-paren form of a NON-DML directive:
        //   source |> name!(Rel(*))(spec)
        // returning! and stdout! have exactly ONE higher-order parameter:
        // with the pipe supplying it, an explicit relational argument leaves
        // the pipe nowhere to land — refuse (EFFECT-ALGEBRA §5/R8; pinned
        // red-first by the effects ball, rules--29_r8_landing).
        "returning" | "stdout" => {
            return Err(DelightQLError::validation_error_categorized(
                "effect/pipe/landing",
                format!(
                    "{operation}! has exactly one higher-order parameter and the pipe \
                     already fills it — the argument leaves the pipe nowhere to land \
                     (EFFECT-ALGEBRA R8). Write `|> {operation}!(*)`, or use \
                     `|> returning_other!(rel(*))(*)` to return the OTHER relation."
                ),
                "pipe has nowhere to land",
            ));
        }
        // Known non-DML BUILT-IN directives build the two-paren pipe
        // invocation (returning_other!, run_namespace!, exit!, run! — the
        // TORTURE-TEST tail's `|> returning_other!(final_summary(*))(*)`);
        // their lowering is Epic-3 work. Constructed here so nothing is
        // silently dropped (REPORT-2.1 note 2); pinned by
        // `piped_two_paren_directive_builds_as_pipe_invocation`.
        //
        // UNKNOWN names stay a parse error: keep! is retired as an unknown
        // DML name with NO curated message (DECISION-MEMO-1.0 Q1 /
        // REPORT-1.5b; pinned by dml/dml_should_fail--37_keep_removed's
        // error://parse/general annotation). The piped two-paren form of a
        // USER effect rule lands with the effect transformer (Epic 3), which
        // can then migrate this refusal deliberately.
        "returning_other" | "run_namespace" | "exit" | "run" => {
            return parse_directive_pipe_invocation(node, input, features)
        }
        // DDL directives take their TARGET as a relational designator —
        // a preserved higher-order term, never a string (Phase 3,
        // canonical invocation): source |> table!(my::ns.dump_table(*))(*).
        "table" | "temp_table" | "temp_view" => {
            return parse_directive_pipe_invocation(node, input, features)
        }
        _ => {
            return Err(DelightQLError::parse_error(format!(
                "Unknown DML operation: {}!. Expected update!, delete!, or insert!",
                operation
            )))
        }
    }
}

/// Parse a piped two-paren directive invocation: `source |> name!(Rel(*))(spec)`
/// (EFFECT-ALGEBRA §1: first parens = parameters, trailing parens = access).
/// Reached through the DML-shaped bang_pipe alternative for non-DML names —
/// e.g. `… |> returning_other!(final_summary(*))(*)` (TORTURE-TEST tail).
/// Produces `UnaryRelationalOperator::DirectivePipeInvocation`; consumed by
/// the effect transformer (Epic 3).
fn parse_directive_pipe_invocation(
    node: CstNode,
    input: RelationalExpression,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    features.mark(crate::pipeline::query_features::QueryFeature::PseudoPredicates);

    let operation = node
        .field_text("operation")
        .ok_or_else(|| DelightQLError::parse_error("No operation in directive pipe invocation"))?;
    let full_name = format!("{}!", operation);

    let argument = if let Some(target_node) = node.field("target") {
        relations::parse_table_access(target_node, features)?
    } else if let Some(anon_node) = node.field("anon_target") {
        RelationalExpression::Relation(relations::parse_anonymous_table(anon_node, features)?)
    } else {
        return Err(DelightQLError::parse_error(
            "No argument in directive pipe invocation",
        ));
    };

    let domain_spec = if let Some(columns_node) = node.field("columns") {
        relations::parse_column_spec(columns_node, features)?
    } else {
        DomainSpec::Glob
    };

    Ok(RelationalExpression::Pipe(Box::new(
        stacksafe::StackSafe::new(PipeExpression {
            source: input,
            operator: UnaryRelationalOperator::DirectivePipeInvocation {
                name: full_name,
                argument: Box::new(argument),
                domain_spec,
            },
            cpr_schema: PhaseBox::phantom(),
        }),
    )))
}

/// Parse the two-parenthesis directive invocation: `name!(args)(access)`.
///
/// CANONICAL INVOCATION (DIRECTIVE-CONVERGENCE-PLAN Phase 3): the first
/// parentheses are the directive's ARGUMENTS and the second are the
/// returned-relation ACCESS specification. This replaces the historical
/// "inline directive table" desugaring (`_(values) |> name!(spec)`), which
/// modeled the first parens as a synthetic anonymous input table and the
/// second as invocation arguments — the faux higher-order form the audit
/// confirmed (`enlist!("ns")(success)` bound `success` against the
/// synthetic input header `col0`). A relational input arrives through a
/// pipe; it is never spelled inside the argument parentheses.
pub(in crate::pipeline::builder_v2) fn parse_inline_directive_table(
    node: CstNode,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    use crate::pipeline::asts::core::expressions::DomainSpec;
    use crate::pipeline::query_features::QueryFeature;

    features.mark(QueryFeature::PseudoPredicates);

    let name = node
        .field_text("name")
        .ok_or_else(|| DelightQLError::parse_error("No name in directive invocation"))?;
    let full_name = format!("{}!", name);

    // Qualified invocation (Phase 2 identity): std::prelude.enlist!(…)(…).
    let namespace: Vec<String> = node
        .field("namespace_path")
        .map(|ns| ns.text().split("::").map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();

    // First parens: the ordered argument list. The grammar shape still
    // admits `;`-separated rows (the historical table-value spelling);
    // more than one row is refused with a teaching diagnostic because
    // arguments are not a table.
    let table_value = node
        .field("table_value")
        .ok_or_else(|| DelightQLError::parse_error("No arguments in directive invocation"))?;
    let mut rows: Vec<Vec<DomainExpression>> = Vec::new();
    for group in table_value.children() {
        if group.kind() != "ho_argument_group" {
            continue;
        }
        for row_node in group.children() {
            if row_node.kind() != "ho_argument_row" {
                continue;
            }
            let mut values = Vec::new();
            for arg in row_node.children() {
                if arg.kind() == "tvf_argument" {
                    values.push(relations::parse_tvf_argument_as_domain_expression(arg)?);
                }
            }
            if !values.is_empty() {
                rows.push(values);
            }
        }
    }
    if rows.len() > 1 {
        return Err(DelightQLError::validation_error_categorized(
            "directive/invocation/arguments",
            format!(
                "the first parentheses of {name}!(…)(…) are its arguments, \
                 not a table — a relational input arrives through a pipe: \
                 rows |> {name}!(…)(*)"
            ),
            "directive arguments",
        ));
    }
    let arguments = rows.pop().unwrap_or_default();

    // Second parens: returned-relation access.
    let access = if let Some(cols) = node.field("columns") {
        relations::parse_column_spec(cols, features)?
    } else {
        // Interior continuation in access position — not yet ruled for the
        // two-paren form; treat as full access.
        DomainSpec::Glob
    };

    Ok(RelationalExpression::Relation(Relation::PseudoPredicate {
        name: full_name,
        namespace,
        arguments,
        access,
        alias: None,
        cpr_schema: PhaseBox::phantom(),
    }))
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
