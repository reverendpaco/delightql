// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Grounding support: function inlining and view expansion
//!
//! When a query uses the grounding operator (^), consulted definitions from
//! grounded namespaces are applied at the unresolved AST level before normal
//! resolution proceeds.
//!
//! **Function inlining**: `double:(x) :- x * 2` in namespace `lib::math` causes
//! `data::test^lib::math.users(*) |> (first_name, double:(balance) as doubled)` to become
//! `... |> (first_name, (balance * 2) as doubled)` before resolution.
//!
//! **View expansion**: `high_balance(*) :- users(*), balance > 1000` causes
//! `data::test^lib::views.high_balance(*)` to expand into the view body with
//! unqualified table references patched to use the data namespace.

use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::Unresolved;
use crate::pipeline::asts::ddl::{
    Clause, DefKind, HeadItems, HoColumnKind, HoGroundPattern, HoParam, HoPositionInfo,
};

// ============================================================================
// Multi-clause selection synthesis
// ============================================================================

/// Assemble a `ClauseSelection` from multiple guarded function clauses,
/// leaving parameter Lvars intact (no substitution).
///
/// THE SYNTHESIZED SELECTION IS ITS OWN SHAPE: the arms carry clause BODIES
/// under the clause's own guard. The authored `CaseExpression` is a
/// different carrier — an author wrote it — so neither is spelled with the
/// other's type.
///
/// Used when converting multi-clause DDL functions into CfeDefinitions; the
/// formals stand as ordinary named references until the frame answers them.
pub(crate) fn build_case_body_from_clauses(
    name: &str,
    clauses: Vec<Clause>,
) -> Result<ast_unresolved::DomainExpression> {
    let mut arms: Vec<crate::pipeline::asts::core::ClauseArm<Unresolved>> = Vec::new();

    for clause in &clauses {
        let params = clause.params();

        // A CLAUSE'S BODY IS WHAT IT COMPUTES.
        let body = clause.as_scalar_body().ok_or_else(|| {
            DelightQLError::parse_error(format!(
                "Expected scalar body for multi-clause function '{name}', got relational"
            ))
        })?;

        let guard = params.iter().find_map(|p| match p {
            HoParam::Scalar { guard, .. } => guard.as_ref(),
            _ => None,
        });
        arms.push(crate::pipeline::asts::core::ClauseArm {
            guard: guard.cloned(),
            result: body.clone(),
        });
    }

    Ok(ast_unresolved::DomainExpression::Application(
        ast_unresolved::FunctionApplication::ClauseSelection(
            crate::pipeline::asts::core::ClauseSelection { arms },
        ),
    ))
}

// ============================================================================
// Parameter substitution (used by sigma predicates)
// ============================================================================

/// The window builtins' signature judgment — the ONE authority, consulted
/// from the ordinary Standard-application road for authored and rebuilt
/// invocations alike. The keyword "function" is the refusal's badge.
pub(super) fn judge_window_row(
    fold: &super::resolver_fold::ResolverFold,
    callee_name: &str,
    supplied: usize,
) -> Result<()> {
    let Some((min, max)) = fold.core.built_in.window_signature(callee_name) else {
        return Ok(());
    };
    if supplied < min as usize || supplied > max as usize {
        return Err(DelightQLError::parse_error(format!(
            "the window function '{callee_name}' takes {} argument{}; the invocation hands it {supplied}",
            if min == max {
                min.to_string()
            } else {
                format!("{min} to {max}")
            },
            if max == 1 { "" } else { "s" },
        )));
    }
    Ok(())
}

/// Compute cross-clause unified position analysis for all HO parameter positions.
///
/// For each position 0..max_params across all clauses:
/// - Determines column_kind: Glob/Argumentative/Scalar
/// - Records scalar ground-pattern evidence across the complete clause set
/// - Collects ground_values: Vec<(ordinal, value)>
/// - Determines column_name: from free-variable clauses (must agree)
///
/// This replaces `extract_ground_scalar_info()` + `validate_mixed_ground_params()`
/// with a single, complete analysis computed at consult time.
pub(crate) fn build_ho_position_analysis(
    group: &crate::pipeline::asts::ddl::DefinitionGroup,
) -> Vec<HoPositionInfo> {
    if group.kind() != DefKind::HoView {
        return Vec::new();
    }
    let heads: Vec<&[HoParam]> = group.clauses().iter().map(Clause::params).collect();

    build_ho_position_analysis_from_heads(&heads)
}

/// Build position analysis from a set of HO head param lists.
///
/// Accepts pre-extracted heads so callers that only have heads (not whole
/// clauses) can use this directly — e.g., the deferred-body HO view path in
/// system.rs where each clause's head is parsed individually.
pub(crate) fn build_ho_position_analysis_from_heads(heads: &[&[HoParam]]) -> Vec<HoPositionInfo> {
    if heads.is_empty() {
        return Vec::new();
    }

    let max_params = heads.iter().map(|h| h.len()).max().unwrap_or(0);
    let mut positions = Vec::with_capacity(max_params);

    for pos in 0..max_params {
        let mut has_glob = false;
        let mut has_argumentative = false;
        let mut arg_columns: Option<Vec<String>> = None;
        let mut has_scalar = false;
        let mut has_ground_scalar = false;
        let mut rule_signatures = Vec::new();
        let mut ground_values: Vec<(usize, String)> = Vec::new();
        let mut column_name: Option<String> = None;

        for (clause_ordinal, head) in heads.iter().enumerate() {
            if let Some(param) = head.get(pos) {
                match param {
                    HoParam::Relation {
                        name,
                        cols: HeadItems::Glob,
                    } => {
                        has_glob = true;
                        // Glob contributes canonical name (table parameter name, e.g., "T")
                        if column_name.is_none() {
                            column_name = Some(name.to_string());
                        }
                    }
                    HoParam::Relation {
                        name,
                        cols: HeadItems::Listed(cols),
                    } => {
                        has_argumentative = true;
                        if arg_columns.is_none() {
                            arg_columns = Some(cols.iter().map(|c| c.supply.spelling()).collect());
                        }
                        // Argumentative contributes canonical name (table parameter name)
                        if column_name.is_none() {
                            column_name = Some(name.to_string());
                        }
                    }
                    HoParam::Scalar { name, .. } => {
                        has_scalar = true;
                        // Free variable — contributes canonical name
                        if column_name.is_none() {
                            column_name = Some(name.to_string());
                        }
                    }
                    HoParam::Rule { name, signature } => {
                        rule_signatures.push(signature);
                        if column_name.is_none() {
                            column_name = Some(name.to_string());
                        }
                    }
                    HoParam::Ground { text, .. } => {
                        has_ground_scalar = true;
                        ground_values.push((clause_ordinal, text.clone()));
                        // A ground position contributes no column NAME: its
                        // spelling is the literal. The canonical name comes
                        // from a sibling clause that binds the position.
                    }
                }
            }
        }

        let column_kind = if let Some(signature) = rule_signatures.first() {
            debug_assert!(rule_signatures
                .iter()
                .all(|candidate| candidate.same_shape(signature)));
            HoColumnKind::Rule((*signature).clone())
        } else if has_glob {
            HoColumnKind::TableGlob
        } else if has_argumentative {
            HoColumnKind::TableArgumentative(arg_columns.unwrap_or_default())
        } else {
            HoColumnKind::Scalar
        };

        let ground_pattern = if !matches!(column_kind, HoColumnKind::Scalar) {
            None
        } else if has_ground_scalar && !has_scalar {
            Some(HoGroundPattern::AllClauses)
        } else if has_ground_scalar && has_scalar {
            Some(HoGroundPattern::SomeClauses)
        } else {
            None
        };

        positions.push(HoPositionInfo {
            position: pos,
            column_kind,
            ground_pattern,
            ground_values,
            column_name,
        });
    }

    positions
}
pub(crate) use crate::pipeline::query_features::HoParamBindings;

fn bind_proffer_scope(
    bindings: &mut HoParamBindings,
    param_name: &str,
    identities: &crate::relation::Planning,
) -> crate::error::Result<()> {
    let scope = identities.authority().reserve_proffer();
    bindings
        .table_scope_params
        .insert(param_name.to_string(), scope);
    Ok(())
}

/// Create structural proffer bindings for an HO view's parameters.
///
/// Used at consult time to parse the view body with placeholder values,
/// enabling early validation of syntax and structure without real call-site args.
pub(crate) fn create_proffer_bindings(
    head: &crate::pipeline::asts::ddl::Head,
    identities: &crate::relation::Planning,
) -> crate::error::Result<HoParamBindings> {
    let mut bindings = HoParamBindings::default();
    for param in head.ho_params.as_deref().unwrap_or_default() {
        match param {
            HoParam::Relation {
                name,
                cols: HeadItems::Glob,
            } => {
                bind_proffer_scope(&mut bindings, name.as_str(), identities)?;
            }
            HoParam::Relation {
                name,
                cols: HeadItems::Listed(items),
            } => {
                let columns: Vec<String> = items.iter().map(|i| i.supply.spelling()).collect();
                let null_row: Vec<crate::pipeline::asts::core::LiteralValue> = columns
                    .iter()
                    .map(|_| crate::pipeline::asts::core::LiteralValue::Null)
                    .collect();
                match lift_scalars_to_anonymous_table(&columns, &[null_row]) {
                    Ok(anon) => {
                        bindings.table_expr_params.insert(name.to_string(), anon);
                    }
                    Err(_) => {
                        bind_proffer_scope(&mut bindings, name.as_str(), identities)?;
                    }
                }
            }
            HoParam::Scalar { name, .. } => {
                bindings.scalar_formals.insert(name.to_string());
                bind_proffer_scope(&mut bindings, name.as_str(), identities)?;
            }
            HoParam::Rule { .. } => {
                // A rule formal is answered only by a closed residual value
                // in the definition-use frame. It is neither a scalar
                // proffer nor a relation carrier.
            }
            HoParam::Ground { name, text } => {
                // A ground position is a constant, not a parameter.
                bindings.scalar_literals.insert(
                    name.to_string(),
                    crate::pipeline::asts::core::LiteralValue::from_stored_ground(text),
                );
            }
        }
    }
    Ok(bindings)
}

/// Synthesize an anonymous table `_(col1, col2 ---- v1, v2; v3, v4)` from column names and rows.
///
/// Routes through the DQL body parser — no mini-pipeline.
/// The lift's rows, headed by the names the parameter declares.
///
/// `None` for anything that is not a bare headerless literal: a relation the
/// author named, an interior, a membership form, or a table that already
/// carries its own header row. Those bind through the carrier, where a
/// reference has a scope to resolve in; only a self-contained literal can
/// stand in the body under a heading the DECLARATION supplies.
///
/// Widths that disagree are left alone, so the arity check reports the
/// mismatch against the relation the author wrote rather than against a
/// silently repaired one.
pub(crate) fn lifted_rows_under_declared_names(
    relation: &ast_unresolved::Chain,
    columns: &[String],
) -> Option<ast_unresolved::Chain> {
    if !relation.continuations().is_empty() {
        return None;
    }
    let ast_unresolved::GroundForm::Literal(table) = relation.head().form() else {
        return None;
    };
    if table.table.body.header.is_some() || table.alias.is_some() || table.outer {
        return None;
    }
    if table
        .table
        .body
        .rows
        .iter()
        .any(|row| row.len() != columns.len())
    {
        return None;
    }
    let headers: Vec<ast_unresolved::DomainExpression> = columns
        .iter()
        .map(|name| ast_unresolved::DomainExpression::lvar_builder(name.clone()).build())
        .collect();
    let mut headed = table.clone();
    headed.table.body.header = Some(crate::pipeline::asts::core::TabularRow(Box::new(
        crate::pipeline::asts::vocabulary::Vec1::try_from_vec(
            headers
                .into_iter()
                .map(|term| crate::pipeline::asts::core::HeaderItem {
                    slot: crate::pipeline::asts::core::Slot::classify(term),
                    sparse: false,
                })
                .collect(),
        )
        .expect("a declared heading is nonempty"),
    )));
    Some(ast_unresolved::Chain::authored(
        ast_unresolved::GroundForm::Literal(headed),
    ))
}

/// The anonymous table a lifted argument becomes: named columns and one row
/// per supplied tuple.
///
/// BUILT, NOT SPELLED. The values arrive as literals and the table is a
/// carrier; rendering them into `_(col ---- val)` text and parsing that back
/// would put a round trip through the grammar in the middle of a construction
/// that already has everything it needs — and would have to re-quote every
/// value correctly to survive it.
pub(crate) fn lift_scalars_to_anonymous_table(
    column_names: &[String],
    rows: &[Vec<crate::pipeline::asts::core::LiteralValue>],
) -> Result<ast_unresolved::Chain> {
    if let Some(row) = rows.iter().find(|row| row.len() != column_names.len()) {
        return Err(DelightQLError::parse_error(format!(
            "a lifted row carries {} value(s); the heading names {}",
            row.len(),
            column_names.len()
        )));
    }
    let column_headers = Some(
        column_names
            .iter()
            .map(|name| ast_unresolved::DomainExpression::lvar_builder(name.clone()).build())
            .collect(),
    );
    let rows = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| {
                    ast_unresolved::DomainExpression::Application(
                        ast_unresolved::FunctionApplication::Ground(value.clone()),
                    )
                })
                .collect()
        })
        .collect::<Vec<_>>();
    let table = crate::pipeline::asts::core::AnonTable::from_values(column_headers, rows)
        .ok_or_else(|| {
            DelightQLError::parse_error("a lifted table has a nonempty heading and body")
        })?;
    Ok(ast_unresolved::Chain::authored(
        ast_unresolved::GroundForm::Literal(crate::pipeline::asts::core::AnonRelation::plain(
            table,
        )),
    ))
}

#[cfg(test)]
mod clause_selection_tests {
    //! THE SYNTHESIZED SELECTION IS ITS OWN SHAPE.
    //!
    //! A multi-clause value rule assembles into `ClauseSelection`, whose arms
    //! carry what a CLAUSE computes — its body, an ordinary value, a crossed
    //! truth included. The authored CASE carrier is a different thing and is
    //! pinned separately.

    use super::build_case_body_from_clauses;
    use crate::ddl::reconstruct;
    use crate::pipeline::asts::core::{DomainExpression, FunctionApplication};

    /// The selection a source's clauses assemble into.
    fn selection(source: &str) -> crate::pipeline::asts::core::ClauseSelection {
        let group = reconstruct::group(source).expect("the group reconstructs");
        let body =
            build_case_body_from_clauses("f", group.into_clauses()).expect("the clauses assemble");
        match body {
            DomainExpression::Application(
                crate::pipeline::asts::core::FunctionApplication::ClauseSelection(selection),
            ) => selection,
            other => panic!("expected a clause selection, got {other:?}"),
        }
    }

    /// Whether an arm's result is a crossed truth.
    fn crossed(arm: &crate::pipeline::asts::core::ClauseArm) -> bool {
        matches!(
            arm.result,
            DomainExpression::Application(FunctionApplication::Crossed(_))
        )
    }

    /// BOTH CLAUSES CROSSED. An existence read as a value is a lawful
    /// value-rule body, so two of them are a lawful group.
    #[test]
    fn every_clause_may_compute_a_crossing() {
        let selection = selection(concat!(
            "served:(uid | uid > 5) :- +orders(, user_id = uid)\n",
            "served:(uid) :- +reviews(, user_id = uid)"
        ));
        assert_eq!(selection.arms.len(), 2);
        assert!(selection.arms.iter().all(crossed));
        // The guardless clause is the group's default, and there is one.
        assert_eq!(
            selection.arms.iter().filter(|a| a.guard.is_none()).count(),
            1
        );
    }

    /// MIXED IS ADMITTED. A clause computes a value either way, and the
    /// value-rule law does not tell a crossing from an ordinary value.
    #[test]
    fn clauses_may_mix_crossed_and_domain_results() {
        let selection = selection(concat!(
            "mixed:(uid | uid > 5) :- +orders(, user_id = uid)\n",
            "mixed:(uid) :- false"
        ));
        assert_eq!(selection.arms.len(), 2);
        assert!(crossed(&selection.arms[0]));
        assert!(!crossed(&selection.arms[1]));
    }

    /// The control: neither clause crossed, and the same shape carries them.
    #[test]
    fn a_domain_valued_group_uses_the_same_selection() {
        let selection = selection(concat!(
            "plain:(uid | uid > 5) :- \"high\"\n",
            "plain:(uid) :- \"low\""
        ));
        assert_eq!(selection.arms.len(), 2);
        assert!(!selection.arms.iter().any(crossed));
    }
}
