// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

use crate::diagnostic::Internal;
use crate::error::Result;
use crate::pipeline::asts::ddl::{
    Clause, DefKind, HeadItems, HoColumnKind, HoGroundPattern, HoParam, HoPositionInfo,
};

/// Compute cross-clause unified position analysis for all HO parameter positions.
///
/// For each position 0..max_params across all clauses:
/// - Determines column_kind: Glob/Argumentative/Scalar
/// - Records scalar ground-pattern evidence across the complete clause set
/// - Labels scalar inputs by position and retains their binders with clause ownership
pub(crate) fn build_ho_position_analysis(
    group: &crate::pipeline::asts::ddl::DefinitionGroup,
) -> Vec<HoPositionInfo> {
    if group.kind() != DefKind::HoView {
        return Vec::new();
    }
    let heads: Vec<&[HoParam]> = group.clauses().iter().map(Clause::params).collect();

    build_ho_position_analysis_from_heads(&heads)
}

/// Build position analysis from every head of one family. Private: an
/// analysis over fewer heads than the family has would be a clause's row
/// posing as the family's.
fn build_ho_position_analysis_from_heads(heads: &[&[HoParam]]) -> Vec<HoPositionInfo> {
    if heads.is_empty() {
        return Vec::new();
    }

    let max_params = heads.iter().map(|h| h.len()).max().unwrap_or(0);
    let mut positions = Vec::with_capacity(max_params);

    for pos in 0..max_params {
        let mut has_glob = false;
        let mut has_argumentative = false;
        let mut arg_columns: Option<Vec<delightql_types::SqlIdentifier>> = None;
        let mut has_scalar = false;
        let mut has_ground_scalar = false;
        let mut rule_signatures = Vec::new();
        let mut column_name: Option<delightql_types::SqlIdentifier> = None;

        for head in heads.iter() {
            if let Some(param) = head.get(pos) {
                match param {
                    HoParam::Relation {
                        name,
                        cols: HeadItems::Glob,
                    } => {
                        has_glob = true;
                        // Glob contributes the declared name (table parameter name, e.g., "T")
                        if column_name.is_none() {
                            column_name = Some(name.clone());
                        }
                    }
                    HoParam::Relation {
                        name,
                        cols: HeadItems::Listed(cols),
                    } => {
                        has_argumentative = true;
                        if arg_columns.is_none() {
                            arg_columns = Some(
                                cols.iter()
                                    .map(|c| match &c.supply {
                                        crate::pipeline::asts::core::definitions::Supply::Ref(
                                            name,
                                        ) => name.clone(),
                                        other => {
                                            delightql_types::SqlIdentifier::new(other.spelling())
                                        }
                                    })
                                    .collect(),
                            );
                        }
                        // Argumentative contributes the declared name (table parameter name)
                        if column_name.is_none() {
                            column_name = Some(name.clone());
                        }
                    }
                    HoParam::Scalar { .. } => {
                        has_scalar = true;
                    }
                    HoParam::Rule { name, signature } => {
                        rule_signatures.push(signature);
                        if column_name.is_none() {
                            column_name = Some(name.clone());
                        }
                    }
                    HoParam::Ground { .. } => {
                        has_ground_scalar = true;
                        // A ground member selects its argument position; its
                        // literal never names a family input.
                    }
                }
            }
        }

        // The family's clauses agree on one rule-valued contract at the
        // position: the family-signature judgment refused them otherwise
        // where the family was declared.
        let column_kind = if let Some(signature) = rule_signatures.first() {
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

        if column_kind == HoColumnKind::Scalar {
            column_name = Some(crate::pipeline::asts::core::definitions::argument_name(pos));
        }
        positions.push(HoPositionInfo {
            position: pos,
            column_kind,
            ground_pattern,
            column_name,
        });
    }

    positions
}

/// THE FAMILY'S DECLARED ROW: what a definition family declares at each
/// parameter position, judged across EVERY clause. One clause's head is only
/// that clause's pattern row — a ground member and a free binder may share a
/// position, and clause order is the author's — so no clause stands for the
/// family. This value is built only from a complete group, and it is the one
/// place a family's declared row is read from.
#[derive(Debug, Clone)]
pub(crate) struct FamilySignature {
    params: Vec<HoParam>,
}

impl FamilySignature {
    /// The declared row of `group`, from all of its clauses.
    pub(crate) fn of(group: &crate::pipeline::asts::ddl::DefinitionGroup) -> Result<Self> {
        let clauses = group.clauses();
        let heads: Vec<&[HoParam]> = clauses.iter().map(Clause::params).collect();
        let positions = build_ho_position_analysis_from_heads(&heads);
        let params = call_row(clauses, &positions)?;
        Ok(FamilySignature { params })
    }

    /// The call row: one callable formal per position. A position some
    /// clauses ground and others bind is a scalar formal; the clause patterns
    /// apply only after admission.
    pub(crate) fn params(&self) -> &[HoParam] {
        &self.params
    }
}

/// Project each analyzed position into its one callable formal. The source
/// parameter a formal copies its shape from is searched for across every
/// clause, never taken from the first.
fn call_row(clauses: &[Clause], positions: &[HoPositionInfo]) -> Result<Vec<HoParam>> {
    let source_at = |position: usize, accepts: fn(&HoParam) -> bool| {
        clauses
            .iter()
            .filter_map(|clause| clause.params().get(position))
            .find(|param| accepts(param))
    };
    let missing = |position: usize| {
        Internal::invariant(
            "grounding::call_row",
            format!("the family has no source parameter for analyzed position {position}"),
        )
    };

    positions
        .iter()
        .map(|position| {
            let name = position
                .column_name
                .as_ref()
                .cloned()
                .ok_or_else(|| missing(position.position))?;
            match (&position.column_kind, &position.ground_pattern) {
                (HoColumnKind::TableGlob, _) => Ok(HoParam::Relation {
                    name,
                    cols: HeadItems::Glob,
                }),
                (HoColumnKind::TableArgumentative(_), _) => {
                    let HoParam::Relation { cols, .. } = source_at(position.position, |param| {
                        matches!(
                            param,
                            HoParam::Relation {
                                cols: HeadItems::Listed(_),
                                ..
                            }
                        )
                    })
                    .ok_or_else(|| missing(position.position))?
                    else {
                        unreachable!("the source predicate admits only listed relations")
                    };
                    Ok(HoParam::Relation {
                        name,
                        cols: cols.clone(),
                    })
                }
                (HoColumnKind::Rule(signature), _) => Ok(HoParam::Rule {
                    name,
                    signature: signature.clone(),
                }),
                (HoColumnKind::Scalar, Some(HoGroundPattern::AllClauses)) => {
                    let HoParam::Ground { text, .. } = source_at(position.position, |param| {
                        matches!(param, HoParam::Ground { .. })
                    })
                    .ok_or_else(|| missing(position.position))?
                    else {
                        unreachable!("the source predicate admits only ground parameters")
                    };
                    Ok(HoParam::Ground {
                        name,
                        text: text.clone(),
                    })
                }
                (HoColumnKind::Scalar, None | Some(HoGroundPattern::SomeClauses)) => {
                    let HoParam::Scalar { callable, .. } = source_at(position.position, |param| {
                        matches!(param, HoParam::Scalar { .. })
                    })
                    .ok_or_else(|| missing(position.position))?
                    else {
                        unreachable!("the source predicate admits only scalar parameters")
                    };
                    Ok(HoParam::Scalar {
                        name,
                        guard: None,
                        callable: *callable,
                    })
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod family_signature_tests {
    //! THE DECLARED ROW IS THE FAMILY'S. The same clauses in either order
    //! give one row and the same ground evidence. A ground member fixes a
    //! value position, and the head law keeps each position one role across
    //! the family, so no row is read from a family that disagrees.

    use super::FamilySignature;
    use crate::ddl::reconstruct;
    use crate::pipeline::asts::ddl::HoParam;

    fn signature(clauses: &[&str]) -> FamilySignature {
        let group = reconstruct::group(&clauses.join("\n")).expect("the group reconstructs");
        FamilySignature::of(&group).expect("the row projects")
    }

    const GROUND: &str = "bump!(1)(*) :- _(n @ 100) |> insert!(log(*))(*)";
    const FREE: &str = "bump!(k)(*) :- _(n @ $.k) |> insert!(log(*))(*)";

    #[test]
    fn clause_order_does_not_change_the_declared_row() {
        let ground_first = signature(&[GROUND, FREE]);
        let ground_second = signature(&[FREE, GROUND]);
        assert_eq!(ground_first.params(), ground_second.params());
        assert!(matches!(
            ground_first.params(),
            [HoParam::Scalar { name, .. }] if name.as_str() == "argument 1"
        ));
    }

    #[test]
    fn a_ground_member_beside_a_relation_formal_never_assembles() {
        let relation = "put!(T(*))(*) :- T(*) |> insert!(log(*))(*)";
        let ground = "put!(1)(*) :- _(n @ 100) |> insert!(log(*))(*)";
        for clauses in [[relation, ground], [ground, relation]] {
            let group = reconstruct::group(&clauses.join("\n")).expect("the head assembler judges no parameter row");
            let err = crate::pipeline::middle::api::judge_declared_family(&group, true)
                .expect_err("the roles disagree where the family is declared");
            assert_eq!(
                err.error_uri(),
                "delightql-error://semantic/ddl/head/param_arity"
            );
        }
    }

}
