// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Join-specific resolution for anonymous relations.

use crate::error::Result;
use crate::names::{Addressing, ColId, Registry};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::ArgumentValue;
use crate::pipeline::asts::core::{AuthoredColumn, ColumnOccurrence};
use crate::pipeline::asts::core::{Comparison, Membership};
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::core::{Probe, ValueRow};
use crate::pipeline::asts::vocabulary::Vec2;
use delightql_types::SqlIdentifier;

fn named(registry: &Registry, column: ColId, name: &str) -> bool {
    let spelling = registry.intern(name, false);
    registry.published_sym(column) == Some(registry.canonical(spelling))
}

fn qualified_header_matches(
    registry: &Registry,
    column: ColId,
    name: &str,
    qualifier: &delightql_types::SqlIdentifier,
) -> bool {
    if !named(registry, column, name) {
        return false;
    }
    let spelling = registry.intern(qualifier.as_str(), qualifier.is_stropped());
    let qualifier = registry.canonical(spelling);
    !registry.qualified_glob(qualifier, &[column]).is_empty()
}

fn bare_header_matches(registry: &Registry, column: ColId, name: &str) -> bool {
    named(registry, column, name) && matches!(registry.addressing(column), Addressing::Bare)
}

pub(super) fn detect_anonymous_table_unification(
    headers: &[ast_unresolved::DomainExpression],
    left_columns: &[ColId],
    right_columns: &[ColId],
    registry: &Registry,
) -> Result<Option<ast_resolved::MemberCorrelation>> {
    let mut using_columns = Vec::new();
    let mut on_conditions = Vec::new();
    for (position, header) in headers.iter().enumerate() {
        match header {
            ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn {
                    name,
                    qualifier: Some(qualifier),
                    ..
                },
            ))) => {
                if left_columns.iter().any(|column| {
                    qualified_header_matches(registry, *column, name.as_str(), qualifier)
                }) {
                    using_columns.push(name.clone());
                } else {
                    return Err(crate::error::DelightQLError::validation_error_categorized(
                        "resolution/anon/qualifier",
                        format!(
                            "anonymous-table header '{}.{}' names no column in scope",
                            qualifier, name
                        ),
                        "the qualifier must name a visible relation containing the column",
                    ));
                }
            }
            ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn {
                    name,
                    qualifier: None,
                    ..
                },
            ))) => {
                // A bare header unifies with a BARE lvar of the same name and
                // with nothing else. Qualification is part of an lvar's
                // complete name (A NAME IS NOT AN ADDRESS): `city` and
                // `people.city` are two names, so they neither unify nor
                // collide — the relations cross, and this header introduces
                // a fresh lvar. Reading the shared final segment as a
                // collision would refuse a legal query.
                if left_columns
                    .iter()
                    .any(|column| bare_header_matches(registry, *column, name.as_str()))
                {
                    using_columns.push(name.clone());
                }
            }
            ast_unresolved::DomainExpression::Application(function) => {
                if let Some(condition) = extract_function_unification(
                    function,
                    left_columns,
                    right_columns,
                    position,
                    registry,
                )? {
                    on_conditions.push(condition);
                }
            }
            _ => return Ok(None),
        }
    }

    if !on_conditions.is_empty() {
        return Ok(Some(ast_resolved::MemberCorrelation::Condition(
            combine_conditions(on_conditions),
        )));
    }
    if using_columns.is_empty() {
        return Ok(None);
    }
    Ok(Some(correspond(&using_columns, registry)))
}

/// The correspondence a set of authored names asks for.
///
/// ONE ACT. Every road that names correspondence columns — the dequalifying
/// access, the unifying anonymous header, the positional pattern, `.*` —
/// reaches the member's correlation through here, so a strop is part of the
/// name in every one of them.
fn correspond(names: &[SqlIdentifier], registry: &Registry) -> ast_resolved::MemberCorrelation {
    ast_resolved::MemberCorrelation::Correspond(ast_resolved::Correspondence::new(
        names
            .iter()
            .map(|name| {
                let spelling = registry.intern(name.as_str(), name.is_stropped());
                registry.canonical(spelling)
            })
            .collect(),
    ))
}

pub(super) fn aliased_anon_would_unify(
    headers: &[ast_unresolved::DomainExpression],
    left_columns: &[ColId],
    registry: &Registry,
) -> Option<ast_resolved::MemberCorrelation> {
    let mut using = Vec::new();
    for header in headers {
        match header {
            ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn {
                    name,
                    qualifier: Some(qualifier),
                    ..
                },
            ))) if left_columns.iter().any(|column| {
                qualified_header_matches(registry, *column, name.as_str(), qualifier)
            }) =>
            {
                using.push(name.clone())
            }
            ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn {
                    name,
                    qualifier: None,
                    ..
                },
            ))) if left_columns
                .iter()
                .any(|column| bare_header_matches(registry, *column, name.as_str())) =>
            {
                using.push(name.clone())
            }
            ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Ground(_),
            ) => {}
            _ => return None,
        }
    }
    (!using.is_empty()).then(|| correspond(&using, registry))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_anon_membership(
    headers: Option<&[ast_unresolved::DomainExpression]>,
    correlation: &Option<ast_resolved::MemberCorrelation>,
    left_columns: &[ColId],
    resolved_right: &ast_resolved::Chain,
    alias: Option<&delightql_types::SqlIdentifier>,
    registry: &Registry,
) -> Result<Option<ast_resolved::TruthExpression>> {
    let headers = match headers {
        Some(headers) if !headers.is_empty() => headers,
        _ => return Ok(None),
    };

    let mut lvars = 0;
    let mut literals = 0;
    for header in headers {
        match header {
            ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn { .. },
            ))) => lvars += 1,
            ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Ground(_),
            ) => literals += 1,
            _ => return Ok(None),
        }
    }
    let unified = correlation
        .as_ref()
        .and_then(ast_resolved::MemberCorrelation::correspondence)
        .map_or(0, |correspondence| correspondence.columns.len());
    let membership = unified == lvars;
    if literals > 0 && !membership {
        return Err(crate::error::DelightQLError::validation_error_categorized(
            "resolution/anon/ground_mixed",
            "a ground membership header cannot be mixed with a fresh column",
            "unify every lvar or remove the ground header",
        ));
    }
    if literals == 0 && !membership {
        return Ok(None);
    }
    if alias.is_some() {
        return Err(crate::error::DelightQLError::validation_error_categorized(
            "resolution/anon/membership_alias",
            "a membership test exports no columns, so its alias names nothing",
            "drop the alias or make the anonymous table relational",
        ));
    }

    let mut probes = Vec::with_capacity(headers.len());
    for header in headers {
        match header {
            ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Ground(value),
            ) => {
                probes.push(ast_resolved::DomainExpression::Application(
                    ast_resolved::FunctionApplication::Ground(value.clone()),
                ));
            }
            ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn {
                    name, qualifier, ..
                },
            ))) => {
                let matches: Vec<_> = left_columns
                    .iter()
                    .copied()
                    .filter(|column| match qualifier {
                        Some(qualifier) => {
                            qualified_header_matches(registry, *column, name.as_str(), qualifier)
                        }
                        None => bare_header_matches(registry, *column, name.as_str()),
                    })
                    .collect();
                let [column] = matches.as_slice() else {
                    unreachable!("a unified membership header must have one structural binding")
                };
                probes.push(ast_resolved::DomainExpression::Reference(Reference::Named(
                    NamedReference(ColumnOccurrence {
                        column: *column,
                        explicit_qualifier: false,
                    }),
                )));
            }
            _ => unreachable!("membership shape admitted a non-probe header"),
        }
    }

    let rows = match (
        &resolved_right.head,
        resolved_right.continuations.is_empty(),
    ) {
        (ast_resolved::Grelex::Literal(anon), true) => &anon.table.body.rows,
        _ => return Ok(None),
    };
    for (position, row) in rows.iter().enumerate() {
        if row.len() != probes.len() {
            return Err(crate::error::DelightQLError::validation_error_categorized(
                "resolution/anon/membership_arity",
                format!(
                    "membership row {} has {} value(s) for {} header(s)",
                    position + 1,
                    row.len(),
                    probes.len()
                ),
                "every candidate row must match the probe width",
            ));
        }
    }
    // The probe SAYS its width and each candidate keeps its own row, so the
    // arity checked above is the arity the lowering reads.
    let probe = if probes.len() == 1 {
        Probe::Value(Box::new(probes.pop().expect("one probe")))
    } else {
        Probe::Row(
            Vec2::try_from_vec(probes).expect("a multi-header probe has at least two values"),
        )
    };
    // A membership tests at least one candidate. The grammar's `;`-separated
    // grid supplies one, so this refusal names a state no author can write
    // rather than assigning "membership in nothing" a truth value.
    let rows = rows.clone().try_map(
        |row| -> Result<ValueRow<crate::pipeline::asts::core::Resolved>> {
            Ok(ValueRow((*row.0).map(ast_resolved::Datum::into_value)))
        },
    )?;
    Ok(Some(ast_resolved::TruthExpression::Membership(
        Membership {
            probe,
            rows,
            negated: false,
            source: crate::pipeline::asts::core::MembershipSource::In,
        },
    )))
}

fn extract_function_unification(
    function: &ast_unresolved::FunctionApplication,
    left_columns: &[ColId],
    right_columns: &[ColId],
    position: usize,
    registry: &Registry,
) -> Result<Option<ast_resolved::TruthExpression>> {
    // The unifying shape is a PLAIN application of one column. A guard or a
    // window is scalar context this rewrite has no place to put, so an
    // application carrying one is not this shape.
    let (reference, arguments) = match function {
        ast_unresolved::FunctionApplication::Standard(application)
            if application.guard.is_none() && application.window.is_none() =>
        {
            (&application.call().callee, &application.call().arguments)
        }
        _ => return Ok(None),
    };
    let [ast_unresolved::ScalarArgument::Value(ArgumentValue::Domain {
        value:
            ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn {
                    name: column_name,
                    qualifier,
                    ..
                },
            ))),
        ..
    })] = arguments.scalar_members()
    else {
        return Ok(None);
    };
    let matches: Vec<_> = left_columns
        .iter()
        .copied()
        .filter(|column| match qualifier {
            Some(qualifier) => {
                qualified_header_matches(registry, *column, column_name.as_str(), qualifier)
            }
            None => named(registry, *column, column_name.as_str()),
        })
        .collect();
    let [left] = matches.as_slice() else {
        return Ok(None);
    };
    let Some(right) = right_columns.get(position).copied() else {
        return Ok(None);
    };
    let left_function = ast_resolved::FunctionApplication::Standard(
        crate::pipeline::asts::core::StandardApplication::plain(
            crate::pipeline::asts::core::PureCall::from_inner(ast_resolved::FunctorCall {
                callee: reference.written_call_identity(registry),
                arguments: ast_resolved::CallArguments::Scalar(vec![
                    ast_resolved::ScalarArgument::plain(
                        ast_resolved::DomainExpression::Reference(Reference::Named(
                            NamedReference(ColumnOccurrence {
                                column: *left,
                                explicit_qualifier: false,
                            }),
                        )),
                    ),
                ]),
                marks: Default::default(),
            }),
        ),
    );
    Ok(Some(ast_resolved::TruthExpression::Comparison(
        Comparison {
            operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
            left: Box::new(ast_resolved::DomainExpression::Application(left_function)),
            right: Box::new(ast_resolved::DomainExpression::Reference(Reference::Named(
                NamedReference(ColumnOccurrence {
                    column: right,
                    explicit_qualifier: false,
                }),
            ))),
        },
    )))
}

fn combine_conditions(
    conditions: Vec<ast_resolved::TruthExpression>,
) -> ast_resolved::TruthExpression {
    ast_resolved::TruthExpression::all(conditions)
        .expect("caller only combines a non-empty condition list")
}

pub(super) fn create_using_condition(
    columns: &[SqlIdentifier],
    registry: &Registry,
) -> Result<ast_resolved::MemberCorrelation> {
    Ok(correspond(columns, registry))
}

/// The join `.*` asks for: USING over every name both sides publish.
///
/// It answers with the same construct the spelled-out `.(a, b)` answers
/// with, because they are one operator with two spellings. A conjunction of
/// equalities would join the same rows and publish a DIFFERENT heading —
/// USING merges the column it joined on, an ON does not — so the two
/// spellings would disagree about what the join publishes.
pub(super) fn create_using_all_condition(
    left_columns: &[ColId],
    right_columns: &[ColId],
    registry: &Registry,
) -> Result<ast_resolved::MemberCorrelation> {
    let columns = shared_using_names(left_columns, right_columns, registry)?
        .into_iter()
        .map(|pair| pair.name)
        .collect();
    Ok(ast_resolved::MemberCorrelation::Correspond(
        ast_resolved::Correspondence::new(columns),
    ))
}

/// One name both sides publish, and the occurrence of it on each.
pub(in crate::pipeline::resolver) struct SharedName {
    pub name: crate::names::Sym,
    pub left: ColId,
    pub right: ColId,
}

/// THE NAMES `.*` ASKS FOR — the one computation, for every placement.
///
/// `.*` renames every name it can, so what it asks for is exactly the set
/// both headings publish. The set is the same question at a join and inside
/// a correlated interior, and answering it twice is how the two placements
/// come to disagree about the same operator.
///
/// EMPTY IS NOT AN ANSWER. `.*` asks for every shared name; when there is
/// none, the step it wrote cannot be performed. Returning zero of them would
/// be read as a completed step — a cross join at one placement and an
/// uncorrelated relation at the other, both silent, both wrong.
pub(in crate::pipeline::resolver) fn shared_using_names(
    left_columns: &[ColId],
    right_columns: &[ColId],
    registry: &Registry,
) -> Result<Vec<SharedName>> {
    let mut seen = Vec::new();
    let mut shared: Vec<SharedName> = Vec::new();
    for right in right_columns.iter().copied() {
        let Some(name) = registry.published_sym(right) else {
            continue;
        };
        if seen.contains(&name) {
            return Err(crate::error::DelightQLError::validation_error_categorized(
                "using/all/ambiguous-right",
                "USING all found more than one right-side column with the same name",
                "rename or project the right relation to a unique heading",
            ));
        }
        seen.push(name);
        let matches: Vec<_> = left_columns
            .iter()
            .copied()
            .filter(|left| registry.published_sym(*left) == Some(name))
            .collect();
        let left = match matches.as_slice() {
            [] => continue,
            [left] => *left,
            _ => {
                return Err(crate::error::DelightQLError::validation_error_categorized(
                    "using/all/ambiguous-left",
                    "USING all found more than one left-side column with the same name",
                    "rename or project the left relation to a unique heading",
                ))
            }
        };
        shared.push(SharedName { name, left, right });
    }
    if shared.is_empty() {
        return Err(crate::error::DelightQLError::validation_error_categorized(
            "using/all/no-shared-columns",
            "No shared columns between left side and right side for .* (USING all)",
            ".* requires at least one column name in common",
        ));
    }
    Ok(shared)
}
