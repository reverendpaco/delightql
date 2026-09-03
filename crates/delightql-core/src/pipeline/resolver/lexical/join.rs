// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! AN ANONYMOUS RIGHT MEMBER'S HEADERS, JUDGED AGAINST THE LEFT ROW.
//!
//! Private to the lexical authority. A qualified header is decided by the
//! same terminal judgment every qualified reference meets — over the left
//! row's own frontier — and lands on the left row's position; a bare
//! header reuses by the one bare-reuse law. Nothing here receives a
//! candidate list to finish a lookup with, and no refusal the judgment
//! earns is read as absence.

use super::lookup::{live_bare_reuse, unify_single_column, written_name};
use super::Frontier;
use crate::error::Result;
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::ArgumentValue;
use crate::pipeline::asts::core::{AuthoredColumn, ColumnOccurrence};
use crate::pipeline::asts::core::{Comparison, Membership};
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::core::{Probe, ValueRow};
use crate::pipeline::asts::vocabulary::Vec2;
use crate::pipeline::resolver::unification::{ColumnReference, UnificationResult};
use crate::relation::PortId;
use delightql_types::SqlIdentifier;

fn not_exact() -> crate::error::DelightQLError {
    crate::error::DelightQLError::validation_error_categorized(
        "resolution/correspondence/not-exact",
        "a correspondence name does not select exactly one port in each operand",
        "project or rename each operand to a unique heading",
    )
}

/// THE POSITION OF THE LEFT ROW A QUALIFIED HEADER NAMES.
///
/// THE QUALIFIER REACHES THE LEXICAL BINDING, not only the heading standing
/// here. `products(…, price, …) as p` binds `price` under the alias's own
/// scope; a join above it republishes that position into the join's, and
/// the republication carries no answering name of its own. So the header
/// is decided the way every other qualified reference is — the frontier's
/// terminal judgment over the offered columns AND the visible bindings —
/// and the position standing here where the addressed occurrence stood is
/// the answer. One road answers `p.price`; there is not a second one for
/// headers. Absence answers `None`; ambiguity, opacity and every other
/// refusal are the refusals they are.
fn qualified_header_position(
    visible: &Frontier,
    left_columns: &[PortId],
    name: &SqlIdentifier,
    qualifier: &SqlIdentifier,
    registry: &crate::relation::Planning,
) -> Result<Option<PortId>> {
    let reference = ColumnReference::Named {
        name: name.clone(),
        qualifier: Some(qualifier.clone()),
    };
    match unify_single_column(
        reference,
        left_columns,
        &visible.bindings(registry),
        registry,
    ) {
        UnificationResult::Resolved(addressed) => Ok(left_columns.iter().copied().find(|column| {
            *column == addressed.column
                || crate::relation::stands_where(registry, *column, addressed.column)
        })),
        UnificationResult::Unresolved(_) => Ok(None),
        UnificationResult::Ambiguous { .. } => Err(not_exact()),
        UnificationResult::Opaque => Err(crate::pipeline::resolver::opaque_reference_refusal()),
        UnificationResult::Refused(refusal) => Err(refusal.into_error()),
    }
}

/// The exactly-one position of the anonymous relation's OWN interface a
/// header name publishes. Landed at resolution, while the complete ordered
/// interface is in hand; a repeated name refuses rather than guessing.
fn own_position(
    registry: &crate::relation::Planning,
    right_columns: &[PortId],
    name: &SqlIdentifier,
) -> Result<PortId> {
    let wanted = written_name(name, registry);
    let hits: Vec<_> = right_columns
        .iter()
        .copied()
        .filter(|port| registry.published_sym(port.column()) == Some(wanted))
        .collect();
    match hits.as_slice() {
        [one] => Ok(*one),
        _ => Err(not_exact()),
    }
}

pub(super) fn detect_anonymous_table_unification(
    headers: &[ast_unresolved::DomainExpression],
    left_columns: &[PortId],
    right_columns: &[PortId],
    visible: &Frontier,
    registry: &crate::relation::Planning,
) -> Result<Option<ast_resolved::MemberCorrelation>> {
    let mut pairs: Vec<crate::relation::form::MergedKey> = Vec::new();
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
                match qualified_header_position(visible, left_columns, name, qualifier, registry)? {
                    None => {
                        return Err(crate::error::DelightQLError::validation_error_categorized(
                            "resolution/anon/qualifier",
                            format!(
                                "anonymous-table header '{}.{}' names no column in scope",
                                qualifier, name
                            ),
                            "the qualifier must name a visible relation containing the column",
                        ))
                    }
                    Some(left) => pairs.push(crate::relation::form::MergedKey {
                        left,
                        right: own_position(registry, right_columns, name)?,
                    }),
                }
            }
            ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn {
                    name,
                    qualifier: None,
                    ..
                },
            ))) => {
                // A bare header unifies with a BARE lvar of the same complete
                // name and with nothing else. Qualification is part of an
                // lvar's complete name (A NAME IS NOT AN ADDRESS): `city` and
                // `people.city` are two names, so they neither unify nor
                // collide — the relations cross, and this header introduces
                // a fresh lvar. Reading the shared final segment as a
                // collision would refuse a legal query. The reuse is EXACT:
                // the pair records the one live bare port and the header's
                // own position, decided here with both interfaces in hand.
                if let Some(left) = live_bare_reuse(left_columns, name, registry)? {
                    pairs.push(crate::relation::form::MergedKey {
                        left,
                        right: own_position(registry, right_columns, name)?,
                    });
                }
            }
            ast_unresolved::DomainExpression::Application(function) => {
                if let Some(condition) = extract_function_unification(
                    function,
                    left_columns,
                    right_columns,
                    position,
                    visible,
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
    if pairs.is_empty() {
        return Ok(None);
    }
    Ok(Some(ast_resolved::MemberCorrelation::Correspond(
        ast_resolved::Correspondence::new(pairs),
    )))
}

/// The correspondence a set of authored names asks for.
///
/// ONE ACT. Every road that names correspondence columns — the dequalifying
/// access, the unifying anonymous header, the positional pattern, `.*` —
/// reaches the member's correlation through here, so a strop is part of the
/// name in every one of them.
fn correspond(
    names: &[SqlIdentifier],
    left: &[PortId],
    right: &[PortId],
    registry: &crate::relation::Planning,
) -> Result<ast_resolved::MemberCorrelation> {
    let names = names.iter().map(|name| written_name(name, registry));
    Ok(ast_resolved::MemberCorrelation::Correspond(
        ast_resolved::Correspondence::between(names, left, right, registry)?,
    ))
}

pub(super) fn aliased_anon_would_unify(
    headers: &[ast_unresolved::DomainExpression],
    left_columns: &[PortId],
    right_columns: &[PortId],
    visible: &Frontier,
    registry: &crate::relation::Planning,
) -> Result<Option<ast_resolved::MemberCorrelation>> {
    let mut pairs: Vec<crate::relation::form::MergedKey> = Vec::new();
    for header in headers {
        match header {
            ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn {
                    name,
                    qualifier: Some(qualifier),
                    ..
                },
            ))) => {
                if let Some(left) =
                    qualified_header_position(visible, left_columns, name, qualifier, registry)?
                {
                    pairs.push(crate::relation::form::MergedKey {
                        left,
                        right: own_position(registry, right_columns, name)?,
                    });
                }
            }
            ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn {
                    name,
                    qualifier: None,
                    ..
                },
            ))) => {
                if let Some(left) = live_bare_reuse(left_columns, name, registry)? {
                    pairs.push(crate::relation::form::MergedKey {
                        left,
                        right: own_position(registry, right_columns, name)?,
                    });
                }
            }
            ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Ground(_),
            ) => {}
            _ => return Ok(None),
        }
    }
    if pairs.is_empty() {
        Ok(None)
    } else {
        Ok(Some(ast_resolved::MemberCorrelation::Correspond(
            ast_resolved::Correspondence::new(pairs),
        )))
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_anon_membership(
    headers: Option<&[ast_unresolved::DomainExpression]>,
    correlation: &Option<ast_resolved::MemberCorrelation>,
    left_columns: &[PortId],
    resolved_right: &ast_resolved::Chain,
    alias: Option<&SqlIdentifier>,
    visible: &Frontier,
    registry: &crate::relation::Planning,
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
        .map_or(0, |correspondence| correspondence.pairs.len());
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
                let column = match qualifier {
                    Some(qualifier) => {
                        qualified_header_position(visible, left_columns, name, qualifier, registry)?
                    }
                    None => live_bare_reuse(left_columns, name, registry)?,
                };
                let Some(column) = column else {
                    unreachable!("a unified membership header must have one structural binding")
                };
                probes.push(ast_resolved::DomainExpression::Reference(Reference::Named(
                    NamedReference(ColumnOccurrence::engine(column)),
                )));
            }
            _ => unreachable!("membership shape admitted a non-probe header"),
        }
    }

    let rows = match (
        resolved_right.head().form(),
        resolved_right.continuations().is_empty(),
    ) {
        (ast_resolved::GroundForm::Literal(anon), true) => &anon.table.body.rows,
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
    left_columns: &[PortId],
    right_columns: &[PortId],
    position: usize,
    visible: &Frontier,
    registry: &crate::relation::Planning,
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
    let [ast_unresolved::ScalarArgument::Value(ArgumentValue {
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
    let left = match qualifier {
        Some(qualifier) => {
            qualified_header_position(visible, left_columns, column_name, qualifier, registry)?
        }
        None => {
            let wanted = written_name(column_name, registry);
            let matches: Vec<_> = left_columns
                .iter()
                .copied()
                .filter(|column| registry.published_sym(column.column()) == Some(wanted))
                .collect();
            match matches.as_slice() {
                [left] => Some(*left),
                _ => None,
            }
        }
    };
    let Some(left) = left else {
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
                    ast_resolved::ScalarArgument::plain(ast_resolved::DomainExpression::Reference(
                        Reference::Named(NamedReference(ColumnOccurrence::engine(left))),
                    )),
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
                NamedReference(ColumnOccurrence::engine(right)),
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
    left_columns: &[PortId],
    right_columns: &[PortId],
    registry: &crate::relation::Planning,
) -> Result<ast_resolved::MemberCorrelation> {
    correspond(columns, left_columns, right_columns, registry)
}

/// The join `.*` asks for: USING over every name both sides publish.
///
/// It answers with the same construct the spelled-out `.(a, b)` answers
/// with, because they are one operator with two spellings. A conjunction of
/// equalities would join the same rows and publish a DIFFERENT heading —
/// USING merges the column it joined on, an ON does not — so the two
/// spellings would disagree about what the join publishes.
pub(super) fn create_using_all_condition(
    left_columns: &[PortId],
    right_columns: &[PortId],
    registry: &crate::relation::Planning,
) -> Result<ast_resolved::MemberCorrelation> {
    let pairs = shared_using_names(left_columns, right_columns, registry)?
        .into_iter()
        .map(|pair| crate::relation::form::MergedKey {
            left: pair.left,
            right: pair.right,
        })
        .collect();
    Ok(ast_resolved::MemberCorrelation::Correspond(
        ast_resolved::Correspondence::new(pairs),
    ))
}

/// One name both sides publish, and the occurrence of it on each.
pub(in crate::pipeline::resolver) struct SharedName {
    pub left: PortId,
    pub right: PortId,
}

/// THE NAMES `.*` ASKS FOR — the one computation, for every placement.
///
/// `.*` renames every name it can, so what it asks for is exactly the set
/// both headings publish. The set is the same question at a join and inside
/// a correlated interior, and answering it twice is how the two placements
/// come to disagree about the same operator. Publication, not permission:
/// the names are the ones the two headings publish to anyone.
///
/// EMPTY IS NOT AN ANSWER. `.*` asks for every shared name; when there is
/// none, the step it wrote cannot be performed. Returning zero of them would
/// be read as a completed step — a cross join at one placement and an
/// uncorrelated relation at the other, both silent, both wrong.
pub(in crate::pipeline::resolver) fn shared_using_names(
    left_columns: &[PortId],
    right_columns: &[PortId],
    registry: &crate::relation::Planning,
) -> Result<Vec<SharedName>> {
    let mut seen = Vec::new();
    let mut shared: Vec<SharedName> = Vec::new();
    for right in right_columns.iter().copied() {
        let Some(name) = registry.published_sym(right.column()) else {
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
            .filter(|left| registry.published_sym(left.column()) == Some(name))
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
        shared.push(SharedName { left, right });
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

/// WHETHER A CONSTRAINT REACHES THE LEFT ROW. A constraint that names a
/// column the left operand publishes is the join's own condition: stated
/// at the join, it lowers against both operands' sites. One that does not
/// stays the right member's own restriction.
pub(super) fn comparison_reaches(
    constraint: &ast_resolved::TruthExpression,
    left: &[PortId],
) -> bool {
    let side = |expr: &ast_resolved::DomainExpression| {
        matches!(
            expr,
            ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
                ColumnOccurrence { column, .. },
            ))) if left.contains(column)
        )
    };
    match constraint {
        ast_resolved::TruthExpression::Comparison(comparison) => {
            side(&comparison.left) || side(&comparison.right)
        }
        _ => false,
    }
}
