// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::vocabulary::Vec1;
use crate::pipeline::resolver::resolver_fold::ResolverFold;
use crate::pipeline::{ast_resolved, ast_unresolved};

fn resolved_ref(column: crate::relation::PortId) -> ast_resolved::DomainExpression {
    ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
        ColumnOccurrence::engine(column),
    )))
}

fn push_unique<T>(
    output: &mut Vec<T>,
    seen: &mut Vec<crate::relation::PortId>,
    column: crate::relation::PortId,
    value: T,
) {
    if !seen.contains(&column) {
        seen.push(column);
        output.push(value);
    }
}

/// `[…]` — by position. A repeated address publishes once; anything that is
/// not an address stands as itself.
pub(in crate::pipeline::resolver) fn resolve_tuple_via_fold(
    fold: &mut ResolverFold,
    tuple: ast_unresolved::Tuple,
) -> Result<ast_resolved::Tuple> {
    use crate::pipeline::asts::core::TupleElement;
    let available = fold.lexical.local_ports(&fold.core.identities)?;
    let mut seen = Vec::new();
    let mut resolved = Vec::new();
    for element in tuple.elements.into_vec() {
        match element {
            // A TUPLE SPREAD EXPANDS THE COLUMNS IT ADDRESSES into
            // positional elements, through the one expansion authority
            // every other enumerating position uses.
            TupleElement::Spread(spread) => {
                let expanded = super::domain_expressions::projection::expand_spread(
                    fold, &spread, &available, true,
                )?;
                for expression in expanded {
                    if let ast_resolved::DomainExpression::Reference(Reference::Named(
                        NamedReference(ColumnOccurrence { column, .. }),
                    )) = expression
                    {
                        push_unique(&mut resolved, &mut seen, column, resolved_ref(column));
                    } else {
                        resolved.push(expression);
                    }
                }
            }
            TupleElement::Value(element) => {
                let expression = fold.transform_domain(element)?;
                if let ast_resolved::DomainExpression::Reference(Reference::Named(
                    NamedReference(ColumnOccurrence { column, .. }),
                )) = expression
                {
                    push_unique(&mut resolved, &mut seen, column, resolved_ref(column));
                } else {
                    resolved.push(expression);
                }
            }
        }
    }
    Ok(ast_resolved::Tuple {
        elements: Vec1::try_from_vec(resolved.into_iter().map(TupleElement::Value).collect())
            .expect("a tuple's elements are values, and deduplication keeps one"),
    })
}

/// `{…}` — by name. A spread expands into the self-keyed members for the
/// columns it addresses; every other member is construction material.
fn resolve_record_members_via_fold(
    fold: &mut ResolverFold,
    record: ast_unresolved::Record,
) -> Result<Vec<ast_resolved::RecordMember>> {
    use crate::pipeline::asts::core::RecordMember;

    let available = fold.lexical.local_ports(&fold.core.identities)?;
    let mut seen = Vec::new();
    let mut resolved = Vec::new();

    for member in record.members.into_vec() {
        match member {
            // A RECORD SPREAD EXPANDS THE COLUMNS IT ADDRESSES into
            // self-keyed members, through the one expansion authority every
            // other enumerating position uses.
            RecordMember::Spread(spread) => {
                let expanded = super::domain_expressions::projection::expand_spread(
                    fold, &spread, &available, true,
                )?;
                for expression in expanded {
                    let ast_resolved::DomainExpression::Reference(reference) = expression else {
                        return Err(DelightQLError::transformation_error(
                            "a record spread addresses columns, and this one expanded to a value",
                            "record_member",
                        ));
                    };
                    let Some(column) =
                        super::domain_expressions::projection::reference_column(&reference)
                    else {
                        return Err(DelightQLError::transformation_error(
                            "a record spread expanded to an address with no occurrence",
                            "record_member",
                        ));
                    };
                    push_unique(
                        &mut resolved,
                        &mut seen,
                        column,
                        RecordMember::SelfKeyed(NamedReference(ColumnOccurrence::engine(column))),
                    );
                }
            }
            // FN.22 (amended): a metadata group as an induced member's
            // body. Outward-acting; whether the containing record stands
            // for a group is judged where the record's position is known.
            RecordMember::Metadata { key, group } => {
                let group = fold.resolve_metadata_group(*group)?;
                resolved.push(RecordMember::Metadata {
                    key,
                    group: Box::new(group),
                });
            }
            RecordMember::SelfKeyed(NamedReference(authored)) => {
                use crate::pipeline::resolver::unification::{ColumnReference, UnificationResult};
                let reference = ColumnReference::Named {
                    name: authored.name.clone(),
                    qualifier: authored.qualifier.clone(),
                };
                let mut witness = crate::pipeline::resolver::Witness::default();
                let result =
                    fold.lexical
                        .address(reference, false, &mut witness, &fold.core.identities)?;
                match result {
                    UnificationResult::Resolved(occurrence) => {
                        let column = occurrence.column;
                        push_unique(
                            &mut resolved,
                            &mut seen,
                            column,
                            RecordMember::SelfKeyed(NamedReference(occurrence)),
                        )
                    }
                    UnificationResult::Unresolved(column) => {
                        return Err(DelightQLError::column_not_found_error(
                            column,
                            "in tree group key",
                        ))
                    }
                    UnificationResult::Opaque => {
                        return Err(crate::pipeline::resolver::opaque_reference_refusal())
                    }
                    UnificationResult::Refused(refusal) => return Err(refusal.into_error()),
                    UnificationResult::Ambiguous { column, tables } => {
                        return Err(DelightQLError::validation_error_categorized(
                            "resolution/ambiguous",
                            format!(
                                "Ambiguous column '{column}' in tree group key: {}",
                                tables.join(", ")
                            ),
                            "qualify the tree-group key",
                        ))
                    }
                }
            }
            RecordMember::Keyed { key, value } => resolved.push(RecordMember::Keyed {
                key,
                value: Box::new(fold.transform_domain(*value)?),
            }),
            RecordMember::Induced { key, value } => resolved.push(RecordMember::Induced {
                key,
                value: Box::new(resolve_enclyph_via_fold(fold, *value)?),
            }),
        }
    }

    Ok(resolved)
}

fn empty_record_call(fold: &ResolverFold) -> ast_resolved::FunctionApplication {
    ast_resolved::FunctionApplication::Standard(
        crate::pipeline::asts::core::StandardApplication::plain(
            crate::pipeline::asts::core::PureCall::from_inner(ast_resolved::FunctorCall {
                callee: fold
                    .core
                    .identities
                    .mint_intrinsic(crate::names::Intrinsic::JsonObject),
                arguments: crate::pipeline::asts::core::operators::CallArguments::Scalar(Vec::new()),
                marks: Default::default(),
            }),
        ),
    )
}

pub(in crate::pipeline::resolver) fn resolve_function_enclyph_via_fold(
    fold: &mut ResolverFold,
    enclyph: ast_unresolved::Enclyph,
) -> Result<ast_resolved::FunctionApplication> {
    let resolved = resolve_enclyph_via_fold(fold, enclyph)?;
    Ok(match resolved {
        ast_resolved::Enclyph::EmptyRecord(()) => empty_record_call(fold),
        enclyph => ast_resolved::FunctionApplication::Enclyph(enclyph),
    })
}

pub(in crate::pipeline::resolver) fn resolve_enclyph_via_fold(
    fold: &mut ResolverFold,
    enclyph: ast_unresolved::Enclyph,
) -> Result<ast_resolved::Enclyph> {
    use crate::pipeline::asts::core::Enclyph;
    Ok(match enclyph {
        Enclyph::Record(record) => {
            let members = resolve_record_members_via_fold(fold, record)?;
            match Vec1::try_from_vec(members) {
                Some(members) => Enclyph::Record(ast_resolved::Record { members }),
                None => Enclyph::EmptyRecord(()),
            }
        }
        Enclyph::Tuple(tuple) => Enclyph::Tuple(Box::new(resolve_tuple_via_fold(fold, *tuple)?)),
        Enclyph::EmptyRecord(empty) => match empty {},
    })
}

pub(in crate::pipeline::resolver) fn resolve_window_frame_via_fold(
    fold: &mut ResolverFold,
    frame: ast_unresolved::WindowFrame,
) -> Result<ast_resolved::WindowFrame> {
    let resolve_bound = |fold: &mut ResolverFold,
                         bound: ast_unresolved::FrameBound|
     -> Result<ast_resolved::FrameBound> {
        match bound {
            ast_unresolved::FrameBound::Unbounded => Ok(ast_resolved::FrameBound::Unbounded),
            ast_unresolved::FrameBound::CurrentRow => Ok(ast_resolved::FrameBound::CurrentRow),
            ast_unresolved::FrameBound::Preceding(expression) => Ok(
                ast_resolved::FrameBound::Preceding(Box::new(fold.transform_domain(*expression)?)),
            ),
            ast_unresolved::FrameBound::Following(expression) => Ok(
                ast_resolved::FrameBound::Following(Box::new(fold.transform_domain(*expression)?)),
            ),
        }
    };
    Ok(ast_resolved::WindowFrame {
        mode: frame.mode,
        start: resolve_bound(fold, frame.start)?,
        end: resolve_bound(fold, frame.end)?,
    })
}
