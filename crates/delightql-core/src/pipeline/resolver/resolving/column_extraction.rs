// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Output-column construction for resolved publication items.
//!
//! Resolution has already replaced addressable lvars with `ColId`s.  This
//! module therefore makes output identity structural: a direct reference is
//! republished, while a value-producing expression is minted as a new
//! occurrence.  No column name is recovered from an id here.
//!
//! The NAME comes from the publication item. The scalar expression underneath
//! supplies only what kind of computation it is, which is what an unnamed
//! output's display name is derived from — so there is no second, character
//! bearing copy of the publication decision to clear afterwards.

use crate::names::{
    Addressing, ColId, ColumnOrigin, Computation, Hint, Registry, Republish, ScopeId, ScopeOrigin,
    ValueFacts, WrapReason,
};
use crate::pipeline::ast_resolved;
use crate::pipeline::asts::core::ColumnOccurrence;

use crate::pipeline::asts::core::{NamedReference, Reference};
use delightql_types::SqlIdentifier;

pub(in crate::pipeline::resolver) fn mint_projection_scope(
    identities: &Registry,
    input_columns: &[ColId],
) -> ScopeId {
    match identities.common_scope(input_columns) {
        Some(input) => identities.mint_derived_scope(
            ScopeOrigin::Wrap {
                input,
                why: WrapReason::Projection,
            },
            Hint::None,
        ),
        None => identities.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None),
    }
}

fn spelling(identities: &Registry, name: &SqlIdentifier) -> crate::names::Spelling {
    identities.intern(name.as_str(), name.is_stropped())
}

fn computed_alias(
    identities: &Registry,
    naming: Option<&SqlIdentifier>,
) -> Option<crate::names::Spelling> {
    naming.map(|name| spelling(identities, name))
}

/// Which kind of computation a function expression IS.
///
/// The `Computation` vocabulary is what an unnamed output's display name is
/// derived from, so collapsing every shape to `Function` made the vocabulary
/// unreachable for the shapes it names: `age + 1` and `upper:(name)` are not
/// the same thing, and reporting one word for both is the display half of
/// telling two concepts apart.
fn computation_of(func: &ast_resolved::FunctionApplication) -> Computation {
    use ast_resolved::FunctionApplication as F;
    match func {
        F::Ground(_) => Computation::Literal,
        F::Open(_) => Computation::Literal,
        F::Infix(_) => Computation::Operator,
        F::Case(_) | F::ClauseSelection(_) => Computation::Case,
        // Everything else is a call: a constructor (`{…}`, `[…]`) is one
        // too. `Aggregate` is not reachable from here — aggregate-ness is
        // the callee's property and this site holds only the call — so no
        // arm claims it rather than guessing from a name.
        F::Scalarized(_) => Computation::Subquery,
        // The pick is a call whose declared mode chose one of its outputs;
        // what the position computes is still the call.
        F::Standard(_) | F::FieldSelect(_) | F::Enclyph(_) | F::Template(_) | F::JsonAccess(_) => {
            Computation::Function
        }
    }
}

fn mint_computed(
    identities: &Registry,
    output_scope: ScopeId,
    naming: Option<&SqlIdentifier>,
    via: Computation,
    shape: crate::names::ValueShape,
) -> ColId {
    identities.mint_column(
        output_scope,
        ColumnOrigin::Computed { via },
        computed_alias(identities, naming),
        // Unnamed is nameless, not internal. A computed column the query
        // did not name is still an output: it holds a slot in the heading,
        // it renders, and position reaches it. `Hygienic` claims the
        // opposite — never addressable AND no part of the heading — which
        // drops the column out of every heading downstream, so a wildcard
        // finds nothing and a relation built over it publishes nothing.
        // `Bare` with no published spelling answers to no name, and
        // answering to no name is the whole of what being unnamed costs.
        if naming.is_some() {
            Addressing::Published
        } else {
            Addressing::Bare
        },
        ValueFacts {
            shape,
            ..Default::default()
        },
    )
}

fn value_shape(
    expr: &ast_resolved::DomainExpression,
    identities: &Registry,
) -> crate::names::ValueShape {
    use crate::pipeline::asts::core::Enclyph;
    match expr {
        ast_resolved::DomainExpression::Application(
            ast_resolved::FunctionApplication::Enclyph(Enclyph::Record(_)),
        ) => crate::names::ValueShape::Record,
        ast_resolved::DomainExpression::Application(
            ast_resolved::FunctionApplication::Enclyph(Enclyph::EmptyRecord(_)),
        ) => crate::names::ValueShape::Record,
        ast_resolved::DomainExpression::Application(
            ast_resolved::FunctionApplication::Enclyph(Enclyph::Tuple(_)),
        ) => crate::names::ValueShape::Tuple,
        ast_resolved::DomainExpression::Application(
            ast_resolved::FunctionApplication::Standard(application),
        ) => {
            let function = application.call().callee;
            let intrinsic = matches!(
                identities.function_origin(function),
                crate::names::FnOrigin::Intrinsic(crate::names::Intrinsic::JsonObject)
            );
            let mut name = String::new();
            let named = identities
                .write_function_name(function, &mut crate::names::sink::Teaching(&mut name))
                .is_ok()
                && name.eq_ignore_ascii_case("json_object");
            if intrinsic || named {
                crate::names::ValueShape::Record
            } else {
                crate::names::ValueShape::Unknown
            }
        }
        _ => crate::names::ValueShape::Unknown,
    }
}


/// Extract the one output column a value publishes under the name its
/// position gave it.
///
/// A reference republishes — renamed when the position named it, passed
/// through when it did not — and a computation mints, published under that
/// name or bare without one.
pub(in crate::pipeline::resolver) fn extract_provided_column(
    expr: &ast_resolved::DomainExpression,
    naming: Option<&SqlIdentifier>,
    _position: usize,
    identities: &Registry,
    output_scope: ScopeId,
) -> Option<ColId> {
    let republish = |source: ColId| {
        identities.republish_column(
            source,
            output_scope,
            if naming.is_some() {
                Republish::Rename
            } else {
                Republish::Passthrough
            },
            naming
                .map(|name| spelling(identities, name))
                .or_else(|| identities.published(source)),
            Addressing::Published,
            |_| {},
        )
    };

    match expr {
        ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence { column, .. },
        ))) => Some(republish(*column)),
        ast_resolved::DomainExpression::Reference(Reference::Ordinal(_)) => None,
        ast_resolved::DomainExpression::Application(func) => Some(mint_computed(
            identities,
            output_scope,
            naming,
            computation_of(func),
            value_shape(expr, identities),
        )),
    }
}

/// The same decision for a whole publication item. A spread publishes through
/// its own expansion, never through one occurrence.
pub(in crate::pipeline::resolver) fn extract_provided_column_for_item(
    item: &ast_resolved::OutItem,
    position: usize,
    identities: &Registry,
    output_scope: ScopeId,
) -> Option<ColId> {
    match item {
        ast_resolved::OutItem::One(one) => match &one.expr {
            ast_resolved::OutValue::Domain(value) => extract_provided_column(
                value,
                one.naming.as_ref(),
                position,
                identities,
                output_scope,
            ),
            // A PUBLISHED CROSSING publishes a column like any other
            // computed value: it mints, and `as` baptizes it. The truth is
            // read AS a value here, so nothing about publication differs.
            ast_resolved::OutValue::Truth(_) => Some(mint_computed(
                identities,
                output_scope,
                one.naming.as_ref(),
                Computation::Operator,
                crate::names::ValueShape::Unknown,
            )),
        },
        ast_resolved::OutItem::Many(_) | ast_resolved::OutItem::Whole => None,
    }
}

/// The column a REDUCTION item publishes.
///
/// A metadata group publishes an interior record keyed by data — a computed
/// value like any other, minted here and baptized by an `as`.
pub(in crate::pipeline::resolver) fn extract_provided_column_for_reduction(
    item: &ast_resolved::ReductionItem,
    position: usize,
    identities: &Registry,
    output_scope: ScopeId,
) -> Option<ColId> {
    match item {
        ast_resolved::ReductionItem::Out(ast_resolved::OutItem::One(one))
            if matches!(
                one.expr.domain(),
                Some(ast_resolved::DomainExpression::Application(
                    ast_resolved::FunctionApplication::Enclyph(
                        crate::pipeline::asts::core::Enclyph::Record(_)
                            | crate::pipeline::asts::core::Enclyph::EmptyRecord(_)
                    )
                ))
            ) =>
        {
            Some(mint_computed(
                identities,
                output_scope,
                one.naming.as_ref(),
                Computation::Function,
                crate::names::ValueShape::Tuple,
            ))
        }
        ast_resolved::ReductionItem::Out(item) => {
            extract_provided_column_for_item(item, position, identities, output_scope)
        }
        // A delegate publishes one column per PAYLOAD item, resolved at
        // the group boundary that owns it — never one output of its own.
        ast_resolved::ReductionItem::Delegate(_) => None,
        ast_resolved::ReductionItem::Metadata(metadata) => Some(mint_computed(
            identities,
            output_scope,
            metadata.naming.as_ref(),
            Computation::Function,
            crate::names::ValueShape::Record,
        )),
        // A pivot publishes one column per value the IN witnessed, minted
        // where those values are read; it provides no single column here.
        ast_resolved::ReductionItem::Pivot(_) => None,
    }
}
