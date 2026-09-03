// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A relation admitted to value position, and the ONE judgment that admits
//! it.
//!
//! CARDINALITY IS AUTHORED, DEGREE IS JUDGED. The compression the author
//! spelled — a zero-key reduction, or the explicit bound to one — is the
//! one-ROW guarantee, and it is structural: the grammar admits no
//! uncompressed inner form. The one-COLUMN guarantee is nobody's spelling.
//! It is asked of the registry HERE, exactly once, and the answer is the
//! occurrence the value publishes.

use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;
use crate::pipeline::resolver::resolver_fold::ResolverFold;

/// Resolve a scalar subquery expression using the fold walker
///
/// Uses the fold's registry and config to resolve the subquery, preserving
/// namespace, CTE, CFE, and grounding context.
/// THE PROOF RIDES IN THE TREE, and the judgment's answer rides with it.
///
/// Resolution puts the compression back on the body to ask the registry the
/// one question it must ask — what does the compressed relation publish —
/// and takes it off again, so the resolved value carries the body, the
/// compression that proves its cardinality, and the occurrence that answers
/// its degree.
pub(in crate::pipeline::resolver) fn resolve_scalar_relation_via_fold(
    fold: &mut ResolverFold,
    relation: ast_unresolved::ScalarRelation,
) -> Result<ast_resolved::ScalarRelation> {
    use crate::pipeline::asts::core::ScalarRelation;

    let identifier = match &relation {
        ScalarRelation::Named { identifier, .. } => identifier.clone(),
        ScalarRelation::Sourceless { .. } => ast_unresolved::QualifiedName {
            namespace_path: ast_unresolved::NamespacePath::empty(),
            name: delightql_types::SqlIdentifier::new("_"),
        },
    };
    let resolved = fold
        .resolve_interior(relation.body().clone().attached())?
        .into_body();

    let sole = sole_column(&resolved, &fold.core.identities, &identifier)?;
    let body = Box::new(ast_resolved::ScalarizedRelation::detach(resolved, sole)?);

    Ok(match relation {
        ScalarRelation::Named { identifier, .. } => ScalarRelation::Named { identifier, body },
        ScalarRelation::Sourceless { .. } => ScalarRelation::Sourceless { body },
    })
}

/// THE DEGREE JUDGMENT, taken once against the registry heading.
///
/// `Known` width 1 admits and answers with that occurrence — so
/// `one_col_table:( |> #(c desc), #<1)` needs no projection. Width greater
/// than one refuses: a value is one value, and picking a column silently is
/// what the engines do and what DelightQL does not. `Opaque` refuses too:
/// unknown never means one.
fn sole_column(
    resolved: &ast_resolved::Chain,
    identities: &crate::relation::Planning,
    identifier: &ast_unresolved::QualifiedName,
) -> Result<crate::relation::PortId> {
    let relation = published_relation(resolved);
    let interface = identities.authority().interface(relation)?;
    if interface.is_opaque() {
        return Err(opaque_heading(identifier));
    }
    let columns = interface.ports();
    match columns {
        [only] => Ok(*only),
        wider => Err(wider_heading(identifier, wider.len())),
    }
}

/// The relation the outermost node of a resolved chain publishes.
#[stacksafe::stacksafe]
fn published_relation(expr: &ast_resolved::Chain) -> &crate::relation::SemanticRelation {
    match expr.continuations().last() {
        Some(step) => step.result(),
        None => expr.head().result(),
    }
}

fn wider_heading(identifier: &ast_unresolved::QualifiedName, width: usize) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "compression/degree",
        format!(
            "'{}' stands where ONE value stands and publishes {width} columns; the \
             compression guarantees one row, not one column",
            identifier.name.as_str()
        ),
        "project the column the value is — `…:( |> (the_column), #<1)` — or reduce to it",
    )
}

fn opaque_heading(identifier: &ast_unresolved::QualifiedName) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "compression/degree",
        format!(
            "'{}' stands where ONE value stands and its heading is not published by the \
             target, so its width is unknown; unknown never means one",
            identifier.name.as_str()
        ),
        "project the column the value is — `…:( |> (the_column), #<1)` — or declare the \
         relation's dimensions at its mention",
    )
}
