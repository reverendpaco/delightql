// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A HIGHER-ORDER CALL'S CARRIERS, as one operation of the carrier
//! authority: the piped source and the caller row it absorbs bound in the
//! caller's world, the admitted relation actuals bound closed, each as the
//! part of the call it is — decided here and nowhere else.

use super::CarrierRecord;
use crate::defuse::bound_use::ClosedRelationActual;
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_unresolved;
use crate::pipeline::resolver::resolver_fold::ResolverFold;

/// Resolve the caller-side carriers before the family is admitted.
///
/// The piped source and the free-scalar join input are the relation
/// FLOWING INTO the call and resolve IN THE CALLER'S WORLD, under the
/// caller's own outer row and qualifier scopes. The relation actuals are
/// VALUES SUPPLIED to the call and resolve CLOSED: a child world with no
/// outer row, no sibling members and no qualifiers, so an interior can
/// read only its own source, its literals and the statement's definitions.
/// A name the closed world cannot answer but the caller's world could is
/// caller capture, and refuses as such.
///
/// Every carrier is BOUND INTO THE RECORD of this act, which reserves its
/// landing as it binds; the landing a formal is addressed by is written
/// into the bindings from the row the bind produced, never reserved ahead
/// of the carrier.
pub(in crate::defuse) fn resolve_carriers(
    caller: &mut ResolverFold<'_, '_>,
    bindings: &mut crate::pipeline::query_features::HoParamBindings,
    pipe_source: Option<(String, ast_unresolved::Chain)>,
    actuals: Vec<(String, crate::relation::form::HoPart, ClosedRelationActual)>,
    join_input: Option<crate::pipeline::resolver::ResolvedRelation>,
) -> Result<CarrierRecord> {
    let mut record = CarrierRecord::default();

    if let Some((formal, source_expr)) = pipe_source {
        let mut caller_scope = caller.child();
        let row = resolve_carrier(
            &mut caller_scope,
            &mut record,
            crate::relation::form::HoPart::PipeSource,
            source_expr,
        )?;
        bindings
            .table_scope_params
            .insert(formal.clone(), row.landing());
        bindings.pipe_carrier = Some((formal, row.landing()));
    }

    for (formal, part, actual) in actuals {
        let resolved = {
            let mut closed = caller.child_closed();
            resolve_carrier(&mut closed, &mut record, part, actual.into_chain())
        };
        let row = match resolved {
            Ok(row) => row,
            Err(DelightQLError::ColumnNotFoundError { column, context })
                if caller_answers(caller, &column) =>
            {
                return Err(DelightQLError::validation_error_categorized(
                    crate::uri_registry::subcat::HO_RELATION_ACTUAL_CAPTURE,
                    format!(
                        "a relation actual is a closed relation value: its interior may not \
                         read `{column}` from the calling row"
                    ),
                    "pass the value as an ordinary argument and read it inside the \
                     definition, or construct the relation first and bind it with `:`",
                ))
                .map_err(|refusal| {
                    // The closed resolution's own account stays attached as
                    // the cause, so a reader can see which reference it was.
                    let _ = context;
                    refusal
                });
            }
            Err(error) => return Err(error),
        };
        bindings.table_scope_params.insert(formal, row.landing());
    }

    if let Some(source) = join_input {
        // THE CALLER ROW BECOMES A CARRIER: the standing row the call
        // absorbed is spent into its structural binding here.
        record.bind_join_input(source, &caller.core.identities)?;
    }

    Ok(record)
}

/// One carrier chain, resolved in the world it is handed and BOUND INTO
/// THE RECORD as a structural carrier: the chain resolves like any
/// relation, the residual carriers crossing this world ride its body, and
/// the bind spends the resolved body, reserving the landing and deriving
/// the carrier as one act. A compiler-built carrier is glob-headed,
/// authors no badge and cannot recurse, so it takes no `WITH` machinery.
pub(super) fn resolve_carrier(
    world: &mut ResolverFold<'_, '_>,
    record: &mut CarrierRecord,
    part: crate::relation::form::HoPart,
    source_expr: ast_unresolved::Chain,
) -> Result<crate::relation::CarrierRow> {
    let crossing = world.crossing_carriers.clone();
    let standing = world.resolve_relational(source_expr)?;
    let standing = if crossing.is_empty() {
        standing
    } else {
        let identities = world.core.identities;
        standing.republished(|chain| {
            crate::pipeline::refiner::pattern_classifier::inject_crossing_carriers(
                chain, &crossing, identities,
            )
        })?
    };
    record.bind(part, standing, &world.core.identities)
}

/// Whether the CALLER'S world would answer the reference the closed world
/// could not: a bare name published by the caller's live row or its outer
/// row, or a qualifier one of its scopes answers to. A diagnostic
/// question — it decides only which refusal to teach.
fn caller_answers(caller: &ResolverFold<'_, '_>, reference: &str) -> bool {
    caller
        .lexical
        .answers_spelling(reference, &caller.core.identities)
}
