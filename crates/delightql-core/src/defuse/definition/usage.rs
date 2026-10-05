// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE SCALAR USAGE LAW of a relational or effect higher-order family.
//!
//! Judged where every clause is in hand — the one assembly every clause
//! group crosses — whichever neck declared the family. A position some
//! clause names by a scalar formal is used when some clause's text selects
//! it — a `$.x` of that clause, or a nested definition's capture of it,
//! invoked or not — or when some clause grounds it, since a ground member
//! tests the actual. Uses combine by position: clause-local names never
//! meet. A bare name is a column, so a same-spelled column is no use.

use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::definitions::{argument_name, ArgumentPosition, HoParam};
use crate::pipeline::asts::ddl::Clause;
use delightql_types::SqlIdentifier;

/// Refuse the family if a named scalar position is used by no clause.
pub(crate) fn judge_scalar_usage(family: &str, clauses: &[Clause]) -> Result<()> {
    let mut named: std::collections::BTreeMap<usize, Vec<(usize, SqlIdentifier)>> =
        std::collections::BTreeMap::new();
    let mut used = std::collections::BTreeSet::new();
    for (ordinal, clause) in clauses.iter().enumerate() {
        for (position, param) in clause.params().iter().enumerate() {
            match param {
                HoParam::Scalar { name, .. } => {
                    named
                        .entry(position)
                        .or_default()
                        .push((ordinal, name.clone()));
                }
                HoParam::Ground { .. } => {
                    used.insert(ArgumentPosition::new(position));
                }
                HoParam::Relation { .. } | HoParam::Rule { .. } => {}
            }
        }
        used.extend(crate::ddl::analyzer::clause_selections(clause)?);
    }
    match named
        .into_iter()
        .find(|(position, _)| !used.contains(&ArgumentPosition::new(*position)))
    {
        None => Ok(()),
        Some((position, binders)) => Err(unused(family, clauses.len(), position, &binders)),
    }
}

/// A scalar position no clause uses. The call interface names the
/// position; each clause's own name is that clause's.
fn unused(
    family: &str,
    clauses: usize,
    position: usize,
    binders: &[(usize, SqlIdentifier)],
) -> DelightQLError {
    let names = binders
        .iter()
        .map(|(clause, name)| {
            if clauses == 1 {
                format!("'{name}'")
            } else {
                format!("'{name}' in clause {}", clause + 1)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let first = &binders
        .first()
        .expect("an unused position was named by some clause")
        .1;
    DelightQLError::from(crate::diagnostic::DdlHead::UnusedScalar {
        message: format!(
            "'{family}': {}, the scalar formal {names}, is used by no clause. A bare name \
             in the body is a column, never the formal: read the parameter as `$.{first}`, \
             or ground the position in a clause head",
            argument_name(position),
        ),
    })
}
