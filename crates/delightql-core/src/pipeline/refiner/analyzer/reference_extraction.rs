// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Structural scope extraction for predicate classification.

use crate::error::{DelightQLError, Result};
use crate::names::{Registry, ScopeId};
use crate::pipeline::refiner::flattener::{FlatPredicate, FlatSegment};
use std::collections::HashSet;

pub(super) fn extract_referenced_tables(
    pred: &FlatPredicate,
    flat: &FlatSegment,
    identities: &Registry,
) -> Result<HashSet<ScopeId>> {
    pred.references
        .iter()
        .map(|column| {
            let scope = identities.scope_of(*column);
            if flat.tables.iter().any(|table| table.identity == scope) {
                return Ok(scope);
            }
            // A hoisted correlation filter speaks in the subquery's interior
            // occurrences, while the segment table stands at the boundary. The
            // boundary column republishes the interior one, and only that chain
            // says which table the reference belongs to — the raw scope is one
            // no operator's table list contains.
            //
            // ALL owners are enumerated, never the first: two occurrences of
            // one materialized relation both republish its interior columns,
            // and table order deciding which arm owns a predicate would move
            // it silently — on an outer join, into different row survival.
            // A table whose dimensions the target never published could
            // carry this reference too, so no other table can be named its
            // sole owner while one is in the list.
            for table in &flat.tables {
                if identities.heading(table.identity).is_opaque() {
                    return Err(crate::pipeline::resolver::opaque_reference_refusal());
                }
            }
            let mut owners = flat.tables.iter().filter(|table| {
                identities
                    .known_heading(table.identity)
                    .map(|heading| {
                        heading
                            .iter()
                            .any(|export| identities.republishes(*export, *column))
                    })
                    .unwrap_or(false)
            });
            match (owners.next(), owners.next()) {
                (Some(owner), None) => Ok(owner.identity),
                (None, _) => Ok(scope),
                (Some(_), Some(_)) => Err(DelightQLError::validation_error(
                    format!(
                        "{column:?} is carried by more than one relation occurrence \
                         here, so the predicate that references it belongs to no \
                         single table"
                    ),
                    "in predicate classification",
                )),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    //! What reference-to-table ownership must REFUSE.
    //!
    //! A hoisted predicate lands in a join ON by whichever table owns its
    //! references. Two occurrences of one materialized relation both
    //! republish its interior columns, and answering with the first would
    //! let table order decide the owner — on an outer join, silently
    //! changing which rows survive.

    use super::extract_referenced_tables;
    use crate::names::{
        Addressing, ColId, ColumnOrigin, Computation, CteRole, Hint, Registry, Republish, ScopeId,
        ScopeOrigin, ValueFacts,
    };
    use crate::pipeline::asts::resolved;
    use crate::pipeline::refiner::flattener::{FlatPredicate, FlatSegment, FlatTable};

    fn interior_column(registry: &Registry) -> ColId {
        let entity = registry.mint_entity(registry.intern("t", false));
        let base = registry.mint_scope(ScopeOrigin::BaseTable { entity }, Hint::None, None);
        let cte = registry.mint_scope(
            ScopeOrigin::Cte {
                input: base,
                role: CteRole::Materialize,
            },
            Hint::None,
            None,
        );
        registry.mint_column(
            cte,
            ColumnOrigin::Computed {
                via: Computation::Operator,
            },
            Some(registry.intern("k", false)),
            Addressing::Published,
            ValueFacts::default(),
        )
    }

    fn boundary_table(registry: &Registry, carries: Option<ColId>, position: usize) -> FlatTable {
        let identity = registry.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
        match carries {
            Some(source) => {
                registry.republish_column(
                    source,
                    identity,
                    Republish::BoundaryExport,
                    registry.published(source),
                    Addressing::Published,
                    |_| {},
                );
            }
            None => {
                registry.mint_column(
                    identity,
                    ColumnOrigin::Computed {
                        via: Computation::Operator,
                    },
                    Some(registry.intern("other", false)),
                    Addressing::Published,
                    ValueFacts::default(),
                );
            }
        }
        FlatTable {
            identity,
            position,
            _scope_id: 0,
            access: resolved::Access::All,
            schema: identity,
            outer: false,
            anonymous_data: None,
            inner_relation_pattern: None,
            preminted_scope: None,
            subquery_segment: None,
            pipe_expr: None,
            consulted_view_query: None,
            _table_filters: vec![],
            tvf_data: None,
        }
    }

    fn segment(tables: Vec<FlatTable>) -> FlatSegment {
        FlatSegment {
            tables,
            predicates: Vec::new(),
            operators: Vec::new(),
        }
    }

    fn predicate_on(column: ColId) -> FlatPredicate {
        FlatPredicate {
            // The predicate's SHAPE is irrelevant here — only the
            // references it is recorded with. A real comparison, because
            // there is no synthetic truth leaf.
            expr: resolved::TruthExpression::Comparison(
                crate::pipeline::asts::core::Comparison {
                    operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
                    left: Box::new(resolved::DomainExpression::Application(resolved::FunctionApplication::Ground(resolved::LiteralValue::Number("1".into()),))),
                    right: Box::new(resolved::DomainExpression::Application(resolved::FunctionApplication::Ground(resolved::LiteralValue::Number("1".into()),))),
                },
            ),
            position: 0,
            references: std::iter::once(column).collect(),
            _scope_id: 0,
            origin: resolved::FilterOrigin::UserWritten,
        }
    }

    fn owners(
        registry: &Registry,
        flat: &FlatSegment,
        column: ColId,
    ) -> crate::error::Result<Vec<ScopeId>> {
        Ok(
            extract_referenced_tables(&predicate_on(column), flat, registry)?
                .into_iter()
                .collect(),
        )
    }

    #[test]
    fn one_carrying_boundary_owns_regardless_of_table_order() {
        let registry = Registry::new(&[]);
        let column = interior_column(&registry);
        let carrier = boundary_table(&registry, Some(column), 0);
        let bystander = boundary_table(&registry, None, 1);
        let owner = carrier.identity;
        let forward = owners(
            &registry,
            &segment(vec![carrier.clone(), bystander.clone()]),
            column,
        );
        let reversed = owners(&registry, &segment(vec![bystander, carrier]), column);
        assert_eq!(forward.unwrap(), vec![owner]);
        assert_eq!(reversed.unwrap(), vec![owner]);
    }

    #[test]
    fn two_carrying_boundaries_refuse() {
        let registry = Registry::new(&[]);
        let column = interior_column(&registry);
        let first = boundary_table(&registry, Some(column), 0);
        let second = boundary_table(&registry, Some(column), 1);
        assert!(owners(&registry, &segment(vec![first, second]), column).is_err());
    }
}
