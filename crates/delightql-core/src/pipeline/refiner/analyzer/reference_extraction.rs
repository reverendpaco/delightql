// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Structural scope extraction for predicate classification.

use crate::error::{DelightQLError, Result};
use crate::names::ScopeId;
use crate::pipeline::refiner::flattener::{FlatPredicate, FlatSegment};
use std::collections::HashSet;

pub(super) fn extract_referenced_tables(
    pred: &FlatPredicate,
    flat: &FlatSegment,
    identities: &crate::relation::Planning,
) -> Result<HashSet<ScopeId>> {
    owning_tables(&pred.references, flat, identities)
}

/// The same ownership answer for any set of references — a whole predicate's
/// or ONE comparison leaf's. Placement asks it of the tree, because a tree
/// lands in one clause; the equality class asks it of the leaf, because each
/// leaf stands in its own cardinality context.
pub(in crate::pipeline::refiner) fn owning_tables(
    references: &HashSet<crate::relation::PortId>,
    flat: &FlatSegment,
    identities: &crate::relation::Planning,
) -> Result<HashSet<ScopeId>> {
    references
        .iter()
        .map(|column| {
            let scope = crate::relation::owner(identities, *column)?;
            if flat
                .tables
                .iter()
                .any(|table| table.relation.scope() == scope)
            {
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
            let authority = identities.authority();
            for table in &flat.tables {
                if authority.interface(&table.relation)?.is_opaque() {
                    return Err(crate::pipeline::resolver::opaque_reference_refusal());
                }
            }
            let mut owners = Vec::new();
            for table in &flat.tables {
                if authority.carries(&table.relation, *column)? {
                    owners.push(table);
                }
            }
            match owners.as_slice() {
                [owner] => Ok(owner.relation.scope()),
                [] => Ok(scope),
                [_, _, ..] => Err(DelightQLError::validation_error(
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
    use crate::names::ScopeId;
    use crate::pipeline::asts::resolved;
    use crate::pipeline::refiner::flattener::{FlatPredicate, FlatSegment, FlatTable};

    fn interior_relation(
        registry: &crate::relation::Planning,
    ) -> (crate::relation::SemanticRelation, crate::relation::PortId) {
        let table_answer = registry.intern("t", false);
        let entity = registry.mint_entity(table_answer);
        let slots = [crate::relation::form::SourceSlot {
            position: 0,
            named: Some(registry.intern("k", false)),
            declared_type: None,
        }];
        let relation = registry
            .authority()
            .derive(crate::relation::RelForm::Source(
                crate::relation::form::SourceSpec {
                    origin: crate::relation::form::SourceOrigin::Catalog { entity },
                    slots: &slots,
                    answers_to: Some(table_answer),
                },
            ))
            .expect("source relation");
        let port =
            crate::relation::published_ports(registry, &relation).expect("source interface")[0];
        (relation, port)
    }

    fn boundary_table(
        registry: &crate::relation::Planning,
        carries: Option<crate::relation::SemanticRelation>,
        position: usize,
    ) -> FlatTable {
        let identity = carries.map_or_else(
            || crate::relation::any_relation(registry),
            |input| {
                registry
                    .authority()
                    .derive(crate::relation::RelForm::Export(
                        crate::relation::form::ExportSpec {
                            input,
                            why: crate::relation::form::ExportWhy::Alias {
                                answer: registry.intern("boundary", false),
                            },
                        },
                    ))
                    .expect("boundary relation")
            },
        );
        FlatTable {
            relation: identity,
            head: None,
            position,
            _scope_id: 0,
            access: resolved::Access::All,
            outer: false,
            anonymous_data: None,
            subquery_segment: None,
            pipe_expr: None,
            _table_filters: vec![],
            tvf_data: None,
        }
    }

    fn segment(tables: Vec<FlatTable>) -> FlatSegment {
        FlatSegment {
            // Reference extraction reads predicates, never the operand the
            // segment was flattened out of; the first table's own relation
            // stands for it here.
            operand: tables[0].relation,
            tables,
            predicates: Vec::new(),
            operators: Vec::new(),
        }
    }

    fn predicate_on(column: crate::relation::PortId) -> FlatPredicate {
        FlatPredicate {
            // The predicate's SHAPE is irrelevant here — only the
            // references it is recorded with. A real comparison, because
            // there is no synthetic truth leaf.
            expr: resolved::TruthExpression::Comparison(crate::pipeline::asts::core::Comparison {
                operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
                left: Box::new(resolved::DomainExpression::Application(
                    resolved::FunctionApplication::Ground(resolved::LiteralValue::Number(
                        "1".into(),
                    )),
                )),
                right: Box::new(resolved::DomainExpression::Application(
                    resolved::FunctionApplication::Ground(resolved::LiteralValue::Number(
                        "1".into(),
                    )),
                )),
            }),
            position: 0,
            references: std::iter::once(column).collect(),
            _scope_id: 0,
            origin: resolved::FilterOrigin::UserWritten,
        }
    }

    fn owners(
        registry: &crate::relation::Planning,
        flat: &FlatSegment,
        column: crate::relation::PortId,
    ) -> crate::error::Result<Vec<ScopeId>> {
        Ok(
            extract_referenced_tables(&predicate_on(column), flat, registry)?
                .into_iter()
                .collect(),
        )
    }

    #[test]
    fn one_carrying_boundary_owns_regardless_of_table_order() {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let (interior, column) = interior_relation(&registry);
        let carrier = boundary_table(&registry, Some(interior), 0);
        let bystander = boundary_table(&registry, None, 1);
        let owner = carrier.relation;
        let forward = owners(
            &registry,
            &segment(vec![carrier.clone(), bystander.clone()]),
            column,
        );
        let reversed = owners(&registry, &segment(vec![bystander, carrier]), column);
        assert_eq!(forward.unwrap(), vec![owner.scope()]);
        assert_eq!(reversed.unwrap(), vec![owner.scope()]);
    }

    #[test]
    fn two_carrying_boundaries_refuse() {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let (interior, column) = interior_relation(&registry);
        let first = boundary_table(&registry, Some(interior), 0);
        let second = boundary_table(&registry, Some(interior), 1);
        assert!(owners(&registry, &segment(vec![first, second]), column).is_err());
    }
}
