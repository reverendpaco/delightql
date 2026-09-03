// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Scope-local reference extraction for the refiner.

use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::asts::core::RelationalMembership;
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::resolved;
use crate::relation::PortId;
use std::collections::HashSet;

pub fn extract_references(expr: &resolved::TruthExpression) -> HashSet<PortId> {
    let mut references = HashSet::new();
    extract_from_boolean(expr, &mut references);
    references
}

/// The same walk over ONE value. A comparison's cardinality context is read
/// off its two operands, and the operands are values.
pub fn extract_value_references(expr: &resolved::DomainExpression) -> HashSet<PortId> {
    let mut references = HashSet::new();
    extract_from_domain(expr, &mut references);
    references
}

fn extract_from_boolean(expr: &resolved::TruthExpression, references: &mut HashSet<PortId>) {
    match expr {
        resolved::TruthExpression::Comparison(resolved::Comparison { left, right, .. }) => {
            extract_from_domain(left, references);
            extract_from_domain(right, references);
        }
        resolved::TruthExpression::Conjunction(parts)
        | resolved::TruthExpression::Disjunction(parts) => {
            for part in parts.iter() {
                extract_from_boolean(part, references);
            }
        }
        resolved::TruthExpression::Not { expr } => {
            extract_from_boolean(expr, references);
        }
        resolved::TruthExpression::Membership(resolved::Membership { probe, rows, .. }) => {
            for value in probe.values() {
                extract_from_domain(value, references);
            }
            for row in rows {
                for value in &row.0 {
                    extract_from_domain(value, references);
                }
            }
        }
        resolved::TruthExpression::RelationalMembership(RelationalMembership { probe, .. }) => {
            for value in probe.values() {
                extract_from_domain(value, references);
            }
        }
        resolved::TruthExpression::Existence(resolved::Existence { .. })
        | resolved::TruthExpression::Sigma(resolved::SigmaApplication { .. }) => {}
    }
}

fn extract_from_function(
    function: &resolved::FunctionApplication,
    references: &mut HashSet<PortId>,
) {
    match function {
        resolved::FunctionApplication::Ground(_) | resolved::FunctionApplication::Open(_) => {}
        // A scalarized relation is a self-contained nested scope; its
        // interior's columns are not this predicate's references.
        resolved::FunctionApplication::Scalarized(_) => {}
        resolved::FunctionApplication::Standard(application) => {
            for argument in application.call().arguments.value_domains() {
                extract_from_domain(argument, references);
            }
        }
        // The pick reads the columns its ARGUMENTS read; the arms are the
        // callee's constants and reference nothing in this predicate.
        resolved::FunctionApplication::FieldSelect(select) => {
            for argument in select.application.call().arguments.value_domains() {
                extract_from_domain(argument, references);
            }
        }
        resolved::FunctionApplication::Crossed(crossing) => {
            extract_from_boolean(crossing.truth(), references)
        }
        resolved::FunctionApplication::Infix(infix) => {
            extract_from_domain(&infix.left, references);
            extract_from_domain(&infix.right, references);
        }
        resolved::FunctionApplication::Template(template) => {
            for part in template.parts() {
                if let resolved::ValueTemplatePart::Interpolation(expression) = part {
                    extract_from_domain(expression, references);
                }
            }
        }
        resolved::FunctionApplication::ClauseSelection(selection) => {
            for arm in &selection.arms {
                if let Some(guard) = &arm.guard {
                    extract_from_boolean(guard, references);
                }
                extract_from_domain(&arm.result, references);
            }
        }
        resolved::FunctionApplication::Case(case) => {
            let default = match case {
                resolved::CaseExpression::Anchored {
                    anchor,
                    arms,
                    default,
                } => {
                    extract_from_domain(anchor, references);
                    for arm in arms.iter() {
                        extract_from_domain(&arm.result, references);
                    }
                    default
                }
                resolved::CaseExpression::Searched { arms, default } => {
                    for arm in arms.iter() {
                        extract_from_boolean(&arm.condition, references);
                        extract_from_domain(&arm.result, references);
                    }
                    default
                }
            };
            if let Some(result) = default {
                extract_from_domain(result, references);
            }
        }
        resolved::FunctionApplication::JsonAccess(access) => {
            extract_from_domain(&access.source, references);
        }
        resolved::FunctionApplication::Enclyph(enclyph) => {
            extract_from_enclyph(enclyph, references);
        }
    }
}

fn extract_from_enclyph(
    enclyph: &resolved::Enclyph,
    references: &mut std::collections::HashSet<PortId>,
) {
    use crate::pipeline::asts::core::{Enclyph, NamedReference, RecordMember};
    match enclyph {
        Enclyph::Record(record) => {
            for member in record.members.iter() {
                match member {
                    RecordMember::Keyed { value, .. } => extract_from_domain(value, references),
                    RecordMember::Induced { value, .. } => extract_from_enclyph(value, references),
                    RecordMember::SelfKeyed(NamedReference(occurrence)) => {
                        references.insert(occurrence.column);
                    }
                    // An authored spread is uninhabited after resolution.
                    RecordMember::Spread(spread) => spread.expanded(),
                    RecordMember::Metadata { group, .. } => {
                        extract_from_metadata_group(group, references)
                    }
                }
            }
        }
        Enclyph::EmptyRecord(_) => {}
        Enclyph::Tuple(tuple) => {
            for element in tuple.elements.iter() {
                extract_from_domain(element.value(), references);
            }
        }
    }
}

/// A metadata level's dependencies: its key column, and its target's.
fn extract_from_metadata_group(
    group: &crate::pipeline::asts::core::MetadataGroup<crate::pipeline::asts::core::Resolved>,
    references: &mut std::collections::HashSet<PortId>,
) {
    use crate::pipeline::asts::core::MetadataTarget;
    references.insert(group.key.column);
    match &group.target {
        MetadataTarget::Enclyph(enclyph) => extract_from_enclyph(enclyph, references),
        MetadataTarget::Group(nested) => extract_from_metadata_group(nested, references),
    }
}

fn extract_from_domain(expr: &resolved::DomainExpression, references: &mut HashSet<PortId>) {
    match expr {
        resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence { column, .. },
        ))) => {
            references.insert(*column);
        }
        resolved::DomainExpression::Reference(Reference::Physical(_)) => {}
        resolved::DomainExpression::Application(function) => {
            extract_from_function(function, references);
        }
        // Uninhabited after resolution, and still written: a match on a
        // REFERENCE cannot omit an uninhabited variant's arm.
        resolved::DomainExpression::Reference(Reference::Ordinal(_)) => {}
    }
}
