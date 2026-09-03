// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// analyzer.rs - Phase 2 of FAR cycle: Classify predicates according to Laws 1-6
//
// The analyzer takes flattened segments and classifies each predicate as:
// - FJC (join condition)
// - FIC (intersect/correlation condition)
// - F (regular filter)
// - Fx (non-participating filter)
// - F! (forbidden by laws)

// Submodules for organized analysis
mod constraint_analyzer;
mod context_builder;
mod exists_analyzer;
mod operator_associator;
mod predicate_classifier;
mod reference_extraction;

// Re-export the ExistsDependencies type for use by rebuilder
pub use self::exists_analyzer::ExistsDependencies;

use self::constraint_analyzer::{create_anonymous_table_join_predicates, process_glob_with_using};
use self::context_builder::{build_law_context, build_scope_sequence};
use self::exists_analyzer::detect_interdependent_exists;
use self::operator_associator::determine_operator_ref;
use self::predicate_classifier::{apply_laws, classify_predicate};
use super::settled::settle_equality_classes;

// THE CARDINALITY QUESTION, asked from one place and answered here. The
// settling reads the same ownership and the same join relationship the
// classifier reads, so a leaf's class and its predicate's placement can never
// disagree about which relations a reference belongs to.
pub(in crate::pipeline::refiner) use self::predicate_classifier::are_in_join_relationship;
pub(in crate::pipeline::refiner) use self::reference_extraction::owning_tables;
use super::flattener::FlatSegment;
use super::laws;
use super::types::*;
use crate::error::Result;

/// Analyzed segment with classified predicates
#[derive(Debug, Clone)]
pub struct AnalyzedSegment {
    /// The relation the segment was flattened out of. See
    /// [`super::flattener::FlatSegment::operand`].
    pub operand: crate::relation::SemanticRelation,

    /// Tables from flattening
    pub tables: Vec<super::flattener::FlatTable>,

    /// Operators from flattening
    pub operators: Vec<super::flattener::FlatOperator>,

    /// Predicates with classifications
    pub predicates: Vec<AnalyzedPredicate>,

    /// EXISTS dependencies for nesting
    pub exists_dependencies: ExistsDependencies,
}

/// Main entry point - analyze a flattened segment
pub fn analyze(
    flat: FlatSegment,
    identities: &crate::relation::Planning,
) -> Result<AnalyzedSegment> {
    let mut analyzed_predicates = Vec::new();

    // Build context for law checking
    let context = build_law_context(&flat, identities);

    // Build table scope sequence for Law 5 (Scope Eagerness)
    let scope_sequence = build_scope_sequence(&flat);

    // Detect interdependent EXISTS before processing predicates
    let exists_dependencies = detect_interdependent_exists(&flat.predicates, identities)?;

    for pred in &flat.predicates {
        // Law 5: Find earliest valid scope point
        let scope_point = laws::find_earliest_scope(&pred.expr, &scope_sequence, identities);

        // Initial classification based on references
        let initial_class = classify_predicate(pred, &flat, &scope_point, identities)?;

        // Apply Laws to check if forbidden or needs reclassification
        let final_class = apply_laws(initial_class, pred, &context, &scope_point, identities)?;

        // Determine which operator this predicate modifies
        let operator_ref = determine_operator_ref(pred, &flat, &scope_point, &final_class);

        analyzed_predicates.push(AnalyzedPredicate {
            class: final_class,
            expr: settle_equality_classes(pred.expr.clone(), &flat, identities)?,
            operator_ref,
            origin: pred.origin.clone(),
        });
    }

    // Extract constraints from anonymous tables and create join predicates (Epoch 3)
    create_anonymous_table_join_predicates(&mut analyzed_predicates, &flat, identities)?;

    log::debug!(
        "Total analyzed predicates after anonymous table processing: {}",
        analyzed_predicates.len()
    );
    for (i, pred) in analyzed_predicates.iter().enumerate() {
        log::debug!(
            "Predicate {}: class={:?}, operator_ref={:?}",
            i,
            pred.class,
            pred.operator_ref
        );
    }

    // Attach each table's dequalifying names to the join that brings it in —
    // one pass, whichever position holds the access.
    let operators = process_glob_with_using(flat.operators.clone(), &flat.tables, identities)?;

    // EPOCH 7: Detect correlation needs for EXISTS-mode anonymous tables
    Ok(AnalyzedSegment {
        operand: flat.operand,
        tables: flat.tables,
        operators,
        predicates: analyzed_predicates,
        exists_dependencies, // Store the dependencies for the rebuilder
    })
}
