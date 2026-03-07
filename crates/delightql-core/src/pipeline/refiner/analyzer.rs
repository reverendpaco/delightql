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
mod lvar_resolver;
mod operator_associator;
mod predicate_classifier;
mod reference_extraction;
mod segment_classifier;

// Re-export the ExistsDependencies type for use by rebuilder
pub use self::exists_analyzer::ExistsDependencies;

use self::constraint_analyzer::{
    create_anonymous_table_join_predicates, process_glob_with_using, process_using_operators,
};
use self::context_builder::{build_law_context, build_scope_sequence};
use self::exists_analyzer::detect_interdependent_exists;
use self::lvar_resolver::create_lvar_using_predicates;
use self::operator_associator::determine_operator_ref;
use self::predicate_classifier::{apply_laws, classify_predicate};
use self::segment_classifier::determine_segment_type;
use super::flattener::FlatSegment;
use super::laws;
use super::types::*;
use crate::error::Result;

/// Analyzed segment with classified predicates
#[derive(Debug, Clone)]
pub struct AnalyzedSegment {
    /// Tables from flattening
    pub tables: Vec<super::flattener::FlatTable>,

    /// Operators from flattening
    pub operators: Vec<super::flattener::FlatOperator>,

    /// Predicates with classifications
    pub predicates: Vec<AnalyzedPredicate>,

    /// Segment type
    pub segment_type: SegmentType,

    /// EXISTS dependencies for nesting
    pub exists_dependencies: ExistsDependencies,
}

/// Main entry point - analyze a flattened segment
pub fn analyze(flat: FlatSegment) -> Result<AnalyzedSegment> {
    let mut analyzed_predicates = Vec::new();

    // Build context for law checking
    let context = build_law_context(&flat);

    // Build table scope sequence for Law 5 (Scope Eagerness)
    let scope_sequence = build_scope_sequence(&flat);

    // Detect interdependent EXISTS before processing predicates
    let exists_dependencies = detect_interdependent_exists(&flat.predicates);

    for pred in &flat.predicates {
        // Law 5: Find earliest valid scope point
        let scope_point = laws::find_earliest_scope(&pred.expr, &scope_sequence);

        // Initial classification based on references
        let initial_class = classify_predicate(pred, &flat, &scope_point)?;

        // Apply Laws to check if forbidden or needs reclassification
        let final_class = apply_laws(initial_class, pred, &context, &scope_point)?;

        // Determine which operator this predicate modifies
        let operator_ref = determine_operator_ref(pred, &flat, &scope_point, &final_class);

        analyzed_predicates.push(AnalyzedPredicate {
            class: final_class,
            expr: pred.expr.clone(),
            operator_ref,
            origin: pred.origin.clone(),
        });
    }

    // Detect and create USING predicates from shared Lvars for positional unification
    create_lvar_using_predicates(&mut analyzed_predicates, &flat);

    // Extract constraints from anonymous tables and create join predicates (Epoch 3)
    create_anonymous_table_join_predicates(&mut analyzed_predicates, &flat);

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

    // Determine segment type using the submodule
    let segment_type = determine_segment_type(&flat);

    // Process GlobWithUsing and update join operators with USING columns
    let operators = process_glob_with_using(flat.operators.clone(), &flat.tables);
    // Process Using operators .(cols) and update join operators with USING columns
    let operators = process_using_operators(operators, &flat.tables);

    // EPOCH 7: Detect correlation needs for EXISTS-mode anonymous tables
    let mut tables_with_correlation = flat.tables;
    detect_anonymous_table_correlations(&mut tables_with_correlation);

    Ok(AnalyzedSegment {
        tables: tables_with_correlation,
        operators,
        predicates: analyzed_predicates,
        segment_type,
        exists_dependencies, // Store the dependencies for the rebuilder
    })
}

/// EPOCH 7: Detect column references in EXISTS-mode anonymous table data rows
/// and populate correlation_refs for inverted IN pattern detection
fn detect_anonymous_table_correlations(tables: &mut [super::flattener::FlatTable]) {
    for table in tables.iter_mut() {
        // Only analyze EXISTS-mode anonymous tables
        if let Some(ref anon_data) = table.anonymous_data {
            if !anon_data.exists_mode {
                continue;
            }

            // Extract column references from data rows
            for row in &anon_data.rows {
                for value in &row.values {
                    extract_column_refs_from_expr(value, &mut table.correlation_refs);
                }
            }

            if !table.correlation_refs.is_empty() {
                log::debug!(
                    "Detected {} correlation refs in EXISTS-mode anonymous table {}",
                    table.correlation_refs.len(),
                    table.identifier.name
                );
            }
        }
    }
}

/// Recursively extract column references (Lvars) from a domain expression
fn extract_column_refs_from_expr(
    expr: &crate::pipeline::asts::resolved::DomainExpression,
    refs: &mut Vec<super::flattener::CorrelationRef>,
) {
    use super::flattener::CorrelationRef;
    use crate::pipeline::asts::resolved;

    match expr {
        resolved::DomainExpression::Lvar { name, .. } => {
            // Found a column reference - add it
            refs.push(CorrelationRef {
                column_name: name.to_string(),
                outer_table: None, // Don't resolve outer table yet (YAGNI for now)
            });
        }
        resolved::DomainExpression::Function(func) => {
            match func {
                resolved::FunctionExpression::Regular { arguments, .. }
                | resolved::FunctionExpression::Curried { arguments, .. } => {
                    for arg in arguments {
                        extract_column_refs_from_expr(arg, refs);
                    }
                }
                resolved::FunctionExpression::Infix { left, right, .. } => {
                    extract_column_refs_from_expr(left, refs);
                    extract_column_refs_from_expr(right, refs);
                }
                resolved::FunctionExpression::HigherOrder {
                    curried_arguments,
                    regular_arguments,
                    ..
                } => {
                    for arg in curried_arguments {
                        extract_column_refs_from_expr(arg, refs);
                    }
                    for arg in regular_arguments {
                        extract_column_refs_from_expr(arg, refs);
                    }
                }
                resolved::FunctionExpression::Lambda { body, .. } => {
                    extract_column_refs_from_expr(body, refs);
                }
                other => panic!("catch-all hit in analyzer.rs extract_column_refs_from_expr (FunctionExpression): {:?}", other),
            }
        }
        resolved::DomainExpression::Parenthesized { inner, .. } => {
            extract_column_refs_from_expr(inner, refs);
        }
        resolved::DomainExpression::Tuple { elements, .. } => {
            for elem in elements {
                extract_column_refs_from_expr(elem, refs);
            }
        }
        // Literals, ScalarSubquery, Case, etc. — no column refs to extract
        // (or refs are in nested subqueries that are handled separately)
        _ => {}
    }
}
