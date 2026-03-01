// context_builder.rs - Build context structures for law checking and scope analysis
//
// This module handles construction of law checking contexts and scope sequences

use super::lvar_resolver::extract_lvar_bindings;
use crate::pipeline::asts::resolved;
use crate::pipeline::refiner::flattener::FlatSegment;
use crate::pipeline::refiner::laws;
use std::collections::HashSet;

/// Build context for law checking
pub(super) fn build_law_context(flat: &FlatSegment) -> laws::LawContext {
    // Determine which tables came from set operations
    let mut setop_tables = HashSet::new();
    for table in &flat.tables {
        if matches!(
            table.operation_context,
            crate::pipeline::refiner::flattener::OperationContext::FromSetOp
        ) {
            setop_tables.insert(
                table
                    .alias
                    .clone()
                    .unwrap_or_else(|| table.identifier.name.to_string()),
            );
        }
    }

    // Determine which tables have PLF patterns
    let mut plf_tables = HashSet::new();
    for table in &flat.tables {
        // Check if table has positional pattern
        if matches!(table.domain_spec, resolved::DomainSpec::Positional(_)) {
            plf_tables.insert(
                table
                    .alias
                    .clone()
                    .unwrap_or_else(|| table.identifier.name.to_string()),
            );
        }
    }

    // Extract Lvar mappings for law checking
    let lvar_map = extract_lvar_bindings(flat);

    laws::LawContext {
        setop_tables,
        plf_tables,
        lvar_map,
    }
}

/// Build scope sequence for Law 5 checking
pub(super) fn build_scope_sequence(flat: &FlatSegment) -> Vec<(usize, HashSet<String>)> {
    let mut sequence = Vec::new();
    let mut current_scope = HashSet::new();

    // Add tables to scope as they appear - use their actual positions!
    for table in &flat.tables {
        let table_name = table
            .alias
            .clone()
            .unwrap_or_else(|| table.identifier.name.to_string());
        current_scope.insert(table_name);
        sequence.push((table.position, current_scope.clone()));
    }

    // Add operator positions
    for op in &flat.operators {
        sequence.push((op.position, current_scope.clone()));
    }

    sequence
}
