// context.rs - Context for flattening operations

use std::collections::HashSet;

/// Context maintained during flattening
pub(super) struct FlattenContext {
    pub position: usize,
    pub scope_id: usize,
    pub tables_in_scope: HashSet<String>,
    pub anon_counter: usize,
}
