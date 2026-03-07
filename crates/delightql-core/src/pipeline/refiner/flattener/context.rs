// context.rs - Context for flattening operations

use std::collections::{HashMap, HashSet};

/// Context maintained during flattening
pub(super) struct FlattenContext {
    pub position: usize,
    pub scope_id: usize,
    pub tables_in_scope: HashSet<String>,
    pub anon_counter: usize,
    /// Maps qualifier aliases to canonical table names for all ancestor
    /// inner-relation scopes. Populated at each depth before recursion.
    pub scope_aliases: HashMap<String, String>,
}
