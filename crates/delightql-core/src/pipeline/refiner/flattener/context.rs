// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// context.rs - Context for flattening operations

use std::collections::HashSet;

/// Context maintained during flattening
pub(super) struct FlattenContext<'a> {
    pub identities: &'a crate::relation::Planning,
    pub position: usize,
    pub scope_id: usize,
    pub tables_in_scope: HashSet<crate::names::ScopeId>,
}
