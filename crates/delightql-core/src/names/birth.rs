// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Exact lexical and physical scope allocation.
//!
//! These operations allocate naming scopes only. A scope is not a semantic
//! relation, and none of these methods can construct one or attach an
//! interface.

use super::id::{EntityId, ScopeId, Spelling};
use super::origin::{Hint, ScopeKind, ScratchRole, WrapReason};
use super::registry::Registry;

impl Registry {
    fn admit_scope(&self, kind: ScopeKind, hint: Hint, parent: Option<ScopeId>) -> ScopeId {
        self.mint_scope(kind, hint, parent)
    }

    pub(crate) fn base_table_scope(&self, entity: EntityId, answer: Spelling) -> ScopeId {
        self.admit_scope(ScopeKind::BaseTable { entity }, Hint::User(answer), None)
    }

    #[cfg(test)]
    pub(crate) fn resolved_access_scope(&self, entity: EntityId, answer: Spelling) -> ScopeId {
        self.admit_scope(ScopeKind::Resolution { entity }, Hint::User(answer), None)
    }

    pub(crate) fn wrap_scope(&self, input: ScopeId, why: WrapReason) -> ScopeId {
        self.admit_scope(ScopeKind::Wrap { why }, Hint::None, Some(input))
    }

    pub(crate) fn join_scope(&self) -> ScopeId {
        self.admit_scope(ScopeKind::Join, Hint::None, None)
    }

    pub(crate) fn anonymous_scope(&self, answer: Option<Spelling>) -> ScopeId {
        self.admit_scope(
            ScopeKind::AnonRelation,
            answer.map_or(Hint::None, Hint::User),
            None,
        )
    }

    #[cfg(test)]
    pub(crate) fn carrier_scope(&self, prefix: &'static str) -> ScopeId {
        self.admit_scope(ScopeKind::AnonRelation, Hint::Prefix(prefix), None)
    }

    pub(crate) fn scratch_scope(&self, role: ScratchRole, prefix: &'static str) -> ScopeId {
        self.admit_scope(ScopeKind::Scratch { role }, Hint::Prefix(prefix), None)
    }
}
