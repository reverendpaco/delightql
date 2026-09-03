// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Exact lexical and physical scope allocation.
//!
//! These operations allocate naming scopes only. A scope is not a semantic
//! relation, and none of these methods can construct one or attach an
//! interface. Relational construction selects among these operations inside
//! its exhaustive form judgment; SQL lowering uses only the physical ones it
//! actually performs.

use super::id::{ColId, EntityId, ScopeId, Spelling};
use super::origin::{CteRole, Hint, HoRole, ScopeKind, ScratchRole, WrapReason};
use super::registry::Registry;

/// How a CTE binding is named: an authored answering name, a compiler-exact
/// emission label (never an answering name), or nothing.
#[derive(Clone, Copy, Debug)]
pub enum CteLabel {
    Answering(Spelling),
    Exact(Spelling),
    Prefixed(&'static str),
    Anonymous,
}

impl Registry {
    fn admit_scope(&self, kind: ScopeKind, hint: Hint, parent: Option<ScopeId>) -> ScopeId {
        self.mint_scope(kind, hint, parent)
    }

    pub(crate) fn base_table_scope(&self, entity: EntityId, answer: Spelling) -> ScopeId {
        self.admit_scope(ScopeKind::BaseTable { entity }, Hint::User(answer), None)
    }

    pub(crate) fn alias_scope(&self, input: ScopeId, answer: Spelling) -> ScopeId {
        self.admit_scope(ScopeKind::UserAlias, Hint::User(answer), Some(input))
    }

    /// THE SCOPE OF A SLOT ROW'S PUBLICATION. An authored owner is the
    /// answer the scope records; a row nobody named records NONE — not a
    /// spelling the frontier withholds, but no spelling at all, so no act
    /// that reads a scope's birth answer can revive a name the author
    /// never granted.
    pub(crate) fn bound_row_scope(&self, input: ScopeId, answer: Option<Spelling>) -> ScopeId {
        self.admit_scope(
            ScopeKind::UserAlias,
            answer.map_or(Hint::None, Hint::User),
            Some(input),
        )
    }

    pub(crate) fn resolved_access_scope(&self, entity: EntityId, answer: Spelling) -> ScopeId {
        self.admit_scope(ScopeKind::Resolution { entity }, Hint::User(answer), None)
    }

    pub(crate) fn stage_scope(&self, input: ScopeId) -> ScopeId {
        self.admit_scope(ScopeKind::PipeStage, Hint::None, Some(input))
    }

    pub(crate) fn wrap_scope(&self, input: ScopeId, why: WrapReason) -> ScopeId {
        self.admit_scope(ScopeKind::Wrap { why }, Hint::None, Some(input))
    }

    pub(crate) fn opaque_scope(&self) -> ScopeId {
        self.admit_scope(ScopeKind::AnonRelation, Hint::None, None)
    }

    pub(crate) fn cte_scope(&self, input: ScopeId, role: CteRole, label: CteLabel) -> ScopeId {
        let hint = match label {
            CteLabel::Answering(spelling) => Hint::User(spelling),
            CteLabel::Exact(spelling) => Hint::Exact(spelling),
            CteLabel::Prefixed(prefix) => Hint::Prefix(prefix),
            CteLabel::Anonymous => Hint::None,
        };
        self.admit_scope(ScopeKind::Cte { role }, hint, Some(input))
    }

    pub(crate) fn join_scope(&self) -> ScopeId {
        self.admit_scope(ScopeKind::Join, Hint::None, None)
    }

    pub(crate) fn set_arm_scope(&self, input: ScopeId, arm: u16) -> ScopeId {
        self.admit_scope(ScopeKind::SetArm { arm }, Hint::None, Some(input))
    }

    pub(crate) fn er_hop_scope(&self, chain: ScopeId, hop: u16, prefix: &'static str) -> ScopeId {
        self.admit_scope(ScopeKind::ErHop { hop }, Hint::Prefix(prefix), Some(chain))
    }

    pub(crate) fn anonymous_scope(&self, answer: Option<Spelling>) -> ScopeId {
        self.admit_scope(
            ScopeKind::AnonRelation,
            answer.map_or(Hint::None, Hint::User),
            None,
        )
    }

    pub(crate) fn carrier_scope(&self, prefix: &'static str) -> ScopeId {
        self.admit_scope(ScopeKind::AnonRelation, Hint::Prefix(prefix), None)
    }

    pub(crate) fn higher_order_scope(&self, role: HoRole, prefix: &'static str) -> ScopeId {
        self.admit_scope(ScopeKind::HoCarrier { role }, Hint::Prefix(prefix), None)
    }

    pub(crate) fn scratch_scope(&self, role: ScratchRole, prefix: &'static str) -> ScopeId {
        self.admit_scope(ScopeKind::Scratch { role }, Hint::Prefix(prefix), None)
    }

    pub(crate) fn exact_scratch_scope(&self, role: ScratchRole, base: Spelling) -> ScopeId {
        self.admit_scope(ScopeKind::Scratch { role }, Hint::Exact(base), None)
    }

    pub(crate) fn interior_scope(&self, owner: ColId) -> ScopeId {
        self.mint_interior_scope(owner, Hint::None)
    }

    pub(crate) fn interior_emission_scope(&self, owner: ColId) -> ScopeId {
        self.admit_scope(ScopeKind::Interior, Hint::None, Some(self.scope_of(owner)))
    }

    pub(crate) fn emission_alias_scope(&self, input: ScopeId) -> ScopeId {
        self.admit_scope(ScopeKind::UserAlias, Hint::None, Some(input))
    }

    pub(crate) fn carrier_wrap_scope(
        &self,
        input: ScopeId,
        why: WrapReason,
        prefix: &'static str,
    ) -> ScopeId {
        self.admit_scope(ScopeKind::Wrap { why }, Hint::Prefix(prefix), Some(input))
    }

    pub(crate) fn exact_emission_scope(
        &self,
        input: ScopeId,
        why: WrapReason,
        base: Spelling,
    ) -> ScopeId {
        self.admit_scope(ScopeKind::Wrap { why }, Hint::Exact(base), Some(input))
    }
}
