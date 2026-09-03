// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The metadata group — `Title:~> {…}`, where a column's VALUES become the
//! record's keys.
//!
//! REDUCTION POSITION ONLY. A metadata group is not a domain expression and
//! not a record constructor member: it yields an interior RECORD keyed by
//! data, which only a reduction can compress. Its key is phase-selected, so
//! the authored characters and the bound occurrence are one carrier rather
//! than an authored/resolved pair that can drift.

use super::super::{Phase, Unresolved};
use super::enclyph::Enclyph;
use super::metadata_types::CteRequirements;
use crate::{lispy::ToLispy, ToLispy};

/// `key_column ':~>' meta_target` — one metadata key per level.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("metadata_group")]
pub struct MetadataGroup<P: Phase = Unresolved> {
    /// The column whose values become this level's keys.
    pub key: P::Col,
    pub target: MetadataTarget<P>,
    /// What the reduction lowering owes this level. `None` until the
    /// tree-group analysis has decided.
    pub cte_requirements: Option<CteRequirements<P>>,
    /// Whether the target SUMMARIZES its group — every constructed member
    /// reduces — so each key holds one object rather than an array of the
    /// group's rows. Decided at resolution, where reductions are known.
    pub summary: bool,
}

/// `meta_target = enclyph_like | metadata_group` — the levels chain, and the
/// bottom of a chain is always a constructed value.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum MetadataTarget<P: Phase = Unresolved> {
    #[lispy("meta_target:enclyph")]
    Enclyph(Enclyph<P>),
    #[lispy("meta_target:group")]
    Group(Box<MetadataGroup<P>>),
}
