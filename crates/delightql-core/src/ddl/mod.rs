// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! DDL definition parsing, building, and analysis.
//!
//! - `analyzer`: Extracts entity references for dependency tracking
//! - `lifecycle`: Catalog lifecycle eligibility — inertness and the imprint-source proofs
//! - `manifest`: Reads `_internal` companions through a judged live source
//! - `reconstruct`: Reads stored definition source back into a typed group

pub mod analyzer;
#[cfg(not(target_arch = "wasm32"))]
pub mod lifecycle;
#[cfg(not(target_arch = "wasm32"))]
pub mod manifest;
pub mod reconstruct;
