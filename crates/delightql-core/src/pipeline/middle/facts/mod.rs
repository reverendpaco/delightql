// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Analyses over the completed representation. Each takes the frozen
//! `Graph`, which exists only after `finish`, runs once, and owns its
//! result.

pub(crate) mod activation;
