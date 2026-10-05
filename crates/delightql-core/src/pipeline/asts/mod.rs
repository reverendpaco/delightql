// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

pub mod core; // Public - needed for SQL AST provenance
pub mod ddl;
pub mod effects;
pub(crate) mod package;
pub mod unresolved;
pub mod vocabulary;
