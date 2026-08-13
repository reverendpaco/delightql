// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The compiler's door to the syntax front door.
//!
//! `delightql-cst` owns parsing, typed access, tooling tokens, concrete
//! structure, defects and authored spans. It answers "what did the author
//! write?"; every judgment about what the writing MEANS is on this side of the
//! boundary, in [`super::normalize`].
//!
//! This module is a re-export and nothing else. An internal alias adds no
//! second authority — there is one generated typed API, and naming it here
//! keeps the compiler's imports short without giving anyone a place to put a
//! wrapper that quietly re-decides something.

pub use delightql_cst::cst;
pub use delightql_cst::{
    outermost, walk, CompanionColumn, Defect, DefectKind, Parser, Root,
    SyntaxTree, Token, TypedNode,
};
