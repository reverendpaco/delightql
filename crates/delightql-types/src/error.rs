// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The error type every crate names: the root of the diagnostic hierarchy.
//!
//! `DelightQLError` is `crate::diagnostic::DelightQLError` — a nested,
//! payload-bearing taxonomy. There is no constructor here that takes a
//! message and guesses an identity, and no identity string a producer can
//! spell: a producer constructs the leaf its refusal IS and converts it
//! with `.into()`.

pub use crate::diagnostic::{DelightQLError, ErrorId, Result};
pub use crate::taxon::ERROR_URI_SCHEME;
