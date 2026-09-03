// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Resolution subsystem for DelightQL
//!
//! The durable resolver core: catalog readers, the built-in vocabulary, and
//! the relation authority's planning capability. Lexical bindings live in the
//! one lexical world the resolver stands in (`crate::defuse::environment`).

pub mod entity;
pub mod registry;

pub use entity::*;
pub use registry::*;
