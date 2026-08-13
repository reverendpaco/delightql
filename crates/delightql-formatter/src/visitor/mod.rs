// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The typed visitor.
//!
//! Layout decisions read the typed CST — fields, generated supertype enums,
//! authored spans — and never a node-kind string, a source regex, or a
//! reparse. What the formatter takes no position on is ECHOED from the
//! author's own tokens, which is why coverage is not a race against the
//! grammar's size.

mod chain;
mod core;
mod lets;
mod stage;
mod verbatim;

pub use core::{Branch, Formatter};
