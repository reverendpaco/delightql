// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The client's own database: one in-memory SQLite per process, in EVERY
//! mode — interactive, one-shot, server, worker. It carries the process
//! context (`session`, `argument`, `environment`), the ordered input
//! ledger, the incident record, and the effective configuration, and it
//! is mounted into every session as `repl::data` with fixed projections.
//!
//! Core owns facts about the language (`sys::diagnostics.finding`); this
//! database owns facts about the process. The two are projected together
//! at exit, never merged.
pub mod bug;
pub mod context;
pub mod exit;
pub mod incident;
pub mod database;
pub mod mount;
#[cfg(feature = "repl")]
pub mod replay;
