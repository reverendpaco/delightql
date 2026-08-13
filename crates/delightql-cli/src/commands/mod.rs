// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Command handlers for DelightQL CLI
//!
//! This module organizes the CLI into distinct subcommands:
//! - query: Execute queries (string/file/stdin/REPL)
//! - format: Format/prettify DelightQL code
//! - jstruct: JSON destructuring from stdin (dql tools jstruct)

pub mod book;
pub mod csvstruct;
pub mod delegate;
pub mod editor;
pub mod explain;
pub mod filemunge;
pub mod format;
pub mod jstruct;
pub mod man;
pub mod query;
pub mod selftest;
pub mod target;
