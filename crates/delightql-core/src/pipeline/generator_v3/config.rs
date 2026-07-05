// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::dialect::SqlDialect;
use crate::pipeline::dialect_pack::DialectPack;
use std::sync::Arc;

#[derive(Clone)]
pub struct GeneratorConfig {
    pub dialect: SqlDialect,
    /// Per-compile image of the dialect_* targeting tables. Empty for
    /// standalone paths — every render falls back to canonical (SQLite).
    pub dialect_pack: Arc<DialectPack>,
    pub indent_width: usize,
    pub pretty_print: bool,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        GeneratorConfig {
            dialect: SqlDialect::default(),
            dialect_pack: Arc::new(DialectPack::empty()),
            indent_width: 2,
            pretty_print: true,
        }
    }
}
