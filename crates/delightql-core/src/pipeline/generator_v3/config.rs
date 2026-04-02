// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::dialect::SqlDialect;
#[derive(Clone)]
pub struct GeneratorConfig {
    pub dialect: SqlDialect,
    pub indent_width: usize,
    pub pretty_print: bool,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        GeneratorConfig {
            dialect: SqlDialect::default(),
            indent_width: 2,
            pretty_print: true,
        }
    }
}
