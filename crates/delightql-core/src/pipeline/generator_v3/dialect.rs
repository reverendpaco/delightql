// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
#[derive(Debug, Clone, Copy, PartialEq, Default)]
#[allow(dead_code)]
pub enum SqlDialect {
    #[default]
    SQLite,
    PostgreSQL,
    MySQL,
    SqlServer,
}
