// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Target-neutral vocabulary consumed by manifest assembly and correspondence.

use crate::diagnostic::Manifest as ManifestDiagnostic;
use crate::error::{DelightQLError, Result};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Materialization {
    Table,
    View,
}

impl Materialization {
    pub fn parse(raw: &str) -> Result<Self> {
        match raw {
            "table" => Ok(Materialization::Table),
            "view" => Ok(Materialization::View),
            other => Err(DelightQLError::from(ManifestDiagnostic::Materialization {
                message: format!(
                    "imprinting() materialization '{}' is not recognized — valid values are \"table\" or \"view\"",
                    other
                ),
            })),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Extent {
    Permanent,
    Temporary,
}

impl Extent {
    pub fn parse(raw: &str) -> Result<Self> {
        match raw {
            "permanent" => Ok(Extent::Permanent),
            "temporary" => Ok(Extent::Temporary),
            other => Err(DelightQLError::from(ManifestDiagnostic::Extent {
                message: format!(
                    "imprinting() extent '{}' is not recognized — valid values are \"permanent\" or \"temporary\"",
                    other
                ),
            })),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ImprintingRow {
    pub entity: String,
    pub materialization: Materialization,
    pub extent: Extent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaRow {
    pub name: String,
    pub col_type: String,
    /// Every row states its column's physical position; a cell that is not
    /// an integer is kept as written so the refusal can name it.
    pub ordinal: OrdinalCell,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrdinalCell {
    Integer(i64),
    Other(String),
}

#[derive(Clone, Debug)]
pub struct ConstraintRow {
    pub column: String,
    pub constraint: String,
    pub constraint_name: String,
}

#[derive(Clone, Debug)]
pub struct DefaultRow {
    pub column: String,
    pub default_val: String,
}
