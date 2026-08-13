// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::pipeline::asts::core::expressions::domain::DomainExpression;
use crate::pipeline::asts::core::expressions::truth::TruthExpression;
use crate::pipeline::asts::core::{Resolved, Unresolved};

pub trait DdlPhase: Clone + crate::pipeline::asts::core::Phase {
    type Scope: Clone + std::fmt::Debug;
    type Column: Clone + std::fmt::Debug;
}

impl DdlPhase for Unresolved {
    type Scope = String;
    type Column = String;
}

impl DdlPhase for Resolved {
    type Scope = crate::names::ScopeId;
    type Column = crate::names::ColId;
}

/// Kind of generated column (VIRTUAL vs STORED).
#[derive(Debug, Clone, PartialEq)]
pub enum GeneratedKind {
    Virtual,
    Stored,
}

impl GeneratedKind {
    /// Parse a manifest generated-column kind without inventing a fallback.
    pub fn parse(raw: &str) -> crate::Result<Self> {
        match raw.to_ascii_lowercase().as_str() {
            "virtual" => Ok(Self::Virtual),
            "stored" => Ok(Self::Stored),
            _ => Err(crate::DelightQLError::validation_error_categorized(
                "imprint/manifest/generated_kind",
                format!(
                    "generated column kind '{}' is not recognized; valid values are \
                     \"virtual\" or \"stored\"",
                    raw
                ),
                "use \"virtual\" or \"stored\" in the generated column of defaults()",
            )),
        }
    }
}

/// A constraint on a column or table (DDL AST layer).
#[derive(Debug, Clone)]
pub enum DdlConstraint<Phase: DdlPhase = Unresolved> {
    PrimaryKey {
        columns: Option<Vec<<Phase as DdlPhase>::Column>>,
    },
    Unique {
        columns: Option<Vec<<Phase as DdlPhase>::Column>>,
    },
    NotNull,
    /// A CHECK's body is a TRUTH — the constraint accepts or rejects a row —
    /// so it is carried as one. A value standing here has no derivation.
    Check {
        expr: TruthExpression<Phase>,
    },
    ForeignKey {
        table: <Phase as DdlPhase>::Scope,
        columns: Vec<<Phase as DdlPhase>::Column>,
    },
}

/// A default value specification (DDL AST layer).
#[derive(Debug, Clone)]
pub enum DdlDefault<Phase: DdlPhase = Unresolved> {
    Value {
        expr: DomainExpression<Phase>,
    },
    Generated {
        expr: DomainExpression<Phase>,
        kind: GeneratedKind,
    },
}

/// A column definition within a CREATE TABLE.
#[derive(Debug, Clone)]
pub struct ColumnDef<Phase: DdlPhase = Unresolved> {
    pub name: <Phase as DdlPhase>::Column,
    pub col_type: String,
    pub constraints: Vec<DdlConstraint<Phase>>,
    pub default: Option<DdlDefault<Phase>>,
}

/// A complete CREATE TABLE definition (DDL AST layer).
#[derive(Debug, Clone)]
pub struct CreateTableDef<Phase: DdlPhase = Unresolved> {
    pub name: <Phase as DdlPhase>::Scope,
    pub temp: bool,
    pub columns: Vec<ColumnDef<Phase>>,
    pub table_constraints: Vec<DdlConstraint<Phase>>,
}

#[cfg(test)]
mod tests {
    use super::GeneratedKind;

    #[test]
    fn generated_kind_accepts_both_values_and_refuses_unknown_spelling() {
        assert_eq!(
            GeneratedKind::parse("virtual").unwrap(),
            GeneratedKind::Virtual
        );
        assert_eq!(
            GeneratedKind::parse("STORED").unwrap(),
            GeneratedKind::Stored
        );

        let error = GeneratedKind::parse("storedd").unwrap_err();
        assert_eq!(
            error.error_uri(),
            "delightql-error://imprint/manifest/generated_kind"
        );
        assert!(error.to_string().contains("storedd"));
    }
}
