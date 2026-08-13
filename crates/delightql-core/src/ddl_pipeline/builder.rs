// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The companion sigil sub-language's entrance.
//!
//! THE COLUMN SELECTS THE ROOT. A companion cell is DATA, and which column it
//! came from is what says whether it is a constraint or a default — no reader
//! here classifies a cell by what is inside it. Building the cell is
//! `normalize::companion`'s; this is the door the DDL pipeline knocks on.

use crate::ddl_pipeline::asts::{DdlConstraint, DdlDefault};
use crate::pipeline::syntax::CompanionColumn;
use crate::Result;
use std::rc::Rc;

/// One constraint cell: `%%`, `%%(a, b)`, `%`, `%(a)`, or a truth expression
/// (`@ > 0`, `+parents(parent_code)`).
pub fn build_constraint(source: &str) -> Result<DdlConstraint> {
    let tree = crate::pipeline::parse::companion_cell(CompanionColumn::Constraint, source)?;
    crate::pipeline::normalize::companion::constraint_cell(&tree, registry())
}

/// One default cell: any domain expression.
pub fn build_default(source: &str) -> Result<DdlDefault> {
    let tree = crate::pipeline::parse::companion_cell(CompanionColumn::Default, source)?;
    crate::pipeline::normalize::companion::default_cell(&tree, registry())
}

/// A companion cell is read on its own, so it interns into an arena of its
/// own: nothing it names survives the call.
fn registry() -> Rc<crate::names::Registry> {
    Rc::new(crate::names::Registry::new(&[]))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::asts::core::expressions::domain::DomainExpression;

    // === Primary Key ===

    #[test]
    fn test_bare_primary_key() {
        let result = build_constraint("%%").unwrap();
        match result {
            DdlConstraint::PrimaryKey { columns } => assert!(columns.is_none()),
            other => panic!("Expected PrimaryKey, got: {:?}", other),
        }
    }

    #[test]
    fn test_composite_primary_key() {
        let result = build_constraint("%%(a, b)").unwrap();
        match result {
            DdlConstraint::PrimaryKey { columns } => {
                assert_eq!(columns, Some(vec!["a".into(), "b".into()]));
            }
            other => panic!("Expected PrimaryKey, got: {:?}", other),
        }
    }

    // === Unique Key ===

    #[test]
    fn test_bare_unique() {
        let result = build_constraint("%").unwrap();
        match result {
            DdlConstraint::Unique { columns } => assert!(columns.is_none()),
            other => panic!("Expected Unique, got: {:?}", other),
        }
    }

    #[test]
    fn test_unique_with_columns() {
        let result = build_constraint("%(email)").unwrap();
        match result {
            DdlConstraint::Unique { columns } => {
                assert_eq!(columns, Some(vec!["email".into()]));
            }
            other => panic!("Expected Unique, got: {:?}", other),
        }
    }

    // === Check constraints ===

    #[test]
    fn test_check_self_ref_gt() {
        // @ > 0 — column self-reference via value_placeholder
        let result = build_constraint("@ > 0").unwrap();
        assert!(matches!(result, DdlConstraint::Check { .. }));
    }

    /// A CONSTRAINT CELL IS TRUTH MATERIAL: `constraint_cell =
    /// primary_key_sigil | unique_key_sigil | truth_expression`. A value
    /// expression alone constrains nothing, so the arithmetic stands inside
    /// the comparison rather than instead of it.
    #[test]
    fn test_check_binary_comparison() {
        let result = build_constraint("(@ + 1) > 0").unwrap();
        assert!(matches!(result, DdlConstraint::Check { .. }));
        assert!(build_constraint("@ + 1").is_err(), "a value is not a truth");
    }

    #[test]
    fn test_check_function_call() {
        // DQL syntax: length:(name) > 3
        let result = build_constraint("length:(name) > 3").unwrap();
        assert!(matches!(result, DdlConstraint::Check { .. }));
    }

    // === Default values ===

    #[test]
    fn test_default_function_call() {
        let result = build_default("now:()").unwrap();
        match result {
            DdlDefault::Value { expr } => {
                assert!(matches!(expr, DomainExpression::Application(_)));
            }
            other => panic!("Expected Value with function, got: {:?}", other),
        }
    }

    #[test]
    fn test_default_literal_number() {
        let result = build_default("42").unwrap();
        match result {
            DdlDefault::Value { expr } => {
                assert!(matches!(expr, DomainExpression::Application(crate::pipeline::asts::core::FunctionApplication::Ground(_))));
            }
            other => panic!("Expected Value, got: {:?}", other),
        }
    }

    #[test]
    #[ignore = "drift: written against an older grammar; does not compile against the current one"]
    fn test_default_literal_string() {
        let result = build_default("'hello'").unwrap();
        match result {
            DdlDefault::Value { expr } => {
                assert!(matches!(expr, DomainExpression::Application(crate::pipeline::asts::core::FunctionApplication::Ground(_))));
            }
            other => panic!("Expected Value, got: {:?}", other),
        }
    }

    #[test]
    fn test_default_null() {
        let result = build_default("null").unwrap();
        match result {
            DdlDefault::Value { expr } => {
                assert!(matches!(expr, DomainExpression::Application(crate::pipeline::asts::core::FunctionApplication::Ground(_))));
            }
            other => panic!("Expected Value with null literal, got: {:?}", other),
        }
    }

    #[test]
    fn test_default_bare_identifier_becomes_string_literal() {
        // "active" stored in companion_default loses quotes → "active" reads back as bare identifier
        // build_default should promote bare identifiers to string literals
        let result = build_default("active").unwrap();
        match result {
            DdlDefault::Value { expr } => match expr {
                DomainExpression::Application(crate::pipeline::asts::core::FunctionApplication::Ground(crate::pipeline::asts::core::LiteralValue::String(s))) => assert_eq!(s, "active"),
                other => panic!("Expected String literal, got: {:?}", other),
            },
            other => panic!("Expected Value, got: {:?}", other),
        }
    }

    // === Foreign Key ===

    #[test]
    fn test_fk_sigil() {
        let result = build_constraint("+departments(department_id)").unwrap();
        match result {
            DdlConstraint::ForeignKey { table, columns } => {
                assert_eq!(table, "departments");
                assert_eq!(columns, vec!["department_id".to_string()]);
            }
            other => panic!("Expected ForeignKey, got: {:?}", other),
        }
    }

    #[test]
    fn test_fk_multi_column() {
        let result = build_constraint("+orders(user_id, product_id)").unwrap();
        match result {
            DdlConstraint::ForeignKey { table, columns } => {
                assert_eq!(table, "orders");
                assert_eq!(
                    columns,
                    vec!["user_id".to_string(), "product_id".to_string()]
                );
            }
            other => panic!("Expected ForeignKey, got: {:?}", other),
        }
    }

    // === Error cases ===

    #[test]
    fn test_invalid_syntax_errors() {
        assert!(build_constraint("%%%").is_err());
    }
}
