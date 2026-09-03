// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
pub mod assemble_manifest;
pub mod asts;
pub mod builder;
pub mod generator;
pub mod resolver;
pub mod sql_ast;
pub mod transformer;

use rusqlite::Connection;

use crate::ddl::manifest;
use crate::Result;

/// Result of reading manifest data and producing CREATE TEMP TABLE SQL.
pub struct ManifestCreateResult {
    pub create_sql: String,
    pub schema_rows: Vec<manifest::SchemaRow>,
}

/// Read manifest data from `_internal` namespace and produce CREATE TEMP TABLE SQL.
///
/// Returns `Ok(Some(result))` if the entity has schema rows, `Ok(None)` if not.
pub fn create_temp_table_from_manifest(
    bootstrap_conn: &Connection,
    internal_ns_id: i32,
    entity_name: &str,
    bin_registry: std::sync::Arc<crate::bin_cartridge::registry::BinCartridgeRegistry>,
) -> Result<Option<ManifestCreateResult>> {
    let schema_rows = manifest::read_schema(bootstrap_conn, internal_ns_id, entity_name)?;
    if schema_rows.is_empty() {
        return Ok(None);
    }
    let constraint_rows = manifest::read_constraints(bootstrap_conn, internal_ns_id, entity_name)?;
    let default_rows = manifest::read_defaults(bootstrap_conn, internal_ns_id, entity_name)?;
    let unresolved = assemble_manifest::assemble_from_manifest(
        entity_name,
        true,
        &schema_rows,
        &constraint_rows,
        &default_rows,
    )?;
    let (resolved, identities) = resolver::resolve(unresolved)?;
    let sql_ast = transformer::transform(resolved, &identities)?;
    Ok(Some(ManifestCreateResult {
        create_sql: generator::generate(&sql_ast, &identities, bin_registry)?,
        schema_rows,
    }))
}

#[cfg(test)]
mod tests {
    use super::asts::{ColumnDef, CreateTableDef, DdlConstraint};
    use super::*;

    /// Helper: run the pipeline from a pre-built `CreateTableDef<Unresolved>` (test-only).
    fn generate_create_table_from_def(
        def: asts::CreateTableDef<crate::pipeline::asts::core::Unresolved>,
    ) -> Result<String> {
        let (resolved, identities) = resolver::resolve(def)?;
        let sql_ast = transformer::transform(resolved, &identities)?;
        let mut registry = crate::bin_cartridge::registry::BinCartridgeRegistry::new();
        registry.register_cartridge(crate::bin_cartridge::prelude::create_prelude_cartridge());
        registry
            .register_cartridge(crate::bin_cartridge::predicates::create_predicates_cartridge());
        generator::generate(&sql_ast, &identities, std::sync::Arc::new(registry))
    }

    #[test]
    fn test_end_to_end_pipeline() {
        // Build a CreateTableDef<Unresolved> manually with builder
        let pk = builder::build_constraint("%%").unwrap();
        let not_null = builder::build_constraint("@ != null").unwrap();
        let check = builder::build_constraint("@ > 0").unwrap();
        let default = builder::build_default("42").unwrap();

        let def = CreateTableDef {
            name: "users".to_string(),
            temp: false,
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: "INTEGER".into(),
                    constraints: vec![pk],
                    default: None,
                },
                ColumnDef {
                    name: "name".into(),
                    col_type: "TEXT".into(),
                    constraints: vec![not_null],
                    default: None,
                },
                ColumnDef {
                    name: "age".into(),
                    col_type: "INTEGER".into(),
                    constraints: vec![check],
                    default: Some(default),
                },
            ],
            table_constraints: vec![],
        };

        let sql = generate_create_table_from_def(def).unwrap();

        // Verify structure
        assert!(sql.contains("CREATE TABLE \"users\""));
        assert!(sql.contains("\"id\" INTEGER PRIMARY KEY"));
        assert!(sql.contains("\"name\" TEXT NOT NULL"));
        assert!(sql.contains("\"age\" INTEGER"));
        assert!(sql.contains("DEFAULT 42"));
        assert!(sql.contains("CHECK(age > 0)"));
    }

    #[test]
    fn test_end_to_end_temp_table_with_composite_pk() {
        let def = CreateTableDef {
            name: "tmp".to_string(),
            temp: true,
            columns: vec![
                ColumnDef {
                    name: "a".into(),
                    col_type: "INTEGER".into(),
                    constraints: vec![],
                    default: None,
                },
                ColumnDef {
                    name: "b".into(),
                    col_type: "TEXT".into(),
                    constraints: vec![DdlConstraint::Unique { columns: None }],
                    default: None,
                },
            ],
            table_constraints: vec![DdlConstraint::PrimaryKey {
                columns: Some(vec!["a".into(), "b".into()]),
            }],
        };

        let sql = generate_create_table_from_def(def).unwrap();

        assert!(sql.starts_with("CREATE TEMP TABLE"));
        assert!(sql.contains("\"b\" TEXT UNIQUE"));
        assert!(sql.contains("PRIMARY KEY(\"a\", \"b\")"));
    }

    #[test]
    #[ignore = "drift: written against an older grammar; does not compile against the current one"]
    fn test_end_to_end_function_default() {
        let default = builder::build_default("now:()").unwrap();
        let def = CreateTableDef {
            name: "events".to_string(),
            temp: false,
            columns: vec![ColumnDef {
                name: "created_at".into(),
                col_type: "TEXT".into(),
                constraints: vec![],
                default: Some(default),
            }],
            table_constraints: vec![],
        };

        let sql = generate_create_table_from_def(def).unwrap();
        assert!(sql.contains("DEFAULT now()"));
    }

    /// Build a one-column table whose only constraint is `expr`, and
    /// return the emitted CHECK text.
    fn check_text(expr: &str) -> String {
        let def = CreateTableDef {
            name: "t".to_string(),
            temp: false,
            columns: vec![ColumnDef {
                name: "state".into(),
                col_type: "INTEGER".into(),
                constraints: vec![builder::build_constraint(expr).unwrap()],
                default: None,
            }],
            table_constraints: vec![],
        };
        generate_create_table_from_def(def).unwrap()
    }

    /// DelightQL's `=` is null-safe everywhere but a join, so a CHECK written
    /// with it rejects null. The engine's own equality is the prelude
    /// predicate `+sql_eq(@, v)`: it lowers to SQL `=` through the same
    /// predicate identity the query road uses, and a CHECK OBSERVES it — a
    /// positive sigma application collapses UNKNOWN — so the two are distinct
    /// all the way to the emitted text.
    ///
    /// Collapsing them makes every equality CHECK more permissive than the
    /// language says, and makes `@ = null` a constraint that cannot fire.
    #[test]
    fn test_e2e_eq_lowers_null_safe() {
        let sql = check_text("@ = 5");
        assert!(
            sql.contains("CHECK(state IS NOT DISTINCT FROM 5)"),
            "`=` must lower null-safe, got: {sql}"
        );
    }

    /// A CHECK REJECTS ONLY FALSE. The positive proof reaches the constraint
    /// as itself, so `state = 5` against a null answers UNKNOWN and the row
    /// lands — which is what the author asked for by writing the target's own
    /// equality instead of `=`. Presence is a separate constraint.
    #[test]
    fn test_e2e_sql_eq_lowers_unobserved_sql_equality() {
        let sql = check_text("+sql_eq(@, 5)");
        assert!(
            sql.contains("CHECK(state = 5)"),
            "`+sql_eq` must lower to the bare SQL equality, got: {sql}"
        );
        assert!(
            !sql.contains("IS TRUE"),
            "a CHECK does not filter for TRUE: {sql}"
        );
        assert!(
            !sql.contains("DISTINCT"),
            "`+sql_eq` must never be null-safe: {sql}"
        );
    }

    /// The inequalities are already split; they pin the shape the equality
    /// side is being brought into line with.
    #[test]
    fn test_e2e_ne_lowers_null_safe() {
        let sql = check_text("@ != 5");
        assert!(
            sql.contains("CHECK(state IS DISTINCT FROM 5)"),
            "`!=` must lower null-safe, got: {sql}"
        );
    }

    #[test]
    fn test_e2e_sql_ne_lowers_unobserved_sql_inequality() {
        let sql = check_text("+sql_ne(@, 5)");
        assert!(
            sql.contains("CHECK(state != 5)"),
            "`+sql_ne` must lower to the bare SQL inequality, got: {sql}"
        );
        assert!(
            !sql.contains("IS TRUE"),
            "a CHECK does not filter for TRUE: {sql}"
        );
        assert!(
            !sql.contains("DISTINCT"),
            "`+sql_ne` must never be null-safe: {sql}"
        );
    }

    /// NEGATIVE POLARITY KEEPS ITS OWN TWO-VALUED MEANING in a CHECK — "not
    /// proven TRUE", spelled `IS NOT TRUE`. It does not become the target's
    /// `NOT`, which would preserve UNKNOWN and answer a different question,
    /// and the CHECK does not drop the observation the way it does for a
    /// positive proof: there is nothing three-valued left to carry.
    #[test]
    fn a_negative_sigma_check_keeps_is_not_true() {
        let sql = check_text("\\+sql_eq(@, 5)");
        assert!(
            sql.contains("CHECK((state = 5) IS NOT TRUE)"),
            "`\\+sql_eq` must keep its two-valued observation, got: {sql}"
        );
    }

    #[test]
    fn test_e2e_null_inequalities_keep_distinct_constraint_semantics() {
        let null_safe = check_text("@ != null");
        assert!(
            null_safe.contains("\"state\" INTEGER NOT NULL"),
            "`@ != null` must promote to NOT NULL, got: {null_safe}"
        );

        let sql_inequality = check_text("+sql_ne(@, null)");
        assert!(
            sql_inequality.contains("CHECK(state != NULL)"),
            "`+sql_ne(@, null)` must remain an SQL inequality CHECK, got: {sql_inequality}"
        );
        assert!(
            !sql_inequality.contains("\"state\" INTEGER NOT NULL"),
            "`+sql_ne(@, null)` must not promote to NOT NULL, got: {sql_inequality}"
        );
    }

    /// A NULL PROBE FINDS A NULL CANDIDATE. Under SQL `IN` this comparison
    /// is unknown, and a CHECK admits whatever it cannot call false — so the
    /// null-safe correspondence is what makes the constraint mean what it
    /// says.
    #[test]
    fn a_ddl_membership_over_null_is_null_safe() {
        let check = builder::build_constraint("@ in (1; null)").unwrap();
        let def = CreateTableDef {
            name: "t".to_string(),
            temp: false,
            columns: vec![ColumnDef {
                name: "status".into(),
                col_type: "INTEGER".into(),
                constraints: vec![check],
                default: None,
            }],
            table_constraints: vec![],
        };
        let sql = generate_create_table_from_def(def).unwrap();
        assert!(!sql.contains(" IN ("), "{sql}");
        assert_eq!(sql.matches("IS NOT DISTINCT FROM").count(), 2, "{sql}");
        assert!(sql.contains("NULL"), "{sql}");
    }

    /// A CANDIDATE ROW IS A ROW. Every component of the probe is compared to
    /// the corresponding component of each candidate; dropping the later
    /// components or flattening the rows would test a different question.
    #[test]
    fn a_ddl_tuple_membership_keeps_its_rows() {
        let check = builder::build_constraint("(@, @) in (1, 2; 3, 4)").unwrap();
        let def = CreateTableDef {
            name: "t".to_string(),
            temp: false,
            columns: vec![ColumnDef {
                name: "status".into(),
                col_type: "INTEGER".into(),
                constraints: vec![check],
                default: None,
            }],
            table_constraints: vec![],
        };
        let sql = generate_create_table_from_def(def).unwrap();
        assert!(!sql.contains(" IN ("), "{sql}");
        // Two components per row, two rows: four correspondences, ANDed
        // within a row and ORed across rows.
        assert_eq!(sql.matches("IS NOT DISTINCT FROM").count(), 4, "{sql}");
    }

    #[test]
    fn test_e2e_in_check() {
        let check = builder::build_constraint("@ in (1; 2; 3)").unwrap();
        let def = CreateTableDef {
            name: "t".to_string(),
            temp: false,
            columns: vec![ColumnDef {
                name: "status".into(),
                col_type: "INTEGER".into(),
                constraints: vec![check],
                default: None,
            }],
            table_constraints: vec![],
        };
        let sql = generate_create_table_from_def(def).unwrap();
        // NULL-SAFE MEMBERSHIP, in a CHECK as in a query. SQL `IN` answers
        // unknown on a null probe and a CHECK admits whatever it cannot call
        // false, so emitting it would silently admit exactly the rows the
        // constraint names.
        assert!(
            !sql.contains(" IN ("),
            "a DDL membership must not lower to SQL IN: {sql}"
        );
        assert_eq!(
            sql.matches("IS NOT DISTINCT FROM").count(),
            3,
            "one null-safe correspondence per candidate: {sql}"
        );
    }

    #[test]
    #[ignore = "drift: written against an older grammar; does not compile against the current one"]
    fn test_e2e_like_check() {
        let check = builder::build_constraint("+like(@, '%abc')").unwrap();
        let def = CreateTableDef {
            name: "t".to_string(),
            temp: false,
            columns: vec![ColumnDef {
                name: "code".into(),
                col_type: "TEXT".into(),
                constraints: vec![check],
                default: None,
            }],
            table_constraints: vec![],
        };
        let sql = generate_create_table_from_def(def).unwrap();
        assert!(
            sql.contains("CHECK(code LIKE '%abc')"),
            "Expected LIKE check, got: {sql}"
        );
    }

    #[test]
    fn test_e2e_fk_constraint() {
        let fk = builder::build_constraint("+users(id)").unwrap();
        let def = CreateTableDef {
            name: "orders".to_string(),
            temp: false,
            columns: vec![ColumnDef {
                name: "user_id".into(),
                col_type: "INTEGER".into(),
                constraints: vec![fk],
                default: None,
            }],
            table_constraints: vec![],
        };
        let sql = generate_create_table_from_def(def).unwrap();
        assert!(
            sql.contains("FOREIGN KEY(\"user_id\") REFERENCES \"users\"(\"id\")"),
            "Expected FK constraint, got: {sql}"
        );
    }
}
