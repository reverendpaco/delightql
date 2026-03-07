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
    let resolved = resolver::resolve(unresolved)?;
    let sql_ast = transformer::transform(resolved)?;
    Ok(Some(ManifestCreateResult {
        create_sql: generator::generate(&sql_ast),
        schema_rows,
    }))
}

#[cfg(test)]
mod tests {
    use super::asts::{ColumnDef, CreateTableDef, DdlConstraint, DdlDefault};
    use super::*;

    /// Helper: run the pipeline from a pre-built `CreateTableDef<Unresolved>` (test-only).
    fn generate_create_table_from_def(
        def: asts::CreateTableDef<crate::pipeline::asts::core::Unresolved>,
    ) -> Result<String> {
        let resolved = resolver::resolve(def)?;
        let sql_ast = transformer::transform(resolved)?;
        Ok(generator::generate(&sql_ast))
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
        assert!(
            sql.contains("CHECK(status IN (1, 2, 3))"),
            "Expected IN check, got: {sql}"
        );
    }

    #[test]
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
