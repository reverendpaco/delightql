//! `consult_tree!()` pseudo-predicate implementation
//!
//! Syntax: `consult_tree!(dir_path, root_namespace)`
//!
//! Example: `consult_tree!("models/", "models")`
//!
//! Recursively walks a directory tree, discovers `.dql` files, and consults
//! each one into a namespace derived from the directory structure.
//!
//! Returns a multi-row result table: one row per file consulted.

use crate::bin_cartridge::{
    BinEntity, EffectExecutable, EntityResult, EntitySignature, OutputSchema, Parameter,
};
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;
use std::path::Path;

/// consult_tree!() pseudo-predicate entity
pub struct ConsultTreePredicate;

impl BinEntity for ConsultTreePredicate {
    fn name(&self) -> &str {
        "consult_tree!"
    }

    fn entity_type(&self) -> EntityType {
        EntityType::BinPseudoPredicate
    }

    fn signature(&self) -> EntitySignature {
        EntitySignature {
            parameters: vec![
                Parameter {
                    name: "dir_path".to_string(),
                    data_type: "String".to_string(),
                    _is_optional: false,
                },
                Parameter {
                    name: "root_namespace".to_string(),
                    data_type: "String".to_string(),
                    _is_optional: false,
                },
            ],
            output_schema: OutputSchema::Relation(vec![
                ("file_path".to_string(), "String".to_string()),
                ("namespace".to_string(), "String".to_string()),
                ("definitions".to_string(), "Integer".to_string()),
            ]),
        }
    }

    fn has_side_effects(&self) -> bool {
        true
    }

    fn as_effect_executable(&self) -> Option<&dyn EffectExecutable> {
        Some(self)
    }
}

impl EffectExecutable for ConsultTreePredicate {
    fn execute(
        &self,
        arguments: &[DomainExpression],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        if arguments.len() != 2 {
            return Err(DelightQLError::database_error(
                format!(
                    "consult_tree!() expects 2 arguments (dir_path, root_namespace), got {}",
                    arguments.len()
                ),
                "Invalid argument count",
            ));
        }

        let dir_path = super::consult::extract_string_literal(&arguments[0], "dir_path")?;
        let root_namespace =
            super::consult::extract_string_literal(&arguments[1], "root_namespace")?;

        if root_namespace.is_empty() {
            return Err(DelightQLError::database_error(
                "consult_tree!() root_namespace cannot be empty",
                "Empty namespace name",
            ));
        }

        let dir = Path::new(&dir_path);
        if !dir.exists() || !dir.is_dir() {
            return Err(DelightQLError::database_error(
                format!(
                    "consult_tree!() directory '{}' does not exist or is not a directory",
                    dir_path
                ),
                "Invalid directory path",
            ));
        }

        // Collect all .dql files recursively
        let mut dql_files = Vec::new();
        collect_dql_files(dir, &mut dql_files)?;
        dql_files.sort();

        // Consult each file
        let mut rows = Vec::new();
        for file_path in &dql_files {
            let relative = file_path.strip_prefix(dir).unwrap_or(file_path.as_path());
            let stem = relative
                .to_string_lossy()
                .strip_suffix(".dql")
                .unwrap_or(&relative.to_string_lossy())
                .to_string();
            let ns_suffix = stem.replace('/', "::");
            let namespace = format!("{}::{}", root_namespace, ns_suffix);

            let file_path_str = file_path.to_string_lossy().to_string();
            let count = super::consult::execute_consult(
                system,
                &file_path_str,
                &namespace,
                Some(&root_namespace),
            )?;

            rows.push(Row {
                values: vec![
                    DomainExpression::Literal {
                        value: LiteralValue::String(file_path_str),
                        alias: None,
                    },
                    DomainExpression::Literal {
                        value: LiteralValue::String(namespace),
                        alias: None,
                    },
                    DomainExpression::Literal {
                        value: LiteralValue::Number(count.to_string()),
                        alias: None,
                    },
                ],
            });
        }

        if rows.is_empty() {
            return Err(DelightQLError::database_error(
                format!("consult_tree!() found no .dql files in '{}'", dir_path),
                "Empty directory tree",
            ));
        }

        let headers = vec![
            DomainExpression::lvar_builder("file_path".to_string()).build(),
            DomainExpression::lvar_builder("namespace".to_string()).build(),
            DomainExpression::lvar_builder("definitions".to_string()).build(),
        ];

        let result_table = Relation::Anonymous {
            column_headers: Some(headers),
            rows,
            alias: alias.map(|s| s.into()),
            outer: false,
            exists_mode: false,
            qua_target: None,
            cpr_schema: PhaseBox::phantom(),
        };

        Ok(EntityResult::Relation(result_table))
    }
}

/// Recursively collect all `.dql` files under a directory.
fn collect_dql_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) -> Result<()> {
    let entries = std::fs::read_dir(dir).map_err(|e| {
        DelightQLError::database_error(
            format!(
                "consult_tree!() failed to read directory '{}': {}",
                dir.display(),
                e
            ),
            "Directory read error",
        )
    })?;

    for entry in entries {
        let entry = entry.map_err(|e| {
            DelightQLError::database_error(
                format!("consult_tree!() directory entry error: {}", e),
                "Directory read error",
            )
        })?;
        let path = entry.path();
        if path.is_dir() {
            collect_dql_files(&path, out)?;
        } else if path.extension().and_then(|e| e.to_str()) == Some("dql") {
            out.push(path);
        }
    }

    Ok(())
}
