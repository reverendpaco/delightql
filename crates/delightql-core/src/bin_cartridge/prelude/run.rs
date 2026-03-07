//! `run!()` pseudo-predicate implementation
//!
//! Syntax: `run!(file_path)`
//!
//! Example: `run!("scripts/setup.dql")`
//!
//! ## Behavior
//!
//! 1. Read file at specified path
//! 2. Parse as sequential query statements (using multi-query parser)
//! 3. Execute each query sequentially with current system
//! 4. Return summary table (queries executed, rows affected)

use crate::bin_cartridge::{
    BinEntity, EffectExecutable, EntityResult, EntitySignature, OutputSchema, Parameter,
};
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::{builder_v2, parser};

/// run!() pseudo-predicate entity
pub struct RunPredicate;

impl BinEntity for RunPredicate {
    fn name(&self) -> &str {
        "run!"
    }

    fn entity_type(&self) -> EntityType {
        EntityType::BinPseudoPredicate
    }

    fn signature(&self) -> EntitySignature {
        EntitySignature {
            parameters: vec![Parameter {
                name: "file_path".to_string(),
                data_type: "String".to_string(),
                _is_optional: false,
            }],
            output_schema: OutputSchema::Relation(vec![
                ("status".to_string(), "String".to_string()),
                ("file_path".to_string(), "String".to_string()),
                ("queries_executed".to_string(), "Int".to_string()),
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

impl EffectExecutable for RunPredicate {
    fn execute(
        &self,
        arguments: &[DomainExpression],
        _alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        // Validate argument count
        if arguments.len() != 1 {
            return Err(DelightQLError::database_error(
                format!(
                    "run!() expects 1 argument (file_path), got {}",
                    arguments.len()
                ),
                "Invalid argument count",
            ));
        }

        // Extract file_path from first argument (must be string literal)
        let file_path = extract_string_literal(&arguments[0], "file_path")?;

        // Validate file path
        if file_path.is_empty() {
            return Err(DelightQLError::database_error(
                "run!() file_path cannot be empty",
                "Empty file path",
            ));
        }

        // Read file contents
        let source_code = std::fs::read_to_string(&file_path).map_err(|e| {
            DelightQLError::database_error(
                format!("run!() failed to read file '{}': {}", file_path, e),
                "File read error",
            )
        })?;

        // Parse file as queries
        let tree = parser::parse(&source_code).map_err(|e| {
            DelightQLError::database_error(
                format!("run!() failed to parse file '{}': {}", file_path, e),
                "Parse error",
            )
        })?;

        let (queries, _features, _assertions, _emits, _dangers, _options, _ddl_blocks) =
            builder_v2::parse_queries(&tree, &source_code).map_err(|e| {
                DelightQLError::database_error(
                    format!("run!() failed to build AST for file '{}': {}", file_path, e),
                    "Build error",
                )
            })?;

        if queries.is_empty() {
            return Err(DelightQLError::database_error(
                format!("run!() file '{}' contains no queries", file_path),
                "No queries found",
            ));
        }

        let query_count = queries.len();

        // Execute each query sequentially with the current system
        for (i, query) in queries.into_iter().enumerate() {
            log::debug!(
                "run!(): Executing query {}/{} from '{}'",
                i + 1,
                query_count,
                file_path
            );

            // Execute query by recursively calling effect executor
            // This allows nested pseudo-predicates in the file
            let _rewritten_query =
                crate::pipeline::effect_executor::execute_effects(query, system)?;

            // Note: We execute for side effects, don't collect results
            // This is intentional - run!() is for executing scripts, not collecting data
        }

        log::debug!(
            "run!(): Successfully executed {} queries from '{}'",
            query_count,
            file_path
        );

        // Return success result table
        let result_table = create_success_table(&file_path, query_count, _alias);
        Ok(EntityResult::Relation(result_table))
    }
}

/// Extract a string literal from a domain expression
fn extract_string_literal(expr: &DomainExpression, param_name: &str) -> Result<String> {
    match expr {
        DomainExpression::Literal {
            value: LiteralValue::String(s),
            ..
        } => Ok(s.clone()),
        _ => Err(DelightQLError::database_error(
            format!(
                "run!() expects '{}' to be a string literal, got: {:?}",
                param_name, expr
            ),
            "Invalid argument type (expected string literal)",
        )),
    }
}

/// Create a success result table for run!()
///
/// Returns: _("success", file_path, queries_executed @ "status", "file_path", "queries_executed")
fn create_success_table(file_path: &str, query_count: usize, alias: Option<String>) -> Relation {
    // Create column headers
    let headers = vec![
        DomainExpression::lvar_builder("status".to_string()).build(),
        DomainExpression::lvar_builder("file_path".to_string()).build(),
        DomainExpression::lvar_builder("queries_executed".to_string()).build(),
    ];

    // Create data row
    let row = Row {
        values: vec![
            DomainExpression::Literal {
                value: LiteralValue::String("success".to_string()),
                alias: None,
            },
            DomainExpression::Literal {
                value: LiteralValue::String(file_path.to_string()),
                alias: None,
            },
            DomainExpression::Literal {
                value: LiteralValue::Number(query_count.to_string()),
                alias: None,
            },
        ],
    };

    Relation::Anonymous {
        column_headers: Some(headers),
        rows: vec![row],
        alias: alias.map(|s| s.into()),
        outer: false,
        exists_mode: false,
        qua_target: None,
        cpr_schema: PhaseBox::phantom(),
    }
}
