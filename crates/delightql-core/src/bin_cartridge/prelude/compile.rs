//! `sys::execution.compile()` bin relation implementation
//!
//! Syntax: `sys::execution.compile("stage", """source""")`
//!
//! Returns a 1-row relation: `(stage, query, representation, error)`
//!
//! On success, `representation` contains the compiled output and `error` is NULL.
//! On failure, `representation` is NULL and `error` contains the error URI.
//!
//! Stages: "cst", "ast-unresolved", "ast-resolved", "ast-refined", "sql"

use crate::bin_cartridge::{
    BinEntity, EffectExecutable, EntityResult, EntitySignature, OutputSchema, Parameter,
};
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::literals::LiteralValue;
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::Pipeline;

pub struct CompilePredicate;

impl BinEntity for CompilePredicate {
    fn name(&self) -> &str {
        "sys::execution.compile"
    }

    fn entity_type(&self) -> EntityType {
        EntityType::BinRelation
    }

    fn signature(&self) -> EntitySignature {
        EntitySignature {
            parameters: vec![
                Parameter {
                    name: "stage".to_string(),
                    data_type: "String".to_string(),
                    _is_optional: false,
                },
                Parameter {
                    name: "source".to_string(),
                    data_type: "String".to_string(),
                    _is_optional: false,
                },
            ],
            output_schema: OutputSchema::Relation(vec![
                ("stage".to_string(), "String".to_string()),
                ("query".to_string(), "String".to_string()),
                ("representation".to_string(), "String".to_string()),
                ("error".to_string(), "String".to_string()),
            ]),
        }
    }

    fn has_side_effects(&self) -> bool {
        false
    }

    fn as_effect_executable(&self) -> Option<&dyn EffectExecutable> {
        Some(self)
    }
}

impl EffectExecutable for CompilePredicate {
    fn execute(
        &self,
        arguments: &[DomainExpression],
        alias: Option<String>,
        system: &mut crate::system::DelightQLSystem,
    ) -> Result<EntityResult> {
        if arguments.len() != 2 {
            return Err(DelightQLError::database_error(
                format!(
                    "sys::execution.compile() expects 2 arguments (stage, source), got {}",
                    arguments.len()
                ),
                "Invalid argument count",
            ));
        }

        let stage = extract_string_literal(&arguments[0], "stage")?;
        let source = extract_string_literal(&arguments[1], "source")?;

        let (representation, error) = match compile_to_stage(system, &stage, &source) {
            Ok(repr) => (Some(repr), None),
            Err(e) => (None, Some(e.error_uri())),
        };

        let relation = build_compile_result(&stage, &source, representation, error, alias);
        Ok(EntityResult::Relation(relation))
    }
}

fn compile_to_stage(
    system: &mut crate::system::DelightQLSystem,
    stage: &str,
    source: &str,
) -> Result<String> {
    let mut pipeline = Pipeline::new(source, system);
    pipeline.render_stage(stage)
}

fn extract_string_literal(expr: &DomainExpression, arg_name: &str) -> Result<String> {
    match expr {
        DomainExpression::Literal {
            value: LiteralValue::String(s),
            ..
        } => Ok(s.clone()),
        _ => Err(DelightQLError::database_error(
            format!(
                "sys::execution.compile() {} must be a string literal",
                arg_name
            ),
            "Invalid argument type",
        )),
    }
}

fn string_literal(val: &str) -> DomainExpression {
    DomainExpression::Literal {
        value: LiteralValue::String(val.to_string()),
        alias: None,
    }
}

fn null_literal() -> DomainExpression {
    DomainExpression::Literal {
        value: LiteralValue::Null,
        alias: None,
    }
}

fn build_compile_result(
    stage: &str,
    query: &str,
    representation: Option<String>,
    error: Option<String>,
    alias: Option<String>,
) -> Relation {
    let headers = vec![
        DomainExpression::lvar_builder("stage".to_string()).build(),
        DomainExpression::lvar_builder("query".to_string()).build(),
        DomainExpression::lvar_builder("representation".to_string()).build(),
        DomainExpression::lvar_builder("error".to_string()).build(),
    ];
    let row = Row {
        values: vec![
            string_literal(stage),
            string_literal(query),
            match &representation {
                Some(repr) => string_literal(repr),
                None => null_literal(),
            },
            match &error {
                Some(uri) => string_literal(uri),
                None => null_literal(),
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
