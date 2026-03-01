//! `like()` sigma predicate implementation
//!
//! Syntax: `+like(value, pattern)` or `\+like(value, pattern)`
//!
//! Example: `users(*), +like(email, "%@gmail.com")`
//!
//! ## Behavior
//!
//! 1. The `+` prefix enforces EXISTS semantics (boolean constraint, no row contribution)
//! 2. Transformed to SQL: `WHERE value LIKE pattern` (or `NOT LIKE` for `\+`)
//! 3. No side effects - purely a filtering constraint
//!
//! ## Why Sigma Predicate?
//!
//! `like(x, y)` conceptually represents an infinite relation - all pairs (x, y)
//! where x matches pattern y. This cannot be enumerated, only tested.
//! Therefore it requires EXISTS semantics and cannot be used as a regular join.

use crate::bin_cartridge::{
    BinEntity, EntitySignature, GeneratorContext, OutputSchema, Parameter, SqlGeneratable,
};
use crate::enums::EntityType;
use crate::error::Result;

/// like() sigma predicate entity
pub struct LikePredicate;

impl BinEntity for LikePredicate {
    fn name(&self) -> &str {
        "like"
    }

    fn entity_type(&self) -> EntityType {
        EntityType::BinSigmaPredicate
    }

    fn signature(&self) -> EntitySignature {
        EntitySignature {
            parameters: vec![
                Parameter {
                    name: "value".to_string(),
                    data_type: "String".to_string(),
                    _is_optional: false,
                },
                Parameter {
                    name: "pattern".to_string(),
                    data_type: "String".to_string(),
                    _is_optional: false,
                },
            ],
            // Sigma predicates conceptually return an infinite relation
            // but are always used with EXISTS semantics, so they don't
            // actually contribute rows to the result
            output_schema: OutputSchema::Void,
        }
    }

    fn has_side_effects(&self) -> bool {
        false
    }

    fn as_effect_executable(&self) -> Option<&dyn crate::bin_cartridge::EffectExecutable> {
        None
    }

    fn as_sql_generatable(&self) -> Option<&dyn SqlGeneratable> {
        Some(self)
    }
}

impl SqlGeneratable for LikePredicate {
    fn generate_sql<'a>(
        &self,
        args: &[crate::pipeline::sql_ast_v3::DomainExpression],
        context: &GeneratorContext<'a>,
        negated: bool,
    ) -> Result<String> {
        if args.len() != 2 {
            return Err(crate::error::DelightQLError::validation_error(
                &format!("like expects 2 arguments, got {}", args.len()),
                "LikePredicate::generate_sql",
            ));
        }

        // Use the render function provided by the transformer
        let left_sql = (context.render_expr)(&args[0]);
        let right_sql = (context.render_expr)(&args[1]);

        // Generate the LIKE expression
        let operator = if negated { "NOT LIKE" } else { "LIKE" };
        Ok(format!("{} {} {}", left_sql, operator, right_sql))
    }
}
