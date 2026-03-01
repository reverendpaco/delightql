//! `between()` sigma predicate implementation
//!
//! Syntax: `+between(value, low, high)` or `\+between(value, low, high)`
//!
//! Example: `users(*), +between(age, 18, 65)`
//!
//! ## Behavior
//!
//! 1. The `+` prefix enforces EXISTS semantics (boolean constraint, no row contribution)
//! 2. Transformed to SQL: `WHERE value BETWEEN low AND high` (or `NOT BETWEEN` for `\+`)
//! 3. No side effects - purely a filtering constraint
//!
//! ## Why Sigma Predicate?
//!
//! `between(x, a, b)` conceptually represents an infinite relation - all triples (x, a, b)
//! where a <= x <= b. This cannot be enumerated, only tested.
//! Therefore it requires EXISTS semantics and cannot be used as a regular join.

use crate::bin_cartridge::{
    BinEntity, EntitySignature, GeneratorContext, OutputSchema, Parameter, SqlGeneratable,
};
use crate::enums::EntityType;
use crate::error::Result;

/// between() sigma predicate entity
pub struct BetweenPredicate;

impl BinEntity for BetweenPredicate {
    fn name(&self) -> &str {
        "between"
    }

    fn entity_type(&self) -> EntityType {
        EntityType::BinSigmaPredicate
    }

    fn signature(&self) -> EntitySignature {
        EntitySignature {
            parameters: vec![
                Parameter {
                    name: "value".to_string(),
                    data_type: "Any".to_string(),
                    _is_optional: false,
                },
                Parameter {
                    name: "low".to_string(),
                    data_type: "Any".to_string(),
                    _is_optional: false,
                },
                Parameter {
                    name: "high".to_string(),
                    data_type: "Any".to_string(),
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

impl SqlGeneratable for BetweenPredicate {
    fn generate_sql<'a>(
        &self,
        args: &[crate::pipeline::sql_ast_v3::DomainExpression],
        context: &GeneratorContext<'a>,
        negated: bool,
    ) -> Result<String> {
        if args.len() != 3 {
            return Err(crate::error::DelightQLError::validation_error(
                &format!("between expects 3 arguments, got {}", args.len()),
                "BetweenPredicate::generate_sql",
            ));
        }

        // Use the render function provided by the transformer
        let value_sql = (context.render_expr)(&args[0]);
        let low_sql = (context.render_expr)(&args[1]);
        let high_sql = (context.render_expr)(&args[2]);

        // Generate the BETWEEN expression
        let operator = if negated { "NOT BETWEEN" } else { "BETWEEN" };
        Ok(format!(
            "{} {} {} AND {}",
            value_sql, operator, low_sql, high_sql
        ))
    }
}
