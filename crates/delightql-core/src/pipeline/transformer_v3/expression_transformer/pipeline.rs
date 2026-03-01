/// Pipeline transform application logic
use crate::error::Result;
use crate::pipeline::asts::addressed::{DomainExpression as AstDomainExpression, FunctionExpression};

/// Substitute an AST expression into @ placeholders in a transform function
/// This keeps everything in AST domain to avoid SQL-to-AST conversion issues
pub fn substitute_ast_in_transform(
    value: AstDomainExpression,
    transform: FunctionExpression,
) -> Result<AstDomainExpression> {
    super::substitution::substitute_ast_in_transform(value, transform)
}
