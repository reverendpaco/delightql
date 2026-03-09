/// CFE substitution via AstTransform<Addressed, Addressed>
///
/// Replaces ~1,760 lines of hand-written walkers (old domain.rs + relational.rs)
/// with a single transform struct and 4 overrides. Structural recursion is handled
/// by the generic walk_transform_* functions in ast_transform.
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_transform::{
    walk_transform_curly_member, walk_transform_domain, walk_transform_function,
    walk_transform_relation, AstTransform,
};
use crate::pipeline::asts::addressed as ast;
use crate::pipeline::asts::core::{Addressed, SubstitutionExpr};
use std::collections::HashMap;

use super::curried::extract_function_from_code;

/// Replace all `ValuePlaceholder` (`@`) nodes in an expression with `replacement`.
fn replace_value_placeholder(
    expr: ast::DomainExpression,
    replacement: &ast::DomainExpression,
) -> ast::DomainExpression {
    struct PlaceholderReplacer<'a> {
        replacement: &'a ast::DomainExpression,
    }
    impl AstTransform<Addressed, Addressed> for PlaceholderReplacer<'_> {
        fn transform_domain(
            &mut self,
            e: ast::DomainExpression,
        ) -> Result<ast::DomainExpression> {
            match e {
                ast::DomainExpression::ValuePlaceholder { .. } => {
                    Ok(self.replacement.clone())
                }
                other => walk_transform_domain(self, other),
            }
        }
    }
    let mut replacer = PlaceholderReplacer { replacement };
    // ValuePlaceholder replacement cannot fail
    replacer.transform_domain(expr).unwrap()
}

// =============================================================================
// Fold struct
// =============================================================================

pub(super) struct CfeSubstituter<'a> {
    pub curried_substitutions: &'a HashMap<String, ast::DomainExpression>,
    pub regular_substitutions: &'a HashMap<String, ast::DomainExpression>,
}

// =============================================================================
// Public entry point (preserves existing API)
// =============================================================================

/// Recursively substitute parameters in a domain expression (HOCFE-aware)
pub fn substitute_in_domain_expression_with_curried(
    expr: ast::DomainExpression,
    curried_substitutions: &HashMap<String, ast::DomainExpression>,
    regular_substitutions: &HashMap<String, ast::DomainExpression>,
) -> Result<ast::DomainExpression> {
    CfeSubstituter {
        curried_substitutions,
        regular_substitutions,
    }
    .transform_domain(expr)
}

// =============================================================================
// AstTransform<Addressed, Addressed> implementation — 4 overrides
// =============================================================================

impl AstTransform<Addressed, Addressed> for CfeSubstituter<'_> {
    // -- Override 1: Domain leaf interception ----------------------------------

    fn transform_domain(&mut self, e: ast::DomainExpression) -> Result<ast::DomainExpression> {
        match e {
            ast::DomainExpression::Substitution(ref sub) => match sub {
                SubstitutionExpr::Parameter { ref name, .. } => self
                    .regular_substitutions
                    .get(name)
                    .cloned()
                    .ok_or_else(|| DelightQLError::ParseError {
                        message: format!("Regular parameter '{}' not found in substitutions", name),
                        source: None,
                        subcategory: None,
                    }),

                SubstitutionExpr::CurriedParameter { ref name, .. } => self
                    .curried_substitutions
                    .get(name)
                    .cloned()
                    .ok_or_else(|| DelightQLError::ParseError {
                        message: format!("Curried parameter '{}' not found in substitutions", name),
                        source: None,
                        subcategory: None,
                    }),

                SubstitutionExpr::ContextParameter { ref name, .. } => self
                    .regular_substitutions
                    .get(name)
                    .cloned()
                    .ok_or_else(|| DelightQLError::ParseError {
                        message: format!("Context parameter '{}' not found in substitutions", name),
                        source: None,
                        subcategory: None,
                    }),

                SubstitutionExpr::ContextMarker => Err(DelightQLError::ParseError {
                    message: "ContextMarker (..) should not appear in CFE body during substitution"
                        .to_string(),
                    source: None,
                    subcategory: None,
                }),
            },

            // Intercept f:(args) where f is a curried lambda — apply lambda inline
            ast::DomainExpression::Function(ast::FunctionExpression::Regular {
                ref name,
                ref arguments,
                ..
            }) if self.curried_substitutions.get(name.as_ref()).is_some_and(|code| {
                matches!(code, ast::DomainExpression::Function(ast::FunctionExpression::Lambda { .. }))
            }) => {
                let curried_code = self.curried_substitutions[name.as_ref()].clone();
                if let ast::DomainExpression::Function(ast::FunctionExpression::Lambda { body, .. }) = curried_code {
                    // Substitute @ in the lambda body with the call-site argument(s)
                    // Lambdas are unary: the single @ is replaced with the first argument
                    let arg = if arguments.len() == 1 {
                        // First, substitute any parameters in the argument itself
                        self.transform_domain(arguments[0].clone())?
                    } else {
                        return Err(DelightQLError::ParseError {
                            message: format!(
                                "Lambda expects 1 argument (via @), got {}",
                                arguments.len()
                            ),
                            source: None,
                            subcategory: None,
                        });
                    };
                    // Replace ValuePlaceholder (@) with the argument in the lambda body
                    let substituted = replace_value_placeholder(*body, &arg);
                    // Recursively transform the result (it may contain more parameters)
                    self.transform_domain(substituted)
                } else {
                    unreachable!()
                }
            }

            other => walk_transform_domain(self, other),
        }
    }

    // -- Override 2: Function expression interception --------------------------

    fn transform_function(
        &mut self,
        f: ast::FunctionExpression,
    ) -> Result<ast::FunctionExpression> {
        match f {
            // Regular: check if name is a curried parameter → extract_function_from_code
            ast::FunctionExpression::Regular {
                name,
                namespace,
                arguments,
                alias,
                conditioned_on,
            } => {
                if let Some(curried_code) = self.curried_substitutions.get(name.as_ref()) {
                    let extracted = extract_function_from_code(
                        curried_code,
                        arguments,
                        alias.map(|s| s.to_string()),
                        conditioned_on,
                        self.curried_substitutions,
                        self.regular_substitutions,
                    )?;
                    // Recursively fold the result: extract_function_from_code substitutes
                    // call_site_args but NOT call_site_condition. walk_function handles
                    // any remaining Parameter/CurriedParameter nodes in the condition.
                    walk_transform_function(self, extracted)
                } else {
                    walk_transform_function(
                        self,
                        ast::FunctionExpression::Regular {
                            name,
                            namespace,
                            arguments,
                            alias,
                            conditioned_on,
                        },
                    )
                }
            }

            // Curried: same curried-parameter check
            ast::FunctionExpression::Curried {
                name,
                namespace,
                arguments,
                conditioned_on,
            } => {
                if let Some(curried_code) = self.curried_substitutions.get(name.as_ref()) {
                    let extracted = extract_function_from_code(
                        curried_code,
                        arguments,
                        None,
                        conditioned_on,
                        self.curried_substitutions,
                        self.regular_substitutions,
                    )?;
                    walk_transform_function(self, extracted)
                } else {
                    walk_transform_function(
                        self,
                        ast::FunctionExpression::Curried {
                            name,
                            namespace,
                            arguments,
                            conditioned_on,
                        },
                    )
                }
            }

            // Window: substitute name from curried code, then walk for recursion
            ast::FunctionExpression::Window {
                name,
                arguments,
                partition_by,
                order_by,
                frame,
                alias,
            } => {
                let actual_name =
                    if let Some(curried_code) = self.curried_substitutions.get(name.as_ref()) {
                        match curried_code {
                            ast::DomainExpression::Function(func) => match func {
                                ast::FunctionExpression::Regular { name, .. }
                                | ast::FunctionExpression::Curried { name, .. }
                                | ast::FunctionExpression::Window { name, .. } => name.clone(),
                                _ => name,
                            },
                            _ => name,
                        }
                    } else {
                        name
                    };
                walk_transform_function(
                    self,
                    ast::FunctionExpression::Window {
                        name: actual_name,
                        arguments,
                        partition_by,
                        order_by,
                        frame,
                        alias,
                    },
                )
            }

            // HigherOrder: validate curried arguments, then walk
            ast::FunctionExpression::HigherOrder {
                name,
                curried_arguments,
                regular_arguments,
                alias,
                conditioned_on,
            } => {
                for arg in &curried_arguments {
                    if let ast::DomainExpression::Function(func) = arg {
                        let has_args = match func {
                            ast::FunctionExpression::Regular { arguments, .. } => {
                                !arguments.is_empty()
                            }
                            ast::FunctionExpression::Curried { arguments, .. } => {
                                !arguments.is_empty()
                            }
                            ast::FunctionExpression::Window { arguments, .. } => {
                                !arguments.is_empty()
                            }
                            ast::FunctionExpression::Lambda { .. } => false,
                            other => panic!("catch-all hit in cfe_substitution/domain.rs transform_function (FunctionExpression has_args): {:?}", other),
                        };
                        if has_args {
                            return Err(DelightQLError::ParseError {
                                message: format!(
                                    "HOCFE function parameters must have no arguments (like f:()), not partial applications. \
                                     Higher-order CFEs can take functions (higher-order) or values (lower-order), but functions must be bare references."
                                ),
                                source: None,
                                subcategory: None,
                            });
                        }
                    }
                }
                walk_transform_function(
                    self,
                    ast::FunctionExpression::HigherOrder {
                        name,
                        curried_arguments,
                        regular_arguments,
                        alias,
                        conditioned_on,
                    },
                )
            }

            // Array: not yet implemented
            ast::FunctionExpression::Array { .. } => Err(DelightQLError::ParseError {
                message: "Array destructuring not yet implemented".to_string(),
                source: None,
                subcategory: None,
            }),

            // MetadataTreeGroup: pass through without recursion into constructor
            m @ ast::FunctionExpression::MetadataTreeGroup { .. } => Ok(m),

            // All other variants: structural recursion via walk_transform_function
            other => walk_transform_function(self, other),
        }
    }

    // -- Override 3: Curly member parameter substitution -----------------------

    fn transform_curly_member(&mut self, m: ast::CurlyMember) -> Result<ast::CurlyMember> {
        match m {
            // Shorthand: if unqualified and name matches a parameter, substitute
            ast::CurlyMember::Shorthand {
                column,
                qualifier,
                schema,
            } if qualifier.is_none() && schema.is_none() => {
                // Check regular params first
                if let Some(substituted_expr) = self.regular_substitutions.get(column.as_ref()) {
                    if let ast::DomainExpression::Lvar {
                        name,
                        qualifier: q,
                        namespace_path: ns,
                        ..
                    } = substituted_expr
                    {
                        return Ok(ast::CurlyMember::Shorthand {
                            column: name.clone(),
                            qualifier: q.clone(),
                            schema: ns.first().map(|s| s.into()),
                        });
                    }
                    return Ok(ast::CurlyMember::KeyValue {
                        key: column.to_string(),
                        nested_reduction: false,
                        value: Box::new(substituted_expr.clone()),
                    });
                }

                // Check curried params
                if let Some(substituted_expr) = self.curried_substitutions.get(column.as_ref()) {
                    if let ast::DomainExpression::Lvar {
                        name,
                        qualifier: q,
                        namespace_path: ns,
                        ..
                    } = substituted_expr
                    {
                        return Ok(ast::CurlyMember::Shorthand {
                            column: name.clone(),
                            qualifier: q.clone(),
                            schema: ns.first().map(|s| s.into()),
                        });
                    }
                    return Ok(ast::CurlyMember::KeyValue {
                        key: column.to_string(),
                        nested_reduction: false,
                        value: Box::new(substituted_expr.clone()),
                    });
                }

                // Not a parameter — keep as shorthand
                Ok(ast::CurlyMember::Shorthand {
                    column,
                    qualifier,
                    schema,
                })
            }

            // Glob/Pattern/OrdinalRange should have been expanded by resolver
            ast::CurlyMember::Glob
            | ast::CurlyMember::Pattern { .. }
            | ast::CurlyMember::OrdinalRange { .. } => Err(DelightQLError::ParseError {
                message:
                    "Glob/Pattern/OrdinalRange in curly member should have been expanded by resolver"
                        .to_string(),
                source: None,
                subcategory: None,
            }),

            // Placeholder: only valid in destructuring
            ast::CurlyMember::Placeholder => Err(DelightQLError::ParseError {
                message: "Placeholder in curly member should only appear in destructuring context"
                    .to_string(),
                source: None,
                subcategory: None,
            }),

            // All other variants (Shorthand with qualifier, Comparison, KeyValue, PathLiteral):
            // structural recursion via walk_curly_member
            other => walk_transform_curly_member(self, other),
        }
    }

    // -- Override 4: Relation invariant checks --------------------------------

    fn transform_relation(&mut self, r: ast::Relation) -> Result<ast::Relation> {
        match r {
            ast::Relation::PseudoPredicate { .. } => {
                panic!(
                    "INTERNAL ERROR: PseudoPredicate should not exist in this phase. \
                     Pseudo-predicates are executed and replaced during Phase 1.X (Effect Executor)."
                )
            }
            other => walk_transform_relation(self, other),
        }
    }
}
