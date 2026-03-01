//! Post-processing: Replace parameter Lvars with Parameter/CurriedParameter/ContextParameter nodes.
//!
//! Three modes, each a small `AstFold<Refined>` implementation:
//!
//! - **Strict** (`StrictParamReplacer`): curried→CurriedParameter, regular→Parameter, else→Error
//! - **Implicit** (`ImplicitParamReplacer`): curried→CurriedParameter, regular→Parameter,
//!   else→ContextParameter (discovered and collected)
//! - **Explicit** (`ExplicitParamReplacer`): provenance-based dispatch

use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_fold::{walk_domain, walk_function, AstFold};
use crate::pipeline::asts::core::expressions::domain::LvarProvenance;
use crate::pipeline::asts::core::{
    DomainExpression, FunctionExpression, NamespacePath, Refined, SubstitutionExpr,
};

// =============================================================================
// Entry points
// =============================================================================

/// STRICT MODE: Replace parameter Lvars with typed parameter nodes.
/// Errors if CFE body references non-parameter columns.
pub(super) fn replace_param_lvars_with_param_types(
    expr: DomainExpression<Refined>,
    curried_params: &[String],
    regular_params: &[String],
) -> Result<DomainExpression<Refined>> {
    StrictParamReplacer {
        curried_params,
        regular_params,
    }
    .fold_domain(expr)
}

/// IMPLICIT MODE: Auto-discover non-parameter Lvars as context parameters.
pub(super) fn replace_params_with_implicit_context(
    expr: DomainExpression<Refined>,
    curried_params: &[String],
    regular_params: &[String],
    discovered_context: &mut Vec<String>,
) -> Result<DomainExpression<Refined>> {
    ImplicitParamReplacer {
        curried_params,
        regular_params,
        discovered_context,
    }
    .fold_domain(expr)
}

/// EXPLICIT MODE: Use provenance to dispatch Lvars to typed parameter nodes.
pub(super) fn replace_params_with_explicit_context(
    expr: DomainExpression<Refined>,
    _curried_params: &[String],
    _regular_params: &[String],
    _declared_context: &[String],
) -> Result<DomainExpression<Refined>> {
    ExplicitParamReplacer.fold_domain(expr)
}

// =============================================================================
// Strict mode
// =============================================================================

struct StrictParamReplacer<'a> {
    curried_params: &'a [String],
    regular_params: &'a [String],
}

impl AstFold<Refined> for StrictParamReplacer<'_> {
    fn fold_domain(&mut self, e: DomainExpression<Refined>) -> Result<DomainExpression<Refined>> {
        match e {
            DomainExpression::Lvar {
                name,
                qualifier: None,
                alias,
                ..
            } => {
                let name_str = name.to_string();
                let alias_str = alias.map(|a| a.to_string());
                if self.curried_params.iter().any(|p| name_str == *p) {
                    Ok(DomainExpression::Substitution(
                        SubstitutionExpr::CurriedParameter {
                            name: name_str,
                            alias: alias_str,
                        },
                    ))
                } else if self.regular_params.iter().any(|p| name_str == *p) {
                    Ok(DomainExpression::Substitution(
                        SubstitutionExpr::Parameter {
                            name: name_str,
                            alias: alias_str,
                        },
                    ))
                } else {
                    Err(DelightQLError::ParseError {
                        message: format!(
                            "CFE body references '{}' which is not a parameter.\n\
                             \n\
                             CFE bodies can only reference their declared parameters, \
                             not columns from call-site scope.\n\
                             \n\
                             Solutions:\n\
                             1. Add '{}' as a parameter: name:(..., {}) : ...\n\
                             2. Use implicit context (future): name:(.., ...) : ...\n\
                             3. Use explicit context (future): name:(..{{{}}}, ...) : ...",
                            name_str, name_str, name_str, name_str
                        ),
                        source: None,
                        subcategory: None,
                    })
                }
            }
            other => walk_domain(self, other),
        }
    }

    fn fold_function(
        &mut self,
        f: FunctionExpression<Refined>,
    ) -> Result<FunctionExpression<Refined>> {
        match f {
            FunctionExpression::MetadataTreeGroup { .. } => Ok(f),
            FunctionExpression::Array { .. } => Err(DelightQLError::ParseError {
                message: "Array destructuring not yet implemented".to_string(),
                source: None,
                subcategory: None,
            }),
            other => walk_function(self, other),
        }
    }
}

// =============================================================================
// Implicit mode
// =============================================================================

struct ImplicitParamReplacer<'a> {
    curried_params: &'a [String],
    regular_params: &'a [String],
    discovered_context: &'a mut Vec<String>,
}

impl AstFold<Refined> for ImplicitParamReplacer<'_> {
    fn fold_domain(&mut self, e: DomainExpression<Refined>) -> Result<DomainExpression<Refined>> {
        match e {
            DomainExpression::Lvar {
                name,
                qualifier: None,
                alias,
                ..
            } => {
                let name_str = name.to_string();
                let alias_str = alias.map(|a| a.to_string());
                if self.curried_params.iter().any(|p| name_str == *p) {
                    Ok(DomainExpression::Substitution(
                        SubstitutionExpr::CurriedParameter {
                            name: name_str,
                            alias: alias_str,
                        },
                    ))
                } else if self.regular_params.iter().any(|p| name_str == *p) {
                    Ok(DomainExpression::Substitution(
                        SubstitutionExpr::Parameter {
                            name: name_str,
                            alias: alias_str,
                        },
                    ))
                } else {
                    if !self.discovered_context.contains(&name_str) {
                        self.discovered_context.push(name_str.clone());
                    }
                    Ok(DomainExpression::Substitution(
                        SubstitutionExpr::ContextParameter {
                            name: name_str,
                            alias: alias_str,
                        },
                    ))
                }
            }
            other => walk_domain(self, other),
        }
    }

    fn fold_function(
        &mut self,
        f: FunctionExpression<Refined>,
    ) -> Result<FunctionExpression<Refined>> {
        match f {
            FunctionExpression::MetadataTreeGroup { .. } => Ok(f),
            other => walk_function(self, other),
        }
    }
}

// =============================================================================
// Explicit mode
// =============================================================================

struct ExplicitParamReplacer;

impl AstFold<Refined> for ExplicitParamReplacer {
    fn fold_domain(&mut self, e: DomainExpression<Refined>) -> Result<DomainExpression<Refined>> {
        match e {
            DomainExpression::Lvar {
                name,
                qualifier: None,
                alias,
                provenance,
                ..
            } => match provenance.get() {
                Some(LvarProvenance::CfeCurriedParameter) => Ok(DomainExpression::Substitution(
                    SubstitutionExpr::CurriedParameter {
                        name: name.to_string(),
                        alias: alias.map(|a| a.to_string()),
                    },
                )),
                Some(LvarProvenance::CfeParameter) => Ok(DomainExpression::Substitution(
                    SubstitutionExpr::Parameter {
                        name: name.to_string(),
                        alias: alias.map(|a| a.to_string()),
                    },
                )),
                Some(LvarProvenance::CfeContext) => Ok(DomainExpression::Substitution(
                    SubstitutionExpr::ContextParameter {
                        name: name.to_string(),
                        alias: alias.map(|a| a.to_string()),
                    },
                )),
                Some(LvarProvenance::RealTable { .. }) | None => Ok(DomainExpression::Lvar {
                    name,
                    qualifier: None,
                    namespace_path: NamespacePath::empty(),
                    alias,
                    provenance,
                }),
            },
            other => walk_domain(self, other),
        }
    }
}
