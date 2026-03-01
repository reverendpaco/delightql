// Domain expression refining - Convert resolved → refined AST nodes
// This phase handles embedded subqueries by calling a CFE-aware refiner
// that populates LvarProvenance based on parameter lists

use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::{refined, resolved};
use crate::pipeline::refiner;

use super::provenance::populate_provenance_in_relational;

/// Refine a domain expression (convert Resolved → Refined)
/// This handles embedded subqueries by calling the full refiner
///
/// For CFE precompilation, also populates LvarProvenance based on parameter lists
pub(super) fn refine_domain_expression(
    expr: resolved::DomainExpression,
    curried_params: &[String],
    regular_params: &[String],
    context_params: &[String],
) -> Result<refined::DomainExpression> {
    CfeRefiner::new(curried_params, regular_params, context_params).rd(expr)
}

/// Bundles CFE parameter lists to eliminate repeated parameter threading
struct CfeRefiner<'a> {
    curried: &'a [String],
    regular: &'a [String],
    context: &'a [String],
}

impl<'a> CfeRefiner<'a> {
    fn new(curried: &'a [String], regular: &'a [String], context: &'a [String]) -> Self {
        Self {
            curried,
            regular,
            context,
        }
    }

    // --- Convenience helpers ---

    fn rd_box(&self, e: Box<resolved::DomainExpression>) -> Result<Box<refined::DomainExpression>> {
        Ok(Box::new(self.rd(*e)?))
    }

    fn rb_box(
        &self,
        e: Box<resolved::BooleanExpression>,
    ) -> Result<Box<refined::BooleanExpression>> {
        Ok(Box::new(self.rb(*e)?))
    }

    fn rd_vec(&self, v: Vec<resolved::DomainExpression>) -> Result<Vec<refined::DomainExpression>> {
        v.into_iter().map(|e| self.rd(e)).collect()
    }

    fn rf_vec(
        &self,
        v: Vec<resolved::FunctionExpression>,
    ) -> Result<Vec<refined::FunctionExpression>> {
        v.into_iter().map(|f| self.rf(f)).collect()
    }

    fn rb_opt_box(
        &self,
        c: Option<Box<resolved::BooleanExpression>>,
    ) -> Result<Option<Box<refined::BooleanExpression>>> {
        c.map(|b| self.rb_box(b)).transpose()
    }

    /// Route a subquery through the real refiner, then fix up provenance
    fn refine_subquery(
        &self,
        subquery: Box<resolved::RelationalExpression>,
    ) -> Result<Box<refined::RelationalExpression>> {
        let mut refined = refiner::refine(*subquery)?;
        populate_provenance_in_relational(&mut refined, self.curried, self.regular, self.context);
        Ok(Box::new(refined))
    }

    // --- Core refiners ---

    /// Refine a domain expression
    fn rd(&self, expr: resolved::DomainExpression) -> Result<refined::DomainExpression> {
        Ok(match expr {
            // Custom: Lvar provenance assignment
            resolved::DomainExpression::Lvar {
                name,
                qualifier,
                namespace_path,
                alias,
                provenance: _,
            } => {
                use refined::LvarProvenance;
                let provenance = if self.curried.iter().any(|p| name == p.as_str()) {
                    log::debug!("🔍 REFINER: Lvar '{}' → CfeCurriedParameter", name);
                    Some(LvarProvenance::CfeCurriedParameter)
                } else if self.regular.iter().any(|p| name == p.as_str()) {
                    log::debug!("🔍 REFINER: Lvar '{}' → CfeParameter", name);
                    Some(LvarProvenance::CfeParameter)
                } else if self.context.iter().any(|p| name == p.as_str()) {
                    log::debug!("🔍 REFINER: Lvar '{}' → CfeContext", name);
                    Some(LvarProvenance::CfeContext)
                } else {
                    log::debug!(
                        "🔍 REFINER: Lvar '{}' → None (real table or unresolved)",
                        name
                    );
                    None
                };
                refined::DomainExpression::Lvar {
                    name,
                    qualifier,
                    namespace_path,
                    alias,
                    provenance: refined::PhaseBox::new(provenance),
                }
            }

            // Custom: subquery routing through real refiner
            resolved::DomainExpression::ScalarSubquery {
                identifier,
                subquery,
                alias,
            } => refined::DomainExpression::ScalarSubquery {
                identifier,
                subquery: self.refine_subquery(subquery)?,
                alias,
            },

            // Container types: recurse to reach Lvars and subqueries
            resolved::DomainExpression::Predicate { expr, alias } => {
                refined::DomainExpression::Predicate {
                    expr: self.rb_box(expr)?,
                    alias,
                }
            }
            resolved::DomainExpression::Function(func) => {
                refined::DomainExpression::Function(self.rf(func)?)
            }
            resolved::DomainExpression::PipedExpression {
                value,
                transforms,
                alias,
            } => refined::DomainExpression::PipedExpression {
                value: self.rd_box(value)?,
                transforms: self.rf_vec(transforms)?,
                alias,
            },
            resolved::DomainExpression::Parenthesized { inner, alias } => {
                refined::DomainExpression::Parenthesized {
                    inner: self.rd_box(inner)?,
                    alias,
                }
            }
            resolved::DomainExpression::Tuple { elements, alias } => {
                refined::DomainExpression::Tuple {
                    elements: self.rd_vec(elements)?,
                    alias,
                }
            }
            resolved::DomainExpression::PivotOf {
                value_column,
                pivot_key,
                pivot_values,
            } => refined::DomainExpression::PivotOf {
                value_column: self.rd_box(value_column)?,
                pivot_key: self.rd_box(pivot_key)?,
                pivot_values,
            },

            // Leaf types: no nested expressions to recurse into
            resolved::DomainExpression::Literal { value, alias } => {
                refined::DomainExpression::Literal { value, alias }
            }
            resolved::DomainExpression::Projection(proj) => {
                refined::DomainExpression::Projection(proj.into())
            }
            resolved::DomainExpression::NonUnifiyingUnderscore => {
                refined::DomainExpression::NonUnifiyingUnderscore
            }
            resolved::DomainExpression::ValuePlaceholder { alias } => {
                refined::DomainExpression::ValuePlaceholder { alias }
            }
            resolved::DomainExpression::Substitution(s) => {
                refined::DomainExpression::Substitution(s)
            }
            resolved::DomainExpression::ColumnOrdinal(_) => {
                refined::DomainExpression::ColumnOrdinal(refined::PhaseBox::phantom())
            }
        })
    }

    /// Refine a boolean expression
    fn rb(&self, expr: resolved::BooleanExpression) -> Result<refined::BooleanExpression> {
        Ok(match expr {
            // Custom: subquery routing through real refiner
            resolved::BooleanExpression::InnerExists {
                exists,
                identifier,
                subquery,
                alias,
                using_columns,
            } => refined::BooleanExpression::InnerExists {
                exists,
                identifier,
                subquery: self.refine_subquery(subquery)?,
                alias,
                using_columns,
            },
            resolved::BooleanExpression::InRelational {
                value,
                subquery,
                identifier,
                negated,
            } => refined::BooleanExpression::InRelational {
                value: self.rd_box(value)?,
                subquery: self.refine_subquery(subquery)?,
                identifier,
                negated,
            },

            // Container types: recurse
            resolved::BooleanExpression::Comparison {
                left,
                operator,
                right,
            } => refined::BooleanExpression::Comparison {
                left: self.rd_box(left)?,
                operator,
                right: self.rd_box(right)?,
            },
            resolved::BooleanExpression::And { left, right } => refined::BooleanExpression::And {
                left: self.rb_box(left)?,
                right: self.rb_box(right)?,
            },
            resolved::BooleanExpression::Or { left, right } => refined::BooleanExpression::Or {
                left: self.rb_box(left)?,
                right: self.rb_box(right)?,
            },
            resolved::BooleanExpression::Not { expr } => refined::BooleanExpression::Not {
                expr: self.rb_box(expr)?,
            },

            // Passthrough via PhaseConvert .into() (no Lvars or subqueries to intercept)
            resolved::BooleanExpression::In {
                value,
                set,
                negated,
            } => refined::BooleanExpression::In {
                value: Box::new((*value).into()),
                set: set.into_iter().map(Into::into).collect(),
                negated,
            },
            resolved::BooleanExpression::Sigma { condition } => refined::BooleanExpression::Sigma {
                condition: Box::new((*condition).into()),
            },

            // Leaf types
            resolved::BooleanExpression::BooleanLiteral { value } => {
                refined::BooleanExpression::BooleanLiteral { value }
            }
            resolved::BooleanExpression::GlobCorrelation { left, right } => {
                refined::BooleanExpression::GlobCorrelation { left, right }
            }
            resolved::BooleanExpression::OrdinalGlobCorrelation { left, right } => {
                refined::BooleanExpression::OrdinalGlobCorrelation { left, right }
            }

            // Error
            resolved::BooleanExpression::Using { .. } => {
                return Err(DelightQLError::parse_error(
                    "USING clause should not appear in CFE bodies",
                ));
            }
        })
    }

    /// Refine a function expression
    fn rf(&self, func: resolved::FunctionExpression) -> Result<refined::FunctionExpression> {
        Ok(match func {
            resolved::FunctionExpression::Regular {
                name,
                namespace,
                arguments,
                alias,
                conditioned_on,
            } => refined::FunctionExpression::Regular {
                name,
                namespace,
                arguments: self.rd_vec(arguments)?,
                alias,
                conditioned_on: self.rb_opt_box(conditioned_on)?,
            },
            resolved::FunctionExpression::Curried {
                name,
                namespace,
                arguments,
                conditioned_on,
            } => refined::FunctionExpression::Curried {
                name,
                namespace,
                arguments: self.rd_vec(arguments)?,
                conditioned_on: self.rb_opt_box(conditioned_on)?,
            },
            resolved::FunctionExpression::Bracket { arguments, alias } => {
                refined::FunctionExpression::Bracket {
                    arguments: self.rd_vec(arguments)?,
                    alias,
                }
            }
            resolved::FunctionExpression::Lambda { body, alias } => {
                refined::FunctionExpression::Lambda {
                    body: self.rd_box(body)?,
                    alias,
                }
            }
            resolved::FunctionExpression::Infix {
                operator,
                left,
                right,
                alias,
            } => refined::FunctionExpression::Infix {
                operator,
                left: self.rd_box(left)?,
                right: self.rd_box(right)?,
                alias,
            },
            resolved::FunctionExpression::StringTemplate { parts, alias } => {
                refined::FunctionExpression::StringTemplate {
                    parts: parts
                        .into_iter()
                        .map(|part| match part {
                            resolved::StringTemplatePart::Text(s) => {
                                Ok(refined::StringTemplatePart::Text(s))
                            }
                            resolved::StringTemplatePart::Interpolation(expr) => {
                                Ok(refined::StringTemplatePart::Interpolation(Box::new(
                                    self.rd(*expr)?,
                                )))
                            }
                        })
                        .collect::<Result<Vec<_>>>()?,
                    alias,
                }
            }
            resolved::FunctionExpression::CaseExpression { arms, alias } => {
                refined::FunctionExpression::CaseExpression {
                    arms: arms
                        .into_iter()
                        .map(|arm| self.refine_case_arm(arm))
                        .collect::<Result<Vec<_>>>()?,
                    alias,
                }
            }
            resolved::FunctionExpression::HigherOrder {
                name,
                curried_arguments,
                regular_arguments,
                alias,
                conditioned_on,
            } => refined::FunctionExpression::HigherOrder {
                name,
                curried_arguments: self.rd_vec(curried_arguments)?,
                regular_arguments: self.rd_vec(regular_arguments)?,
                alias,
                conditioned_on: self.rb_opt_box(conditioned_on)?,
            },
            resolved::FunctionExpression::Curly {
                members,
                inner_grouping_keys,
                cte_requirements: _,
                alias,
            } => refined::FunctionExpression::Curly {
                members: members
                    .into_iter()
                    .map(|m| self.refine_curly_member(m))
                    .collect::<Result<Vec<_>>>()?,
                inner_grouping_keys: self.rd_vec(inner_grouping_keys)?,
                cte_requirements: None,
                alias,
            },
            resolved::FunctionExpression::Array { .. } => {
                return Err(DelightQLError::ParseError {
                    message: "Array destructuring not yet implemented".to_string(),
                    source: None,
                    subcategory: None,
                });
            }
            resolved::FunctionExpression::MetadataTreeGroup {
                key_column,
                key_qualifier,
                key_schema,
                constructor,
                keys_only,
                cte_requirements,
                alias,
            } => refined::FunctionExpression::MetadataTreeGroup {
                key_column,
                key_qualifier,
                key_schema,
                constructor: Box::new(self.rf(*constructor)?),
                keys_only,
                cte_requirements: cte_requirements.map(|req| req.into()),
                alias,
            },
            resolved::FunctionExpression::Window {
                name,
                arguments,
                partition_by,
                order_by,
                frame,
                alias,
            } => refined::FunctionExpression::Window {
                name,
                arguments: self.rd_vec(arguments)?,
                partition_by: self.rd_vec(partition_by)?,
                order_by: order_by
                    .into_iter()
                    .map(|spec| {
                        Ok(refined::OrderingSpec {
                            column: self.rd(spec.column)?,
                            direction: spec.direction,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
                frame: frame.map(|f| self.refine_window_frame(f)).transpose()?,
                alias,
            },
            resolved::FunctionExpression::JsonPath {
                source,
                path,
                alias,
            } => refined::FunctionExpression::JsonPath {
                source: self.rd_box(source)?,
                path: self.rd_box(path)?,
                alias,
            },
        })
    }

    fn refine_case_arm(&self, arm: resolved::CaseArm) -> Result<refined::CaseArm> {
        Ok(match arm {
            resolved::CaseArm::Simple {
                test_expr,
                value,
                result,
            } => refined::CaseArm::Simple {
                test_expr: self.rd_box(test_expr)?,
                value,
                result: self.rd_box(result)?,
            },
            resolved::CaseArm::CurriedSimple { value, result } => refined::CaseArm::CurriedSimple {
                value,
                result: self.rd_box(result)?,
            },
            resolved::CaseArm::Searched { condition, result } => refined::CaseArm::Searched {
                condition: self.rb_box(condition)?,
                result: self.rd_box(result)?,
            },
            resolved::CaseArm::Default { result } => refined::CaseArm::Default {
                result: self.rd_box(result)?,
            },
        })
    }

    fn refine_curly_member(&self, member: resolved::CurlyMember) -> Result<refined::CurlyMember> {
        Ok(match member {
            resolved::CurlyMember::Shorthand {
                column,
                qualifier,
                schema,
            } => refined::CurlyMember::Shorthand {
                column,
                qualifier,
                schema,
            },
            resolved::CurlyMember::Comparison { condition } => refined::CurlyMember::Comparison {
                condition: self.rb_box(condition)?,
            },
            resolved::CurlyMember::KeyValue {
                key,
                nested_reduction,
                value,
            } => refined::CurlyMember::KeyValue {
                key,
                nested_reduction,
                value: self.rd_box(value)?,
            },
            resolved::CurlyMember::PathLiteral { path, alias } => {
                refined::CurlyMember::PathLiteral {
                    path: self.rd_box(path)?,
                    alias,
                }
            }
            resolved::CurlyMember::Glob
            | resolved::CurlyMember::Pattern { .. }
            | resolved::CurlyMember::OrdinalRange { .. } => {
                return Err(DelightQLError::ParseError {
                    message: "Glob/Pattern/OrdinalRange in curly member should have been expanded by resolver".to_string(),
                    source: None,
                    subcategory: None,
                });
            }
            resolved::CurlyMember::Placeholder => {
                return Err(DelightQLError::ParseError {
                    message:
                        "Placeholder in curly member should only appear in destructuring context"
                            .to_string(),
                    source: None,
                    subcategory: None,
                });
            }
        })
    }

    fn refine_window_frame(&self, frame: resolved::WindowFrame) -> Result<refined::WindowFrame> {
        Ok(refined::WindowFrame {
            mode: frame.mode,
            start: self.refine_frame_bound(frame.start)?,
            end: self.refine_frame_bound(frame.end)?,
        })
    }

    fn refine_frame_bound(&self, bound: resolved::FrameBound) -> Result<refined::FrameBound> {
        Ok(match bound {
            resolved::FrameBound::Unbounded => refined::FrameBound::Unbounded,
            resolved::FrameBound::CurrentRow => refined::FrameBound::CurrentRow,
            resolved::FrameBound::Preceding(e) => refined::FrameBound::Preceding(self.rd_box(e)?),
            resolved::FrameBound::Following(e) => refined::FrameBound::Following(self.rd_box(e)?),
        })
    }
}
