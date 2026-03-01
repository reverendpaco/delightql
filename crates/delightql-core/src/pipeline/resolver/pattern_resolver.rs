// pattern_resolver.rs - Unified pattern resolution to prevent duplicate code paths
// This is the single entry point for ALL pattern types (Glob, GlobWithUsing, Positional, etc.)
//
// INTEGRATION STATUS: Phase 2 Complete - Architecture implemented but not yet wired in
//
// TO INTEGRATE (Phase 3):
// 1. In resolve_relation_with_registry, replace preserve_domain_spec() calls with:
//    - Create PatternResolver instance
//    - Call resolve_pattern() to get PatternResult
//    - Use PatternResult to:
//      a) Set output columns in BubbledState
//      b) Apply WHERE constraints as Filter expressions
//      c) Store JOIN conditions for later application
//
// 2. In JOIN resolution, check for PatternResult.using_columns and apply them
//
// This architecture is ready for positional patterns - when they're added to the
// grammar and builder, they'll automatically flow through this single unified path.

use super::string_templates::build_concat_chain_as_function;
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved::StringTemplatePart;
use crate::pipeline::asts::core::{ProjectionExpr, SubstitutionExpr};
use crate::pipeline::asts::unresolved::LiteralValue;
use crate::pipeline::asts::unresolved::NamespacePath;
use crate::pipeline::asts::{resolved as ast_resolved, unresolved as ast_unresolved};

/// Normalized representation of all column specifications
/// This allows us to handle all pattern types through a single code path
#[derive(Debug, Clone)]
pub enum NormalizedColumnSpec {
    /// SELECT * - all columns
    All,
    /// SELECT * WITH USING(...) - all columns with join deduplication
    AllWithUsing(Vec<String>),
    /// Explicit column selection (from positional or column lists)
    Explicit(Vec<ColumnSelection>),
}

/// Represents a single column selection with potential constraints
#[derive(Debug, Clone)]
pub struct ColumnSelection {
    /// Position in the source table (0-based)
    pub source_position: usize,
    /// Name to use in output
    pub output_name: String,
    /// Optional constraint on this column
    pub constraint: Option<PatternConstraint>,
}

/// Constraints that can be applied to columns in patterns
#[derive(Debug, Clone)]
pub enum PatternConstraint {
    /// Column must equal a literal value (e.g., = 3)
    Literal(LiteralValue),
    /// Column must equal another column (for unification)
    Reference(QualifiedColumnRef),
    /// Column should be skipped (placeholder _)
    Skip,
    /// Complex expression constraint
    Expression(Box<ast_unresolved::DomainExpression>),
}

/// Reference to a column that might be qualified
#[derive(Debug, Clone)]
pub struct QualifiedColumnRef {
    #[allow(dead_code)]
    pub table: Option<String>,
    pub column: String,
}

/// Context for join operations
#[derive(Debug)]
pub struct JoinContext {
    /// Columns available from the left side of the join
    pub left_columns: Vec<ast_resolved::ColumnMetadata>,
}

impl JoinContext {
    pub fn from(columns: &[ast_resolved::ColumnMetadata]) -> Self {
        JoinContext {
            left_columns: columns.to_vec(),
        }
    }

    pub fn has_column(&self, name: &str) -> bool {
        self.left_columns
            .iter()
            .any(|col| super::col_name_eq(col.name(), name))
    }

    pub fn find_column(&self, name: &str) -> Option<&ast_resolved::ColumnMetadata> {
        self.left_columns
            .iter()
            .find(|col| super::col_name_eq(col.name(), name))
    }
}

/// Result of pattern resolution
#[derive(Debug)]
pub struct PatternResult {
    /// Columns to output
    pub output_columns: Vec<ast_resolved::ColumnMetadata>,
    /// WHERE constraints to apply
    pub where_constraints: Vec<ast_resolved::BooleanExpression>,
    /// JOIN conditions to apply
    pub join_conditions: Vec<ast_resolved::BooleanExpression>,
    /// Columns for USING clause (if any)
    pub using_columns: Option<Vec<String>>,
}

/// The unified pattern resolver that makes duplicate paths impossible
pub struct PatternResolver {
    // For now, PatternResolver operates without registry dependency
    // Future: integrate with registry for CTE/schema lookups
}

impl Default for PatternResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl PatternResolver {
    pub fn new() -> Self {
        PatternResolver {}
    }

    /// THE SINGLE ENTRY POINT - All patterns go through here
    pub fn resolve_pattern(
        &self,
        pattern: &ast_unresolved::DomainSpec,
        table_schema: &[ast_resolved::ColumnMetadata],
        table_name: &str,
        join_context: Option<&JoinContext>,
    ) -> Result<PatternResult> {
        // First normalize to common representation
        let normalized = self.normalize_pattern(pattern, table_schema)?;

        // Then resolve through single path
        self.resolve_normalized(normalized, table_schema, table_name, join_context)
    }

    /// Convert any pattern type to normalized representation
    fn normalize_pattern(
        &self,
        pattern: &ast_unresolved::DomainSpec,
        table_schema: &[ast_resolved::ColumnMetadata],
    ) -> Result<NormalizedColumnSpec> {
        match pattern {
            ast_unresolved::DomainSpec::Glob => Ok(NormalizedColumnSpec::All),

            // Bare is like Glob but with unqualified names (handled at resolution time)
            ast_unresolved::DomainSpec::Bare => Ok(NormalizedColumnSpec::All),

            ast_unresolved::DomainSpec::GlobWithUsing(cols) => {
                Ok(NormalizedColumnSpec::AllWithUsing(cols.clone()))
            }

            ast_unresolved::DomainSpec::Positional(exprs) => {
                // Convert positional patterns to explicit selections
                let selections = self.positional_to_selections(exprs, table_schema)?;
                Ok(NormalizedColumnSpec::Explicit(selections))
            }
        }
    }

    /// Convert positional expressions to column selections
    fn positional_to_selections(
        &self,
        exprs: &[ast_unresolved::DomainExpression],
        table_schema: &[ast_resolved::ColumnMetadata],
    ) -> Result<Vec<ColumnSelection>> {
        let mut selections = Vec::new();

        for (idx, expr) in exprs.iter().enumerate() {
            if idx >= table_schema.len() {
                return Err(DelightQLError::ParseError {
                    message: format!(
                        "Positional pattern has {} elements but table has only {} columns",
                        exprs.len(),
                        table_schema.len()
                    ),
                    source: None,
                    subcategory: None,
                });
            }

            match expr {
                // Simple identifier: users(id, name, email)
                ast_unresolved::DomainExpression::Lvar {
                    name, qualifier, ..
                } => {
                    if qualifier.is_some() {
                        // Qualified reference like o.status - this is for unification
                        selections.push(ColumnSelection {
                            source_position: idx,
                            output_name: name.to_string(),
                            constraint: Some(PatternConstraint::Reference(QualifiedColumnRef {
                                table: qualifier.as_ref().map(|s| s.to_string()),
                                column: name.to_string(),
                            })),
                        });
                    } else {
                        // Simple name - rename the column
                        selections.push(ColumnSelection {
                            source_position: idx,
                            output_name: name.to_string(),
                            constraint: None,
                        });
                    }
                }

                // Placeholder: users(_, name, _)
                ast_unresolved::DomainExpression::NonUnifiyingUnderscore => {
                    selections.push(ColumnSelection {
                        source_position: idx,
                        output_name: table_schema[idx].name().to_string(),
                        constraint: Some(PatternConstraint::Skip),
                    });
                }

                // Literal constraint: reviews(_, _, _, 3, _)
                ast_unresolved::DomainExpression::Literal { value, .. } => {
                    // This position must equal the literal
                    selections.push(ColumnSelection {
                        source_position: idx,
                        output_name: table_schema[idx].name().to_string(),
                        constraint: Some(PatternConstraint::Literal(value.clone())),
                    });
                }

                // Complex expression (future extension)
                _ => {
                    selections.push(ColumnSelection {
                        source_position: idx,
                        output_name: table_schema[idx].name().to_string(),
                        constraint: Some(PatternConstraint::Expression(Box::new(expr.clone()))),
                    });
                }
            }
        }

        Ok(selections)
    }

    /// Resolve normalized pattern to final result
    fn resolve_normalized(
        &self,
        spec: NormalizedColumnSpec,
        table_schema: &[ast_resolved::ColumnMetadata],
        table_name: &str,
        join_context: Option<&JoinContext>,
    ) -> Result<PatternResult> {
        match spec {
            NormalizedColumnSpec::All => {
                // Simple SELECT * - return all columns, no constraints
                Ok(PatternResult {
                    output_columns: table_schema.to_vec(),
                    where_constraints: vec![],
                    join_conditions: vec![],
                    using_columns: None,
                })
            }

            NormalizedColumnSpec::AllWithUsing(cols) => {
                // SELECT * with USING deduplication
                let join_conditions = if let Some(ctx) = join_context {
                    self.generate_using_conditions(&cols, table_name, ctx)?
                } else {
                    vec![]
                };

                Ok(PatternResult {
                    output_columns: table_schema.to_vec(),
                    where_constraints: vec![],
                    join_conditions,
                    using_columns: Some(cols),
                })
            }

            NormalizedColumnSpec::Explicit(selections) => {
                self.resolve_explicit_selections(selections, table_schema, table_name, join_context)
            }
        }
    }

    /// Resolve explicit column selections (the complex case)
    fn resolve_explicit_selections(
        &self,
        selections: Vec<ColumnSelection>,
        table_schema: &[ast_resolved::ColumnMetadata],
        table_name: &str,
        join_context: Option<&JoinContext>,
    ) -> Result<PatternResult> {
        let mut output_columns = Vec::new();
        let mut where_constraints = Vec::new();
        let mut join_conditions = Vec::new();
        let mut using_columns = Vec::new();

        for sel in selections {
            // Skip placeholder columns
            if matches!(sel.constraint, Some(PatternConstraint::Skip)) {
                continue;
            }

            // Get the source column
            let source_col = &table_schema[sel.source_position];

            // Create output column with potential rename
            let mut output_col = source_col.clone();
            if sel.output_name != source_col.name() {
                // Column is being renamed — mark as user-named since the user
                // explicitly chose this name in a positional binding like table(x, y)
                output_col = output_col.with_name(sel.output_name.clone());
                output_col.has_user_name = true;
            }

            // Handle constraints
            if let Some(constraint) = sel.constraint {
                match constraint {
                    PatternConstraint::Literal(val) => {
                        // Generate WHERE constraint: column = literal
                        where_constraints
                            .push(create_literal_constraint(source_col, table_name, val));

                        // Mark column for hygienic aliasing (will be hidden from output)
                        output_col.needs_hygienic_alias = true;
                        output_columns.push(output_col);
                    }

                    PatternConstraint::Reference(qual_ref) => {
                        // Generate JOIN condition for unification
                        if let Some(ctx) = join_context {
                            if let Some(left_col) = ctx.find_column(&qual_ref.column) {
                                join_conditions.push(create_unification_condition(
                                    left_col, source_col, table_name,
                                ));
                                using_columns.push(sel.output_name.clone());
                            }
                        }
                        // Reference constraint - add to output for JOIN unification
                        output_columns.push(output_col);
                    }

                    PatternConstraint::Skip => {
                        // Already handled above
                    }

                    PatternConstraint::Expression(expr) => {
                        // Generate WHERE constraint: column = expression
                        where_constraints
                            .push(create_expression_constraint(source_col, table_name, &expr)?);

                        // Mark column for hygienic aliasing (will be hidden from output)
                        output_col.needs_hygienic_alias = true;
                        output_columns.push(output_col);
                    }
                }
            } else {
                // No constraint - regular column, add to output
                output_columns.push(output_col);
            }

            // Check for implicit unification (same column name in join)
            if let Some(ctx) = join_context {
                if ctx.has_column(&sel.output_name) && !using_columns.contains(&sel.output_name) {
                    // This column exists on the left - create unification
                    if let Some(left_col) = ctx.find_column(&sel.output_name) {
                        join_conditions.push(create_unification_condition(
                            left_col, source_col, table_name,
                        ));
                        using_columns.push(sel.output_name.clone());
                    }
                }
            }
        }

        Ok(PatternResult {
            output_columns,
            where_constraints,
            join_conditions,
            using_columns: if using_columns.is_empty() {
                None
            } else {
                Some(using_columns)
            },
        })
    }

    /// Generate USING conditions for join
    fn generate_using_conditions(
        &self,
        using_cols: &[String],
        right_table: &str,
        join_context: &JoinContext,
    ) -> Result<Vec<ast_resolved::BooleanExpression>> {
        let mut conditions = Vec::new();

        for col_name in using_cols {
            if let Some(left_col) = join_context.find_column(col_name) {
                // Create equality condition: left.col = right.col
                let right_ref = ast_resolved::DomainExpression::Lvar {
                    name: col_name.clone().into(),
                    qualifier: Some(right_table.into()),
                    namespace_path: NamespacePath::empty(),
                    alias: None,
                    provenance: ast_resolved::PhaseBox::phantom(),
                };

                let left_qualifier = match &left_col.fq_table.name {
                    ast_resolved::TableName::Named(name) => Some(name.to_string()),
                    ast_resolved::TableName::Fresh => None,
                };

                let left_ref = ast_resolved::DomainExpression::Lvar {
                    name: col_name.clone().into(),
                    qualifier: left_qualifier.map(|s| s.into()),
                    namespace_path: NamespacePath::empty(),
                    alias: None,
                    provenance: ast_resolved::PhaseBox::phantom(),
                };

                conditions.push(ast_resolved::BooleanExpression::Comparison {
                    operator: "=".to_string(),
                    left: Box::new(left_ref),
                    right: Box::new(right_ref),
                });
            }
        }

        Ok(conditions)
    }
}

// Helper functions for creating constraints

fn create_literal_constraint(
    column: &ast_resolved::ColumnMetadata,
    table_name: &str,
    value: LiteralValue,
) -> ast_resolved::BooleanExpression {
    let col_ref = ast_resolved::DomainExpression::Lvar {
        name: column.name().into(),
        qualifier: Some(table_name.into()),
        namespace_path: NamespacePath::empty(),
        alias: None,
        provenance: ast_resolved::PhaseBox::phantom(),
    };

    let literal = ast_resolved::DomainExpression::Literal { value, alias: None };

    ast_resolved::BooleanExpression::Comparison {
        operator: "traditional_eq".to_string(),
        left: Box::new(col_ref),
        right: Box::new(literal),
    }
}

fn create_unification_condition(
    left_col: &ast_resolved::ColumnMetadata,
    right_col: &ast_resolved::ColumnMetadata,
    right_table: &str,
) -> ast_resolved::BooleanExpression {
    let left_qualifier = match &left_col.fq_table.name {
        ast_resolved::TableName::Named(name) => Some(name.to_string()),
        ast_resolved::TableName::Fresh => None,
    };

    let left_ref = ast_resolved::DomainExpression::Lvar {
        name: left_col.name().into(),
        qualifier: left_qualifier.map(|s| s.into()),
        namespace_path: NamespacePath::empty(),
        alias: None,
        provenance: ast_resolved::PhaseBox::phantom(),
    };

    let right_ref = ast_resolved::DomainExpression::Lvar {
        name: right_col.name().into(),
        qualifier: Some(right_table.into()),
        namespace_path: NamespacePath::empty(),
        alias: None,
        provenance: ast_resolved::PhaseBox::phantom(),
    };

    ast_resolved::BooleanExpression::Comparison {
        operator: "traditional_eq".to_string(),
        left: Box::new(left_ref),
        right: Box::new(right_ref),
    }
}

fn create_expression_constraint(
    column: &ast_resolved::ColumnMetadata,
    table_name: &str,
    expr: &ast_unresolved::DomainExpression,
) -> Result<ast_resolved::BooleanExpression> {
    let col_ref = ast_resolved::DomainExpression::Lvar {
        name: column.name().into(),
        qualifier: Some(table_name.into()),
        namespace_path: NamespacePath::empty(),
        alias: None,
        provenance: ast_resolved::PhaseBox::phantom(),
    };

    // Convert unresolved expression to resolved expression
    // For now, we'll do a simple conversion - this should be enhanced
    // to properly resolve the expression through the resolver pipeline
    let resolved_expr = convert_unresolved_to_resolved_expression(expr)?;

    Ok(ast_resolved::BooleanExpression::Comparison {
        operator: "traditional_eq".to_string(),
        left: Box::new(col_ref),
        right: Box::new(resolved_expr),
    })
}

fn convert_unresolved_to_resolved_expression(
    expr: &ast_unresolved::DomainExpression,
) -> Result<ast_resolved::DomainExpression> {
    match expr {
        ast_unresolved::DomainExpression::Literal { value, alias } => {
            Ok(ast_resolved::DomainExpression::Literal {
                value: value.clone(),
                alias: alias.clone(),
            })
        }
        ast_unresolved::DomainExpression::Lvar {
            name,
            qualifier,
            namespace_path,
            alias,
            provenance: _,
        } => Ok(ast_resolved::DomainExpression::Lvar {
            name: name.clone(),
            qualifier: qualifier.clone(),
            namespace_path: namespace_path.clone(),
            alias: alias.clone(),
            provenance: ast_resolved::PhaseBox::phantom(),
        }),
        ast_unresolved::DomainExpression::Function(func) => Ok(
            ast_resolved::DomainExpression::Function(convert_unresolved_function(func)?),
        ),
        ast_unresolved::DomainExpression::NonUnifiyingUnderscore => {
            Ok(ast_resolved::DomainExpression::NonUnifiyingUnderscore)
        }
        ast_unresolved::DomainExpression::ValuePlaceholder { alias } => {
            Ok(ast_resolved::DomainExpression::ValuePlaceholder {
                alias: alias.clone(),
            })
        }
        ast_unresolved::DomainExpression::Substitution(ref sub) => match sub {
            SubstitutionExpr::Parameter { .. }
            | SubstitutionExpr::CurriedParameter { .. }
            | SubstitutionExpr::ContextMarker => {
                Ok(ast_resolved::DomainExpression::Substitution(sub.clone()))
            }
            SubstitutionExpr::ContextParameter { .. } => {
                // ContextParameter should never exist in unresolved phase - it's only created during
                // postprocessing in refined phase for CCAFE feature
                Err(DelightQLError::ParseError {
                    message: "ContextParameter should not appear in unresolved phase".to_string(),
                    source: None,
                    subcategory: None,
                })
            }
        },
        ast_unresolved::DomainExpression::Projection(ref proj) => match proj {
            ProjectionExpr::Glob {
                qualifier,
                namespace_path,
            } => Ok(ast_resolved::DomainExpression::Projection(
                ProjectionExpr::Glob {
                    qualifier: qualifier.clone(),
                    namespace_path: namespace_path.clone(),
                },
            )),
            ProjectionExpr::Pattern { pattern, alias } => {
                // Patterns are preserved for later expansion
                Ok(ast_resolved::DomainExpression::Projection(
                    ProjectionExpr::Pattern {
                        pattern: pattern.clone(),
                        alias: alias.clone(),
                    },
                ))
            }
            ProjectionExpr::ColumnRange(_) => {
                // These should be resolved by now in patterns
                Err(DelightQLError::ParseError {
                    message: "Column ranges not supported in pattern constraints".to_string(),
                    source: None,
                    subcategory: None,
                })
            }
            ProjectionExpr::JsonPathLiteral {
                segments,
                root_is_array,
                alias,
            } => Ok(ast_resolved::DomainExpression::Projection(
                ProjectionExpr::JsonPathLiteral {
                    segments: segments.clone(),
                    root_is_array: *root_is_array,
                    alias: alias.clone(),
                },
            )),
        },
        ast_unresolved::DomainExpression::Predicate { expr, alias } => {
            // Convert the predicate expression
            let resolved_pred = convert_unresolved_boolean_expression(expr)?;
            Ok(ast_resolved::DomainExpression::Predicate {
                expr: Box::new(resolved_pred),
                alias: alias.clone(),
            })
        }
        ast_unresolved::DomainExpression::PipedExpression {
            value,
            transforms,
            alias,
        } => {
            // Convert the value and transforms
            Ok(ast_resolved::DomainExpression::PipedExpression {
                value: Box::new(convert_unresolved_to_resolved_expression(value)?),
                transforms: transforms
                    .iter()
                    .map(convert_unresolved_function)
                    .collect::<Result<Vec<_>>>()?,
                alias: alias.clone(),
            })
        }
        ast_unresolved::DomainExpression::Parenthesized { inner, alias } => {
            Ok(ast_resolved::DomainExpression::Parenthesized {
                inner: Box::new(convert_unresolved_to_resolved_expression(inner)?),
                alias: alias.clone(),
            })
        }
        ast_unresolved::DomainExpression::Tuple { elements, alias } => {
            Ok(ast_resolved::DomainExpression::Tuple {
                elements: elements
                    .iter()
                    .map(convert_unresolved_to_resolved_expression)
                    .collect::<Result<_>>()?,
                alias: alias.clone(),
            })
        }
        ast_unresolved::DomainExpression::ScalarSubquery { .. } => {
            Err(DelightQLError::ParseError {
                message: "Scalar subqueries not supported in pattern constraints".to_string(),
                source: None,
                subcategory: None,
            })
        }
        ast_unresolved::DomainExpression::ColumnOrdinal(_) => {
            // These should be resolved by now in patterns
            Err(DelightQLError::ParseError {
                message: "Column ordinals not supported in pattern constraints".to_string(),
                source: None,
                subcategory: None,
            })
        }

        // Pivot: convert both children
        ast_unresolved::DomainExpression::PivotOf {
            value_column,
            pivot_key,
            pivot_values,
        } => Ok(ast_resolved::DomainExpression::PivotOf {
            value_column: Box::new(convert_unresolved_to_resolved_expression(value_column)?),
            pivot_key: Box::new(convert_unresolved_to_resolved_expression(pivot_key)?),
            pivot_values: pivot_values.clone(),
        }),
    }
}

fn convert_unresolved_function(
    func: &ast_unresolved::FunctionExpression,
) -> Result<ast_resolved::FunctionExpression> {
    match func {
        ast_unresolved::FunctionExpression::Regular {
            name,
            namespace,
            arguments,
            alias,
            conditioned_on,
        } => {
            let resolved_args: Result<Vec<_>> = arguments
                .iter()
                .map(convert_unresolved_to_resolved_expression)
                .collect();
            let resolved_condition = conditioned_on
                .as_ref()
                .map(|cond| convert_unresolved_boolean_expression(cond.as_ref()))
                .transpose()?;
            Ok(ast_resolved::FunctionExpression::Regular {
                name: name.clone(),
                namespace: namespace.clone(),
                arguments: resolved_args?,
                alias: alias.clone(),
                conditioned_on: resolved_condition.map(Box::new),
            })
        }
        ast_unresolved::FunctionExpression::Curried {
            name,
            namespace,
            arguments,
            conditioned_on,
        } => {
            let resolved_args: Result<Vec<_>> = arguments
                .iter()
                .map(convert_unresolved_to_resolved_expression)
                .collect();
            let resolved_condition = conditioned_on
                .as_ref()
                .map(|cond| convert_unresolved_boolean_expression(cond.as_ref()))
                .transpose()?;
            Ok(ast_resolved::FunctionExpression::Curried {
                name: name.clone(),
                namespace: namespace.clone(),
                arguments: resolved_args?,
                conditioned_on: resolved_condition.map(Box::new),
            })
        }
        ast_unresolved::FunctionExpression::Bracket { arguments, alias } => {
            let resolved_args: Result<Vec<_>> = arguments
                .iter()
                .map(convert_unresolved_to_resolved_expression)
                .collect();
            Ok(ast_resolved::FunctionExpression::Bracket {
                arguments: resolved_args?,
                alias: alias.clone(),
            })
        }
        ast_unresolved::FunctionExpression::Infix {
            operator,
            left,
            right,
            alias,
        } => Ok(ast_resolved::FunctionExpression::Infix {
            operator: operator.clone(),
            left: Box::new(convert_unresolved_to_resolved_expression(left)?),
            right: Box::new(convert_unresolved_to_resolved_expression(right)?),
            alias: alias.clone(),
        }),
        ast_unresolved::FunctionExpression::Lambda { body, alias } => {
            Ok(ast_resolved::FunctionExpression::Lambda {
                body: Box::new(convert_unresolved_to_resolved_expression(body)?),
                alias: alias.clone(),
            })
        }
        ast_unresolved::FunctionExpression::StringTemplate { parts, alias } => {
            // Expand StringTemplate to concat expression here for pattern resolution
            // Convert parts to resolved
            let mut resolved_parts = Vec::new();
            for part in parts {
                match part {
                    ast_unresolved::StringTemplatePart::Text(text) => {
                        resolved_parts.push(StringTemplatePart::Text(text.clone()));
                    }
                    ast_unresolved::StringTemplatePart::Interpolation(expr) => {
                        let resolved_expr = convert_unresolved_to_resolved_expression(expr)?;
                        resolved_parts
                            .push(StringTemplatePart::Interpolation(Box::new(resolved_expr)));
                    }
                }
            }

            // Build concat chain from parts
            Ok(build_concat_chain_as_function(
                resolved_parts,
                alias.clone(),
            ))
        }
        ast_unresolved::FunctionExpression::CaseExpression { .. } => {
            Err(crate::error::DelightQLError::not_implemented(
                "CASE expression in positional pattern context",
            ))
        }
        ast_unresolved::FunctionExpression::HigherOrder {
            name,
            curried_arguments,
            regular_arguments,
            alias,
            conditioned_on,
        } => {
            // Process curried arguments
            let resolved_curried: Result<Vec<_>> = curried_arguments
                .iter()
                .map(convert_unresolved_to_resolved_expression)
                .collect();

            // Process regular arguments
            let resolved_regular: Result<Vec<_>> = regular_arguments
                .iter()
                .map(convert_unresolved_to_resolved_expression)
                .collect();

            // Process filter condition if present
            let resolved_condition = conditioned_on
                .as_ref()
                .map(|cond| convert_unresolved_boolean_expression(cond.as_ref()))
                .transpose()?;

            Ok(ast_resolved::FunctionExpression::HigherOrder {
                name: name.clone(),
                curried_arguments: resolved_curried?,
                regular_arguments: resolved_regular?,
                alias: alias.clone(),
                conditioned_on: resolved_condition.map(Box::new),
            })
        }
        ast_unresolved::FunctionExpression::Curly {
            members,
            inner_grouping_keys: _,
            cte_requirements: _,
            alias,
        } => {
            // Tree groups pass through unchanged (Epoch 1)
            use crate::pipeline::asts::{resolved, unresolved};
            let resolved_members: Vec<resolved::CurlyMember> = members
                .iter()
                .map(|m| -> Result<resolved::CurlyMember> {
                    Ok(match m {
                        unresolved::CurlyMember::Shorthand {
                            column,
                            qualifier,
                            schema,
                        } => resolved::CurlyMember::Shorthand {
                            column: column.clone(),
                            qualifier: qualifier.clone(),
                            schema: schema.clone(),
                        },
                        unresolved::CurlyMember::Comparison { condition } => {
                            resolved::CurlyMember::Comparison {
                                condition: Box::new(convert_unresolved_boolean_expression(
                                    condition,
                                )?),
                            }
                        }
                        unresolved::CurlyMember::KeyValue {
                            key,
                            nested_reduction,
                            value,
                        } => resolved::CurlyMember::KeyValue {
                            key: key.clone(),
                            nested_reduction: *nested_reduction,
                            value: Box::new(convert_unresolved_to_resolved_expression(value)?),
                        },
                        // TG-ERGONOMIC-INDUCTOR: Pass through - will be expanded in main resolver
                        unresolved::CurlyMember::Glob => resolved::CurlyMember::Glob,
                        unresolved::CurlyMember::Pattern { pattern } => {
                            resolved::CurlyMember::Pattern {
                                pattern: pattern.clone(),
                            }
                        }
                        unresolved::CurlyMember::OrdinalRange { start, end } => {
                            resolved::CurlyMember::OrdinalRange {
                                start: *start,
                                end: *end,
                            }
                        }
                        // Placeholder passes through to resolved phase
                        unresolved::CurlyMember::Placeholder => resolved::CurlyMember::Placeholder,
                        // PATH FIRST-CLASS: Epoch 4 - PathLiteral passes through with path conversion
                        unresolved::CurlyMember::PathLiteral { path, alias } => {
                            resolved::CurlyMember::PathLiteral {
                                path: Box::new(convert_unresolved_to_resolved_expression(path)?),
                                alias: alias.clone(),
                            }
                        }
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(ast_resolved::FunctionExpression::Curly {
                members: resolved_members,
                inner_grouping_keys: vec![], // Pattern resolver doesn't populate this
                cte_requirements: None,      // Phase R2+ will populate this
                alias: alias.clone(),
            })
        }
        ast_unresolved::FunctionExpression::MetadataTreeGroup {
            key_column,
            key_qualifier,
            key_schema,
            constructor,
            alias,
            keys_only,
            cte_requirements: _,
        } => {
            // Tree groups pass through unchanged (Epoch 1)
            Ok(ast_resolved::FunctionExpression::MetadataTreeGroup {
                key_column: key_column.clone(),
                key_qualifier: key_qualifier.clone(),
                key_schema: key_schema.clone(),
                constructor: Box::new(convert_unresolved_function(constructor)?),
                keys_only: *keys_only,
                cte_requirements: None,
                alias: alias.clone(),
            })
        }
        ast_unresolved::FunctionExpression::Window {
            name,
            arguments,
            partition_by,
            order_by,
            frame: _frame,
            alias,
        } => {
            // Window functions: convert arguments, partition_by, and order_by
            let resolved_arguments = arguments
                .iter()
                .map(convert_unresolved_to_resolved_expression)
                .collect::<Result<Vec<_>>>()?;

            let resolved_partition = partition_by
                .iter()
                .map(convert_unresolved_to_resolved_expression)
                .collect::<Result<Vec<_>>>()?;

            let resolved_order = order_by
                .iter()
                .map(|spec| {
                    Ok(ast_resolved::OrderingSpec {
                        column: convert_unresolved_to_resolved_expression(&spec.column)?,
                        direction: spec.direction.clone(),
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(ast_resolved::FunctionExpression::Window {
                name: name.clone(),
                arguments: resolved_arguments,
                partition_by: resolved_partition,
                order_by: resolved_order,
                frame: None, // Frame bounds not resolved in pattern resolution
                alias: alias.clone(),
            })
        }
        _ => unimplemented!("JsonPath not yet implemented in this phase"),
    }
}

fn convert_unresolved_boolean_expression(
    expr: &ast_unresolved::BooleanExpression,
) -> Result<ast_resolved::BooleanExpression> {
    match expr {
        ast_unresolved::BooleanExpression::Comparison {
            operator,
            left,
            right,
        } => Ok(ast_resolved::BooleanExpression::Comparison {
            operator: operator.clone(),
            left: Box::new(convert_unresolved_to_resolved_expression(left)?),
            right: Box::new(convert_unresolved_to_resolved_expression(right)?),
        }),
        ast_unresolved::BooleanExpression::And { left, right } => {
            Ok(ast_resolved::BooleanExpression::And {
                left: Box::new(convert_unresolved_boolean_expression(left)?),
                right: Box::new(convert_unresolved_boolean_expression(right)?),
            })
        }
        ast_unresolved::BooleanExpression::Or { left, right } => {
            Ok(ast_resolved::BooleanExpression::Or {
                left: Box::new(convert_unresolved_boolean_expression(left)?),
                right: Box::new(convert_unresolved_boolean_expression(right)?),
            })
        }
        ast_unresolved::BooleanExpression::Not { expr } => {
            Ok(ast_resolved::BooleanExpression::Not {
                expr: Box::new(convert_unresolved_boolean_expression(expr)?),
            })
        }
        ast_unresolved::BooleanExpression::Using { columns } => {
            Ok(ast_resolved::BooleanExpression::Using {
                columns: columns.clone(),
            })
        }
        ast_unresolved::BooleanExpression::InnerExists {
            exists: _,
            identifier: _,
            subquery: _,
            alias: _,
            using_columns: _,
        } => {
            // For now, we'll skip complex subquery conversion
            // This would need proper relational expression conversion
            Err(DelightQLError::ParseError {
                message: "EXISTS expressions not supported in pattern constraints".to_string(),
                source: None,
                subcategory: None,
            })
        }
        ast_unresolved::BooleanExpression::In {
            value,
            set,
            negated,
        } => Ok(ast_resolved::BooleanExpression::In {
            value: Box::new(convert_unresolved_to_resolved_expression(value)?),
            set: set
                .iter()
                .map(convert_unresolved_to_resolved_expression)
                .collect::<Result<Vec<_>>>()?,
            negated: *negated,
        }),
        ast_unresolved::BooleanExpression::InRelational { .. } => Err(DelightQLError::ParseError {
            message: "IN subquery expressions not supported in pattern constraints".to_string(),
            source: None,
            subcategory: None,
        }),
        ast_unresolved::BooleanExpression::BooleanLiteral { value } => {
            Ok(ast_resolved::BooleanExpression::BooleanLiteral { value: *value })
        }
        ast_unresolved::BooleanExpression::Sigma { .. } => {
            // Sigma predicates not yet fully supported in pattern context
            Err(crate::error::DelightQLError::not_implemented(
                "Sigma predicates in pattern destructuring not yet supported",
            ))
        }
        ast_unresolved::BooleanExpression::GlobCorrelation { left, right } => {
            Ok(ast_resolved::BooleanExpression::GlobCorrelation {
                left: left.clone(),
                right: right.clone(),
            })
        }
        ast_unresolved::BooleanExpression::OrdinalGlobCorrelation { left, right } => {
            Ok(ast_resolved::BooleanExpression::OrdinalGlobCorrelation {
                left: left.clone(),
                right: right.clone(),
            })
        }
    }
}

// Extension trait for ColumnMetadata
trait ColumnMetadataExt {
    fn with_name(&self, name: String) -> ast_resolved::ColumnMetadata;
}

impl ColumnMetadataExt for ast_resolved::ColumnMetadata {
    fn with_name(&self, name: String) -> ast_resolved::ColumnMetadata {
        let mut renamed = self.clone();
        // Set the new name as an alias
        renamed.set_alias(name);
        renamed
    }
}
