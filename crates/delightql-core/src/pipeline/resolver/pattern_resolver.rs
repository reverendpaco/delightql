// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// pattern_resolver.rs - Unified pattern resolution to prevent duplicate code paths
// This is the single entry point for ALL pattern types (Glob, GlobWithUsing, Positional, etc.)
//
// This is the LIVE entry point: `resolve_pattern()` serves resolver_fold,
// relation_resolver, and grounding for every pattern kind — positional
// patterns (full-arity destructuring) included.

use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_transform::{walk_transform_boolean, walk_transform_domain, AstTransform};
use crate::pipeline::asts::core::{
    BooleanExpression, DomainExpression, ProjectionExpr, Resolved, SubstitutionExpr, Unresolved,
};
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
            .any(|col| delightql_types::SqlIdentifier::str_eq(col.name(), name))
    }

    pub fn find_column(&self, name: &str) -> Option<&ast_resolved::ColumnMetadata> {
        self.left_columns
            .iter()
            .find(|col| delightql_types::SqlIdentifier::str_eq(col.name(), name))
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

            // GlobWithUsingAll: USING expansion happens at the join level, not here
            ast_unresolved::DomainSpec::GlobWithUsingAll => Ok(NormalizedColumnSpec::All),

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

                let left_qualifier = match left_col.qualifier() {
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
    let left_qualifier = match left_col.qualifier() {
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

/// Strict phase converter: Unresolved → Resolved using AstTransform's default walks.
/// Unlike the permissive `PhaseConverter` in type_conversion.rs (which uses placeholders),
/// this converter returns errors for unsupported variants in the pattern constraint context:
/// ColumnOrdinal, ColumnRange, ScalarSubquery, InnerExists, InRelational, Sigma.
struct StrictPhaseConverter;

impl AstTransform<Unresolved, Resolved> for StrictPhaseConverter {
    fn transform_domain(
        &mut self,
        expr: DomainExpression<Unresolved>,
    ) -> Result<DomainExpression<Resolved>> {
        match expr {
            DomainExpression::ColumnOrdinal(_) => {
                // These should be resolved by now in patterns
                Err(DelightQLError::ParseError {
                    message: "Column ordinals not supported in pattern constraints".to_string(),
                    source: None,
                    subcategory: None,
                })
            }
            DomainExpression::Projection(ProjectionExpr::ColumnRange(_)) => {
                // These should be resolved by now in patterns
                Err(DelightQLError::ParseError {
                    message: "Column ranges not supported in pattern constraints".to_string(),
                    source: None,
                    subcategory: None,
                })
            }
            DomainExpression::ScalarSubquery { .. } => Err(DelightQLError::ParseError {
                message: "Scalar subqueries not supported in pattern constraints".to_string(),
                source: None,
                subcategory: None,
            }),
            DomainExpression::Substitution(SubstitutionExpr::ContextParameter { .. }) => {
                // ContextParameter should never exist in unresolved phase - it's only created during
                // postprocessing in refined phase for CCAFE feature
                Err(DelightQLError::ParseError {
                    message: "ContextParameter should not appear in unresolved phase".to_string(),
                    source: None,
                    subcategory: None,
                })
            }
            other => walk_transform_domain(self, other),
        }
    }

    fn transform_boolean(
        &mut self,
        expr: BooleanExpression<Unresolved>,
    ) -> Result<BooleanExpression<Resolved>> {
        match expr {
            BooleanExpression::InnerExists { .. } => {
                // Complex subquery conversion not supported in pattern constraints
                Err(DelightQLError::ParseError {
                    message: "EXISTS expressions not supported in pattern constraints".to_string(),
                    source: None,
                    subcategory: None,
                })
            }
            BooleanExpression::InRelational { .. } => Err(DelightQLError::ParseError {
                message: "IN subquery expressions not supported in pattern constraints".to_string(),
                source: None,
                subcategory: None,
            }),
            BooleanExpression::Sigma { .. } => {
                // Sigma predicates not yet fully supported in pattern context
                Err(DelightQLError::not_implemented(
                    "Sigma predicates in pattern destructuring not yet supported",
                ))
            }
            other => walk_transform_boolean(self, other),
        }
    }
}

fn convert_unresolved_to_resolved_expression(
    expr: &ast_unresolved::DomainExpression,
) -> Result<ast_resolved::DomainExpression> {
    StrictPhaseConverter.transform_domain(expr.clone())
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
