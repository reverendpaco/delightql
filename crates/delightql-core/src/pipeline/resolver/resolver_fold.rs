//! ResolverFold — the resolver as an AstTransform<Unresolved, Resolved>.
//!
//! All recursive calls within resolve_relational_impl now go through
//! `self.resolve_relational()` / `self.resolve_child()` instead of creating
//! temporary folds via the free function wrapper.
//!
//! Scope stack (`push_scope`/`pop_scope`) manages outer_context and grounding
//! at recursion boundaries. Expression-level hooks (transform_sigma,
//! transform_operator) read from `self.available` / `self.in_correlation`.
//!
//! The free function in mod.rs remains for callers outside this file
//! (relation_resolver, predicates, subqueries, etc.).

use super::unification::ColumnReference;
use super::{BubbledState, DmlPipeKind, ResolutionConfig};
use crate::pipeline::ast_resolved::NamespacePath;
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::asts::core::operators::DmlKind;
use crate::pipeline::asts::core::phases::{Resolved, Unresolved};
use crate::pipeline::asts::core::RelationalExpression;
use crate::pipeline::{ast_resolved, ast_unresolved};
use delightql_types::error::{DelightQLError, Result};
use delightql_types::schema::ColumnInfo;

/// Scope frame — tracks context at recursion boundaries.
struct ResolverScope {
    outer_context: Option<Vec<ast_resolved::ColumnMetadata>>,
    grounding: Option<ast_unresolved::GroundedPath>,
}

/// The resolver as an AstTransform<Unresolved, Resolved>.
///
/// Holds the EntityRegistry, config, and a scope stack. The `last_bubbled`
/// sidecar carries BubbledState out of transform_relational since the trait
/// return type is just `Result<Node<Q>>`.
pub(super) struct ResolverFold<'reg, 'db> {
    pub registry: &'reg mut crate::resolution::EntityRegistry<'db>,
    pub config: ResolutionConfig,
    scope: Vec<ResolverScope>,
    /// Populated by transform_relational, consumed by callers via take_bubbled().
    pub last_bubbled: Option<BubbledState>,
    /// Available columns for expression-level resolution. Set before calling
    /// transform_sigma / transform_operator / transform_domain / transform_boolean.
    pub(super) available: Vec<ast_resolved::ColumnMetadata>,
    /// Whether we're in a correlation context (for deferred validation).
    pub(super) in_correlation: bool,
    /// Pivot IN values for operator resolution.
    pivot_in_values: std::collections::HashMap<String, Vec<String>>,
    /// Output columns from the last operator resolution (sidecar like last_bubbled).
    last_operator_output: Option<Vec<ast_resolved::ColumnMetadata>>,
    /// Pending join input for inverted CTE strategy.
    /// Set by the Join handler when the right side is an HO TVF; consumed by resolve_tvf
    /// if the HO view has free scalar params.
    pub(super) pending_ho_join_input: Option<ast_unresolved::RelationalExpression>,
    /// Set to true when resolve_tvf absorbed the pending join input via inverted CTE.
    pub(super) ho_join_input_absorbed: bool,
}

impl<'reg, 'db> ResolverFold<'reg, 'db> {
    pub fn new(
        registry: &'reg mut crate::resolution::EntityRegistry<'db>,
        config: ResolutionConfig,
        outer_context: Option<Vec<ast_resolved::ColumnMetadata>>,
        grounding: Option<ast_unresolved::GroundedPath>,
    ) -> Self {
        Self {
            registry,
            config,
            scope: vec![ResolverScope {
                outer_context,
                grounding,
            }],
            last_bubbled: None,
            available: vec![],
            in_correlation: false,
            pivot_in_values: std::collections::HashMap::new(),
            last_operator_output: None,
            pending_ho_join_input: None,
            ho_join_input_absorbed: false,
        }
    }

    pub fn current_outer_context(&self) -> Option<&[ast_resolved::ColumnMetadata]> {
        self.scope.last().and_then(|s| s.outer_context.as_deref())
    }

    pub fn current_grounding(&self) -> Option<&ast_unresolved::GroundedPath> {
        self.scope.last().and_then(|s| s.grounding.as_ref())
    }

    fn push_scope(
        &mut self,
        outer: Option<Vec<ast_resolved::ColumnMetadata>>,
        grounding: Option<ast_unresolved::GroundedPath>,
    ) {
        self.scope.push(ResolverScope {
            outer_context: outer,
            grounding,
        });
    }

    fn pop_scope(&mut self) {
        self.scope.pop();
    }

    /// Push scope, resolve child through self.resolve_relational(), pop scope.
    /// Use for recursive calls that need DIFFERENT context than the current scope.
    fn resolve_child(
        &mut self,
        child: ast_unresolved::RelationalExpression,
        outer: Option<Vec<ast_resolved::ColumnMetadata>>,
        grounding: Option<ast_unresolved::GroundedPath>,
    ) -> Result<(ast_resolved::RelationalExpression, BubbledState)> {
        self.push_scope(outer, grounding);
        let result = self.resolve_relational(child);
        self.pop_scope();
        result
    }

    /// Convenience: transform_relational + extract BubbledState.
    pub fn resolve_relational(
        &mut self,
        expr: ast_unresolved::RelationalExpression,
    ) -> Result<(ast_resolved::RelationalExpression, BubbledState)> {
        let resolved = self.transform_relational(expr)?;
        let bubbled = self
            .last_bubbled
            .take()
            .expect("BubbledState must be set by transform_relational");
        Ok((resolved, bubbled))
    }

    /// Core relational resolution logic. Contains the match body formerly in
    /// `resolve_relational_expression_with_registry`.
    ///
    /// Reads outer_context and grounding from the scope stack — no parameters needed.
    #[stacksafe::stacksafe]
    pub(super) fn resolve_relational_impl(
        &mut self,
        expr: ast_unresolved::RelationalExpression,
    ) -> Result<(ast_resolved::RelationalExpression, BubbledState)> {
        let outer_context: Option<Vec<ast_resolved::ColumnMetadata>> =
            self.current_outer_context().map(|s| s.to_vec());
        let grounding: Option<ast_unresolved::GroundedPath> = self.current_grounding().cloned();

        // Borrow as refs for compatibility with existing code
        let outer_context = outer_context.as_deref();
        let grounding = grounding.as_ref();

        match expr {
            // Handle Relations specially to use resolve_entity
            ast_unresolved::RelationalExpression::Relation(rel) => {
                self.resolve_relation_impl(rel, outer_context, grounding)
            }

            // Handle Filter through registry (but check for EXISTS first)
            ast_unresolved::RelationalExpression::Filter {
                source,
                condition,
                origin,
                cpr_schema: _,
            } => {
                // Check for EXISTS in the condition and handle through registry
                if let ast_unresolved::SigmaCondition::Predicate(pred) = &condition {
                    if let ast_unresolved::BooleanExpression::InnerExists {
                        subquery,
                        exists,
                        identifier,
                        alias,
                        using_columns,
                    } = pred
                    {
                        // === INLINED handle_exists_subquery START ===
                        let resolved_subquery = {
                            let subquery_expr = *subquery.clone();
                            // Resolve the EXISTS subquery with current context for correlation
                            let combined_context = if let Some(outer) = outer_context {
                                // Combine outer context with current source columns
                                let (resolved_source_temp, _source_bubbled_temp) =
                                    self.resolve_relational(*source.clone())?;
                                let source_schema_temp =
                                    super::extract_cpr_schema(&resolved_source_temp)?;
                                let source_columns_temp = match &source_schema_temp {
                                    ast_resolved::CprSchema::Resolved(cols) => cols.clone(),
                                    other => panic!("catch-all hit in mod.rs resolve_relational_expression (EXISTS outer+source schema): {:?}", other),
                                };
                                let mut combined = outer.to_vec();
                                combined.extend(source_columns_temp);
                                Some(combined)
                            } else {
                                // Just use source columns for context
                                let (resolved_source_temp, _) =
                                    self.resolve_child(*source.clone(), None, grounding.cloned())?;
                                let source_schema_temp =
                                    super::extract_cpr_schema(&resolved_source_temp)?;
                                match &source_schema_temp {
                                    ast_resolved::CprSchema::Resolved(cols) => Some(cols.clone()),
                                    other => panic!("catch-all hit in mod.rs resolve_relational_expression (EXISTS source schema): {:?}", other),
                                }
                            };

                            // For EXISTS subqueries, the combined context contains outer
                            // source columns. Interdependent EXISTS (e.g.,
                            // +orders(...), +order_items(...), +products(, order_items.x = products.y))
                            // reference tables from sibling EXISTS scopes. Enrich the
                            // context with columns from all EXISTS tables found in the
                            // source expression so that cross-EXISTS references validate.
                            let mut enriched_context = combined_context.unwrap_or_default();
                            super::collect_exists_table_columns(
                                &*source,
                                self.registry,
                                &mut enriched_context,
                            )?;

                            // Config swap for EXISTS: validate_in_correlation = true
                            let exists_config = ResolutionConfig {
                                validate_in_correlation: true,
                                ..self.config.clone()
                            };
                            let saved_config = std::mem::replace(&mut self.config, exists_config);
                            let grounding_for_exists = grounding.cloned();
                            let result = self.resolve_child(
                                subquery_expr,
                                Some(enriched_context),
                                grounding_for_exists,
                            );
                            self.config = saved_config;
                            let (resolved_subquery, _) = result?;

                            resolved_subquery
                        };
                        // === INLINED handle_exists_subquery END ===

                        // Continue with normal filter processing but with resolved EXISTS
                        let (resolved_source, source_bubbled) = self.resolve_relational(*source)?;

                        let source_schema = super::extract_cpr_schema(&resolved_source)?;
                        let available_columns = match &source_schema {
                            ast_resolved::CprSchema::Resolved(cols) => cols.clone(),
                            other => panic!("catch-all hit in mod.rs resolve_relational_expression (TVF source schema): {:?}", other),
                        };

                        let resolved_identifier = ast_resolved::QualifiedName {
                            namespace_path: identifier.namespace_path.clone(),
                            name: identifier.name.clone(),
                            grounding: None,
                        };

                        // Synthesize correlation predicates from USING columns
                        let final_subquery = super::resolving::synthesize_using_correlation(
                            resolved_subquery,
                            using_columns,
                            &resolved_identifier,
                            &available_columns,
                        );

                        // Create resolved EXISTS condition
                        let resolved_exists = ast_resolved::BooleanExpression::InnerExists {
                            exists: *exists,
                            identifier: resolved_identifier,
                            subquery: Box::new(final_subquery),
                            alias: alias.clone(),
                            using_columns: using_columns.clone(),
                        };
                        let resolved_condition =
                            ast_resolved::SigmaCondition::Predicate(resolved_exists);

                        return Ok((
                            ast_resolved::RelationalExpression::Filter {
                                source: Box::new(resolved_source),
                                condition: resolved_condition,
                                origin,
                                cpr_schema: ast_resolved::PhaseBox::new(source_schema),
                            },
                            source_bubbled,
                        ));
                    }
                }

                let (resolved_source, source_bubbled) = self.resolve_relational(*source)?;

                let source_schema = super::extract_cpr_schema(&resolved_source)?;

                // Get columns for condition resolution.
                // Prefer source_bubbled.i_provide — it carries the user alias (e.g., `as a`)
                // so qualified refs like `a.first_name` can match. The cpr_schema on the
                // AST node may have internal body names (e.g., from ConsultedView expansion)
                // that don't reflect the alias.
                let source_columns = if !source_bubbled.i_provide.is_empty() {
                    source_bubbled.i_provide.clone()
                } else {
                    match &source_schema {
                        ast_resolved::CprSchema::Resolved(cols) => cols.clone(),
                        ast_resolved::CprSchema::Failed {
                            resolved_columns, ..
                        } => resolved_columns.clone(),
                        ast_resolved::CprSchema::Unresolved(cols) => cols.clone(),
                        ast_resolved::CprSchema::Unknown => vec![],
                    }
                };

                // Combine outer context with source columns for correlation support
                // This allows correlated predicates to reference both:
                // - Columns from the current source (e.g., orders.user_id)
                // - Columns from outer context (e.g., CFE parameters like buyer_id)
                let available_columns = if let Some(outer) = outer_context {
                    let mut combined = outer.to_vec();
                    combined.extend(source_columns);
                    combined
                } else {
                    source_columns
                };

                // Resolve condition using combined schema (source + outer context)
                // Use outer_context presence as heuristic for correlation contexts,
                // unless validate_in_correlation is set (EXISTS subqueries where
                // the full column set is known and validation is safe)
                self.in_correlation =
                    outer_context.is_some() && !self.config.validate_in_correlation;
                self.available = available_columns;
                let resolved_condition = self.transform_sigma(condition)?;

                // If this is a destructuring filter, add the destructured columns to the schema
                let final_schema = match &resolved_condition {
                    ast_resolved::SigmaCondition::Destructure {
                        destructured_schema,
                        ..
                    } => {
                        if std::env::var("DQL_DEBUG").is_ok() {
                            eprintln!("DESTRUCTURE FILTER DETECTED - adding columns to schema");
                        }
                        // Add destructured columns to source schema
                        let mut updated_columns = match &source_schema {
                            ast_resolved::CprSchema::Resolved(cols) => {
                                if std::env::var("DQL_DEBUG").is_ok() {
                                    eprintln!("Source has {} columns:", cols.len());
                                    for col in cols {
                                        eprintln!(
                                            "  - {}",
                                            col.info.original_name().unwrap_or("<no name>")
                                        );
                                    }
                                }
                                cols.clone()
                            }
                            other => {
                                panic!("catch-all hit in mod.rs resolve_relational_expression (destructure filter schema): {:?}", other);
                            }
                        };
                        for mapping in destructured_schema.data() {
                            if std::env::var("DQL_DEBUG").is_ok() {
                                eprintln!("Adding destructured column: {}", mapping.column_name);
                            }
                            updated_columns.push(ast_resolved::ColumnMetadata {
                                info: ast_resolved::ColumnProvenance::from_column(
                                    mapping.column_name.clone(),
                                ),
                                fq_table: ast_resolved::FqTable {
                                    parents_path: NamespacePath::empty(),
                                    name: ast_resolved::TableName::Fresh,
                                    backend_schema: ast_resolved::PhaseBox::from_optional_schema(
                                        None,
                                    ),
                                },
                                table_position: None,
                                has_user_name: true,
                                needs_hygienic_alias: false,
                                needs_sql_rename: false,
                                interior_schema: None,
                            });
                        }
                        if std::env::var("DQL_DEBUG").is_ok() {
                            eprintln!("Final schema has {} columns", updated_columns.len());
                        }
                        ast_resolved::CprSchema::Resolved(updated_columns)
                    }
                    _ => source_schema,
                };

                // Update bubbled state for destructuring filters
                let final_bubbled = match &resolved_condition {
                    ast_resolved::SigmaCondition::Destructure {
                        destructured_schema,
                        ..
                    } => {
                        // Add destructured columns to bubbled i_provide
                        let mut updated_bubbled = source_bubbled;
                        for mapping in destructured_schema.data() {
                            // Create ColumnMetadata for the destructured column
                            updated_bubbled
                                .i_provide
                                .push(ast_resolved::ColumnMetadata {
                                    info: ast_resolved::ColumnProvenance::from_column(
                                        mapping.column_name.clone(),
                                    ),
                                    fq_table: ast_resolved::FqTable {
                                        parents_path: NamespacePath::empty(),
                                        name: ast_resolved::TableName::Fresh,
                                        backend_schema:
                                            ast_resolved::PhaseBox::from_optional_schema(None),
                                    },
                                    table_position: None,
                                    has_user_name: true,
                                    needs_hygienic_alias: false,
                                    needs_sql_rename: false,
                                    interior_schema: None,
                                });
                        }
                        updated_bubbled
                    }
                    _ => source_bubbled,
                };

                Ok((
                    ast_resolved::RelationalExpression::Filter {
                        source: Box::new(resolved_source),
                        condition: resolved_condition,
                        origin,
                        cpr_schema: ast_resolved::PhaseBox::new(final_schema),
                    },
                    final_bubbled,
                ))
            }

            // Handle Join through registry
            ast_unresolved::RelationalExpression::Join {
                left,
                right,
                join_condition,
                join_type,
                cpr_schema: _,
            } => {
                // Inverted CTE strategy: if right side is a TVF (potential HO view),
                // stash the unresolved left so resolve_tvf can absorb it if needed.
                let right_is_tvf = matches!(
                    right.as_ref(),
                    ast_unresolved::RelationalExpression::Relation(
                        ast_unresolved::Relation::TVF { .. }
                    )
                );
                if right_is_tvf {
                    self.pending_ho_join_input = Some((*left).clone());
                    self.ho_join_input_absorbed = false;
                }

                let (resolved_left, left_bubbled) = self.resolve_relational(*left)?;

                let left_schema = super::extract_cpr_schema(&resolved_left)?;
                let left_columns = match &left_schema {
                    ast_resolved::CprSchema::Resolved(cols) => cols.clone(),
                    other => panic!("catch-all hit in mod.rs resolve_relational_expression (join left_columns): {:?}", other),
                };

                // For EXISTS joins, we need to combine outer context with left columns
                let right_context: Vec<ast_resolved::ColumnMetadata> =
                    if let Some(outer) = outer_context {
                        let mut combined = outer.to_vec();
                        combined.extend(left_columns.clone());
                        combined
                    } else {
                        left_columns.clone()
                    };

                // Check if right side uses positional patterns and needs unification
                let (resolved_right, right_bubbled, positional_join_condition, where_constraints) =
                    if let ast_unresolved::RelationalExpression::Relation(ref rel) = right.as_ref()
                    {
                        match rel {
                            ast_unresolved::Relation::Ground {
                                identifier,
                                canonical_name: _,
                                alias,
                                domain_spec: ast_unresolved::DomainSpec::Positional(patterns),
                                outer,
                                mutation_target: _,
                                passthrough: _,
                                cpr_schema: _,
                                hygienic_injections: _,
                            } => {
                                // Use the SAME pattern resolver that single tables use!
                                let table_name = &identifier.name;
                                let schema = self.registry.database.schema();

                                // Get table schema — check CTEs first, then database
                                let maybe_table_columns = if let Some(cte_schema) =
                                    self.registry.query_local.lookup_cte(table_name)
                                {
                                    match cte_schema {
                                        ast_resolved::CprSchema::Resolved(cols) => Some(
                                            cols.iter()
                                                .enumerate()
                                                .map(|(idx, col)| ColumnInfo {
                                                    name: col.name().into(),
                                                    nullable: true,
                                                    position: idx + 1,
                                                })
                                                .collect(),
                                        ),
                                        _ => {
                                            return Err(DelightQLError::TableNotFoundError {
                                                table_name: table_name.to_string(),
                                                context:
                                                    "CTE schema not resolved for positional pattern"
                                                        .to_string(),
                                            });
                                        }
                                    }
                                } else {
                                    schema.get_table_columns(None, table_name)
                                };

                                // Track connection_id for namespace-qualified tables
                                // (the positional-pattern shortcut bypasses resolve_ground,
                                // so we must track manually to catch cross-connection joins)
                                if !identifier.namespace_path.is_empty() {
                                    if let Ok(Some((_, connection_id, _))) =
                                        self.registry.database.lookup_table_with_namespace(
                                            &identifier.namespace_path,
                                            table_name,
                                        )
                                    {
                                        self.registry.track_connection_id(connection_id);
                                    }
                                }

                                if let Some(table_columns) = maybe_table_columns {
                                    // CTE or database table — use existing mini-pipeline

                                    // VALIDATE: Positional pattern length must match table columns
                                    if patterns.len() != table_columns.len() {
                                        return Err(DelightQLError::validation_error(
                                        format!(
                                            "Positional pattern incomplete - table '{}' has {} columns but pattern specifies {} elements",
                                            table_name, table_columns.len(), patterns.len()
                                        ),
                                        "Pattern references unknown table".to_string()
                                    ));
                                    }

                                    // Convert to ColumnMetadata for pattern resolver.
                                    // Use alias as fq_table.name when present — this is the
                                    // SQL-visible name, so qualified refs like `t.val` match.
                                    let visible_name = alias.as_deref().unwrap_or(table_name);
                                    let table_schema: Vec<ast_resolved::ColumnMetadata> = table_columns
                                        .iter()
                                        .enumerate()
                                        .map(|(idx, col)| {
                                            ast_resolved::ColumnMetadata::new(
                                                ast_resolved::ColumnProvenance::from_column(
                                                    col.name.clone(),
                                                ),
                                                ast_resolved::FqTable {
                                                    parents_path: NamespacePath::empty(),
                                                    name: ast_resolved::TableName::Named(
                                                        visible_name.into(),
                                                    ),
                                                    backend_schema:
                                                        ast_resolved::PhaseBox::from_optional_schema(
                                                            None,
                                                        ),
                                                },
                                                Some(idx + 1),
                                            )
                                        })
                                        .collect();

                                    // Create join context with left columns
                                    let join_ctx = super::JoinContext {
                                        left_columns: left_columns.clone(),
                                    };

                                    // Use the SAME pattern resolver!
                                    let pattern_resolver = super::PatternResolver::new();
                                    let pattern_result = pattern_resolver.resolve_pattern(
                                        &ast_unresolved::DomainSpec::Positional(patterns.clone()),
                                        &table_schema,
                                        table_name,
                                        Some(&join_ctx),
                                    )?;

                                    // Build the resolved relation from pattern result
                                    // Create positional domain spec with resolved columns as Lvar expressions
                                    let resolved_exprs: Vec<ast_resolved::DomainExpression> =
                                        pattern_result
                                            .output_columns
                                            .iter()
                                            .map(|col| ast_resolved::DomainExpression::Lvar {
                                                name: col.name().into(),
                                                qualifier: Some(table_name.clone()),
                                                namespace_path: NamespacePath::empty(),
                                                alias: None,
                                                provenance: ast_resolved::PhaseBox::phantom(),
                                            })
                                            .collect();

                                    let resolved_relation = ast_resolved::Relation::Ground {
                                        identifier: ast_resolved::QualifiedName {
                                            namespace_path: identifier.namespace_path.clone(),
                                            name: table_name.clone(),
                                            grounding: None,
                                        },
                                        canonical_name: ast_resolved::PhaseBox::new(None),
                                        domain_spec: ast_resolved::DomainSpec::Positional(
                                            resolved_exprs,
                                        ),
                                        alias: alias.clone(),
                                        outer: *outer,
                                        mutation_target: false,
                                        passthrough: false,
                                        cpr_schema: ast_resolved::PhaseBox::new(
                                            ast_resolved::CprSchema::Resolved(
                                                pattern_result.output_columns.clone(),
                                            ),
                                        ),
                                        hygienic_injections: Vec::new(),
                                    };

                                    let resolved_expr =
                                        ast_resolved::RelationalExpression::Relation(
                                            resolved_relation,
                                        );

                                    // Get bubbled state
                                    let bubbled = BubbledState::resolved(
                                        pattern_result.output_columns.clone(),
                                    );

                                    // Generate USING condition if there are unification columns
                                    let join_cond =
                                        if let Some(using_cols) = pattern_result.using_columns {
                                            if !using_cols.is_empty() {
                                                Some(super::create_using_condition(using_cols)?)
                                            } else {
                                                None
                                            }
                                        } else {
                                            None
                                        };

                                    // Return WHERE constraints to be handled at join level
                                    (
                                        resolved_expr,
                                        bubbled,
                                        join_cond,
                                        pattern_result.where_constraints,
                                    )
                                } else {
                                    // Not CTE or database — likely a consulted entity.
                                    // Route through the full resolver which handles consulted
                                    // entities (views, facts) and applies positional patterns.
                                    let right_expr =
                                        ast_unresolved::RelationalExpression::Relation(rel.clone());
                                    let (resolved, bubbled) = self.resolve_child(
                                        right_expr,
                                        Some(right_context.clone()),
                                        grounding.cloned(),
                                    )?;

                                    // Derive join conditions: check which lvar names in the
                                    // positional pattern match left-side column names.
                                    let mut using_cols: Vec<String> = Vec::new();
                                    for pattern in patterns {
                                        if let ast_unresolved::DomainExpression::Lvar {
                                            name, ..
                                        } = pattern
                                        {
                                            let lvar_name = name.as_str();
                                            let matches_left = left_columns
                                                .iter()
                                                .any(|col| col.name() == lvar_name);
                                            if matches_left
                                                && !using_cols.iter().any(|c| c == lvar_name)
                                            {
                                                using_cols.push(lvar_name.to_string());
                                            }
                                        }
                                    }
                                    let join_cond = if using_cols.is_empty() {
                                        None
                                    } else {
                                        Some(super::create_using_condition(using_cols)?)
                                    };

                                    (resolved, bubbled, join_cond, vec![])
                                }
                            }
                            ast_unresolved::Relation::Anonymous { column_headers, .. } => {
                                // Handle anonymous table unification
                                let (resolved, bubbled) = self.resolve_child(
                                    *right.clone(),
                                    Some(right_context.clone()),
                                    grounding.cloned(),
                                )?;

                                // Extract right-side columns from resolved anonymous table
                                let right_cpr_schema =
                                    super::helpers::extraction::extract_cpr_schema(&resolved)?;
                                let right_columns = match right_cpr_schema {
                                    ast_resolved::CprSchema::Resolved(cols) => cols,
                                    other => panic!("catch-all hit in mod.rs resolve_relational_expression (anonymous table right_columns): {:?}", other),
                                };

                                // Check for unification opportunities based on column names
                                let anon_join_condition = if let Some(headers) = column_headers {
                                    super::detect_anonymous_table_unification(
                                        headers,
                                        &left_columns,
                                        &right_columns,
                                    )?
                                } else {
                                    None
                                };

                                (resolved, bubbled, anon_join_condition, vec![])
                            }
                            ast_unresolved::Relation::Ground {
                                domain_spec:
                                    ast_unresolved::DomainSpec::GlobWithUsing(ref using_cols),
                                ..
                            } => {
                                // GlobWithUsing on consulted views (or any non-positional entity):
                                // resolve the entity, then create USING join condition from the
                                // specified columns.
                                let using_cols = using_cols.clone();
                                let (resolved, bubbled) = self.resolve_child(
                                    *right,
                                    Some(right_context.clone()),
                                    grounding.cloned(),
                                )?;
                                let join_cond = if !using_cols.is_empty() {
                                    Some(super::join_resolver::create_using_condition(using_cols)?)
                                } else {
                                    None
                                };
                                (resolved, bubbled, join_cond, vec![])
                            }
                            ast_unresolved::Relation::Ground {
                                domain_spec: ast_unresolved::DomainSpec::GlobWithUsingAll,
                                ..
                            } => {
                                // GlobWithUsingAll: resolve the right side, then compute
                                // shared columns between left and right as USING columns.
                                let (resolved, bubbled) = self.resolve_child(
                                    *right,
                                    Some(right_context.clone()),
                                    grounding.cloned(),
                                )?;
                                let right_cols = &bubbled.i_provide;
                                let shared: Vec<String> = right_cols
                                    .iter()
                                    .filter(|rc| {
                                        left_columns
                                            .iter()
                                            .any(|lc| super::col_name_eq(lc.name(), rc.name()))
                                    })
                                    .map(|rc| rc.name().to_string())
                                    .collect();
                                if shared.is_empty() {
                                    return Err(DelightQLError::validation_error_categorized(
                                        "using/all/no-shared-columns",
                                        format!(
                                            "No shared columns between left side and right side for .* (USING all)"
                                        ),
                                        ".* requires at least one column name in common",
                                    ));
                                }
                                let join_cond =
                                    super::join_resolver::create_using_condition(shared)?;
                                (resolved, bubbled, Some(join_cond), vec![])
                            }
                            _ => {
                                let (resolved, bubbled) = self.resolve_child(
                                    *right,
                                    Some(right_context.clone()),
                                    grounding.cloned(),
                                )?;
                                (resolved, bubbled, None, vec![])
                            }
                        }
                    } else {
                        let (resolved, bubbled) = self.resolve_child(
                            *right,
                            Some(right_context.clone()),
                            grounding.cloned(),
                        )?;
                        (resolved, bubbled, None, vec![])
                    };

                // Inverted CTE: if the right side absorbed the left, skip join assembly.
                // The right side's ConsultedView already contains the left as an internal CTE.
                if self.ho_join_input_absorbed {
                    self.ho_join_input_absorbed = false;
                    self.pending_ho_join_input = None;
                    return Ok((resolved_right, right_bubbled));
                }
                // Clean up pending state if not absorbed
                if right_is_tvf {
                    self.pending_ho_join_input = None;
                    self.ho_join_input_absorbed = false;
                }

                // Join conditions need to be preserved and bubbled
                let mut join_bubbled = BubbledState::resolved(vec![]);
                let resolved_condition = if let Some(cond) = join_condition {
                    match cond {
                        ast_unresolved::BooleanExpression::Using { columns } => {
                            // USING is structural, not a predicate — pass through directly
                            Some(ast_resolved::BooleanExpression::Using { columns })
                        }
                        _ => {
                            // For now, keep the condition as None but bubble the needs
                            // The condition will be resolved later when filters are processed
                            let schema = self.registry.database.schema();
                            let cte_context = &mut self.registry.query_local.ctes;
                            let (_unresolved_cond, cond_bubbled) =
                                super::bubble_predicate_expression(
                                    cond,
                                    schema,
                                    cte_context,
                                    Some(&left_columns),
                                )?;
                            join_bubbled = cond_bubbled;
                            None // Will be attached later via filter-to-join transformation
                        }
                    }
                } else {
                    positional_join_condition
                };

                // Handle USING deduplication if present
                let using_columns =
                    super::extract_inline_using_columns(&resolved_right).or_else(|| {
                        // For positional patterns, extract USING columns from the join condition
                        if let Some(ast_resolved::BooleanExpression::Using { columns }) =
                            &resolved_condition
                        {
                            Some(
                                columns
                                    .iter()
                                    .map(|col| match col {
                                        ast_resolved::UsingColumn::Regular(qname) => {
                                            qname.name.to_string()
                                        }
                                        ast_resolved::UsingColumn::Negated(qname) => {
                                            qname.name.to_string()
                                        }
                                    })
                                    .collect(),
                            )
                        } else {
                            None
                        }
                    });

                // Combine schemas with USING deduplication.
                // Use i_provide (which carries user aliases like "a", "s") rather than
                // extract_cpr_schema (which may have internal body names from ConsultedView).
                // This ensures the join's cpr_schema reflects the external interface.
                let combined_schema = {
                    let left_cols = &left_bubbled.i_provide;
                    let right_cols = &right_bubbled.i_provide;
                    if left_cols.is_empty() && right_cols.is_empty() {
                        ast_resolved::CprSchema::Unknown
                    } else {
                        let mut combined = left_cols.clone();
                        if let Some(using_cols) = &using_columns {
                            let using_names: std::collections::HashSet<String> =
                                using_cols.iter().cloned().collect();
                            let filtered_right: Vec<_> = right_cols
                                .iter()
                                .filter(|col| !using_names.contains(col.name()))
                                .cloned()
                                .collect();
                            combined.extend(filtered_right);
                        } else {
                            combined.extend(right_cols.clone());
                        }
                        ast_resolved::CprSchema::Resolved(combined)
                    }
                };

                // Also deduplicate in the bubbled state
                let final_right_bubbled = if let Some(using_cols) = using_columns {
                    let using_names: std::collections::HashSet<String> =
                        using_cols.into_iter().collect();
                    let filtered_i_provide: Vec<_> = right_bubbled
                        .i_provide
                        .into_iter()
                        .filter(|col| !using_names.contains(col.name()))
                        .collect();
                    BubbledState {
                        i_provide: filtered_i_provide,
                        i_need: right_bubbled.i_need,
                    }
                } else {
                    right_bubbled
                };

                // Create the join
                let mut result_expr = ast_resolved::RelationalExpression::Join {
                    left: Box::new(resolved_left),
                    right: Box::new(resolved_right),
                    join_condition: resolved_condition,
                    join_type,
                    cpr_schema: ast_resolved::PhaseBox::new(combined_schema.clone()),
                };

                // Apply WHERE constraints from positional patterns if any
                if !where_constraints.is_empty() {
                    // Combine multiple constraints with AND
                    let combined_constraint = if where_constraints.len() == 1 {
                        where_constraints
                            .into_iter()
                            .next()
                            .expect("Checked len==1 above")
                    } else {
                        where_constraints
                            .into_iter()
                            .reduce(|left, right| ast_resolved::BooleanExpression::And {
                                left: Box::new(left),
                                right: Box::new(right),
                            })
                            .expect("Checked non-empty above, reduce must succeed")
                    };

                    // Wrap the join in a Filter
                    // Note: These are combined constraints from multiple tables in a join
                    result_expr = ast_resolved::RelationalExpression::Filter {
                        source: Box::new(result_expr),
                        condition: ast_resolved::SigmaCondition::Predicate(combined_constraint),
                        origin: ast_resolved::FilterOrigin::PositionalLiteral {
                            source_table: "__join__".to_string(), // Special marker for combined join constraints
                        },
                        cpr_schema: ast_resolved::PhaseBox::new(combined_schema),
                    };
                }

                Ok((
                    result_expr,
                    BubbledState::combine(
                        BubbledState::combine(left_bubbled, final_right_bubbled),
                        join_bubbled,
                    ),
                ))
            }

            // Handle Pipe through registry — LINEARIZED
            // Collects the pipe chain into a flat list, resolves the base once,
            // then iterates operators bottom-up. Eliminates pipe-spine recursion.
            ast_unresolved::RelationalExpression::Pipe(boxed_pipe_expr) => {
                let pipe_expr = (*boxed_pipe_expr).into_inner();

                // Early intercept: piped HO view application desugars BEFORE source resolution
                if let ast_unresolved::UnaryRelationalOperator::HoViewApplication {
                    ref function,
                    ref first_parens_spec,
                    ref arguments,
                    ref namespace,
                    ..
                } = pipe_expr.operator
                {
                    // Look up the HO view entity.
                    // When namespace is explicit (e.g., std::json.tg_keys), use
                    // lookup_entity with the FQ namespace — same as the non-piped
                    // TVF path. Otherwise, search enlisted namespaces by bare name.
                    let entity = if let Some(ref ns) = namespace {
                        let fq = super::grounding::namespace_path_to_fq(ns);
                        self.registry
                            .consult
                            .lookup_entity(function, &fq)
                            .filter(|e| {
                                e.entity_type
                                    == crate::enums::EntityType::DqlHoTemporaryViewExpression
                                        .as_i32()
                            })
                            .ok_or_else(|| {
                                crate::error::DelightQLError::validation_error(
                                    format!(
                                        "Unknown piped HO view '{}.{}'. Ensure the namespace is consulted.",
                                        fq, function
                                    ),
                                    "Piped HO view not found",
                                )
                            })?
                    } else {
                        self.registry
                            .consult
                            .lookup_enlisted_ho_view(function)?
                            .ok_or_else(|| {
                                crate::error::DelightQLError::validation_error(
                                    format!(
                                        "Unknown piped HO view '{}'. Ensure the namespace is consulted and engaged.",
                                        function
                                    ),
                                    "Piped HO view not found",
                                )
                            })?
                    };

                    // Build first_parens_spec if not already set
                    let spec = first_parens_spec
                        .clone()
                        .unwrap_or(ast_unresolved::DomainSpec::Glob);

                    let groups_ref: Option<&[_]> = if arguments.is_empty() {
                        None
                    } else {
                        Some(arguments.as_slice())
                    };
                    let (table_bindings, scalar_spec, _pipe_idx) =
                        super::grounding::split_ho_first_parens(
                            &spec,
                            &entity,
                            Some(&pipe_expr.source),
                            groups_ref,
                        )?;

                    // Build grounding context
                    let ns_parts: Vec<String> =
                        entity.namespace.split("::").map(String::from).collect();
                    let entity_ns =
                        ast_unresolved::NamespacePath::from_parts(ns_parts).map_err(|e| {
                            crate::error::DelightQLError::database_error(
                                format!(
                                    "Invalid namespace '{}' for HO view '{}': {:?}",
                                    entity.namespace, function, e
                                ),
                                format!("{:?}", e),
                            )
                        })?;
                    let ho_grounding = ast_unresolved::GroundedPath {
                        data_ns: ast_unresolved::NamespacePath::empty(),
                        grounded_ns: vec![entity_ns],
                    };

                    // Scope ER-rule lookups to the HO-view's namespace
                    let ho_config = if !entity.namespace.is_empty() && entity.namespace != "main" {
                        ResolutionConfig {
                            resolution_namespace: Some(entity.namespace.clone()),
                            ..self.config.clone()
                        }
                    } else {
                        self.config.clone()
                    };

                    let (expr, bubbled, _absorbed) = super::relation_resolver::expand_ho_view(
                        function,
                        &entity,
                        &scalar_spec,
                        table_bindings,
                        Some(pipe_expr.source),
                        None, // no join_input for pipes
                        None,
                        &ho_grounding,
                        self.registry,
                        outer_context,
                        &ho_config,
                        None,
                    )?;
                    return Ok((expr, bubbled));
                }

                // Collect the pipe chain into a flat list, stopping at HoViewApplication
                // (which needs unresolved source for expansion and is handled recursively).
                let mut segments: Vec<ast_unresolved::UnaryRelationalOperator> = Vec::new();
                let mut current = ast_unresolved::RelationalExpression::Pipe(Box::new(
                    stacksafe::StackSafe::new(pipe_expr),
                ));
                while let ast_unresolved::RelationalExpression::Pipe(pipe) = current {
                    let pipe = (*pipe).into_inner();
                    if matches!(
                        &pipe.operator,
                        ast_unresolved::UnaryRelationalOperator::HoViewApplication { .. }
                    ) {
                        // Leave this Pipe (and everything below) as the base
                        // for recursive resolution via resolve_relational_expression_with_registry
                        current = ast_unresolved::RelationalExpression::Pipe(Box::new(
                            stacksafe::StackSafe::new(pipe),
                        ));
                        break;
                    }
                    segments.push(pipe.operator);
                    current = pipe.source;
                }
                segments.reverse(); // source-code order: innermost first
                let base = current;

                let mut pivot_in_values;
                let source_grounding;
                let mutation_targets;
                let dml_pipe_ops: Vec<DmlPipeKind>;
                let mut resolved_source;
                let mut source_bubbled;

                {
                    // Pre-processing extractions from the base (once, not per-pipe).
                    // These functions walk through Pipes/Filters to find data at the Ground level.
                    pivot_in_values = super::extract_in_predicate_values(&base);
                    source_grounding = super::extract_grounding_from_source(&base);
                    mutation_targets = super::find_mutation_targets(&base);

                    // Pre-compute DML pipe ops for shape validation.
                    // The DML terminal is always the last segment; classify all preceding
                    // segments in outermost-first order (reversed from source-code order).
                    dml_pipe_ops = if segments.last().map_or(false, |op| {
                        matches!(
                            op,
                            ast_unresolved::UnaryRelationalOperator::DmlTerminal { .. }
                        )
                    }) {
                        segments[..segments.len() - 1]
                            .iter()
                            .rev()
                            .map(|op| super::classify_single_dml_op(op))
                            .collect()
                    } else {
                        vec![]
                    };

                    // Resolve the base expression through registry.
                    // If base is Pipe(HoView, ...), recursion handles the expansion.
                    let (rs, sb) = self.resolve_relational(base)?;
                    resolved_source = rs;
                    source_bubbled = sb;

                    // Extract IN values from the resolved base (catches InRelational
                    // with anonymous fact tables, e.g., from HO scalar-lifted params).
                    let resolved_pivot_values =
                        super::extract_in_predicate_values_from_resolved(&resolved_source);
                    for (k, v) in resolved_pivot_values {
                        pivot_in_values.entry(k).or_insert(v);
                    }
                }

                // Iterate pipe segments bottom-up (innermost operator first)
                for operator in segments {
                    // Check for unresolved columns before pipe (scope barrier)
                    if !source_bubbled.i_need.is_empty() {
                        let first_unresolved = &source_bubbled.i_need[0];
                        let qual_str = match first_unresolved {
                            ColumnReference::Named {
                                name, qualifier, ..
                            } => qualifier
                                .as_ref()
                                .map(|q| format!("{}.{}", q, name))
                                .unwrap_or_else(|| name.clone()),
                            ColumnReference::Ordinal {
                                position, reverse, ..
                            } => {
                                if *reverse {
                                    format!("|-{}|", position)
                                } else {
                                    format!("|{}|", position)
                                }
                            }
                        };

                        return Err(DelightQLError::ColumnNotFoundError {
                            column: qual_str,
                            context: "Column reference before pipe operator cannot be resolved (scope barrier)".to_string(),
                        });
                    }

                    // Get available columns from source
                    let mut source_has_unknown_schema = false;
                    let available_columns = if source_bubbled.i_provide.is_empty() {
                        let source_schema = super::extract_cpr_schema(&resolved_source)?;
                        if std::env::var("DQL_DEBUG").is_ok() {
                            eprintln!("PIPE: Extracted schema from source");
                        }
                        match &source_schema {
                            ast_resolved::CprSchema::Resolved(cols) => {
                                if std::env::var("DQL_DEBUG").is_ok() {
                                    eprintln!("PIPE: Source has {} columns:", cols.len());
                                    for col in cols {
                                        eprintln!(
                                            "  PIPE: - {}",
                                            col.info.original_name().unwrap_or("<no name>")
                                        );
                                    }
                                }
                                cols.clone()
                            }
                            ast_resolved::CprSchema::Failed { .. } => {
                                return Err(DelightQLError::ParseError {
                                    message: "Cannot pipe from a relation with unresolved columns"
                                        .to_string(),
                                    source: None,
                                    subcategory: None,
                                });
                            }
                            ast_resolved::CprSchema::Unresolved(_) => {
                                return Err(DelightQLError::ParseError {
                                    message: "Cannot pipe from an unresolved relation".to_string(),
                                    source: None,
                                    subcategory: None,
                                });
                            }
                            ast_resolved::CprSchema::Unknown => {
                                source_has_unknown_schema = true;
                                vec![]
                            }
                        }
                    } else {
                        source_bubbled.i_provide.clone()
                    };

                    // USING→correlation intercept
                    if let ast_unresolved::UnaryRelationalOperator::Using { ref columns } = operator
                    {
                        if let Some(outer) = outer_context {
                            let inner_table_name =
                                super::extract_base_ground_name(&resolved_source);
                            let inner_qn = ast_resolved::QualifiedName {
                                namespace_path: ast_resolved::NamespacePath::empty(),
                                name: inner_table_name.unwrap_or_else(|| "unknown".into()),
                                grounding: None,
                            };

                            let correlation_filters =
                                super::resolving::build_using_correlation_filters(
                                    columns, &inner_qn, outer,
                                );

                            resolved_source =
                                super::insert_filters_at_base(resolved_source, correlation_filters);
                            continue;
                        }
                    }

                    // Validate !! mutation target markers for DML terminals
                    if let ast_unresolved::UnaryRelationalOperator::DmlTerminal {
                        ref kind,
                        ref target,
                        ..
                    } = operator
                    {
                        if mutation_targets.len() > 1 {
                            return Err(DelightQLError::validation_error_categorized(
                                "dml/marker/multiple",
                                format!("DML source has !! on multiple relations: {}", mutation_targets.join(", ")),
                                "Only one relation can be marked with !! — the mutation target must be unambiguous",
                            ));
                        }

                        match kind {
                            DmlKind::Insert => {
                                if !mutation_targets.is_empty() {
                                    return Err(DelightQLError::validation_error_categorized(
                                        "dml/marker/forbidden",
                                        format!("insert! source must not have !! marker (found on: {})", mutation_targets.join(", ")),
                                        "Remove !! from the source relation — insert reads from source, it does not mutate it".to_string(),
                                    ));
                                }
                            }
                            DmlKind::Update | DmlKind::Delete | DmlKind::Keep => {
                                let kind_name = match kind {
                                    DmlKind::Update => "update!",
                                    DmlKind::Delete => "delete!",
                                    DmlKind::Keep => "keep!",
                                    _ => unreachable!(),
                                };
                                if mutation_targets.is_empty() {
                                    return Err(DelightQLError::validation_error_categorized(
                                        "dml/marker/missing",
                                        format!("{} requires !! on the source relation that will be mutated", kind_name),
                                        format!("Mark the source with !!: {}!!(*)  — this makes the mutation target explicit", target),
                                    ));
                                }
                                if !mutation_targets.iter().any(|t| t == target) {
                                    return Err(DelightQLError::validation_error_categorized(
                                        "dml/marker/mismatch",
                                        format!("!! source table '{}' does not match {} target '{}'", mutation_targets[0], kind_name, target),
                                        format!("The !! marker must be on the same table as the DML target: {}!!(*)  |> {}({}(*))", target, kind_name, target),
                                    ));
                                }
                            }
                        }

                        // Shape validation using pre-computed dml_pipe_ops
                        let pipe_ops = &dml_pipe_ops;

                        match kind {
                            DmlKind::Update => {
                                let has_transform = pipe_ops
                                    .iter()
                                    .any(|op| matches!(op, DmlPipeKind::Transform));
                                if !has_transform {
                                    let has_non_filter_ops = pipe_ops.iter().any(|op| {
                                        matches!(
                                            op,
                                            DmlPipeKind::ProjectOut
                                                | DmlPipeKind::RenameCover
                                                | DmlPipeKind::TupleOrdering
                                                | DmlPipeKind::General
                                                | DmlPipeKind::Modulo
                                                | DmlPipeKind::AggregatePipe
                                        )
                                    });
                                    if has_non_filter_ops {
                                        return Err(DelightQLError::validation_error_categorized(
                                            "dml/shape/update_no_transform",
                                            "update! requires a Transform ($$) to specify column assignments — embed (+), project-out (-), rename (*), ordering (#), and projection do not produce SET clauses",
                                            "Use $$(new_value as column_name) before update! to specify what to change",
                                        ));
                                    }
                                } else {
                                    let has_aggregate = pipe_ops.iter().any(|op| {
                                        matches!(
                                            op,
                                            DmlPipeKind::Modulo | DmlPipeKind::AggregatePipe
                                        )
                                    });
                                    if has_aggregate {
                                        return Err(DelightQLError::validation_error_categorized(
                                            "dml/source/aggregate",
                                            "Cannot aggregate/group data before update! — aggregation changes the row identity, making it impossible to map results back to source rows",
                                            "Remove the aggregate/group-by pipe before the DML operation",
                                        ));
                                    }
                                    let transform_count = pipe_ops
                                        .iter()
                                        .filter(|op| matches!(op, DmlPipeKind::Transform))
                                        .count();
                                    if transform_count > 1 {
                                        return Err(DelightQLError::validation_error_categorized(
                                            "dml/shape/update_no_transform",
                                            "update! requires exactly one Transform ($$) — multiple covers produce ambiguous SET clauses",
                                            "Combine the transforms into a single $$(expr1 as col1, expr2 as col2) before update!",
                                        ));
                                    }
                                    let has_ordering = pipe_ops
                                        .iter()
                                        .any(|op| matches!(op, DmlPipeKind::TupleOrdering));
                                    if has_ordering {
                                        return Err(DelightQLError::validation_error_categorized(
                                            "dml/shape/update_no_transform",
                                            "Ordering (#) before update! is meaningless — UPDATE does not preserve row order",
                                            "Remove the ordering pipe from the DML pipeline",
                                        ));
                                    }
                                }
                            }
                            DmlKind::Delete | DmlKind::Keep => {
                                let kind_name = if matches!(kind, DmlKind::Delete) {
                                    "delete!"
                                } else {
                                    "keep!"
                                };
                                let has_transform = pipe_ops
                                    .iter()
                                    .any(|op| matches!(op, DmlPipeKind::Transform));
                                if has_transform {
                                    let sub = if matches!(kind, DmlKind::Delete) {
                                        "dml/shape/delete_with_cover"
                                    } else {
                                        "dml/shape/keep_with_cover"
                                    };
                                    return Err(DelightQLError::validation_error_categorized(
                                        sub,
                                        format!("{} discards column data — a Transform ($$) before it is wasted", kind_name),
                                        format!("Remove the Transform before {} — only filters affect which rows are deleted/kept", kind_name),
                                    ));
                                }
                                let has_shape_ops = pipe_ops.iter().any(|op| {
                                    matches!(
                                        op,
                                        DmlPipeKind::ProjectOut
                                            | DmlPipeKind::RenameCover
                                            | DmlPipeKind::General
                                    )
                                });
                                if has_shape_ops {
                                    let sub = if matches!(kind, DmlKind::Delete) {
                                        "dml/shape/delete_with_cover"
                                    } else {
                                        "dml/shape/keep_with_cover"
                                    };
                                    return Err(DelightQLError::validation_error_categorized(
                                        sub,
                                        format!("{} discards column data — shape-changing operators (embed, project-out, rename, projection) before it are wasted", kind_name),
                                        format!("Remove shape-changing pipes before {} — only filters affect which rows are deleted/kept", kind_name),
                                    ));
                                }
                                let has_aggregate = pipe_ops.iter().any(|op| {
                                    matches!(op, DmlPipeKind::Modulo | DmlPipeKind::AggregatePipe)
                                });
                                if has_aggregate {
                                    return Err(DelightQLError::validation_error_categorized(
                                        "dml/source/aggregate",
                                        format!("Cannot aggregate/group data before {} — aggregation changes the row identity", kind_name),
                                        "Remove the aggregate/group-by pipe before the DML operation",
                                    ));
                                }
                            }
                            DmlKind::Insert => {
                                // Insert is more permissive — projections, transforms, etc. are valid
                                // for shaping the data before insertion. Aggregates are suspicious but
                                // not necessarily wrong (e.g., insert aggregated results into a summary table).
                            }
                        }
                    }

                    // Bubble the operator to collect column needs
                    let schema = self.registry.database.schema();
                    let cte_context = &mut self.registry.query_local.ctes;
                    let (unresolved_operator, operator_bubbled) =
                        super::bubbling::bubble_unary_operator(operator, schema, cte_context)?;

                    // Validate that all operator needs can be satisfied
                    if !operator_bubbled.i_need.is_empty() && !source_has_unknown_schema {
                        super::validate_and_get_resolved(
                            operator_bubbled.i_need.clone(),
                            &available_columns,
                            "in pipe operator",
                        )?;
                    }

                    // Inline consulted functions before resolution
                    let unresolved_operator = if let Some(grounding) = grounding {
                        super::grounding::inline_consulted_functions_in_operator(
                            unresolved_operator,
                            grounding,
                            &self.registry.consult,
                        )?
                    } else {
                        let source_data_ns = source_grounding.as_ref().map(|g| &g.data_ns);
                        super::grounding::inline_consulted_functions_in_operator_borrowed(
                            unresolved_operator,
                            &self.registry.consult,
                            source_data_ns,
                        )?
                    };

                    // Resolve the operator at the pipe boundary with the source schema
                    self.available = available_columns.clone();
                    self.pivot_in_values = pivot_in_values.clone();
                    let resolved_operator = self.transform_operator(unresolved_operator)?;
                    let mut output_columns = self
                        .last_operator_output
                        .take()
                        .expect("transform_operator must populate last_operator_output");

                    // After a pipe, columns become Fresh (scope barrier).
                    // Exception: value-level covers ($$ and $) preserve table provenance.
                    let preserves_scope = matches!(
                        &resolved_operator,
                        ast_resolved::UnaryRelationalOperator::Transform { .. }
                            | ast_resolved::UnaryRelationalOperator::InteriorDrillDown { .. }
                    );

                    for (idx, col) in output_columns.iter_mut().enumerate() {
                        let previous_table = col.fq_table.name.clone();

                        if !preserves_scope {
                            col.fq_table.name = ast_resolved::TableName::Fresh;
                        }
                        col.table_position = Some(idx + 1);

                        col.info = col
                            .info
                            .clone()
                            .with_identity(ast_resolved::ColumnIdentity {
                                name: col.info.name().unwrap_or("<unnamed>").into(),
                                context: ast_resolved::IdentityContext::PipeBarrier {
                                    previous_table,
                                    fresh_scope: idx + 1,
                                },
                                phase: ast_resolved::TransformationPhase::Resolved,
                                table_qualifier: if preserves_scope {
                                    col.fq_table.name.clone()
                                } else {
                                    ast_resolved::TableName::Fresh
                                },
                            });

                        // Seal column identity: after a pipe barrier, the column's
                        // public name is its effective name. original_name() == name().
                        col.info = col.info.clone().promote_at_barrier();
                    }

                    // Construct resolved pipe, accumulate as new source
                    let pipe = ast_resolved::PipeExpression {
                        source: resolved_source,
                        operator: resolved_operator,
                        cpr_schema: ast_resolved::PhaseBox::new(ast_resolved::CprSchema::Resolved(
                            output_columns.clone(),
                        )),
                    };
                    resolved_source = ast_resolved::RelationalExpression::Pipe(Box::new(
                        stacksafe::StackSafe::new(pipe),
                    ));
                    source_bubbled = BubbledState::resolved(output_columns);
                }

                Ok((resolved_source, source_bubbled))
            }

            // Handle SetOperation through registry
            ast_unresolved::RelationalExpression::SetOperation {
                operator,
                operands,
                correlation: unresolved_corr,
                cpr_schema: _,
            } => {
                // Resolve each operand
                let mut resolved_operands = Vec::new();
                let mut bubbled_states = Vec::new();

                for operand in operands {
                    let (resolved, bubbled) = self.resolve_relational(operand)?;
                    resolved_operands.push(resolved);
                    bubbled_states.push(bubbled);
                }

                // Ensure all operands have compatible schemas
                if resolved_operands.is_empty() {
                    return Err(DelightQLError::ParseError {
                        message: "SetOperation requires at least one operand".to_string(),
                        source: None,
                        subcategory: None,
                    });
                }

                // Collect all schemas
                let mut schemas = Vec::new();
                for operand in &resolved_operands {
                    schemas.push(super::extract_cpr_schema(operand)?);
                }

                // Validate and build final schema based on operator
                let final_schema = match operator {
                    ast_unresolved::SetOperator::UnionAllPositional => {
                        // Positional union - require same column count
                        for i in 1..schemas.len() {
                            super::validate_set_operation_schemas(
                                &operator,
                                &schemas[0],
                                &schemas[i],
                            )?;
                        }
                        schemas[0].clone()
                    }
                    ast_unresolved::SetOperator::SmartUnionAll => {
                        // Smart union - all must have same column names (order can differ)
                        for i in 1..schemas.len() {
                            super::validate_set_operation_schemas(
                                &operator,
                                &schemas[0],
                                &schemas[i],
                            )?;
                        }
                        schemas[0].clone()
                    }
                    ast_unresolved::SetOperator::UnionCorresponding => {
                        // Build unified schema for CORRESPONDING
                        super::build_corresponding_schema(&schemas)?
                    }
                    ast_unresolved::SetOperator::MinusCorresponding => {
                        // Minus uses left operand's schema (rows in left not in right)
                        // Require same column names by name match
                        for i in 1..schemas.len() {
                            super::validate_set_operation_schemas(
                                &operator,
                                &schemas[0],
                                &schemas[i],
                            )?;
                        }
                        schemas[0].clone()
                    }
                };

                // Pass through correlation (resolver doesn't set it, refiner will)
                let resolved_correlation = unresolved_corr.into();

                Ok((
                    ast_resolved::RelationalExpression::SetOperation {
                        operator,
                        operands: resolved_operands,
                        correlation: resolved_correlation,
                        cpr_schema: ast_resolved::PhaseBox::new(final_schema),
                    },
                    BubbledState::resolved(vec![]), // SetOperations don't bubble anything
                ))
            }

            ast_unresolved::RelationalExpression::ErJoinChain { relations } => {
                let context = self.config.er_context.as_ref().ok_or_else(|| {
                    DelightQLError::validation_error(
                        "ER-join operator & requires an 'under context:' directive",
                        "Missing ER-context",
                    )
                })?;

                Ok(super::expand_er_join_chain(
                    relations,
                    context,
                    self.registry,
                    outer_context,
                    &self.config,
                    grounding,
                    None,
                    None,
                )?)
            }

            ast_unresolved::RelationalExpression::ErTransitiveJoin { left, right } => {
                let context = self.config.er_context.as_ref().ok_or_else(|| {
                    DelightQLError::validation_error(
                        "ER-transitive-join operator && requires an 'under context:' directive",
                        "Missing ER-context",
                    )
                })?;

                Ok(super::expand_er_transitive_join(
                    *left,
                    *right,
                    context,
                    self.registry,
                    outer_context,
                    &self.config,
                    grounding,
                )?)
            }
        }
    }

    /// Operator resolution — delegates to `resolving::resolve_operator_via_fold`.
    pub(super) fn resolve_operator_impl(
        &mut self,
        operator: ast_unresolved::UnaryRelationalOperator,
        available: &[ast_resolved::ColumnMetadata],
        pivot_in_values: &std::collections::HashMap<String, Vec<String>>,
    ) -> Result<(
        ast_resolved::UnaryRelationalOperator,
        Vec<ast_resolved::ColumnMetadata>,
    )> {
        super::resolving::resolve_operator_via_fold(self, operator, available, pivot_in_values)
    }

    /// Relation dispatch — formerly the free function `resolve_relation_with_registry`.
    /// Matches on the Relation variant and delegates to the appropriate helper in
    /// `relation_resolver`. The helpers remain as free functions; only the
    /// dispatch is absorbed so `self.registry` / `self.config` are threaded
    /// implicitly.
    #[stacksafe::stacksafe]
    fn resolve_relation_impl(
        &mut self,
        rel: ast_unresolved::Relation,
        outer_context: Option<&[ast_resolved::ColumnMetadata]>,
        grounding: Option<&ast_unresolved::GroundedPath>,
    ) -> Result<(ast_resolved::RelationalExpression, BubbledState)> {
        match &rel {
            ast_unresolved::Relation::Ground { .. } => super::relation_resolver::resolve_ground(
                rel,
                self.registry,
                outer_context,
                &self.config,
                grounding,
            ),
            ast_unresolved::Relation::Anonymous { .. } => {
                super::relation_resolver::resolve_anonymous(rel, self, outer_context)
            }
            ast_unresolved::Relation::TVF { .. } => {
                let join_input = self.pending_ho_join_input.take();
                let (expr, bubbled, absorbed) = super::relation_resolver::resolve_tvf(
                    rel,
                    self.registry,
                    outer_context,
                    &self.config,
                    join_input,
                )?;
                self.ho_join_input_absorbed = absorbed;
                Ok((expr, bubbled))
            }
            ast_unresolved::Relation::InnerRelation { .. } => {
                super::relation_resolver::resolve_inner_relation(
                    rel,
                    self.registry,
                    outer_context,
                    &self.config,
                    grounding,
                )
            }
            ast_unresolved::Relation::ConsultedView { .. } => {
                panic!(
                    "INTERNAL ERROR: ConsultedView should not appear as input to relation resolution. \
                     ConsultedView is created by the resolver, not consumed at this point."
                )
            }
            ast_unresolved::Relation::PseudoPredicate { .. } => {
                panic!(
                    "INTERNAL ERROR: PseudoPredicate should not exist in this phase. \
                     Pseudo-predicates are executed and replaced during Phase 1.X (Effect Executor)."
                )
            }
        }
    }
}

impl<'reg, 'db> AstTransform<Unresolved, Resolved> for ResolverFold<'reg, 'db> {
    fn transform_relational(
        &mut self,
        e: RelationalExpression<Unresolved>,
    ) -> Result<RelationalExpression<Resolved>> {
        let (resolved, bubbled) = self.resolve_relational_impl(e)?;
        self.last_bubbled = Some(bubbled);
        Ok(resolved)
    }

    fn transform_domain(
        &mut self,
        expr: ast_unresolved::DomainExpression,
    ) -> Result<ast_resolved::DomainExpression> {
        use crate::pipeline::ast_transform::walk_transform_domain;
        use crate::pipeline::asts::core::ProjectionExpr;

        match expr {
            // StringTemplate at domain level → concat chain (returns DomainExpression, not Function)
            ast_unresolved::DomainExpression::Function(
                ast_unresolved::FunctionExpression::StringTemplate { parts, alias },
            ) => {
                let mut resolved_parts: Vec<ast_resolved::StringTemplatePart<Resolved>> =
                    Vec::new();
                for part in parts {
                    match part {
                        ast_unresolved::StringTemplatePart::Text(text) => {
                            resolved_parts
                                .push(ast_resolved::StringTemplatePart::<Resolved>::Text(text));
                        }
                        ast_unresolved::StringTemplatePart::Interpolation(expr) => {
                            let resolved_expr = self.transform_domain(*expr)?;
                            resolved_parts.push(
                                ast_resolved::StringTemplatePart::<Resolved>::Interpolation(
                                    Box::new(resolved_expr),
                                ),
                            );
                        }
                    }
                }
                Ok(super::string_templates::build_concat_chain(
                    resolved_parts,
                    alias,
                ))
            }

            // Simple expressions — column validation, ordinal resolution, literal conversion
            ast_unresolved::DomainExpression::Lvar { .. }
            | ast_unresolved::DomainExpression::ColumnOrdinal(_)
            | ast_unresolved::DomainExpression::Literal { .. }
            | ast_unresolved::DomainExpression::ValuePlaceholder { .. }
            | ast_unresolved::DomainExpression::Substitution(_)
            | ast_unresolved::DomainExpression::NonUnifiyingUnderscore => {
                let available = self.available.clone();
                super::resolving::domain_expressions::simple::resolve_simple_expr(
                    expr,
                    &available,
                    self.in_correlation,
                )
            }

            // JsonPathLiteral is value-like — pass through
            ast_unresolved::DomainExpression::Projection(ProjectionExpr::JsonPathLiteral {
                segments,
                root_is_array,
                alias,
            }) => Ok(ast_resolved::DomainExpression::Projection(
                ProjectionExpr::JsonPathLiteral {
                    segments,
                    root_is_array,
                    alias,
                },
            )),

            // Non-JsonPathLiteral Projection → error (only valid in projection context)
            ast_unresolved::DomainExpression::Projection(_) => {
                super::resolving::domain_expressions::simple::resolve_projection_only_expr(expr)
            }

            // ScalarSubquery → fresh resolution context with registry
            ast_unresolved::DomainExpression::ScalarSubquery {
                identifier,
                subquery,
                alias,
            } => {
                super::resolving::domain_expressions::subqueries::resolve_scalar_subquery_via_fold(
                    self,
                    identifier,
                    *subquery,
                    alias.map(|s| s.to_string()),
                )
            }

            // Everything else: walk handles structural descent
            // Function(non-StringTemplate) → transform_function
            // Predicate → transform_boolean
            // PipedExpression, Parenthesized, Tuple, PivotOf → recursive transform_domain
            other => walk_transform_domain(self, other),
        }
    }

    fn transform_boolean(
        &mut self,
        pred: ast_unresolved::BooleanExpression,
    ) -> Result<ast_resolved::BooleanExpression> {
        use crate::pipeline::ast_transform::walk_transform_boolean;

        match pred {
            // In → desugar to anonymous InnerExists
            ast_unresolved::BooleanExpression::In {
                value,
                set,
                negated,
            } => {
                let resolved_value = self.transform_domain((*value).clone())?;
                let resolved_set = set
                    .iter()
                    .map(|expr| self.transform_domain(expr.clone()))
                    .collect::<Result<Vec<_>>>()?;
                Ok(super::resolving::predicates::desugar_in_to_anonymous(
                    resolved_value,
                    resolved_set,
                    negated,
                ))
            }

            // InRelational → fresh subquery resolution via registry
            ast_unresolved::BooleanExpression::InRelational {
                value,
                subquery,
                identifier,
                negated,
            } => {
                let resolved_value = self.transform_domain(*value)?;
                let available = self.available.clone();
                let config = self.config.clone();
                let (resolved_subquery, _) = super::resolve_relational_expression_with_registry(
                    *subquery,
                    self.registry,
                    Some(&available),
                    &config,
                    None,
                )?;
                Ok(ast_resolved::BooleanExpression::InRelational {
                    value: Box::new(resolved_value),
                    subquery: Box::new(resolved_subquery),
                    identifier,
                    negated,
                })
            }

            // InnerExists → fresh subquery resolution + USING correlation
            ast_unresolved::BooleanExpression::InnerExists {
                exists,
                identifier,
                subquery,
                alias,
                using_columns,
            } => {
                let available = self.available.clone();
                let config = self.config.clone();
                let (resolved_subquery, _) = super::resolve_relational_expression_with_registry(
                    *subquery,
                    self.registry,
                    Some(&available),
                    &config,
                    None,
                )?;
                let final_subquery = super::resolving::predicates::synthesize_using_correlation(
                    resolved_subquery,
                    &using_columns,
                    &identifier,
                    &available,
                );
                Ok(ast_resolved::BooleanExpression::InnerExists {
                    exists,
                    identifier,
                    subquery: Box::new(final_subquery),
                    alias,
                    using_columns,
                })
            }

            // Using → convert column types
            ast_unresolved::BooleanExpression::Using { columns } => {
                Ok(ast_resolved::BooleanExpression::Using {
                    columns: columns
                        .into_iter()
                        .map(super::helpers::converters::convert_using_column)
                        .collect(),
                })
            }

            // Sigma → resolve + unwrap Predicate variant
            ast_unresolved::BooleanExpression::Sigma { condition } => {
                let resolved_condition = self.transform_sigma(*condition)?;
                match resolved_condition {
                    ast_resolved::SigmaCondition::Predicate(inner_bool) => Ok(inner_bool),
                    other => Ok(ast_resolved::BooleanExpression::Sigma {
                        condition: Box::new(other),
                    }),
                }
            }

            // Everything else: walk handles structural descent
            // Comparison, And, Or, Not, BooleanLiteral, GlobCorrelation, OrdinalGlobCorrelation
            other => walk_transform_boolean(self, other),
        }
    }

    fn transform_function(
        &mut self,
        func: ast_unresolved::FunctionExpression,
    ) -> Result<ast_resolved::FunctionExpression> {
        use crate::pipeline::ast_transform::walk_transform_function;
        use crate::pipeline::asts::core::SubstitutionExpr;

        match func {
            // StringTemplate in function context → Lambda
            ast_unresolved::FunctionExpression::StringTemplate { parts, alias } => {
                let concat_expr =
                    super::resolving::helpers::build_concat_chain_with_placeholders(parts)?;
                Ok(ast_resolved::FunctionExpression::Lambda {
                    body: Box::new(concat_expr),
                    alias,
                })
            }

            // Regular: Glob-preserving arg resolution + CCAFE validation
            ast_unresolved::FunctionExpression::Regular {
                name,
                namespace,
                arguments,
                alias,
                conditioned_on,
            } => {
                let resolved_args =
                    super::resolving::functions::resolve_function_arguments_via_fold(
                        self, arguments,
                    )?;

                // CCAFE validation
                {
                    let available = self.available.clone();
                    let cfe_defs = &self.registry.query_local.cfes;
                    if !resolved_args.is_empty() {
                        if let ast_resolved::DomainExpression::Substitution(
                            SubstitutionExpr::ContextMarker,
                        ) = resolved_args[0]
                        {
                            if let Some(cfe_def) = cfe_defs.get(name.as_ref()) {
                                if !cfe_def.context_params.is_empty() {
                                    let available_names: std::collections::HashSet<String> =
                                        available
                                            .iter()
                                            .map(|col| col.name().to_string())
                                            .collect();
                                    let mut missing_params = Vec::new();
                                    for ctx_param in &cfe_def.context_params {
                                        if !available_names.contains(ctx_param) {
                                            missing_params.push(ctx_param.clone());
                                        }
                                    }
                                    if !missing_params.is_empty() {
                                        let context_mode = if cfe_def.allows_positional_context_call
                                        {
                                            "explicit"
                                        } else {
                                            "implicit (auto-discovered)"
                                        };
                                        return Err(DelightQLError::ParseError {
                                            message: format!(
                                                "CFE '{}' requires context columns that don't exist in current scope.\n\
                                                 \n\
                                                 Missing columns: {}\n\
                                                 Available columns: {}\n\
                                                 \n\
                                                 Context mode: {}\n\
                                                 Context parameters: {}",
                                                name,
                                                missing_params.join(", "),
                                                available.iter().map(|c| c.name()).collect::<Vec<_>>().join(", "),
                                                context_mode,
                                                cfe_def.context_params.join(", ")
                                            ),
                                            source: None,
                                            subcategory: None,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }

                let resolved_condition = if let Some(cond) = conditioned_on {
                    Some(Box::new(self.transform_boolean(*cond)?))
                } else {
                    None
                };
                Ok(ast_resolved::FunctionExpression::Regular {
                    name,
                    namespace,
                    arguments: resolved_args,
                    alias,
                    conditioned_on: resolved_condition,
                })
            }

            // Curried: Glob-preserving arg resolution
            ast_unresolved::FunctionExpression::Curried {
                name,
                namespace,
                arguments,
                conditioned_on,
            } => {
                let resolved_args =
                    super::resolving::functions::resolve_function_arguments_via_fold(
                        self, arguments,
                    )?;
                let resolved_condition = if let Some(cond) = conditioned_on {
                    Some(Box::new(self.transform_boolean(*cond)?))
                } else {
                    None
                };
                Ok(ast_resolved::FunctionExpression::Curried {
                    name,
                    namespace,
                    arguments: resolved_args,
                    conditioned_on: resolved_condition,
                })
            }

            // HigherOrder: Glob-preserving arg resolution
            ast_unresolved::FunctionExpression::HigherOrder {
                name,
                curried_arguments,
                regular_arguments,
                alias,
                conditioned_on,
            } => {
                let resolved_curried_args =
                    super::resolving::functions::resolve_function_arguments_via_fold(
                        self,
                        curried_arguments,
                    )?;
                let resolved_regular_args =
                    super::resolving::functions::resolve_function_arguments_via_fold(
                        self,
                        regular_arguments,
                    )?;
                let resolved_condition = if let Some(cond) = conditioned_on {
                    Some(Box::new(self.transform_boolean(*cond)?))
                } else {
                    None
                };
                Ok(ast_resolved::FunctionExpression::HigherOrder {
                    name,
                    curried_arguments: resolved_curried_args,
                    regular_arguments: resolved_regular_args,
                    alias,
                    conditioned_on: resolved_condition,
                })
            }

            // Window: Glob-preserving arg resolution + partition/order/frame
            ast_unresolved::FunctionExpression::Window {
                name,
                arguments,
                partition_by,
                order_by,
                frame,
                alias,
            } => {
                let resolved_arguments =
                    super::resolving::functions::resolve_function_arguments_via_fold(
                        self, arguments,
                    )?;
                let resolved_partition = partition_by
                    .into_iter()
                    .map(|expr| self.transform_domain(expr))
                    .collect::<Result<Vec<_>>>()?;
                let resolved_order = order_by
                    .into_iter()
                    .map(|spec| {
                        let resolved_col = self.transform_domain(spec.column)?;
                        Ok(ast_resolved::OrderingSpec {
                            column: resolved_col,
                            direction: spec.direction,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let resolved_frame = frame
                    .map(|f| super::resolving::functions::resolve_window_frame_via_fold(self, f))
                    .transpose()?;
                Ok(ast_resolved::FunctionExpression::Window {
                    name,
                    arguments: resolved_arguments,
                    partition_by: resolved_partition,
                    order_by: resolved_order,
                    frame: resolved_frame,
                    alias,
                })
            }

            // Bracket: ergonomic inductor expansion against available columns
            ast_unresolved::FunctionExpression::Bracket { .. } => {
                super::resolving::functions::resolve_bracket_via_fold(self, func)
            }

            // Curly: ergonomic inductor expansion + column validation
            ast_unresolved::FunctionExpression::Curly { .. } => {
                super::resolving::functions::resolve_curly_via_fold(self, func)
            }

            // Everything else: walk handles structural descent
            // Infix, Lambda, CaseExpression, Array, MetadataTreeGroup, JsonPath
            other => walk_transform_function(self, other),
        }
    }

    fn transform_sigma(
        &mut self,
        s: ast_unresolved::SigmaCondition,
    ) -> Result<ast_resolved::SigmaCondition> {
        use crate::pipeline::ast_transform::walk_transform_sigma;

        match s {
            // SigmaCall → consulted sigma lookup + expansion
            ast_unresolved::SigmaCondition::SigmaCall {
                functor,
                arguments,
                exists,
            } => {
                // Check if functor matches a consulted sigma predicate (entity_type = 9)
                if let Some(entity) = self.registry.consult.lookup_enlisted_sigma(&functor)? {
                    let expanded = super::resolving::predicates::expand_consulted_sigma(
                        &entity.definition,
                        &functor,
                        arguments,
                        exists,
                    )?;
                    let resolved = self.transform_boolean(expanded)?;
                    return Ok(ast_resolved::SigmaCondition::Predicate(resolved));
                }

                // Fall through to existing path (bin cartridge sigma predicates)
                let resolved_args = arguments
                    .into_iter()
                    .map(|arg| self.transform_domain(arg))
                    .collect::<Result<Vec<_>>>()?;
                Ok(ast_resolved::SigmaCondition::SigmaCall {
                    functor,
                    arguments: resolved_args,
                    exists,
                })
            }

            // Destructure → validate pattern + convert
            ast_unresolved::SigmaCondition::Destructure {
                json_column,
                pattern,
                mode,
                destructured_schema: _,
            } => {
                let resolved_col = self.transform_domain(*json_column)?;
                let key_mappings =
                    super::resolving::predicates::extract_key_mappings_from_unresolved_pattern(
                        &pattern,
                    )?;
                super::resolving::predicates::validate_unresolved_pattern_for_mode(
                    &pattern, &mode,
                )?;
                super::resolving::predicates::validate_no_sibling_explosions(&pattern)?;
                let pattern_func =
                    super::resolving::predicates::convert_destructure_pattern_to_resolved(
                        *pattern,
                    )?;
                Ok(ast_resolved::SigmaCondition::Destructure {
                    json_column: Box::new(resolved_col),
                    pattern: Box::new(pattern_func),
                    mode,
                    destructured_schema: ast_resolved::PhaseBox::from_mappings(key_mappings),
                })
            }

            // Everything else: walk handles (Predicate, TupleOrdinal)
            other => walk_transform_sigma(self, other),
        }
    }

    fn transform_operator(
        &mut self,
        o: ast_unresolved::UnaryRelationalOperator,
    ) -> Result<ast_resolved::UnaryRelationalOperator> {
        let available = self.available.clone();
        let pivot = self.pivot_in_values.clone();
        let (resolved, output_columns) = self.resolve_operator_impl(o, &available, &pivot)?;
        self.last_operator_output = Some(output_columns);
        Ok(resolved)
    }
}
