//! Unified SQL builder with two-phase typestate.
//!
//! The single type that flows through the transformer. Replaces the v3 trio of
//! `QueryBuildState` + `ScopeSource` + `RelationalResult`. Scope and SQL can
//! never be separated — the builder owns both, and the entire category of
//! "phantom scope name" bugs is structurally impossible.
//!
//! # Two-phase typestate
//!
//! `Builder<P>` is parameterized by a phantom type `P`:
//!
//! - **`Builder<Unprojected>`** — accumulating FROM/WHERE/JOIN. No SELECT list.
//!   Can transition to `Builder<Projected>` via `add_projection()`,
//!   `add_group_by()`, or `project_all()`.
//!
//! - **`Builder<Projected>`** — SELECT list is set. Can add ORDER BY, LIMIT,
//!   DISTINCT, push CTEs, perform set operations, or finalize via `to_sql()`.
//!   Can demote back to `Builder<Unprojected>` via `demote()` (wraps as subquery).
//!
//! `to_sql()` exists only on `Builder<Projected>`. Calling it on
//! `Builder<Unprojected>` is a compile error — no runtime check needed.
//!
//! # Traits
//!
//! - **`Qualify`** — `pub(crate)` trait for the `scalar/` lowering module.
//!   Read-only view of the builder's scope for column qualification.
//!   `r_lower_*` functions never call qualify() directly — they pass `&builder`
//!   to `s_lower_expression()`, which uses `Qualify` internally.

pub(in crate::pipeline) mod names;
pub(in crate::pipeline::transformer_v4) mod state;

use std::marker::PhantomData;

use crate::error::Result;
use crate::pipeline::asts::core::{ColumnMetadata, TableName};
use crate::pipeline::sql_ast_v3::{
    Cte, DomainExpression, JoinCondition, JoinType, OrderTerm, QueryExpression, SelectBuilder,
    SelectItem, SqlPredicate, TableExpression, TvfArgument,
};

// ---------------------------------------------------------------------------
// QualifiedColumn — what `Qualify` returns
// ---------------------------------------------------------------------------

/// A column reference with its qualification facts.
///
/// This is what `Qualify` returns: just the column name and the qualifier
/// string. The scalar module is responsible for constructing the actual
/// `DomainExpression` from these facts — the builder never touches SQL
/// expression constructors.
pub struct QualifiedColumn {
    /// The column name.
    pub name: String,
    /// The SQL qualifier (table alias or scope name).
    /// `None` means the column is unqualified (outermost scope / Top).
    pub qualifier: Option<String>,
}

pub(in crate::pipeline) use names::NameGenerator;
pub(in crate::pipeline::transformer_v4) use state::{col_name, col_qualifier, table_name_sql};
use state::{BuilderState, ScopeEntry};

// ---------------------------------------------------------------------------
// Phase marker types
// ---------------------------------------------------------------------------

/// Marker: no SELECT list has been set. Accumulating FROM/WHERE/JOIN.
pub struct Unprojected;

/// Marker: SELECT list is set. Ready for ORDER BY, LIMIT, CTE, finalization.
pub struct Projected;

// ---------------------------------------------------------------------------
// Qualify — the scalar lowering module's read-only interface
// ---------------------------------------------------------------------------

/// Read-only view of the builder's scope for column qualification.
///
/// Only the `scalar/` lowering module uses this. `r_lower_*` functions pass
/// `&builder` to `s_lower_expression()`, which calls `qualify()` internally.
/// Nobody outside `scalar/` ever qualifies directly.
///
/// Phase-independent: implemented for `Builder<P>` for all `P`.
pub(crate) trait Qualify {
    /// Qualify an unqualified column reference.
    ///
    /// Looks up `col_name` in the current scope's columns:
    /// - Exactly one match → returns that column's qualifier
    /// - Zero matches → identity stack walk, then error
    /// - Multiple matches → error (caller should use `try_qualify_with_table`)
    fn qualify(&self, col_name: &str) -> Result<QualifiedColumn>;

    /// Try to qualify a table-qualified column reference.
    ///
    /// Returns `Some` if this scope contains a column matching (table, col),
    /// `None` if it doesn't — genuinely not found in this scope.
    fn try_qualify_with_table(&self, col_name: &str, table: &str) -> Option<QualifiedColumn>;

    /// Snapshot this scope's columns for use as an outer scope.
    ///
    /// Called at scalar subquery entry points to capture the enclosing
    /// scope into `TransformCtx.outer_columns`. Default: empty (no columns
    /// to contribute — appropriate for DummyQualify, ChainedQualify, etc.).
    fn scope_columns(&self) -> Vec<ColumnMetadata> {
        vec![]
    }
}

// ---------------------------------------------------------------------------
// GroupBySpec — builder input for GROUP BY operations
// ---------------------------------------------------------------------------

/// Specification for a GROUP BY operation.
///
/// Preserves the key/aggregate distinction that the DQL AST has
/// (`%(keys ~> aggregates)`) and that raw SQL dissolves into separate
/// GROUP BY and SELECT clauses. The builder uses this to construct both
/// clauses from a single source of truth.
pub struct GroupBySpec {
    /// Group keys — appear in both GROUP BY and SELECT.
    /// These are the partitioning expressions (e.g., `department`, `country`).
    pub keys: Vec<SelectItem>,
    /// Aggregate reductions — appear only in SELECT.
    /// These are the per-group computations (e.g., `COUNT(*)`, `SUM(salary)`).
    pub aggregates: Vec<SelectItem>,
}

// ---------------------------------------------------------------------------
// CTE input/output types for lateral construction
// ---------------------------------------------------------------------------

/// Read-only view of the previous step's scope, passed to `push_cte` closures.
///
/// The closure uses `CteInput` to qualify column references against the
/// previous step's output. It never sees the CTE name (that's the builder's
/// business) — only the scope name and available columns.
pub struct CteInput {
    /// The scope name of the previous step's output.
    scope_name: TableName,
    /// The columns available from the previous step.
    columns: Vec<ColumnMetadata>,
}

impl CteInput {
    pub(super) fn new(scope_name: TableName, columns: Vec<ColumnMetadata>) -> Self {
        Self {
            scope_name,
            columns,
        }
    }

    /// Qualify a column reference against this input's scope.
    ///
    /// Delegates to the shared `qualify_in_columns` — same tiers as
    /// `Builder::qualify()`.
    pub fn qualify(&self, col_name_str: &str) -> Result<QualifiedColumn> {
        qualify_in_columns(
            col_name_str,
            &self.columns,
            state::table_name_str(&self.scope_name).unwrap_or("<fresh>"),
        )
    }

    /// Qualify a table-qualified column reference (e.g., `o.id` → `cte_2.id_2`).
    ///
    /// Walks the identity stack to find columns by their original table/name.
    pub fn qualify_with_table(&self, col_name: &str, table: &str) -> Option<QualifiedColumn> {
        try_qualify_with_table_in_columns(col_name, table, &self.columns)
    }

    /// The scope name for the input (useful for FROM references).
    pub fn scope_name(&self) -> &TableName {
        &self.scope_name
    }
}

impl Qualify for CteInput {
    fn qualify(&self, col_name: &str) -> Result<QualifiedColumn> {
        self.qualify(col_name)
    }

    fn try_qualify_with_table(&self, col_name: &str, table: &str) -> Option<QualifiedColumn> {
        self.qualify_with_table(col_name, table)
    }
}

/// What a `push_cte` closure returns: the CTE body and its output columns.
pub struct CteBody {
    /// The query expression that defines this CTE.
    pub query: QueryExpression,
    /// Column names this CTE produces. The builder assigns qualifiers
    /// (using the auto-generated CTE name).
    pub output_columns: Vec<String>,
}

// ---------------------------------------------------------------------------
// Builder<P> — the concrete type
// ---------------------------------------------------------------------------

/// Unified SQL builder with two-phase typestate.
///
/// This is the single type that flows through the entire transformer.
/// The phantom type `P` is either `Unprojected` or `Projected`.
///
/// # Invariants
///
/// - Scope and SQL are always consistent. Every state transition that
///   changes the SQL structure also updates the scope.
/// - Names are unique. The `NameGenerator` ensures every auto-generated
///   alias or CTE name is unique within the query.
/// - CTEs accumulate internally. No RefCell, no post-hoc harvesting.
///   `to_sql()` wraps the final query in a WITH clause if CTEs exist.
pub struct Builder<P> {
    state: BuilderState,
    names: NameGenerator,
    /// CTEs accumulated during lateral construction (push_cte calls).
    /// Emitted as a WITH clause by `to_sql()`.
    accumulated_ctes: Vec<Cte>,
    /// Zero-cost phase marker. `Unprojected` or `Projected`.
    _phase: PhantomData<P>,
}

// ---------------------------------------------------------------------------
// Phase-independent methods
// ---------------------------------------------------------------------------

impl<P> Builder<P> {
    /// Access the name generator (for forking to child builders).
    pub(in crate::pipeline::transformer_v4) fn names(&self) -> &NameGenerator {
        &self.names
    }

    /// Read-only access to the scope's columns.
    pub(in crate::pipeline::transformer_v4) fn columns(&self) -> &[ColumnMetadata] {
        &self.state.scope().columns
    }

    /// Internal: change the phase marker without changing any runtime state.
    /// Used by methods that perform a compile-time phase transition.
    fn rephase<Q>(self) -> Builder<Q> {
        Builder {
            state: self.state,
            names: self.names,
            accumulated_ctes: self.accumulated_ctes,
            _phase: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// Builder<Unprojected> — accumulation phase
// ---------------------------------------------------------------------------

impl Builder<Unprojected> {
    /// Create a builder from a ground table reference.
    ///
    /// This is the leaf case — the base of the dive-and-bubble recursion.
    /// The table name/alias and columns come from the AST annotation
    /// (the resolver decided them).
    pub(in crate::pipeline::transformer_v4) fn from_table(
        table: TableExpression,
        scope_name: TableName,
        columns: Vec<ColumnMetadata>,
        names: NameGenerator,
    ) -> Self {
        Self {
            state: BuilderState::Table {
                table,
                scope: ScopeEntry::new(scope_name, columns),
            },
            names,
            accumulated_ctes: Vec::new(),
            _phase: PhantomData,
        }
    }

    /// Create a builder from a pre-built `QueryExpression` (Frozen state).
    ///
    /// Used for anonymous tables and other constructs that produce SQL directly
    /// (e.g., `SELECT 1 AS x UNION ALL SELECT 2`). The query is already final;
    /// further operations (filter, projection) will wrap it as a subquery.
    pub(in crate::pipeline::transformer_v4) fn from_frozen(
        query: QueryExpression,
        scope_name: TableName,
        columns: Vec<ColumnMetadata>,
        names: NameGenerator,
    ) -> Self {
        Self {
            state: BuilderState::Frozen {
                query,
                scope: ScopeEntry::new(scope_name, columns),
            },
            names,
            accumulated_ctes: Vec::new(),
            _phase: PhantomData,
        }
    }

    /// Add a WHERE predicate. ANDs with existing WHERE if present.
    /// If the current state has GROUP BY, wraps as subquery first.
    pub fn add_where(mut self, pred: SqlPredicate) -> Result<Self> {
        self.state = self.state.ensure_filterable(&self.names)?;
        let expr = pred.into_expr();
        match &mut self.state {
            BuilderState::Segment { filters, .. } => {
                filters.push(expr);
            }
            // After demote(), state is Select with has_projection=false.
            // ensure_filterable passes this through (no GROUP BY).
            BuilderState::Select { select, .. } => {
                let taken = std::mem::replace(select, SelectBuilder::new());
                *select = taken.and_where(expr);
            }
            _ => unreachable!("ensure_filterable guarantees Segment or Select"),
        }
        Ok(self)
    }

    /// Prepare this builder for use as one side of a JOIN.
    ///
    /// Returns the table expression, requalified scope, remaining names,
    /// and accumulated CTEs. If the state needs wrapping (Segment, Select,
    /// Frozen), it becomes a subquery with a generated alias, and the
    /// scope columns are requalified to that alias.
    ///
    /// Use this when the join condition must be lowered against the
    /// post-wrap scopes (which is always the case — the condition's
    /// qualifiers must match the SQL aliases that actually appear).
    pub(in crate::pipeline::transformer_v4) fn into_join_operand(self) -> Result<JoinOperand> {
        let (table, scope) = self.state.into_table_expr(&self.names)?;
        Ok(JoinOperand {
            table,
            columns: scope.columns,
            names: self.names,
            ctes: self.accumulated_ctes,
        })
    }

    /// Assemble a join from prepared operands (from `into_join_operand`).
    ///
    /// For USING joins, right-side columns named in the USING list are excluded
    /// from the scope — SQL merges those columns automatically, and including
    /// them would produce duplicates in the SELECT list.
    pub(in crate::pipeline::transformer_v4) fn from_join(
        left: JoinOperand,
        mut right: JoinOperand,
        kind: JoinType,
        condition: JoinCondition,
    ) -> Self {
        // If both sides reference the same table/CTE without aliases,
        // alias the right side to avoid SQL ambiguity (e.g. FROM x INNER JOIN x).
        if let (Some(l_name), Some(r_name)) = (
            effective_table_name(&left.table),
            effective_table_name(&right.table),
        ) {
            if l_name == r_name {
                if let TableExpression::Table { alias, .. } = &mut right.table {
                    if alias.is_none() {
                        let fresh = left.names.next_table_name("t");
                        let alias_str = table_name_sql(&fresh).to_string();
                        // Requalify right columns to the new alias scope
                        let old_scope = ScopeEntry::new(fresh.clone(), right.columns);
                        let new_scope = old_scope.requalified(fresh);
                        right.columns = new_scope.columns;
                        *alias = Some(alias_str);
                    }
                }
            }
        }
        let join_expr = TableExpression::Join {
            left: Box::new(left.table),
            right: Box::new(right.table),
            join_type: kind,
            join_condition: condition.clone(),
        };
        let mut columns = left.columns;
        // For USING joins, SQL merges the USING columns — they appear once
        // in the output (from the left side). Exclude them from the right side.
        match &condition {
            JoinCondition::Using(using_cols) => {
                columns.extend(
                    right
                        .columns
                        .into_iter()
                        .filter(|c| !using_cols.iter().any(|uc| uc == col_name(c))),
                );
            }
            _ => {
                columns.extend(right.columns);
            }
        }
        let names = left.names;
        let mut ctes = left.ctes;
        ctes.extend(right.ctes);
        let join_scope_name = names.next_table_name("join");
        let scope = ScopeEntry::new(join_scope_name, columns);
        Self {
            state: BuilderState::Segment {
                from: vec![join_expr],
                filters: Vec::new(),
                order_by: Vec::new(),
                limit_offset: None,
                scope,
            },
            names,
            accumulated_ctes: ctes,
            _phase: PhantomData,
        }
    }

    /// Set the SELECT list. Transitions Unprojected → Projected.
    ///
    /// Disambiguates duplicate aliases: if two items share an alias
    /// (e.g., `u.id AS id` and `o.id AS id` from a join), the second
    /// gets a numeric suffix (`id_2`). This keeps the SELECT list and
    /// the scope in lockstep — wrapping later will find the unique name
    /// in both the inner SQL and the outer scope.
    pub fn add_projection(mut self, items: Vec<SelectItem>) -> Result<Builder<Projected>> {
        self.state = self.state.ensure_projectable(&self.names)?;
        match self.state {
            BuilderState::Select {
                select,
                has_group_by,
                scope: ref input_scope,
                ..
            } => {
                let input_columns = input_scope.columns.clone();
                let items = disambiguate_aliases(items);
                // Set the projection and generate a new scope name.
                let new_scope_name = self.names.next_table_name("t");
                // Atomic: derive columns AND write aliases back to items.
                let (items, output_columns) =
                    derive_columns_from_items(items, &new_scope_name, &input_columns);
                let select = select.set_select(items);
                Ok(Builder {
                    state: BuilderState::Select {
                        select,
                        has_projection: true,
                        has_group_by,
                        scope: ScopeEntry::new(new_scope_name, output_columns),
                    },
                    names: self.names,
                    accumulated_ctes: self.accumulated_ctes,
                    _phase: PhantomData,
                })
            }
            _ => unreachable!("ensure_projectable guarantees Select state"),
        }
    }

    /// Set GROUP BY with keys and aggregate reductions.
    /// Transitions Unprojected → Projected.
    pub fn add_group_by(mut self, spec: GroupBySpec) -> Result<Builder<Projected>> {
        self.state = self.state.ensure_projectable(&self.names)?;
        match self.state {
            BuilderState::Select {
                select,
                scope: ref input_scope,
                ..
            } => {
                let input_columns = input_scope.columns.clone();

                // Extract GROUP BY expressions from keys
                let group_exprs: Vec<DomainExpression> = spec
                    .keys
                    .iter()
                    .filter_map(|item| match item {
                        SelectItem::Expression { expr, .. } => Some(expr.clone()),
                        _ => None,
                    })
                    .collect();

                // SELECT list = keys ++ aggregates
                let mut select_items = spec.keys;
                select_items.extend(spec.aggregates);

                // Atomic: derive columns AND write aliases back to items.
                let new_scope_name = self.names.next_table_name("t");
                let (select_items, output_columns) =
                    derive_columns_from_items(select_items, &new_scope_name, &input_columns);

                let mut select = select.set_select(select_items);
                // Only emit GROUP BY if there are actual key expressions.
                // Empty keys = whole-table aggregate (SELECT count(*) FROM t).
                if !group_exprs.is_empty() {
                    select = select.group_by(group_exprs);
                }

                Ok(Builder {
                    state: BuilderState::Select {
                        select,
                        has_projection: true,
                        has_group_by: true,
                        scope: ScopeEntry::new(new_scope_name, output_columns),
                    },
                    names: self.names,
                    accumulated_ctes: self.accumulated_ctes,
                    _phase: PhantomData,
                })
            }
            _ => unreachable!("ensure_projectable guarantees Select state"),
        }
    }

    /// Add a passthrough projection listing all scope columns explicitly.
    ///
    /// Used when the pipe has no explicit projection segment. The builder
    /// tracks that this is a passthrough (`has_projection = false`) — a
    /// subsequent `add_projection` on the Projected result can *replace*
    /// rather than wrap.
    ///
    /// Uses `disambiguated_select_items()` — not `scope_to_select_items()` —
    /// so that duplicate column names (e.g. `u.id` and `o.id` after a join)
    /// get unique aliases (`id`, `id_2`). The scope is updated to match,
    /// with prior identities tracking the original (qualifier, name) pair.
    /// This keeps scope and SQL in lockstep: wrapping later will find the
    /// disambiguated name both in the scope and in the inner SELECT.
    pub fn project_all(mut self) -> Result<Builder<Projected>> {
        self.state = self.state.ensure_projectable(&self.names)?;
        match &mut self.state {
            BuilderState::Select { select, scope, .. } => {
                let (items, disambiguated_names) = scope.disambiguated_select_items();
                let items = if items.is_empty() {
                    vec![SelectItem::star()]
                } else {
                    items
                };
                let taken = std::mem::replace(select, SelectBuilder::new());
                *select = taken.set_select(items);

                // Remove hygienic columns from scope — they were kept for
                // qualify (e.g. filter references _label_0) but should not
                // appear in the output.
                scope.columns.retain(|c| !c.needs_hygienic_alias);

                // Update scope column names to match the disambiguated
                // SELECT aliases. Columns that were renamed get their
                // old identity pushed onto the provenance stack.
                for (col, new_name) in scope.columns.iter_mut().zip(disambiguated_names.iter()) {
                    if col_name(col) != new_name.as_str() {
                        state::push_scope_transition(
                            col,
                            Some(new_name.as_str()),
                            &scope.name,
                            crate::pipeline::asts::core::provenance::IdentityContext::Generated {
                                reason: "disambiguation".to_string(),
                                position: 0,
                            },
                        );
                    }
                }
            }
            _ => unreachable!("ensure_projectable guarantees Select"),
        }
        Ok(self.rephase())
    }

    /// Ensure the builder is not in Frozen state by wrapping if necessary.
    ///
    /// This is needed before lowering expressions that will be used in
    /// ORDER BY or WHERE clauses — if the builder is Frozen, `add_order_by`
    /// would wrap it as a subquery, changing the scope. Expressions must be
    /// lowered against the post-wrap scope, so call this first.
    pub fn ensure_not_frozen(self) -> Result<Self> {
        match &self.state {
            BuilderState::Frozen { .. } => Ok(Self {
                state: self.state.wrap_preserving_name(&self.names)?,
                ..self
            }),
            _ => Ok(self),
        }
    }

    /// Add ORDER BY terms. On Unprojected, these accumulate in the Segment
    /// or append to the Select (after demote, state may be Select).
    pub fn add_order_by(self, terms: Vec<OrderTerm>) -> Result<Self> {
        match self.state {
            BuilderState::Segment {
                from,
                filters,
                mut order_by,
                limit_offset,
                scope,
            } => {
                order_by.extend(terms);
                Ok(Self {
                    state: BuilderState::Segment {
                        from,
                        filters,
                        order_by,
                        limit_offset,
                        scope,
                    },
                    ..self
                })
            }
            BuilderState::Table { table, scope } => {
                let promoted = Self {
                    state: BuilderState::Segment {
                        from: vec![table],
                        filters: Vec::new(),
                        order_by: Vec::new(),
                        limit_offset: None,
                        scope,
                    },
                    ..self
                };
                promoted.add_order_by(terms)
            }
            // After demote(), state is Select with has_projection=false.
            BuilderState::Select {
                select,
                has_projection,
                has_group_by,
                scope,
            } => {
                let mut select = select;
                for term in terms {
                    select = select.order_by(term);
                }
                Ok(Self {
                    state: BuilderState::Select {
                        select,
                        has_projection,
                        has_group_by,
                        scope,
                    },
                    ..self
                })
            }
            BuilderState::Frozen { .. } => {
                let wrapped = Self {
                    state: self.state.wrap_as_subquery(&self.names)?,
                    ..self
                };
                wrapped.add_order_by(terms)
            }
        }
    }

    /// Add LIMIT with optional OFFSET. On Unprojected, stored in the Segment
    /// or applied to the Select (after demote, state may be Select).
    pub fn add_limit(self, count: i64, offset: Option<i64>) -> Result<Self> {
        match self.state {
            BuilderState::Segment {
                from,
                filters,
                order_by,
                scope,
                ..
            } => Ok(Self {
                state: BuilderState::Segment {
                    from,
                    filters,
                    order_by,
                    limit_offset: Some((count, offset)),
                    scope,
                },
                ..self
            }),
            BuilderState::Table { table, scope } => {
                let promoted = Self {
                    state: BuilderState::Segment {
                        from: vec![table],
                        filters: Vec::new(),
                        order_by: Vec::new(),
                        limit_offset: None,
                        scope,
                    },
                    ..self
                };
                promoted.add_limit(count, offset)
            }
            // After demote(), state is Select with has_projection=false.
            BuilderState::Select {
                select,
                has_projection,
                has_group_by,
                scope,
            } => {
                let select = match offset {
                    Some(off) => select.limit_offset(count, off),
                    None => select.limit(count),
                };
                Ok(Self {
                    state: BuilderState::Select {
                        select,
                        has_projection,
                        has_group_by,
                        scope,
                    },
                    ..self
                })
            }
            BuilderState::Frozen { .. } => {
                let wrapped = Self {
                    state: self.state.wrap_as_subquery(&self.names)?,
                    ..self
                };
                wrapped.add_limit(count, offset)
            }
        }
    }

    /// Expand a JSON array column into rows via `json_each`.
    ///
    /// Wraps the current builder as a subquery, adds a `json_each(source.column)`
    /// TVF, and builds a new SELECT from caller-provided context and interior items.
    ///
    /// The builder handles the scope split: context items inherit source provenance
    /// (for inductive chaining like `A.B(*).C(*)`), interior items get fresh
    /// provenance with `column` as table qualifier.
    ///
    /// Callers provide two closures that receive the SQL alias strings:
    /// - `context_items_fn(source_alias)` — passthrough items from the source
    /// - `interior_items_fn(tvf_alias)` — new items extracted from the TVF
    pub fn expand_with_json_each(
        self,
        column: &str,
        tvf_prefix: &str,
        context_items_fn: impl FnOnce(&str) -> Vec<SelectItem>,
        interior_items_fn: impl FnOnce(&str) -> Vec<SelectItem>,
        groundings: &[(String, String)],
    ) -> Result<Builder<Projected>> {
        use crate::pipeline::asts::core::provenance::{ColumnProvenance, QualificationSource};

        let source_columns: Vec<ColumnMetadata> = self.columns().to_vec();
        let names_fork = self.names().fork();
        let source_alias = names_fork.next_table_name("t");
        let tvf_alias = names_fork.next_table_name(tvf_prefix);

        let source_query = self.project_all()?.to_sql()?;
        let source_alias_str = table_name_sql(&source_alias).to_string();
        let tvf_alias_str = table_name_sql(&tvf_alias).to_string();

        let source_table = TableExpression::subquery(source_query, &source_alias_str);
        let je_tvf = TableExpression::TVF {
            schema: None,
            function: "json_each".to_string(),
            arguments: vec![TvfArgument::QualifiedRef {
                qualifier: source_alias_str.clone(),
                column: column.to_string(),
            }],
            alias: Some(tvf_alias_str.clone()),
        };

        let context_items = context_items_fn(&source_alias_str);
        let num_context = context_items.len();
        let mut all_items: Vec<SelectItem> = context_items;
        all_items.extend(interior_items_fn(&tvf_alias_str));

        // Snapshot pre-disambiguation aliases for provenance.
        let pre_disambig: Vec<Option<String>> = all_items
            .iter()
            .map(|item| match item {
                SelectItem::Expression { alias, .. } => alias.clone(),
                _ => None,
            })
            .collect();
        let all_items = disambiguate_aliases(all_items);

        // Build SELECT with optional grounding WHERE conditions.
        let mut sb = SelectBuilder::new()
            .set_select(all_items.clone())
            .from_tables(vec![source_table, je_tvf]);
        for (schema_name, value) in groundings {
            sb = sb.and_where(DomainExpression::RawSql(format!(
                "json_extract({}.value, '$.{}') = '{}'",
                tvf_alias_str,
                schema_name,
                value.replace('\'', "''")
            )));
        }
        let select = sb
            .build()
            .map_err(|e| crate::error::DelightQLError::ParseError {
                message: format!("json_each expansion: {}", e),
                source: None,
                subcategory: None,
            })?;

        let query = QueryExpression::Select(Box::new(select));
        let scope_name = TableName::Fresh;

        // Context items inherit source provenance; interior items get fresh.
        let columns: Vec<ColumnMetadata> = all_items
            .iter()
            .enumerate()
            .filter_map(|(i, item)| {
                let SelectItem::Expression { alias, expr } = item else {
                    return None;
                };
                let name = alias.as_deref().unwrap_or("_expr");
                let original = pre_disambig
                    .get(i)
                    .and_then(|a| a.as_deref())
                    .unwrap_or(name);

                let prov = if i < num_context {
                    // Context: inherit source provenance matched by expression column name.
                    let source_col = extract_column_name(expr);
                    let lookup = source_col.as_deref().unwrap_or(original);
                    let mut p = source_columns
                        .iter()
                        .find(|c| col_name(c) == lookup)
                        .map(|c| c.info.clone())
                        .unwrap_or_else(|| ColumnProvenance::from_column(original));
                    if name != original {
                        p = p.with_alias(name);
                    }
                    p
                } else {
                    // Interior: fresh provenance, column name as table qualifier.
                    let p = ColumnProvenance::from_table_column(
                        original,
                        TableName::Named(column.into()),
                        QualificationSource::Resolver,
                    );
                    if name != original {
                        p.with_alias(name)
                    } else {
                        p
                    }
                };
                Some(ColumnMetadata::new(prov, scope_name.clone(), Some(i)))
            })
            .collect();

        Ok(Builder::from_query(query, scope_name, columns, names_fork))
    }
}

// ---------------------------------------------------------------------------
// Builder<Projected> — finishing phase
// ---------------------------------------------------------------------------

impl Builder<Projected> {
    /// Create a builder from a frozen QueryExpression.
    ///
    /// Used when embedding a pre-built query (e.g., from EntityRegistry)
    /// or after external construction (set operations, recursive CTEs).
    pub(in crate::pipeline::transformer_v4) fn from_query(
        query: QueryExpression,
        scope_name: TableName,
        columns: Vec<ColumnMetadata>,
        names: NameGenerator,
    ) -> Self {
        Self {
            state: BuilderState::Frozen {
                query,
                scope: ScopeEntry::new(scope_name, columns),
            },
            names,
            accumulated_ctes: Vec::new(),
            _phase: PhantomData,
        }
    }

    /// Re-projection on a Projected builder. Wraps as subquery first
    /// (unless current projection is a passthrough), then sets new SELECT list.
    pub fn add_projection(mut self, items: Vec<SelectItem>) -> Result<Self> {
        self.state = self.state.ensure_projectable(&self.names)?;
        match self.state {
            BuilderState::Select {
                select,
                has_group_by,
                scope: ref input_scope,
                ..
            } => {
                let input_columns = input_scope.columns.clone();
                let new_scope_name = self.names.next_table_name("t");
                let (items, output_columns) =
                    derive_columns_from_items(items, &new_scope_name, &input_columns);
                let select = select.set_select(items);
                Ok(Self {
                    state: BuilderState::Select {
                        select,
                        has_projection: true,
                        has_group_by,
                        scope: ScopeEntry::new(new_scope_name, output_columns),
                    },
                    names: self.names,
                    accumulated_ctes: self.accumulated_ctes,
                    _phase: PhantomData,
                })
            }
            _ => unreachable!("ensure_projectable guarantees Select state"),
        }
    }

    /// Add a window function column to the current projection.
    ///
    /// Wraps as subquery, then re-projects with `SELECT *, window_fn AS alias`.
    /// Used by bag intersection (min_multiplicity) to add ROW_NUMBER.
    pub fn add_window_column(
        self,
        func_name: &str,
        args: Vec<DomainExpression>,
        partition_by: Vec<DomainExpression>,
        order_by: Vec<(DomainExpression, crate::pipeline::sql_ast_v3::ordering::OrderDirection)>,
        alias: &str,
    ) -> Result<Self> {
        // Wrap current state as subquery so window function sees finalized rows
        let wrapped = Self {
            state: self.state.wrap_as_subquery(&self.names)?,
            names: self.names,
            accumulated_ctes: self.accumulated_ctes,
            _phase: PhantomData,
        };

        // Build SELECT *, window_fn AS alias
        let window_item = SelectItem::Expression {
            expr: DomainExpression::WindowFunction {
                name: func_name.to_string(),
                args,
                partition_by,
                order_by,
                frame: None,
            },
            alias: Some(alias.to_string()),
        };

        match wrapped.state {
            BuilderState::Select {
                select,
                has_group_by,
                scope: ref input_scope,
                ..
            } => {
                let input_columns = input_scope.columns.clone();
                let new_scope_name = wrapped.names.next_table_name("t");

                // Start with Star + window column
                let items = vec![SelectItem::Star, window_item];
                let (items, output_columns) =
                    derive_columns_from_items(items, &new_scope_name, &input_columns);
                let select = select.set_select(items);

                Ok(Self {
                    state: BuilderState::Select {
                        select,
                        has_projection: true,
                        has_group_by,
                        scope: ScopeEntry::new(new_scope_name, output_columns),
                    },
                    names: wrapped.names,
                    accumulated_ctes: wrapped.accumulated_ctes,
                    _phase: PhantomData,
                })
            }
            _ => unreachable!("wrap_as_subquery guarantees Select state"),
        }
    }

    /// Set DISTINCT on the current SELECT.
    pub fn add_distinct(self) -> Result<Self> {
        match self.state {
            BuilderState::Select {
                select,
                has_projection,
                has_group_by,
                scope,
            } => Ok(Self {
                state: BuilderState::Select {
                    select: select.distinct(),
                    has_projection,
                    has_group_by,
                    scope,
                },
                ..self
            }),
            _ => {
                let wrapped = Self {
                    state: self.state.wrap_as_subquery(&self.names)?,
                    names: self.names,
                    accumulated_ctes: self.accumulated_ctes,
                    _phase: PhantomData,
                };
                wrapped.add_distinct()
            }
        }
    }

    // --- Set operations ---

    /// UNION ALL with another builder. Both finalized, combined as set op.
    pub fn union_all(self, right: Self) -> Result<Self> {
        self.set_operation(right, crate::pipeline::sql_ast_v3::SetOperator::UnionAll)
    }

    /// EXCEPT (MINUS) with another builder.
    pub fn except(self, right: Self) -> Result<Self> {
        self.set_operation(right, crate::pipeline::sql_ast_v3::SetOperator::Except)
    }

    // --- Lateral construction ---

    /// Freeze the current state as a CTE body, auto-name it, and update
    /// scope to reference the new CTE.
    pub fn push_cte<F>(mut self, body: F) -> Result<Self>
    where
        F: FnOnce(&CteInput) -> Result<CteBody>,
    {
        // Ensure the current scope name is reachable at CTE level.
        // If the state is a Select with a subquery alias (e.g., t_1 from
        // wrap_as_subquery), that alias is only visible inside the SELECT,
        // not at WITH scope. Materialize as a preliminary CTE in that case.
        {
            let scope = self.state.scope();
            let scope_str = table_name_sql(&scope.name);
            let reachable = matches!(&self.state, BuilderState::Table { .. })
                || self.accumulated_ctes.iter().any(|c| c.name() == scope_str);

            if !reachable {
                let scope_clone = scope.clone();
                let source_query = self.state.materialize()?;
                let source_name = self.names.next_name("cte");
                self.accumulated_ctes
                    .push(Cte::new(source_name.clone(), source_query));

                let source_scope =
                    TableName::Named(delightql_types::SqlIdentifier::from(source_name.as_str()));
                let requalified = scope_clone.requalified(source_scope);
                let cte_table = TableExpression::table(&source_name);
                self.state = BuilderState::Select {
                    select: SelectBuilder::new()
                        .from_tables(vec![cte_table])
                        .set_select(vec![SelectItem::star()]),
                    has_projection: false,
                    has_group_by: false,
                    scope: requalified,
                };
            }
        }

        // Snapshot the current scope as CteInput — now guaranteed reachable.
        let current_scope = self.state.scope().clone();
        let input = CteInput::new(current_scope.name.clone(), current_scope.columns.clone());

        // Call the closure to get the CTE body
        let cte_body = body(&input)?;

        // Generate a CTE name
        let cte_name = self.names.next_name("cte");
        let cte_scope = TableName::Named(delightql_types::SqlIdentifier::from(cte_name.as_str()));

        let output_columns = build_cte_output_columns(
            &cte_body.output_columns,
            &current_scope.columns,
            &cte_name,
            &cte_scope,
        );

        // Accumulate the CTE
        self.accumulated_ctes
            .push(Cte::new(cte_name.clone(), cte_body.query));

        // Transition to a new Select FROM the CTE.
        // The old state is discarded — the closure already used it to build the CTE body.
        // The new state references the CTE by name so that subsequent operations
        // (add_projection, another push_cte) operate on the CTE's output.
        let cte_table = TableExpression::table(&cte_name);
        let new_scope = ScopeEntry::new(cte_scope, output_columns);
        self.state = BuilderState::Select {
            select: SelectBuilder::new()
                .from_tables(vec![cte_table])
                .set_select(vec![SelectItem::star()]),
            has_projection: false,
            has_group_by: false,
            scope: new_scope,
        };

        Ok(self)
    }

    // --- Phase demotion ---

    /// Demote to `Builder<Unprojected>` by wrapping as a subquery in FROM.
    ///
    /// "You are not the top-level query — you are demoted to subsidiary."
    /// Used when a projected query needs to be composed into a larger query
    /// (e.g., as a join child or pipe source).
    pub fn demote(self) -> Result<Builder<Unprojected>> {
        Ok(Builder {
            state: self.state.wrap_as_subquery(&self.names)?,
            names: self.names,
            accumulated_ctes: self.accumulated_ctes,
            _phase: PhantomData,
        })
    }

    // --- Finalization (ONLY on Projected) ---

    /// Consume the builder and emit the final SQL.
    /// Accumulated CTEs become a WITH clause wrapping the query.
    ///
    /// This method exists only on `Builder<Projected>`. Calling it on
    /// `Builder<Unprojected>` is a compile error.
    pub fn to_sql(self) -> Result<QueryExpression> {
        let query = self.state.materialize()?;

        if self.accumulated_ctes.is_empty() {
            Ok(query)
        } else {
            // If the inner query already has a WITH clause, merge CTEs
            // into a single WITH to avoid `WITH ... WITH ...` syntax errors.
            match query {
                QueryExpression::WithCte {
                    ctes: inner_ctes,
                    query: inner_query,
                } => {
                    let mut merged = inner_ctes;
                    merged.extend(self.accumulated_ctes);
                    Ok(QueryExpression::WithCte {
                        ctes: merged,
                        query: inner_query,
                    })
                }
                other => Ok(QueryExpression::WithCte {
                    ctes: self.accumulated_ctes,
                    query: Box::new(other),
                }),
            }
        }
    }

    // --- Private helpers ---

    /// Shared implementation for set operations (UNION ALL, INTERSECT, EXCEPT).
    fn set_operation(
        self,
        right: Self,
        op: crate::pipeline::sql_ast_v3::SetOperator,
    ) -> Result<Self> {
        let mut ctes = self.accumulated_ctes;
        ctes.extend(right.accumulated_ctes);

        let left_scope = self.state.scope().clone();
        let left_query = self.state.materialize()?;
        let right_query = right.state.materialize()?;

        let combined = QueryExpression::SetOperation {
            op,
            left: Box::new(left_query),
            right: Box::new(right_query),
        };

        // Output scope: use left's columns with a new generated name
        let set_scope_name = self.names.next_table_name("set");
        let output_scope = left_scope.requalified(set_scope_name);

        Ok(Self {
            state: BuilderState::Frozen {
                query: combined,
                scope: output_scope,
            },
            names: self.names,
            accumulated_ctes: ctes,
            _phase: PhantomData,
        })
    }
}

// ---------------------------------------------------------------------------
// Qualify — phase-independent implementation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Shared qualify logic — ONE implementation, used by Builder, JoinOperand,
// and CteInput. No consumer outside this module reimplements these tiers.
// ---------------------------------------------------------------------------

/// Qualify an unqualified column name against a column list.
///
/// Tier 1: exact name match
/// Tier 2: case-insensitive match
/// Tier 3: identity stack walk (renamed by disambiguation)
pub(in crate::pipeline::transformer_v4) fn qualify_in_columns(
    col_name_str: &str,
    columns: &[ColumnMetadata],
    scope_label: &str,
) -> Result<QualifiedColumn> {
    // Unknown-schema passthrough: no columns means we have no information
    // (e.g., passthrough tables like sqlite_master).
    if columns.is_empty() {
        return Ok(QualifiedColumn {
            name: col_name_str.to_string(),
            qualifier: None,
        });
    }

    // Tier 1: exact name match.
    let matches: Vec<_> = columns
        .iter()
        .filter(|c| col_name(c) == col_name_str)
        .collect();

    match matches.len() {
        1 => Ok(QualifiedColumn {
            name: col_name(matches[0]).to_string(),
            qualifier: col_qualifier(matches[0]).map(|s| s.to_string()),
        }),
        0 => {
            // Tier 2: case-insensitive match.
            let ci_lower = col_name_str.to_ascii_lowercase();
            let ci_matches: Vec<_> = columns.iter().filter(|c| {
                col_name(c).to_ascii_lowercase() == ci_lower
            }).collect();
            if ci_matches.len() == 1 {
                return Ok(QualifiedColumn {
                    name: col_name(ci_matches[0]).to_string(),
                    qualifier: col_qualifier(ci_matches[0]).map(|s| s.to_string()),
                });
            }

            // Tier 3: identity stack walk.
            let historical: Vec<_> = columns.iter().filter(|c| {
                c.info.identity_stack().iter().any(|id| {
                    id.name.as_str() == col_name_str
                })
            }).collect();
            match historical.len() {
                1 => Ok(QualifiedColumn {
                    name: col_name(historical[0]).to_string(),
                    qualifier: col_qualifier(historical[0]).map(|s| s.to_string()),
                }),
                0 => Err(crate::error::DelightQLError::ParseError {
                    message: format!(
                        "qualify: column '{}' not found in scope '{}'",
                        col_name_str, scope_label,
                    ),
                    source: None,
                    subcategory: None,
                }),
                _ => Err(crate::error::DelightQLError::ParseError {
                    message: format!(
                        "qualify: column '{}' is ambiguous via provenance in scope '{}' ({} matches)",
                        col_name_str, scope_label, historical.len(),
                    ),
                    source: None,
                    subcategory: None,
                }),
            }
        }
        _ => Err(crate::error::DelightQLError::ParseError {
            message: format!(
                "qualify: column '{}' is ambiguous in scope '{}' ({} matches) — use table-qualified reference",
                col_name_str, scope_label, matches.len(),
            ),
            source: None,
            subcategory: None,
        }),
    }
}

/// Try to qualify a table-qualified column name against a column list.
///
/// Tier 1: exact (name, qualifier) match
/// Tier 2: identity stack walk for (table, name) pair
pub(in crate::pipeline::transformer_v4) fn try_qualify_with_table_in_columns(
    col_name_str: &str,
    table: &str,
    columns: &[ColumnMetadata],
) -> Option<QualifiedColumn> {
    // Tier 1: exact match — column name AND current qualifier both match.
    let found = columns
        .iter()
        .find(|c| col_name(c) == col_name_str && col_qualifier(c).map_or(false, |q| q == table));

    if let Some(col) = found {
        return Some(QualifiedColumn {
            name: col_name(col).to_string(),
            qualifier: col_qualifier(col).map(|s| s.to_string()),
        });
    }

    // Tier 2: identity stack walk.
    let historical = columns.iter().find(|c| {
        c.info.identity_stack().iter().any(|id| {
            id.name.as_str() == col_name_str
                && match &id.table_qualifier {
                    TableName::Named(s) => s.as_str() == table,
                    TableName::Fresh => false,
                }
        })
    });

    if let Some(col) = historical {
        return Some(QualifiedColumn {
            name: col_name(col).to_string(),
            qualifier: col_qualifier(col).map(|s| s.to_string()),
        });
    }

    // Tier 3: "_" qualifier — anonymous pipe output.
    //
    // `_` is used in conditions like `_.id = o.user_id` to refer to the
    // anonymous (un-aliased) left side of a join after a pipe. After
    // `demote()`, the pipe output columns have `PipeBarrier { previous_table:
    // Fresh }` at the front of their identity stack. Match by that marker.
    if table == "_" {
        use crate::pipeline::asts::core::provenance::IdentityContext;
        let anon = columns.iter().find(|c| {
            col_name(c) == col_name_str
                && c.info.identity_stack().first().map_or(false, |id| {
                    matches!(
                        &id.context,
                        IdentityContext::PipeBarrier {
                            previous_table: TableName::Fresh,
                            ..
                        }
                    )
                })
        });
        if let Some(col) = anon {
            return Some(QualifiedColumn {
                name: col_name(col).to_string(),
                qualifier: col_qualifier(col).map(|s| s.to_string()),
            });
        }
    }

    None
}

// ---------------------------------------------------------------------------
// Builder Qualify — delegates to shared functions
// ---------------------------------------------------------------------------

impl<P> Qualify for Builder<P> {
    fn qualify(&self, col_name_str: &str) -> Result<QualifiedColumn> {
        let scope = self.state.scope();
        let mut qc = qualify_in_columns(
            col_name_str,
            &scope.columns,
            state::table_name_str(&scope.name).unwrap_or("<fresh>"),
        )?;
        // Empty-scope passthrough: fill in the scope's own qualifier.
        if qc.qualifier.is_none() && scope.columns.is_empty() {
            qc.qualifier = state::table_name_str(&scope.name).map(|s| s.to_string());
        }
        Ok(qc)
    }

    fn try_qualify_with_table(&self, col_name_str: &str, table: &str) -> Option<QualifiedColumn> {
        if table == "_" {
            // Try unambiguous lookup first; fall through to Tier 3 if ambiguous.
            if let Some(qc) = self
                .qualify(col_name_str)
                .ok()
                .filter(|q| q.qualifier.is_some())
            {
                return Some(qc);
            }
        }
        try_qualify_with_table_in_columns(col_name_str, table, &self.state.scope().columns)
    }

    fn scope_columns(&self) -> Vec<ColumnMetadata> {
        self.state.scope().columns.clone()
    }
}

// ---------------------------------------------------------------------------
// JoinOperand — post-consumption join side that implements Qualify.
//
// Created by `into_join_operand()`. The builder is consumed but the qualify
// logic stays in THIS module, using the shared functions above.
// ---------------------------------------------------------------------------

/// One side of a join after the builder has been consumed.
///
/// Holds the table expression, columns, and accumulated state. Implements
/// `Qualify` so the join condition can be lowered against it without
/// reimplementing the lookup tiers outside the builder module.
pub(in crate::pipeline::transformer_v4) struct JoinOperand {
    pub table: TableExpression,
    pub columns: Vec<ColumnMetadata>,
    pub names: NameGenerator,
    pub ctes: Vec<Cte>,
}

impl Qualify for JoinOperand {
    fn qualify(&self, col_name_str: &str) -> Result<QualifiedColumn> {
        qualify_in_columns(col_name_str, &self.columns, "<join operand>")
    }

    fn try_qualify_with_table(&self, col_name_str: &str, table: &str) -> Option<QualifiedColumn> {
        if table == "_" {
            // Try unambiguous lookup first; fall through to Tier 3 if ambiguous.
            if let Some(qc) = self
                .qualify(col_name_str)
                .ok()
                .filter(|q| q.qualifier.is_some())
            {
                return Some(qc);
            }
        }
        try_qualify_with_table_in_columns(col_name_str, table, &self.columns)
    }
}

impl JoinOperand {
    /// Resolve TVF QualifiedRef arguments in this operand's table expression
    /// against an external scope. Converts `QualifiedRef { "anon", "a" }` to
    /// `ColumnRef` with the correct post-wrap qualifier (e.g., `t_1.a`).
    pub fn resolve_tvf_args(&mut self, scope: &dyn Qualify) {
        resolve_tvf_args_in_table(&mut self.table, scope);
    }
}

fn resolve_tvf_args_in_table(table: &mut TableExpression, scope: &dyn Qualify) {
    match table {
        TableExpression::TVF { arguments, .. } => {
            for arg in arguments.iter_mut() {
                if let TvfArgument::QualifiedRef { qualifier, column } = arg {
                    if let Some(qc) = scope.try_qualify_with_table(column, qualifier) {
                        if let Some(q) = qc.qualifier {
                            *arg = TvfArgument::QualifiedRef {
                                qualifier: q,
                                column: qc.name,
                            };
                        }
                    }
                }
            }
        }
        TableExpression::Join { left, right, .. } => {
            resolve_tvf_args_in_table(left, scope);
            resolve_tvf_args_in_table(right, scope);
        }
        _ => {}
    }
}

/// Chained qualify: try inner scope, then outer scope.
///
/// Used for joins (left + right) and correlated subqueries (inner + outer).
/// Lives in the builder module — the chaining logic is part of the qualify
/// contract, not something consumers should reimplement.
pub(in crate::pipeline::transformer_v4) struct ChainedQualify<'a> {
    pub inner: &'a dyn Qualify,
    pub outer: &'a dyn Qualify,
}

impl Qualify for ChainedQualify<'_> {
    fn qualify(&self, col_name: &str) -> Result<QualifiedColumn> {
        match self.inner.qualify(col_name) {
            Ok(qc) => Ok(qc),
            Err(_) => self.outer.qualify(col_name),
        }
    }

    fn try_qualify_with_table(&self, col_name: &str, table: &str) -> Option<QualifiedColumn> {
        self.inner
            .try_qualify_with_table(col_name, table)
            .or_else(|| self.outer.try_qualify_with_table(col_name, table))
    }
}

// ---------------------------------------------------------------------------
// BuilderState helpers (private, on the state enum)
// ---------------------------------------------------------------------------

impl BuilderState {
    /// Extract the table expression for use in a JOIN's FROM.
    ///
    /// Returns the table expression and a requalified scope. When the state
    /// is wrapped as a subquery, the scope columns are requalified to the
    /// new alias — callers must use the returned scope, not a stale copy.
    ///
    /// - Table → returns the table directly, scope unchanged
    /// - Segment → builds a subquery, requalifies to alias
    /// - Select / Frozen → builds a subquery, requalifies to alias
    fn into_table_expr(self, names: &NameGenerator) -> Result<(TableExpression, ScopeEntry)> {
        match self {
            Self::Table { table, scope } => Ok((table, scope)),
            // Segment flattening disabled. The core fixes (explicit
            // materialize + prior_identities + global disambiguation)
            // handle correctness. Flattening is a future optimization
            // for cleaner SQL output, not a correctness mechanism.
            other => {
                let scope = other.scope().clone();
                let query = other.materialize()?;
                // Preserve user-provided aliases (Named) — these are
                // meaningful identifiers that other parts of the query
                // may reference (e.g., TVF arguments in a join).
                // Only generate fresh names for anonymous scopes.
                let alias = match &scope.name {
                    TableName::Named(_) => scope.name.clone(),
                    _ => names.next_table_name("t"),
                };
                let new_scope = scope.requalified_with_disambiguation(alias);
                let alias_str = table_name_sql(&new_scope.name).to_string();
                Ok((TableExpression::subquery(query, alias_str), new_scope))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract the effective SQL table name from a TableExpression.
/// Returns the alias if present, otherwise the base table name.
fn effective_table_name(table: &TableExpression) -> Option<&str> {
    match table {
        TableExpression::Table { alias, name, .. } => {
            Some(alias.as_deref().unwrap_or(name.as_str()))
        }
        TableExpression::Subquery { alias, .. } => Some(alias.as_str()),
        _ => None,
    }
}

/// Derive output column specs from select items — atomic operation.
///
/// Takes ownership of the items and returns (items, columns) where every
/// SelectItem is guaranteed to have an explicit alias matching the
/// corresponding scope entry's column name. This is the invariant:
/// scope names and SQL aliases never diverge.
///
/// - Aliased items: scope uses the alias, item unchanged.
/// - Bare column refs: scope uses the column name, item unchanged
///   (SQL output name is the column name).
/// - Complex expressions without alias: generates `_expr_{i}` and writes
///   it as an `AS` alias on the SelectItem.
fn derive_columns_from_items(
    items: Vec<SelectItem>,
    scope_name: &TableName,
    input_columns: &[ColumnMetadata],
) -> (Vec<SelectItem>, Vec<ColumnMetadata>) {
    use crate::pipeline::asts::core::provenance::{ColumnProvenance, QualificationSource};
    use crate::pipeline::asts::core::TableName;

    let mut columns = Vec::new();
    let mut out_items = Vec::with_capacity(items.len());

    for (i, item) in items.into_iter().enumerate() {
        match item {
            SelectItem::Star => {
                // Star preserves existing columns — expand input scope
                // so downstream qualify() can find them.
                for col in input_columns {
                    columns.push(col.clone());
                }
                out_items.push(SelectItem::Star);
            }
            SelectItem::QualifiedStar { qualifier } => {
                // Expand matching input columns for this qualifier.
                for col in input_columns {
                    if let Some(q) = col_qualifier(col) {
                        if q == qualifier.table_name() {
                            columns.push(col.clone());
                        }
                    }
                }
                out_items.push(SelectItem::QualifiedStar { qualifier });
            }
            SelectItem::Expression { expr, alias } => {
                let (name, final_alias) = if let Some(alias) = alias {
                    // Alias present — scope name matches SQL alias.
                    (alias.clone(), Some(alias))
                } else if let DomainExpression::Column { name, .. } = &expr {
                    // Bare column — SQL output name is the column name.
                    (name.clone(), None)
                } else {
                    // Complex expression — generate name AND write as alias.
                    let generated = format!("_expr_{}", i);
                    (generated.clone(), Some(generated))
                };

                // Inherit the input column's full provenance when possible.
                // This preserves the identity stack so that Tier 2 lookups
                // (identity stack walk) can resolve renamed columns back to
                // their original (table, name) pair.
                let provenance = if let Some(input_col) = find_input_column(&expr, input_columns) {
                    let mut prov = input_col.info.clone();
                    // If the output name differs from the input name (e.g.,
                    // disambiguation renamed "id" → "id_2"), push a scope
                    // transition so the stack records the rename.
                    if col_name(input_col) != name {
                        push_scope_transition_on_provenance(&mut prov, name.as_str(), scope_name);
                    }
                    prov
                } else {
                    // No matching input column (complex expression, literal, etc.)
                    ColumnProvenance::from_table_column(
                        name.as_str(),
                        scope_name.clone(),
                        QualificationSource::Resolver,
                    )
                };
                columns.push(ColumnMetadata::new(provenance, TableName::Fresh, None));
                out_items.push(SelectItem::Expression {
                    expr,
                    alias: final_alias,
                });
            }
        }
    }
    (out_items, columns)
}

/// Find the input column matching a SQL column expression.
///
/// For simple column references (`qualifier.name`), returns the matching
/// input column so its identity stack can be inherited by the output column.
fn find_input_column<'a>(
    expr: &DomainExpression,
    input_columns: &'a [ColumnMetadata],
) -> Option<&'a ColumnMetadata> {
    if let DomainExpression::Column {
        name: expr_name,
        qualifier,
    } = expr
    {
        let expr_qual = qualifier.as_ref().map(|q| q.table_name());
        input_columns.iter().find(|c| {
            col_name(c) == expr_name.as_str()
                && (expr_qual.is_none() || col_qualifier(c) == expr_qual)
        })
    } else {
        None
    }
}

/// Push a scope transition directly on a ColumnProvenance.
///
/// Like `push_scope_transition` on ColumnMetadata, but operates on
/// the provenance directly when we don't have a full ColumnMetadata yet.
fn push_scope_transition_on_provenance(
    prov: &mut crate::pipeline::asts::core::provenance::ColumnProvenance,
    name: &str,
    scope: &TableName,
) {
    use crate::pipeline::asts::core::provenance::{
        ColumnIdentity, IdentityContext, TransformationPhase,
    };
    prov.push_identity(ColumnIdentity {
        name: delightql_types::SqlIdentifier::from(name),
        context: IdentityContext::PipeBarrier {
            previous_table: TableName::Fresh,
            fresh_scope: 0,
        },
        phase: TransformationPhase::Transformer,
        table_qualifier: scope.clone(),
    });
}

/// Extract the column name from a SQL `Column { name, .. }` expression.
/// Used by `expand_with_json_each` to match context items against source columns
/// for provenance inheritance.
fn extract_column_name(expr: &DomainExpression) -> Option<String> {
    match expr {
        DomainExpression::Column { name, .. } => Some(name.clone()),
        _ => None,
    }
}

/// Ensure every SELECT item produces a unique output name.
///
/// Covers both aliased items (`expr AS name`) and bare column references
/// (`table.col` with no alias). For bare columns, the "effective name" is
/// the column name from the expression. When two items share an effective
/// name, the second gets `_2`, `_3`, etc. — and an explicit `AS` alias is
/// added so the SQL reflects the disambiguated name.
///
/// This is the single chokepoint: all operators that build SELECT lists
/// (retain, embed, map-cover, project-out, etc.) pass through
/// `add_projection()`, which calls this function. No individual operator
/// needs to worry about duplicates.
/// Build output columns for a CTE, carrying forward provenance from input columns.
///
/// When an output column matches an input column by name, the input's identity
/// stack is preserved so downstream CTE levels can resolve qualified references
/// (e.g., `o.id` → `id_2`) through history.
fn build_cte_output_columns(
    output_names: &[String],
    input_columns: &[ColumnMetadata],
    cte_name: &str,
    cte_scope: &TableName,
) -> Vec<ColumnMetadata> {
    use crate::pipeline::asts::core::provenance::ColumnProvenance;

    output_names
        .iter()
        .map(|name| {
            let input_col = input_columns
                .iter()
                .find(|c| col_name(c) == name.as_str());

            let provenance = if let Some(src) = input_col {
                let mut prov = src.info.clone();
                prov.push_identity(
                    crate::pipeline::asts::core::provenance::ColumnIdentity {
                        name: delightql_types::SqlIdentifier::from(name.as_str()),
                        context: crate::pipeline::asts::core::provenance::IdentityContext::CteRegistration {
                            cte_name: cte_name.to_string(),
                            origin: crate::pipeline::asts::core::provenance::CteOrigin::CompilerGenerated,
                        },
                        phase: crate::pipeline::asts::core::provenance::TransformationPhase::Transformer,
                        table_qualifier: cte_scope.clone(),
                    },
                );
                prov
            } else {
                ColumnProvenance::from_table_column(
                    name.as_str(),
                    cte_scope.clone(),
                    crate::pipeline::asts::core::provenance::QualificationSource::Resolver,
                )
            };

            ColumnMetadata::new(
                provenance,
                TableName::Named(delightql_types::SqlIdentifier::from(cte_name)),
                None,
            )
        })
        .collect()
}

pub(in crate::pipeline::transformer_v4) fn disambiguate_aliases(
    items: Vec<SelectItem>,
) -> Vec<SelectItem> {
    use std::collections::HashSet;

    let mut used: HashSet<String> = HashSet::new();
    let mut needs_fix = false;

    // Quick scan: if no duplicates, return as-is (common case).
    for item in &items {
        if let SelectItem::Expression { expr, alias } = item {
            let effective = alias
                .as_deref()
                .or_else(|| effective_column_name(expr))
                .unwrap_or("");
            if !effective.is_empty() && !used.insert(effective.to_string()) {
                needs_fix = true;
                break;
            }
        }
    }
    if !needs_fix {
        return items;
    }

    // Slow path: rebuild with unique names.
    used.clear();
    items
        .into_iter()
        .map(|item| match item {
            SelectItem::Expression { expr, alias } => {
                let effective = alias
                    .as_deref()
                    .or_else(|| effective_column_name(&expr))
                    .unwrap_or("")
                    .to_string();
                if effective.is_empty() {
                    return SelectItem::Expression { expr, alias };
                }
                let unique = state::unique_name(&effective, &mut used);
                // Only add/change the alias if disambiguation changed the name
                // or an alias was already present.
                let new_alias = if unique != effective || alias.is_some() {
                    Some(unique)
                } else {
                    None
                };
                SelectItem::Expression {
                    expr,
                    alias: new_alias,
                }
            }
            other => other,
        })
        .collect()
}

/// Extract the column name from a bare column expression (no alias).
/// Returns `Some("col")` for `Column { name: "col", .. }`, `None` otherwise.
fn effective_column_name(expr: &DomainExpression) -> Option<&str> {
    match expr {
        DomainExpression::Column { name, .. } => Some(name.as_str()),
        _ => None,
    }
}
