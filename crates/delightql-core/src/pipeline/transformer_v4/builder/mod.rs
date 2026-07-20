// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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
    /// The MATCHED column holds a tree (known interior schema). Carried
    /// by qualification itself so the identity question is answered
    /// once — a separate by-name lookup can select a different
    /// same-named column than the qualifier did. False on fallback
    /// paths with no column record behind them.
    pub has_interior_schema: bool,
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

    /// Whether the column this reference RESOLVES TO holds a tree (a
    /// known interior schema — a staged tree-group column). Embedding
    /// such a column into a JSON constructor must go through `json()`
    /// or the engine escapes it as TEXT.
    ///
    /// Rides the qualification road itself — never a separate by-name
    /// scan, which can select a DIFFERENT same-named column than the
    /// qualifier did and wrap a plain string in `json()`.
    fn tree_valued(&self, col_name: &str, qualifier: Option<&str>) -> bool {
        match qualifier {
            Some(q) => self
                .try_qualify_with_table(col_name, q)
                .map(|qc| qc.has_interior_schema)
                .unwrap_or(false),
            None => self
                .qualify(col_name)
                .map(|qc| qc.has_interior_schema)
                .unwrap_or(false),
        }
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

    fn scope_columns(&self) -> Vec<ColumnMetadata> {
        self.columns.clone()
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

    /// Adopt the resolver-stamped interior schemas into this scope. The
    /// transformer re-derives stage output columns structurally and cannot
    /// see tree-typedness; the resolver's CprSchema for the same stage can.
    /// Matches by name, only when unambiguous on BOTH sides — never
    /// positionally (an index zip misbinds when either side reorders), and
    /// never overwrites an interior the scope already carries.
    pub(in crate::pipeline::transformer_v4) fn adopt_interior_schemas(
        &mut self,
        cpr: &crate::pipeline::asts::resolved::CprSchema,
    ) {
        let crate::pipeline::asts::resolved::CprSchema::Resolved(cpr_cols) = cpr else {
            return;
        };
        let scope = self.state.scope_mut();
        for cc in cpr_cols {
            let Some(interior) = &cc.interior_schema else { continue };
            let name = cc.name();
            let mut hits = scope
                .columns
                .iter_mut()
                .filter(|c| c.name().eq_ignore_ascii_case(name));
            if let (Some(target), None) = (hits.next(), hits.next()) {
                if target.interior_schema.is_none() {
                    target.interior_schema = Some(interior.clone());
                }
            }
        }
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

/// What a json_each expansion iterates — the caller's structural knowledge,
/// carried into the SQL AST as an internal TVF name so dialects with
/// separate array/object TVFs can spell each form correctly.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JsonEachKind {
    /// A JSON array the transformer built (melt packets, tree_group columns).
    Array,
    /// A `JSON_GROUP_OBJECT` map (metadata tree groups, `key:~>`).
    Object,
}

impl JsonEachKind {
    fn internal_fn_name(self) -> &'static str {
        match self {
            JsonEachKind::Array => crate::pipeline::naming::INTERNAL_JSON_EACH_ARRAY,
            JsonEachKind::Object => crate::pipeline::naming::INTERNAL_JSON_EACH_OBJECT,
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
                        .filter(|c| !using_cols.iter().any(|uc| delightql_types::SqlIdentifier::str_eq(uc, col_name(c)))),
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

    /// Join two operands FULL OUTER with USING, projecting each USING
    /// column as `COALESCE(left.col, right.col)`.
    ///
    /// The merged column must carry the key of WHICHEVER side is present.
    /// A one-sided qualified projection (`left.col`) is NULL on exactly
    /// the other side's orphan rows — the rows full outer exists to keep.
    pub(in crate::pipeline::transformer_v4) fn from_join_full_outer_using(
        left: JoinOperand,
        right: JoinOperand,
        using_cols: Vec<String>,
    ) -> Result<Builder<Projected>> {
        let side_qualifier = |cols: &[ColumnMetadata], name: &str| -> Option<String> {
            cols.iter()
                .find(|c| delightql_types::SqlIdentifier::str_eq(name, col_name(c)))
                .and_then(|c| state::col_qualifier(c).map(str::to_string))
        };
        let coalesce_sides: Vec<(String, Option<String>, Option<String>)> = using_cols
            .iter()
            .map(|uc| {
                (
                    uc.clone(),
                    side_qualifier(&left.columns, uc),
                    side_qualifier(&right.columns, uc),
                )
            })
            .collect();

        let joined = Self::from_join(
            left,
            right,
            JoinType::Full,
            JoinCondition::Using(using_cols),
        );

        let scope_items = match &joined.state {
            BuilderState::Segment { scope, .. } => scope.disambiguated_select_items().0,
            _ => unreachable!("from_join returns Segment state"),
        };
        let items: Vec<SelectItem> = scope_items
            .into_iter()
            .map(|item| match item {
                SelectItem::Expression { expr, alias } => {
                    let sides = alias.as_deref().and_then(|a| {
                        coalesce_sides.iter().find(|(name, _, _)| {
                            delightql_types::SqlIdentifier::str_eq(name, a)
                        })
                    });
                    match sides {
                        Some((name, Some(lq), Some(rq))) => SelectItem::Expression {
                            expr: DomainExpression::Function {
                                name: "coalesce".to_string(),
                                args: vec![
                                    DomainExpression::with_qualifier(
                                        crate::pipeline::sql_ast_v3::ColumnQualifier::table(lq),
                                        name,
                                    ),
                                    DomainExpression::with_qualifier(
                                        crate::pipeline::sql_ast_v3::ColumnQualifier::table(rq),
                                        name,
                                    ),
                                ],
                                distinct: false,
                            },
                            alias,
                        },
                        _ => SelectItem::Expression { expr, alias },
                    }
                }
                other => other,
            })
            .collect();

        joined.add_projection(items)
    }

    /// Assemble a left-deep join from N prepared operands (from
    /// `into_join_operand`) WITHOUT wrapping intermediate joins as subqueries.
    ///
    /// `operands` must be non-empty; `conditions` holds one entry per join
    /// (`operands.len() - 1` entries), applied left-to-right:
    /// `op[0] JOIN op[1] ON conditions[0]`, then that result `JOIN op[2] ON
    /// conditions[1]`, and so on.
    ///
    /// Unlike chaining `from_join` two-at-a-time, the intermediate joins are
    /// kept as a single nested `TableExpression::Join` tree (left-associative,
    /// exactly like SQL's `a JOIN b JOIN c`). No intermediate `SELECT *`
    /// subquery wrap is introduced, so each operand keeps its own alias and the
    /// per-operand column provenance survives — the caller projects the final
    /// output explicitly, qualifying each column to the operand that owns it.
    ///
    /// Assumes operands have distinct table identities (each
    /// `into_join_operand` mints a fresh subquery alias), so no same-name
    /// collision aliasing is performed.
    pub(in crate::pipeline::transformer_v4) fn from_joins(
        operands: Vec<JoinOperand>,
        conditions: Vec<(JoinType, JoinCondition)>,
    ) -> Self {
        assert!(
            !operands.is_empty(),
            "from_joins requires at least one operand"
        );
        assert_eq!(
            conditions.len(),
            operands.len() - 1,
            "from_joins requires exactly one condition per join"
        );

        let mut iter = operands.into_iter();
        let first = iter.next().expect("non-empty checked above");
        let mut acc_table = first.table;
        let mut acc_columns = first.columns;
        let mut acc_ctes = first.ctes;
        let names = first.names;

        for (operand, (kind, condition)) in iter.zip(conditions.into_iter()) {
            acc_table = TableExpression::Join {
                left: Box::new(acc_table),
                right: Box::new(operand.table),
                join_type: kind,
                join_condition: condition,
            };
            acc_columns.extend(operand.columns);
            acc_ctes.extend(operand.ctes);
        }

        let join_scope_name = names.next_table_name("join");
        let scope = ScopeEntry::new(join_scope_name, acc_columns);
        Self {
            state: BuilderState::Segment {
                from: vec![acc_table],
                filters: Vec::new(),
                order_by: Vec::new(),
                limit_offset: None,
                scope,
            },
            names,
            accumulated_ctes: acc_ctes,
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

    /// Expand a JSON column into rows via `json_each`.
    ///
    /// Wraps the current builder as a subquery, adds a `json_each(source.column)`
    /// TVF, and builds a new SELECT from caller-provided context and interior items.
    ///
    /// The builder handles the scope split: context items inherit source provenance
    /// (for inductive chaining like `A.B(*).C(*)`), interior items get fresh
    /// provenance with `column` as table qualifier.
    ///
    /// `kind` declares what the caller KNOWS it is iterating — an array it
    /// built (narrow/drill/destructure) or a `JSON_GROUP_OBJECT` map
    /// (metadata tree groups). SQLite's `json_each` doesn't care; dialects
    /// with separate array/object TVFs render each internal name differently.
    ///
    /// Callers provide two closures that receive the SQL alias strings:
    /// - `context_items_fn(source_alias)` — passthrough items from the source
    /// - `interior_items_fn(tvf_alias)` — new items extracted from the TVF
    pub fn expand_with_json_each(
        self,
        column: &str,
        tvf_prefix: &str,
        kind: JsonEachKind,
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
            function: kind.internal_fn_name().to_string(),
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

        // INNER join json_each: a NULL or empty interior IS empty — it
        // contributes zero
        // rows to the expansion, in every form (drill, narrow, brace,
        // destructure). Interior expansion is not an outer join; a parent
        // with no children vanishes rather than surviving as a row of NULL
        // children. This also closes the round trip with tree-group
        // CONSTRUCTION, which already elides all-NULL contributor rows to
        // produce `[]`. Pinned by directive_contract/17_null_interior_is_empty
        // and the drill/narrow cardinality law sum(cardinality(r.t)).
        let joined_from = TableExpression::Join {
            left: Box::new(source_table),
            right: Box::new(je_tvf),
            join_type: JoinType::Inner,
            join_condition: JoinCondition::On(DomainExpression::literal(
                crate::pipeline::asts::core::LiteralValue::Boolean(true),
            )),
        };
        let mut sb = SelectBuilder::new()
            .set_select(all_items.clone())
            .from_tables(vec![joined_from]);
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
                        .find(|c| delightql_types::SqlIdentifier::str_eq(col_name(c), lookup))
                        .map(|c| c.info.clone())
                        .or_else(|| {
                            // Identity walk before fresh: recover a source column
                            // whose current spelling has diverged from the referenced
                            // name. Uniqueness-guarded — zero or ≥2 stack matches fall
                            // through; ambiguity is never resolved by first-match.
                            let mut historical = source_columns.iter().filter(|c| {
                                c.info
                                    .identity_stack()
                                    .iter()
                                    .any(|id| id.name == lookup)
                            });
                            match (historical.next(), historical.next()) {
                                (Some(c), None) => Some(c.info.clone()),
                                _ => None,
                            }
                        })
                        // Honest Fresh: the name matches nothing in the source
                        // scope, current or historical — no identity to inherit.
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
        order_by: Vec<(
            DomainExpression,
            crate::pipeline::sql_ast_v3::ordering::OrderDirection,
        )>,
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
/// Tier 1: spelling match (ASCII case-insensitive, per the equality authority)
/// Tier 2: identity stack walk (renamed by disambiguation)
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
            has_interior_schema: false,
        });
    }

    // Tier 1: spelling match (ASCII case-insensitive per the equality authority).
    let matches: Vec<_> = columns
        .iter()
        .filter(|c| delightql_types::SqlIdentifier::str_eq(col_name(c), col_name_str))
        .collect();

    match matches.len() {
        1 => Ok(QualifiedColumn {
            name: col_name(matches[0]).to_string(),
            qualifier: col_qualifier(matches[0]).map(|s| s.to_string()),
            has_interior_schema: matches[0].interior_schema.is_some(),
        }),
        0 => {
            // Tier 2: identity stack walk.
            let historical: Vec<_> = columns.iter().filter(|c| {
                c.info.identity_stack().iter().any(|id| {
                    id.name == col_name_str
                })
            }).collect();
            match historical.len() {
                1 => Ok(QualifiedColumn {
                    name: col_name(historical[0]).to_string(),
                    qualifier: col_qualifier(historical[0]).map(|s| s.to_string()),
                    has_interior_schema: historical[0].interior_schema.is_some(),
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
    let found = columns.iter().find(|c| {
        delightql_types::SqlIdentifier::str_eq(col_name(c), col_name_str)
            && delightql_types::SqlIdentifier::opt_str_eq(col_qualifier(c), Some(table))
    });

    if let Some(col) = found {
        return Some(QualifiedColumn {
            name: col_name(col).to_string(),
            qualifier: col_qualifier(col).map(|s| s.to_string()),
            has_interior_schema: col.interior_schema.is_some(),
        });
    }

    // Tier 2: identity stack walk. Uniqueness-guarded like every other
    // stack walk in this module — a self-join carries the same original
    // (table, name) at the bottom of BOTH sides' stacks, and ambiguity
    // is never resolved by first-match; zero or ≥2 matches fall through.
    let mut historical = columns.iter().filter(|c| {
        c.info.identity_stack().iter().any(|id| {
            id.name == col_name_str
                && match &id.table_qualifier {
                    TableName::Named(s) => *s == table,
                    TableName::Fresh => false,
                }
        })
    });

    if let (Some(col), None) = (historical.next(), historical.next()) {
        return Some(QualifiedColumn {
            name: col_name(col).to_string(),
            qualifier: col_qualifier(col).map(|s| s.to_string()),
            has_interior_schema: col.interior_schema.is_some(),
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
            delightql_types::SqlIdentifier::str_eq(col_name(c), col_name_str)
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
                has_interior_schema: col.interior_schema.is_some(),
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
                let mut passthrough_source = None;
                let provenance = if let Some(m) = find_input_column(&expr, input_columns) {
                    let mut prov = m.col.info.clone();
                    match m.derived_via {
                        None => {
                            // Direct column reference. If the output name differs
                            // from the input name (e.g., disambiguation renamed
                            // "id" → "id_2"), push a scope transition so the stack
                            // records the rename.
                            if !delightql_types::SqlIdentifier::str_eq(col_name(m.col), name.as_str()) {
                                push_scope_transition_on_provenance(
                                    &mut prov,
                                    name.as_str(),
                                    scope_name,
                                );
                            }
                            // A passthrough carries the SAME value, so every
                            // value fact rides along (declared type,
                            // nullability, interior heading). Derived values
                            // (the Some arm) get honest unknowns — a cast/
                            // function output is not the source value.
                            passthrough_source = Some(m.col);
                        }
                        Some(via) => {
                            // The value changed (cast/function/arithmetic). The
                            // stack records a derivation ALWAYS — even when the
                            // output name equals the source, the underlying column
                            // is no longer usable as-is.
                            push_derived_on_provenance(
                                &mut prov,
                                name.as_str(),
                                col_name(m.col),
                                via,
                                scope_name,
                            );
                        }
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
                let out_col = match passthrough_source {
                    Some(src) => {
                        ColumnMetadata::carrying(src, provenance, TableName::Fresh, None)
                    }
                    None => ColumnMetadata::new(provenance, TableName::Fresh, None),
                };
                columns.push(out_col);
                out_items.push(SelectItem::Expression {
                    expr,
                    alias: final_alias,
                });
            }
        }
    }
    (out_items, columns)
}

/// The single distinct column reference feeding a value-transforming
/// expression, if there is exactly one. Opaque subtrees (raw SQL,
/// subqueries, window functions, stars) poison the answer to None —
/// they can reference columns invisibly.
fn single_source_column(expr: &DomainExpression) -> Option<(&str, Option<&str>)> {
    use delightql_types::SqlIdentifier;

    let mut refs: Vec<(&str, Option<&str>)> = Vec::new();
    // None from the walk = a poisoning (opaque) subtree.
    collect_source_columns(expr, &mut refs)?;

    // Distinctness is by identifier value, not spelling: SqlIdentifier's
    // Eq/Hash already fold ASCII case (STRING-FLOOR Tier 3 — no ad hoc
    // case ops at the site).
    let mut seen: Vec<(SqlIdentifier, Option<SqlIdentifier>)> = Vec::new();
    let mut unique: Option<(&str, Option<&str>)> = None;
    for (name, qual) in refs {
        let key = (SqlIdentifier::from(name), qual.map(SqlIdentifier::from));
        if !seen.contains(&key) {
            seen.push(key);
            unique = Some((name, qual));
        }
    }
    match seen.len() {
        1 => unique,
        _ => None,
    }
}

/// Walk `expr`, pushing every column reference into `out`. Returns None the
/// moment an opaque subtree is hit (it may reference columns the walk cannot
/// see). Exhaustive by construction: a future `DomainExpression` variant must
/// decide here whether it is transparent, recursive, or poisoning.
fn collect_source_columns<'a>(
    expr: &'a DomainExpression,
    out: &mut Vec<(&'a str, Option<&'a str>)>,
) -> Option<()> {
    match expr {
        DomainExpression::Column { name, qualifier } => {
            out.push((name.as_str(), qualifier.as_ref().map(|q| q.table_name())));
            Some(())
        }
        DomainExpression::Literal(_) => Some(()),
        DomainExpression::Cast { expr, .. }
        | DomainExpression::Unary { expr, .. }
        | DomainExpression::Parens(expr) => collect_source_columns(expr, out),
        DomainExpression::Binary { left, right, .. } => {
            collect_source_columns(left, out)?;
            collect_source_columns(right, out)
        }
        DomainExpression::Function { args, .. } => {
            for arg in args {
                collect_source_columns(arg, out)?;
            }
            Some(())
        }
        DomainExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } => {
            if let Some(scrutinee) = expr {
                collect_source_columns(scrutinee, out)?;
            }
            for clause in when_clauses {
                collect_source_columns(clause.when(), out)?;
                collect_source_columns(clause.then(), out)?;
            }
            if let Some(else_expr) = else_clause {
                collect_source_columns(else_expr, out)?;
            }
            Some(())
        }
        DomainExpression::InList { expr, values, .. } => {
            collect_source_columns(expr, out)?;
            for value in values {
                collect_source_columns(value, out)?;
            }
            Some(())
        }
        DomainExpression::Tuple(items) => {
            for item in items {
                collect_source_columns(item, out)?;
            }
            Some(())
        }
        // Opaque: these can reference columns the walk cannot enumerate, so a
        // single-source claim over them would be unsound. Poison to None.
        DomainExpression::Star
        | DomainExpression::RawSql(_)
        | DomainExpression::Subquery(_)
        | DomainExpression::InSubquery { .. }
        | DomainExpression::Exists { .. }
        | DomainExpression::WindowFunction { .. }
        | DomainExpression::PredicateRewrite { .. } => None,
    }
}

/// A matched input column, plus how the output derives from it.
struct InputColumnMatch<'a> {
    col: &'a ColumnMetadata,
    /// Set when the match came through a value-transforming expression
    /// (single-source rule) rather than a direct column reference.
    derived_via: Option<String>,
}

/// Match a `(name, qualifier)` reference against the input columns.
///
/// Tier 1: current (name, qualifier) spelling match.
/// Tier 2: identity stack walk — recover a column whose current spelling has
///         diverged from the referenced (historical) name. Same matching
///         semantics as `try_qualify_with_table_in_columns` Tier 2. Guarded:
///         zero or ≥2 matches return None so ambiguity is never resolved by
///         first-match.
fn find_by_name_and_qual<'a>(
    name: &str,
    qual: Option<&str>,
    input_columns: &'a [ColumnMetadata],
) -> Option<&'a ColumnMetadata> {
    // Tier 1. With a qualifier the (name, qual) pair is unique
    // post-disambiguation; without one, name-only matching over a
    // multi-source scope must be uniqueness-guarded — an ambiguous
    // identity is no identity (the caller's Honest-Fresh fallback is
    // the correct answer, not the first column encountered).
    if qual.is_some() {
        if let Some(col) = input_columns.iter().find(|c| {
            delightql_types::SqlIdentifier::str_eq(col_name(c), name)
                && delightql_types::SqlIdentifier::opt_str_eq(col_qualifier(c), qual)
        }) {
            return Some(col);
        }
    } else {
        let mut hits = input_columns
            .iter()
            .filter(|c| delightql_types::SqlIdentifier::str_eq(col_name(c), name));
        if let (Some(col), None) = (hits.next(), hits.next()) {
            return Some(col);
        }
    }

    // Tier 2.
    let mut historical = input_columns.iter().filter(|c| {
        c.info.identity_stack().iter().any(|id| {
            id.name == name
                && match qual {
                    Some(q) => {
                        matches!(&id.table_qualifier, TableName::Named(s) if *s == q)
                    }
                    None => true,
                }
        })
    });
    match (historical.next(), historical.next()) {
        (Some(col), None) => Some(col),
        _ => None,
    }
}

/// Find the input column matching a SQL column expression.
///
/// For simple column references (`qualifier.name`), returns the matching input
/// column so its identity stack can be inherited by the output column.
///
/// Parens are unwrapped first (notation, not value transformation). A direct
/// column reference inherits verbatim (`derived_via: None`). A value-
/// transforming expression inherits iff exactly one distinct source column
/// feeds it (STRING-FLOOR cast-lineage ruling), reported with the transform's
/// diagnostic spelling in `derived_via`.
fn find_input_column<'a>(
    expr: &DomainExpression,
    input_columns: &'a [ColumnMetadata],
) -> Option<InputColumnMatch<'a>> {
    // Parens are notation, not value transformation — see through them.
    if let DomainExpression::Parens(inner) = expr {
        return find_input_column(inner, input_columns);
    }

    // Direct column reference: inherit the stack as a synonym, not a derivation.
    if let DomainExpression::Column {
        name: expr_name,
        qualifier,
    } = expr
    {
        let expr_qual = qualifier.as_ref().map(|q| q.table_name());
        return find_by_name_and_qual(expr_name.as_str(), expr_qual, input_columns)
            .map(|col| InputColumnMatch {
                col,
                derived_via: None,
            });
    }

    // Value-transforming expression: name-lineage extends iff exactly one
    // distinct source column feeds it; opaque subtrees poison to None.
    let (src_name, src_qual) = single_source_column(expr)?;
    let col = find_by_name_and_qual(src_name, src_qual, input_columns)?;
    // `via` is diagnostic only (never rendered) and reflects the top-level
    // transform kind — parens are already unwrapped above.
    let via = match expr {
        DomainExpression::Cast { .. } => "cast".to_string(),
        DomainExpression::Function { name, .. } => name.clone(),
        DomainExpression::Unary { op, .. } => format!("{:?}", op),
        DomainExpression::Binary { op, .. } => format!("{:?}", op),
        DomainExpression::Case { .. } => "case".to_string(),
        DomainExpression::InList { .. } => "in".to_string(),
        DomainExpression::Tuple(_) => "tuple".to_string(),
        _ => "derived".to_string(),
    };
    Some(InputColumnMatch {
        col,
        derived_via: Some(via),
    })
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

/// Push a Derived identity — the output column came through a value-
/// transforming expression fed by a single source column (`previous_name`).
/// `via` is the transform's diagnostic spelling; it is never rendered.
fn push_derived_on_provenance(
    prov: &mut crate::pipeline::asts::core::provenance::ColumnProvenance,
    name: &str,
    previous_name: &str,
    via: String,
    scope: &TableName,
) {
    use crate::pipeline::asts::core::provenance::{
        ColumnIdentity, IdentityContext, TransformationPhase,
    };
    prov.push_identity(ColumnIdentity {
        name: delightql_types::SqlIdentifier::from(name),
        context: IdentityContext::Derived {
            previous_name: previous_name.to_string(),
            via,
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

#[cfg(test)]
mod find_input_column_tests {
    use super::*;
    use crate::pipeline::asts::core::provenance::{
        ColumnIdentity, ColumnProvenance, IdentityContext, QualificationSource,
        TransformationPhase,
    };
    use crate::pipeline::ast_refined::LiteralValue;
    use crate::pipeline::sql_ast_v3::{BinaryOperator, ColumnQualifier};

    /// Column whose current spelling is `current` but whose stack carries a
    /// historical `(hist_qual, hist_name)` entry underneath.
    fn renamed_col(current: &str, hist_name: &str, hist_qual: &str) -> ColumnMetadata {
        let mut prov = ColumnProvenance::from_table_column(
            hist_name,
            TableName::Named(hist_qual.into()),
            QualificationSource::Resolver,
        );
        prov.push_identity(ColumnIdentity {
            name: delightql_types::SqlIdentifier::from(current),
            context: IdentityContext::PipeBarrier {
                previous_table: TableName::Fresh,
                fresh_scope: 0,
            },
            phase: TransformationPhase::Transformer,
            table_qualifier: TableName::Fresh,
        });
        ColumnMetadata::new(prov, TableName::Fresh, None)
    }

    /// Column whose current spelling equals its original table name.
    fn simple_col(name: &str, qual: &str) -> ColumnMetadata {
        let prov = ColumnProvenance::from_table_column(
            name,
            TableName::Named(qual.into()),
            QualificationSource::Resolver,
        );
        ColumnMetadata::new(prov, TableName::Named(qual.into()), None)
    }

    fn col_expr(name: &str, qualifier: Option<&str>) -> DomainExpression {
        DomainExpression::Column {
            name: name.to_string(),
            qualifier: qualifier.map(ColumnQualifier::table),
        }
    }

    fn cast_expr(inner: DomainExpression) -> DomainExpression {
        DomainExpression::Cast {
            expr: Box::new(inner),
            type_name: "text".to_string(),
        }
    }

    fn add_expr(left: DomainExpression, right: DomainExpression) -> DomainExpression {
        DomainExpression::Binary {
            left: Box::new(left),
            op: BinaryOperator::Add,
            right: Box::new(right),
        }
    }

    fn func_expr(name: &str, args: Vec<DomainExpression>) -> DomainExpression {
        DomainExpression::Function {
            name: name.to_string(),
            args,
            distinct: false,
        }
    }

    #[test]
    fn tier2_recovers_renamed_column() {
        let cols = vec![renamed_col("id_2", "id", "o")];
        let expr = col_expr("id", Some("o"));
        let found = find_input_column(&expr, &cols).expect("stack match");
        assert_eq!(col_name(found.col), "id_2");
        assert!(found.derived_via.is_none());
    }

    #[test]
    fn tier2_unqualified_historical() {
        let cols = vec![renamed_col("id_2", "id", "o")];
        let expr = col_expr("id", None);
        let found = find_input_column(&expr, &cols).expect("unique stack match");
        assert_eq!(col_name(found.col), "id_2");
    }

    #[test]
    fn tier2_ambiguity_returns_none() {
        let cols = vec![
            renamed_col("id_2", "id", "o"),
            renamed_col("id_3", "id", "c"),
        ];
        let expr = col_expr("id", None);
        assert!(find_input_column(&expr, &cols).is_none());
    }

    #[test]
    fn tier1_still_wins() {
        let cols = vec![simple_col("id", "o"), renamed_col("id_2", "id", "c")];
        let expr = col_expr("id", None);
        let found = find_input_column(&expr, &cols).expect("current-name match");
        assert_eq!(col_name(found.col), "id");
    }

    #[test]
    fn parens_unwrap() {
        let cols = vec![simple_col("id", "o")];
        let expr = DomainExpression::Parens(Box::new(col_expr("id", None)));
        let found = find_input_column(&expr, &cols).expect("unwrapped parens match");
        assert_eq!(col_name(found.col), "id");
    }

    #[test]
    fn derived_cast_inherits() {
        let cols = vec![simple_col("id", "o")];
        let expr = cast_expr(col_expr("id", Some("o")));
        let found = find_input_column(&expr, &cols).expect("single-source cast match");
        assert_eq!(col_name(found.col), "id");
        assert_eq!(found.derived_via.as_deref(), Some("cast"));
    }

    #[test]
    fn derived_two_sources_none() {
        let cols = vec![simple_col("a", "o"), simple_col("b", "o")];
        let expr = add_expr(col_expr("a", None), col_expr("b", None));
        assert!(find_input_column(&expr, &cols).is_none());
    }

    #[test]
    fn derived_same_column_twice_ok() {
        let cols = vec![simple_col("a", "o")];
        let expr = add_expr(col_expr("a", None), col_expr("a", None));
        let found = find_input_column(&expr, &cols).expect("one distinct source");
        assert_eq!(col_name(found.col), "a");
        assert!(found.derived_via.is_some());
    }

    #[test]
    fn derived_poisoned_none() {
        let cols = vec![simple_col("id", "o")];
        let expr = func_expr(
            "coalesce",
            vec![col_expr("id", None), DomainExpression::RawSql("x".to_string())],
        );
        assert!(find_input_column(&expr, &cols).is_none());
    }

    #[test]
    fn derived_through_rename() {
        // Cast of the historical name recovers the renamed column via Tier 2,
        // reported through the derived path.
        let cols = vec![renamed_col("id_2", "id", "o")];
        let expr = cast_expr(col_expr("id", Some("o")));
        let found = find_input_column(&expr, &cols).expect("Tier-2 recovery through derive");
        assert_eq!(col_name(found.col), "id_2");
        assert_eq!(found.derived_via.as_deref(), Some("cast"));
    }

    #[test]
    fn derived_literal_only_none() {
        let cols = vec![simple_col("id", "o")];
        let expr = func_expr(
            "abs",
            vec![DomainExpression::Literal(LiteralValue::Number("1".to_string()))],
        );
        assert!(find_input_column(&expr, &cols).is_none());
    }
}
