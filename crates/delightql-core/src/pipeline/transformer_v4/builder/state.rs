//! Internal state machine for the SQL builder.
//!
//! Four states with well-defined transitions. The builder's public methods
//! trigger transitions internally — callers never see this enum.
//!
//! This is where `prepare_source_builder` goes to die. One implementation,
//! one place, all transitions auditable.

use std::collections::HashSet;

use crate::error::Result;
use crate::pipeline::asts::core::provenance::{
    ColumnIdentity, IdentityContext, TransformationPhase,
};
use crate::pipeline::asts::core::{ColumnMetadata, TableName};
use crate::pipeline::sql_ast_v3::{
    ColumnQualifier, DomainExpression, OrderTerm, QueryExpression, SelectBuilder, SelectItem,
    TableExpression,
};
use delightql_types::SqlIdentifier;

use super::names::NameGenerator;

// ---------------------------------------------------------------------------
// Column helpers — read current name/qualifier from ColumnMetadata
// ---------------------------------------------------------------------------

/// Read the current (top-of-stack) column name from a `ColumnMetadata`.
pub(in crate::pipeline::transformer_v4) fn col_name(col: &ColumnMetadata) -> &str {
    col.info.name().unwrap_or("_unnamed")
}

/// Read the current qualifier from a `ColumnMetadata`'s identity stack.
///
/// Returns the table_qualifier from the top of the identity stack, converted
/// to a string. Returns `None` for `TableName::Fresh` (unqualified).
pub(in crate::pipeline::transformer_v4) fn col_qualifier(col: &ColumnMetadata) -> Option<&str> {
    col.info.current_table_qualifier().and_then(|tq| match tq {
        TableName::Named(s) => Some(s.as_str()),
        TableName::Fresh => None,
    })
}

/// Push a scope transition onto a `ColumnMetadata`'s identity stack.
///
/// This is the "concentrated how" of naming: every scope change in the builder
/// goes through this function. It pushes a new `ColumnIdentity` with the
/// column's current name (or a new disambiguated name) and the new scope's
/// `TableName` as qualifier.
pub(in crate::pipeline::transformer_v4) fn push_scope_transition(
    col: &mut ColumnMetadata,
    new_name: Option<&str>,
    new_scope: &TableName,
    context: IdentityContext,
) {
    let name = match new_name {
        Some(n) => SqlIdentifier::from(n),
        None => SqlIdentifier::from(col_name(col)),
    };
    col.info.push_identity(ColumnIdentity {
        name,
        context,
        phase: TransformationPhase::Transformer,
        table_qualifier: new_scope.clone(),
    });
}

// ---------------------------------------------------------------------------
// Scope helpers
// ---------------------------------------------------------------------------

/// The SQL-visible name string for a `TableName`.
///
/// Returns `"_anon"` for `Fresh` — anonymous tables without aliases
/// may reach this when used as CTE sources or tree-group inputs.
pub(in crate::pipeline::transformer_v4) fn table_name_sql(name: &TableName) -> &str {
    match name {
        TableName::Named(s) => s.as_str(),
        TableName::Fresh => "_anon",
    }
}

/// String representation of a `TableName` for matching purposes (qualify lookups).
///
/// Returns `None` for `Fresh` (no qualifier needed in SQL).
pub(in crate::pipeline::transformer_v4) fn table_name_str(name: &TableName) -> Option<&str> {
    match name {
        TableName::Named(s) => Some(s.as_str()),
        TableName::Fresh => None,
    }
}

// ---------------------------------------------------------------------------
// Scope types
// ---------------------------------------------------------------------------

/// The builder's scope: what name it publishes and what columns it advertises.
///
/// Every `BuilderState` variant carries a `ScopeEntry`. Every state transition
/// preserves or updates the scope. The scope is the builder's identity contract
/// with its parent: "I am called X and I have columns Y."
///
/// Columns are `ColumnMetadata` — the same structure used by the resolver and
/// refiner. The identity stack (`ColumnProvenance`) flows through unchanged;
/// the builder pushes new `ColumnIdentity` entries for scope transitions,
/// disambiguation, and wrapping.
#[derive(Debug, Clone)]
pub(super) struct ScopeEntry {
    /// The published name for this scope level.
    /// A parent uses this as the subquery alias or CTE reference when
    /// composing this builder into a larger query.
    pub(super) name: TableName,
    /// All columns available at this scope level.
    pub(super) columns: Vec<ColumnMetadata>,
}

impl ScopeEntry {
    pub(super) fn new(name: TableName, columns: Vec<ColumnMetadata>) -> Self {
        Self { name, columns }
    }

    /// Build explicit SELECT items from the scope columns, disambiguating
    /// duplicates with numeric suffixes. Returns the items AND the
    /// disambiguated column names (for building the requalified scope).
    ///
    /// E.g., if the scope has `u.id` and `o.id`, the output is:
    ///   items: [u.id AS id, ..., o.id AS id_2, ...]
    ///   names: ["id", ..., "id_2", ...]
    pub(super) fn disambiguated_select_items(&self) -> (Vec<SelectItem>, Vec<String>) {
        let mut used: HashSet<String> = HashSet::new();
        let mut items = Vec::with_capacity(self.columns.len());
        let mut names = Vec::with_capacity(self.columns.len());

        for col in &self.columns {
            if col.needs_hygienic_alias {
                continue;
            }
            let name = col_name(col);
            let disambiguated = unique_name(name, &mut used);

            let expr = match col_qualifier(col) {
                Some(q) => DomainExpression::with_qualifier(ColumnQualifier::table(q), name),
                None => DomainExpression::column(name),
            };
            items.push(SelectItem::Expression {
                expr,
                alias: Some(disambiguated.clone()),
            });
            names.push(disambiguated);
        }

        (items, names)
    }

    /// Create a new ScopeEntry with the given name, requalifying all columns
    /// and disambiguating duplicate names.
    ///
    /// This is the lossless wrapping operation: the old identity is preserved
    /// in each column's `ColumnProvenance` identity stack (via push), and
    /// duplicate names get numeric suffixes so every column has a unique name
    /// in the new scope.
    pub(super) fn requalified_with_disambiguation(&self, new_name: TableName) -> Self {
        let mut used: HashSet<String> = HashSet::new();
        let columns = self
            .columns
            .iter()
            .map(|c| {
                let name = col_name(c);
                let disambiguated = unique_name(name, &mut used);

                let mut new_col = c.clone();
                let new_col_name = if disambiguated != name {
                    Some(disambiguated.as_str())
                } else {
                    None
                };
                push_scope_transition(
                    &mut new_col,
                    new_col_name,
                    &new_name,
                    IdentityContext::PipeBarrier {
                        previous_table: TableName::Fresh,
                        fresh_scope: 0,
                    },
                );
                new_col
            })
            .collect();
        Self {
            name: new_name,
            columns,
        }
    }

    /// Simple requalification without disambiguation.
    ///
    /// Use this only when duplicate names are impossible (e.g., single-table
    /// scope, or post-disambiguation scope). For wrapping joins, use
    /// `requalified_with_disambiguation()`.
    pub(super) fn requalified(&self, new_name: TableName) -> Self {
        let columns = self
            .columns
            .iter()
            .map(|c| {
                let mut new_col = c.clone();
                push_scope_transition(
                    &mut new_col,
                    None,
                    &new_name,
                    IdentityContext::PipeBarrier {
                        previous_table: TableName::Fresh,
                        fresh_scope: 0,
                    },
                );
                new_col
            })
            .collect();
        Self {
            name: new_name,
            columns,
        }
    }
}

// ---------------------------------------------------------------------------
// State machine
// ---------------------------------------------------------------------------

/// The four states of the SQL builder.
///
/// ```text
///               ┌──────────┐
///   from_table  │          │  add_where       ┌───────────┐
///   ──────────▶ │  Table   │ ───────────────▶ │  Segment  │
///               │          │                  │           │
///               └──────────┘                  └───────────┘
///                     │                            │
///                     │ add_projection             │ add_projection
///                     │ add_group_by               │ finalize
///                     ▼                            ▼
///               ┌──────────┐              ┌──────────────┐
///               │          │              │              │
///               │  Select  │              │    Select    │
///               │          │              │  (from seg)  │
///               └──────────┘              └──────────────┘
///                     │                         │
///                     │ push_cte                │
///                     ▼                         ▼
///               ┌──────────┐
///               │          │  push_subquery / wrap
///               │  Frozen  │ ──────────────────────▶ back to Select
///               │          │
///               └──────────┘
///                     │
///                     │ to_sql
///                     ▼
///               QueryExpression (final output)
/// ```
pub(super) enum BuilderState {
    /// Bare table reference. Nothing added yet.
    ///
    /// Transitions:
    /// - `add_where` → Segment
    /// - `add_projection` / `add_group_by` → Select
    /// - `join` → Segment (both tables in FROM)
    Table {
        table: TableExpression,
        scope: ScopeEntry,
    },

    /// Flat accumulation: FROM + WHERE + ORDER BY + LIMIT, no SELECT list.
    /// Supports flat joins (multiple tables in FROM via join tree).
    ///
    /// Transitions:
    /// - `add_where` → Segment (append filter)
    /// - `join` → Segment (extend FROM with join)
    /// - `add_projection` / `add_group_by` → Select (finalize)
    Segment {
        from: Vec<TableExpression>,
        filters: Vec<DomainExpression>,
        order_by: Vec<OrderTerm>,
        limit_offset: Option<(i64, Option<i64>)>,
        scope: ScopeEntry,
    },

    /// SELECT statement being assembled via SelectBuilder.
    ///
    /// Transitions:
    /// - `add_where` (no GROUP BY) → Select (AND to existing WHERE)
    /// - `add_where` (has GROUP BY) → wrap as subquery, then WHERE
    /// - `add_projection` → wrap as subquery, new Select
    /// - `add_order_by` / `add_limit` → Select
    /// - `push_subquery` → Select (wrap in new FROM)
    /// - `push_cte` → Frozen
    /// - `to_sql` → finalize
    Select {
        select: SelectBuilder,
        /// Whether the select list has been explicitly set (not just `*`).
        has_projection: bool,
        /// Whether GROUP BY has been set.
        has_group_by: bool,
        scope: ScopeEntry,
    },

    /// Frozen query expression. Further operations require wrapping.
    ///
    /// Transitions:
    /// - `add_where` / `add_projection` → wrap as subquery, Select
    /// - `push_subquery` → Select
    /// - `push_cte` → Frozen (add CTE to accumulated list)
    /// - `join` → wrap as subquery, Segment
    /// - `to_sql` → emit (with accumulated CTEs if any)
    Frozen {
        query: QueryExpression,
        scope: ScopeEntry,
    },
}

impl BuilderState {
    /// Read-only access to the scope.
    pub(super) fn scope(&self) -> &ScopeEntry {
        match self {
            Self::Table { scope, .. }
            | Self::Segment { scope, .. }
            | Self::Select { scope, .. }
            | Self::Frozen { scope, .. } => scope,
        }
    }

    // -----------------------------------------------------------------------
    // State transitions
    // -----------------------------------------------------------------------

    /// Internal: convert any state into a `QueryExpression` with explicit
    /// columns from the scope.
    ///
    /// Used by `wrap_as_subquery` to embed the current state as a FROM source.
    /// Table and Segment use the scope's `disambiguated_select_items()` to
    /// produce a lossless inner SELECT — no `SELECT *`, every column named
    /// explicitly, duplicates disambiguated.
    ///
    /// For the public terminal operation, see `Builder<Projected>::to_sql()`,
    /// which only exists on Projected builders (compile-time enforced).
    pub(super) fn materialize(self) -> Result<QueryExpression> {
        match self {
            Self::Table { table, scope } => {
                let (items, _names) = scope.disambiguated_select_items();
                let select_items = if items.is_empty() {
                    vec![SelectItem::star()]
                } else {
                    items
                };
                let stmt = SelectBuilder::new()
                    .select_all(select_items)
                    .from_tables(vec![table])
                    .build()
                    .map_err(|e| build_err(&e))?;
                Ok(QueryExpression::Select(Box::new(stmt)))
            }
            Self::Segment {
                from,
                filters,
                order_by,
                limit_offset,
                scope,
            } => {
                let (items, _names) = scope.disambiguated_select_items();
                let select_items = if items.is_empty() {
                    vec![SelectItem::star()]
                } else {
                    items
                };
                let mut sb = SelectBuilder::new()
                    .select_all(select_items)
                    .from_tables(from);
                for filter in filters {
                    sb = sb.and_where(filter);
                }
                for term in order_by {
                    sb = sb.order_by(term);
                }
                if let Some((count, offset)) = limit_offset {
                    match offset {
                        Some(off) => sb = sb.limit_offset(count, off),
                        None => sb = sb.limit(count),
                    }
                }
                let stmt = sb.build().map_err(|e| build_err(&e))?;
                Ok(QueryExpression::Select(Box::new(stmt)))
            }
            Self::Select { select, .. } => {
                let stmt = select.build().map_err(|e| build_err(&e))?;
                Ok(QueryExpression::Select(Box::new(stmt)))
            }
            Self::Frozen { query, .. } => Ok(query),
        }
    }

    /// Wrap the current state as a subquery in FROM with a generated alias.
    pub(super) fn wrap_as_subquery(self, names: &NameGenerator) -> Result<Self> {
        let alias = names.next_table_name("t");
        self.wrap_as_subquery_named(alias, names)
    }

    /// Wrap the current state as a subquery in FROM with a specific alias.
    ///
    /// The inner SELECT uses explicit, disambiguated columns from the scope.
    /// The outer scope is requalified with disambiguation so every column
    /// has a unique name. Prior identities are preserved in each column's
    /// `ColumnProvenance` identity stack.
    pub(super) fn wrap_as_subquery_named(
        self,
        alias: TableName,
        _names: &NameGenerator,
    ) -> Result<Self> {
        let scope = self.scope().clone();
        let mut query = self.materialize()?;
        let alias_str = table_name_sql(&alias).to_string();

        // SQLite ultra-reserved column names: even quoted as `"true"`, SQLite
        // parses `t_1."true"` as a boolean after the dot. Fix by renaming
        // them in the inner query's SELECT aliases BEFORE wrapping.
        let needs_stropping = scope
            .columns
            .iter()
            .any(|c| is_sqlite_ultra_reserved(col_name(c)));
        if needs_stropping {
            strop_query_aliases(&mut query);
        }

        // Build scope with potentially-stropped names.
        let stropped_scope = if needs_stropping {
            let mut s = scope.clone();
            for c in &mut s.columns {
                let name = col_name(c).to_string();
                if is_sqlite_ultra_reserved(&name) {
                    let safe = format!("_{}", name);
                    push_scope_transition(
                        c,
                        Some(safe.as_str()),
                        &s.name,
                        IdentityContext::PipeBarrier {
                            previous_table: TableName::Fresh,
                            fresh_scope: 0,
                        },
                    );
                }
            }
            s
        } else {
            scope
        };

        let new_scope = stropped_scope.requalified_with_disambiguation(alias);
        let sb = SelectBuilder::new()
            .select_all(vec![SelectItem::star()])
            .from_subquery(query, alias_str);

        Ok(Self::Select {
            select: sb,
            has_projection: false,
            has_group_by: false,
            scope: new_scope,
        })
    }

    /// Transition into a state where WHERE can be added.
    ///
    /// - Table → Segment (start accumulating filters)
    /// - Segment → Segment (no change, just append)
    /// - Select (no GROUP BY) → Select (AND to WHERE)
    /// - Select (has GROUP BY) → wrap as subquery, then filterable
    /// - Frozen → wrap as subquery, then filterable
    pub(super) fn ensure_filterable(self, names: &NameGenerator) -> Result<Self> {
        match self {
            Self::Table { table, scope } => Ok(Self::Segment {
                from: vec![table],
                filters: Vec::new(),
                order_by: Vec::new(),
                limit_offset: None,
                scope,
            }),
            Self::Segment { .. } => Ok(self),
            Self::Select { has_group_by, .. } if !has_group_by => Ok(self),
            Self::Select { .. } => self.wrap_as_subquery(names),
            Self::Frozen { .. } => self.wrap_preserving_name(names),
        }
    }

    /// Transition into a state where a SELECT list can be set.
    ///
    /// - Table → start building SELECT FROM table
    /// - Segment → finalize segment into SELECT
    /// - Select (already has projection) → wrap as subquery first
    /// - Select (no projection yet) → use directly
    /// - Frozen → wrap as subquery
    pub(super) fn ensure_projectable(self, names: &NameGenerator) -> Result<Self> {
        match self {
            Self::Table { table, scope } => {
                let sb = SelectBuilder::new().from_tables(vec![table]);
                Ok(Self::Select {
                    select: sb,
                    has_projection: false,
                    has_group_by: false,
                    scope,
                })
            }
            Self::Segment {
                from,
                filters,
                order_by,
                limit_offset,
                scope,
            } => {
                let mut sb = SelectBuilder::new().from_tables(from);
                for filter in filters {
                    sb = sb.and_where(filter);
                }
                for term in order_by {
                    sb = sb.order_by(term);
                }
                if let Some((count, offset)) = limit_offset {
                    match offset {
                        Some(off) => sb = sb.limit_offset(count, off),
                        None => sb = sb.limit(count),
                    }
                }
                Ok(Self::Select {
                    select: sb,
                    has_projection: false,
                    has_group_by: false,
                    scope,
                })
            }
            Self::Select {
                has_projection: true,
                ..
            } => self.wrap_as_subquery(names),
            Self::Select {
                has_projection: false,
                ..
            } => Ok(self),
            Self::Frozen { .. } => self.wrap_preserving_name(names),
        }
    }
}

impl BuilderState {
    /// Wrap as subquery, preserving the existing scope name when meaningful.
    ///
    /// Named scopes carry through to the subquery alias (the consumer expects
    /// to qualify against them). `Fresh` has no name to preserve, so we
    /// generate one.
    pub(super) fn wrap_preserving_name(self, names: &NameGenerator) -> Result<Self> {
        let existing = self.scope().name.clone();
        match existing {
            TableName::Fresh => self.wrap_as_subquery(names),
            named => self.wrap_as_subquery_named(named, names),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn build_err(msg: &str) -> crate::error::DelightQLError {
    crate::error::DelightQLError::ParseError {
        message: format!("Builder state finalization: {}", msg),
        source: None,
        subcategory: None,
    }
}

/// Pick a unique name for `base` given the set of already-used names.
/// First occurrence keeps the base name. Subsequent occurrences get `_2`, `_3`,
/// etc. — but if the candidate itself is already used (e.g. `id_2` exists as a
/// prior disambiguation), keep incrementing until a truly unused name is found.
pub(in crate::pipeline::transformer_v4) fn unique_name(
    base: &str,
    used: &mut HashSet<String>,
) -> String {
    if used.insert(base.to_string()) {
        return base.to_string();
    }
    let mut suffix = 2;
    loop {
        let candidate = format!("{}_{}", base, suffix);
        if used.insert(candidate.clone()) {
            return candidate;
        }
        suffix += 1;
    }
}

/// Column names that SQLite parses as literals even when double-quoted
/// after a dot (`t_1."true"` → boolean 1, not column reference).
fn is_sqlite_ultra_reserved(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "true" | "false" | "null"
    )
}

/// Rename ultra-reserved column aliases in a materialized query's SELECT list.
/// e.g. `6 AS "null"` → `6 AS "_null"`.
fn strop_query_aliases(query: &mut QueryExpression) {
    match query {
        QueryExpression::Select(stmt) => {
            for item in stmt.select_list_mut() {
                if let SelectItem::Expression { alias: Some(a), .. } = item {
                    if is_sqlite_ultra_reserved(a) {
                        *a = format!("_{}", a);
                    }
                }
            }
        }
        QueryExpression::SetOperation { left, right, .. } => {
            strop_query_aliases(left);
            strop_query_aliases(right);
        }
        QueryExpression::WithCte { query: inner, .. } => {
            strop_query_aliases(inner);
        }
        QueryExpression::Values { .. } => {}
    }
}
