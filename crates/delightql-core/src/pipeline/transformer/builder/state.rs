// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Internal state machine for the SQL builder.
//!
//! Four states with well-defined transitions. The builder's public methods
//! trigger transitions internally — callers never see this enum. Every state
//! carries the same [`Publication`], so what a state advertises and what its
//! statement emits are one value, not two that have to be kept in step.

use crate::error::Result;
use crate::pipeline::sql_ast::{
    DomainExpression, OrderTerm, QueryExpression, SelectBuilder, SelectItem, TableExpression,
};

use super::names::NameGenerator;
use super::publication::{Hygiene, Publication};

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
        scope: Publication,
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
        /// The row clause this level carries, whole. A cap with no
        /// offset, an offset with no cap, or both — the SQL AST's own shape.
        row_clause: Option<crate::pipeline::sql_ast::ordering::Limit>,
        scope: Publication,
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
        scope: Publication,
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
        scope: Publication,
    },
}

impl BuilderState {
    /// What this state publishes.
    pub(super) fn publication(&self) -> &Publication {
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

    /// Convert any state into a `QueryExpression` that publishes what the
    /// state advertises.
    ///
    /// Used by `wrap_as_subquery` to embed the current state as a FROM
    /// source. Table and Segment emit the publication's own list — no
    /// `SELECT *`, every output named — so the layer above finds in the SQL
    /// exactly the occurrences the heading claims.
    ///
    /// For the public terminal operation, see `Builder<Projected>::to_sql()`,
    /// which only exists on Projected builders (compile-time enforced).
    pub(super) fn materialize(self, names: &NameGenerator) -> Result<QueryExpression> {
        let identities = names.identities();
        match self {
            // A Table or Segment state materializes only as an intermediate
            // layer — a final statement is always a Select, set by
            // add_projection. An intermediate layer CARRIES its hygienic
            // columns: something above still stands on them (a hoisted
            // pattern constraint, a chained USING key), and its requalified
            // heading keeps them, so a list that dropped them would leave
            // the layer publishing less than it claims.
            Self::Table { table, scope } => {
                let sb = SelectBuilder::new()
                    .select_all(star_when_empty(
                        scope.select_items(identities, Hygiene::Carry),
                    ))
                    .from_tables(vec![table]);
                Ok(QueryExpression::Select(Box::new(scope.publish(sb)?)))
            }
            Self::Segment {
                from,
                filters,
                order_by,
                row_clause,
                scope,
            } => {
                let mut sb = SelectBuilder::new()
                    .select_all(star_when_empty(
                        scope.select_items(identities, Hygiene::Carry),
                    ))
                    .from_tables(from);
                for filter in filters {
                    sb = sb.and_where(filter);
                }
                for term in order_by {
                    sb = sb.order_by(term);
                }
                if let Some(clause) = row_clause {
                    sb = sb.limit_from(clause);
                }
                Ok(QueryExpression::Select(Box::new(scope.publish(sb)?)))
            }
            Self::Select { select, scope, .. } => {
                Ok(QueryExpression::Select(Box::new(scope.publish(select)?)))
            }
            Self::Frozen { query, .. } => Ok(query),
        }
    }

    /// Wrap the current state as a subquery in FROM with a generated alias.
    pub(super) fn wrap_as_subquery(self, names: &NameGenerator) -> Result<Self> {
        let origin = super::wrap_origin(
            self.publication().outputs(),
            names.identities(),
            crate::names::WrapReason::Projection,
        );
        let alias = names.fresh(origin);
        let identity = alias.identity();
        self.wrap_as_subquery_with_scope(identity, names)
    }

    fn wrap_as_subquery_with_scope(
        self,
        alias: crate::names::ScopeId,
        names: &NameGenerator,
    ) -> Result<Self> {
        let scope = self.publication().clone();
        let new_scope = scope.requalified_for_subquery(alias, names.identities())?;
        let aliases = scope.pairs_with(&new_scope);
        let mut query = self.materialize(names)?;
        rewrite_output_aliases(&mut query, alias, &aliases, names.identities())?;
        // The body under this star was just republished into `alias`, so what
        // the star stands for is that body's heading — read off the wrap, not
        // inferred later from whatever the check happens to be comparing to.
        let sb = SelectBuilder::new()
            .select_all(vec![SelectItem::star(new_scope.identities_in_order())])
            .from_subquery(query, alias);

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
                row_clause: None,
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
                row_clause,
                scope,
            } => {
                let mut sb = SelectBuilder::new().from_tables(from);
                for filter in filters {
                    sb = sb.and_where(filter);
                }
                for term in order_by {
                    sb = sb.order_by(term);
                }
                if let Some(clause) = row_clause {
                    sb = sb.limit_from(clause);
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

/// The occurrence the wrapping alias publishes for one output of its body.
///
/// A rewrite is a lookup, not a search, and the tiers are tried in order of how
/// much they assume. The output's own occurrence, if a pair carries it, is the
/// answer and cannot be ambiguous — an occurrence appears at most once as a
/// source. Only when no pair names it exactly do value and then published name
/// stand in, and those two can genuinely fail to pick: `(id as a, id as b)`
/// projects one column into two slots, so both pairs answer to the same value
/// while standing for different outputs. Sameness of value is not identity of
/// occurrence, and reaching for it first makes that pair unwrappable.
///
/// Two pairs answering to one output on a tier that cannot tell them apart is a
/// heading that cannot be republished, and an output no tier answers to is one
/// the wrapper does not carry — both refuse. Leaving such an output alone is
/// the tempting move and the wrong one: it strands an occurrence of the inner
/// scope inside a statement that now claims to produce the outer one.
fn rename_target(
    output: crate::names::ColId,
    aliases: &[(crate::names::ColId, crate::names::ColId)],
    identities: &crate::names::Registry,
) -> Result<crate::names::ColId> {
    // An occurrence appears at most once as a source, so this tier cannot be
    // ambiguous — but the pair list is a slice and does not carry that. Taking
    // the first match asserts the invariant instead of checking it, and where
    // it fails the wrapper silently renames one of two outputs to the other's
    // target. Enumerate, like the tiers below.
    let mut exact = aliases
        .iter()
        .filter(|(source, _)| *source == output)
        .map(|(_, target)| *target);
    match (exact.next(), exact.next()) {
        (Some(target), None) => return Ok(target),
        (Some(_), Some(_)) => {
            return Err(crate::error::DelightQLError::parse_error(format!(
                "subquery output {output:?} is paired more than once by the alias wrapping it"
            )))
        }
        (None, _) => {}
    }

    let mut by_value = aliases
        .iter()
        .filter(|(source, _)| identities.same_value(*source, output))
        .map(|(_, target)| *target);
    match (by_value.next(), by_value.next()) {
        (Some(target), None) => return Ok(target),
        (Some(_), Some(_)) => {
            return Err(crate::error::DelightQLError::parse_error(format!(
                "subquery output {output:?} has more than one rename target by value"
            )))
        }
        (None, _) => {}
    }

    let mut by_name = aliases
        .iter()
        .filter(|(source, _)| identities.published_sym(*source) == identities.published_sym(output))
        .map(|(_, target)| *target);
    match (by_name.next(), by_name.next()) {
        (Some(target), None) => Ok(target),
        (Some(_), Some(_)) => Err(crate::error::DelightQLError::parse_error(format!(
            "subquery output {output:?} has more than one rename target by name"
        ))),
        (None, _) => Err(crate::error::DelightQLError::parse_error(format!(
            "subquery output {output:?} answers to no column of the alias wrapping it"
        ))),
    }
}

/// Re-publish a query's outputs into `alias`, the scope of the FROM entry it
/// is about to become the body of.
///
/// Rewriting the output aliases without re-stamping the statement's result
/// scope would leave it publishing occurrences of a scope it does not claim
/// to produce, so no consumer's qualification could be checked against it.
/// The two are one act, which is why every output is resolved before any of
/// them — and before the stamp — moves: a statement half-way through this is
/// exactly the state the pairing exists to rule out.
pub(in crate::pipeline::transformer) fn rewrite_output_aliases(
    query: &mut QueryExpression,
    alias: crate::names::ScopeId,
    aliases: &[(crate::names::ColId, crate::names::ColId)],
    identities: &crate::names::Registry,
) -> Result<()> {
    match query {
        QueryExpression::Select(statement) => {
            // An item publishes an occurrence whether or not it spells one: a
            // bare column reference is output under that column's own name, so
            // it names an occurrence exactly as an aliased item does. Passing
            // over it leaves the body publishing an inner column while the
            // wrapper claims the one just minted — the disagreement this
            // function exists to prevent, arriving through the item shape
            // that spells no alias. Re-publishing it spells the alias out.
            statement
                .republish(alias, |output| {
                    rename_target(output, aliases, identities).map_err(|error| error.to_string())
                })
                .map_err(crate::error::DelightQLError::parse_error)?;
        }
        QueryExpression::SetOperation { left, right, .. } => {
            rewrite_output_aliases(left, alias, aliases, identities)?;
            rewrite_output_aliases(right, alias, aliases, identities)?;
        }
        QueryExpression::WithCte { query, .. } => {
            rewrite_output_aliases(query, alias, aliases, identities)?
        }
        QueryExpression::Values { .. } => {}
    }
    Ok(())
}

impl BuilderState {
    /// Wrap as subquery, preserving the existing scope name when meaningful.
    ///
    /// Named scopes carry through to the subquery alias (the consumer expects
    /// to qualify against them). `Fresh` has no name to preserve, so we
    /// generate one.
    pub(super) fn wrap_preserving_name(self, names: &NameGenerator) -> Result<Self> {
        let scope = self.publication().clone();
        let alias = scope.at_scope();
        let query = self.materialize(names)?;
        Ok(Self::Select {
            select: SelectBuilder::new()
                .select_all(vec![SelectItem::star(scope.identities_in_order())])
                .from_subquery(query, alias),
            has_projection: false,
            has_group_by: false,
            scope,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A statement needs a select list. A layer publishing nothing has none to
/// give, and `*` is what SQL offers instead — the one place a list is not the
/// publication's own.
fn star_when_empty(items: Vec<SelectItem>) -> Vec<SelectItem> {
    if items.is_empty() {
        vec![SelectItem::star_over_nothing()]
    } else {
        items
    }
}
