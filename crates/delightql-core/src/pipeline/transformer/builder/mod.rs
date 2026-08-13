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
pub(in crate::pipeline) mod publication;
pub(in crate::pipeline::transformer) mod state;

use std::marker::PhantomData;

use crate::error::Result;
use crate::pipeline::asts::core::ColumnMetadata;
use crate::pipeline::sql_ast::{
    Cte, DomainExpression, JoinCondition, JoinType, OrderTerm, QueryExpression, SelectBuilder,
    SelectItem, SqlPredicate, TableExpression, TvfArgument,
};

// ---------------------------------------------------------------------------
// QualifiedColumn — what `Qualify` returns
// ---------------------------------------------------------------------------


pub(in crate::pipeline) use names::NameGenerator;
pub(in crate::pipeline::transformer) use names::ScopeName;
pub(in crate::pipeline) use publication::{
    correlation_carriers, publish_at, Alignment, Hygiene, Publication,
};
use state::BuilderState;

pub(super) fn wrap_origin(
    columns: &[ColumnMetadata],
    identities: &crate::names::Registry,
    why: crate::names::WrapReason,
) -> crate::names::ScopeOrigin {
    ColumnMetadata::common_identity_scope(columns, identities)
        .map(|input| crate::names::ScopeOrigin::Wrap { input, why })
        .unwrap_or(crate::names::ScopeOrigin::AnonRelation)
}

/// The GROUP BY clause is the key items' expressions — the clause groups by
/// what the keys select, never by the names they publish under.
fn group_by_expressions(keys: &[SelectItem]) -> Vec<DomainExpression> {
    keys.iter()
        .filter_map(|item| match item {
            SelectItem::Expression { expr, .. } => Some(expr.clone()),
            _ => None,
        })
        .collect()
}

pub(super) fn pipe_origin(
    columns: &[ColumnMetadata],
    identities: &crate::names::Registry,
) -> crate::names::ScopeOrigin {
    ColumnMetadata::common_identity_scope(columns, identities)
        .map(|input| crate::names::ScopeOrigin::PipeStage { input })
        .unwrap_or(crate::names::ScopeOrigin::AnonRelation)
}

fn cte_origin(
    columns: &[ColumnMetadata],
    identities: &crate::names::Registry,
    role: crate::names::CteRole,
) -> crate::names::ScopeOrigin {
    ColumnMetadata::common_identity_scope(columns, identities)
        .map(|input| crate::names::ScopeOrigin::Cte { input, role })
        .unwrap_or(crate::names::ScopeOrigin::AnonRelation)
}

/// Stand a CTE body at a scope of its own, re-aliasing its select list into it.
///
/// A body assembled from more than one source — grouping keys carried over
/// from the input's heading beside aggregates minted for the body — has no
/// scope that owns all of it, so asking which scope its outputs share answers
/// only by accident. It mints the scope it stands at instead, and re-aliases
/// the list in the same act: stating what a statement outputs and aliasing the
/// list that outputs it are not separable, and doing one without the other
/// leaves the body publishing a heading its stated scope does not own.
///
/// Returns the scope to build at and, in select-list order, what it publishes.
pub(in crate::pipeline::transformer) fn stand_cte_body_at(
    items: &mut [SelectItem],
    input: crate::names::ScopeId,
    why: crate::names::WrapReason,
    identities: &crate::names::Registry,
) -> Result<(crate::names::ScopeId, Vec<crate::names::ColId>)> {
    let at = identities.mint_scope(
        crate::names::ScopeOrigin::Wrap { input, why },
        crate::names::Hint::None,
        None,
    );
    let mut outputs = Vec::with_capacity(items.len());
    for item in items {
        let SelectItem::Expression {
            alias: Some(alias), ..
        } = item
        else {
            return Err(crate::error::DelightQLError::parse_error(
                "a CTE body has an output it does not name",
            ));
        };
        let published = identities.republish_column(
            *alias,
            at,
            crate::names::Republish::BoundaryExport,
            identities.published(*alias),
            identities.addressing(*alias),
            |_| {},
        );
        *alias = published;
        outputs.push(published);
    }
    crate::probe::probing!(tree, {
        crate::probe::probe!(tree, "cte body {at:?} over {input:?} {why:?}");
        for output in &outputs {
            crate::probe::probe!(
                tree,
                "  publishes {output:?} {:?}",
                crate::probe::chain(identities, *output)
            );
        }
    });
    Ok((at, outputs))
}

pub(super) fn remint_heading(
    columns: Vec<ColumnMetadata>,
    identities: &crate::names::Registry,
    into: crate::names::ScopeId,
    how: crate::names::Republish,
) -> Vec<ColumnMetadata> {
    crate::probe::probe!(
        heading,
        "remint into {into:?} {how:?} from {:?}\n{}",
        columns
            .iter()
            .map(ColumnMetadata::identity)
            .collect::<Vec<_>>(),
        std::backtrace::Backtrace::force_capture()
    );
    columns
        .into_iter()
        .map(|column| {
            let source = column.identity();
            let identity = identities.republish_column(
                source,
                into,
                how,
                identities.published(source),
                identities.addressing(source),
                |_| {},
            );
            ColumnMetadata::new(identity)
        })
        .collect()
}

/// Follow an occurrence owned by a join back to the arm that publishes it.
///
/// A join names a heading but never an SQL alias — `FROM a JOIN b` offers `a`
/// and `b` and nothing else — so a reference qualified by the join scope names
/// a table no statement contains. The heading is still real, and a select list
/// goes on *publishing* under it; only the reading side comes back down here.
/// Both halves of a select item pass through: the expression reads the arm,
/// the alias keeps the join's occurrence, and that split is what lets two
/// arms' `id` be told apart in the output while staying unambiguous in the
/// input.
///
/// The walk is a chain, not a search: an occurrence has one source, so there
/// is nothing to choose between.
///
/// Each step lands in one of that join's own two operands, or the walk stops.
/// A chain step is not by itself a road out of a join: an occurrence may
/// republish something from outside the statement entirely — an interior
/// expression carrying the segment occurrence it computes, say — and following
/// that names a table this FROM does not offer. The operands are what the FROM
/// establishes, so they are the only place to land.
///
/// It lands only on an occurrence its own scope publishes. An arm that renames
/// or unifies on the way out — a positional pattern is both — holds a source
/// column it never outputs, and reading through to that one names a column the
/// arm does not have. Where the walk cannot land, the join's own occurrence
/// stands, which is what it did before any of this.
pub(super) fn read_through_joins(
    identities: &crate::names::Registry,
    column: crate::names::ColId,
) -> crate::names::ColId {
    let mut cur = column;
    while let crate::names::ScopeOrigin::Join { left, right } =
        identities.origin_of(identities.scope_of(cur))
    {
        let crate::names::ColumnOrigin::Republished { from, .. } = identities.origin_of_col(cur)
        else {
            return column;
        };
        let landed = identities.scope_of(from);
        if landed != left && landed != right {
            return column;
        }
        cur = from;
    }
    if identities
        .heading(identities.scope_of(cur))
        .columns_seen()
        .iter()
        .any(|column| *column == cur)
    {
        cur
    } else {
        column
    }
}

/// Republish a finished query's heading under the scope that is about to name
/// it, and rewrite the query to output what it now claims.
///
/// Minting a scope over a body and rewriting that body are one act. Half of it
/// leaves the alias claiming a heading the statement does not output — and
/// nothing in the SQL text shows it, because the spelling under the new
/// occurrence is the spelling under the old one. Every wrap goes through here
/// so that half is not reachable.
pub(in crate::pipeline::transformer) fn republish_under(
    query: &mut QueryExpression,
    scope: crate::names::ScopeId,
    columns: &[ColumnMetadata],
    identities: &crate::names::Registry,
    how: crate::names::Republish,
) -> Result<Vec<ColumnMetadata>> {
    let republished = remint_heading(columns.to_vec(), identities, scope, how);
    let aliases: Vec<_> = columns
        .iter()
        .zip(&republished)
        .map(|(source, target)| (source.identity(), target.identity()))
        .collect();
    state::rewrite_output_aliases(query, scope, &aliases, identities)?;
    Ok(republished)
}

/// Re-anchor a select list onto the scope it will actually sit on.
///
/// A caller lowers its items against the scope the builder had when it handed
/// them over. Settling into a projectable state can put a subquery boundary
/// underneath first, and the boundary republishes the whole heading — so every
/// occurrence the items name is one the body no longer outputs, and the SELECT
/// above it qualifies by a scope that is now two levels down. The republication
/// chain is the road back, the same one every other reference takes.
///
/// Only a bare column moves. An item computing something has already been
/// lowered against the pre-wrap scope in full, and re-anchoring one occurrence
/// inside it would leave the rest behind.
fn reanchor_select_items(items: Vec<SelectItem>, qualify: &dyn Qualify) -> Result<Vec<SelectItem>> {
    // Every column reference in an item's expression re-anchors, not just
    // the bare-column shape: a cover wraps its target in a function or a
    // CASE, and a reference left inside the wrapper keeps naming the
    // occurrence it was lowered against — one no emitted FROM entry may
    // publish. Aliases stay: they are the resolver's output stamps.
    // Subquery interiors keep their own qualification road (map_columns
    // does not enter them), and a correlated outer reference passes
    // through rebind untouched.
    let failure = std::cell::RefCell::new(None);
    let items = items
        .into_iter()
        .map(|item| match item {
            SelectItem::Expression { expr, alias } => SelectItem::Expression {
                expr: expr.map_columns(&|column| match qualify.rebind(column) {
                    Ok(landed) => landed,
                    Err(error) => {
                        failure.borrow_mut().get_or_insert(error);
                        column
                    }
                }),
                alias,
            },
            other => other,
        })
        .collect();
    match failure.into_inner() {
        Some(error) => Err(error),
        None => Ok(items),
    }
}

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
    fn identities(&self) -> &crate::names::Registry;


    /// Snapshot this scope's columns for use as an outer scope.
    ///
    /// Called at scalar subquery entry points to capture the enclosing
    /// scope into `TransformCtx.outer_columns`. Default: empty (no columns
    /// to contribute — appropriate for DummyQualify, ChainedQualify, etc.).
    fn scope_columns(&self) -> Vec<ColumnMetadata> {
        vec![]
    }

    /// Re-anchor a resolved reference onto the occurrence this scope publishes
    /// for it.
    ///
    /// A reference is addressed at resolution against the scope its operator
    /// published then. Every boundary crossed since — a subquery wrapper, a
    /// join operand — republishes that heading under fresh occurrences, and the
    /// reference then names one no emitted statement carries. The republication
    /// chain is the road back whenever there is one: a candidate stands for the
    /// reference when the reference is on its chain.
    ///
    /// It is not the only road. The resolver republishes a heading into the
    /// scope it minted for a segment, and the transformer republishes the same
    /// heading across the boundary it had to insert — two routes from one
    /// source, so the reference and the candidate come out *siblings*, with
    /// neither on the other's chain. `rebind_by_value` is that case, and it is
    /// bounded the same way the chain tier is: one candidate or none.
    ///
    /// A reference this scope does not publish — a correlated outer column —
    /// has no candidate here and passes through untouched.
    ///
    /// One reference re-anchors onto one occurrence. A scope publishing the
    /// reference twice — the same column selected into two slots — offers two
    /// candidates with equal claim, and picking the earlier one silently
    /// decides which slot the reference meant. Refuse instead: the reference
    /// is genuinely ambiguous here, and only its author can say which slot.
    fn rebind(&self, column: crate::names::ColId) -> Result<crate::names::ColId> {
        let columns = self.scope_columns();
        crate::probe::probing!(rebind, {
            let ids = self.identities();
            crate::probe::probe!(rebind, "ref {:?}", crate::probe::chain(ids, column));
            for candidate in &columns {
                let candidate = candidate.identity();
                crate::probe::probe!(
                    rebind,
                    "  cand {:?} republishes={} same_value={}",
                    crate::probe::chain(ids, candidate),
                    ids.republishes(candidate, column),
                    ids.same_value(candidate, column)
                );
            }
        });
        let landed = if columns
            .iter()
            .any(|candidate| candidate.identity() == column)
        {
            column
        } else {
            let mut candidates = columns
                .iter()
                .map(ColumnMetadata::identity)
                .filter(|candidate| self.identities().republishes(*candidate, column));
            match (candidates.next(), candidates.next()) {
                (Some(candidate), None) => candidate,
                (None, _) => match self.rebind_by_value(column, &columns)? {
                    Some(candidate) => candidate,
                    None => self.rebind_across_joins(column, &columns)?,
                },
                (Some(_), Some(_)) => {
                    return Err(crate::error::DelightQLError::parse_error(format!(
                        "{column:?} is published more than once here, so a reference to it names \
                         no single column"
                    )))
                }
            }
        };
        Ok(self.read_through_joins(landed))
    }

    /// Answer a reference with the one candidate carrying its value.
    ///
    /// The sibling case: a boundary the transformer inserted republishes the
    /// same heading the resolver republished into the segment's own scope, so
    /// the reference and the candidate meet at a shared source rather than on
    /// one chain. Value identity is what relates them.
    ///
    /// Three outcomes, and they are three: no candidate carries the value, one
    /// does, or more than one does.
    ///
    /// `Ok(None)` is "nothing here answers", which hands the question to the
    /// next tier. More than one is not that — a scope carrying the value twice
    /// (the same column selected into two slots) offers two with equal claim,
    /// and choosing between them would decide which slot the reference meant.
    /// Refusing is the whole reason the tier is bounded, so it refuses rather
    /// than declining: declining would fall to a tier that answers a different
    /// question and, failing that, leave the reference standing at a scope no
    /// FROM entry establishes — the ambiguity emitted instead of reported.
    fn rebind_by_value(
        &self,
        column: crate::names::ColId,
        columns: &[ColumnMetadata],
    ) -> Result<Option<crate::names::ColId>> {
        let mut carrying = columns
            .iter()
            .map(ColumnMetadata::identity)
            .filter(|candidate| self.identities().same_value(*candidate, column));
        match (carrying.next(), carrying.next()) {
            (Some(candidate), None) => Ok(Some(candidate)),
            (None, _) => Ok(None),
            (Some(_), Some(_)) => Err(crate::error::DelightQLError::parse_error(format!(
                "{column:?} names a value this scope publishes more than once, so a \
                 reference to it names no single column"
            ))),
        }
    }

    /// Answer a reference addressed at one join scope with the occurrence a
    /// second join scope publishes for the same value.
    ///
    /// A join publishes a heading and carries no SQL alias, so two join scopes
    /// standing over the same operands are indistinguishable downstream: the
    /// headings hold the same values, and neither is on the other's
    /// republication chain. They are siblings, and the chain test declines
    /// correctly. Value identity decides between them instead — and only where
    /// exactly one occurrence here carries the value, since a heading
    /// publishing it twice offers two answers with equal claim.
    ///
    /// Confined to a join on both sides. Value identity alone is too loose to
    /// re-anchor on: two aliases of one table publish the same values, so a
    /// correlated outer reference would be captured by whichever local
    /// occurrence happened to stand for the same column.
    fn rebind_across_joins(
        &self,
        column: crate::names::ColId,
        columns: &[ColumnMetadata],
    ) -> Result<crate::names::ColId> {
        let identities = self.identities();
        let joined = |c: crate::names::ColId| {
            matches!(
                identities.origin_of(identities.scope_of(c)),
                crate::names::ScopeOrigin::Join { .. }
            )
        };
        if !joined(column) {
            return Ok(column);
        }
        let mut candidates = columns
            .iter()
            .map(ColumnMetadata::identity)
            .filter(|candidate| joined(*candidate) && identities.same_value(*candidate, column));
        match (candidates.next(), candidates.next()) {
            (Some(candidate), None) => Ok(candidate),
            (None, _) => Ok(column),
            // Equal claim is not the same as no claim. Passing the reference
            // through leaves `read_through_joins` to walk the sibling it was
            // already addressed at and land on that arm — an answer, arrived at
            // by declining to choose. Only the author knows which was meant.
            (Some(_), Some(_)) => Err(crate::error::DelightQLError::parse_error(format!(
                "{column:?} names a value this join publishes more than once, so a \
                 reference to it stands for no single column"
            ))),
        }
    }

    fn read_through_joins(&self, column: crate::names::ColId) -> crate::names::ColId {
        read_through_joins(self.identities(), column)
    }

    fn tree_valued(&self, column: crate::names::ColId) -> bool {
        self.identities().facts(column).interior.is_some()
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
    scope: crate::names::ScopeId,
    /// The columns available from the previous step.
    columns: Vec<ColumnMetadata>,
    identities: std::rc::Rc<crate::names::Registry>,
}

impl CteInput {
    pub(super) fn new(
        scope: crate::names::ScopeId,
        columns: Vec<ColumnMetadata>,
        identities: std::rc::Rc<crate::names::Registry>,
    ) -> Self {
        Self {
            scope,
            columns,
            identities,
        }
    }

    /// The scope name for the input (useful for FROM references).
    pub fn scope(&self) -> crate::names::ScopeId {
        self.scope
    }
}

impl Qualify for CteInput {
    fn identities(&self) -> &crate::names::Registry {
        &self.identities
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
    pub output_columns: Vec<crate::names::ColId>,
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
    identities: std::rc::Rc<crate::names::Registry>,
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
    pub(in crate::pipeline::transformer) fn names(&self) -> &NameGenerator {
        &self.names
    }

    pub(in crate::pipeline::transformer) fn identities(
        &self,
    ) -> &std::rc::Rc<crate::names::Registry> {
        &self.identities
    }

    /// Read-only access to the scope's columns.
    pub(in crate::pipeline::transformer) fn columns(&self) -> &[ColumnMetadata] {
        self.state.publication().outputs()
    }

    /// What this builder publishes.
    pub(in crate::pipeline::transformer) fn publication(&self) -> &Publication {
        self.state.publication()
    }

    /// Internal: change the phase marker without changing any runtime state.
    /// Used by methods that perform a compile-time phase transition.
    fn rephase<Q>(self) -> Builder<Q> {
        Builder {
            state: self.state,
            names: self.names,
            identities: self.identities,
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
    fn intrinsic(self) -> crate::names::Intrinsic {
        match self {
            JsonEachKind::Array => crate::names::Intrinsic::JsonEachArray,
            JsonEachKind::Object => crate::names::Intrinsic::JsonEachObject,
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
    pub(in crate::pipeline::transformer) fn from_table(
        table: TableExpression,
        scope_name: ScopeName,
        columns: Vec<ColumnMetadata>,
        names: NameGenerator,
        identities: std::rc::Rc<crate::names::Registry>,
    ) -> Result<Self> {
        let scope = scope_name.into_scope();
        Ok(Self {
            state: BuilderState::Table {
                table,
                scope: Publication::at(scope, columns, &identities)?,
            },
            names,
            identities,
            accumulated_ctes: Vec::new(),
            _phase: PhantomData,
        })
    }

    /// Create a builder from a pre-built `QueryExpression` (Frozen state).
    ///
    /// Used for anonymous tables and other constructs that produce SQL directly
    /// (e.g., `SELECT 1 AS x UNION ALL SELECT 2`). The query is already final;
    /// further operations (filter, projection) will wrap it as a subquery.
    pub(in crate::pipeline::transformer) fn from_frozen(
        mut query: QueryExpression,
        scope_name: ScopeName,
        columns: Vec<ColumnMetadata>,
        names: NameGenerator,
        identities: std::rc::Rc<crate::names::Registry>,
    ) -> Result<Self> {
        // When the entry remints the heading (the given columns live in
        // another scope), the frozen body still publishes the given
        // occurrences. A layer's outputs and its heading are one
        // publication act: rewrite the body's aliases so the query emits
        // exactly the occurrences the heading claims.
        let at = scope_name.into_scope();
        let columns = if ColumnMetadata::common_identity_scope(&columns, &identities) == Some(at) {
            columns
        } else {
            let reminted = remint_heading(
                columns.clone(),
                &identities,
                at,
                crate::names::Republish::Passthrough,
            );
            let renames: Vec<_> = columns
                .iter()
                .zip(&reminted)
                .filter(|(source, target)| source.identity() != target.identity())
                .map(|(source, target)| (source.identity(), target.identity()))
                .collect();
            state::rewrite_output_aliases(&mut query, at, &renames, &identities)?;
            reminted
        };
        let scope = Publication::at(at, columns, &identities)?;
        Ok(Self {
            state: BuilderState::Frozen { query, scope },
            names,
            identities,
            accumulated_ctes: Vec::new(),
            _phase: PhantomData,
        })
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
    pub(in crate::pipeline::transformer) fn into_join_operand(self) -> Result<JoinOperand> {
        let (table, scope) = self.state.into_table_expr(&self.names)?;
        Ok(JoinOperand {
            table,
            columns: scope.outputs().to_vec(),
            names: self.names,
            identities: self.identities,
            ctes: self.accumulated_ctes,
        })
    }

    /// Assemble a join from prepared operands (from `into_join_operand`).
    ///
    /// For USING joins, right-side columns named in the USING list are excluded
    /// from the scope — SQL merges those columns automatically, and including
    /// them would produce duplicates in the SELECT list.
    pub(in crate::pipeline::transformer) fn from_join(
        left: JoinOperand,
        mut right: JoinOperand,
        kind: JoinType,
        condition: JoinCondition,
    ) -> Result<Self> {
        if let (Some(left_scope), Some(right_scope)) =
            (effective_scope(&left.table), effective_scope(&right.table))
        {
            if left_scope == right_scope {
                let fresh_scope = left
                    .names
                    .fresh(crate::names::ScopeOrigin::UserAlias { of: right_scope })
                    .identity();
                right.table = TableExpression::Scope(fresh_scope);
                right.columns = Publication::at(right_scope, right.columns, &left.identities)?
                    .requalified(
                        fresh_scope,
                        &left.identities,
                        crate::names::Republish::Rename,
                    )?
                    .outputs()
                    .to_vec();
            }
        }
        let left_origin_scope =
            ColumnMetadata::common_identity_scope(&left.columns, &left.identities);
        let right_origin_scope =
            ColumnMetadata::common_identity_scope(&right.columns, &left.identities);
        let join_expr = TableExpression::Join {
            left: Box::new(left.table),
            right: Box::new(right.table),
            join_type: kind,
            join_condition: condition.clone(),
        };
        let mut columns = left.columns;
        // For USING joins, SQL merges the USING columns — they appear once
        // in a star expansion (from the left side). The right side's are
        // held aside here and rejoin the heading below as HYGIENIC
        // occurrences: never emitted by a star, never answering a name,
        // but still on the republication chain, because a reference the
        // resolver bound to the right arm has to reach the occurrence the
        // right alias still publishes — dropping it entirely leaves that
        // reference standing at a scope no FROM entry establishes.
        let mut merged_right: Vec<ColumnMetadata> = Vec::new();
        match &condition {
            JoinCondition::Using(using_cols) => {
                for column in right.columns {
                    let merged = using_cols.iter().any(|using| {
                        left.identities.published_sym(*using)
                            == left.identities.published_sym(column.identity())
                    });
                    if merged {
                        merged_right.push(column);
                    } else {
                        columns.push(column);
                    }
                }
            }
            _ => {
                columns.extend(right.columns);
            }
        }
        let names = left.names;
        let identities = left.identities;
        let mut ctes = left.ctes;
        ctes.extend(right.ctes);
        let join_origin = match (left_origin_scope, right_origin_scope) {
            (Some(left), Some(right)) => crate::names::ScopeOrigin::Join { left, right },
            _ => crate::names::ScopeOrigin::AnonRelation,
        };
        let join_scope_name = names.fresh(join_origin);
        let mut columns = remint_heading(
            columns,
            &identities,
            join_scope_name.identity(),
            crate::names::Republish::JoinArm,
        );
        for column in merged_right {
            let source = column.identity();
            let identity = identities.republish_column(
                source,
                join_scope_name.identity(),
                crate::names::Republish::JoinArm,
                identities.published(source),
                crate::names::Addressing::Hygienic,
                |_| {},
            );
            columns.push(ColumnMetadata::new(identity));
        }
        let scope = Publication::at(join_scope_name.identity(), columns, &identities)?;
        Ok(Self {
            state: BuilderState::Segment {
                from: vec![join_expr],
                filters: Vec::new(),
                order_by: Vec::new(),
                row_clause: None,
                scope,
            },
            names,
            identities,
            accumulated_ctes: ctes,
            _phase: PhantomData,
        })
    }

    /// Join two operands FULL OUTER with USING, projecting each USING
    /// column as `COALESCE(left.col, right.col)`.
    ///
    /// The merged column must carry the key of WHICHEVER side is present.
    /// A one-sided qualified projection (`left.col`) is NULL on exactly
    /// the other side's orphan rows — the rows full outer exists to keep.
    pub(in crate::pipeline::transformer) fn from_join_full_outer_using(
        left: JoinOperand,
        right: JoinOperand,
        using_cols: Vec<crate::names::ColId>,
    ) -> Result<Builder<Projected>> {
        let identities = std::rc::Rc::clone(&left.identities);
        let coalesce_sides: Vec<_> = using_cols
            .iter()
            .filter_map(|using| {
                let published = identities.published_sym(*using)?;
                let left_hits: Vec<_> = left
                    .columns
                    .iter()
                    .filter(|column| identities.published_sym(column.identity()) == Some(published))
                    .collect();
                let right_hits: Vec<_> = right
                    .columns
                    .iter()
                    .filter(|column| identities.published_sym(column.identity()) == Some(published))
                    .collect();
                match (left_hits.as_slice(), right_hits.as_slice()) {
                    ([left], [right]) => Some((published, left.identity(), right.identity())),
                    _ => None,
                }
            })
            .collect();

        let joined = Self::from_join(
            left,
            right,
            JoinType::Full,
            JoinCondition::Using(using_cols),
        )?;

        let scope_items = joined
            .publication()
            .select_items(&joined.identities, Hygiene::Drop);
        let items: Vec<SelectItem> = scope_items
            .into_iter()
            .map(|item| match item {
                SelectItem::Expression { expr, alias } => {
                    let sides = alias.and_then(|column| {
                        let published = identities.published_sym(column)?;
                        coalesce_sides
                            .iter()
                            .find(|(name, _, _)| *name == published)
                    });
                    match sides {
                        Some((_, left, right)) => SelectItem::Expression {
                            expr: DomainExpression::Function {
                                name: "coalesce".into(),
                                args: vec![
                                    DomainExpression::Column(*left),
                                    DomainExpression::Column(*right),
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
    pub(in crate::pipeline::transformer) fn from_joins(
        operands: Vec<JoinOperand>,
        conditions: Vec<(JoinType, JoinCondition)>,
    ) -> Result<Self> {
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
        let identities = first.identities;

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

        let join_origin = pipe_origin(&acc_columns, &identities);
        let join_scope_name = names.fresh(join_origin);
        // The heading stays on the operand occurrences: a flat join's FROM
        // carries the operand aliases and nothing else, so a heading
        // reminted into the join scope would be published by no alias any
        // reference can render against. The scope id stamps the segment; it
        // owns no columns, and the publication says so.
        let scope =
            Publication::over_operands(join_scope_name.identity(), acc_columns, &identities)?;
        Ok(Self {
            state: BuilderState::Segment {
                from: vec![acc_table],
                filters: Vec::new(),
                order_by: Vec::new(),
                row_clause: None,
                scope,
            },
            names,
            identities,
            accumulated_ctes: acc_ctes,
            _phase: PhantomData,
        })
    }

    /// Set the SELECT list and publish the scope the resolver already bound
    /// this projection to. Transitions Unprojected → Projected.
    ///
    /// Which occurrences a projection publishes is decided at resolution, and
    /// every reference downstream of the operator is addressed against them.
    /// Minting a second set here would leave those references owned by a scope
    /// no emitted statement carries, so the statement would be built out of
    /// occurrences its own consumers cannot see.
    ///
    /// Each item must already be aliased with an occurrence of `scope`, in
    /// heading order; the caller establishes that before taking this road and
    /// falls back to `add_projection` when it does not hold.
    pub fn add_projection_publishing(
        mut self,
        items: Vec<SelectItem>,
        scope: crate::names::ScopeId,
        columns: Vec<ColumnMetadata>,
    ) -> Result<Builder<Projected>> {
        self.state = self.state.ensure_projectable(&self.names)?;
        let items = reanchor_select_items(items, &self)?;
        match self.state {
            BuilderState::Select {
                select,
                has_group_by,
                ..
            } => Ok(Builder {
                state: BuilderState::Select {
                    select: select.set_select(items),
                    has_projection: true,
                    has_group_by,
                    scope: Publication::at(scope, columns, &self.identities)?,
                },
                names: self.names,
                identities: self.identities,
                accumulated_ctes: self.accumulated_ctes,
                _phase: PhantomData,
            }),
            _ => unreachable!("ensure_projectable guarantees Select state"),
        }
    }

    /// Set the SELECT list, minting a fresh scope for what it publishes.
    /// Transitions Unprojected → Projected.
    ///
    /// For projections the resolver did not bind — the ones the transformer
    /// synthesises. When a resolved scope is in hand, use
    /// `add_projection_publishing`.
    ///
    /// The items name columns; what those columns are CALLED is settled once,
    /// by baptism, over the finished bundle. Two items competing for one
    /// spelling are an ambiguity, and an ambiguity poisons both sides — so
    /// there is no first survivor to hand the plain name to here.
    pub fn add_projection(mut self, items: Vec<SelectItem>) -> Result<Builder<Projected>> {
        self.state = self.state.ensure_projectable(&self.names)?;
        let items = reanchor_select_items(items, &self)?;
        match self.state {
            BuilderState::Select {
                select,
                has_group_by,
                scope: ref input_scope,
                ..
            } => {
                let input_columns = input_scope.outputs().to_vec();
                // Set the projection and generate a new scope name.
                let origin = wrap_origin(
                    &input_columns,
                    &self.identities,
                    crate::names::WrapReason::Projection,
                );
                let fresh_scope = self.names.fresh(origin);
                let identity_scope = fresh_scope.identity();
                // Atomic: derive columns AND write aliases back to items.
                let (items, output_columns) = derive_columns_from_items(
                    items,
                    identity_scope,
                    &input_columns,
                    &self.identities,
                    None,
                    None,
                );
                let select = select.set_select(items);
                Ok(Builder {
                    state: BuilderState::Select {
                        select,
                        has_projection: true,
                        has_group_by,
                        scope: Publication::at(identity_scope, output_columns, &self.identities)?,
                    },
                    names: self.names,
                    identities: self.identities,
                    accumulated_ctes: self.accumulated_ctes,
                    _phase: PhantomData,
                })
            }
            _ => unreachable!("ensure_projectable guarantees Select state"),
        }
    }

    /// Set GROUP BY and publish the scope the resolver already bound this
    /// reducing segment to. Transitions Unprojected → Projected.
    ///
    /// The reducing counterpart of `add_projection_publishing`, and it exists
    /// for the same reason: a reduction's output occurrences are decided at
    /// resolution, and a fresh set minted here answers to none of the
    /// references downstream that were addressed against them.
    pub fn add_group_by_publishing(
        mut self,
        spec: GroupBySpec,
        scope: crate::names::ScopeId,
        columns: Vec<ColumnMetadata>,
    ) -> Result<Builder<Projected>> {
        self.state = self.state.ensure_projectable(&self.names)?;
        match self.state {
            BuilderState::Select { select, .. } => {
                let group_exprs = group_by_expressions(&spec.keys);
                let mut select_items = spec.keys;
                select_items.extend(spec.aggregates);

                let mut select = select.set_select(select_items);
                if !group_exprs.is_empty() {
                    select = select.group_by(group_exprs);
                }

                Ok(Builder {
                    state: BuilderState::Select {
                        select,
                        has_projection: true,
                        has_group_by: true,
                        scope: Publication::at(scope, columns, &self.identities)?,
                    },
                    names: self.names,
                    identities: self.identities,
                    accumulated_ctes: self.accumulated_ctes,
                    _phase: PhantomData,
                })
            }
            _ => unreachable!("ensure_projectable guarantees Select state"),
        }
    }

    /// Set GROUP BY with keys and aggregate reductions, minting a fresh output
    /// scope. Transitions Unprojected → Projected.
    ///
    /// For reductions the resolver did not bind. When a resolved scope is in
    /// hand, use `add_group_by_publishing`.
    pub fn add_group_by(mut self, spec: GroupBySpec) -> Result<Builder<Projected>> {
        self.state = self.state.ensure_projectable(&self.names)?;
        match self.state {
            BuilderState::Select {
                select,
                scope: ref input_scope,
                ..
            } => {
                let input_columns = input_scope.outputs().to_vec();

                let group_exprs = group_by_expressions(&spec.keys);

                // SELECT list = keys ++ aggregates
                let aggregate_from = spec.keys.len();
                let mut select_items = spec.keys;
                select_items.extend(spec.aggregates);

                // Atomic: derive columns AND write aliases back to items.
                let origin = wrap_origin(
                    &input_columns,
                    &self.identities,
                    crate::names::WrapReason::Aggregate,
                );
                let fresh_scope = self.names.fresh(origin);
                let identity_scope = fresh_scope.identity();
                let (select_items, output_columns) = derive_columns_from_items(
                    select_items,
                    identity_scope,
                    &input_columns,
                    &self.identities,
                    Some(aggregate_from),
                    None,
                );

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
                        scope: Publication::at(identity_scope, output_columns, &self.identities)?,
                    },
                    names: self.names,
                    identities: self.identities,
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
    /// Every item carries the COLUMN it publishes, not a spelling. Two
    /// columns wanting one name — `u.id` and `o.id` after a join — are an
    /// ambiguity that baptism arbitrates for the whole bundle at once, and
    /// arbitrating it here instead would decide the same question twice and
    /// leave the scope and the emitted SQL free to disagree.
    pub fn project_all(self) -> Result<Builder<Projected>> {
        self.project_all_with(Hygiene::Drop)
    }

    /// Project every column, keeping the hygienic ones.
    ///
    /// For an intermediate wrap that something above still stands on. The
    /// heading and the emitted list must agree either way: dropping a column
    /// from the list while the caller's heading keeps it is what leaves an
    /// expansion naming a source no FROM entry offers.
    pub fn project_all_carrying_hygiene(self) -> Result<Builder<Projected>> {
        self.project_all_with(Hygiene::Carry)
    }

    fn project_all_with(mut self, hygiene: Hygiene) -> Result<Builder<Projected>> {
        self.state = self.state.ensure_projectable(&self.names)?;
        match &mut self.state {
            BuilderState::Select { select, scope, .. } => {
                let items = scope.select_items(&self.identities, hygiene);
                let items = if items.is_empty() {
                    vec![SelectItem::star_over_nothing()]
                } else {
                    items
                };
                let taken = std::mem::replace(select, SelectBuilder::new());
                *select = taken.set_select(items);

                // Hygienic columns were kept for qualify (e.g. a filter
                // referencing _label_0) and no caller addresses them in the
                // output. The view and the list are pruned together or not at
                // all: a heading that keeps what the list dropped is the
                // disagreement this whole species is made of.
                if hygiene == Hygiene::Drop {
                    scope.prune_hygienic(&self.identities);
                }
            }
            _ => unreachable!("ensure_projectable guarantees Select"),
        }
        Ok(self.rephase())
    }

    /// The star list `project_all` would set, with the state made projectable
    /// and the list handed back instead of applied.
    ///
    /// A caller holding the scope the resolver bound to this segment runs the
    /// list through the one adopt-then-publish road rather than a second copy
    /// of it. An empty list means there is nothing to publish — `project_all`
    /// falls back to `*` there.
    pub fn projectable_star_items(mut self) -> Result<(Self, Vec<SelectItem>)> {
        self.state = self.state.ensure_projectable(&self.names)?;
        let items = match &self.state {
            BuilderState::Select { scope, .. } => {
                scope.select_items(&self.identities, Hygiene::Drop)
            }
            _ => unreachable!("ensure_projectable guarantees Select"),
        };
        Ok((self, items))
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
                row_clause,
                scope,
            } => {
                order_by.extend(terms);
                Ok(Self {
                    state: BuilderState::Segment {
                        from,
                        filters,
                        order_by,
                        row_clause,
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
                        row_clause: None,
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

    /// The row clause this level already carries.
    fn row_clause(&self) -> Option<&crate::pipeline::sql_ast::ordering::Limit> {
        match &self.state {
            BuilderState::Segment { row_clause, .. } => row_clause.as_ref(),
            BuilderState::Select { select, .. } => select.limit_clause(),
            // A table carries no clause, and a frozen query wraps below
            // whether or not one stands on it.
            BuilderState::Table { .. } | BuilderState::Frozen { .. } => None,
        }
    }

    /// Put a row clause on this level, promoting or wrapping as the state
    /// requires. The caller has already decided that this level may take one.
    fn set_row_clause(self, clause: crate::pipeline::sql_ast::ordering::Limit) -> Result<Self> {
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
                    row_clause: Some(clause),
                    scope,
                },
                ..self
            }),
            BuilderState::Table { table, scope } => Ok(Self {
                state: BuilderState::Segment {
                    from: vec![table],
                    filters: Vec::new(),
                    order_by: Vec::new(),
                    row_clause: Some(clause),
                    scope,
                },
                ..self
            }),
            // After demote(), state is Select with has_projection=false.
            BuilderState::Select {
                select,
                has_projection,
                has_group_by,
                scope,
            } => Ok(Self {
                state: BuilderState::Select {
                    select: select.limit_from(clause),
                    has_projection,
                    has_group_by,
                    scope,
                },
                ..self
            }),
            BuilderState::Frozen { .. } => {
                let wrapped = Self {
                    state: self.state.wrap_as_subquery(&self.names)?,
                    ..self
                };
                wrapped.set_row_clause(clause)
            }
        }
    }

    /// A BOUND IS A RELATION, NOT A CLAUSE.
    ///
    /// `n` rows of a relation that is already bounded is a bound over THAT
    /// relation, so an operand already carrying a cap becomes a subquery and
    /// the new cap stands outside it. Overwriting would answer a different
    /// question with no sign that it had: `#<0, #<1` is empty and
    /// `#>1, #<1` is the second row, and neither is the first row.
    ///
    /// A level carrying only an OFFSET is the exception, because that is
    /// what the offset was waiting for: `#>a` says where the cap starts
    /// counting, so the two are one clause.
    pub fn add_limit(self, count: i64, offset: Option<i64>) -> Result<Self> {
        use crate::pipeline::sql_ast::ordering::Limit;

        let compose = match self.row_clause() {
            None => None,
            Some(existing) if existing.count().is_none() && offset.is_none() => {
                Some(existing.capped_at(count))
            }
            Some(_) => {
                let wrapped = Self {
                    state: self.state.wrap_as_subquery(&self.names)?,
                    ..self
                };
                return wrapped.add_limit(count, offset);
            }
        };
        let clause = compose.unwrap_or_else(|| match offset {
            Some(off) => Limit::with_offset(count, off),
            None => Limit::new(count),
        });
        self.set_row_clause(clause)
    }

    /// PUBLISH VALUES SO THEY CAN BE NAMED TWICE.
    ///
    /// A reader that must ask several questions of one value cannot write the
    /// expression several times: an expression written twice is evaluated
    /// twice, and a volatile one is then two values. There is no SQL
    /// expression form that binds a value inside a row, so the values go where
    /// a row already has slots — a projection — and the reader stands one
    /// publication above and asks columns.
    ///
    /// All of them in ONE boundary. A second boundary republishes the first
    /// one's outputs, and a column recorded against the level below that is a
    /// name the reader can no longer see.
    ///
    /// The values are lowered against THIS level, so they land beside the
    /// columns they read; `Qualify::rebind` carries the reader's references
    /// across any further boundary.
    pub fn bind_row_values(
        self,
        values: Vec<(DomainExpression, crate::names::ColId)>,
    ) -> Result<Self> {
        let (builder, mut items) = self.projectable_star_items()?;
        for (expr, alias) in values {
            items.push(SelectItem::Expression {
                expr,
                alias: Some(alias),
            });
        }
        builder.add_projection(items)?.demote()
    }

    /// A SKIP WITH NO MAXIMUM. `#>n` selects no cap, so it names no count —
    /// the target's spelling for that is the generator's to write. Applied
    /// to a level that already carries a clause it is a skip over THAT
    /// relation, and wraps.
    pub fn add_offset(self, offset: i64) -> Result<Self> {
        use crate::pipeline::sql_ast::ordering::Limit;

        if self.row_clause().is_some() {
            let wrapped = Self {
                state: self.state.wrap_as_subquery(&self.names)?,
                ..self
            };
            return wrapped.add_offset(offset);
        }
        self.set_row_clause(Limit::offset_only(offset))
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
        column: crate::names::ColId,
        _tvf_prefix: &str,
        kind: JsonEachKind,
        context_items_fn: impl FnOnce(&[crate::names::ColId]) -> Vec<SelectItem>,
        interior_items_fn: impl FnOnce(crate::names::ColId, crate::names::ColId) -> Vec<SelectItem>,
        groundings: &[crate::pipeline::asts::core::operators::ResolvedInteriorGrounding],
    ) -> Result<Builder<Projected>> {
        let identities = std::rc::Rc::clone(&self.identities);
        let names = self.names().fork();
        // Take the heading from the PROJECTED builder, not from this one.
        // `project_all` drops hygienic columns from both the select list and
        // the scope, so a heading read beforehand claims columns the subquery
        // below will not output — and every one of them is then republished
        // into the source scope, where the expansion's context items name it
        // and no FROM entry offers it.
        let projected = self.project_all_carrying_hygiene()?;
        let source_metadata = projected.columns().to_vec();
        let source_origin = wrap_origin(
            &source_metadata,
            &identities,
            crate::names::WrapReason::Projection,
        );
        let source_scope = names.fresh(source_origin).identity();
        let mut source_query = projected.to_sql()?;
        let source_columns: Vec<_> = republish_under(
            &mut source_query,
            source_scope,
            &source_metadata,
            &identities,
            crate::names::Republish::BoundaryExport,
        )?
        .into_iter()
        .map(|column| column.identity())
        .collect();
        crate::probe::probing!(destructure, {
            crate::probe::probe!(
                destructure,
                "expand over {:?}",
                crate::probe::chain(&identities, column)
            );
            for candidate in &source_columns {
                crate::probe::probe!(
                    destructure,
                    "  have {:?}",
                    crate::probe::chain(&identities, *candidate)
                );
            }
        });
        let source_column = source_columns
            .iter()
            .copied()
            .find(|candidate| identities.same_value(*candidate, column))
            .ok_or_else(|| crate::error::DelightQLError::ParseError {
                message: "json expansion source column is not in the input heading".to_string(),
                source: None,
                subcategory: None,
            })?;

        let tvf_scope = names
            .fresh(crate::names::ScopeOrigin::Interior { of: column })
            .identity();
        let key_spelling = identities.intern("key", false);
        let value_spelling = identities.intern("value", false);
        let key_column = identities.mint_column(
            tvf_scope,
            crate::names::ColumnOrigin::Computed {
                via: crate::names::Computation::Function,
            },
            Some(key_spelling),
            crate::names::Addressing::Published,
            crate::names::ValueFacts::default(),
        );
        let value_column = identities.mint_column(
            tvf_scope,
            crate::names::ColumnOrigin::Computed {
                via: crate::names::Computation::Function,
            },
            Some(value_spelling),
            crate::names::Addressing::Published,
            crate::names::ValueFacts::default(),
        );

        let mut items = context_items_fn(&source_columns);
        items.extend(interior_items_fn(key_column, value_column));
        let output_scope = names
            .fresh(crate::names::ScopeOrigin::Join {
                left: source_scope,
                right: tvf_scope,
            })
            .identity();
        let mut inputs: Vec<_> = source_metadata.clone();
        inputs.push(ColumnMetadata::new(key_column));
        inputs.push(ColumnMetadata::new(value_column));
        let (items, columns) =
            derive_columns_from_items(items, output_scope, &inputs, &identities, None, None);
        let joined_from = TableExpression::Join {
            left: Box::new(TableExpression::subquery(source_query, source_scope)),
            right: Box::new(TableExpression::TVF {
                function: identities.mint_intrinsic(kind.intrinsic()),
                arguments: vec![TvfArgument::Column(source_column)],
                alias: tvf_scope,
            }),
            join_type: JoinType::Inner,
            join_condition: JoinCondition::On(DomainExpression::literal(
                crate::pipeline::asts::core::LiteralValue::Boolean(true),
            )),
        };
        let mut select = SelectBuilder::new()
            .set_select(items.clone())
            .from_tables(vec![joined_from]);
        for grounding in groundings {
            let path = DomainExpression::PublishedJsonPathLiteral(grounding.column);
            let extracted = DomainExpression::function(
                "json_extract",
                vec![DomainExpression::Column(value_column), path],
            );
            select = select.and_where(DomainExpression::Binary {
                left: Box::new(extracted),
                op: crate::pipeline::sql_ast::BinaryOperator::Equal,
                right: Box::new(DomainExpression::literal(
                    crate::pipeline::asts::core::LiteralValue::String(grounding.value.clone()),
                )),
            });
        }
        let select = publication::publish_at(
            output_scope,
            columns.iter().map(ColumnMetadata::identity),
            select,
            &identities,
        )?;
        let query = QueryExpression::Select(Box::new(select));
        Builder::from_query(
            query,
            ScopeName::Resolved(output_scope),
            columns,
            names,
            identities,
        )
    }
}

// ---------------------------------------------------------------------------
// Builder<Projected> — finishing phase
// ---------------------------------------------------------------------------

impl Builder<Projected> {
    /// Adopt a finished query that already publishes the heading it claims.
    ///
    /// Used when embedding a pre-built query (e.g., from EntityRegistry)
    /// or after external construction (set operations, recursive CTEs).
    ///
    /// The heading is taken as given, not reminted into the scope. Reminting
    /// here renamed the entry while the frozen body went on publishing what
    /// it always had — the alias claiming one heading over a statement
    /// outputting another, spelled identically, so nothing downstream could
    /// tell. A caller whose columns live elsewhere republishes them and
    /// rewrites the body it stands over; that is one act and there is no
    /// road here that performs half of it.
    pub(in crate::pipeline::transformer) fn from_query(
        query: QueryExpression,
        scope_name: ScopeName,
        columns: Vec<ColumnMetadata>,
        names: NameGenerator,
        identities: std::rc::Rc<crate::names::Registry>,
    ) -> Result<Self> {
        let scope = Publication::at(scope_name.into_scope(), columns, &identities)?;
        scope.check_query(&query)?;
        Ok(Self {
            state: BuilderState::Frozen { query, scope },
            names,
            identities,
            accumulated_ctes: Vec::new(),
            _phase: PhantomData,
        })
    }

    /// Re-projection publishing a scope already bound, the Projected-phase
    /// counterpart of the Unprojected road of the same name. Same rule: the
    /// occurrences a projection publishes were decided before lowering, and
    /// minting a second set here answers to none of the references addressed
    /// against the first.
    pub fn add_projection_publishing(
        mut self,
        items: Vec<SelectItem>,
        scope: crate::names::ScopeId,
        columns: Vec<ColumnMetadata>,
    ) -> Result<Self> {
        self.state = self.state.ensure_projectable(&self.names)?;
        let items = reanchor_select_items(items, &self)?;
        match self.state {
            BuilderState::Select {
                select,
                has_group_by,
                ..
            } => Ok(Self {
                state: BuilderState::Select {
                    select: select.set_select(items),
                    has_projection: true,
                    has_group_by,
                    scope: Publication::at(scope, columns, &self.identities)?,
                },
                names: self.names,
                identities: self.identities,
                accumulated_ctes: self.accumulated_ctes,
                _phase: PhantomData,
            }),
            _ => unreachable!("ensure_projectable guarantees Select state"),
        }
    }

    /// Re-projection on a Projected builder. Wraps as subquery first
    /// (unless current projection is a passthrough), then sets new SELECT list.
    pub fn add_projection(mut self, items: Vec<SelectItem>) -> Result<Self> {
        self.state = self.state.ensure_projectable(&self.names)?;
        let items = reanchor_select_items(items, &self)?;
        match self.state {
            BuilderState::Select {
                select,
                has_group_by,
                scope: ref input_scope,
                ..
            } => {
                let input_columns = input_scope.outputs().to_vec();
                let origin = wrap_origin(
                    &input_columns,
                    &self.identities,
                    crate::names::WrapReason::Projection,
                );
                let fresh_scope = self.names.fresh(origin);
                let identity_scope = fresh_scope.identity();
                let (items, output_columns) = derive_columns_from_items(
                    items,
                    identity_scope,
                    &input_columns,
                    &self.identities,
                    None,
                    None,
                );
                let select = select.set_select(items);
                Ok(Self {
                    state: BuilderState::Select {
                        select,
                        has_projection: true,
                        has_group_by,
                        scope: Publication::at(identity_scope, output_columns, &self.identities)?,
                    },
                    names: self.names,
                    identities: self.identities,
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
            crate::pipeline::sql_ast::ordering::OrderDirection,
        )>,
        alias: crate::names::ColId,
    ) -> Result<Self> {
        // Wrap current state as subquery so window function sees finalized rows
        let wrapped = Self {
            state: self.state.wrap_as_subquery(&self.names)?,
            names: self.names,
            identities: self.identities,
            accumulated_ctes: self.accumulated_ctes,
            _phase: PhantomData,
        };

        match wrapped.state {
            BuilderState::Select {
                select,
                has_group_by,
                scope: ref input_scope,
                ..
            } => {
                let input_columns = input_scope.outputs().to_vec();

                // The caller lowered the window's argument, partition and
                // order references against the pre-wrap builder; the wrap
                // just moved every column one publication deeper. Each
                // reference must land on the occurrence THIS layer's FROM
                // publishes — an already-anchored reference republishes
                // itself and passes through, and a reference two layer
                // columns both republish is left alone for the self-check
                // to name.
                let reanchor = |column: crate::names::ColId| -> crate::names::ColId {
                    let mut owners = input_columns.iter().filter(|candidate| {
                        wrapped.identities.republishes(candidate.identity(), column)
                    });
                    match (owners.next(), owners.next()) {
                        (Some(owner), None) => owner.identity(),
                        _ => column,
                    }
                };
                let window_item = SelectItem::Expression {
                    expr: DomainExpression::WindowFunction {
                        name: func_name.to_string(),
                        args,
                        distinct: false,
                        partition_by,
                        order_by,
                        frame: None,
                    }
                    .map_columns(&reanchor),
                    alias: Some(alias),
                };
                let origin = wrap_origin(
                    &input_columns,
                    &wrapped.identities,
                    crate::names::WrapReason::Projection,
                );
                let fresh_scope = wrapped.names.fresh(origin);
                let identity_scope = fresh_scope.identity();

                // Start with Star + window column
                let items = vec![
                    SelectItem::star(input_columns.iter().map(ColumnMetadata::identity).collect()),
                    window_item,
                ];
                let (items, output_columns) = derive_columns_from_items(
                    items,
                    identity_scope,
                    &input_columns,
                    &wrapped.identities,
                    None,
                    Some(crate::names::MintReason::RowNumber),
                );
                let select = select.set_select(items);

                Ok(Self {
                    state: BuilderState::Select {
                        select,
                        has_projection: true,
                        has_group_by,
                        scope: Publication::at(
                            identity_scope,
                            output_columns,
                            &wrapped.identities,
                        )?,
                    },
                    names: wrapped.names,
                    identities: wrapped.identities,
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
                    identities: self.identities,
                    accumulated_ctes: self.accumulated_ctes,
                    _phase: PhantomData,
                };
                wrapped.add_distinct()
            }
        }
    }

    /// Adopt a finished query that already publishes the heading it claims.
    ///
    /// A complete SELECT or set operation IS a projected query — which is
    /// what `set_operation` already relies on when it hands back a frozen
    /// union. This is that same fact reached from the alignment road, where
    /// every arm was built at the output scope and aliased to its columns.
    pub(in crate::pipeline::transformer) fn adopt_finished(
        query: QueryExpression,
        scope_name: ScopeName,
        columns: Vec<ColumnMetadata>,
        names: NameGenerator,
        identities: std::rc::Rc<crate::names::Registry>,
    ) -> Result<Self> {
        let scope = Publication::at(scope_name.into_scope(), columns, &identities)?;
        scope.check_query(&query)?;
        Ok(Self {
            state: BuilderState::Frozen { query, scope },
            names,
            identities,
            accumulated_ctes: Vec::new(),
            _phase: PhantomData,
        })
    }

    // --- Set operations ---

    /// UNION ALL with another builder. Both finalized, combined as set op.
    pub fn union_all(self, right: Self) -> Result<Self> {
        self.set_operation(right, crate::pipeline::sql_ast::SetOperator::UnionAll)
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
            let scope = self.state.publication();
            let reachable = matches!(&self.state, BuilderState::Table { .. })
                || self
                    .accumulated_ctes
                    .iter()
                    .any(|cte| cte.scope() == scope.at_scope());

            if !reachable {
                let scope_clone = scope.clone();
                let mut source_query = self.state.materialize(&self.names)?;
                let source_origin = cte_origin(
                    scope_clone.outputs(),
                    &self.identities,
                    crate::names::CteRole::Materialize,
                );
                let source_name = self.names.fresh(source_origin);
                let source_identity = source_name.identity();
                let columns = republish_under(
                    &mut source_query,
                    source_identity,
                    scope_clone.outputs(),
                    &self.identities,
                    crate::names::Republish::BoundaryExport,
                )?;
                self.accumulated_ctes
                    .push(Cte::new(source_identity, source_query));
                let requalified = Publication::at(source_identity, columns, &self.identities)?;
                let cte_table = TableExpression::Scope(source_identity);
                self.state = BuilderState::Select {
                    select: SelectBuilder::new()
                        .from_tables(vec![cte_table])
                        .set_select(vec![SelectItem::star(requalified.identities_in_order())]),
                    has_projection: false,
                    has_group_by: false,
                    scope: requalified,
                };
            }
        }

        // Snapshot the current scope as CteInput — now guaranteed reachable.
        let current_scope = self.state.publication().clone();
        let input = CteInput::new(
            current_scope.at_scope(),
            current_scope.outputs().to_vec(),
            std::rc::Rc::clone(&self.identities),
        );

        // Call the closure to get the CTE body
        let mut cte_body = body(&input)?;

        // Generate a CTE name
        let cte_origin = cte_origin(
            current_scope.outputs(),
            &self.identities,
            crate::names::CteRole::Materialize,
        );
        let fresh_cte = self.names.fresh(cte_origin);
        let cte_identity = fresh_cte.identity();

        let output_columns = build_cte_output_columns(
            &cte_body.output_columns,
            current_scope.outputs(),
            &self.identities,
            cte_identity,
        );

        // Naming the CTE and re-aliasing the body it binds are one act. Every
        // reader of the CTE addresses the occurrences just minted for it; a
        // body still publishing its own leaves the binding claiming a heading
        // the statement under it does not output.
        let rebound: Vec<_> = cte_body
            .output_columns
            .iter()
            .zip(&output_columns)
            .map(|(source, target)| (*source, target.identity()))
            .collect();
        state::rewrite_output_aliases(
            &mut cte_body.query,
            cte_identity,
            &rebound,
            &self.identities,
        )?;

        // Accumulate the CTE
        self.accumulated_ctes
            .push(Cte::new(cte_identity, cte_body.query));

        // Transition to a new Select FROM the CTE.
        // The old state is discarded — the closure already used it to build the CTE body.
        // The new state references the CTE by name so that subsequent operations
        // (add_projection, another push_cte) operate on the CTE's output.
        let cte_table = TableExpression::Scope(cte_identity);
        let new_scope = Publication::at(cte_identity, output_columns, &self.identities)?;
        self.state = BuilderState::Select {
            select: SelectBuilder::new()
                .from_tables(vec![cte_table])
                .set_select(vec![SelectItem::star(new_scope.identities_in_order())]),
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
            identities: self.identities,
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
        let query = self.state.materialize(&self.names)?;

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
        op: crate::pipeline::sql_ast::SetOperator,
    ) -> Result<Self> {
        let mut ctes = self.accumulated_ctes;
        ctes.extend(right.accumulated_ctes);

        let left_scope = self.state.publication().clone();
        let right_scope = right.state.publication().clone();
        let mut left_query = self.state.materialize(&self.names)?;
        let mut right_query = right.state.materialize(&right.names)?;

        // Output scope: use left's columns with a new generated name
        let set_origin = wrap_origin(
            left_scope.outputs(),
            &self.identities,
            crate::names::WrapReason::SetOperation,
        );
        let set_scope_name = self.names.fresh(set_origin);
        let set_identity = set_scope_name.identity();

        // Both arms publish the merged heading, not just the one it was minted
        // from. SQL takes the output names from the first arm, which is why
        // rewriting only the left looks sufficient and is not: the second arm's
        // items still name occurrences of its own scope, and every check that
        // reads a set operation reads both sides.
        let columns = republish_under(
            &mut left_query,
            set_identity,
            left_scope.outputs(),
            &self.identities,
            crate::names::Republish::ArmMerge,
        )?;
        let paired: Vec<_> = right_scope
            .outputs()
            .iter()
            .zip(&columns)
            .map(|(source, target)| (source.identity(), target.identity()))
            .collect();
        state::rewrite_output_aliases(&mut right_query, set_identity, &paired, &self.identities)?;
        let output_scope = Publication::at(set_identity, columns, &self.identities)?;

        let combined = QueryExpression::SetOperation {
            op,
            left: Box::new(left_query),
            right: Box::new(right_query),
        };

        Ok(Self {
            state: BuilderState::Frozen {
                query: combined,
                scope: output_scope,
            },
            names: self.names,
            identities: self.identities,
            accumulated_ctes: ctes,
            _phase: PhantomData,
        })
    }
}

// ---------------------------------------------------------------------------
// Qualify — phase-independent implementation
// ---------------------------------------------------------------------------



// ---------------------------------------------------------------------------
// Builder Qualify — delegates to shared functions
// ---------------------------------------------------------------------------

impl<P> Qualify for Builder<P> {
    fn identities(&self) -> &crate::names::Registry {
        &self.identities
    }

    fn scope_columns(&self) -> Vec<ColumnMetadata> {
        self.state.publication().outputs().to_vec()
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
pub(in crate::pipeline::transformer) struct JoinOperand {
    pub table: TableExpression,
    pub columns: Vec<ColumnMetadata>,
    pub names: NameGenerator,
    pub identities: std::rc::Rc<crate::names::Registry>,
    pub ctes: Vec<Cte>,
}

impl Qualify for JoinOperand {
    fn identities(&self) -> &crate::names::Registry {
        &self.identities
    }

    fn scope_columns(&self) -> Vec<ColumnMetadata> {
        self.columns.clone()
    }
}

impl JoinOperand {
    /// Re-anchor this operand's TVF arguments onto the occurrences the other
    /// side of the join publishes.
    ///
    /// A TVF argument is a lateral reference — `pragma_table_info(src.tbl)`
    /// reads a column the other operand carries. The TVF's table expression is
    /// built standalone, before any join, so the argument is addressed at the
    /// scope that published it then; preparing the other operand may wrap it,
    /// and the wrapper's alias is what the emitted FROM entry carries. The
    /// argument travels the same road the join condition already takes.
    ///
    /// An argument the other side does not publish passes through untouched:
    /// that is a correlation reaching further out, and `rebind` leaves it.
    pub fn resolve_tvf_args(&mut self, scope: &dyn Qualify) -> Result<()> {
        rebind_tvf_arguments(&mut self.table, scope)
    }
}

/// Rebind the TVF arguments in a join tree's own FROM entries.
///
/// Descends through `Join` only. A `Subquery` is a sealed FROM path whose
/// entries name scopes established inside it, and re-anchoring one of those
/// against a scope out here would move a reference that is already correct.
fn rebind_tvf_arguments(table: &mut TableExpression, scope: &dyn Qualify) -> Result<()> {
    match table {
        TableExpression::TVF { arguments, .. } => {
            for argument in arguments {
                if let TvfArgument::Column(column) = argument {
                    *column = scope.rebind(*column)?;
                }
            }
            Ok(())
        }
        TableExpression::Join { left, right, .. } => {
            rebind_tvf_arguments(left, scope)?;
            rebind_tvf_arguments(right, scope)
        }
        _ => Ok(()),
    }
}

/// Chained qualify: try inner scope, then outer scope.
///
/// Used for joins (left + right) and correlated subqueries (inner + outer).
/// Lives in the builder module — the chaining logic is part of the qualify
/// contract, not something consumers should reimplement.
pub(in crate::pipeline::transformer) struct ChainedQualify<'a> {
    pub inner: &'a dyn Qualify,
    pub outer: &'a dyn Qualify,
}

impl Qualify for ChainedQualify<'_> {
    fn identities(&self) -> &crate::names::Registry {
        self.inner.identities()
    }

    fn scope_columns(&self) -> Vec<ColumnMetadata> {
        let mut columns = self.inner.scope_columns();
        columns.extend(self.outer.scope_columns());
        columns
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
    fn into_table_expr(self, names: &NameGenerator) -> Result<(TableExpression, Publication)> {
        match self {
            Self::Table { table, scope } => Ok((table, scope)),
            // Segment flattening disabled. The core fixes (explicit
            // materialize + prior_identities + global disambiguation)
            // handle correctness. Flattening is a future optimization
            // for cleaner SQL output, not a correctness mechanism.
            other => {
                let scope = other.publication().clone();
                let mut query = other.materialize(names)?;
                let origin = wrap_origin(
                    scope.outputs(),
                    names.identities(),
                    crate::names::WrapReason::Projection,
                );
                let identity = names.fresh(origin).identity();
                // The wrap is emission plumbing around a join operand — the
                // same relation, not a boundary that consumes it — so the
                // republication kind keeps the ownership walk open.
                let new_scope = scope.requalified(
                    identity,
                    names.identities(),
                    crate::names::Republish::EmissionWrap,
                )?;
                // Requalifying the scope without re-publishing the query that
                // fills it leaves the subquery emitting the occurrences it had
                // inside while the alias over it claims the ones just minted —
                // the disagreement no consumer's qualification can be checked
                // against. The two are one act, here as at every other wrap.
                let aliases = scope.pairs_with(&new_scope);
                state::rewrite_output_aliases(&mut query, identity, &aliases, names.identities())?;
                Ok((TableExpression::subquery(query, identity), new_scope))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn effective_scope(table: &TableExpression) -> Option<crate::names::ScopeId> {
    match table {
        TableExpression::Entity { alias, .. } => *alias,
        TableExpression::Scope(scope)
        | TableExpression::Subquery { alias: scope, .. }
        | TableExpression::TVF { alias: scope, .. } => Some(*scope),
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
    scope: crate::names::ScopeId,
    input_columns: &[ColumnMetadata],
    identities: &crate::names::Registry,
    aggregate_from: Option<usize>,
    minted_reason: Option<crate::names::MintReason>,
) -> (Vec<SelectItem>, Vec<ColumnMetadata>) {
    let mut columns = Vec::new();
    let mut out_items = Vec::with_capacity(items.len());
    for (i, item) in items.into_iter().enumerate() {
        match item {
            SelectItem::Star { .. } => {
                // The star's expansion is rewritten here, not carried over:
                // this projection republishes every input into its own scope,
                // so what the star stands for downstream is the occurrences
                // just minted, not the ones it stood for above.
                let mut expansion = Vec::with_capacity(input_columns.len());
                for col in input_columns {
                    let source = col.identity();
                    let output = identities.republish_column(
                        source,
                        scope,
                        crate::names::Republish::Passthrough,
                        identities.published(source),
                        identities.addressing(source),
                        |_| {},
                    );
                    columns.push(ColumnMetadata::new(output));
                    expansion.push(output);
                }
                out_items.push(SelectItem::star(expansion));
            }
            SelectItem::Expression { expr, alias } => {
                let direct = match &expr {
                    DomainExpression::Column(column) => input_columns
                        .iter()
                        .find(|candidate| candidate.identity() == *column),
                    _ => None,
                };
                let published = alias
                    .and_then(|column| identities.published(column))
                    .or_else(|| direct.and_then(|column| identities.published(column.identity())));
                let addressing = alias
                    .map(|column| identities.addressing(column))
                    .or_else(|| direct.map(|column| identities.addressing(column.identity())))
                    .unwrap_or(crate::names::Addressing::Bare);
                // Three facts come off an item's output — its name, its
                // addressing, and its lineage — and all three answer to the
                // same question: which occurrence did the caller say this item
                // publishes? An explicit alias IS that answer, which is why the
                // two above take it. Lineage taking the source instead left the
                // named occurrence on no chain at all, so a reader addressed
                // against it found nothing carrying it: an item written
                // `Column(tvf_key) AS country` published the TVF's key and
                // orphaned the `country` the resolver had put in the heading.
                //
                // With no alias the item names nothing of its own and stands
                // for what it reads, which is the source. Minting is for an item
                // that names neither — a computed value — and a minted column
                // descends from nothing, so it must not be reached for while
                // either name is available.
                let carried = alias.or_else(|| direct.map(ColumnMetadata::identity));
                let output = match carried {
                    Some(source) => identities.republish_column(
                        source,
                        scope,
                        crate::names::Republish::Passthrough,
                        published,
                        addressing,
                        |_| {},
                    ),
                    None => identities.mint_column(
                        scope,
                        minted_reason
                            .map(|by| crate::names::ColumnOrigin::Minted { by })
                            .unwrap_or_else(|| crate::names::ColumnOrigin::Computed {
                                via: aggregate_from
                                    .filter(|start| i >= *start)
                                    .map(|_| crate::names::Computation::Aggregate)
                                    .unwrap_or_else(|| computation_for_sql_expr(&expr)),
                            }),
                        published,
                        addressing,
                        Default::default(),
                    ),
                };
                columns.push(ColumnMetadata::new(output));
                out_items.push(SelectItem::Expression {
                    expr,
                    alias: Some(output),
                });
            }
        }
    }
    (out_items, columns)
}

fn computation_for_sql_expr(expr: &DomainExpression) -> crate::names::Computation {
    match expr {
        DomainExpression::Literal(_)
        | DomainExpression::PublishedNameLiteral(_)
        | DomainExpression::PublishedJsonPathLiteral(_)
        | DomainExpression::JsonPathLiteral(_)
        | DomainExpression::ScopeNameLiteral(_) => crate::names::Computation::Literal,
        DomainExpression::Cast { .. } => crate::names::Computation::Cast,
        DomainExpression::Function { .. } => crate::names::Computation::Function,
        DomainExpression::WindowFunction { .. } => crate::names::Computation::Window,
        DomainExpression::Case { .. } => crate::names::Computation::Case,
        DomainExpression::Exists { .. } | DomainExpression::Subquery(_) => {
            crate::names::Computation::Subquery
        }
        DomainExpression::Parens(inner) => computation_for_sql_expr(inner),
        DomainExpression::Column(_)
        | DomainExpression::Binary { .. }
        | DomainExpression::Unary { .. }
        | DomainExpression::Star
        | DomainExpression::PredicateRewrite { .. }
        | DomainExpression::Observation { .. } => crate::names::Computation::Operator,
    }
}

/// Build output columns for a CTE, carrying forward provenance from input columns.
///
/// An output column that IS an input column keeps that column's identity, so a
/// qualified reference written at a downstream CTE level still reaches the
/// occurrence it named. Pairing is by value, never by spelling — the spelling
/// does not exist yet, and a column whose name the compiler will invent has no
/// spelling to pair on at all.
fn build_cte_output_columns(
    output_columns: &[crate::names::ColId],
    input_columns: &[ColumnMetadata],
    identities: &crate::names::Registry,
    scope: crate::names::ScopeId,
) -> Vec<ColumnMetadata> {
    output_columns
        .iter()
        .map(|output| {
            let input_col = input_columns
                .iter()
                .find(|candidate| identities.same_value(candidate.identity(), *output));
            let identity = match input_col {
                Some(source) => identities.republish_column(
                    source.identity(),
                    scope,
                    crate::names::Republish::BoundaryExport,
                    identities.published(*output),
                    identities.addressing(*output),
                    |_| {},
                ),
                None => identities.republish_column(
                    *output,
                    scope,
                    crate::names::Republish::BoundaryExport,
                    identities.published(*output),
                    identities.addressing(*output),
                    |_| {},
                ),
            };
            ColumnMetadata::new(identity)
        })
        .collect()
}



#[cfg(test)]
mod rebind_tests {
    //! What `rebind` must REFUSE.
    //!
    //! The value tier answers a reference with the one candidate carrying its
    //! value. "The one" is the bound that makes the tier sound, so a scope
    //! carrying the value twice has to refuse — declining instead would fall
    //! through to a tier answering a different question and, failing that,
    //! leave the reference standing at a scope no FROM entry establishes: the
    //! ambiguity emitted rather than reported.

    use super::Qualify;
    use crate::names::{
        Addressing, ColId, ColumnOrigin, Hint, Registry, Republish, ScopeOrigin, ValueFacts,
        WrapReason,
    };
    use crate::pipeline::asts::core::ColumnMetadata;

    struct Scope {
        identities: Registry,
        columns: Vec<ColumnMetadata>,
    }

    impl Qualify for Scope {
        fn identities(&self) -> &Registry {
            &self.identities
        }
        fn scope_columns(&self) -> Vec<ColumnMetadata> {
            self.columns.clone()
        }
    }

    /// One base column, republished into `slots` slots of one projection —
    /// `(id as a, id as b)` — and then across a boundary, so the reference and
    /// the candidates are siblings rather than one chain.
    fn one_value_in(slots: usize) -> (Registry, ColId, Vec<ColumnMetadata>) {
        let reg = Registry::new(&[]);
        let entity = reg.mint_entity(reg.intern("t", false));
        let base = reg.mint_scope(ScopeOrigin::BaseTable { entity }, Hint::None, None);
        let id = reg.mint_column(
            base,
            ColumnOrigin::CatalogColumn {
                entity,
                position: 0,
            },
            Some(reg.intern("id", false)),
            Addressing::Published,
            ValueFacts::default(),
        );

        let projected = reg.mint_scope(ScopeOrigin::PipeStage { input: base }, Hint::None, None);
        let slotted: Vec<ColId> = (0..slots)
            .map(|slot| {
                reg.republish_column(
                    id,
                    projected,
                    Republish::Rename,
                    Some(reg.intern(&format!("s{slot}"), false)),
                    Addressing::Published,
                    |_| {},
                )
            })
            .collect();

        // What the resolver addressed the reference against.
        let segment = reg.mint_scope(
            ScopeOrigin::PipeStage { input: projected },
            Hint::None,
            None,
        );
        let reference = reg.republish_column(
            slotted[0],
            segment,
            Republish::Passthrough,
            reg.published(slotted[0]),
            Addressing::Published,
            |_| {},
        );

        // What the transformer's own boundary published — the sibling.
        let wrapped = reg.mint_scope(
            ScopeOrigin::Wrap {
                input: projected,
                why: WrapReason::Projection,
            },
            Hint::None,
            None,
        );
        let columns = slotted
            .iter()
            .map(|slot| {
                let column = reg.republish_column(
                    *slot,
                    wrapped,
                    Republish::Passthrough,
                    reg.published(*slot),
                    Addressing::Published,
                    |_| {},
                );
                ColumnMetadata::new(column)
            })
            .collect();
        (reg, reference, columns)
    }

    #[test]
    fn one_sibling_carrying_the_value_answers() {
        let (identities, reference, columns) = one_value_in(1);
        let scope = Scope {
            identities,
            columns,
        };
        let landed = scope.rebind(reference).expect("one candidate answers");
        assert_eq!(landed, scope.columns[0].identity());
    }

    #[test]
    fn two_siblings_carrying_one_value_refuse() {
        let (identities, reference, columns) = one_value_in(2);
        let scope = Scope {
            identities,
            columns,
        };
        let error = scope
            .rebind(reference)
            .expect_err("two candidates with equal claim must refuse, not decline");
        assert!(
            error.to_string().contains("more than once"),
            "the refusal must say why: {error}"
        );
    }
}
