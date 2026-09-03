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
pub(in crate::pipeline) mod layout;
pub(in crate::pipeline) mod names;
pub(in crate::pipeline) mod state;

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

pub(in crate::pipeline) use layout::{Hygiene, SqlLayout};
pub(in crate::pipeline) use names::NameGenerator;
pub(in crate::pipeline::transformer) use names::ScopeName;
use state::BuilderState;

pub(super) fn wrap_origin(
    columns: &[ColumnMetadata],
    identities: &crate::names::Registry,
    why: crate::names::WrapReason,
) -> crate::names::ScopeId {
    ColumnMetadata::common_identity_scope(columns, identities).map_or_else(
        || identities.anonymous_scope(None),
        |input| identities.wrap_scope(input, why),
    )
}

/// The GROUP BY clause is the key items' expressions — the clause groups by
/// what the keys select, never by the names they publish under.
fn group_by_expressions(keys: &[SelectItem]) -> Vec<DomainExpression> {
    keys.iter()
        .filter_map(|item| item.expr().cloned())
        .collect()
}

pub(super) fn pipe_origin(
    columns: &[ColumnMetadata],
    identities: &crate::names::Registry,
) -> crate::names::ScopeId {
    ColumnMetadata::common_identity_scope(columns, identities).map_or_else(
        || identities.anonymous_scope(None),
        |input| identities.stage_scope(input),
    )
}

fn cte_origin(
    columns: &[ColumnMetadata],
    identities: &crate::names::Registry,
    role: crate::names::CteRole,
) -> crate::names::ScopeId {
    ColumnMetadata::common_identity_scope(columns, identities).map_or_else(
        || identities.anonymous_scope(None),
        |input| identities.cte_scope(input, role, crate::names::CteLabel::Anonymous),
    )
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
) -> Result<(
    crate::names::ScopeId,
    Vec<crate::names::ColId>,
    Vec<crate::names::ColId>,
)> {
    let at = identities.wrap_scope(input, why);
    let mut outputs = Vec::with_capacity(items.len());
    let mut aliases = Vec::with_capacity(items.len());
    for item in items {
        let SelectItem::Publishing {
            slot: alias,
            printed: true,
            ..
        } = item
        else {
            return Err(crate::error::DelightQLError::parse_error(
                "a CTE body has an output it does not name",
            ));
        };
        aliases.push(*alias);
        let published = identities.rebind_sql_column(*alias, at, identities.published(*alias));
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
    Ok((at, outputs, aliases))
}

pub(super) fn remint_heading(
    columns: Vec<ColumnMetadata>,
    identities: &crate::names::Registry,
    into: crate::names::ScopeId,
) -> Vec<ColumnMetadata> {
    columns
        .into_iter()
        .map(|column| {
            let source = column.identity();
            let identity = identities.rebind_sql_column(source, into, identities.published(source));
            ColumnMetadata::new(identity)
        })
        .collect()
}

/// Stage a statement into a plan-lifetime relation that HOLDS what it emits.
///
/// The authority derives the relation and republishes those occurrences into
/// it in one act, so the stored interface IS the created table's heading.
/// Rewriting the statement's aliases onto them afterwards is rendering: it
/// names in SQL what the authority already published.
pub(in crate::pipeline::transformer) fn stage_holding(
    query: &mut QueryExpression,
    emits: &[ColumnMetadata],
    staged: crate::relation::SemanticRelation,
    identities: &crate::names::Registry,
) -> Result<(crate::relation::SemanticRelation, Vec<ColumnMetadata>)> {
    let emitted: Vec<_> = emits.iter().map(ColumnMetadata::identity).collect();
    let published: Vec<ColumnMetadata> = crate::relation::published_ports(identities, &staged)?
        .into_iter()
        .map(|port| ColumnMetadata::new(port.column()))
        .collect();
    let aliases: Vec<_> = emitted
        .iter()
        .copied()
        .zip(published.iter().map(ColumnMetadata::identity))
        .collect();
    state::rewrite_output_aliases(query, staged.scope(), &aliases, identities)?;
    Ok((staged, published))
}

pub(in crate::pipeline::transformer) fn republish_under(
    query: &mut QueryExpression,
    scope: crate::names::ScopeId,
    columns: &[ColumnMetadata],
    identities: &crate::names::Registry,
) -> Result<Vec<ColumnMetadata>> {
    let republished = remint_heading(columns.to_vec(), identities, scope);
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
        .map(|item| match item.expr() {
            Some(expr) => item.with_expr(expr.clone().map_columns(&|column| {
                match qualify.rebind_physical(column) {
                    Ok(landed) => landed,
                    Err(error) => {
                        failure.borrow_mut().get_or_insert(error);
                        column
                    }
                }
            })),
            None => item,
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum MergeSources {
    /// Ordinary consumers read the one retained output position.
    Collapsed,
    /// A full-outer projection still needs both operands to build COALESCE.
    RetainedForProjection,
}

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

    /// Every exact site this qualifier's references may reach. Empty for a
    /// scope that emits nothing.
    fn sql_sites(&self) -> Vec<crate::sql_binding::SqlSiteId> {
        Vec::new()
    }

    /// Bind a semantic port to the physical column emitted for it.
    ///
    /// NO DEFAULT. There is no universal "maybe this qualifier emits":
    /// every scope answers this in its own words, and a scope that emits
    /// nothing says exactly that. An emitting one delegates to
    /// [`Emitted::emitted_port`], which reads the binding at the ONE site
    /// its type carries.
    fn rebind_port(&self, port: crate::relation::PortId) -> Result<crate::names::ColId>;

    /// The exact physical output position occupied by a semantic port.
    fn slot_of_port(&self, port: crate::relation::PortId) -> Result<usize>;

    fn slot_of_physical(&self, column: crate::names::ColId) -> Result<usize>;

    /// Re-anchor one physical slot through SQL-only wrapping recorded at
    /// the exact emitted site. A slot absent from every site this
    /// qualifier reaches is an outer physical reference and stays
    /// unchanged.
    fn rebind_physical(&self, column: crate::names::ColId) -> Result<crate::names::ColId> {
        let matches = self
            .sql_sites()
            .into_iter()
            .filter_map(|site| {
                self.identities()
                    .bindings()
                    .physical_at(site, column)
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        match matches.as_slice() {
            [] => Ok(column),
            [landed] => Ok(*landed),
            _ => Err(crate::error::DelightQLError::parse_error(format!(
                "physical column {column:?} occurs at more than one exact SQL site"
            ))),
        }
    }

    /// Snapshot this scope's columns for use as an outer scope.
    ///
    /// Called at scalar subquery entry points to capture the enclosing
    /// scope into `TransformCtx.outer_columns`. Default: empty (no columns
    /// to contribute).
    fn scope_columns(&self) -> Vec<ColumnMetadata> {
        vec![]
    }

    fn tree_valued(&self, column: crate::names::ColId) -> bool {
        self.identities().is_tree_valued(column)
    }
}

/// A QUALIFIER WHOSE COLUMNS ARE THE REALIZATION OF ONE EXACT SITE.
///
/// Emission is a PROPERTY OF THE QUALIFIER, not a question asked of every
/// one of them. A scope that emits nothing — an anonymous row's literals —
/// is simply not one of these, and a scope that reaches TWO sites — a set
/// correlation naming its arms, a chained inner-and-outer view — is not one
/// either: it says which of its sites answers, in its own words.
pub(crate) trait Emitting: Qualify {
    fn site(&self) -> crate::sql_binding::SqlSiteId;
}

/// The answers every emitting qualifier gives the same way.
///
/// Blanket over [`Emitting`], so an implementor states its site and
/// nothing else. There is no `Option` on this road: the site is a field,
/// not a question.
pub(crate) trait Emitted {
    fn emitted_port(&self, port: crate::relation::PortId) -> Result<crate::names::ColId>;
    fn emitted_slot(&self, port: crate::relation::PortId) -> Result<usize>;
    fn emitted_physical_slot(&self, column: crate::names::ColId) -> Result<usize>;
    fn emitted_sites(&self) -> Vec<crate::sql_binding::SqlSiteId>;
}

impl<T: Emitting + ?Sized> Emitted for T {
    fn emitted_port(&self, port: crate::relation::PortId) -> Result<crate::names::ColId> {
        self.identities().bindings().at(self.site(), port)
    }

    fn emitted_slot(&self, port: crate::relation::PortId) -> Result<usize> {
        self.identities().bindings().slot_at(self.site(), port)
    }

    fn emitted_physical_slot(&self, column: crate::names::ColId) -> Result<usize> {
        self.identities()
            .bindings()
            .physical_slot_at(self.site(), column)?
            .ok_or_else(|| {
                crate::error::DelightQLError::parse_error(format!(
                    "physical column {column:?} is absent from this exact SQL site"
                ))
            })
    }

    fn emitted_sites(&self) -> Vec<crate::sql_binding::SqlSiteId> {
        vec![self.site()]
    }
}

/// One emitting qualifier's `Qualify` answers, all four from its site.
macro_rules! qualifies_by_emitting {
    () => {
        fn sql_sites(&self) -> Vec<crate::sql_binding::SqlSiteId> {
            <Self as crate::pipeline::transformer::builder::Emitted>::emitted_sites(self)
        }
        fn rebind_port(&self, port: crate::relation::PortId) -> Result<crate::names::ColId> {
            <Self as crate::pipeline::transformer::builder::Emitted>::emitted_port(self, port)
        }
        fn slot_of_port(&self, port: crate::relation::PortId) -> Result<usize> {
            <Self as crate::pipeline::transformer::builder::Emitted>::emitted_slot(self, port)
        }
        fn slot_of_physical(&self, column: crate::names::ColId) -> Result<usize> {
            <Self as crate::pipeline::transformer::builder::Emitted>::emitted_physical_slot(
                self, column,
            )
        }
    };
}
pub(in crate::pipeline::transformer) use qualifies_by_emitting;

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
    site: crate::sql_binding::SqlSiteId,
    identities: std::rc::Rc<crate::names::Registry>,
}

impl CteInput {
    pub(super) fn new(
        scope: crate::names::ScopeId,
        columns: Vec<ColumnMetadata>,
        site: crate::sql_binding::SqlSiteId,
        identities: std::rc::Rc<crate::names::Registry>,
    ) -> Self {
        Self {
            scope,
            columns,
            site,
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

    crate::pipeline::transformer::builder::qualifies_by_emitting!();
}

impl crate::pipeline::transformer::builder::Emitting for CteInput {
    fn site(&self) -> crate::sql_binding::SqlSiteId {
        self.site
    }
}

/// What a `push_cte` closure returns: the CTE body and its output columns.
pub struct CteBody {
    /// The query expression that defines this CTE.
    pub query: QueryExpression,
    /// Column names this CTE produces. The builder assigns qualifiers
    /// (using the auto-generated CTE name).
    pub output_columns: Vec<crate::names::ColId>,
    /// For every output, the exact input slot it carries. `None` marks an
    /// output computed by this physical layer rather than a carried slot.
    pub input_slots: Vec<Option<usize>>,
    /// The exact physical aliases the body used before standing at its own
    /// SQL scope, in output order.
    pub physical_aliases: Vec<crate::names::ColId>,
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
    pub(in crate::pipeline::transformer) fn publication(&self) -> &SqlLayout {
        self.state.publication()
    }

    /// Complete one semantic-to-physical site after an exact relational
    /// operation has produced its builder.
    pub(in crate::pipeline::transformer) fn bind_relation(
        mut self,
        relation: crate::relation::SemanticRelation,
        sealed: &crate::relation::Relations,
    ) -> Result<Self> {
        self.state
            .publication_mut()
            .bind(&relation, sealed, &self.identities)?;
        Ok(self)
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
                scope: SqlLayout::new(scope, columns, &identities),
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
        query: QueryExpression,
        scope_name: ScopeName,
        columns: Vec<ColumnMetadata>,
        names: NameGenerator,
        identities: std::rc::Rc<crate::names::Registry>,
    ) -> Result<Self> {
        // A FROZEN QUERY IS STILL AN EMISSION: its columns get their slot
        // identities here, so the layer it becomes has a site like every
        // other. Which semantic ports those slots carry is a later act's.
        let physical: Vec<crate::names::ColId> =
            columns.iter().map(ColumnMetadata::identity).collect();
        let site = identities.bindings().bind_physical(&physical);
        Self::from_frozen_at_site(query, scope_name, columns, names, identities, site)
    }

    pub(in crate::pipeline::transformer) fn from_frozen_at_site(
        mut query: QueryExpression,
        scope_name: ScopeName,
        columns: Vec<ColumnMetadata>,
        names: NameGenerator,
        identities: std::rc::Rc<crate::names::Registry>,
        prior_site: crate::sql_binding::SqlSiteId,
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
            let reminted = remint_heading(columns.clone(), &identities, at);
            let renames: Vec<_> = columns
                .iter()
                .zip(&reminted)
                .filter(|(source, target)| source.identity() != target.identity())
                .map(|(source, target)| (source.identity(), target.identity()))
                .collect();
            state::rewrite_output_aliases(&mut query, at, &renames, &identities)?;
            reminted
        };
        let mut scope = SqlLayout::new(at, columns, &identities);
        scope.resite(prior_site, layout::Resite::Rebound, &identities)?;
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
    /// Put this level in the state a predicate will be ADDED to, before the
    /// predicate is lowered.
    ///
    /// A filter may need a level of its own — a grouping or a row bound
    /// stands under it — and the references it lowers must name the SQL
    /// aliases that level actually emits. The same reason
    /// `into_join_operand` prepares a join's sides before its condition.
    pub(in crate::pipeline::transformer) fn ready_for_filter(mut self) -> Result<Self> {
        self.state = self.state.ensure_filterable(&self.names)?;
        Ok(self)
    }

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
        let (table, publication) = self.state.into_table_expr(&self.names)?;
        Ok(JoinOperand {
            table,
            publication,
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
    /// `emitted_swapped` says the SQL operands are the semantic operands the
    /// other way round: a RIGHT OUTER join is emitted as a LEFT one over
    /// swapped sides so every target can spell it. THE RELATION'S POSITIONS
    /// DID NOT MOVE — the binding pairs them with the emitted list by
    /// position — so the published block follows the interface while the
    /// FROM clause follows the emission.
    pub(in crate::pipeline::transformer) fn from_join(
        left: JoinOperand,
        right: JoinOperand,
        kind: JoinType,
        condition: JoinCondition,
        emitted_swapped: bool,
    ) -> Result<Self> {
        Self::from_join_with_merge_sources(
            left,
            right,
            kind,
            condition,
            emitted_swapped,
            MergeSources::Collapsed,
        )
    }

    fn from_join_with_merge_sources(
        left: JoinOperand,
        mut right: JoinOperand,
        kind: JoinType,
        condition: JoinCondition,
        emitted_swapped: bool,
        merge_sources: MergeSources,
    ) -> Result<Self> {
        if let (Some(left_scope), Some(right_scope)) =
            (effective_scope(&left.table), effective_scope(&right.table))
        {
            if left_scope == right_scope {
                let fresh_scope = left.names.emission_alias(right_scope).identity();
                right.table = TableExpression::Scope(fresh_scope);
                // THE OPERAND RE-PUBLISHES ITSELF. What it emits and where
                // that stands travel together, so the re-aliased side is
                // this side re-stated rather than a layout assembled from a
                // scope, a heading and a site picked separately.
                right.publication = right
                    .publication
                    .requalified(fresh_scope, &left.identities)?;
            }
        }
        let join_expr = TableExpression::Join {
            left: Box::new(left.table),
            right: Box::new(right.table),
            join_type: kind,
            join_condition: condition.clone(),
        };
        let split_support = |publication: &SqlLayout| -> Result<(Vec<_>, Vec<_>)> {
            let mut semantic = Vec::new();
            let mut support = Vec::new();
            for column in publication.outputs() {
                let is_support = left
                    .identities
                    .bindings()
                    .is_support(publication.site(), column.identity())?;
                if is_support {
                    support.push(column.clone());
                } else {
                    semantic.push(column.clone());
                }
            }
            Ok((semantic, support))
        };
        let (mut columns, left_support) = split_support(&left.publication)?;
        let (right_columns, right_support) = split_support(&right.publication)?;
        let mut emitted_left_width = columns.len();
        let mut merge_aliases = Vec::new();
        // A MERGED PAIR IS PUBLISHED ONCE. Semantic construction records
        // both operand ports against the result port the SEMANTIC LEFT
        // contributed, so the physical site keeps that slot and binding
        // translates the other side's reference to it without a hidden
        // output column. A right outer join is emitted with its operands
        // exchanged, and then the slot to keep is the SQL right's.
        match &condition {
            JoinCondition::Merge(pairs) if emitted_swapped => {
                let merged: Vec<_> = pairs.iter().map(|pair| pair.left).collect();
                if merge_sources == MergeSources::Collapsed {
                    merge_aliases.extend(pairs.iter().map(|pair| (pair.left, pair.right)));
                }
                columns.retain(|column| !merged.contains(&column.identity()));
                emitted_left_width = columns.len();
                columns.extend(right_columns);
            }
            JoinCondition::Merge(pairs) => {
                if merge_sources == MergeSources::Collapsed {
                    merge_aliases.extend(pairs.iter().map(|pair| (pair.right, pair.left)));
                }
                for column in right_columns {
                    let merged = pairs.iter().any(|pair| pair.right == column.identity());
                    if !merged {
                        columns.push(column);
                    }
                }
            }
            _ => {
                columns.extend(right_columns);
            }
        }
        if emitted_swapped {
            columns.rotate_left(emitted_left_width);
        }
        columns.extend(left_support);
        columns.extend(right_support);
        let names = left.names;
        let identities = left.identities;
        let operand_sites = [left.publication.site(), right.publication.site()];
        let mut ctes = left.ctes;
        ctes.extend(right.ctes);
        let join_scope_name = names.join();
        // A flat join emits the operand slots under the operand aliases.
        // Reminting them into the synthetic join scope would bind every port
        // to a qualifier no FROM entry emits (`j_*.column`).
        let mut scope = SqlLayout::new(join_scope_name.identity(), columns, &identities);
        scope.recognize_merge_aliases(&merge_aliases, &identities)?;
        scope.recognize_operand_aliases(&operand_sites, &identities)?;
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

    /// Join two operands FULL OUTER on merged pairs, projecting each merged
    /// column as `COALESCE(left.col, right.col)`.
    ///
    /// The merged column must carry the key of WHICHEVER side is present.
    /// A one-sided qualified projection (`left.col`) is NULL on exactly
    /// the other side's orphan rows — the rows full outer exists to keep.
    pub(in crate::pipeline::transformer) fn from_join_full_outer_merge(
        left: JoinOperand,
        right: JoinOperand,
        pairs: Vec<crate::pipeline::sql_ast::MergedSlots>,
    ) -> Result<Builder<Projected>> {
        let coalesce_sides = pairs.clone();

        let joined = Self::from_join_with_merge_sources(
            left,
            right,
            JoinType::Full,
            JoinCondition::Merge(pairs),
            false,
            MergeSources::RetainedForProjection,
        )?;

        let scope_items = joined
            .publication()
            .select_items(&joined.identities, Hygiene::Drop);
        let items: Vec<SelectItem> = scope_items
            .into_iter()
            .map(|item| {
                let crate::pipeline::sql_ast::Publishes::One(column) = item.publishes() else {
                    return item;
                };
                match coalesce_sides.iter().find(|pair| pair.left == column) {
                    Some(pair) => item.with_expr(DomainExpression::Function {
                        name: "coalesce".into(),
                        args: vec![
                            DomainExpression::Column(pair.left),
                            DomainExpression::Column(pair.right),
                        ],
                        distinct: false,
                    }),
                    None => item,
                }
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
        let mut acc_columns = first.publication.outputs().to_vec();
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
            acc_columns.extend(operand.publication.outputs().to_vec());
            acc_ctes.extend(operand.ctes);
        }

        let join_origin = pipe_origin(&acc_columns, &identities);
        let join_scope_name = names.fresh(join_origin);
        // The heading stays on the operand occurrences: a flat join's FROM
        // carries the operand aliases and nothing else, so a heading
        // reminted into the join scope would be published by no alias any
        // reference can render against. The scope id stamps the segment; it
        // owns no columns, and the publication says so.
        let scope = SqlLayout::new(join_scope_name.identity(), acc_columns, &identities);
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
                    scope: SqlLayout::new(scope, columns, &self.identities),
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
                let origin = wrap_origin(
                    &input_columns,
                    &self.identities,
                    crate::names::WrapReason::Projection,
                );
                let identity_scope = self.names.fresh(origin).identity();
                let (items, output_columns) = derive_columns_from_items(
                    items,
                    identity_scope,
                    &input_columns,
                    &self.identities,
                );
                Ok(Builder {
                    state: BuilderState::Select {
                        select: select.set_select(items),
                        has_projection: true,
                        has_group_by,
                        scope: SqlLayout::new(identity_scope, output_columns, &self.identities),
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

    /// Append SQL-only outputs while carrying the exact semantic site prefix.
    pub(in crate::pipeline::transformer) fn add_support_projection(
        self,
        items: Vec<SelectItem>,
    ) -> Result<Builder<Projected>> {
        let prior = self.state.publication().site();
        let mut projected = self.add_projection(items)?;
        projected.state.publication_mut().resite(
            prior,
            layout::Resite::Extended,
            &projected.identities,
        )?;
        Ok(projected)
    }

    /// Select exact physical positions while preserving every semantic port.
    pub(in crate::pipeline::transformer) fn select_physical_projection(
        self,
        items: Vec<SelectItem>,
        selected: &[usize],
    ) -> Result<Builder<Projected>> {
        let prior = self.state.publication().site();
        let mut projected = self.add_projection(items)?;
        projected.state.publication_mut().resite(
            prior,
            layout::Resite::Projected(selected),
            &projected.identities,
        )?;
        Ok(projected)
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
                        scope: SqlLayout::new(scope, columns, &self.identities),
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
                        scope: SqlLayout::new(identity_scope, output_columns, &self.identities),
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
            BuilderState::Select {
                select,
                scope,
                has_projection,
                ..
            } => {
                if hygiene == Hygiene::Drop {
                    scope.prune_hygienic(&self.identities)?;
                }
                let needs_output_scope = scope
                    .outputs()
                    .iter()
                    .any(|column| self.identities.scope_of(column.identity()) != scope.at_scope());
                let items = if needs_output_scope {
                    let published = scope.requalified(scope.at_scope(), &self.identities)?;
                    let items = scope
                        .outputs()
                        .iter()
                        .zip(published.outputs())
                        .map(|(source, output)| SelectItem::Publishing {
                            expr: DomainExpression::Column(source.identity()),
                            slot: output.identity(),
                            printed: true,
                        })
                        .collect();
                    *scope = published;
                    // This level is now the ONE definition of the
                    // republished occurrences: a later re-projection must
                    // wrap it, never replace it, or every reference to an
                    // output names a select that no longer exists. The
                    // same-scope passthrough below stays replaceable.
                    *has_projection = true;
                    items
                } else {
                    scope.select_items(&self.identities, Hygiene::Carry)
                };
                let items = if items.is_empty() {
                    vec![SelectItem::star_over_nothing()]
                } else {
                    items
                };
                let taken = std::mem::replace(select, SelectBuilder::new());
                *select = taken.set_select(items);
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
    /// Expressions that will be used in ORDER BY or WHERE must be lowered
    /// against the post-wrap scope, so call this first.
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
        // THE ANCHOR KEEPS THE OCCURRENCE IT WAS MINTED AS. Every expression
        // that reads it was written against that occurrence before this
        // level existed, so a projection that reminted the alias would leave
        // all of them naming a column no statement emits. The level stands
        // at the scope its input stands at, which is the scope the anchors
        // were minted into.
        let scope = self.publication().at_scope();
        let prior = self.publication().site();
        let mut columns = self.columns().to_vec();
        let mut items: Vec<SelectItem> = columns
            .iter()
            .map(|column| SelectItem::Publishing {
                expr: DomainExpression::Column(column.identity()),
                slot: column.identity(),
                printed: true,
            })
            .collect();
        for (expr, alias) in values {
            items.push(SelectItem::Publishing {
                expr,
                slot: alias,
                printed: true,
            });
            columns.push(ColumnMetadata::new(alias));
        }
        // The anchors are SQL-only support: the semantic interface this level
        // already realizes is the exact prefix of what it now emits, so the
        // binding carries through rather than being abandoned.
        let mut projected = self.add_projection_publishing(items, scope, columns)?;
        projected.state.publication_mut().resite(
            prior,
            layout::Resite::Extended,
            &projected.identities,
        )?;
        projected.demote()
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
        context_items_fn: impl FnOnce(&[crate::names::ColId], usize) -> Vec<(usize, SelectItem)>,
        interior_items_fn: impl FnOnce(crate::names::ColId, crate::names::ColId) -> Vec<SelectItem>,
        groundings: &[crate::pipeline::asts::core::operators::ResolvedInteriorGrounding],
    ) -> Result<Builder<Projected>> {
        let identities = std::rc::Rc::clone(&self.identities);
        let names = self.names().fork();
        let column = self.rebind_physical(column)?;
        let source_slot = self
            .columns()
            .iter()
            .position(|candidate| candidate.identity() == column)
            .ok_or_else(|| {
                crate::error::DelightQLError::parse_error(
                    "json expansion source column is not in the input heading",
                )
            })?;
        // Take the heading from the PROJECTED builder, not from this one.
        // `project_all` drops hygienic columns from both the select list and
        // the scope, so a heading read beforehand claims columns the subquery
        // below will not output — and every one of them is then republished
        // into the source scope, where the expansion's context items name it
        // and no FROM entry offers it.
        let projected = self.project_all_carrying_hygiene()?;
        let source_site = projected.publication().site();
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
        let source_column = source_columns[source_slot];

        let tvf_scope = names.interior_emission(column).identity();
        let key_spelling = identities.intern("key", false);
        let value_spelling = identities.intern("value", false);
        let key_column = identities.sql_column(
            tvf_scope,
            Some(key_spelling),
            crate::names::Addressing::Published,
        );
        let value_column = identities.sql_column(
            tvf_scope,
            Some(value_spelling),
            crate::names::Addressing::Published,
        );

        let context = context_items_fn(&source_columns, source_slot);
        let mut layout: Vec<Option<usize>> =
            context.iter().map(|(source, _)| Some(*source)).collect();
        let mut items: Vec<_> = context.into_iter().map(|(_, item)| item).collect();
        let interior = interior_items_fn(key_column, value_column);
        layout.extend(std::iter::repeat_n(None, interior.len()));
        items.extend(interior);
        let output_scope = names.join().identity();
        let mut inputs: Vec<_> = source_metadata.clone();
        inputs.push(ColumnMetadata::new(key_column));
        inputs.push(ColumnMetadata::new(value_column));
        let (items, columns) = derive_columns_from_items(items, output_scope, &inputs, &identities);
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
            let path = DomainExpression::PublishedJsonPathLiteral(grounding.column.column());
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
        let select = (select)
            .standing_at(output_scope)
            .map_err(crate::error::DelightQLError::parse_error)?;
        let query = QueryExpression::Select(Box::new(select));
        let mut result = Builder::from_query(
            query,
            ScopeName::Resolved(output_scope),
            columns,
            names,
            identities,
        )?;
        result.state.publication_mut().resite(
            source_site,
            layout::Resite::Reshaped(&layout),
            &result.identities,
        )?;
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Builder<Projected> — finishing phase
// ---------------------------------------------------------------------------

impl Builder<Projected> {
    /// Adopt a finished query that already publishes the heading it claims.
    ///
    /// Used when embedding a pre-built query (e.g., from ResolverCore)
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
        let scope = SqlLayout::new(scope_name.into_scope(), columns, &identities);
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
                    scope: SqlLayout::new(scope, columns, &self.identities),
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
                );
                let select = select.set_select(items);
                Ok(Self {
                    state: BuilderState::Select {
                        select,
                        has_projection: true,
                        has_group_by,
                        scope: SqlLayout::new(identity_scope, output_columns, &self.identities),
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
        let before_wrap = self
            .state
            .publication()
            .outputs()
            .iter()
            .map(ColumnMetadata::identity)
            .collect::<Vec<_>>();
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
                let input_site = input_scope.site();
                let input_columns = input_scope.outputs().to_vec();
                if before_wrap.len() != input_columns.len() {
                    return Err(crate::error::DelightQLError::parse_error(
                        "a window wrap changed the width of its exact input publication",
                    ));
                }
                let wrapped_positions = before_wrap
                    .iter()
                    .copied()
                    .zip(input_columns.iter().map(ColumnMetadata::identity))
                    .collect::<std::collections::HashMap<_, _>>();

                let failure = std::cell::RefCell::new(None);
                let reanchor = |column: crate::names::ColId| -> crate::names::ColId {
                    if let Some(landed) = wrapped_positions.get(&column) {
                        return *landed;
                    }
                    let answer = wrapped
                        .identities
                        .bindings()
                        .physical_at(input_scope.site(), column);
                    match answer {
                        Ok(Some(landed)) => landed,
                        Ok(None) => column,
                        Err(error) => {
                            failure.borrow_mut().get_or_insert(error);
                            column
                        }
                    }
                };
                let window_item = SelectItem::Publishing {
                    expr: DomainExpression::WindowFunction {
                        name: func_name.to_string(),
                        args,
                        distinct: false,
                        partition_by,
                        order_by,
                        frame: None,
                    }
                    .map_columns(&reanchor),
                    slot: alias,
                    printed: true,
                };
                if let Some(error) = failure.into_inner() {
                    return Err(error);
                }
                let origin = wrap_origin(
                    &input_columns,
                    &wrapped.identities,
                    crate::names::WrapReason::Projection,
                );
                let fresh_scope = wrapped.names.fresh(origin);
                let identity_scope = fresh_scope.identity();

                // Spell the carried positions out. A star has no SQL syntax
                // with which a later wrapping alias can rename its individual
                // outputs, while this support layer must carry the exact
                // positions and append one physical-only slot.
                let mut items = input_scope.select_items(&wrapped.identities, Hygiene::Carry);
                items.push(window_item);
                let (items, output_columns) = derive_columns_from_items(
                    items,
                    identity_scope,
                    &input_columns,
                    &wrapped.identities,
                );
                let select = select.set_select(items);

                let mut publication =
                    SqlLayout::new(identity_scope, output_columns, &wrapped.identities);
                publication.resite(input_site, layout::Resite::Extended, &wrapped.identities)?;
                Ok(Self {
                    state: BuilderState::Select {
                        select,
                        has_projection: true,
                        has_group_by,
                        scope: publication,
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

    // --- Set operations ---

    /// Stack two branches under the publication the SET RESULT already owns.
    ///
    /// The positions a set publishes are the result relation's, decided when
    /// the authority derived it. Minting a scope over the stack instead
    /// would publish positions nothing was addressed against and leave the
    /// semantic ones standing for no emitted column — which is the recovery
    /// question the physical binding exists to make unnecessary.
    ///
    /// Both branches are rewritten onto those positions, so what the
    /// combined statement outputs and what it claims to publish are one act.
    /// Stack two branches under one result, in the accumulation's flavor.
    ///
    /// `accumulation` is the SQL operator the DQL accumulation resolved to
    /// — `UNION ALL` for a bag, `UNION` for a `%`-badged fixpoint's clauses.
    /// The flavor is CARRIED here, never read back off the stacked tree.
    pub fn stack_at(
        self,
        right: Self,
        accumulation: crate::pipeline::sql_ast::SetOperator,
        at: crate::names::ScopeId,
        outputs: &[crate::names::ColId],
    ) -> Result<Self> {
        let mut ctes = self.accumulated_ctes;
        ctes.extend(right.accumulated_ctes);
        let mut stacked = Vec::with_capacity(2);
        for (state, names) in [(self.state, &self.names), (right.state, &right.names)] {
            let branch = state.publication().clone();
            if branch.outputs().len() != outputs.len() {
                return Err(crate::error::DelightQLError::parse_error(format!(
                    "a set branch publishing {} positions cannot stack under a result \
                     publishing {}",
                    branch.outputs().len(),
                    outputs.len()
                )));
            }
            let paired: Vec<_> = branch
                .outputs()
                .iter()
                .zip(outputs)
                .map(|(source, target)| (source.identity(), *target))
                .collect();
            let mut query = state.materialize(names)?;
            state::rewrite_output_aliases(&mut query, at, &paired, &self.identities)?;
            stacked.push(query);
        }
        let right_query = stacked.pop().expect("two branches");
        let left_query = stacked.pop().expect("two branches");
        Ok(Self {
            state: BuilderState::Frozen {
                query: QueryExpression::SetOperation {
                    op: accumulation,
                    left: Box::new(left_query),
                    right: Box::new(right_query),
                },
                scope: SqlLayout::new(
                    at,
                    outputs.iter().copied().map(ColumnMetadata::new).collect(),
                    &self.identities,
                ),
            },
            names: self.names,
            identities: self.identities,
            accumulated_ctes: ctes,
            _phase: PhantomData,
        })
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
                let requalified = scope_clone.requalified(source_identity, &self.identities)?;
                state::rewrite_output_aliases(
                    &mut source_query,
                    source_identity,
                    &scope_clone.pairs_with(&requalified),
                    &self.identities,
                )?;
                self.accumulated_ctes
                    .push(Cte::ordinary(source_identity, source_query));
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
            current_scope.site(),
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

        let output_columns =
            build_cte_output_columns(&cte_body.output_columns, &self.identities, cte_identity);
        if cte_body.input_slots.len() != output_columns.len() {
            return Err(crate::error::DelightQLError::parse_error(
                "a CTE body and its physical slot map have different widths",
            ));
        }

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
            .push(Cte::ordinary(cte_identity, cte_body.query));

        // Transition to a new Select FROM the CTE.
        // The old state is discarded — the closure already used it to build the CTE body.
        // The new state references the CTE by name so that subsequent operations
        // (add_projection, another push_cte) operate on the CTE's output.
        let cte_table = TableExpression::Scope(cte_identity);
        let mut new_scope = SqlLayout::new(cte_identity, output_columns, &self.identities);
        new_scope.resite(
            current_scope.site(),
            layout::Resite::Reshaped(&cte_body.input_slots),
            &self.identities,
        )?;
        let aliased = new_scope.site();
        new_scope.resite(
            aliased,
            layout::Resite::Aliased(&cte_body.output_columns),
            &self.identities,
        )?;
        let aliased = new_scope.site();
        new_scope.resite(
            aliased,
            layout::Resite::Aliased(&cte_body.physical_aliases),
            &self.identities,
        )?;
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

    crate::pipeline::transformer::builder::qualifies_by_emitting!();
}

impl<P> crate::pipeline::transformer::builder::Emitting for Builder<P> {
    fn site(&self) -> crate::sql_binding::SqlSiteId {
        self.state.publication().site()
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
    /// WHAT THIS SIDE PUBLISHES, WHOLE. The heading and the physical slots
    /// it stands on are one value, taken from the level this operand was
    /// prepared at — so an operand cannot be re-aliased by pairing a
    /// heading with a binding site chosen anywhere else.
    publication: SqlLayout,
    pub names: NameGenerator,
    pub identities: std::rc::Rc<crate::names::Registry>,
    pub ctes: Vec<Cte>,
}

impl Qualify for JoinOperand {
    fn identities(&self) -> &crate::names::Registry {
        &self.identities
    }

    fn scope_columns(&self) -> Vec<ColumnMetadata> {
        self.publication.outputs().to_vec()
    }

    crate::pipeline::transformer::builder::qualifies_by_emitting!();
}

impl crate::pipeline::transformer::builder::Emitting for JoinOperand {
    fn site(&self) -> crate::sql_binding::SqlSiteId {
        self.publication.site()
    }
}

impl JoinOperand {
    /// Everything this side publishes, in heading order.
    pub fn columns(&self) -> &[ColumnMetadata] {
        self.publication.outputs()
    }

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
                    *column = scope.rebind_physical(*column)?;
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

/// Qualify references written against an operation result while its SQL is
/// still being assembled from operand sites.
///
/// Refinement may move a predicate from a join result into the join itself.
/// Its ports remain ports of that exact result; the construction-owned
/// ancestry is the only evidence that can translate them onto an operand.
pub(in crate::pipeline::transformer) struct AncestralQualify<'a> {
    operands: &'a dyn Qualify,
    /// THE TOTAL MAP, built ONCE from the construction record.
    ///
    /// Every position of the operation that the operands realize — its own
    /// ports and the ports of relations refinement recorded it as
    /// replacing — paired with the ONE operand column that realizes it.
    /// Built here, at construction, over the enumerable set the record
    /// names: nothing below searches, and nothing below picks a winner
    /// among candidates one reference at a time.
    landed: std::collections::HashMap<crate::relation::PortId, crate::names::ColId>,
    /// The record the lazy descent below reads: a recorded pair may name a
    /// port of a relation the rebuild replaced without a replacement row —
    /// its own carry edges still say which base positions realize it.
    relations: crate::relation::Relations,
}

impl<'a> AncestralQualify<'a> {
    /// PRODUCE THE MAP FOR THIS OPERATION OVER THESE OPERAND SITES.
    ///
    /// Refinement may move a predicate from a join result into the join
    /// itself. Its ports remain ports of that exact result; the
    /// construction-owned ancestry is what carries them onto an operand,
    /// and it is read HERE — once, for every position at once — rather
    /// than consulted per reference.
    ///
    /// A position several operand columns realize is left OUT of the map:
    /// a reference to it has no one column it could mean, and the refusal
    /// belongs where the reference is written, naming the port.
    pub(in crate::pipeline::transformer) fn over(
        operation: &crate::relation::SemanticRelation,
        relations: &crate::relation::Relations,
        operands: &'a dyn Qualify,
    ) -> Result<Self> {
        let mut landed = std::collections::HashMap::new();
        let published = relations.interface(operation)?.ports().to_vec();
        // Each position of the operation, and each position of a relation
        // this operation replaced — both are things a reference here can
        // name, and the record says which output of this operation each
        // one became.
        // EVERY construction ancestor of an output can be named by a moved
        // predicate — a merged key two joins deep as much as a directly
        // replaced port — and each maps to the output its carry chain
        // reaches. The walk reads recorded edges only.
        let mut sources: Vec<(crate::relation::PortId, crate::relation::PortId)> = Vec::new();
        for port in &published {
            let mut frontier = vec![*port];
            let mut seen: Vec<crate::relation::PortId> = Vec::new();
            while let Some(ancestor) = frontier.pop() {
                if seen.contains(&ancestor) {
                    continue;
                }
                seen.push(ancestor);
                sources.push((ancestor, *port));
                frontier.extend(relations.carried_from(ancestor));
            }
        }
        for (old, new) in relations.translated_ports(operation)? {
            sources.push((old, new));
        }
        for (named, output) in sources {
            if landed.contains_key(&named) {
                continue;
            }
            // The walk DESCENDS the carry edges construction wrote and
            // stops each branch the moment an operand realizes it: a
            // merged key two joins deep is still the record's answer, one
            // recorded edge at a time. Nothing here reads position,
            // spelling, or width.
            let mut columns = Vec::new();
            let mut frontier = vec![output];
            let mut walked: Vec<crate::relation::PortId> = Vec::new();
            while let Some(port) = frontier.pop() {
                if walked.contains(&port) {
                    continue;
                }
                walked.push(port);
                if port != named {
                    if let Ok(column) = operands.rebind_port(port) {
                        if !columns.contains(&column) {
                            columns.push(column);
                        }
                        continue;
                    }
                }
                frontier.extend(relations.carried_from(port));
            }
            if let [column] = columns.as_slice() {
                landed.insert(named, *column);
            }
        }
        Ok(AncestralQualify {
            operands,
            landed,
            relations: relations.clone(),
        })
    }
}

impl Qualify for AncestralQualify<'_> {
    fn identities(&self) -> &crate::names::Registry {
        self.operands.identities()
    }

    fn rebind_port(&self, port: crate::relation::PortId) -> Result<crate::names::ColId> {
        // AN OPERAND'S OWN POSITION IS THE OPERAND'S ANSWER.
        if let Ok(column) = self.operands.rebind_port(port) {
            return Ok(column);
        }
        if let Some(column) = self.landed.get(&port) {
            return Ok(*column);
        }
        // A recorded pair may name a port of a relation the rebuild stood
        // over without writing a replacement row for it — an intermediate
        // join's merged output. Its own carry edges still say which
        // positions realize it: descend them, stopping each branch at the
        // first position an operand emits. One column is the answer;
        // several is an ambiguity, and none is the refusal below.
        let mut columns: Vec<crate::names::ColId> = Vec::new();
        let mut frontier = self.relations.carried_from(port);
        let mut walked: Vec<crate::relation::PortId> = vec![port];
        while let Some(ancestor) = frontier.pop() {
            if walked.contains(&ancestor) {
                continue;
            }
            walked.push(ancestor);
            if let Ok(column) = self.operands.rebind_port(ancestor) {
                if !columns.contains(&column) {
                    columns.push(column);
                }
                continue;
            }
            if let Some(column) = self.landed.get(&ancestor) {
                if !columns.contains(column) {
                    columns.push(*column);
                }
                continue;
            }
            frontier.extend(self.relations.carried_from(ancestor));
        }
        if let [column] = columns.as_slice() {
            return Ok(*column);
        }
        Err(crate::error::DelightQLError::parse_error(format!(
            "an operation-result port {port:?} has no construction-recorded \
             physical operand among {:?}",
            self.operands.sql_sites(),
        )))
    }

    fn sql_sites(&self) -> Vec<crate::sql_binding::SqlSiteId> {
        self.operands.sql_sites()
    }

    // A POSITION IS THE OPERANDS' ANSWER. This view translates a port onto
    // an operand; where the operand lays it out is the operand's own
    // business, and asking here would be asking a view for a layout it
    // does not own.
    fn slot_of_port(&self, port: crate::relation::PortId) -> Result<usize> {
        self.operands.slot_of_port(port)
    }

    fn slot_of_physical(&self, column: crate::names::ColId) -> Result<usize> {
        self.operands.slot_of_physical(column)
    }

    fn scope_columns(&self) -> Vec<ColumnMetadata> {
        self.operands.scope_columns()
    }
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

    fn rebind_port(&self, port: crate::relation::PortId) -> Result<crate::names::ColId> {
        match self.inner.rebind_port(port) {
            Ok(column) => Ok(column),
            Err(inner) => self.outer.rebind_port(port).or(Err(inner)),
        }
    }
    fn sql_sites(&self) -> Vec<crate::sql_binding::SqlSiteId> {
        let mut sites = self.inner.sql_sites();
        sites.extend(self.outer.sql_sites());
        sites
    }

    // A CHAIN OF TWO SCOPES LAYS OUT NEITHER. The inner one answers where
    // it can and the outer one after it; a POSITION belongs to whichever
    // one holds the reference, and that one is asked directly.
    fn slot_of_port(&self, port: crate::relation::PortId) -> Result<usize> {
        match self.inner.slot_of_port(port) {
            Ok(slot) => Ok(slot),
            Err(inner) => self.outer.slot_of_port(port).or(Err(inner)),
        }
    }

    fn slot_of_physical(&self, column: crate::names::ColId) -> Result<usize> {
        match self.inner.slot_of_physical(column) {
            Ok(slot) => Ok(slot),
            Err(inner) => self.outer.slot_of_physical(column).or(Err(inner)),
        }
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
    fn into_table_expr(self, names: &NameGenerator) -> Result<(TableExpression, SqlLayout)> {
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
                let new_scope = scope.requalified(identity, names.identities())?;
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
) -> (Vec<SelectItem>, Vec<ColumnMetadata>) {
    let mut columns = Vec::new();
    let mut out_items = Vec::with_capacity(items.len());
    for item in items {
        match item {
            SelectItem::Star { reads, .. } => {
                // The star's expansion is rewritten here, not carried over:
                // this projection republishes every input into its own scope,
                // so what the star stands for downstream is the occurrences
                // just minted, not the ones it stood for above. With the
                // resolver's heading in hand the star republishes THOSE
                // occurrences through an emission wrap — the projection is
                // the same relation re-staged for SQL, so ownership reports
                // read through it to the scope the resolver bound.
                let mut expansion = Vec::with_capacity(input_columns.len());
                for col in input_columns {
                    let source = col.identity();
                    let output =
                        identities.rebind_sql_column(source, scope, identities.published(source));
                    columns.push(ColumnMetadata::new(output));
                    expansion.push(output);
                }
                out_items.push(SelectItem::Star { reads, expansion });
            }
            SelectItem::Publishing {
                expr,
                slot,
                printed,
            } => {
                let alias = printed.then_some(slot);
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
                    Some(source) => identities.rebind_sql_column(source, scope, published),
                    None => identities.sql_column(scope, published, addressing),
                };
                columns.push(ColumnMetadata::new(output));
                out_items.push(SelectItem::Publishing {
                    expr,
                    slot: output,
                    printed: true,
                });
            }
            // COMPILER SCAFFOLDING RIDES THROUGH. It publishes no
            // occurrence, so this projection has none to re-stage for it and
            // nothing downstream addresses the slot.
            SelectItem::Scaffolding { expr, slot } => {
                out_items.push(SelectItem::Scaffolding { expr, slot });
            }
        }
    }
    (out_items, columns)
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
    identities: &crate::names::Registry,
    scope: crate::names::ScopeId,
) -> Vec<ColumnMetadata> {
    output_columns
        .iter()
        .map(|output| {
            let identity =
                identities.rebind_sql_column(*output, scope, identities.published(*output));
            ColumnMetadata::new(identity)
        })
        .collect()
}
