// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Non-consuming whole-tree AST inspection/collection.
//!
//! `AstVisit<P>` is the read-only, non-cloning sibling of
//! [`AstTransform<P, Q>`](crate::pipeline::ast_transform). Where `AstTransform`
//! consumes a P-phase tree and rebuilds a Q-phase tree, `AstVisit` borrows a
//! single-phase tree (`&Node<P>`) and lets a caller INSPECT or COLLECT without
//! rebuilding. The default walk encodes the AST's COMPLETE structural recursion
//! ONCE, mirroring `walk_transform_*` edge-for-edge, in the same canonical
//! order. This is the durable whole-tree closure that INDUCTIVE-TRAVERSAL-PLAN
//! §4 mandates the analysis layer share.
//!
//! # Two distinct axes of caller control (PLAN §4; INVENTORY §5)
//!
//! 1. **Boundary / descent** — every `enter_*` hook returns a [`Descent`]:
//!    - [`Descent::Continue`] — descend into this node's children (default);
//!    - [`Descent::SkipSubtree`] — the caller handled this node; do NOT descend
//!      its children, and do NOT call its `exit_*` hook (the node is treated as
//!      fully handled). This is how a scope-local caller draws a boundary at a
//!      nested subquery (INVENTORY §5 finding 1), or any caller prunes CTE
//!      bodies / consulted-view bodies / operator-arg subqueries / HO args
//!      (findings 4–8, 10);
//!    - [`Descent::Break`] — stop the ENTIRE walk immediately (finding 12).
//! 2. **Order** — paired `enter_*` (pre-order) / `exit_*` (post-order) hooks on
//!    the SAME canonical walk. Directive-demand order is semantically
//!    load-bearing (finding 11), so enter/exit is REQUIRED, not optional.
//!
//! Hooks carry caller state as struct fields on the implementor (exactly as the
//! `AstTransform` consumers hold their accumulator), may COLLECT into that
//! state, and may FAIL: every hook returns `Result<Descent>`, and an `Err`
//! short-circuits the walk via `?` (finding 13).
//!
//! # Relationship to `walk_transform_*`
//!
//! The walk reaches every recursive edge `walk_transform_*` reaches, including
//! `SetOperation.correlation` (borrowed here and consumed through the transform
//! walk's payload-specific map; INVENTORY §5 finding 9). The
//! `WithCfes.cfes` / `WithPrecompiledCfes.cfes`
//! bodies are NOT auto-descended (they live in a non-phase-parameterized,
//! Unresolved-pinned side structure the generic walk cannot type); a caller
//! that wants a CFE body walks it by ROOTING a fresh visit at the body, which
//! is exactly the W12 `collect_function_refs` pattern.
//!
//! # `AstVisitMut` is deliberately NOT built here
//!
//! Phase A did not demand an in-place mutable visitor (ruling R-I5): owned
//! same-phase rewrites ride `AstTransform<P, P>`, whose recursion is already
//! written. The one genuinely-mutable analysis case — the W8/W9 tree-group
//! naming walk (`_tg_N` assignment) — is DEFERRED to Phase D's decision, not
//! forced into a third recursion scheme now.

use crate::error::Result;
use crate::pipeline::asts::core::expressions::functions::{CaseArm, StringTemplatePart};
use crate::pipeline::asts::core::expressions::metadata_types::CteRequirements;
use crate::pipeline::asts::core::expressions::relational::InnerRelationPattern;
use crate::pipeline::asts::core::operators::{
    ColumnSelector, FrameBound, HoArgument, WindowFrame,
};
use crate::pipeline::asts::core::{
    ArrayMember, BooleanExpression, CteBinding, CurlyMember, DomainExpression, DomainSpec,
    FunctionExpression, ModuloSpec, OrderingSpec, PipeExpression, Query, Relation,
    RelationalExpression, RenameSpec, RepositionSpec, Row, SigmaCondition, UnaryRelationalOperator,
};

// =============================================================================
// Descent — the boundary/descent control signal
// =============================================================================

/// The control signal an `enter_*` hook returns to steer the walk.
///
/// This is the non-consuming analogue of `ast_transform::FoldAction`, but where
/// `FoldAction` chooses between re-descending or replacing a rebuilt node,
/// `Descent` chooses a BOUNDARY (`SkipSubtree`) or early exit (`Break`) — the
/// two axes INVENTORY §5 shows the whole-tree analyses actually need.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Descent {
    /// Descend into this node's children (the default).
    Continue,
    /// The caller handled this node here: do NOT descend its children, and do
    /// NOT call its `exit_*` hook. Prunes exactly this subtree; siblings still
    /// walk. Returning `SkipSubtree` from an `exit_*` hook is meaningless (there
    /// is nothing left to prune) and is treated as `Continue`.
    SkipSubtree,
    /// Stop the entire walk immediately. No further nodes (siblings, parents,
    /// or their `exit_*` hooks) are visited.
    Break,
}

// =============================================================================
// Control-flow macros (internal)
// =============================================================================

/// Run an `enter_*` hook and act on its `Descent`:
/// `Break` aborts the whole walk; `SkipSubtree` returns from THIS walk fn as a
/// completed node (no children, no exit); `Continue` falls through to descent.
macro_rules! enter {
    ($hook:expr) => {
        match $hook? {
            Descent::Break => return Ok(Descent::Break),
            Descent::SkipSubtree => return Ok(Descent::Continue),
            Descent::Continue => {}
        }
    };
}

/// Descend into one child walk; propagate a `Break` up immediately.
macro_rules! child {
    ($walk:expr) => {
        if let Descent::Break = $walk? {
            return Ok(Descent::Break);
        }
    };
}

/// Run an `exit_*` hook as the final act of a walk fn: `Break` aborts, anything
/// else completes this node normally.
macro_rules! exit {
    ($hook:expr) => {
        return match $hook? {
            Descent::Break => Ok(Descent::Break),
            _ => Ok(Descent::Continue),
        }
    };
}

// =============================================================================
// Trait
// =============================================================================

/// A non-consuming whole-tree inspection/collection over single-phase AST nodes.
///
/// Every primary recursive node type gets a paired `enter_*` / `exit_*` hook.
/// Defaults are inert (`enter_*` returns `Continue`, `exit_*` returns `Continue`)
/// and call NOTHING but the identity — override the hooks you care about and let
/// the `walk_visit_*` free functions do the structural descent, exactly as
/// `AstTransform` implementors override `transform_*` and lean on
/// `walk_transform_*`.
#[allow(unused_variables)]
pub trait AstVisit<P> {
    fn enter_query(&mut self, q: &Query<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }
    fn exit_query(&mut self, q: &Query<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }

    fn enter_relational(&mut self, e: &RelationalExpression<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }
    fn exit_relational(&mut self, e: &RelationalExpression<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }

    fn enter_relation(&mut self, r: &Relation<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }
    fn exit_relation(&mut self, r: &Relation<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }

    fn enter_boolean(&mut self, e: &BooleanExpression<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }
    fn exit_boolean(&mut self, e: &BooleanExpression<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }

    fn enter_domain(&mut self, e: &DomainExpression<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }
    fn exit_domain(&mut self, e: &DomainExpression<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }

    fn enter_function(&mut self, f: &FunctionExpression<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }
    fn exit_function(&mut self, f: &FunctionExpression<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }

    fn enter_operator(&mut self, o: &UnaryRelationalOperator<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }
    fn exit_operator(&mut self, o: &UnaryRelationalOperator<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }

    fn enter_sigma(&mut self, s: &SigmaCondition<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }
    fn exit_sigma(&mut self, s: &SigmaCondition<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }

    fn enter_pipe(&mut self, p: &PipeExpression<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }
    fn exit_pipe(&mut self, p: &PipeExpression<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }

    fn enter_inner_relation(&mut self, i: &InnerRelationPattern<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }
    fn exit_inner_relation(&mut self, i: &InnerRelationPattern<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }
}

// =============================================================================
// Walk functions — top-level
// =============================================================================

pub fn walk_visit_query<P, F: AstVisit<P> + ?Sized>(v: &mut F, query: &Query<P>) -> Result<Descent> {
    enter!(v.enter_query(query));
    match query {
        Query::Relational(expr) => child!(walk_visit_relational(v, expr)),
        Query::WithCtes { ctes, query } => {
            for c in ctes {
                child!(walk_visit_cte_binding(v, c));
            }
            child!(walk_visit_relational(v, query));
        }
        // `cfes` bodies live in a non-phase-parameterized, Unresolved-pinned side
        // structure the generic walk cannot type — mirror walk_transform_query
        // (which passes `cfes` through) and descend only the main query. A caller
        // wanting a CFE body roots a fresh visit at it (the W12 pattern).
        Query::WithCfes { query, .. }
        | Query::WithPrecompiledCfes { query, .. }
        | Query::ReplTempTable { query, .. }
        | Query::WithErContext { query, .. }
        | Query::ReplTempView { query, .. } => child!(walk_visit_query(v, query)),
    }
    exit!(v.exit_query(query));
}

pub fn walk_visit_cte_binding<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    cte: &CteBinding<P>,
) -> Result<Descent> {
    walk_visit_relational(v, &cte.expression)
}

// =============================================================================
// Walk functions — relational layer
// =============================================================================

#[stacksafe::stacksafe]
pub fn walk_visit_relational<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    expr: &RelationalExpression<P>,
) -> Result<Descent> {
    enter!(v.enter_relational(expr));
    match expr {
        RelationalExpression::Relation(rel) => child!(walk_visit_relation(v, rel)),
        RelationalExpression::Join {
            left,
            right,
            join_condition,
            ..
        } => {
            child!(walk_visit_relational(v, left));
            child!(walk_visit_relational(v, right));
            if let Some(c) = join_condition {
                child!(walk_visit_boolean(v, c));
            }
        }
        RelationalExpression::Filter {
            source, condition, ..
        } => {
            child!(walk_visit_relational(v, source));
            child!(walk_visit_sigma(v, condition));
        }
        RelationalExpression::Pipe(pipe) => {
            let pipe: &PipeExpression<P> = pipe;
            child!(walk_visit_pipe(v, pipe));
        }
        RelationalExpression::SetOperation {
            operands,
            correlation,
            ..
        } => {
            for e in operands {
                child!(walk_visit_relational(v, e));
            }
            // `correlation()` is the payload-specific
            // borrow (not a phase-generic one — see PhaseBox::correlation).
            if let Some(c) = correlation.correlation() {
                child!(walk_visit_boolean(v, c));
            }
        }
        RelationalExpression::IntersectCorresponding {
            operands,
            correlation,
            ..
        } => {
            for e in operands {
                child!(walk_visit_relational(v, e));
            }
            child!(walk_visit_boolean(v, correlation));
        }
        RelationalExpression::ErJoinChain { relations, .. } => {
            for r in relations {
                child!(walk_visit_relation(v, r));
            }
        }
        RelationalExpression::ErTransitiveJoin { left, right, .. } => {
            child!(walk_visit_relational(v, left));
            child!(walk_visit_relational(v, right));
        }
    }
    exit!(v.exit_relational(expr));
}

pub fn walk_visit_relation<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    rel: &Relation<P>,
) -> Result<Descent> {
    enter!(v.enter_relation(rel));
    match rel {
        Relation::Ground { domain_spec, .. } => child!(walk_visit_domain_spec(v, domain_spec)),
        Relation::Anonymous {
            column_headers,
            rows,
            ..
        } => {
            if let Some(headers) = column_headers {
                for h in headers {
                    child!(walk_visit_domain(v, h));
                }
            }
            for r in rows {
                child!(walk_visit_row(v, r));
            }
        }
        Relation::TVF {
            domain_spec,
            ho_arguments,
            ..
        } => {
            child!(walk_visit_domain_spec(v, domain_spec));
            for a in ho_arguments {
                match a {
                    HoArgument::Table(r) => child!(walk_visit_relational(v, r)),
                    HoArgument::Scalar(d) => child!(walk_visit_domain(v, d)),
                }
            }
        }
        Relation::InnerRelation { pattern, .. } => child!(walk_visit_inner_relation(v, pattern)),
        Relation::ConsultedView { body, .. } => child!(walk_visit_query(v, body)),
        Relation::PseudoPredicate {
            arguments, access, ..
        } => {
            for e in arguments {
                child!(walk_visit_domain(v, e));
            }
            // The receipt-access spec is a recursive field: a demand or
            // subquery embedded in it must be visible to every AstVisit
            // tenant (effect discipline, R9 discovery, compile purity).
            // A recursive field without its walker edge is the historical
            // bug class the inductive-traversal work exists to prevent.
            child!(walk_visit_domain_spec(v, access));
        }
    }
    exit!(v.exit_relation(rel));
}

pub fn walk_visit_inner_relation<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    pattern: &InnerRelationPattern<P>,
) -> Result<Descent> {
    enter!(v.enter_inner_relation(pattern));
    match pattern {
        InnerRelationPattern::Indeterminate { subquery, .. }
        | InnerRelationPattern::UncorrelatedDerivedTable { subquery, .. } => {
            child!(walk_visit_relational(v, subquery));
        }
        InnerRelationPattern::CorrelatedScalarJoin {
            correlation_filters,
            subquery,
            ..
        } => {
            for f in correlation_filters {
                child!(walk_visit_boolean(v, f));
            }
            child!(walk_visit_relational(v, subquery));
        }
        InnerRelationPattern::CorrelatedGroupJoin {
            correlation_filters,
            aggregations,
            subquery,
            ..
        } => {
            for f in correlation_filters {
                child!(walk_visit_boolean(v, f));
            }
            for e in aggregations {
                child!(walk_visit_domain(v, e));
            }
            child!(walk_visit_relational(v, subquery));
        }
    }
    exit!(v.exit_inner_relation(pattern));
}

// =============================================================================
// Walk functions — pipe / operator / sigma
// =============================================================================

pub fn walk_visit_pipe<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    pipe: &PipeExpression<P>,
) -> Result<Descent> {
    enter!(v.enter_pipe(pipe));
    child!(walk_visit_relational(v, &pipe.source));
    child!(walk_visit_operator(v, &pipe.operator));
    exit!(v.exit_pipe(pipe));
}

pub fn walk_visit_operator<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    op: &UnaryRelationalOperator<P>,
) -> Result<Descent> {
    enter!(v.enter_operator(op));
    match op {
        UnaryRelationalOperator::General { expressions, .. }
        | UnaryRelationalOperator::ProjectOut { expressions, .. } => {
            for e in expressions {
                child!(walk_visit_domain(v, e));
            }
        }
        UnaryRelationalOperator::Modulo { spec, .. } => child!(walk_visit_modulo_spec(v, spec)),
        UnaryRelationalOperator::TupleOrdering { specs, .. } => {
            for s in specs {
                child!(walk_visit_ordering_spec(v, s));
            }
        }
        UnaryRelationalOperator::MapCover {
            function,
            columns,
            conditioned_on,
            ..
        } => {
            child!(walk_visit_function(v, function));
            for c in columns {
                child!(walk_visit_domain(v, c));
            }
            if let Some(c) = conditioned_on {
                child!(walk_visit_boolean(v, c));
            }
        }
        UnaryRelationalOperator::RenameCover { specs } => {
            for s in specs {
                child!(walk_visit_rename_spec(v, s));
            }
        }
        UnaryRelationalOperator::Transform {
            transformations,
            conditioned_on,
        } => {
            for (expr, _alias, _qual) in transformations {
                child!(walk_visit_domain(v, expr));
            }
            if let Some(c) = conditioned_on {
                child!(walk_visit_boolean(v, c));
            }
        }
        UnaryRelationalOperator::AggregatePipe { aggregations } => {
            for e in aggregations {
                child!(walk_visit_domain(v, e));
            }
        }
        UnaryRelationalOperator::Reposition { moves } => {
            for m in moves {
                child!(walk_visit_reposition_spec(v, m));
            }
        }
        UnaryRelationalOperator::EmbedMapCover {
            function, selector, ..
        } => {
            child!(walk_visit_function(v, function));
            child!(walk_visit_column_selector(v, selector));
        }
        UnaryRelationalOperator::HoViewApplication {
            first_parens_spec,
            domain_spec,
            ..
        } => {
            if let Some(s) = first_parens_spec {
                child!(walk_visit_domain_spec(v, s));
            }
            child!(walk_visit_domain_spec(v, domain_spec));
        }
        UnaryRelationalOperator::DmlTerminal { domain_spec, .. } => {
            child!(walk_visit_domain_spec(v, domain_spec));
        }
        UnaryRelationalOperator::DirectiveTerminal { arguments, .. } => {
            for e in arguments {
                child!(walk_visit_domain(v, e));
            }
        }
        UnaryRelationalOperator::DirectivePipeInvocation {
            argument,
            domain_spec,
            ..
        } => {
            child!(walk_visit_relational(v, argument));
            child!(walk_visit_domain_spec(v, domain_spec));
        }
        // Leaf operators — no recursive children.
        UnaryRelationalOperator::MetaIze
        | UnaryRelationalOperator::Witness { .. }
        | UnaryRelationalOperator::Qualify
        | UnaryRelationalOperator::Using { .. }
        | UnaryRelationalOperator::UsingAll
        | UnaryRelationalOperator::InteriorDrillDown { .. }
        | UnaryRelationalOperator::NarrowingDestructure { .. }
        | UnaryRelationalOperator::SignedWitness => {}
    }
    exit!(v.exit_operator(op));
}

pub fn walk_visit_sigma<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    cond: &SigmaCondition<P>,
) -> Result<Descent> {
    enter!(v.enter_sigma(cond));
    match cond {
        SigmaCondition::Predicate(pred) => child!(walk_visit_boolean(v, pred)),
        SigmaCondition::TupleOrdinal(_) => {}
        SigmaCondition::Destructure {
            json_column,
            pattern,
            ..
        } => {
            child!(walk_visit_domain(v, json_column));
            child!(walk_visit_function(v, pattern));
        }
        SigmaCondition::SigmaCall { arguments, .. } => {
            for e in arguments {
                child!(walk_visit_domain(v, e));
            }
        }
    }
    exit!(v.exit_sigma(cond));
}

// =============================================================================
// Walk functions — core expressions
// =============================================================================

pub fn walk_visit_boolean<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    expr: &BooleanExpression<P>,
) -> Result<Descent> {
    enter!(v.enter_boolean(expr));
    match expr {
        BooleanExpression::Comparison { left, right, .. } => {
            child!(walk_visit_domain(v, left));
            child!(walk_visit_domain(v, right));
        }
        BooleanExpression::And { left, right } | BooleanExpression::Or { left, right } => {
            child!(walk_visit_boolean(v, left));
            child!(walk_visit_boolean(v, right));
        }
        BooleanExpression::Not { expr } => child!(walk_visit_boolean(v, expr)),
        BooleanExpression::InnerExists { subquery, .. } => {
            child!(walk_visit_relational(v, subquery));
        }
        BooleanExpression::In { value, set, .. } => {
            child!(walk_visit_domain(v, value));
            for e in set {
                child!(walk_visit_domain(v, e));
            }
        }
        BooleanExpression::InRelational {
            value, subquery, ..
        } => {
            child!(walk_visit_domain(v, value));
            child!(walk_visit_relational(v, subquery));
        }
        BooleanExpression::Sigma { condition } => child!(walk_visit_sigma(v, condition)),
        // Leaf variants.
        BooleanExpression::Using { .. }
        | BooleanExpression::BooleanLiteral { .. }
        | BooleanExpression::GlobCorrelation { .. }
        | BooleanExpression::OrdinalGlobCorrelation { .. } => {}
    }
    exit!(v.exit_boolean(expr));
}

pub fn walk_visit_domain<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    expr: &DomainExpression<P>,
) -> Result<Descent> {
    enter!(v.enter_domain(expr));
    match expr {
        DomainExpression::Function(f) => child!(walk_visit_function(v, f)),
        DomainExpression::Predicate { expr, .. } => child!(walk_visit_boolean(v, expr)),
        DomainExpression::PipedExpression {
            value, transforms, ..
        } => {
            child!(walk_visit_domain(v, value));
            for (_dir, f) in transforms {
                child!(walk_visit_function(v, f));
            }
        }
        DomainExpression::Parenthesized { inner, .. } => child!(walk_visit_domain(v, inner)),
        DomainExpression::Tuple { elements, .. } => {
            for e in elements {
                child!(walk_visit_domain(v, e));
            }
        }
        DomainExpression::ScalarSubquery { subquery, .. } => {
            child!(walk_visit_relational(v, subquery));
        }
        DomainExpression::PivotOf {
            value_column,
            pivot_key,
            ..
        } => {
            child!(walk_visit_domain(v, value_column));
            child!(walk_visit_domain(v, pivot_key));
        }
        // Leaf variants — `Projection` has only non-recursive members (mirror
        // walk_transform_domain, which never recurses into it).
        DomainExpression::Lvar { .. }
        | DomainExpression::Literal { .. }
        | DomainExpression::Projection(_)
        | DomainExpression::NonUnifiyingUnderscore
        | DomainExpression::ValuePlaceholder { .. }
        | DomainExpression::Substitution(_)
        | DomainExpression::ColumnOrdinal(_) => {}
    }
    exit!(v.exit_domain(expr));
}

pub fn walk_visit_function<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    func: &FunctionExpression<P>,
) -> Result<Descent> {
    enter!(v.enter_function(func));
    match func {
        FunctionExpression::Regular {
            arguments,
            conditioned_on,
            ..
        }
        | FunctionExpression::Curried {
            arguments,
            conditioned_on,
            ..
        } => {
            for e in arguments {
                child!(walk_visit_domain(v, e));
            }
            if let Some(c) = conditioned_on {
                child!(walk_visit_boolean(v, c));
            }
        }
        FunctionExpression::HigherOrder {
            curried_arguments,
            regular_arguments,
            conditioned_on,
            ..
        } => {
            for e in curried_arguments {
                child!(walk_visit_domain(v, e));
            }
            for e in regular_arguments {
                child!(walk_visit_domain(v, e));
            }
            if let Some(c) = conditioned_on {
                child!(walk_visit_boolean(v, c));
            }
        }
        FunctionExpression::Bracket { arguments, .. } => {
            for e in arguments {
                child!(walk_visit_domain(v, e));
            }
        }
        FunctionExpression::Curly {
            members,
            inner_grouping_keys,
            cte_requirements,
            ..
        } => {
            for m in members {
                child!(walk_visit_curly_member(v, m));
            }
            for e in inner_grouping_keys {
                child!(walk_visit_domain(v, e));
            }
            if let Some(r) = cte_requirements {
                child!(walk_visit_cte_requirements(v, r));
            }
        }
        FunctionExpression::Array { members, .. } => {
            for m in members {
                child!(walk_visit_array_member(v, m));
            }
        }
        FunctionExpression::MetadataTreeGroup {
            constructor,
            cte_requirements,
            ..
        } => {
            child!(walk_visit_function(v, constructor));
            if let Some(r) = cte_requirements {
                child!(walk_visit_cte_requirements(v, r));
            }
        }
        FunctionExpression::Lambda { body, .. } => child!(walk_visit_domain(v, body)),
        FunctionExpression::Infix { left, right, .. } => {
            child!(walk_visit_domain(v, left));
            child!(walk_visit_domain(v, right));
        }
        FunctionExpression::StringTemplate { parts, .. } => {
            for p in parts {
                child!(walk_visit_string_template_part(v, p));
            }
        }
        FunctionExpression::CaseExpression { arms, .. } => {
            for a in arms {
                child!(walk_visit_case_arm(v, a));
            }
        }
        FunctionExpression::Window {
            arguments,
            partition_by,
            order_by,
            frame,
            ..
        } => {
            for e in arguments {
                child!(walk_visit_domain(v, e));
            }
            for e in partition_by {
                child!(walk_visit_domain(v, e));
            }
            for o in order_by {
                child!(walk_visit_ordering_spec(v, o));
            }
            if let Some(f) = frame {
                child!(walk_visit_window_frame(v, f));
            }
        }
        FunctionExpression::JsonPath { source, path, .. } => {
            child!(walk_visit_domain(v, source));
            child!(walk_visit_domain(v, path));
        }
    }
    exit!(v.exit_function(func));
}

// =============================================================================
// Walk functions — supporting containers (no enter/exit hooks; pure descent,
// mirroring the corresponding walk_transform_* helpers edge-for-edge)
// =============================================================================

pub fn walk_visit_domain_spec<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    spec: &DomainSpec<P>,
) -> Result<Descent> {
    match spec {
        DomainSpec::Positional(exprs) => {
            for e in exprs {
                child!(walk_visit_domain(v, e));
            }
        }
        DomainSpec::Glob
        | DomainSpec::GlobWithUsing(_)
        | DomainSpec::GlobWithUsingAll
        | DomainSpec::Bare => {}
    }
    Ok(Descent::Continue)
}

pub fn walk_visit_ordering_spec<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    spec: &OrderingSpec<P>,
) -> Result<Descent> {
    walk_visit_domain(v, &spec.column)
}

pub fn walk_visit_modulo_spec<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    spec: &ModuloSpec<P>,
) -> Result<Descent> {
    match spec {
        ModuloSpec::Columns(columns) => {
            for e in columns {
                child!(walk_visit_domain(v, e));
            }
        }
        ModuloSpec::GroupBy {
            reducing_by,
            reducing_on,
            delegates,
        } => {
            for ode in reducing_by {
                child!(walk_visit_domain(v, &ode.expr));
            }
            for ode in reducing_on {
                child!(walk_visit_domain(v, &ode.expr));
            }
            for w in delegates {
                for ode in &w.payload {
                    child!(walk_visit_domain(v, &ode.expr));
                }
                for o in &w.order {
                    child!(walk_visit_ordering_spec(v, o));
                }
            }
        }
    }
    Ok(Descent::Continue)
}

pub fn walk_visit_rename_spec<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    spec: &RenameSpec<P>,
) -> Result<Descent> {
    walk_visit_domain(v, &spec.from)
}

pub fn walk_visit_reposition_spec<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    spec: &RepositionSpec<P>,
) -> Result<Descent> {
    walk_visit_domain(v, &spec.column)
}

pub fn walk_visit_row<P, F: AstVisit<P> + ?Sized>(v: &mut F, row: &Row<P>) -> Result<Descent> {
    for e in &row.values {
        child!(walk_visit_domain(v, e));
    }
    Ok(Descent::Continue)
}

pub fn walk_visit_column_selector<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    selector: &ColumnSelector<P>,
) -> Result<Descent> {
    match selector {
        ColumnSelector::Explicit(exprs) => {
            for e in exprs {
                child!(walk_visit_domain(v, e));
            }
        }
        ColumnSelector::Regex(_)
        | ColumnSelector::All
        | ColumnSelector::Positional { .. }
        | ColumnSelector::MultipleRegex(_)
        | ColumnSelector::Resolved { .. } => {}
    }
    Ok(Descent::Continue)
}

pub fn walk_visit_window_frame<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    frame: &WindowFrame<P>,
) -> Result<Descent> {
    child!(walk_visit_frame_bound(v, &frame.start));
    child!(walk_visit_frame_bound(v, &frame.end));
    Ok(Descent::Continue)
}

pub fn walk_visit_frame_bound<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    bound: &FrameBound<P>,
) -> Result<Descent> {
    match bound {
        FrameBound::Preceding(expr) | FrameBound::Following(expr) => {
            child!(walk_visit_domain(v, expr));
        }
        FrameBound::Unbounded | FrameBound::CurrentRow => {}
    }
    Ok(Descent::Continue)
}

pub fn walk_visit_string_template_part<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    part: &StringTemplatePart<P>,
) -> Result<Descent> {
    match part {
        StringTemplatePart::Interpolation(expr) => child!(walk_visit_domain(v, expr)),
        StringTemplatePart::Text(_) => {}
    }
    Ok(Descent::Continue)
}

pub fn walk_visit_array_member<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    member: &ArrayMember<P>,
) -> Result<Descent> {
    match member {
        ArrayMember::Index { path, .. } => child!(walk_visit_domain(v, path)),
    }
    Ok(Descent::Continue)
}

pub fn walk_visit_curly_member<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    member: &CurlyMember<P>,
) -> Result<Descent> {
    match member {
        CurlyMember::Comparison { condition } => child!(walk_visit_boolean(v, condition)),
        CurlyMember::KeyValue { value, .. } => child!(walk_visit_domain(v, value)),
        CurlyMember::PathLiteral { path, .. } => child!(walk_visit_domain(v, path)),
        CurlyMember::Shorthand { .. }
        | CurlyMember::Glob
        | CurlyMember::Pattern { .. }
        | CurlyMember::OrdinalRange { .. }
        | CurlyMember::Placeholder => {}
    }
    Ok(Descent::Continue)
}

pub fn walk_visit_case_arm<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    arm: &CaseArm<P>,
) -> Result<Descent> {
    match arm {
        CaseArm::Simple {
            test_expr, result, ..
        } => {
            child!(walk_visit_domain(v, test_expr));
            child!(walk_visit_domain(v, result));
        }
        CaseArm::CurriedSimple { result, .. } => child!(walk_visit_domain(v, result)),
        CaseArm::Searched { condition, result } => {
            child!(walk_visit_boolean(v, condition));
            child!(walk_visit_domain(v, result));
        }
        CaseArm::Default { result } => child!(walk_visit_domain(v, result)),
    }
    Ok(Descent::Continue)
}

fn walk_visit_cte_requirements<P, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    reqs: &CteRequirements<P>,
) -> Result<Descent> {
    for (_name, expr) in &reqs.accumulated_grouping_keys {
        child!(walk_visit_domain(v, expr));
    }
    for e in &reqs.join_keys {
        child!(walk_visit_domain(v, e));
    }
    Ok(Descent::Continue)
}

#[cfg(test)]
mod tests;
