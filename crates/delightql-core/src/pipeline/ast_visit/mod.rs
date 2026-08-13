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
//! a bag operation's correlation predicate. The `WithCfes.cfes`
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
use crate::pipeline::asts::core::expressions::functions::{
    CaseExpression, FunctorCall, ValueTemplatePart,
};
use crate::pipeline::asts::core::expressions::metadata_types::CteRequirements;
use crate::pipeline::asts::core::expressions::relational::InnerRelationPattern;
use crate::pipeline::asts::core::operators::{EmbedMapCover, MapCover};
use crate::pipeline::asts::core::operators::{FrameBound, HoArgument, WindowFrame};
use crate::pipeline::asts::core::{
    Access, AnonTable, Chain, Continuation, CteBinding, Datum, DomainExpression, Enclyph,
    FunctionApplication, Grelex, GroupSpec, MemberCorrelation, MetadataGroup, MetadataTarget,
    OrderingSpec, Phase, PipeOp, Query, RecordMember, ReductionItem, Relation, TabularRow,
    TruthExpression,
};
use crate::pipeline::asts::core::{
    Comparison, Existence, Membership, RelationalMembership, SigmaApplication,
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
pub trait AstVisit<P: Phase> {
    fn enter_query(&mut self, q: &Query<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }
    fn exit_query(&mut self, q: &Query<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }

    fn enter_relational(&mut self, e: &Chain<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }
    fn exit_relational(&mut self, e: &Chain<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }

    fn enter_callable(&mut self, c: &crate::pipeline::asts::core::Callable<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }
    fn exit_callable(&mut self, c: &crate::pipeline::asts::core::Callable<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }

    fn enter_relation(&mut self, r: &Relation<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }
    fn exit_relation(&mut self, r: &Relation<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }

    fn enter_boolean(&mut self, e: &TruthExpression<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }
    fn exit_boolean(&mut self, e: &TruthExpression<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }

    fn enter_domain(&mut self, e: &DomainExpression<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }
    fn exit_domain(&mut self, e: &DomainExpression<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }

    fn enter_function(&mut self, f: &FunctionApplication<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }
    fn exit_function(&mut self, f: &FunctionApplication<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }

    /// A publication item: the value, the name the author gave it, and the
    /// occurrence it publishes, together. A visitor that needs the name a
    /// value publishes under reads it HERE — the value itself does not carry
    /// one.
    fn enter_out_item(&mut self, i: &crate::pipeline::asts::core::OutItem<P>) -> Result<Descent> {
        let _ = i;
        Ok(Descent::Continue)
    }

    fn enter_operator(&mut self, o: &PipeOp<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }
    fn exit_operator(&mut self, o: &PipeOp<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }

    fn enter_continuation(&mut self, c: &Continuation<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }
    fn exit_continuation(&mut self, c: &Continuation<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }

    fn enter_anon_table(&mut self, a: &AnonTable<P>) -> Result<Descent> {
        Ok(Descent::Continue)
    }
    fn exit_anon_table(&mut self, a: &AnonTable<P>) -> Result<Descent> {
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

pub fn walk_visit_query<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    query: &Query<P>,
) -> Result<Descent> {
    enter!(v.enter_query(query));
    // `cfes` bodies live in a non-phase-parameterized, Unresolved-pinned side
    // structure the generic walk cannot type — mirror walk_transform_query
    // (which carries `cfes` through the phase door) and descend the bindings
    // and the body. A caller wanting a CFE body roots a fresh visit at it
    // (the W12 pattern).
    for c in &query.ctes {
        child!(walk_visit_cte_binding(v, c));
    }
    child!(walk_visit_relational(v, &query.body));
    exit!(v.exit_query(query));
}

pub fn walk_visit_cte_binding<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    cte: &CteBinding<P>,
) -> Result<Descent> {
    walk_visit_relational(v, &cte.expression)
}

// =============================================================================
// Walk functions — relational layer
// =============================================================================

#[stacksafe::stacksafe]
pub fn walk_visit_relational<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    expr: &Chain<P>,
) -> Result<Descent> {
    enter!(v.enter_relational(expr));
    match &expr.head {
        Grelex::Reference(rel) => child!(walk_visit_relation(v, rel)),
        Grelex::Literal(anon) => child!(walk_visit_anon_table(v, &anon.table)),
    }
    for continuation in &expr.continuations {
        child!(walk_visit_continuation(v, continuation));
    }
    exit!(v.exit_relational(expr));
}

pub fn walk_visit_anon_table<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    anon: &AnonTable<P>,
) -> Result<Descent> {
    enter!(v.enter_anon_table(anon));
    if let Some(headers) = &anon.body.header {
        for header in headers.iter() {
            child!(walk_visit_slot(v, &header.slot));
        }
    }
    for row in &anon.body.rows {
        child!(walk_visit_tabular_row(v, row));
    }
    exit!(v.exit_anon_table(anon));
}

#[stacksafe::stacksafe]
pub fn walk_visit_continuation<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    continuation: &Continuation<P>,
) -> Result<Descent> {
    enter!(v.enter_continuation(continuation));
    match continuation {
        Continuation::Access { access, .. } => {
            child!(walk_visit_access(v, access));
        }
        Continuation::Restrict { condition, .. } => {
            child!(walk_visit_boolean(v, condition));
        }
        // A correlation names two arms by scope; there is no expression
        // beneath it to visit.
        Continuation::Bound { .. } | Continuation::Correlate { .. } => {}
        // A PATTERN HOLDS NO EXPRESSION. Its members bind names and reach
        // with paths, so a walk over the destructure's children reaches the
        // source and stops.
        Continuation::Destructure { source, .. } => {
            child!(walk_visit_domain(v, source));
        }
        Continuation::Member {
            rhs, correlation, ..
        } => {
            child!(walk_visit_relational(v, rhs));
            // A correspondence holds names, not expressions: there is no
            // child to descend into.
            if let Some(c) = correlation.as_ref().and_then(MemberCorrelation::condition) {
                child!(walk_visit_boolean(v, c));
            }
        }
        Continuation::BagOp {
            arm, correlation, ..
        } => {
            child!(walk_visit_relational(v, arm));
            if let Some(correlation) = P::correlation(correlation) {
                // A whole-heading correlation names arms, not expressions:
                // there is no truth beneath it to visit.
                if let Some(predicate) = correlation.predicate.expression() {
                    child!(walk_visit_boolean(v, predicate));
                }
            }
        }
        Continuation::Pipe { operator, .. } => {
            child!(walk_visit_operator(v, operator));
        }
        Continuation::ErJoin(carried) => {
            child!(walk_visit_relational(v, &P::er_join(carried).rhs));
        }
        Continuation::Structural(step) => match &step.form {
            crate::pipeline::asts::core::StructuralForm::Ordering { specs } => {
                for s in specs {
                    child!(walk_visit_ordering_spec(v, s));
                }
            }
            // A reposition addresses columns; the fixed-heading forms, the
            // drill's occurrences and the narrowing's pattern hold no
            // expression child to reach.
            crate::pipeline::asts::core::StructuralForm::Reposition { .. }
            | crate::pipeline::asts::core::StructuralForm::Meta
            | crate::pipeline::asts::core::StructuralForm::Witness { .. }
            | crate::pipeline::asts::core::StructuralForm::SignedWitness
            | crate::pipeline::asts::core::StructuralForm::Drill { .. }
            | crate::pipeline::asts::core::StructuralForm::Narrow { .. } => {}
        },
    }
    exit!(v.exit_continuation(continuation));
}

pub fn walk_visit_relation<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    rel: &Relation<P>,
) -> Result<Descent> {
    enter!(v.enter_relation(rel));
    match rel {
        Relation::FunctorCall { call, .. } => child!(walk_visit_functor_call(v, call.call())),
        // A ground read names a relation and nothing else: what was asked of
        // it is the access continuation beside it, walked in its own right.
        Relation::Ground { .. } => {}
        Relation::InnerRelation { pattern, .. } => child!(walk_visit_inner_relation(v, pattern)),
        Relation::ConsultedView { body, .. } => {
            child!(walk_visit_query(v, body))
        }
    }
    exit!(v.exit_relation(rel));
}

pub fn walk_visit_inner_relation<P: Phase, F: AstVisit<P> + ?Sized>(
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

pub fn walk_visit_operator<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    op: &PipeOp<P>,
) -> Result<Descent> {
    enter!(v.enter_operator(op));
    match op {
        PipeOp::Project(items) | PipeOp::Embed(items) => {
            for item in items {
                child!(walk_visit_out_item(v, item));
            }
        }
        // A selector ADDRESSES columns: references and spreads, with no
        // expression child to reach.
        PipeOp::ProjectOut(_) => {}
        PipeOp::Group(spec) => child!(walk_visit_group_spec(v, spec)),
        PipeOp::MapCover(MapCover {
            callable,
            guard,
            cells,
            ..
        }) => {
            if let Some(function) = P::cover_callable(callable) {
                child!(walk_visit_callable(v, function));
            }
            for cell in cells {
                child!(walk_visit_domain(v, &cell.expr));
            }
            if let Some(c) = guard {
                child!(walk_visit_boolean(v, c));
            }
        }
        // A rename ADDRESSES columns and names them; neither side is an
        // expression.
        PipeOp::Rename(_) => {}
        PipeOp::Transform {
            items: transformations,
            guard: conditioned_on,
        } => {
            for item in transformations {
                child!(walk_visit_out_value(v, &item.expr));
            }
            if let Some(c) = conditioned_on {
                child!(walk_visit_boolean(v, c));
            }
        }
        // A reposition's spec is a reference and a position: an address,
        // with no expression child to reach.
        PipeOp::EmbedMapCover(EmbedMapCover {
            callable, cells, ..
        }) => {
            if let Some(function) = P::cover_callable(callable) {
                child!(walk_visit_callable(v, function));
            }
            for cell in cells {
                child!(walk_visit_domain(v, &cell.expr));
            }
        } // Leaf operators — no recursive children.
    }
    exit!(v.exit_operator(op));
}

// =============================================================================
// Walk functions — core expressions
// =============================================================================

pub fn walk_visit_boolean<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    expr: &TruthExpression<P>,
) -> Result<Descent> {
    enter!(v.enter_boolean(expr));
    match expr {
        TruthExpression::Comparison(Comparison { left, right, .. }) => {
            child!(walk_visit_domain(v, left));
            child!(walk_visit_domain(v, right));
        }
        TruthExpression::Not { expr } => child!(walk_visit_boolean(v, expr)),
        TruthExpression::Existence(Existence {
            relation: subquery, ..
        }) => {
            child!(walk_visit_relational(v, subquery));
        }
        TruthExpression::Membership(Membership { probe, rows, .. }) => {
            for value in probe.values() {
                child!(walk_visit_domain(v, value));
            }
            for row in rows {
                for value in &row.0 {
                    child!(walk_visit_domain(v, value));
                }
            }
        }
        TruthExpression::RelationalMembership(RelationalMembership {
            probe,
            relation: subquery,
            ..
        }) => {
            for value in probe.values() {
                child!(walk_visit_domain(v, value));
            }
            child!(walk_visit_relational(v, subquery));
        }
        TruthExpression::Sigma(SigmaApplication {
            proof: crate::pipeline::asts::core::NamedProof::Body(body),
            ..
        }) => child!(walk_visit_boolean(v, P::sigma_body(body))),
        TruthExpression::Sigma(SigmaApplication {
            proof: crate::pipeline::asts::core::NamedProof::Call(call),
            ..
        }) => {
            child!(walk_visit_functor_call(v, call.call()))
        }
        TruthExpression::Conjunction(parts) | TruthExpression::Disjunction(parts) => {
            for part in parts.iter() {
                child!(walk_visit_boolean(v, part));
            }
        }
    }
    exit!(v.exit_boolean(expr));
}

/// An argument's value: the domain road, or the crossing's truth. Both are
/// reached, so a subquery beneath a crossed argument is not lost.
pub fn walk_visit_argument_value<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    value: &crate::pipeline::asts::core::ArgumentValue<P>,
) -> Result<Descent> {
    match value {
        crate::pipeline::asts::core::ArgumentValue::Domain { value, .. } => {
            walk_visit_domain(v, value)
        }
        crate::pipeline::asts::core::ArgumentValue::Truth(crossing) => {
            walk_visit_boolean(v, crossing.truth())
        }
    }
}

/// A published value: the domain road, or the crossing's truth. The walk
/// reaches BOTH, so a subquery beneath a published existence is not lost to
/// the recursion-closure matrix.
pub fn walk_visit_out_value<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    value: &crate::pipeline::asts::core::OutValue<P>,
) -> Result<Descent> {
    match value {
        crate::pipeline::asts::core::OutValue::Domain(domain) => walk_visit_domain(v, domain),
        crate::pipeline::asts::core::OutValue::Truth(crossing) => {
            walk_visit_boolean(v, crossing.truth())
        }
    }
}

pub fn walk_visit_domain<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    expr: &DomainExpression<P>,
) -> Result<Descent> {
    enter!(v.enter_domain(expr));
    match expr {
        DomainExpression::Application(f) => child!(walk_visit_function(v, f)),
        // Leaf variants — `Projection` has only non-recursive members (mirror
        // walk_transform_domain, which never recurses into it).
        DomainExpression::Reference(_) => {}
    }
    exit!(v.exit_domain(expr));
}

pub fn walk_visit_function<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    func: &FunctionApplication<P>,
) -> Result<Descent> {
    enter!(v.enter_function(func));
    match func {
        crate::pipeline::asts::core::FunctionApplication::Ground(_)
        | crate::pipeline::asts::core::FunctionApplication::Open(_) => {}
        crate::pipeline::asts::core::FunctionApplication::Standard(application) => {
            child!(walk_visit_standard_application(v, application))
        }
        crate::pipeline::asts::core::FunctionApplication::Enclyph(enclyph) => {
            child!(walk_visit_enclyph(v, enclyph))
        }
        crate::pipeline::asts::core::FunctionApplication::ClauseSelection(selection) => {
            for arm in &selection.arms {
                if let Some(guard) = &arm.guard {
                    child!(walk_visit_boolean(v, guard));
                }
                child!(walk_visit_out_value(v, &arm.result));
            }
        }
        crate::pipeline::asts::core::FunctionApplication::Infix(infix) => {
            child!(walk_visit_domain(v, &infix.left));
            child!(walk_visit_domain(v, &infix.right));
        }
        crate::pipeline::asts::core::FunctionApplication::Template(template) => {
            for p in template.parts() {
                child!(walk_visit_value_template_part(v, p));
            }
        }
        crate::pipeline::asts::core::FunctionApplication::Case(case) => {
            child!(walk_visit_case(v, case));
        }
        crate::pipeline::asts::core::FunctionApplication::Scalarized(relation) => {
            child!(walk_visit_scalarized(v, relation.body()));
        }
        // The call is the pick's one child value. The declaration beneath it
        // is the CALLEE's, reached through the phase's witness so a walk
        // written for one phase cannot descend into a payload another has.
        crate::pipeline::asts::core::FunctionApplication::FieldSelect(select) => {
            child!(walk_visit_standard_application(v, &select.application));
            if let Some(witness) = P::mode_witness(&select.dependency) {
                child!(walk_visit_mode(v, &witness.mode));
            }
        }
        // The path is a spec, not a child value: there is nothing under
        // it for a walk to reach.
        crate::pipeline::asts::core::FunctionApplication::JsonAccess(access) => {
            child!(walk_visit_domain(v, &access.source));
        }
    }
    exit!(v.exit_function(func));
}

/// AN APPLICATION'S WHOLE PAYLOAD: the call's argument row, the window it is
/// modified by, and the guard it is filtered by. A reader asking what values
/// an application contains asks HERE, so no reader hand-picks a subset of
/// them.
/// A CALLABLE OWNS ITS OWN SLOT.
///
/// A visitor reaches a callable through its own hook, so a walk that is
/// counting or spending an OUTER landing draws its boundary here by
/// returning `SkipSubtree` — the slot beneath belongs to this callable, and
/// nothing above it may consume one.
/// A DECLARED MODE: every arm's output row, and the default's.
pub fn walk_visit_mode<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    mode: &crate::pipeline::asts::core::FactFunctionMode<P>,
) -> Result<Descent> {
    for arm in &mode.arms {
        for output in &arm.outputs {
            child!(walk_visit_domain(v, output));
        }
    }
    if let Some(default) = &mode.default {
        for output in default {
            child!(walk_visit_domain(v, output));
        }
    }
    Ok(Descent::Continue)
}

/// A RELATION MADE ONE VALUE: the body it compresses, and the compression.
pub fn walk_visit_scalarized<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    body: &crate::pipeline::asts::core::ScalarizedRelation<P>,
) -> Result<Descent> {
    use crate::pipeline::asts::core::Scalarization;
    child!(walk_visit_relational(v, &body.body));
    match &body.scalarization {
        Scalarization::ZeroKeyReduction(items) => {
            for item in items.iter() {
                child!(walk_visit_reduction_item(v, item));
            }
        }
        Scalarization::BoundToOne { ordering } => {
            for spec in ordering {
                child!(walk_visit_ordering_spec(v, spec));
            }
        }
    }
    Ok(Descent::Continue)
}

pub fn walk_visit_callable<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    callable: &crate::pipeline::asts::core::Callable<P>,
) -> Result<Descent> {
    enter!(v.enter_callable(callable));
    match callable {
        crate::pipeline::asts::core::Callable::Functor(application) => {
            child!(walk_visit_standard_application(v, application))
        }
        crate::pipeline::asts::core::Callable::String(template) => {
            for part in template.parts() {
                child!(walk_visit_value_template_part(v, part));
            }
        }
        crate::pipeline::asts::core::Callable::Lambda(lambda) => {
            child!(walk_visit_domain(v, &lambda.body))
        }
    }
    exit!(v.exit_callable(callable));
}

pub fn walk_visit_standard_application<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    application: &crate::pipeline::asts::core::StandardApplication<P>,
) -> Result<Descent> {
    child!(walk_visit_functor_call(v, application.call()));
    if let Some(window) = &application.window {
        for expr in &window.partition {
            child!(walk_visit_domain(v, expr));
        }
        for ordering in &window.ordering {
            child!(walk_visit_ordering_spec(v, ordering));
        }
        if let Some(frame) = &window.frame {
            child!(walk_visit_window_frame(v, frame));
        }
    }
    if let Some(guard) = &application.guard {
        child!(walk_visit_boolean(v, guard));
    }
    Ok(Descent::Continue)
}

/// A CALL'S OWN PAYLOAD: the argument row. A guard and a window are the
/// scalar POSITION's context and are reached through the application that
/// owns them.
pub fn walk_visit_functor_call<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    call: &FunctorCall<P>,
) -> Result<Descent> {
    use crate::pipeline::asts::core::operators::{CallArguments, ScalarArgument};
    match &call.call().arguments {
        CallArguments::None => {}
        CallArguments::HigherOrder(part) => {
            for argument in part.members.iter() {
                match argument {
                    HoArgument::Relation(relation) => child!(walk_visit_relational(v, relation)),
                    HoArgument::Value(value) => child!(walk_visit_argument_value(v, value)),
                    // Structural row marks: nothing beneath to reach.
                    HoArgument::Landing(_) | HoArgument::Skip => {}
                }
            }
        }
        CallArguments::Scalar(members) => {
            for member in members {
                match member {
                    ScalarArgument::Value(value) => child!(walk_visit_argument_value(v, value)),
                    // A spread enumerates, a star names the whole, and a
                    // context marker selects a calling mode: none evaluates,
                    // so none has a child to reach.
                    ScalarArgument::Callable(_)
                    | ScalarArgument::Spread(_)
                    | ScalarArgument::Star
                    | ScalarArgument::Context(_) => {}
                }
            }
        }
    }
    Ok(Descent::Continue)
}

// =============================================================================
// Walk functions — supporting containers (no enter/exit hooks; pure descent,
// mirroring the corresponding walk_transform_* helpers edge-for-edge)
// =============================================================================

pub fn walk_visit_access<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    spec: &Access<P>,
) -> Result<Descent> {
    match spec {
        Access::Slots(slots) => {
            for slot in slots {
                child!(walk_visit_slot(v, slot));
            }
        }
        Access::All | Access::Dequalify(_) | Access::DequalifyAll | Access::Unasked => {}
    }
    Ok(Descent::Continue)
}

pub fn walk_visit_slot<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    slot: &crate::pipeline::asts::core::Slot<P>,
) -> Result<Descent> {
    match slot {
        crate::pipeline::asts::core::Slot::Constraint(
            crate::pipeline::asts::core::SlotConstraint::Truth { value, .. },
        ) => child!(walk_visit_boolean(v, value.truth())),
        other => {
            if let Some(term) = other.term() {
                child!(walk_visit_domain(v, &term));
            }
        }
    }
    Ok(Descent::Continue)
}

pub fn walk_visit_ordering_spec<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    spec: &OrderingSpec<P>,
) -> Result<Descent> {
    walk_visit_domain(v, &spec.column)
}

/// A publication item's children are its value's. A spread enumerates and
/// evaluates nothing, so there is no scalar child under it to visit.
pub fn walk_visit_out_item<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    item: &crate::pipeline::asts::core::OutItem<P>,
) -> Result<Descent> {
    enter!(v.enter_out_item(item));
    match item {
        crate::pipeline::asts::core::OutItem::One(one) => walk_visit_out_value(v, &one.expr),
        // Neither of the other two computes a value, so neither has a
        // child to reach.
        crate::pipeline::asts::core::OutItem::Many(_)
        | crate::pipeline::asts::core::OutItem::Whole => Ok(Descent::Continue),
    }
}

pub fn walk_visit_group_spec<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    spec: &GroupSpec<P>,
) -> Result<Descent> {
    match spec {
        GroupSpec::Distinct { keys } => {
            for item in keys.iter() {
                child!(walk_visit_out_item(v, item));
            }
        }
        GroupSpec::Reduce {
            keys,
            reductions,
            plan,
        } => {
            for item in keys {
                child!(walk_visit_out_item(v, item));
            }
            for item in reductions.iter() {
                child!(walk_visit_reduction_item(v, item));
            }
            for group in &plan.tree_groups {
                child!(walk_visit_cte_requirements(v, &group.requirements));
            }
        }
    }
    Ok(Descent::Continue)
}

pub fn walk_visit_tabular_row<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    row: &TabularRow<Datum<P>>,
) -> Result<Descent> {
    for datum in row.iter() {
        if let Datum::Value(value) = datum {
            child!(walk_visit_domain(v, value));
        }
    }
    Ok(Descent::Continue)
}

pub fn walk_visit_window_frame<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    frame: &WindowFrame<P>,
) -> Result<Descent> {
    child!(walk_visit_frame_bound(v, &frame.start));
    child!(walk_visit_frame_bound(v, &frame.end));
    Ok(Descent::Continue)
}

pub fn walk_visit_frame_bound<P: Phase, F: AstVisit<P> + ?Sized>(
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

pub fn walk_visit_value_template_part<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    part: &ValueTemplatePart<P>,
) -> Result<Descent> {
    match part {
        ValueTemplatePart::Interpolation(expr) => child!(walk_visit_domain(v, expr)),
        ValueTemplatePart::Text(_) => {}
    }
    Ok(Descent::Continue)
}

pub fn walk_visit_enclyph<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    enclyph: &Enclyph<P>,
) -> Result<Descent> {
    match enclyph {
        Enclyph::Record(record) => {
            for m in record.members.iter() {
                child!(walk_visit_record_member(v, m));
            }
        }
        Enclyph::EmptyRecord(_) => {}
        Enclyph::Tuple(tuple) => {
            for e in tuple.elements.iter() {
                child!(walk_visit_domain(v, e));
            }
        }
    }
    Ok(Descent::Continue)
}

pub fn walk_visit_record_member<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    member: &RecordMember<P>,
) -> Result<Descent> {
    match member {
        RecordMember::Keyed { value, .. } => child!(walk_visit_domain(v, value)),
        RecordMember::Induced { value, .. } => child!(walk_visit_enclyph(v, value)),
        RecordMember::Spread(_) | RecordMember::SelfKeyed(_) => {}
    }
    Ok(Descent::Continue)
}

/// A REDUCTION PUBLISHES ONE COLUMN, and the two things that publish one
/// have different children to reach.
pub fn walk_visit_reduction_item<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    item: &ReductionItem<P>,
) -> Result<Descent> {
    match item {
        ReductionItem::Out(item) => child!(walk_visit_out_item(v, item)),
        ReductionItem::Metadata(metadata) => child!(walk_visit_metadata_group(v, &metadata.group)),
        ReductionItem::Pivot(pivot) => {
            child!(walk_visit_domain(v, &pivot.value_column));
            child!(walk_visit_domain(v, &pivot.pivot_key));
        }
        ReductionItem::Delegate(delegate) => {
            for item in &delegate.payload {
                child!(walk_visit_out_item(v, item));
            }
            for o in &delegate.order {
                child!(walk_visit_ordering_spec(v, o));
            }
        }
    }
    Ok(Descent::Continue)
}

pub fn walk_visit_metadata_group<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    group: &MetadataGroup<P>,
) -> Result<Descent> {
    match &group.target {
        MetadataTarget::Enclyph(enclyph) => child!(walk_visit_enclyph(v, enclyph)),
        MetadataTarget::Group(nested) => child!(walk_visit_metadata_group(v, nested)),
    }
    if let Some(r) = &group.cte_requirements {
        child!(walk_visit_cte_requirements(v, r));
    }
    Ok(Descent::Continue)
}

/// The anchor is the CASE's, reached once; the arms are its own.
pub fn walk_visit_case<P: Phase, F: AstVisit<P> + ?Sized>(
    v: &mut F,
    case: &CaseExpression<P>,
) -> Result<Descent> {
    let default = match case {
        CaseExpression::Anchored {
            anchor,
            arms,
            default,
        } => {
            child!(walk_visit_domain(v, anchor));
            for arm in arms.iter() {
                child!(walk_visit_domain(v, &arm.result));
            }
            default
        }
        CaseExpression::Searched { arms, default } => {
            for arm in arms.iter() {
                child!(walk_visit_boolean(v, &arm.condition));
                child!(walk_visit_domain(v, &arm.result));
            }
            default
        }
    };
    if let Some(result) = default {
        child!(walk_visit_domain(v, result));
    }
    Ok(Descent::Continue)
}

fn walk_visit_cte_requirements<P: Phase, F: AstVisit<P> + ?Sized>(
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
