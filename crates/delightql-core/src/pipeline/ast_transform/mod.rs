// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Cross-phase AST transformation infrastructure.
//!
//! `AstTransform<P, Q>` transforms AST nodes from phase `P` to phase `Q`.
//! Walk functions handle structural descent; implementors override methods
//! to hook specific node types. Default walk uses the crate-private checked
//! conversion primitive for Q-phase metadata — preserving data while
//! retagging the phase. Hooks
//! override to populate real data when needed.
//!
//! This is the single walk infrastructure for the entire pipeline.

use crate::error::Result;
use crate::pipeline::asts::core::expressions::functions::{
    CaseExpression, FunctorCall, PureCall, SealedCall, ValueTemplate, ValueTemplatePart,
};
use crate::pipeline::asts::core::expressions::metadata_types::CteRequirements;
use crate::pipeline::asts::core::expressions::relational::{InnerRelationPattern, Realized};
use crate::pipeline::asts::core::operators::{EmbedMapCover, MapCover};
use crate::pipeline::asts::core::operators::{FrameBound, WindowFrame};
use crate::pipeline::asts::core::ArgumentValue;
use crate::pipeline::asts::core::{
    Access, AnonTable, BagCorrelation, Chain, Continuation, CorrPred, Datum, DelegateSpec,
    DomainExpression, Enclyph, ErJoinStep, FunctionApplication, Glob, GroupSpec, HeaderItem,
    MemberCorrelation, MetadataGroup, MetadataTarget, NamedOutItem, NamedReference, OrderingSpec,
    OutItem, PatternTarget, Phase, PipeOp, Query, Record, RecordMember, RecordPattern,
    RecordPatternMember, ReductionItem, ReductionPlan, Reference, RegexSelector, Relation,
    RenameSource, RenameSpec, RepositionSpec, SelectorItem, Slot, Spread, TabularBody, TabularRow,
    TreeGroupPlan, TreePattern, TruthExpression, Tuple, TupleElement, WholeHeading,
};
use crate::pipeline::asts::core::{
    Comparison, Existence, Membership, Probe, RelationalMembership, SigmaApplication, ValueRow,
};

// =============================================================================
// FoldAction
// =============================================================================

/// Controls whether the walk recurses into children after a hook returns.
pub enum FoldAction<T> {
    /// The hook pre-processed the node; the walk should recurse into its
    /// children as usual.
    Continue(T),
    /// The hook fully handled this subtree (e.g., ran FAR on it). The walk
    /// should use this node as-is, skipping child recursion.
    Replaced(T),
}

impl<T> FoldAction<T> {
    pub fn into_inner(self) -> T {
        match self {
            FoldAction::Continue(t) | FoldAction::Replaced(t) => t,
        }
    }
}

// =============================================================================
// Payload folds for a same-phase rewrite
// =============================================================================

/// The payload folds for a rewrite that stays in one phase.
///
/// Each `P::…` and `Q::…` is the SAME type here, so carrying a payload
/// across is not a decision anyone could get wrong and not a retag of
/// anything: the value is already in the phase it is going to. A rewrite
/// that crosses phases cannot use this — the types differ, and the compiler
/// says so. What a node PUBLISHES is not among these: a relation identity
/// crosses by the phases' own door (`phases::carry_scope`), and no walk is
/// asked for one.
macro_rules! same_phase_payload_folds {
    ($phase:ty) => {
        fn fold_output(
            &mut self,
            output: <$phase as crate::pipeline::asts::core::Phase>::Output,
        ) -> crate::error::Result<<$phase as crate::pipeline::asts::core::Phase>::Output> {
            Ok(output)
        }
        fn fold_scalar_output(
            &mut self,
            output: <$phase as crate::pipeline::asts::core::Phase>::ScalarOutput,
        ) -> crate::error::Result<<$phase as crate::pipeline::asts::core::Phase>::ScalarOutput> {
            Ok(output)
        }
        fn fold_destructure(
            &mut self,
            destructure: <$phase as crate::pipeline::asts::core::Phase>::Destructure,
        ) -> crate::error::Result<<$phase as crate::pipeline::asts::core::Phase>::Destructure> {
            Ok(destructure)
        }
        fn fold_drill(
            &mut self,
            drill: <$phase as crate::pipeline::asts::core::Phase>::Drill,
        ) -> crate::error::Result<<$phase as crate::pipeline::asts::core::Phase>::Drill> {
            Ok(drill)
        }
        fn fold_column_ordinal(
            &mut self,
            ordinal: <$phase as crate::pipeline::asts::core::Phase>::ColumnOrdinal,
        ) -> crate::error::Result<<$phase as crate::pipeline::asts::core::Phase>::ColumnOrdinal> {
            Ok(ordinal)
        }
        fn fold_column_range(
            &mut self,
            range: <$phase as crate::pipeline::asts::core::Phase>::ColumnRange,
        ) -> crate::error::Result<<$phase as crate::pipeline::asts::core::Phase>::ColumnRange> {
            Ok(range)
        }
        fn fold_entity(
            &mut self,
            entity: <$phase as crate::pipeline::asts::core::Phase>::Entity,
        ) -> crate::error::Result<<$phase as crate::pipeline::asts::core::Phase>::Entity> {
            Ok(entity)
        }
        fn fold_col(
            &mut self,
            column: <$phase as crate::pipeline::asts::core::Phase>::Col,
        ) -> crate::error::Result<<$phase as crate::pipeline::asts::core::Phase>::Col> {
            Ok(column)
        }
        fn fold_binder(
            &mut self,
            binder: <$phase as crate::pipeline::asts::core::Phase>::Binder,
        ) -> crate::error::Result<<$phase as crate::pipeline::asts::core::Phase>::Binder> {
            Ok(binder)
        }
        fn fold_placeholder(
            &mut self,
            landing: <$phase as crate::pipeline::asts::core::Phase>::Placeholder,
        ) -> crate::error::Result<<$phase as crate::pipeline::asts::core::Phase>::Placeholder> {
            Ok(landing)
        }
        fn fold_rename_target(
            &mut self,
            target: <$phase as crate::pipeline::asts::core::Phase>::RenameTarget,
        ) -> crate::error::Result<<$phase as crate::pipeline::asts::core::Phase>::RenameTarget> {
            Ok(target)
        }
        fn fold_open_leaf(
            &mut self,
            leaf: <$phase as crate::pipeline::asts::core::Phase>::OpenLeaf,
        ) -> crate::error::Result<<$phase as crate::pipeline::asts::core::Phase>::OpenLeaf> {
            Ok(leaf)
        }
        fn fold_cover_callable(
            &mut self,
            callable: <$phase as crate::pipeline::asts::core::Phase>::CoverCallable,
        ) -> crate::error::Result<<$phase as crate::pipeline::asts::core::Phase>::CoverCallable> {
            Ok(callable)
        }
        fn fold_context_marker(
            &mut self,
            marker: <$phase as crate::pipeline::asts::core::Phase>::ContextMarker,
        ) -> crate::error::Result<<$phase as crate::pipeline::asts::core::Phase>::ContextMarker> {
            Ok(marker)
        }
    };
}

pub(crate) use same_phase_payload_folds;

/// The answer for a payload that does not exist yet when this fold runs.
///
/// A slot the authored phase holds nothing for is minted by the pass that
/// decides it — the resolver, where a self-reference binds or a pattern's
/// columns come into being. A fold walking past cannot supply the value,
/// and there is no default to hand back either: a default IS an answer, and
/// inventing one is how several passes came to disagree about the same
/// fact.
macro_rules! minted_where_it_is_decided {
    ($($method:ident -> $target:ty : $what:literal),+ $(,)?) => {
        $(
            fn $method(&mut self, _: ()) -> crate::error::Result<$target> {
                Err(Internal::invariant("phase_payload", concat!(
                        $what,
                        " is minted where it is decided, and this fold is not that place",
                    )))
            }
        )+
    };
}

/// The answer for a column reference at the edge out of the authored phase.
///
/// A name is bound where it is resolved: against a heading, in a scope. A
/// fold walking past one holds neither, so it refuses instead of carrying
/// characters into a phase whose column IS an identity — the panic that used
/// to catch this stood at the far end of the pipeline, in the lowering.
/// A column an earlier pass already bound travels forward as itself: whoever
/// bound it had the heading this fold does not.
macro_rules! column_is_bound_where_it_is_resolved {
    () => {
        fn fold_col(
            &mut self,
            column: crate::pipeline::asts::core::AuthoredColumn,
        ) -> crate::error::Result<crate::pipeline::asts::core::ColumnOccurrence> {
            let crate::pipeline::asts::core::AuthoredColumn { name, .. } = column;
            Err(Internal::invariant(
                "lvar",
                format!(
                    "the column reference '{name}' is bound where it is resolved, \
                     and this fold walked past a name nobody looked up"
                ),
            ))
        }
    };
}

pub(crate) use column_is_bound_where_it_is_resolved;

/// The answer for a caller-pattern BINDER at the edge out of the authored
/// phase.
///
/// A slot binds a name to a dimension of the relation it stands in. A fold
/// walking past one has no relation and no dimension, so it refuses. The
/// pattern resolver, which has both, builds its own slots.
macro_rules! binder_is_bound_where_the_pattern_is_resolved {
    () => {
        fn fold_binder(
            &mut self,
            binder: crate::pipeline::asts::core::WrittenBinder,
        ) -> crate::error::Result<crate::relation::PortId> {
            Err(Internal::invariant(
                "slot_bind",
                format!(
                    "the slot binding '{}' is bound where the caller pattern is \
                     resolved, and this fold walked past a pattern nobody bound",
                    binder.name
                ),
            ))
        }
    };
}

pub(crate) use binder_is_bound_where_the_pattern_is_resolved;

/// The answer for a pipe LANDING at the edge out of the authored phase.
///
/// `@` names which formal receives the piped relation, and the invocation
/// that reads it records that as an argument role. A landing reaching a fold
/// is one no invocation read: there is no pipe for it to name, so it is
/// refused rather than carried into a tree where it would stand for a value
/// nobody supplied.
macro_rules! a_landing_is_consumed_where_the_pipe_is_applied {
    () => {
        fn fold_placeholder(
            &mut self,
            _: crate::pipeline::asts::core::AtSign,
        ) -> crate::error::Result<crate::pipeline::asts::vocabulary::Never> {
            Err($crate::diagnostic::DelightQLError::from(
                $crate::diagnostic::Ho::PipeLanding {
                    message: "`@` names the parameter a piped relation lands at, and this one \
                 stands in no invocation under a pipe"
                        .to_string(),
                },
            ))
        }
    };
}

pub(crate) use a_landing_is_consumed_where_the_pipe_is_applied;

/// The answer for a CONTEXT MARKER at the edge out of the authored phase.
///
/// `..` selects a context-aware definition's calling mode, and the call's
/// instantiation consumes it. A marker reaching a fold is one no
/// instantiation read: the callee declares no context, or the marker stands
/// where no call is being instantiated at all.
macro_rules! a_context_marker_is_consumed_where_the_call_instantiates {
    () => {
        fn fold_context_marker(
            &mut self,
            _: crate::pipeline::asts::core::ContextMarker,
        ) -> crate::error::Result<crate::pipeline::asts::vocabulary::Never> {
            Err($crate::diagnostic::DelightQLError::from(
                $crate::diagnostic::Resolution::ContextMarkerPosition {
                    message:
                        "`..` selects the context calling mode of a context-aware definition, \
                 and this call instantiates none"
                            .to_string(),
                },
            ))
        }
    };
}

pub(crate) use a_context_marker_is_consumed_where_the_call_instantiates;

/// The answer for a POSITION at the edge out of the authored phase.
///
/// `|2|` and `|1:3|` are spellings of a column reference, answered against a
/// heading exactly as a name is. A fold with no heading cannot answer one, so
/// it refuses. It does not hand back an underscore: a position that unifies
/// with nothing is a different query from the one that was written.
macro_rules! position_is_resolved_against_a_heading {
    () => {
        fn fold_column_ordinal(
            &mut self,
            ordinal: crate::pipeline::asts::core::ColumnOrdinal,
        ) -> crate::error::Result<crate::pipeline::asts::vocabulary::Never> {
            Err(Internal::invariant(
                "column_ordinal",
                format!(
                    "the column position '{}' is resolved against a heading, and \
                     this fold walked past one with no heading to answer it",
                    crate::lispy::ToLispy::to_lispy(&ordinal)
                ),
            ))
        }

        fn fold_column_range(
            &mut self,
            range: crate::pipeline::asts::core::ColumnRange,
        ) -> crate::error::Result<crate::pipeline::asts::vocabulary::Never> {
            Err(Internal::invariant(
                "column_range",
                format!(
                    "the column range '{}' is resolved against a heading, and this \
                     fold walked past one with no heading to answer it",
                    crate::lispy::ToLispy::to_lispy(&range)
                ),
            ))
        }
    };
}

pub(crate) use position_is_resolved_against_a_heading;

/// The answer for a payload that CANNOT exist on either side of this edge.
///
/// There is no value to receive and none to return, so the body is a match
/// with no arms — the compiler proves it cannot run.
macro_rules! uninhabited_payload_folds {
    ($($method:ident),+ $(,)?) => {
        $(
            fn $method(
                &mut self,
                payload: crate::pipeline::asts::vocabulary::Never,
            ) -> crate::error::Result<crate::pipeline::asts::vocabulary::Never> {
                match payload {}
            }
        )+
    };
}

pub(crate) use uninhabited_payload_folds;

/// The answer for a payload already decided when this fold runs: it travels
/// forward unchanged, and there is no door here that re-decides it.
macro_rules! decided_payload_travels_forward {
    ($($method:ident($payload:ty)),+ $(,)?) => {
        $(
            fn $method(&mut self, payload: $payload) -> crate::error::Result<$payload> {
                Ok(payload)
            }
        )+
    };
}

pub(crate) use decided_payload_travels_forward;
pub(crate) use minted_where_it_is_decided;

// =============================================================================
// Trait
// =============================================================================

/// A consuming cross-phase transformation over AST nodes.
///
/// Transforms nodes from phase `P` to phase `Q`. Every method takes ownership
/// of a P-phase node and returns `Result<Q-phase node>`. The structural
/// methods default to the matching `walk_transform_*` function, which owns
/// the tree's recursion once.
///
/// The PAYLOAD methods below have no defaults, and that is the point. A
/// phase selects what a field holds, so there is no mechanical way to carry
/// one across an edge: `P::Col` and `Q::Col` are different types and only
/// the implementor knows what the Q-phase value is. A fold that has
/// nothing to say about a payload cannot silently retag it — it does not
/// compile until it answers. Each answer may fail, so a transition that
/// cannot be made is an error rather than a fabricated value.
///
/// WHAT A NODE PUBLISHES IS NOT A PAYLOAD, and there is deliberately no
/// method here that answers with a relation identity — not for a head or a
/// step's result, not for the arm a whole-heading correlation names. An
/// identity is the relation authority's answer at the node's derivation;
/// it crosses a phase, or survives a same-phase rewrite, through the
/// phases' own doors (`phases::carry_scope`, `phases::carry_correlation_arm`),
/// which take no answer from the walk. A walk rewrites what a node HOLDS;
/// what it IS was decided where it was built.
#[allow(unused_variables)]
pub trait AstTransform<P: Phase, Q: Phase> {
    // -- Payload folds: required, one per phase-selected field ----------------

    /// A binding's decided-once recursion fact.

    /// A CTE binding's subject: the authored spelling and effect declaration
    /// before resolution, the exact bound scope after. The resolver's own
    /// CTE road is the only spender, so a cross-phase walk refuses here.

    /// The head and provenance resolution spends from an authored binding.

    /// One expression's output decision.
    fn fold_output(&mut self, output: P::Output) -> Result<Q::Output>;

    /// The ONE column a scalarized relation publishes.
    fn fold_scalar_output(&mut self, output: P::ScalarOutput) -> Result<Q::ScalarOutput>;

    /// The columns a destructuring pattern produces.
    fn fold_destructure(&mut self, destructure: P::Destructure) -> Result<Q::Destructure>;
    fn fold_drill(&mut self, drill: P::Drill) -> Result<Q::Drill>;

    /// A positional column reference.
    fn fold_column_ordinal(&mut self, ordinal: P::ColumnOrdinal) -> Result<Q::ColumnOrdinal>;

    /// A positional column range.
    fn fold_column_range(&mut self, range: P::ColumnRange) -> Result<Q::ColumnRange>;

    /// What a call names.
    fn fold_entity(&mut self, entity: P::Entity) -> Result<Q::Entity>;

    /// What a column reference names.
    fn fold_col(&mut self, column: P::Col) -> Result<Q::Col>;

    /// The name a caller-pattern slot offers.
    fn fold_binder(&mut self, binder: P::Binder) -> Result<Q::Binder>;

    /// The `@` that names which formal receives a piped relation.
    fn fold_placeholder(&mut self, landing: P::Placeholder) -> Result<Q::Placeholder>;

    /// The name a rename asks its target to answer to. Authored, a literal
    /// or template; bound, the spelling resolution minted for it.
    fn fold_rename_target(&mut self, target: P::RenameTarget) -> Result<Q::RenameTarget>;

    /// The open leaf. The position that applies an open body spends it
    /// during resolution, so a fold that is not that position refuses —
    /// and a bound phase has none to fold.
    fn fold_open_leaf(&mut self, leaf: P::OpenLeaf) -> Result<Q::OpenLeaf>;

    /// A cover's authored callable. The cover's own resolution applies and
    /// spends it, so any other fold refuses; a bound phase has none.
    fn fold_cover_callable(&mut self, callable: P::CoverCallable) -> Result<Q::CoverCallable>;
    /// The `..` that selects a call's context mode.
    fn fold_context_marker(&mut self, marker: P::ContextMarker) -> Result<Q::ContextMarker>;
    /// The higher-order landing slot. A phase-crossing fold SPENDS it: the
    /// judgment that read it ran in the resolver's own road, so the fold
    /// carries nothing forward.

    // -- Primary transform methods --------------------------------------------

    fn transform_query(&mut self, q: Query<P>) -> Result<Query<Q>> {
        walk_transform_query(self, q)
    }

    fn transform_relational(&mut self, e: Chain<P>) -> Result<Chain<Q>> {
        walk_transform_relational(self, e)
    }

    fn transform_boolean(&mut self, e: TruthExpression<P>) -> Result<TruthExpression<Q>> {
        walk_transform_boolean(self, e)
    }

    fn transform_domain(&mut self, e: DomainExpression<P>) -> Result<DomainExpression<Q>> {
        walk_transform_domain(self, e)
    }

    fn transform_function(&mut self, f: FunctionApplication<P>) -> Result<FunctionApplication<Q>> {
        walk_transform_function(self, f)
    }

    /// A CALLABLE OWNS ITS OWN SLOT. A rewrite spending an OUTER landing
    /// draws its boundary by overriding this and returning the callable
    /// unchanged.
    fn transform_callable(
        &mut self,
        c: crate::pipeline::asts::core::Callable<P>,
    ) -> Result<crate::pipeline::asts::core::Callable<Q>> {
        walk_transform_callable(self, c)
    }

    /// What a ground read NAMES, as the authored phase holds it. A leaf: a
    /// spelling before resolution, nothing after, so a bound phase's walk
    /// has nothing here to answer with — which relation a resolved read is
    /// stands in the head's own result. The default is the phases' door.
    fn transform_mention(&mut self, mention: P::Mention) -> Result<Q::Mention> {
        crate::pipeline::asts::core::phases::carry_mention::<P, Q>(mention)
    }

    // THERE IS DELIBERATELY NO HOOK THAT ANSWERS WITH A NODE'S STRUCTURE — not
    // a head, not a step, not a continuation, an operator, an access, a
    // projection item, a grouping, an ordering, a pattern, a header. A
    // relation's interface and support law were derived from that structure,
    // so a hook that answered with one could stand a witness's syntax under
    // an ordering's record. The structure crosses by the walk functions' own
    // reconstruction (`walk_transform_*`), which rebuild the variant they were
    // handed around what it holds; the hooks below answer for LEAVES — a
    // value, a truth, a reference, a callable, an authored mention — and for
    // whole relational SUBTREES, whose published identity the holding node
    // checks.

    /// REALIZE AN INTERIOR, once it has crossed: the one hook that answers
    /// with an interior, and the one place a walk may stand a different body
    /// under a head's kept identity.
    ///
    /// The answer is a [`Realized`] — the classification, over the body the
    /// interior stood over or over a `Replacement` the relation authority
    /// judged for that body — and the chain carrier judges it against the
    /// body the head actually holds before building the head. A walk with no
    /// realization to perform keeps the interior, which is the default.
    fn realize_interior(&mut self, pattern: InnerRelationPattern<Q>) -> Result<Realized<Q>> {
        Ok(Realized::kept(pattern))
    }

    // -- The subtree hook ------------------------------------------------------
    // Override to return FoldAction::Replaced when a pass fully handles a
    // relational subtree (the refiner's FAR cycle). What the subtree
    // publishes is checked by the node that holds it: an operand nested in a
    // step or a head comes back publishing what it published, or the fold
    // refuses there.

    fn transform_relational_action(&mut self, e: Chain<P>) -> Result<FoldAction<Chain<Q>>> {
        self.transform_relational(e).map(FoldAction::Continue)
    }

    // -- Leaf hooks -------------------------------------------------------------

    fn transform_enclyph(&mut self, e: Enclyph<P>) -> Result<Enclyph<Q>> {
        walk_transform_enclyph(self, e)
    }

    fn transform_record_member(&mut self, m: RecordMember<P>) -> Result<RecordMember<Q>> {
        walk_transform_record_member(self, m)
    }

    fn transform_case(&mut self, c: CaseExpression<P>) -> Result<CaseExpression<Q>> {
        walk_transform_case(self, c)
    }

    fn transform_value_template_part(
        &mut self,
        p: ValueTemplatePart<P>,
    ) -> Result<ValueTemplatePart<Q>> {
        walk_transform_value_template_part(self, p)
    }

    fn transform_reference(&mut self, r: Reference<P>) -> Result<Reference<Q>> {
        walk_transform_reference(self, r)
    }

    fn transform_window_frame(&mut self, f: WindowFrame<P>) -> Result<WindowFrame<Q>> {
        walk_transform_window_frame(self, f)
    }

    fn transform_frame_bound(&mut self, b: FrameBound<P>) -> Result<FrameBound<Q>> {
        walk_transform_frame_bound(self, b)
    }
}

// =============================================================================
// Walk functions — leaf containers
// =============================================================================

pub fn walk_transform_access<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    spec: Access<P>,
) -> Result<Access<Q>> {
    match spec {
        Access::All => Ok(Access::All),
        Access::Dequalify(cols) => Ok(Access::Dequalify(cols)),
        Access::DequalifyAll => Ok(Access::DequalifyAll),
        // A slot is folded by its OWN shape. Routing it through
        // `into_term`/`classify` made the fold decide the variant from the
        // term that came back, which turned every bound slot into a term the
        // moment the phase changed.
        Access::Slots(slots) => Ok(Access::Slots(
            slots.try_map(|slot| transform_slot(t, slot))?,
        )),
        Access::Unasked => Ok(Access::Unasked),
    }
}

pub fn transform_slot<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    slot: Slot<P>,
) -> Result<Slot<Q>> {
    match slot {
        Slot::Bind(binder) => Ok(Slot::Bind(t.fold_binder(binder)?)),
        Slot::Anon => Ok(Slot::Anon),
        Slot::Reuse(NamedReference(column)) => Ok(Slot::Reuse(NamedReference(t.fold_col(column)?))),
        Slot::Constraint(term) => Ok(Slot::Constraint(Box::new(t.transform_domain(*term)?))),
    }
}

pub fn walk_transform_ordering_spec<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    spec: OrderingSpec<P>,
) -> Result<OrderingSpec<Q>> {
    Ok(OrderingSpec {
        column: t.transform_domain(spec.column)?,
        direction: spec.direction,
    })
}

/// One publication item across a phase change. The naming travels as
/// authored: a fold that re-derived it from the value would be answering a
/// question the position already answered.
pub fn walk_transform_out_item<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    item: OutItem<P>,
) -> Result<OutItem<Q>> {
    match item {
        OutItem::One(one) => {
            // Q-phase output stamp: the checked fold preserves data.
            let expr = t.transform_domain(one.expr.clone())?;
            Ok(OutItem::One(one.folded(t, expr)?))
        }
        OutItem::Many(spread) => Ok(OutItem::Many(walk_transform_spread(t, spread)?)),
        OutItem::Whole => Ok(OutItem::Whole),
    }
}

pub fn walk_transform_named_out_item<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    item: NamedOutItem<P>,
) -> Result<NamedOutItem<Q>> {
    let expr = t.transform_domain(item.expr.clone())?;
    item.folded(t, expr)
}

pub fn walk_transform_group_spec<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    spec: GroupSpec<P>,
) -> Result<GroupSpec<Q>> {
    match spec {
        GroupSpec::Distinct { keys } => Ok(GroupSpec::Distinct {
            keys: keys.try_map(|item| walk_transform_out_item(t, item))?,
        }),
        GroupSpec::Reduce {
            keys,
            reductions,
            plan,
        } => Ok(GroupSpec::Reduce {
            keys: keys
                .into_iter()
                .map(|item| walk_transform_out_item(t, item))
                .collect::<Result<Vec<_>>>()?,
            reductions: reductions.try_map(|item| walk_transform_reduction_item(t, item))?,
            plan: transform_reduction_plan(t, plan)?,
        }),
    }
}

pub fn walk_transform_rename_spec<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    spec: RenameSpec<P>,
) -> Result<RenameSpec<Q>> {
    Ok(RenameSpec {
        from: walk_transform_rename_source(t, spec.from)?,
        to: t.fold_rename_target(spec.to)?,
    })
}

pub fn walk_transform_reposition_spec<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    spec: RepositionSpec<P>,
) -> Result<RepositionSpec<Q>> {
    Ok(RepositionSpec {
        column: t.transform_reference(spec.column)?,
        position: spec.position,
    })
}

pub fn walk_transform_tabular_row<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    row: TabularRow<Datum<P>>,
) -> Result<TabularRow<Datum<Q>>> {
    Ok(TabularRow(Box::new((*row.0).try_map(
        |datum| match datum {
            Datum::Value(value) => t.transform_domain(value).map(Datum::Value),
            Datum::SparseFill { column, fallback } => Ok(Datum::SparseFill { column, fallback }),
        },
    )?)))
}

pub fn walk_transform_window_frame<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    frame: WindowFrame<P>,
) -> Result<WindowFrame<Q>> {
    Ok(WindowFrame {
        mode: frame.mode,
        start: t.transform_frame_bound(frame.start)?,
        end: t.transform_frame_bound(frame.end)?,
    })
}

pub fn walk_transform_frame_bound<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    bound: FrameBound<P>,
) -> Result<FrameBound<Q>> {
    match bound {
        FrameBound::Unbounded => Ok(FrameBound::Unbounded),
        FrameBound::CurrentRow => Ok(FrameBound::CurrentRow),
        FrameBound::Preceding(expr) => {
            Ok(FrameBound::Preceding(Box::new(t.transform_domain(*expr)?)))
        }
        FrameBound::Following(expr) => {
            Ok(FrameBound::Following(Box::new(t.transform_domain(*expr)?)))
        }
    }
}

// =============================================================================
// Walk functions — expression containers
// =============================================================================

pub fn walk_transform_value_template_part<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    part: ValueTemplatePart<P>,
) -> Result<ValueTemplatePart<Q>> {
    match part {
        ValueTemplatePart::Text(s) => Ok(ValueTemplatePart::Text(s)),
        ValueTemplatePart::Interpolation(expr) => Ok(ValueTemplatePart::Interpolation(Box::new(
            t.transform_domain(*expr)?,
        ))),
    }
}

/// A reference across a phase change. Named and positional spellings ask
/// the same addressing authority, so one walker answers for both.
pub fn walk_transform_reference<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    reference: Reference<P>,
) -> Result<Reference<Q>> {
    match reference {
        Reference::Named(NamedReference(column)) => {
            Ok(Reference::Named(NamedReference(t.fold_col(column)?)))
        }
        Reference::Ordinal(ordinal) => Ok(Reference::Ordinal(t.fold_column_ordinal(ordinal)?)),
        Reference::Physical(column) => Ok(Reference::Physical(Q::admit_physical(
            P::into_physical(column)?,
        )?)),
    }
}

pub fn walk_transform_enclyph<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    enclyph: Enclyph<P>,
) -> Result<Enclyph<Q>> {
    match enclyph {
        Enclyph::Record(record) => Ok(Enclyph::Record(Record {
            members: record.members.try_map(|m| t.transform_record_member(m))?,
        })),
        Enclyph::EmptyRecord(_) => Ok(Enclyph::EmptyRecord(Q::admit_empty_record()?)),
        Enclyph::Tuple(tuple) => Ok(Enclyph::Tuple(Box::new(Tuple {
            elements: tuple.elements.try_map(|e| {
                Ok::<_, crate::error::DelightQLError>(match e {
                    TupleElement::Value(value) => TupleElement::Value(t.transform_domain(value)?),
                    TupleElement::Spread(spread) => {
                        TupleElement::Spread(walk_transform_spread(t, spread)?)
                    }
                })
            })?,
        }))),
    }
}

pub fn walk_transform_record_member<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    member: RecordMember<P>,
) -> Result<RecordMember<Q>> {
    match member {
        RecordMember::Keyed { key, value } => Ok(RecordMember::Keyed {
            key,
            value: Box::new(t.transform_domain(*value)?),
        }),
        RecordMember::Induced { key, value } => Ok(RecordMember::Induced {
            key,
            value: Box::new(t.transform_enclyph(*value)?),
        }),
        RecordMember::Spread(spread) => Ok(RecordMember::Spread(walk_transform_spread(t, spread)?)),
        RecordMember::Metadata { key, group } => Ok(RecordMember::Metadata {
            key,
            group: Box::new(walk_transform_metadata_group(t, *group)?),
        }),
        RecordMember::SelfKeyed(NamedReference(column)) => {
            Ok(RecordMember::SelfKeyed(NamedReference(t.fold_col(column)?)))
        }
    }
}

/// A REDUCTION PUBLISHES ONE COLUMN, and the two things that publish one
/// cross unchanged in their own arms.
pub fn walk_transform_reduction_item<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    item: ReductionItem<P>,
) -> Result<ReductionItem<Q>> {
    match item {
        ReductionItem::Out(item) => Ok(ReductionItem::Out(walk_transform_out_item(t, item)?)),
        ReductionItem::Metadata(metadata) => {
            let group = walk_transform_metadata_group(t, metadata.group.clone())?;
            Ok(ReductionItem::Metadata(metadata.folded(t, group)?))
        }
        ReductionItem::Pivot(pivot) => Ok(ReductionItem::Pivot(
            crate::pipeline::asts::core::PivotSpec {
                value_column: Box::new(t.transform_domain(*pivot.value_column)?),
                pivot_key: Box::new(t.transform_domain(*pivot.pivot_key)?),
                values: pivot.values,
            },
        )),
        ReductionItem::Delegate(delegate) => Ok(ReductionItem::Delegate(DelegateSpec {
            payload: delegate
                .payload
                .into_iter()
                .map(|item| walk_transform_out_item(t, item))
                .collect::<Result<Vec<_>>>()?,
            order: delegate
                .order
                .into_iter()
                .map(|o| walk_transform_ordering_spec(t, o))
                .collect::<Result<Vec<_>>>()?,
        })),
    }
}

pub fn walk_transform_metadata_group<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    group: MetadataGroup<P>,
) -> Result<MetadataGroup<Q>> {
    Ok(MetadataGroup {
        key: t.fold_col(group.key)?,
        target: match group.target {
            MetadataTarget::Enclyph(enclyph) => {
                MetadataTarget::Enclyph(t.transform_enclyph(enclyph)?)
            }
            MetadataTarget::Group(nested) => {
                MetadataTarget::Group(Box::new(walk_transform_metadata_group(t, *nested)?))
            }
        },
        cte_requirements: group
            .cte_requirements
            .map(|r| transform_cte_requirements(t, r))
            .transpose()?,
    })
}

/// A PATTERN CROSSES AS A PATTERN. Its binders are phase-selected; its keys,
/// reaches and names are spec material that no phase decides.
pub fn walk_transform_tree_pattern<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    pattern: TreePattern<P>,
) -> Result<TreePattern<Q>> {
    match pattern {
        TreePattern::Record(record) => Ok(TreePattern::Record(RecordPattern {
            members: record.members.try_map(|member| -> Result<_> {
                match member {
                    RecordPatternMember::Binder(binder) => {
                        Ok(RecordPatternMember::Binder(t.fold_binder(binder)?))
                    }
                    RecordPatternMember::Keyed { key, binder } => Ok(RecordPatternMember::Keyed {
                        key,
                        binder: t.fold_binder(binder)?,
                    }),
                    RecordPatternMember::Nested {
                        key,
                        iteration,
                        pattern,
                    } => Ok(RecordPatternMember::Nested {
                        key,
                        iteration,
                        pattern: Box::new(walk_transform_tree_pattern(t, *pattern)?),
                    }),
                    RecordPatternMember::Path(binding) => Ok(RecordPatternMember::Path(binding)),
                    RecordPatternMember::Metadata { key, target } => {
                        Ok(RecordPatternMember::Metadata {
                            key: t.fold_binder(key)?,
                            target: match target {
                                PatternTarget::Pattern(inner) => PatternTarget::Pattern(Box::new(
                                    walk_transform_tree_pattern(t, *inner)?,
                                )),
                                PatternTarget::Disregarded => PatternTarget::Disregarded,
                            },
                        })
                    }
                    RecordPatternMember::Disregarded => Ok(RecordPatternMember::Disregarded),
                }
            })?,
        })),
        TreePattern::Array(array) => Ok(TreePattern::Array(array)),
    }
}

pub fn walk_transform_case<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    case: CaseExpression<P>,
) -> Result<CaseExpression<Q>> {
    Ok(match case {
        CaseExpression::Anchored {
            anchor,
            arms,
            default,
        } => CaseExpression::Anchored {
            anchor: Box::new(t.transform_domain(*anchor)?),
            arms: crate::pipeline::asts::vocabulary::Vec1::try_from_vec(
                arms.into_vec()
                    .into_iter()
                    .map(|arm| {
                        Ok(crate::pipeline::asts::core::MatchArm {
                            term: arm.term,
                            result: Box::new(t.transform_domain(*arm.result)?),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
            .expect("a phase change preserves arm count"),
            default: default
                .map(|result| {
                    Ok::<_, crate::error::DelightQLError>(Box::new(t.transform_domain(*result)?))
                })
                .transpose()?,
        },
        CaseExpression::Searched { arms, default } => CaseExpression::Searched {
            arms: crate::pipeline::asts::vocabulary::Vec1::try_from_vec(
                arms.into_vec()
                    .into_iter()
                    .map(|arm| {
                        Ok(crate::pipeline::asts::core::SearchedArm {
                            condition: Box::new(t.transform_boolean(*arm.condition)?),
                            result: Box::new(t.transform_domain(*arm.result)?),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
            .expect("a phase change preserves arm count"),
            default: default
                .map(|result| {
                    Ok::<_, crate::error::DelightQLError>(Box::new(t.transform_domain(*result)?))
                })
                .transpose()?,
        },
    })
}

// =============================================================================
// Walk functions — core expressions
// =============================================================================

pub fn walk_transform_domain<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    expr: DomainExpression<P>,
) -> Result<DomainExpression<Q>> {
    match expr {
        // Leaf variants — no recursive children, phase-agnostic fields only
        DomainExpression::Reference(reference) => Ok(DomainExpression::Reference(
            t.transform_reference(reference)?,
        )),
        DomainExpression::Application(FunctionApplication::Ground(value)) => Ok(
            DomainExpression::Application(FunctionApplication::Ground(value)),
        ),
        DomainExpression::Application(FunctionApplication::Open(hole)) => Ok(
            DomainExpression::Application(FunctionApplication::Open(t.fold_open_leaf(hole)?)),
        ),
        // Recursive variants
        DomainExpression::Application(f) => {
            Ok(DomainExpression::Application(t.transform_function(f)?))
        }
    }
}

/// Carry a whole-heading correlation across a phase change. The MODE
/// travels: which columns the two arms pair is found by name in one form and
/// by position in the other, and a pass that dropped the distinction could
/// not tell the two spellings apart again. The ARMS are relation identities
/// and cross by the phases' door: the walk is handed nothing here and
/// answers nothing.
pub fn transform_whole_heading<P: Phase, Q: Phase>(
    whole: WholeHeading<P>,
) -> Result<WholeHeading<Q>> {
    use crate::pipeline::asts::core::phases::carry_correlation_arm;
    Ok(match whole {
        WholeHeading::ByName { left, right } => WholeHeading::ByName {
            left: carry_correlation_arm::<P, Q>(left)?,
            right: carry_correlation_arm::<P, Q>(right)?,
        },
        WholeHeading::ByPosition { left, right } => WholeHeading::ByPosition {
            left: carry_correlation_arm::<P, Q>(left)?,
            right: carry_correlation_arm::<P, Q>(right)?,
        },
    })
}

/// Carry a pair-scoped correlation's predicate across a phase change.
pub fn transform_corr_pred<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    predicate: CorrPred<P>,
) -> Result<CorrPred<Q>> {
    Ok(match predicate {
        CorrPred::Expression(expression) => CorrPred::Expression(t.transform_boolean(expression)?),
        CorrPred::Whole(whole) => CorrPred::Whole(transform_whole_heading::<P, Q>(whole)?),
    })
}

/// Carry a member's correlation across a phase change.
///
/// The two arms travel by different roads and neither is the other's
/// fallback: a condition is transformed as the truth it is, and a
/// correspondence is carried through the phase door that decides whether the
/// target phase may hold one at all.
pub fn transform_member_correlation<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    correlation: MemberCorrelation<P>,
) -> Result<MemberCorrelation<Q>> {
    Ok(match correlation {
        MemberCorrelation::Condition(condition) => {
            MemberCorrelation::Condition(t.transform_boolean(condition)?)
        }
        MemberCorrelation::Correspond(carried) => MemberCorrelation::Correspond(
            crate::pipeline::asts::core::phases::carry_correspondence::<P, Q>(carried)?,
        ),
        MemberCorrelation::Cartesian(decided) => MemberCorrelation::Cartesian(
            crate::pipeline::asts::core::phases::carry_decided::<P, Q>(decided)?,
        ),
    })
}

pub fn walk_transform_boolean<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    expr: TruthExpression<P>,
) -> Result<TruthExpression<Q>> {
    match expr {
        TruthExpression::Comparison(Comparison {
            operator,
            left,
            right,
        }) => Ok(TruthExpression::Comparison(Comparison {
            operator,
            left: Box::new(t.transform_domain(*left)?),
            right: Box::new(t.transform_domain(*right)?),
        })),
        // The count survives a per-member rewrite, so an n-ary composition
        // does not have to reprove that it still has two members.
        TruthExpression::Conjunction(parts) => Ok(TruthExpression::Conjunction(Box::new(
            (*parts).try_map(|part| t.transform_boolean(part))?,
        ))),
        TruthExpression::Disjunction(parts) => Ok(TruthExpression::Disjunction(Box::new(
            (*parts).try_map(|part| t.transform_boolean(part))?,
        ))),
        TruthExpression::Not { expr } => Ok(TruthExpression::Not {
            expr: Box::new(t.transform_boolean(*expr)?),
        }),
        TruthExpression::Existence(Existence {
            polarity,
            relation: subquery,
            addressing,
        }) => Ok(TruthExpression::Existence(Existence {
            polarity,
            relation: Box::new(t.transform_relational_action(*subquery)?.into_inner()),
            addressing: crate::pipeline::asts::core::phases::carry_probe_addressing::<P, Q>(
                addressing,
            )?,
        })),
        TruthExpression::Membership(Membership {
            probe,
            rows,
            negated,
            source,
        }) => Ok(TruthExpression::Membership(Membership {
            probe: transform_probe(t, probe)?,
            rows: rows.try_map(|row| -> Result<_> {
                Ok(ValueRow(row.0.try_map(|e| t.transform_domain(e))?))
            })?,
            negated,
            source,
        })),
        TruthExpression::RelationalMembership(RelationalMembership {
            probe,
            relation,
            addressing,
            negated,
        }) => Ok(TruthExpression::RelationalMembership(
            RelationalMembership {
                probe: transform_probe(t, probe)?,
                relation: Box::new(t.transform_relational_action(*relation)?.into_inner()),
                addressing: crate::pipeline::asts::core::phases::carry_probe_addressing::<P, Q>(
                    addressing,
                )?,
                negated,
            },
        )),
        TruthExpression::Sigma(SigmaApplication { proof, polarity }) => {
            use crate::pipeline::asts::core::NamedProof;
            let proof = match proof {
                NamedProof::Call(call) => NamedProof::Call(transform_pure_call(t, call)?),
                NamedProof::Body(body) => {
                    let body = t.transform_boolean(P::into_sigma_body(body))?;
                    NamedProof::Body(Q::admit_sigma_body(body)?)
                }
            };
            Ok(TruthExpression::Sigma(SigmaApplication { proof, polarity }))
        }
    }
}

/// An argument's value, carrying its own DISTINCT. One walk, so the
/// modifier cannot be dropped crossing a phase.
pub fn transform_argument_value<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    value: ArgumentValue<P>,
) -> Result<ArgumentValue<Q>> {
    Ok(ArgumentValue {
        distinct: value.distinct,
        value: t.transform_domain(value.value)?,
    })
}

/// A probe carries VALUES, and the two shapes differ only in how many. One
/// walk, so a row probe cannot lose its width crossing a phase.
pub fn transform_probe<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    probe: Probe<P>,
) -> Result<Probe<Q>> {
    Ok(match probe {
        Probe::Value(value) => Probe::Value(Box::new(t.transform_domain(*value)?)),
        Probe::Row(values) => Probe::Row(values.try_map(|value| t.transform_domain(value))?),
    })
}

/// A RELATION MADE ONE VALUE, rewritten whole: the body it compresses, the
/// compression that proves it, and the column the degree judgment answered
/// with.
pub fn transform_scalar_relation<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    relation: crate::pipeline::asts::core::ScalarRelation<P>,
) -> Result<crate::pipeline::asts::core::ScalarRelation<Q>> {
    use crate::pipeline::asts::core::ScalarRelation;
    Ok(match relation {
        ScalarRelation::Named { identifier, body } => ScalarRelation::Named {
            identifier,
            body: Box::new(transform_scalarized(t, *body)?),
        },
        ScalarRelation::Sourceless { body } => ScalarRelation::Sourceless {
            body: Box::new(transform_scalarized(t, *body)?),
        },
    })
}

pub fn transform_scalarized<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    body: crate::pipeline::asts::core::ScalarizedRelation<P>,
) -> Result<crate::pipeline::asts::core::ScalarizedRelation<Q>> {
    // ONE ROAD FOR EVERY PASS. The compression goes back on the chain it
    // closes, the pass rewrites the relation it always rewrote, and the
    // compression comes off again — so a pass sees exactly the relation it
    // saw before this carrier existed, and the carrier is what stands
    // between the phases.
    let output = t.fold_scalar_output(body.output.clone())?;
    let attached = body.attached();
    let rewritten = t.transform_relational_action(attached)?.into_inner();
    crate::pipeline::asts::core::ScalarizedRelation::detach(rewritten, output)
}

pub fn walk_transform_callable<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    callable: crate::pipeline::asts::core::Callable<P>,
) -> Result<crate::pipeline::asts::core::Callable<Q>> {
    use crate::pipeline::asts::core::Callable;
    Ok(match callable {
        Callable::Functor(application) => {
            Callable::Functor(transform_standard_application(t, application)?)
        }
        Callable::String(template) => Callable::String(
            ValueTemplate::interpolating(
                template
                    .into_parts()
                    .into_iter()
                    .map(|p| t.transform_value_template_part(p))
                    .collect::<Result<Vec<_>>>()?,
            )
            .expect("a phase change preserves the interpolation invariant"),
        ),
        Callable::Lambda(lambda) => Callable::Lambda(crate::pipeline::asts::core::Lambda {
            body: Box::new(t.transform_domain(*lambda.body)?),
        }),
    })
}

/// A DECLARED MODE, across a phase change. The declared names and the ground
/// match rows are spellings and values, not phase payloads; only the output
/// rows hold tree nodes, and they travel by the ordinary domain fold.
pub fn transform_fact_function_mode<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    mode: crate::pipeline::asts::core::FactFunctionMode<P>,
) -> Result<crate::pipeline::asts::core::FactFunctionMode<Q>> {
    use crate::pipeline::asts::core::{FactFunctionArm, FactFunctionMode};
    let arms = mode.arms.try_map(|arm| -> Result<FactFunctionArm<Q>> {
        Ok(FactFunctionArm {
            inputs: arm.inputs,
            outputs: arm.outputs.try_map(|out| t.transform_domain(out))?,
        })
    })?;
    let default = match mode.default {
        Some(row) => Some(row.try_map(|out| t.transform_domain(out))?),
        None => None,
    };
    Ok(FactFunctionMode {
        inputs: mode.inputs,
        outputs: mode.outputs,
        arms,
        default,
    })
}

pub fn walk_transform_function<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    func: FunctionApplication<P>,
) -> Result<FunctionApplication<Q>> {
    match func {
        FunctionApplication::Ground(value) => Ok(FunctionApplication::Ground(value)),
        FunctionApplication::Open(hole) => Ok(FunctionApplication::Open(t.fold_open_leaf(hole)?)),
        FunctionApplication::Standard(application) => Ok(FunctionApplication::Standard(
            transform_standard_application(t, application)?,
        )),
        // The pick travels with its declaration. `admit_mode_witness` is the
        // one door, so whether a phase may hold one is decided by the phases
        // and not by whichever fold happened to be written.
        FunctionApplication::FieldSelect(select) => {
            let witness = match P::into_mode_witness(select.dependency) {
                Some(witness) => Some(crate::pipeline::asts::core::ModeWitness {
                    entity: witness.entity,
                    mode: transform_fact_function_mode(t, witness.mode)?,
                    inputs: witness
                        .inputs
                        .into_iter()
                        .map(|input| t.fold_scalar_output(input))
                        .collect::<Result<_>>()?,
                    selected: witness.selected,
                }),
                None => None,
            };
            Ok(FunctionApplication::FieldSelect(
                crate::pipeline::asts::core::FieldSelect {
                    application: transform_standard_application(t, select.application)?,
                    field: t.fold_col(select.field)?,
                    dependency: Q::admit_mode_witness(witness)?,
                },
            ))
        }
        FunctionApplication::Enclyph(enclyph) => {
            Ok(FunctionApplication::Enclyph(t.transform_enclyph(enclyph)?))
        }
        FunctionApplication::Infix(infix) => Ok(FunctionApplication::Infix(
            crate::pipeline::asts::core::InfixApplication {
                operator: infix.operator,
                left: Box::new(t.transform_domain(*infix.left)?),
                right: Box::new(t.transform_domain(*infix.right)?),
            },
        )),
        FunctionApplication::Template(template) => Ok(FunctionApplication::Template(
            ValueTemplate::interpolating(
                template
                    .into_parts()
                    .into_iter()
                    .map(|p| t.transform_value_template_part(p))
                    .collect::<Result<Vec<_>>>()?,
            )
            .expect("a phase change preserves the interpolation invariant"),
        )),
        // The synthesized SELECTION carries clause bodies, ordinary values.
        FunctionApplication::ClauseSelection(selection) => Ok(
            FunctionApplication::ClauseSelection(crate::pipeline::asts::core::ClauseSelection {
                arms: selection
                    .arms
                    .into_iter()
                    .map(|arm| -> Result<_> {
                        Ok(crate::pipeline::asts::core::ClauseArm {
                            guard: arm.guard.map(|g| t.transform_boolean(g)).transpose()?,
                            result: t.transform_domain(arm.result)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            }),
        ),
        FunctionApplication::Case(case) => Ok(FunctionApplication::Case(t.transform_case(case)?)),
        FunctionApplication::Scalarized(relation) => Ok(FunctionApplication::Scalarized(
            transform_scalar_relation(t, relation)?,
        )),
        FunctionApplication::JsonAccess(access) => Ok(FunctionApplication::JsonAccess(
            crate::pipeline::asts::core::JsonAccess {
                source: Box::new(t.transform_domain(*access.source)?),
                path: access.path,
            },
        )),
        // THE ONE PHASE ROAD: the crossing folds its own truth through this
        // walk's truth road and survives as the same crossing. Nothing here
        // decides that a truth is a value — that was decided where it was
        // authored — and nothing here chooses which truth it is.
        FunctionApplication::Crossed(crossing) => {
            Ok(FunctionApplication::Crossed(crossing.folded(t)?))
        }
    }
}

fn transform_functor_call_inner<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    call: FunctorCall<P>,
) -> Result<FunctorCall<Q>> {
    use crate::pipeline::asts::core::operators::{
        CallArguments, HoArgument, HoPart, ScalarArgument,
    };
    let arguments = match call.arguments {
        CallArguments::None => CallArguments::None,
        CallArguments::Scalar(members) => CallArguments::Scalar(
            members
                .into_iter()
                .map(|member| match member {
                    ScalarArgument::Value(value) => {
                        transform_argument_value(t, value).map(ScalarArgument::Value)
                    }
                    ScalarArgument::Callable(callable) => {
                        t.transform_callable(callable).map(ScalarArgument::Callable)
                    }
                    ScalarArgument::Spread(spread) => {
                        walk_transform_spread(t, spread).map(ScalarArgument::Spread)
                    }
                    ScalarArgument::Star => Ok(ScalarArgument::Star),
                    ScalarArgument::Context(marker) => {
                        t.fold_context_marker(marker).map(ScalarArgument::Context)
                    }
                })
                .collect::<Result<Vec<_>>>()?,
        ),
        CallArguments::HigherOrder(part) => {
            CallArguments::HigherOrder(HoPart::of(part.into_members().try_map(|argument| {
                match argument {
                    // THE KIND TRAVELS WITH THE MEMBER. A fold rewrites what
                    // a position carries; it does not decide what a position
                    // IS, so a landed relation crosses a phase landed.
                    HoArgument::Relation(relation) => t
                        .transform_relational_action(relation)
                        .map(|r| HoArgument::Relation(r.into_inner())),
                    HoArgument::Rule(rule) => t
                        .transform_relational_action(rule)
                        .map(|r| HoArgument::Rule(r.into_inner())),
                    HoArgument::Landed(relation) => t
                        .transform_relational_action(relation)
                        .map(|r| HoArgument::Landed(r.into_inner())),
                    HoArgument::Value(value) => {
                        transform_argument_value(t, value).map(HoArgument::Value)
                    }
                    HoArgument::Landing(landing) => {
                        t.fold_placeholder(landing).map(HoArgument::Landing)
                    }
                    HoArgument::Skip => Ok(HoArgument::Skip),
                }
            })?))
        }
    };
    Ok(FunctorCall {
        callee: t.fold_entity(call.callee)?,
        arguments,
        marks: call.marks,
    })
}

/// AN APPLICATION, REWRITTEN WHOLE. The call, the window it is modified by
/// and the guard it is filtered by reach the same rewrite the visit reaches,
/// so what a walk counted is what a transform spends.
pub fn transform_standard_application<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    application: crate::pipeline::asts::core::StandardApplication<P>,
) -> Result<crate::pipeline::asts::core::StandardApplication<Q>> {
    let call = transform_pure_call(t, application.call)?;
    let window = application
        .window
        .map(|window| {
            Ok::<_, crate::error::DelightQLError>(crate::pipeline::asts::core::WindowSpec {
                partition: window
                    .partition
                    .into_iter()
                    .map(|expr| t.transform_domain(expr))
                    .collect::<Result<Vec<_>>>()?,
                ordering: window
                    .ordering
                    .into_iter()
                    .map(|ordering| walk_transform_ordering_spec(t, ordering))
                    .collect::<Result<Vec<_>>>()?,
                frame: window
                    .frame
                    .map(|frame| t.transform_window_frame(frame))
                    .transpose()?,
            })
        })
        .transpose()?;
    let guard = application
        .guard
        .map(|condition| t.transform_boolean(*condition).map(Box::new))
        .transpose()?;
    Ok(crate::pipeline::asts::core::StandardApplication {
        call,
        guard,
        window,
    })
}

pub fn transform_pure_call<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    call: PureCall<P>,
) -> Result<PureCall<Q>> {
    Ok(PureCall::from_inner(transform_functor_call_inner(
        t,
        call.into_inner(),
    )?))
}

fn transform_sealed_call<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    call: SealedCall<P>,
) -> Result<SealedCall<Q>> {
    let effect = call.is_effect();
    Ok(SealedCall::from_inner(
        transform_functor_call_inner(t, call.into_inner())?,
        effect,
    ))
}

// =============================================================================
// Walk functions — sigma, operator, pipe, inner_relation
// =============================================================================

pub fn walk_transform_operator<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    op: PipeOp<P>,
) -> Result<PipeOp<Q>> {
    match op {
        PipeOp::Project(items) => Ok(PipeOp::Project(
            items.try_map(|item| walk_transform_out_item(t, item))?,
        )),
        PipeOp::Embed(items) => Ok(PipeOp::Embed(
            items.try_map(|item| walk_transform_out_item(t, item))?,
        )),
        PipeOp::Group(spec) => Ok(PipeOp::Group(walk_transform_group_spec(t, spec)?)),
        PipeOp::MapCover(MapCover {
            callable,
            selector,
            guard,
            cells,
        }) => Ok(PipeOp::MapCover(MapCover {
            callable: t.fold_cover_callable(callable)?,
            selector: selector
                .into_iter()
                .map(|item| walk_transform_selector_item(t, item))
                .collect::<Result<Vec<_>>>()?,
            guard: guard
                .map(|c| t.transform_boolean(*c).map(|b| Box::new(b)))
                .transpose()?,
            cells: cells
                .into_iter()
                .map(|cell| {
                    Ok(crate::pipeline::asts::core::operators::AppliedCell {
                        column: cell.column,
                        expr: t.transform_domain(cell.expr)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        })),
        PipeOp::ProjectOut(selector) => Ok(PipeOp::ProjectOut(
            selector
                .into_iter()
                .map(|item| walk_transform_selector_item(t, item))
                .collect::<Result<Vec<_>>>()?,
        )),
        PipeOp::Rename(specs) => Ok(PipeOp::Rename(
            specs.try_map(|s| walk_transform_rename_spec(t, s))?,
        )),
        PipeOp::Transform { items, guard } => Ok(PipeOp::Transform {
            items: items.try_map(|item| walk_transform_named_out_item(t, item))?,
            guard: guard
                .map(|c| t.transform_boolean(*c).map(|b| Box::new(b)))
                .transpose()?,
        }),
        PipeOp::EmbedMapCover(EmbedMapCover {
            callable,
            naming,
            selector,
            cells,
        }) => Ok(PipeOp::EmbedMapCover(EmbedMapCover {
            callable: t.fold_cover_callable(callable)?,
            naming,
            selector: selector
                .into_iter()
                .map(|item| walk_transform_selector_item(t, item))
                .collect::<Result<Vec<_>>>()?,
            cells: cells
                .into_iter()
                .map(|cell| {
                    Ok(crate::pipeline::asts::core::operators::AppliedCell {
                        column: cell.column,
                        expr: t.transform_domain(cell.expr)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        })),
    }
}

pub fn walk_transform_anon_table<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    anon: AnonTable<P>,
) -> Result<AnonTable<Q>> {
    Ok(AnonTable {
        body: TabularBody {
            header: anon
                .body
                .header
                .map(|header| {
                    (*header.0).try_map(|item| -> Result<HeaderItem<Q>> {
                        Ok(HeaderItem {
                            slot: transform_slot(t, item.slot)?,
                            sparse: item.sparse,
                        })
                    })
                })
                .transpose()?
                .map(|row| TabularRow(Box::new(row))),
            rows: anon
                .body
                .rows
                .try_map(|row| walk_transform_tabular_row(t, row))?,
        },
    })
}

pub fn walk_transform_continuation<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    continuation: Continuation<P>,
) -> Result<Continuation<Q>> {
    match continuation {
        Continuation::Access { access, named } => Ok(Continuation::Access {
            access: walk_transform_access(t, access)?,
            named: crate::pipeline::asts::core::phases::carry_stage_name::<P, Q>(named)?,
        }),
        Continuation::Correlate { whole } => Ok(Continuation::Correlate {
            whole: transform_whole_heading::<P, Q>(whole)?,
        }),
        Continuation::Restrict { condition, origin } => Ok(Continuation::Restrict {
            condition: t.transform_boolean(condition)?,
            origin,
        }),
        // The condition is rewritten INSIDE the value the correlation act
        // minted, and whether the destination phase holds one is the
        // phases' answer through the one door.
        Continuation::Correlated(correlated) => Ok(Continuation::Correlated(
            crate::pipeline::asts::core::phases::carry_correlated::<P, Q>(
                correlated,
                |condition| t.transform_boolean(condition),
            )?,
        )),
        Continuation::Bound { bound } => Ok(Continuation::Bound { bound }),
        Continuation::Destructure {
            source,
            pattern,
            mode,
            schema,
        } => Ok(Continuation::Destructure {
            source: Box::new(t.transform_domain(*source)?),
            pattern: walk_transform_tree_pattern(t, pattern)?,
            mode,
            schema: t.fold_destructure(schema)?,
        }),
        Continuation::Member {
            rhs,
            correlation,
            join_type,
        } => Ok(Continuation::Member {
            rhs: t.transform_relational_action(rhs)?.into_inner(),
            correlation: Q::admit_member_correlation(
                P::into_member_correlation(correlation)
                    .map(|c| transform_member_correlation(t, c))
                    .transpose()?,
            )?,
            // A comma with no decided orientation is an inner join, and the
            // refined phase works with a decided one: the narrowing is the
            // phases' answer, made here for every member that crosses.
            join_type: Q::join_orientation(join_type),
        }),
        Continuation::BagOp {
            operator,
            arm,
            correlation,
        } => Ok(Continuation::BagOp {
            operator,
            arm: t.transform_relational_action(arm)?.into_inner(),
            correlation: Q::admit_correlation(
                P::into_correlation(correlation)
                    .map(|c| {
                        Ok::<_, crate::error::DelightQLError>(BagCorrelation {
                            with_arm: c.with_arm,
                            predicate: transform_corr_pred(t, c.predicate)?,
                            min_multiplicity: c.min_multiplicity,
                        })
                    })
                    .transpose()?,
            )?,
        }),
        Continuation::Pipe { operator, named } => Ok(Continuation::Pipe {
            operator: walk_transform_operator(t, operator)?,
            // The one door: a fold between two phases that both hold
            // authored characters carries the name, and a fold INTO a phase
            // that has spent it refuses rather than dropping it silently.
            // Which of those this is, is the phases' answer, not the
            // walker's.
            named: crate::pipeline::asts::core::phases::carry_stage_name::<P, Q>(named)?,
        }),
        Continuation::ErJoin(carried) => {
            let step = P::into_er_join(carried);
            Ok(Continuation::ErJoin(Q::admit_er_join(ErJoinStep {
                transitive: step.transitive,
                context: step.context,
                left_spelling: step.left_spelling,
                right_spelling: step.right_spelling,
                rhs: t.transform_relational(step.rhs)?,
            })?))
        }
        Continuation::Structural(step) => Ok(Continuation::Structural(
            walk_transform_structural_step(t, step)?,
        )),
    }
}

/// A structural run step crossing phases: the form's payloads rephase and
/// the stage name carries through the one door every stage name uses.
/// Exhaustive over [`StructuralForm`], so a new structural kind cannot cross
/// a phase without deciding its walk here.
pub fn walk_transform_structural_step<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    step: crate::pipeline::asts::core::StructuralStep<P>,
) -> Result<crate::pipeline::asts::core::StructuralStep<Q>> {
    use crate::pipeline::asts::core::{StructuralForm, StructuralStep};
    let crate::pipeline::asts::core::StructuralStep { form, named } = step;
    let form = match form {
        StructuralForm::Ordering { specs, bound } => StructuralForm::Ordering {
            specs: specs
                .into_iter()
                .map(|s| walk_transform_ordering_spec(t, s))
                .collect::<Result<Vec<_>>>()?,
            bound,
        },
        StructuralForm::Reposition { moves } => StructuralForm::Reposition {
            moves: moves
                .into_iter()
                .map(|m| walk_transform_reposition_spec(t, m))
                .collect::<Result<Vec<_>>>()?,
        },
        StructuralForm::Meta => StructuralForm::Meta,
        StructuralForm::Witness { polarity } => StructuralForm::Witness { polarity },
        StructuralForm::SignedWitness => StructuralForm::SignedWitness,
        StructuralForm::Drill { drill } => StructuralForm::Drill {
            drill: t.fold_drill(drill)?,
        },
        StructuralForm::Narrow {
            nest,
            pattern,
            schema,
        } => StructuralForm::Narrow {
            nest: t.transform_reference(nest)?,
            pattern: match walk_transform_tree_pattern(t, TreePattern::Record(pattern))? {
                TreePattern::Record(pattern) => pattern,
                TreePattern::Array(_) => unreachable!("a record pattern crosses as one"),
            },
            schema: t.fold_destructure(schema)?,
        },
    };
    Ok(StructuralStep {
        form,
        named: crate::pipeline::asts::core::phases::carry_stage_name::<P, Q>(named)?,
    })
}


/// The deferred items of a pattern cross as VALUES: each keeps the position
/// it was minted for and its value takes the leaf hook.
fn transform_deferred<P: Phase, Q: Phase, T: AstTransform<P, Q> + ?Sized>(
    t: &mut T,
    deferred: Vec<crate::pipeline::asts::core::expressions::DeferredItem<P>>,
) -> Result<Vec<crate::pipeline::asts::core::expressions::DeferredItem<Q>>> {
    deferred
        .into_iter()
        .map(|item| item.crossing(|value| t.transform_domain(value)))
        .collect()
}

pub fn walk_transform_inner_relation<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    pattern: InnerRelationPattern<P>,
) -> Result<InnerRelationPattern<Q>> {
    match pattern {
        InnerRelationPattern::Indeterminate {
            identifier,
            subquery,
        } => Ok(InnerRelationPattern::Indeterminate {
            identifier,
            subquery: Box::new(t.transform_relational_action(*subquery)?.into_inner()),
        }),
        InnerRelationPattern::UncorrelatedDerivedTable {
            identifier,
            subquery,
            is_consulted_view,
        } => Ok(InnerRelationPattern::UncorrelatedDerivedTable {
            identifier,
            subquery: Box::new(t.transform_relational_action(*subquery)?.into_inner()),
            is_consulted_view,
        }),
        InnerRelationPattern::CorrelatedScalarJoin {
            identifier,
            correlation_filters,
            deferred,
            subquery,
        } => Ok(InnerRelationPattern::CorrelatedScalarJoin {
            identifier,
            correlation_filters: correlation_filters
                .into_iter()
                .map(|f| t.transform_boolean(f))
                .collect::<Result<Vec<_>>>()?,
            deferred: transform_deferred(t, deferred)?,
            subquery: Box::new(t.transform_relational_action(*subquery)?.into_inner()),
        }),
        InnerRelationPattern::CorrelatedGroupJoin {
            identifier,
            correlation_filters,
            aggregations,
            deferred,
            subquery,
        } => Ok(InnerRelationPattern::CorrelatedGroupJoin {
            identifier,
            correlation_filters: correlation_filters
                .into_iter()
                .map(|f| t.transform_boolean(f))
                .collect::<Result<Vec<_>>>()?,
            aggregations: aggregations
                .into_iter()
                .map(|e| t.transform_domain(e))
                .collect::<Result<Vec<_>>>()?,
            deferred: transform_deferred(t, deferred)?,
            subquery: Box::new(t.transform_relational_action(*subquery)?.into_inner()),
        }),
    }
}

// =============================================================================
// Walk functions — relational layer
// =============================================================================

pub fn walk_transform_relation<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    rel: Relation<P>,
) -> Result<Relation<Q>> {
    match rel {
        Relation::FunctorCall { call, alias } => Ok(Relation::FunctorCall {
            call: transform_sealed_call(t, call)?,
            alias: crate::pipeline::asts::core::phases::carry_stage_name::<P, Q>(alias)?,
        }),
        Relation::Ground { mention, outer } => Ok(Relation::Ground {
            mention: t.transform_mention(mention)?,
            outer,
        }),
        Relation::InnerRelation {
            pattern,
            alias,
            outer,
        } => Ok(Relation::InnerRelation {
            pattern: walk_transform_inner_relation(t, pattern)?,
            alias,
            outer,
        }),
        Relation::ConsultedView { body, outer } => Ok(Relation::ConsultedView {
            body: Box::new(t.transform_query(*body)?),
            outer,
        }),
    }
}

#[stacksafe::stacksafe]
pub fn walk_transform_relational<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    expr: Chain<P>,
) -> Result<Chain<Q>> {
    expr.folded(t)
}

// =============================================================================
// Walk functions — top-level
// =============================================================================

pub fn walk_transform_query<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    query: Query<P>,
) -> Result<Query<Q>> {
    let Query { locals, body } = query;
    Ok(Query::binding(
        // THE BLOCK CROSSES WHOLE. Whether the claims and the definitions
        // survive is the phases' decision, not this walker's, and the
        // walker is never handed a ledger to pair with bindings.
        locals.crossed(t)?,
        t.transform_relational_action(body)?.into_inner(),
    ))
}

// =============================================================================
// Helpers
// =============================================================================

/// Transform CteRequirements from phase P to phase Q.
pub fn transform_cte_requirements<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    reqs: CteRequirements<P>,
) -> Result<CteRequirements<Q>> {
    Ok(CteRequirements {
        needs_cte: reqs.needs_cte,
        accumulated_grouping_keys: reqs
            .accumulated_grouping_keys
            .into_iter()
            .map(|(name, expr)| Ok((name, t.transform_domain(expr)?)))
            .collect::<Result<Vec<_>>>()?,
        join_keys: reqs
            .join_keys
            .into_iter()
            .map(|e| t.transform_domain(e))
            .collect::<Result<Vec<_>>>()?,
        location: reqs.location,
        nested_members_info: reqs.nested_members_info,
    })
}

pub fn transform_reduction_plan<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    plan: ReductionPlan<P>,
) -> Result<ReductionPlan<Q>> {
    Ok(ReductionPlan {
        tree_groups: plan
            .tree_groups
            .into_iter()
            .map(|group| {
                Ok(TreeGroupPlan {
                    location: group.location,
                    item_index: group.item_index,
                    requirements: transform_cte_requirements(t, group.requirements)?,
                })
            })
            .collect::<Result<Vec<_>>>()?,
    })
}

/// A spread across a phase change. The authored witness goes through the
/// door that refuses: a phase that has SPENT its enumerations refuses to
/// receive one, because a fold still carrying a spread walked past the
/// container that expands it.
pub fn walk_transform_spread<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    spread: Spread<P>,
) -> Result<Spread<Q>> {
    match spread {
        Spread::Glob(glob) => Ok(Spread::Glob(Glob {
            qualifier: glob.qualifier,
            namespace_path: glob.namespace_path,
            authored: Q::admit_enumeration()?,
        })),
        Spread::Regex(regex) => Ok(Spread::Regex(RegexSelector {
            pattern: regex.pattern,
            authored: Q::admit_enumeration()?,
        })),
        Spread::PositionalSpan(range) => Ok(Spread::PositionalSpan(t.fold_column_range(range)?)),
    }
}

/// One enumerated addressing item across a phase change.
pub fn walk_transform_selector_item<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    item: SelectorItem<P>,
) -> Result<SelectorItem<Q>> {
    match item {
        SelectorItem::Reference(reference) => {
            Ok(SelectorItem::Reference(t.transform_reference(reference)?))
        }
        SelectorItem::Spread(spread) => Ok(SelectorItem::Spread(walk_transform_spread(t, spread)?)),
    }
}

/// A rename's source across a phase change.
pub fn walk_transform_rename_source<P: Phase, Q: Phase, F: AstTransform<P, Q> + ?Sized>(
    t: &mut F,
    source: RenameSource<P>,
) -> Result<RenameSource<Q>> {
    match source {
        RenameSource::Reference(reference) => {
            Ok(RenameSource::Reference(t.transform_reference(reference)?))
        }
        RenameSource::Regex(regex) => Ok(RenameSource::Regex(RegexSelector {
            pattern: regex.pattern,
            authored: Q::admit_enumeration()?,
        })),
        RenameSource::Glob(glob) => Ok(RenameSource::Glob(Glob {
            qualifier: glob.qualifier,
            namespace_path: glob.namespace_path,
            authored: Q::admit_enumeration()?,
        })),
    }
}
