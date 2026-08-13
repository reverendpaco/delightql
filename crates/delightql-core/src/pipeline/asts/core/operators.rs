// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Unary operators and pipe operations

use super::{
    DomainExpression, GroupSpec, NamedOutItem, OutItem, Phase, RenameSpec, TruthExpression,
    Unresolved,
};
use crate::{lispy::ToLispy, ToLispy};

/// A call's argument group, by the stratum the grammar gives it.
///
/// THE STRATA ARE DISTINCT BY TYPE: a scalar application's argument row
/// (`f:(…)`) and a functor's first-parens group (`g(…)(…)`) admit different
/// members, so a consumer of one cannot be handed the other. `None` is a
/// functor form that wrote no group — an empty `ho_part` is unspellable, so
/// the higher-order stratum is nonempty by construction.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum CallArguments<P: Phase = Unresolved> {
    #[lispy("call_arguments:none")]
    None,
    #[lispy("call_arguments:scalar")]
    Scalar(Vec<ScalarArgument<P>>),
    #[lispy("call_arguments:higher_order")]
    HigherOrder(HoPart<P>),
}

/// The nonempty first-parens group of a higher-order call.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("ho_part")]
pub struct HoPart<P: Phase = Unresolved> {
    pub members: Box<crate::pipeline::asts::vocabulary::Vec1<HoArgument<P>>>,
    /// THE LANDING, until the resolver judges it: the member index a piped
    /// relation was substituted at — the first formal, or the one authored
    /// `@`. One slot, so a second source is unrepresentable rather than
    /// counted.
    pub landing: P::HoLanding,
}

/// One member of a higher-order argument group: a relation, or the value an
/// identifier or ground supplies. Which relations bind relation formals and
/// which stand in lifted scalar slots is the callee descriptor's decision at
/// resolution, never a mark on the argument.
///
/// THE POSITION IS THE FORMAL. A pipe substitutes its relation into the
/// formal that receives it — the landing hole's own index, or the formal the
/// callee's category appoints when no hole is written — so a direct call and
/// a piped one are indistinguishable here, and no argument carries a source
/// role for anything to count.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum HoArgument<P: Phase = Unresolved> {
    #[lispy("ho_argument:relation")]
    Relation(super::Chain<P>),
    #[lispy("ho_argument:value")]
    Value(super::expressions::ArgumentValue<P>),
    /// `@` — the formal that receives the piped relation. Structural
    /// argument-row information, never a value: the invocation that reads it
    /// substitutes its relation there, and the payload is uninhabited after
    /// resolution, so an unspent landing cannot survive into a closed query.
    #[lispy("ho_argument:landing")]
    Landing(P::Placeholder),
    /// `_` — a disregarded argument position. The callee's descriptor judges
    /// what the position means at resolution; the mark itself binds nothing
    /// and computes nothing.
    #[lispy("ho_argument:skip")]
    Skip,
}

/// One member of a scalar application's argument row.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum ScalarArgument<P: Phase = Unresolved> {
    /// The argument's VALUE: a domain expression, or the licensed truth
    /// crossing. DISTINCT is argument data and lives on the value, so
    /// `%expr` cannot be manufactured outside an argument row.
    #[lispy("scalar_argument:value")]
    Value(super::expressions::ArgumentValue<P>),
    /// A CALLABLE HANDED TO A FORMAL. `f:(:(@ * 2), x)` supplies a form
    /// with an open slot, and that slot is the CALLEE's to supply where the
    /// body applies it. An outer landing walking this call reaches the
    /// argument and stops, because the slot beneath belongs to the callable.
    #[lispy("scalar_argument:callable")]
    Callable(super::expressions::Callable<P>),
    /// An AUTHORED enumeration: `f:(t.*)`, `f:(/re/)`. It stands for the
    /// several values it covers, so it is not one scalar and cannot be
    /// mistaken for one. Resolution spends it — the payload is uninhabited
    /// afterwards.
    #[lispy("scalar_argument:spread")]
    Spread(super::expressions::Spread<P>),
    /// `count:(*)` — the whole operand, named rather than addressed.
    #[lispy("scalar_argument:star")]
    Star,
    /// `..` — the context calling mode of a context-aware definition. An
    /// argument-row position, never a value: instantiation consumes it, and
    /// the payload is uninhabited after resolution, so a marker cannot be
    /// manufactured outside an argument row or survive into a closed query.
    #[lispy("scalar_argument:context")]
    Context(P::ContextMarker),
}

impl<P: Phase> HoArgument<P> {
    /// The DOMAIN value this argument supplies. A relation, a crossed
    /// truth, or a structural mark answers `None`.
    pub fn scalar_domain(&self) -> Option<&super::DomainExpression<P>> {
        match self {
            Self::Value(value) => value.domain(),
            Self::Relation(_) | Self::Landing(_) | Self::Skip => None,
        }
    }

    pub fn scalar_domain_mut(&mut self) -> Option<&mut super::DomainExpression<P>> {
        match self {
            Self::Value(value) => value.domain_mut(),
            Self::Relation(_) | Self::Landing(_) | Self::Skip => None,
        }
    }

    pub fn relation(&self) -> Option<&super::Chain<P>> {
        match self {
            Self::Relation(relation) => Some(relation),
            Self::Value(_) | Self::Landing(_) | Self::Skip => None,
        }
    }

    pub fn relation_mut(&mut self) -> Option<&mut super::Chain<P>> {
        match self {
            Self::Relation(relation) => Some(relation),
            Self::Value(_) | Self::Landing(_) | Self::Skip => None,
        }
    }

    pub fn into_relation(self) -> Option<super::Chain<P>> {
        match self {
            Self::Relation(relation) => Some(relation),
            Self::Value(_) | Self::Landing(_) | Self::Skip => None,
        }
    }
}

impl<P: Phase> ScalarArgument<P> {
    /// An undecorated value argument — the ordinary road for a compiler-built
    /// or normalized scalar.
    pub fn plain(value: super::DomainExpression<P>) -> Self {
        Self::Value(super::expressions::ArgumentValue::plain(value))
    }

    /// The DOMAIN value this argument supplies. A crossed truth, callable,
    /// spread, star or context marker answers `None`.
    pub fn scalar_domain(&self) -> Option<&super::DomainExpression<P>> {
        match self {
            Self::Value(value) => value.domain(),
            Self::Callable(_) | Self::Spread(_) | Self::Star | Self::Context(_) => None,
        }
    }

    pub fn scalar_domain_mut(&mut self) -> Option<&mut super::DomainExpression<P>> {
        match self {
            Self::Value(value) => value.domain_mut(),
            Self::Callable(_) | Self::Spread(_) | Self::Star | Self::Context(_) => None,
        }
    }
}

impl<P: Phase> CallArguments<P> {
    /// A higher-order group from the members a builder collected: nonempty
    /// members make the group, and none make `None` — an empty `ho_part` is
    /// unspellable, so nothing can construct one here either.
    pub fn higher_order(members: Vec<HoArgument<P>>) -> Self {
        match crate::pipeline::asts::vocabulary::Vec1::try_from_vec(members) {
            Some(part) => Self::HigherOrder(HoPart {
                members: Box::new(part),
                landing: P::HoLanding::default(),
            }),
            None => Self::None,
        }
    }

    /// Whether the call was handed no argument at all. A higher-order
    /// group is nonempty by construction, so only `None` and an empty
    /// scalar row answer yes.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::None => true,
            Self::Scalar(members) => members.is_empty(),
            Self::HigherOrder(_) => false,
        }
    }

    /// The higher-order group, when the call has one.
    pub fn ho(&self) -> Option<&HoPart<P>> {
        match self {
            Self::HigherOrder(part) => Some(part),
            Self::None | Self::Scalar(_) => None,
        }
    }

    pub fn ho_mut(&mut self) -> Option<&mut HoPart<P>> {
        match self {
            Self::HigherOrder(part) => Some(part),
            Self::None | Self::Scalar(_) => None,
        }
    }

    /// The higher-order stratum's members, in argument order; empty when the
    /// call has no such group.
    pub fn ho_members(&self) -> impl Iterator<Item = &HoArgument<P>> {
        self.ho().into_iter().flat_map(|part| part.members.iter())
    }

    pub fn ho_members_mut(&mut self) -> impl Iterator<Item = &mut HoArgument<P>> {
        self.ho_mut()
            .into_iter()
            .flat_map(|part| part.members.iter_mut())
    }

    /// The relations the higher-order group supplies, in argument order.
    pub fn relations(&self) -> impl Iterator<Item = &super::Chain<P>> {
        self.ho_members().filter_map(HoArgument::relation)
    }

    /// Every VALUE the argument group supplies, in order: the scalar row's
    /// domain values, or the higher-order group's identifier and ground
    /// values. Relations, crossings, callables and enumerations are not
    /// values and do not appear.
    pub fn value_domains(&self) -> impl Iterator<Item = &super::DomainExpression<P>> {
        self.ho_members()
            .filter_map(HoArgument::scalar_domain)
            .chain(
                self.scalar_members()
                    .iter()
                    .filter_map(ScalarArgument::scalar_domain),
            )
    }

    /// The scalar stratum's members; empty when the call is not a scalar
    /// application.
    pub fn scalar_members(&self) -> &[ScalarArgument<P>] {
        match self {
            Self::Scalar(members) => members.as_slice(),
            Self::None | Self::HigherOrder(_) => &[],
        }
    }
}

/// Window frame specification for window functions
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("window_frame")]
pub struct WindowFrame<P: Phase = Unresolved> {
    pub mode: FrameMode,
    pub start: FrameBound<P>,
    pub end: FrameBound<P>,
}

/// Frame mode for window functions
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum FrameMode {
    #[lispy("frame_mode:groups")]
    Groups,
    #[lispy("frame_mode:rows")]
    Rows,
    #[lispy("frame_mode:range")]
    Range,
}

/// Frame bound for window functions
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum FrameBound<P: Phase = Unresolved> {
    #[lispy("frame_bound:unbounded")]
    Unbounded,
    #[lispy("frame_bound:current_row")]
    CurrentRow,
    #[lispy("frame_bound:preceding")]
    Preceding(Box<DomainExpression<P>>),
    #[lispy("frame_bound:following")]
    Following(Box<DomainExpression<P>>),
}

/// Column alias for embed map cover operations
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum ColumnAlias {
    /// Literal alias: "foo"
    #[lispy("column_alias:literal")]
    Literal(String),
    /// Template with @ placeholder: "{@}_suffix"
    #[lispy("column_alias:template")]
    Template(ColumnNameTemplate),
}

/// Column name template containing @ placeholders
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub struct ColumnNameTemplate {
    /// Template string containing {@} placeholders
    pub template: String,
}

// Re-cored from refined.rs
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum JoinType {
    /// Regular inner join (comma without markers)
    Inner,
    /// Left outer join (? on right table)
    LeftOuter,
    /// Right outer join (? on left table)
    RightOuter,
    /// Full outer join (? on both tables)
    FullOuter,
}

/// THE SEMANTIC PIPE-OPERATOR PRODUCTION: what an anonymous `|>` step can
/// be. The normalizer knows which spelling it read and constructs that
/// exact member; no classifier recovers one from another later. Chain
/// structure — ordering, bounds, access, witnesses, drills, narrowing,
/// stage naming — is `Continuation`'s, not a member here.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum PipeOp<P: Phase = Unresolved> {
    /// `( … )` — projection: the heading is what the items publish, and
    /// nothing else.
    #[lispy("pipe_op:project")]
    Project(crate::pipeline::asts::vocabulary::Vec1<OutItem<P>>),
    /// `+( … )` — embed: EXTENSION rather than replacement. The items are
    /// the ADDED columns only; the operand's whole heading rides in front
    /// of them, supplied by the one shared projection algorithm rather
    /// than a synthesized leading glob a consumer could mistake for
    /// authored.
    #[lispy("pipe_op:embed")]
    Embed(crate::pipeline::asts::vocabulary::Vec1<OutItem<P>>),
    /// `%( … )` — group: distinct or reduce, the spec says which.
    #[lispy("pipe_op:group")]
    Group(GroupSpec<P>),
    /// `$(f:(...))(...)` — map cover.
    #[lispy("pipe_op:map_cover")]
    MapCover(MapCover<P>),
    /// `-( … )` — project out. The payload admits what the GRAMMAR admits
    /// (selector items, including regex and glob spreads) while SEMANTICS
    /// writes `'-(' reference+ ')'`; which one governs is the owner's
    /// pending call, so the wider carrier is kept and no site narrows it.
    #[lispy("pipe_op:project_out")]
    ProjectOut(Vec<super::expressions::SelectorItem<P>>),
    /// `*( … )` — rename cover.
    #[lispy("pipe_op:rename")]
    Rename(crate::pipeline::asts::vocabulary::Vec1<RenameSpec<P>>),
    /// `$$( … )` — transform: many-to-many column redefinition.
    #[lispy("pipe_op:transform")]
    Transform {
        /// Every item names the slot it writes, by type. The grammar
        /// refuses `$$()`, and the carrier says it too.
        items: crate::pipeline::asts::vocabulary::Vec1<NamedOutItem<P>>,
        guard: Option<Box<TruthExpression<P>>>,
    },
    /// `+$(f)(...)` — combined embed + map cover.
    #[lispy("pipe_op:embed_map_cover")]
    EmbedMapCover(EmbedMapCover<P>),
}

/// The map cover's payload: the callable as AUTHORED, the columns it
/// covers, the guard that conditions the cover — and, once resolution has
/// applied the callable per covered cell, the applied cells themselves.
/// The callable is spent by that application, so a bound cover carries
/// cells and no callable at all.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("map_cover")]
pub struct MapCover<P: Phase = Unresolved> {
    pub callable: P::CoverCallable,
    pub selector: Vec<super::expressions::SelectorItem<P>>,
    pub guard: Option<Box<TruthExpression<P>>>,
    /// One applied expression per covered cell — empty before resolution.
    pub cells: Vec<AppliedCell<P>>,
}

/// The embed-map-cover's payload: the callable as AUTHORED, the naming
/// template for the added columns, and the columns it covers. As with the
/// map cover, resolution applies the callable per cell and spends it.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("embed_map_cover")]
pub struct EmbedMapCover<P: Phase = Unresolved> {
    pub callable: P::CoverCallable,
    pub naming: Option<ColumnAlias>,
    pub selector: Vec<super::expressions::SelectorItem<P>>,
    /// One applied expression per covered cell — empty before resolution.
    pub cells: Vec<AppliedCell<P>>,
}

/// ONE COVERED CELL, applied: the occurrence the cover selected and the
/// closed expression the application produced for it. The open leaf was
/// spent producing `expr`, which is why this carrier exists — a closed
/// phase holds the application's RESULT, never its unapplied body.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("applied_cell")]
pub struct AppliedCell<P: Phase = Unresolved> {
    pub column: crate::names::ColId,
    pub expr: DomainExpression<P>,
}

#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("resolved_interior_grounding")]
pub struct ResolvedInteriorGrounding {
    pub column: crate::names::ColId,
    pub value: String,
}

/// The drill as AUTHORED: the interior column and selections by name,
/// groundings as literal pairs.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("authored_drill")]
pub struct AuthoredDrill {
    pub column: String,
    pub glob: bool,
    pub columns: Vec<String>,
    pub groundings: Vec<(String, String)>,
}

/// The drill, BOUND: the interior column and selections as occurrences.
/// The glob is spent at binding — what remains is what it selected.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("bound_drill")]
pub struct BoundDrill {
    pub column: crate::names::ColId,
    pub columns: Vec<crate::names::ColId>,
    pub groundings: Vec<ResolvedInteriorGrounding>,
}
