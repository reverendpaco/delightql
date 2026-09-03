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
///
/// THE ROW IS THE WHOLE OF IT. A landed relation is a MEMBER — see
/// [`HoArgument::Landed`] — so there is no index beside the row to keep in
/// step with it: nothing can reorder the members and leave a landing
/// pointing elsewhere, retain a landing whose member is no longer a
/// relation, or hold a landing at a position that no longer exists. The
/// members are private for the same reason the index is absent: the only
/// operations offered preserve each member's KIND, so no intervening phase
/// can turn a landed relation into an ordinary argument and silently demote
/// a piped call to the direct-call road.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("ho_part")]
pub struct HoPart<P: Phase = Unresolved> {
    members: Box<crate::pipeline::asts::vocabulary::Vec1<HoArgument<P>>>,
}

impl<P: Phase> HoPart<P> {
    /// The one road from collected members to a group.
    pub fn of(members: crate::pipeline::asts::vocabulary::Vec1<HoArgument<P>>) -> Self {
        Self {
            members: Box::new(members),
        }
    }

    pub fn members(&self) -> &crate::pipeline::asts::vocabulary::Vec1<HoArgument<P>> {
        &self.members
    }

    pub fn into_members(self) -> crate::pipeline::asts::vocabulary::Vec1<HoArgument<P>> {
        *self.members
    }
}

/// THE LANDED MEMBER: the relation a pipe put in a row, and the place it
/// occupies. One value, read off the row itself.
#[derive(Debug, Clone, Copy)]
pub struct Landed<'a, P: Phase> {
    /// The member's position, which IS the formal it faces.
    pub position: usize,
    pub relation: &'a super::Chain<P>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationProvenance {
    Authored,
    Landed,
}

#[derive(Debug, Clone, Copy)]
pub struct JudgedRelation<'a, P: Phase> {
    pub position: usize,
    pub relation: &'a super::Chain<P>,
    pub provenance: RelationProvenance,
}

/// The construction-owned exhaustive judgment over one higher-order row.
/// Every consumer receives the same answer about relational members and the
/// unique landed provenance; nobody scans for first/last matches itself.
pub struct JudgedCallArguments<'a, P: Phase> {
    relations: Vec<JudgedRelation<'a, P>>,
    landed: Option<Landed<'a, P>>,
}

impl<'a, P: Phase> JudgedCallArguments<'a, P> {
    pub fn relations(&self) -> &[JudgedRelation<'a, P>] {
        &self.relations
    }

    pub fn landed(&self) -> Option<Landed<'a, P>> {
        self.landed.as_ref().map(|landed| Landed {
            position: landed.position,
            relation: landed.relation,
        })
    }
}

/// One member of a higher-order argument group: a relation, or the value an
/// identifier or ground supplies. Which relations bind relation formals and
/// which stand in lifted scalar slots is the callee descriptor's decision at
/// resolution, never a mark on the argument.
///
/// THE POSITION IS THE FORMAL. A pipe substitutes its relation into the
/// formal that receives it — the landing hole's own place, or the row's
/// final place when no hole is written — so a direct call and a piped one
/// bind the same formals to the same values, and the landing is spent here:
/// nothing downstream re-decides WHERE a pipe went.
///
/// What a landed member still records is WHERE ITS RELATION CAME FROM, and
/// only that. The two provenances have always meant different resolutions —
/// a piped source resolves in the caller's world and rides its own carrier,
/// an authored actual is admitted closed — so this is the fact the resolver
/// already needed, held ON the member instead of in an index beside the row
/// that could drift out of step with it.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum HoArgument<P: Phase = Unresolved> {
    #[lispy("ho_argument:relation")]
    Relation(super::Chain<P>),
    /// A configured or unconfigured rule designator. Its distinct carrier
    /// keeps an incomplete application out of relation-valued positions;
    /// only a declared rule formal may construct or forward its residual.
    #[lispy("ho_argument:rule")]
    Rule(super::Chain<P>),
    /// THE RELATION A PIPE LANDED HERE. It is a relation like the authored
    /// one beside it and binds the same formal; what the kind records is
    /// WHERE IT CAME FROM, which is the one thing a later phase must not
    /// have to rediscover — a piped source resolves in the caller's world
    /// and rides its own carrier, an authored actual is admitted closed.
    /// The build is the only road that mints it, so the relation and its
    /// position cannot come apart.
    #[lispy("ho_argument:landed")]
    Landed(super::Chain<P>),
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
    /// The value this argument supplies. A relation or a structural mark
    /// answers `None`.
    pub fn scalar_domain(&self) -> Option<&super::DomainExpression<P>> {
        match self {
            Self::Value(value) => Some(&value.value),
            Self::Relation(_) | Self::Rule(_) | Self::Landed(_) | Self::Landing(_) | Self::Skip => {
                None
            }
        }
    }

    pub fn scalar_domain_mut(&mut self) -> Option<&mut super::DomainExpression<P>> {
        match self {
            Self::Value(value) => Some(&mut value.value),
            Self::Relation(_) | Self::Rule(_) | Self::Landed(_) | Self::Landing(_) | Self::Skip => {
                None
            }
        }
    }

    /// The relation this member supplies, WHEREVER it came from. A formal
    /// binds a relation the same way whether the author wrote it or a pipe
    /// landed it, so every reader asking "what relation is here" asks this.
    pub fn relation(&self) -> Option<&super::Chain<P>> {
        match self {
            Self::Relation(relation) | Self::Landed(relation) => Some(relation),
            Self::Rule(_) | Self::Value(_) | Self::Landing(_) | Self::Skip => None,
        }
    }

    /// The same relation, writable IN PLACE. The member's kind is not
    /// reachable through it: a rewrite may replace what a position carries,
    /// never what the position IS.
    pub fn relation_mut(&mut self) -> Option<&mut super::Chain<P>> {
        match self {
            Self::Relation(relation) | Self::Landed(relation) => Some(relation),
            Self::Rule(_) | Self::Value(_) | Self::Landing(_) | Self::Skip => None,
        }
    }

    pub fn rule(&self) -> Option<&super::Chain<P>> {
        match self {
            Self::Rule(rule) => Some(rule),
            Self::Relation(_)
            | Self::Landed(_)
            | Self::Value(_)
            | Self::Landing(_)
            | Self::Skip => None,
        }
    }
}

impl<P: Phase> ScalarArgument<P> {
    /// An undecorated value argument — the ordinary road for a compiler-built
    /// or normalized scalar.
    pub fn plain(value: super::DomainExpression<P>) -> Self {
        Self::Value(super::expressions::ArgumentValue::plain(value))
    }

    /// The value this argument supplies. A callable, spread, star or
    /// context marker answers `None`.
    pub fn scalar_domain(&self) -> Option<&super::DomainExpression<P>> {
        match self {
            Self::Value(value) => Some(&value.value),
            Self::Callable(_) | Self::Spread(_) | Self::Star | Self::Context(_) => None,
        }
    }

    pub fn scalar_domain_mut(&mut self) -> Option<&mut super::DomainExpression<P>> {
        match self {
            Self::Value(value) => Some(&mut value.value),
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
            Some(part) => Self::HigherOrder(HoPart::of(part)),
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

    /// The higher-order stratum's members, in argument order; empty when the
    /// call has no such group.
    pub fn ho_members(&self) -> impl Iterator<Item = &HoArgument<P>> {
        self.ho().into_iter().flat_map(|part| part.members().iter())
    }

    /// THE LANDED MEMBER, read off the row. `None` is a direct call — every
    /// member authored — and that is the only thing an absence can mean,
    /// because there is no second fact for this one to disagree with.
    ///
    /// The enumeration is EXHAUSTIVE, so a second landed member is caught
    /// rather than shadowed: the build lands exactly once (a second `@`
    /// refuses there), and a row carrying two has been damaged by something
    /// between. That fails closed here instead of demoting the call to the
    /// direct-call road, which is what silently reading only the first would
    /// do.
    pub fn judged(&self) -> crate::error::Result<JudgedCallArguments<'_, P>> {
        let mut found: Option<Landed<'_, P>> = None;
        let mut relations = Vec::new();
        for (position, member) in self.ho_members().enumerate() {
            match member {
                HoArgument::Relation(relation) => relations.push(JudgedRelation {
                    position,
                    relation,
                    provenance: RelationProvenance::Authored,
                }),
                HoArgument::Landed(relation) => {
                    if found.is_some() {
                        return Err(crate::error::DelightQLError::parse_error(
                            "a call carries two landed relations; one pipe lands once",
                        ));
                    }
                    found = Some(Landed { position, relation });
                    relations.push(JudgedRelation {
                        position,
                        relation,
                        provenance: RelationProvenance::Landed,
                    });
                }
                HoArgument::Rule(_)
                | HoArgument::Value(_)
                | HoArgument::Landing(_)
                | HoArgument::Skip => {}
            }
        }
        Ok(JudgedCallArguments {
            relations,
            landed: found,
        })
    }

    /// REWRITE WHAT EACH RELATION POSITION CARRIES, kind intact. This is the
    /// whole of the mutation an intervening phase is offered over the row:
    /// it can replace a relation with another relation, and it cannot turn a
    /// landed member into an authored one, an argument into a landing, or
    /// change how many members there are.
    pub fn rewrite_relations(
        &mut self,
        mut rewrite: impl FnMut(&super::Chain<P>) -> crate::error::Result<super::Chain<P>>,
    ) -> crate::error::Result<()> {
        if let Self::HigherOrder(part) = self {
            for member in part.members.iter_mut() {
                if let Some(relation) = member.relation_mut() {
                    *relation = rewrite(relation)?;
                }
            }
        }
        Ok(())
    }

    /// Put `relation` at the row's FIRST relation position, whatever kind
    /// that position is; a row with no relation position is left alone.
    pub fn replace_first_relation(&mut self, relation: super::Chain<P>) {
        if let Self::HigherOrder(part) = self {
            for member in part.members.iter_mut() {
                if let Some(carried) = member.relation_mut() {
                    *carried = relation;
                    return;
                }
            }
        }
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

    /// The same values, writable in place.
    pub fn value_domains_mut(&mut self) -> Vec<&mut super::DomainExpression<P>> {
        match self {
            Self::None => Vec::new(),
            Self::Scalar(members) => members
                .iter_mut()
                .filter_map(ScalarArgument::scalar_domain_mut)
                .collect(),
            Self::HigherOrder(part) => part
                .members
                .iter_mut()
                .filter_map(HoArgument::scalar_domain_mut)
                .collect(),
        }
    }

    /// The scalar stratum's members; empty when the call is not a scalar
    /// application.
    pub fn scalar_members(&self) -> &[ScalarArgument<P>] {
        match self {
            Self::Scalar(members) => members.as_slice(),
            Self::None | Self::HigherOrder(_) => &[],
        }
    }

    pub fn scalar_members_mut(&mut self) -> &mut [ScalarArgument<P>] {
        match self {
            Self::Scalar(members) => members.as_mut_slice(),
            Self::None | Self::HigherOrder(_) => &mut [],
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
    pub column: crate::relation::PortId,
    pub expr: DomainExpression<P>,
}

#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("resolved_interior_grounding")]
pub struct ResolvedInteriorGrounding {
    pub column: crate::relation::PortId,
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
    pub column: crate::relation::PortId,
    pub columns: Vec<crate::relation::PortId>,
    pub selection: crate::relation::form::DrillSelection,
    pub groundings: Vec<ResolvedInteriorGrounding>,
}

#[cfg(test)]
mod landed_member {
    use super::*;
    use crate::pipeline::asts::core::{Chain, GroundForm, Unresolved};

    fn relation() -> Chain<Unresolved> {
        Chain::authored(GroundForm::Reference(
            crate::pipeline::asts::core::Relation::Ground {
                mention: crate::pipeline::asts::core::GroundMention::named(
                    crate::pipeline::asts::core::QualifiedName {
                        namespace_path: crate::pipeline::asts::core::NamespacePath::empty(),
                        name: delightql_types::SqlIdentifier::new("t"),
                    },
                ),
                outer: false,
            },
        ))
    }

    /// A DIRECT CALL IS EVERY MEMBER AUTHORED, and an absent landing can
    /// mean nothing else — there is no second fact for it to disagree with.
    #[test]
    fn an_authored_row_has_no_landing() {
        let row: CallArguments<Unresolved> =
            CallArguments::higher_order(vec![HoArgument::Relation(relation())]);
        assert!(row
            .judged()
            .expect("a well-formed row answers")
            .landed()
            .is_none());
    }

    /// THE LANDED MEMBER CARRIES ITS OWN POSITION. Nothing states it
    /// separately, so the position an answer reports is the position the
    /// member is at, by construction.
    #[test]
    fn a_landed_member_is_its_own_position() {
        let row: CallArguments<Unresolved> = CallArguments::higher_order(vec![
            HoArgument::Value(super::super::expressions::ArgumentValue::plain(
                crate::pipeline::asts::core::DomainExpression::Application(
                    crate::pipeline::asts::core::FunctionApplication::Ground(
                        crate::pipeline::asts::core::LiteralValue::Number("1".into()),
                    ),
                ),
            )),
            HoArgument::Landed(relation()),
        ]);
        let landed = row
            .judged()
            .expect("a well-formed row answers")
            .landed()
            .expect("the row carries a landing");
        assert_eq!(landed.position, 1);
    }

    /// A ROW REORDERED AROUND THE LANDING KEEPS IT. The member moves with
    /// the row because it IS the landing; there is no index left behind
    /// pointing at what used to be there.
    #[test]
    fn reordering_the_row_moves_the_landing_with_it() {
        let members = vec![
            HoArgument::Landed(relation()),
            HoArgument::Relation(relation()),
        ];
        let mut reordered = members.clone();
        reordered.reverse();
        let before: CallArguments<Unresolved> = CallArguments::higher_order(members);
        let after: CallArguments<Unresolved> = CallArguments::higher_order(reordered);
        assert_eq!(before.judged().unwrap().landed().unwrap().position, 0);
        assert_eq!(after.judged().unwrap().landed().unwrap().position, 1);
    }

    /// REWRITING WHAT A POSITION CARRIES CANNOT CHANGE WHAT IT IS. This is
    /// the whole mutation an intervening phase is offered, so the landing
    /// survives every rewrite rather than depending on each one to preserve
    /// it.
    #[test]
    fn a_rewrite_cannot_unland_a_member() {
        let mut row: CallArguments<Unresolved> = CallArguments::higher_order(vec![
            HoArgument::Relation(relation()),
            HoArgument::Landed(relation()),
        ]);
        row.rewrite_relations(|_| Ok(relation()))
            .expect("the rewrite replaces relations");
        assert_eq!(row.judged().unwrap().landed().unwrap().position, 1);
        row.replace_first_relation(relation());
        assert_eq!(row.judged().unwrap().landed().unwrap().position, 1);
    }

    /// TWO LANDINGS FAIL CLOSED. The build lands once, so a row carrying two
    /// has been damaged; reading only the first would hand the call on with
    /// one relation silently reinterpreted as an authored argument.
    #[test]
    fn two_landed_members_refuse_rather_than_shadow() {
        let row: CallArguments<Unresolved> = CallArguments::higher_order(vec![
            HoArgument::Landed(relation()),
            HoArgument::Landed(relation()),
        ]);
        assert!(row.judged().is_err());
    }
}
