// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE TWO LEXICAL WORLDS.
//!
//! A compilation resolves names in exactly one of two owned environments:
//!
//! - the USE world — the query the author is compiling: its own CTEs, CFEs,
//!   structural carriers, aliases and plan-created relations, reaching the
//!   session scope (`home`, or the namespace a consulted goal is a form of);
//! - a BODY world — one opened definition: its own locals, the relation
//!   formals the caller resolved, the declaration reach captured from the
//!   statement's one catalog state, and the data world its declaring
//!   namespace publishes — the one an explicit `ground!` bound when that
//!   namespace is a derivative of a grounded world.
//!
//! The two are distinct owned types. Nothing converts one into the other,
//! nothing parents one on the other, and the body constructor takes no use
//! world: a body reads its declaration reach, its locals, its formals, and
//! its grounding, and there is no value in this module through which it can
//! read a caller's bindings or the caller's ordinary data world.

pub(crate) mod lookup;
pub(crate) mod reach;
pub(crate) mod scoped;

pub(in crate::defuse) use scoped::ChoeSpending;
pub(crate) use scoped::{ScopedCfe, ScopedEffectArms, ScopedHo, ScopedSlotWorld};

pub(crate) use lookup::RelationAnswer;
pub(crate) use reach::DeclarationReach;

use crate::diagnostic::{Cfe, DelightQLError, Ho, Resolution};
use std::collections::HashMap;

use crate::error::Result;

/// One lexical CTE registration. A recursive frontier cannot be registered
/// without the exact definition instance it serves; ordinary authored and
/// compiler CTEs carry no invented instance evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LocalCte {
    Ordinary(crate::relation::SemanticRelation),
    Frontier {
        relation: crate::relation::SemanticRelation,
        frontier: super::instance::DefinitionFrontier,
    },
}

impl LocalCte {
    pub(crate) fn relation(&self) -> crate::relation::SemanticRelation {
        match self {
            LocalCte::Ordinary(relation) | LocalCte::Frontier { relation, .. } => *relation,
        }
    }

    pub(crate) fn frontier(self) -> Option<super::instance::DefinitionFrontier> {
        match self {
            LocalCte::Ordinary(_) => None,
            LocalCte::Frontier { frontier, .. } => Some(frontier),
        }
    }
}

/// THE IDENTITY OF ONE LEXICAL QUERY FRAME — the block of query-local
/// claims one authored query owns. Minted only when a name fact enters a
/// world, so a frame nothing pushed cannot be named, and the number a live
/// stack happens to have reached is not a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QueryFrameId(u64);

impl QueryFrameId {
    /// The counter is PROCESS-WIDE, so no two worlds ever mint the same
    /// frame: a site that reached a world it was not minted in names a
    /// frame that world provably does not hold, rather than one that
    /// happens to carry the same ordinal. A frame identity never reaches
    /// emitted output, so sharing the counter costs no determinism there.
    fn mint() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        QueryFrameId(NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }
}

/// WHERE A QUERY-LOCAL DEFINITION WAS DECLARED: the lexical frame holding
/// its claim, and the horizon that claim's position minted.
///
/// The pair is stamped by the REGISTRATION that put the definition in the
/// world, and it is PRIVATE TO THIS MODULE. Nothing outside constructs one,
/// receives one, or holds one: a site travels only inside the selected
/// definition it belongs to, so there is no safe signature through which a
/// caller could pair one definition's declaration with another's body, and
/// no count of the frames a caller happens to have open can stand in for
/// the frame a declaration is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LexicalSite {
    frame: QueryFrameId,
    horizon: crate::pipeline::asts::core::LexicalHorizon,
}

/// One authored query's claims, under the identity the world minted for
/// them.
#[derive(Debug, Clone)]
struct QueryFrame {
    id: QueryFrameId,
    names: crate::pipeline::asts::core::QueryLocalNames,
    /// THE RELATION BINDINGS this frame's block declared, kept unresolved.
    /// A body made of many statements resolves them afresh in each — the
    /// world they read changes as the plan creates relations — so they stay
    /// with the frame that declared them rather than travelling on a walk.
    /// Empty for a frame whose one query resolved its own bindings.
    bindings: Vec<crate::pipeline::ast_unresolved::CteBinding>,
}

/// The manifestation returned by the one query-local selection operation.
pub(crate) enum QueryLocalSelection {
    Relation(LocalCte),
    Value(ScopedCfe),
    HigherOrder(ScopedHo),
    /// An effect-marked CTE label: every same-label arm the block declared,
    /// under the site that declared them.
    EffectRelation(ScopedEffectArms),
    /// The query's own effect-mirror CHOE, selected as the same carrier a
    /// pure one is: one law for both manifestations of a scoped definition.
    EffectHigherOrder(ScopedHo),
}

pub(crate) enum QueryLocalRegistration {
    Relation {
        name: delightql_types::SqlIdentifier,
        relation: crate::relation::SemanticRelation,
    },
    SyntheticRelation {
        name: delightql_types::SqlIdentifier,
        relation: crate::relation::SemanticRelation,
    },
    Value(crate::pipeline::asts::core::CfeDefinition),
    HigherOrder(crate::pipeline::asts::core::HoDefinition),
    /// One effect-marked CTE arm. The label denotes the corresponding union
    /// of every arm declared under it, so arms accumulate in authored order.
    EffectRelation {
        name: delightql_types::SqlIdentifier,
        arm: crate::pipeline::ast_unresolved::CteBinding,
    },
}

/// The bindings ONE lexical frame owns. Private storage shared by both
/// worlds; the worlds differ in what stands beneath these bindings, never in
/// how a binding is kept.
///
/// Every map is keyed by the authored spelling and compares by the
/// identifier law — `SqlIdentifier`'s equality folds an unstropped spelling
/// and keeps a stropped one verbatim.
#[derive(Debug, Default)]
struct Locals {
    ctes: HashMap<delightql_types::SqlIdentifier, LocalCte>,
    synthetic_ctes: HashMap<delightql_types::SqlIdentifier, LocalCte>,
    /// The carriers in view at this level, as the records that bound them
    /// hold them: a body addresses its formals through this, and a nested
    /// call inherits from it.
    carriers: crate::defuse::carriers::CarrierRecord,
    cfes: HashMap<delightql_types::SqlIdentifier, Vec<ScopedCfe>>,
    /// The query-scoped higher-order definitions — the CHOEs — by bare
    /// subject, pure and effect mirror alike.
    hos: HashMap<delightql_types::SqlIdentifier, ScopedHo>,
    /// The effect-marked CTE labels, each holding every arm declared under
    /// it in authored order, stamped with the site that declared them.
    effect_ctes: HashMap<delightql_types::SqlIdentifier, ScopedEffectArms>,
    /// Nested unresolved queries' common name facts, outermost first, each
    /// under the identity minted when it entered.
    query_names: Vec<QueryFrame>,
    /// THE OPEN DECLARATIONS, outermost first. Every query-scoped body
    /// resolving at its authored declaration site stands here — a CTE
    /// binding, a CFE body, a CHOE body alike — so the INNERMOST of them
    /// governs visibility, whichever kind it is. Two stacks could not say
    /// which of a CTE horizon and a CHOE body was entered last.
    declarations: Vec<ActiveDeclaration>,
    aliases: HashMap<delightql_types::SqlIdentifier, delightql_types::SqlIdentifier>,
    /// Relations earlier statements of the same plan created. Their heading
    /// is lexical knowledge, but the relation is a physical DML target
    /// rather than a SQL CTE; a same-name CTE still shadows one.
    materialized: HashMap<delightql_types::SqlIdentifier, crate::relation::SemanticRelation>,
    /// OPEN INSTANTIATION FRAMES, innermost last: a scoped definition or
    /// a curried closing resolving in THIS world answers its formals from
    /// the top frame. A body's own base formals live on the body itself.
    instantiations: Vec<Frame>,
}

/// One open instantiation frame: the formals a scoped definition spends,
/// and — for a QUERY-SCOPED BODY, a CHOE's — the body's own lexical scope
/// standing over the world it was declared in.
#[derive(Debug)]
struct Frame {
    formals: FormalBindings,
    scope: Option<BodyScope>,
}

/// THE SCOPE OF ONE OPENED CHOE BODY. The body is the query's own text,
/// so it resolves in the query's world — but through this frame: the
/// relation formals the caller resolved stand here as structural carriers,
/// every binding the body's own resolution registers (its clause CTEs, a
/// frontier, an alias) lands here and dies with the frame, and the world
/// beneath answers a query-local name only when the body's LEXICAL HORIZON
/// admits it — a binding declared after the CHOE is unknown inside it,
/// however live it is at the call site. Only the innermost such frame is
/// consulted: an enclosing CHOE's frame is another body's text.
#[derive(Debug)]
struct BodyScope {
    ctes: HashMap<delightql_types::SqlIdentifier, LocalCte>,
    synthetic_ctes: HashMap<delightql_types::SqlIdentifier, LocalCte>,
    cfes: HashMap<delightql_types::SqlIdentifier, Vec<ScopedCfe>>,
    hos: HashMap<delightql_types::SqlIdentifier, ScopedHo>,
    effect_ctes: HashMap<delightql_types::SqlIdentifier, ScopedEffectArms>,
    carriers: crate::defuse::carriers::CarrierRecord,
    aliases: HashMap<delightql_types::SqlIdentifier, delightql_types::SqlIdentifier>,
}

/// ONE OPEN QUERY-SCOPED BODY, standing at the site its declaration was
/// stamped with — a CTE binding, a CFE body, a pure CHOE body, an effect
/// CHOE body. There is no second state: every body opening, on every road,
/// names the declaration it reads from.
#[derive(Debug)]
struct ActiveDeclaration {
    site: LexicalSite,
    /// HOW MANY QUERY FRAMES STOOD WHEN THIS BODY OPENED. Everything below
    /// that mark was already standing — it is the CALLER'S text, whatever
    /// its depth — and everything at or above it this body pushed itself.
    /// The distinction cannot be read off a frame index alone: a body that
    /// opens on the world its demand stands in (an effect CHOE does) has
    /// the demand site's own frames sitting above its declaration's.
    opened_over: usize,
}

impl Locals {
    /// Snapshot this lexical store for a closed query-scoped rule value.
    /// The operation stays private to the environment authority: callers
    /// carry the resulting world whole and cannot pair its parts again.
    fn closed_copy(&self) -> Self {
        Locals {
            ctes: self.ctes.clone(),
            synthetic_ctes: self.synthetic_ctes.clone(),
            carriers: self.carriers.clone(),
            cfes: self.cfes.clone(),
            hos: self.hos.clone(),
            effect_ctes: self.effect_ctes.clone(),
            query_names: self.query_names.clone(),
            declarations: self
                .declarations
                .iter()
                .map(ActiveDeclaration::closed_copy)
                .collect(),
            aliases: self.aliases.clone(),
            materialized: self.materialized.clone(),
            instantiations: self.instantiations.iter().map(Frame::closed_copy).collect(),
        }
    }

    /// The innermost open CHOE body scope, if the world is currently
    /// resolving one.
    fn body_scope(&self) -> Option<&BodyScope> {
        self.instantiations
            .iter()
            .rev()
            .find_map(|frame| frame.scope.as_ref())
    }

    fn body_scope_mut(&mut self) -> Option<&mut BodyScope> {
        self.instantiations
            .iter_mut()
            .rev()
            .find_map(|frame| frame.scope.as_mut())
    }

    /// WHERE THE GOVERNING DECLARATION STANDS: the position, in this
    /// world's frame stack, of the frame the innermost open query-scoped
    /// body was DECLARED in.
    ///
    /// `Ok(None)` means no body is open and the whole stack is in view.
    /// A site whose frame has left the stack is not a visibility answer of
    /// any kind — it is a broken pairing, and it refuses rather than
    /// silently hiding or revealing names.
    fn governing(
        &self,
    ) -> Result<Option<(usize, crate::pipeline::asts::core::LexicalHorizon, usize)>> {
        let (site, opened_over) = match self.declarations.last() {
            None => return Ok(None),
            Some(declaration) => (declaration.site, declaration.opened_over),
        };
        let at = self
            .query_names
            .iter()
            .position(|frame| frame.id == site.frame)
            .ok_or_else(|| {
                crate::diagnostic::Internal::invariant(
                    "defuse::environment",
                    "an open query-scoped body names a lexical frame this world does not hold",
                )
            })?;
        Ok(Some((at, site.horizon, opened_over)))
    }

    /// The horizon governing one frame of this world's stack.
    ///
    /// ONE RULE for every query-scoped body, whatever declared it: the
    /// frame the open declaration stands in is admitted up to that
    /// declaration's horizon, every frame nested INSIDE it is admitted
    /// whole, and a frame the declaration itself is nested in is another
    /// query's text. Which frame that is comes from the declaration's own
    /// stamped site — never from how many frames happen to be open.
    fn horizon_for(
        &self,
        query_index: usize,
    ) -> Result<Option<crate::pipeline::asts::core::LexicalHorizon>> {
        let Some((at, horizon, opened_over)) = self.governing()? else {
            return Ok(Some(crate::pipeline::asts::core::LexicalHorizon::all()));
        };
        Ok(if query_index == at {
            Some(horizon)
        } else if query_index >= opened_over {
            // Pushed by this body's own resolution — its own nested text.
            Some(crate::pipeline::asts::core::LexicalHorizon::all())
        } else {
            // Standing before this body opened: another query's text.
            None
        })
    }

    /// The instantiation frames a formal lookup walks, innermost first,
    /// stopping AT the first CHOE body frame: the body's text has no
    /// enclosing instantiation to read formals from.
    fn formal_frames(&self) -> impl Iterator<Item = &FormalBindings> {
        let mut stopped = false;
        self.instantiations
            .iter()
            .rev()
            .take_while(move |frame| {
                if stopped {
                    return false;
                }
                stopped = frame.scope.is_some();
                true
            })
            .map(|frame| &frame.formals)
    }
}

impl Frame {
    fn closed_copy(&self) -> Self {
        Frame {
            formals: self.formals.closed_copy(),
            scope: self.scope.as_ref().map(BodyScope::closed_copy),
        }
    }
}

impl BodyScope {
    fn closed_copy(&self) -> Self {
        BodyScope {
            ctes: self.ctes.clone(),
            synthetic_ctes: self.synthetic_ctes.clone(),
            cfes: self.cfes.clone(),
            hos: self.hos.clone(),
            effect_ctes: self.effect_ctes.clone(),
            carriers: self.carriers.clone(),
            aliases: self.aliases.clone(),
        }
    }
}

impl ActiveDeclaration {
    fn closed_copy(&self) -> Self {
        ActiveDeclaration {
            site: self.site,
            opened_over: self.opened_over,
        }
    }
}

/// THE CALLER-RESOLVED BINDINGS a definition's formals spend. Keyed by the
/// identifier law (`SqlIdentifier` equality folds an unstropped spelling
/// and keeps a stropped one verbatim), held PRIVATELY by the world that
/// answers them, and never `Clone`: a body and its formals cannot be
/// paired independently, and no caller reads the maps back out.
#[derive(Debug, Default)]
pub(crate) struct FormalBindings {
    /// The frame's own identities: each declared formal spelling maps to
    /// the declared position it was issued at. Values and callables are
    /// keyed by that identity, never the spelling, and the three maps are
    /// sealed together from ONE inventory — no identity ever leaves a
    /// frame, so a binding from one frame can never answer a same-spelled
    /// formal of another.
    ids: HashMap<delightql_types::SqlIdentifier, FormalId>,
    values: HashMap<FormalId, crate::pipeline::asts::resolved::DomainExpression>,
    callables: HashMap<FormalId, crate::defuse::callable::CallableBinding>,
    rules: HashMap<FormalId, crate::defuse::ho::RuleValueId>,
}

/// A frame-local formal identity: the declared ordinal and role. Private
/// to this module and never exported, so two inventories' identities can
/// meet only if their frames are first decomposed, which the type forbids.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FormalId {
    ordinal: u32,
    role: FormalRole,
}

/// One DECLARED parameter's role in its family's formal inventory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(in crate::defuse) enum FormalRole {
    Value,
    Capture,
    Callable,
    Rule,
}

impl FormalRole {
    fn spelled(self) -> &'static str {
        match self {
            FormalRole::Value => "value",
            FormalRole::Capture => "capture",
            FormalRole::Callable => "code",
            FormalRole::Rule => "rule",
        }
    }
}

/// THE FAMILY'S FORMAL INVENTORY: one identity per DECLARED parameter,
/// issued over the declaration BEFORE any actual binds. `FormalId` has no
/// other constructor, so a `(FormalId, actual)` pair can only come from
/// an inventory operation over the declared parameters — a name supplied
/// by binding code is never the authority key, an undeclared spelling
/// REFUSES, and a positional binding whose count disagrees with the
/// declaration REFUSES rather than truncating.
pub(crate) struct FormalInventory {
    entries: Vec<(delightql_types::SqlIdentifier, FormalId)>,
    values: HashMap<FormalId, crate::pipeline::asts::resolved::DomainExpression>,
    callables: HashMap<FormalId, crate::defuse::callable::CallableBinding>,
    rules: HashMap<FormalId, crate::defuse::ho::RuleValueId>,
}

impl FormalInventory {
    pub(in crate::defuse) fn declared(
        declared: impl IntoIterator<Item = (delightql_types::SqlIdentifier, FormalRole)>,
    ) -> Self {
        let entries = declared
            .into_iter()
            .enumerate()
            .map(|(ordinal, (name, role))| {
                (
                    name,
                    FormalId {
                        ordinal: ordinal as u32,
                        role,
                    },
                )
            })
            .collect();
        FormalInventory {
            entries,
            values: HashMap::new(),
            callables: HashMap::new(),
            rules: HashMap::new(),
        }
    }

    fn ids_of(&self, role: FormalRole) -> Vec<FormalId> {
        self.entries
            .iter()
            .filter(|(_, id)| id.role == role)
            .map(|(_, id)| id.clone())
            .collect()
    }

    /// THE CARDINALITY LAW of a positional binding: one actual per
    /// declared parameter of the role, no more and no fewer. A mismatch is
    /// a categorized refusal BEFORE anything binds — never a silent `zip`.
    fn total(role: FormalRole, declared: usize, supplied: usize) -> Result<()> {
        if declared == supplied {
            return Ok(());
        }
        Err(DelightQLError::from(Cfe::Arity {
            message: format!(
                "the definition declares {declared} {} parameter{}; {supplied} actual{} \
                 supplied",
                role.spelled(),
                if declared == 1 { "" } else { "s" },
                if supplied == 1 { " was" } else { "s were" },
            ),
        }))
    }

    /// Bind ordered VALUE actuals to the declared parameters of one role,
    /// in declaration order. Total: the counts must agree.
    pub(in crate::defuse) fn bind_positional(
        &mut self,
        role: FormalRole,
        items: impl IntoIterator<Item = crate::pipeline::asts::resolved::DomainExpression>,
    ) -> Result<()> {
        let ids = self.ids_of(role);
        let items: Vec<_> = items.into_iter().collect();
        Self::total(role, ids.len(), items.len())?;
        for (id, value) in ids.into_iter().zip(items) {
            self.values.insert(id, value);
        }
        Ok(())
    }

    /// Bind ordered CODE actuals to the declared callable parameters.
    /// Total: the counts must agree.
    pub(in crate::defuse) fn bind_callables_positional(
        &mut self,
        code: impl IntoIterator<Item = crate::defuse::callable::CallableBinding>,
    ) -> Result<()> {
        let ids = self.ids_of(FormalRole::Callable);
        let code: Vec<_> = code.into_iter().collect();
        Self::total(FormalRole::Callable, ids.len(), code.len())?;
        for (id, binding) in ids.into_iter().zip(code) {
            self.callables.insert(id, binding);
        }
        Ok(())
    }

    /// Bind one closed residual to the rule formal the declaration issued.
    pub(in crate::defuse) fn bind_rule_named(
        &mut self,
        name: &delightql_types::SqlIdentifier,
        value: crate::defuse::ho::RuleValueId,
    ) -> crate::error::Result<()> {
        let Some((_, id)) = self
            .entries
            .iter()
            .find(|(declared, id)| declared == name && id.role == FormalRole::Rule)
        else {
            return Err(DelightQLError::from(Ho::RuleFormal {
                message: format!("'{name}' is not a declared rule-valued parameter"),
            }));
        };
        self.rules.insert(id.clone(), value);
        Ok(())
    }

    /// Bind one actual to a DECLARED name. A spelling the family never
    /// declared has no lawful binding and refuses.
    pub(in crate::defuse) fn bind_named(
        &mut self,
        name: &delightql_types::SqlIdentifier,
        value: crate::pipeline::asts::resolved::DomainExpression,
    ) -> crate::error::Result<()> {
        let Some((_, id)) = self
            .entries
            .iter()
            .find(|(declared, id)| declared == name && id.role != FormalRole::Callable)
        else {
            return Err(DelightQLError::from(Cfe::FormalsUndeclared {
                message: format!(
                    "'{}' is not a declared parameter of this definition — a \
                     binding must name a declared formal",
                    name
                ),
            }));
        };
        let id = id.clone();
        self.values.insert(id, value);
        Ok(())
    }

    /// SEAL by consuming the inventory: the actual maps it bound itself,
    /// keyed by its own pre-issued identities, beside its own name table.
    /// No identities ever cross an API, so a frame cannot be assembled
    /// from another inventory's bindings.
    pub(in crate::defuse) fn sealed(self) -> FormalBindings {
        FormalBindings {
            ids: self.entries.into_iter().collect(),
            values: self.values,
            callables: self.callables,
            rules: self.rules,
        }
    }
}

impl FormalBindings {
    /// Copy one sealed frame only while closing an entire lexical world.
    /// This stays private so bindings cannot detach from their world.
    fn closed_copy(&self) -> Self {
        FormalBindings {
            ids: self.ids.clone(),
            values: self.values.clone(),
            callables: self.callables.clone(),
            rules: self.rules.clone(),
        }
    }

    /// One value, read-only — the sealed pattern converter's door. The
    /// spelling maps through the ISSUED identity table.
    pub(crate) fn value(
        &self,
        name: &delightql_types::SqlIdentifier,
    ) -> Option<&crate::pipeline::asts::resolved::DomainExpression> {
        self.values.get(self.ids.get(name)?)
    }

    fn callable(
        &self,
        name: &delightql_types::SqlIdentifier,
    ) -> Option<&crate::defuse::callable::CallableBinding> {
        self.callables.get(self.ids.get(name)?)
    }

    fn rule(
        &self,
        name: &delightql_types::SqlIdentifier,
    ) -> Option<crate::defuse::ho::RuleValueId> {
        self.rules.get(self.ids.get(name)?).copied()
    }

    fn covers_value(&self, name: &delightql_types::SqlIdentifier) -> bool {
        self.ids
            .get(name)
            .is_some_and(|id| self.values.contains_key(id))
    }

    pub(in crate::defuse) fn is_empty(&self) -> bool {
        self.values.is_empty() && self.callables.is_empty() && self.rules.is_empty()
    }
}

/// Which data world a world's free data names address. There is no caller,
/// provider, or session arm: a hole is unbound, bound by an explicit
/// `ground!` publication, or answered by a scratch namespace's own ambient
/// data world.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DataBinding {
    Ungrounded,
    /// Bound by an explicit `ground!` publication: the world's namespace is
    /// a derivative of a grounded world. The grounding derived every
    /// lexical dependency it reaches into a derivative bound the same way,
    /// so a body opened from here reaches derivatives that publish this
    /// binding themselves — no opener hands a data world down.
    Grounded {
        data_ns: String,
    },
    /// A scratch namespace's ambient data world. It answers this world's
    /// own holes and derives nothing: a definition opened from an inline
    /// block keeps its own world.
    Ambient {
        data_ns: String,
    },
}

impl DataBinding {
    /// The binding a namespace's OWN publication supplies.
    fn of_reach(reach: &DeclarationReach) -> Self {
        match reach.root_default_data_ns() {
            Some(data_ns) if reach.root_kind() == "grounded" => DataBinding::Grounded {
                data_ns: data_ns.to_string(),
            },
            Some(data_ns) => DataBinding::Ambient {
                data_ns: data_ns.to_string(),
            },
            None => DataBinding::Ungrounded,
        }
    }

    /// The data world this binding answers holes from, if any.
    pub(crate) fn data_ns(&self) -> Option<&str> {
        match self {
            DataBinding::Ungrounded => None,
            DataBinding::Grounded { data_ns } | DataBinding::Ambient { data_ns } => Some(data_ns),
        }
    }
}

/// The caller's world: the query being compiled and the session scope it
/// stands in. Owned and never cloned; a body cannot be built from it.
#[derive(Debug)]
pub(crate) struct UseEnvironment {
    locals: Locals,
    reach: DeclarationReach,
    data: DataBinding,
}

impl UseEnvironment {
    /// The session's use world, rooted at `scope_fq` — `home` at the
    /// prompt, or the namespace a consulted goal is a form of.
    /// A demanded program's own use world CARRYING ITS FORMALS: the frame
    /// is part of construction, never attached to a live world.
    pub(in crate::defuse) fn session_with_formals(
        consult: &crate::resolution::ConsultRegistry,
        scope_fq: &str,
        formals: FormalBindings,
    ) -> Result<Self> {
        let mut world = Self::session(consult, scope_fq)?;
        if !formals.is_empty() {
            world.locals.instantiations.push(Frame {
                formals,
                scope: None,
            });
        }
        Ok(world)
    }

    pub(crate) fn session(
        consult: &crate::resolution::ConsultRegistry,
        scope_fq: &str,
    ) -> Result<Self> {
        let reach = match consult.catalog() {
            #[cfg(not(target_arch = "wasm32"))]
            Some(catalog) => reach::capture(catalog, scope_fq, reach::World::Session)?,
            _ => DeclarationReach::empty(scope_fq),
        };
        let data = DataBinding::of_reach(&reach);
        Ok(UseEnvironment {
            locals: Locals::default(),
            reach,
            data,
        })
    }

    /// A use world for a registry built without a system (tests, the DDL
    /// manifest road): it reaches nothing beyond its own locals and the
    /// database catalog.
    pub(crate) fn detached() -> Self {
        UseEnvironment {
            locals: Locals::default(),
            reach: DeclarationReach::empty("home"),
            data: DataBinding::Ungrounded,
        }
    }

    /// Register a relation an earlier statement of the same plan created.
    /// A USE (program) world only, BY TYPE: a plan creation is the
    /// program's own state, and a consulted body reads it only through an
    /// explicit actual — never ambiently.
    pub(crate) fn register_materialized(
        &mut self,
        name: delightql_types::SqlIdentifier,
        relation: crate::relation::SemanticRelation,
    ) {
        self.locals.materialized.insert(name, relation);
    }
}

/// The declaration a body opens under: the declaring namespace and the
/// reach captured for it — its own declared enlistments, aliases, and
/// exposures, as the statement's catalog state holds them.
#[derive(Debug, Clone)]
pub(crate) struct DeclarationEnvironment {
    namespace: String,
    reach: DeclarationReach,
}

impl DeclarationEnvironment {
    /// The environment of a SELECTED family: its declaring namespace's own
    /// world, captured under THE READ THAT SELECTED THE FAMILY — no other
    /// registry or read can be supplied beside it.
    pub(in crate::defuse) fn of_family(family: &super::select::LinkedFamily<'_>) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        let reach = reach::capture(
            family.catalog(),
            family.namespace(),
            reach::World::Declaration,
        )?;
        #[cfg(target_arch = "wasm32")]
        let reach = DeclarationReach::empty(family.namespace());
        Ok(DeclarationEnvironment {
            namespace: family.namespace().to_string(),
            reach,
        })
    }

    /// The environment of a namespace for a COMPILER-SYNTHESIZED body (the
    /// liminal wrapper): synthesizing a body is not using a definition, so
    /// the namespace is reached as the session reaches it.
    pub(in crate::defuse) fn of_namespace(
        consult: &crate::resolution::ConsultRegistry,
        namespace: &str,
    ) -> Result<Self> {
        let reach = match consult.catalog() {
            #[cfg(not(target_arch = "wasm32"))]
            Some(catalog) => reach::capture(catalog, namespace, reach::World::Session)?,
            _ => DeclarationReach::empty(namespace),
        };
        Ok(DeclarationEnvironment {
            namespace: namespace.to_string(),
            reach,
        })
    }

    /// The declaring namespace, for diagnostics.
    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The reach the declaration's names are selected under.
    pub(in crate::defuse) fn reach(&self) -> &DeclarationReach {
        &self.reach
    }
}

/// The enclosing row a context-aware body (`..`) declared it reads. The
/// ROW — the caller's lexical position, borrowed — and nothing else: no
/// CTE, CFE, alias, or plan relation of the caller rides with it, and no
/// copy of what the caller can address is made.
#[derive(Clone, Copy)]
pub(crate) struct EnclosingRow<'p>(pub(crate) &'p crate::pipeline::resolver::Position<'p>);

/// One opened body's world. Constructed only inside the definition-use
/// authority, from a declaration environment and the caller-resolved
/// relation formals — never from a use world.
#[derive(Debug)]
pub(crate) struct BodyEnvironment {
    locals: Locals,
    declaration: DeclarationEnvironment,
    /// The caller-resolved scalar and callable actuals this body's formal
    /// references spend — owned HERE, born at `open`, never paired from
    /// outside.
    formals: FormalBindings,
    data: DataBinding,
}

impl BodyEnvironment {
    /// THE ONE BODY CONSTRUCTOR. The relation formals arrive as resolved
    /// carriers under their opaque structural identities, and the
    /// declaring namespace's OWN publication decides the data binding: a
    /// derivative of a grounded world is bound to that grounding's data
    /// world by its namespace row, and every dependency a grounded body
    /// reaches is a derivative that publishes the same — so nothing here
    /// takes a data world from the opener, and a caller's ambient world can
    /// never reach a hole.
    ///
    /// The opening LICENSE is minted only by the admitted authority's own
    /// world derivation, so the declaration, relation formals, and scalar
    /// formals that meet here were derived from one admitted use — no other
    /// module can assemble a body world from independently supplied parts.
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::defuse) fn open(
        _license: super::admitted::BodyOpening,
        declaration: DeclarationEnvironment,
        carriers: &crate::defuse::carriers::CarrierRecord,
        formals: FormalBindings,
    ) -> Self {
        let data = DataBinding::of_reach(&declaration.reach);
        let mut locals = Locals::default();
        locals.carriers = carriers.formals_only();
        BodyEnvironment {
            locals,
            declaration,
            formals,
            data,
        }
    }
}

/// The one carrier the resolver holds. Every lexical registration and
/// lookup goes through it; which world answers is the variant's, and a
/// value of one variant contains nothing of the other.
#[derive(Debug)]
pub(crate) enum Environment {
    Use(UseEnvironment),
    Body(BodyEnvironment),
}

/// The declaration and lexical world captured by one query-scoped residual.
/// Its variants and fields are private: the selected CHOE retains this
/// carrier whole, while no caller can replace its locals, formals, reach, or
/// data binding independently.
#[derive(Debug)]
pub(crate) struct ClosedLexicalWorld {
    world: ClosedWorld,
}

#[derive(Debug)]
enum ClosedWorld {
    Use(UseEnvironment),
    Body(BodyEnvironment),
}

impl Clone for ClosedLexicalWorld {
    fn clone(&self) -> Self {
        let world = match &self.world {
            ClosedWorld::Use(world) => ClosedWorld::Use(UseEnvironment {
                locals: world.locals.closed_copy(),
                reach: world.reach.clone(),
                data: world.data.clone(),
            }),
            ClosedWorld::Body(world) => ClosedWorld::Body(BodyEnvironment {
                locals: world.locals.closed_copy(),
                declaration: world.declaration.clone(),
                formals: world.formals.closed_copy(),
                data: world.data.clone(),
            }),
        };
        ClosedLexicalWorld { world }
    }
}

impl ClosedLexicalWorld {
    /// Reconstitute the captured world for one affine body opening. Reusing
    /// a value clones this closed carrier as a unit.
    pub(in crate::defuse) fn open(self) -> Environment {
        match self.world {
            ClosedWorld::Use(world) => Environment::Use(world),
            ClosedWorld::Body(world) => Environment::Body(world),
        }
    }
}

impl Environment {
    /// Close the lexical world at query-scoped rule-value construction.
    /// Selection and capture occur in the same definition-use operation.
    pub(in crate::defuse) fn close_lexical_world(&self) -> ClosedLexicalWorld {
        let world = match self {
            Environment::Use(world) => ClosedWorld::Use(UseEnvironment {
                locals: world.locals.closed_copy(),
                reach: world.reach.clone(),
                data: world.data.clone(),
            }),
            Environment::Body(world) => ClosedWorld::Body(BodyEnvironment {
                locals: world.locals.closed_copy(),
                declaration: world.declaration.clone(),
                formals: world.formals.closed_copy(),
                data: world.data.clone(),
            }),
        };
        ClosedLexicalWorld { world }
    }

    pub(crate) fn push_query_names(&mut self, names: crate::pipeline::asts::core::QueryLocalNames) {
        self.locals_mut().query_names.push(QueryFrame {
            id: QueryFrameId::mint(),
            names,
            bindings: Vec::new(),
        });
    }

    pub(crate) fn pop_query_names(&mut self) {
        self.locals_mut()
            .query_names
            .pop()
            .expect("a query resolution pops the name fact it pushed");
    }

    /// A CTE BINDING'S BODY OPENS. The binding is resolved by the very act
    /// that pushed its query's name fact, so the innermost frame IS the
    /// frame this binding was declared in — the site is minted from the
    /// declaration, not recovered from a depth.
    pub(crate) fn push_binding_declaration(
        &mut self,
        horizon: crate::pipeline::asts::core::LexicalHorizon,
    ) {
        let locals = self.locals_mut();
        let frame = locals
            .query_names
            .last()
            .expect("a query-scoped binding requires its query name fact")
            .id;
        let opened_over = locals.query_names.len();
        locals.declarations.push(ActiveDeclaration {
            site: LexicalSite { frame, horizon },
            opened_over,
        });
    }

    /// A QUERY-SCOPED VALUE DEFINITION'S BODY OPENS AT ITS OWN DECLARATION
    /// SITE — the site its registration stamped, read from the carrier the
    /// selection produced. The site is not an argument, so no caller can
    /// open one definition's body at another's lexical position.
    pub(in crate::defuse::environment) fn push_scoped_declaration(
        &mut self,
        definition: &ScopedCfe,
    ) {
        let locals = self.locals_mut();
        let opened_over = locals.query_names.len();
        locals.declarations.push(ActiveDeclaration {
            site: definition.site(),
            opened_over,
        });
    }

    /// The pop of whichever declaration road opened one: the CTE-binding
    /// road's [`Self::push_binding_declaration`] outside this module, the
    /// scoped-definition authority's own openings inside it.
    pub(crate) fn pop_declaration(&mut self) {
        self.locals_mut()
            .declarations
            .pop()
            .expect("a query-scoped body pops the declaration it pushed");
    }

    /// ONE STATEMENT, RESTATED under the block standing here: its own body
    /// beneath that block's relation bindings. A statement that arrived with
    /// bindings of its own is not one of these — the door refuses to pair a
    /// caller-supplied block with this world's declarations.
    pub(in crate::defuse) fn restate_with_standing_bindings(
        &self,
        query: crate::pipeline::ast_unresolved::Query,
    ) -> Result<crate::pipeline::ast_unresolved::Query> {
        if !query.locals.is_empty() {
            return Err(crate::diagnostic::Internal::invariant(
                "defuse::environment",
                "a statement of a standing block carries no block of its own",
            ));
        }
        Ok(crate::pipeline::ast_unresolved::Query::binding(
            crate::pipeline::asts::core::QueryLocals::bindings_only(self.standing_bindings()?),
            query.body,
        ))
    }

    /// THE RELATION BINDINGS IN LEXICAL REACH of the body standing here.
    ///
    /// DERIVED FROM THE FRAMES, never rebuilt by a walk: every frame the
    /// open declaration admits contributes the bindings that declaration's
    /// horizon lets it see, outermost first — the order a nearer binding
    /// needs to read a farther one — and a spelling a nearer frame declares
    /// takes the outer one's place entirely, claim and binding together,
    /// because selection stops at the nearer claim. A claimless
    /// compiler-built binding belongs to its own frame alone.
    ///
    /// They are cloned per statement because each statement resolves them
    /// against the world as that statement finds it: the plan creates
    /// relations between statements, and a binding reads what exists when
    /// it runs. What never moves is which names are in reach.
    pub(in crate::defuse) fn standing_bindings(
        &self,
    ) -> Result<Vec<crate::pipeline::ast_unresolved::CteBinding>> {
        use crate::pipeline::asts::core::queries::QueryLocalJudgment;
        use crate::pipeline::asts::core::QueryLocalDemand;
        let locals = self.locals();
        let mut nearer: std::collections::HashSet<&delightql_types::SqlIdentifier> =
            std::collections::HashSet::new();
        let mut reachable = Vec::new();
        for (index, frame) in locals.query_names.iter().enumerate() {
            let Some(horizon) = locals.horizon_for(index)? else {
                continue;
            };
            reachable.push((index, frame, horizon));
        }
        // Innermost first for the shadowing decision, then restored to
        // outermost-first for resolution order.
        let mut kept: Vec<Vec<crate::pipeline::ast_unresolved::CteBinding>> =
            vec![Vec::new(); reachable.len()];
        for (slot, (_, frame, horizon)) in reachable.iter().enumerate().rev() {
            let mut declared_here = Vec::new();
            for binding in &frame.bindings {
                match binding.subject().authored_name() {
                    None => kept[slot].push(binding.clone()),
                    Some(name) => {
                        if nearer.contains(name) {
                            continue;
                        }
                        if matches!(
                            frame
                                .names
                                .judge(name, *horizon, QueryLocalDemand::Relation),
                            QueryLocalJudgment::Lawful(_)
                        ) {
                            declared_here.push(name);
                            kept[slot].push(binding.clone());
                        }
                    }
                }
            }
            nearer.extend(declared_here);
        }
        Ok(kept.into_iter().flatten().collect())
    }

    /// ONE QUERY-LOCAL SELECTION. Wrong kind and not-yet-visible are closed    /// ONE QUERY-LOCAL SELECTION. Wrong kind and not-yet-visible are closed
    /// refusals; `Ok(None)` alone proves local absence and licenses an outer
    /// lookup road.
    pub(crate) fn select_query_local(
        &self,
        name: &delightql_types::SqlIdentifier,
        demand: crate::pipeline::asts::core::QueryLocalDemand,
        scoped: Option<&ScopedSlotWorld>,
    ) -> Result<Option<QueryLocalSelection>> {
        use crate::pipeline::asts::core::QueryLocalKind;
        let locals = self.locals();
        // A compiler-generated CTE deliberately carries no authored name
        // claim. Its private manifestation may stand beneath a same-spelled
        // outer CHOE while the compiler-built main reads that exact carrier.
        if demand == crate::pipeline::asts::core::QueryLocalDemand::Relation {
            if let Some(cte) = locals
                .body_scope()
                .and_then(|scope| scope.synthetic_ctes.get(name))
                .or_else(|| locals.synthetic_ctes.get(name))
            {
                return Ok(Some(QueryLocalSelection::Relation(cte.clone())));
            }
        }
        // A SEALED SLOT supplies the site its own body stands at; every
        // other position reads the site the innermost open declaration
        // stamped. Both answer the same question of the same frame, so
        // neither road decides visibility by counting frames.
        let supplied = match scoped.map(ScopedSlotWorld::site) {
            Some(site) => Some((
                locals
                    .query_names
                    .iter()
                    .position(|frame| frame.id == site.frame)
                    .ok_or_else(|| {
                        crate::diagnostic::Internal::invariant(
                            "defuse::environment",
                            "a sealed slot names a lexical frame this world does not hold",
                        )
                    })?,
                site.horizon,
            )),
            None => None,
        };
        let mut selected_kind = None;
        for (query_index, frame) in locals.query_names.iter().enumerate().rev() {
            let names = &frame.names;
            let scoped_horizon = match supplied {
                Some((at, horizon)) => match query_index.cmp(&at) {
                    std::cmp::Ordering::Less => None,
                    std::cmp::Ordering::Equal => Some(horizon),
                    std::cmp::Ordering::Greater => {
                        Some(crate::pipeline::asts::core::LexicalHorizon::all())
                    }
                },
                None => locals.horizon_for(query_index)?,
            };
            let Some(scoped_horizon) = scoped_horizon else {
                continue;
            };
            match names.select(name, scoped_horizon, demand)? {
                Some(kind) => {
                    selected_kind = Some((kind, scoped_horizon));
                    break;
                }
                None => continue,
            }
        }
        match selected_kind {
            None => Ok(None),
            Some((kind, selected_horizon)) => {
                let locals = self.locals();
                let selected = match kind {
                    QueryLocalKind::Relation => locals
                        .body_scope()
                        .and_then(|scope| scope.ctes.get(name))
                        .or_else(|| locals.ctes.get(name))
                        .cloned()
                        .map(QueryLocalSelection::Relation),
                    QueryLocalKind::Value => locals
                        .body_scope()
                        .and_then(|scope| scope.cfes.get(name))
                        .or_else(|| locals.cfes.get(name))
                        .and_then(|definitions| {
                            definitions
                                .iter()
                                .rev()
                                .find(|scoped| selected_horizon.contains(scoped.declared_horizon()))
                        })
                        .cloned()
                        .map(QueryLocalSelection::Value),
                    QueryLocalKind::HigherOrder => locals
                        .body_scope()
                        .and_then(|scope| scope.hos.get(name))
                        .or_else(|| locals.hos.get(name))
                        .cloned()
                        .map(QueryLocalSelection::HigherOrder),
                    // THE EFFECT MANIFESTATIONS ANSWER HERE TOO. One
                    // selection authority for every query-local kind: the
                    // effect walk has no parallel block to search, so an
                    // effect body reads its declarations exactly as a pure
                    // one does.
                    QueryLocalKind::EffectRelation => locals
                        .body_scope()
                        .and_then(|scope| scope.effect_ctes.get(name))
                        .or_else(|| locals.effect_ctes.get(name))
                        .cloned()
                        .map(QueryLocalSelection::EffectRelation),
                    QueryLocalKind::EffectHigherOrder => locals
                        .body_scope()
                        .and_then(|scope| scope.hos.get(name))
                        .or_else(|| locals.hos.get(name))
                        .cloned()
                        .map(QueryLocalSelection::EffectHigherOrder),
                };
                selected.map(Some).ok_or_else(|| {
                    DelightQLError::from(Resolution::CallableUnknown {
    message: format!(
                            "query-local name '{name}' is visible here, but its {} is not available while resolving {}",
                            kind.description(),
                            demand.description()
                        ),
})
                })
            }
        }
    }
    fn locals(&self) -> &Locals {
        match self {
            Environment::Use(world) => &world.locals,
            Environment::Body(world) => &world.locals,
        }
    }

    fn locals_mut(&mut self) -> &mut Locals {
        match self {
            Environment::Use(world) => &mut world.locals,
            Environment::Body(world) => &mut world.locals,
        }
    }

    pub(crate) fn reach(&self) -> &DeclarationReach {
        match self {
            Environment::Use(world) => &world.reach,
            Environment::Body(world) => &world.declaration.reach,
        }
    }

    pub(crate) fn data(&self) -> &DataBinding {
        match self {
            Environment::Use(world) => &world.data,
            Environment::Body(world) => &world.data,
        }
    }

    /// The scope this world names in diagnostics.
    pub(crate) fn display_scope(&self) -> &str {
        self.reach().root_fq()
    }

    /// Whether names in this world are answered under a declaration
    /// (true for a body world, and for a use world rooted anywhere but the
    /// interactive session).
    pub(crate) fn is_declaration(&self) -> bool {
        match self {
            Environment::Use(world) => world.reach.root_fq() != "home",
            Environment::Body(_) => true,
        }
    }

    /// The one registration road for query-local manifestations. Selection
    /// never reads these stores without first consuming the common name fact.
    pub(crate) fn register_query_local(&mut self, registration: QueryLocalRegistration) {
        let locals = self.locals_mut();
        // A DEFINITION whose body is resolved later — a value or a
        // higher-order one — is registered inside the query whose name fact
        // holds its claim: `resolve_query_with` pushes the fact and then
        // registers the manifestations it spent. The innermost frame is
        // therefore that definition's declaring frame BY CONSTRUCTION, and
        // it is stamped now so no later act has to work it out. A relation
        // binding needs none: its body is resolved where it is written, and
        // a compiler-built one carries no authored claim at all.
        let declaring_frame = || {
            locals
                .query_names
                .last()
                .expect("a query-local definition stands inside its query's name fact")
                .id
        };
        match registration {
            QueryLocalRegistration::Relation { name, relation } => {
                match locals.body_scope_mut() {
                    Some(scope) => scope.ctes.insert(name, LocalCte::Ordinary(relation)),
                    None => locals.ctes.insert(name, LocalCte::Ordinary(relation)),
                };
            }
            QueryLocalRegistration::SyntheticRelation { name, relation } => {
                match locals.body_scope_mut() {
                    Some(scope) => scope
                        .synthetic_ctes
                        .insert(name, LocalCte::Ordinary(relation)),
                    None => locals
                        .synthetic_ctes
                        .insert(name, LocalCte::Ordinary(relation)),
                };
            }
            // THE DECLARATION SITE IS STAMPED HERE, by the act that puts
            // the definition in the world: the frame is the one whose name
            // fact holds this definition's claim, and the horizon is the
            // one that claim's position minted. Nothing else pairs them.
            QueryLocalRegistration::Value(cfe) => {
                let site = LexicalSite {
                    frame: declaring_frame(),
                    horizon: cfe.horizon(),
                };
                let name = cfe.name.clone();
                let scoped = ScopedCfe::stamped(cfe, site);
                match locals.body_scope_mut() {
                    Some(scope) => scope.cfes.entry(name).or_default().push(scoped),
                    None => locals.cfes.entry(name).or_default().push(scoped),
                };
            }
            // The label's arms accumulate in authored order under the site
            // that declared them: the label denotes their corresponding
            // union, so a later arm joins the earlier ones rather than
            // replacing them.
            QueryLocalRegistration::EffectRelation { name, arm } => {
                let site = LexicalSite {
                    frame: declaring_frame(),
                    horizon: crate::pipeline::asts::core::LexicalHorizon::all(),
                };
                match locals.body_scope_mut() {
                    Some(scope) => scope.effect_ctes.entry(name),
                    None => locals.effect_ctes.entry(name),
                }
                .or_insert_with(|| ScopedEffectArms::stamped(site))
                .admit(arm);
            }
            QueryLocalRegistration::HigherOrder(ho) => {
                let site = LexicalSite {
                    frame: declaring_frame(),
                    horizon: *ho.horizon(),
                };
                let name = ho.name().clone();
                let scoped = ScopedHo::stamped(ho, site);
                match locals.body_scope_mut() {
                    Some(scope) => scope.hos.insert(name, scoped),
                    None => locals.hos.insert(name, scoped),
                };
            }
        }
    }

    pub(crate) fn register_frontier(
        &mut self,
        frontier: super::instance::DefinitionFrontier,
        relation: crate::relation::SemanticRelation,
    ) {
        let name = frontier.name().clone();
        let bound = LocalCte::Frontier { relation, frontier };
        let locals = self.locals_mut();
        match locals.body_scope_mut() {
            Some(scope) => scope.synthetic_ctes.insert(name, bound),
            None => locals.synthetic_ctes.insert(name, bound),
        };
    }

    /// THE CARRIERS A CONSTRUCTION BOUND COME INTO VIEW: the record's
    /// formals join those at this level, as the record holds them, for the
    /// mentions that address them here.
    pub(crate) fn adopt_carriers(&mut self, record: &crate::defuse::carriers::CarrierRecord) {
        let locals = self.locals_mut();
        match locals.body_scope_mut() {
            Some(scope) => scope.carriers.absorb(record.formals_only()),
            None => locals.carriers.absorb(record.formals_only()),
        }
    }

    pub(crate) fn register_alias(
        &mut self,
        alias: delightql_types::SqlIdentifier,
        target: delightql_types::SqlIdentifier,
    ) {
        let locals = self.locals_mut();
        match locals.body_scope_mut() {
            Some(scope) => scope.aliases.insert(alias, target),
            None => locals.aliases.insert(alias, target),
        };
    }

    /// Enter an INSTANTIATION: a scoped definition or a curried closing
    /// resolving in this world answers its formals from this frame for
    /// exactly the lease's extent. The frame goes INTO the world and the
    /// lease's drop takes it out again — there is no push a caller could
    /// leave open, and no pop a caller could mispair.
    pub(in crate::defuse) fn instantiated(&mut self, frame: FormalBindings) -> Instantiated<'_> {
        self.push_frame(Frame {
            formals: frame,
            scope: None,
        });
        Instantiated { world: self }
    }

    /// Enter a CHOE BODY: its formal frame, its relation carriers, and the
    /// lexical horizon it was declared under stand on this world as ONE
    /// frame for exactly the lease's extent. The body's own registrations
    /// land in the frame and leave with it; the world beneath answers only
    /// what the horizon admits.
    /// The definition arrives WHOLE, so the site the body opens at is the
    /// site its own registration stamped: there is no signature here that
    /// takes a body beside a lexical position chosen for it.
    pub(in crate::defuse::environment) fn opened_body(
        &mut self,
        formals: FormalBindings,
        carriers: &crate::defuse::carriers::CarrierRecord,
        definition: &ScopedHo,
    ) -> Instantiated<'_> {
        let locals = self.locals_mut();
        let opened_over = locals.query_names.len();
        locals.declarations.push(ActiveDeclaration {
            site: definition.declaration_site(),
            opened_over,
        });
        self.push_frame(Self::body_frame(formals, carriers));
        Instantiated { world: self }
    }

    fn body_frame(
        formals: FormalBindings,
        carriers: &crate::defuse::carriers::CarrierRecord,
    ) -> Frame {
        Frame {
            formals,
            scope: Some(BodyScope {
                ctes: HashMap::new(),
                synthetic_ctes: HashMap::new(),
                cfes: HashMap::new(),
                hos: HashMap::new(),
                effect_ctes: HashMap::new(),
                carriers: carriers.formals_only(),
                aliases: HashMap::new(),
            }),
        }
    }

    fn push_frame(&mut self, frame: Frame) {
        self.locals_mut().instantiations.push(frame);
    }

    /// A frame leaves, and — when it was a body's — the declaration it
    /// opened leaves with it. The two are pushed and popped by the same
    /// acts, so neither can outlive the other.
    fn pop_frame(&mut self) {
        let frame = self
            .locals_mut()
            .instantiations
            .pop()
            .expect("an instantiation lease pops the frame it pushed");
        if frame.scope.is_some() {
            self.pop_declaration();
        }
    }

    /// The caller-resolved VALUE a formal name spends in this world: the
    /// innermost open instantiation first, then a body's own formals. A
    /// use world with no open instantiation answers nothing.
    pub(crate) fn formal_value(
        &self,
        name: &delightql_types::SqlIdentifier,
    ) -> Option<crate::pipeline::asts::resolved::DomainExpression> {
        for frame in self.locals().formal_frames() {
            if let Some(value) = frame.value(name) {
                return Some(value.clone());
            }
        }
        match self {
            Environment::Body(world) => world.formals.value(name).cloned(),
            Environment::Use(_) => None,
        }
    }

    /// The CLOSED CALLABLE a code-formal name spends in this world.
    pub(crate) fn formal_callable(
        &self,
        name: &delightql_types::SqlIdentifier,
    ) -> Option<crate::defuse::callable::CallableBinding> {
        for frame in self.locals().formal_frames() {
            if let Some(binding) = frame.callable(name) {
                return Some(binding.clone());
            }
        }
        match self {
            Environment::Body(world) => world.formals.callable(name).cloned(),
            Environment::Use(_) => None,
        }
    }

    /// The exact closed residual identity a rule formal spends.
    pub(crate) fn formal_rule(
        &self,
        name: &delightql_types::SqlIdentifier,
    ) -> Option<crate::defuse::ho::RuleValueId> {
        for frame in self.locals().formal_frames() {
            if let Some(binding) = frame.rule(name) {
                return Some(binding);
            }
        }
        match self {
            Environment::Body(world) => world.formals.rule(name),
            Environment::Use(_) => None,
        }
    }

    /// Whether an UNQUALIFIED spelling names a value formal here — the
    /// unification prefilter's question.
    pub(crate) fn covers_value_formal(&self, name: &str) -> bool {
        let key = delightql_types::SqlIdentifier::new(name);
        self.locals()
            .formal_frames()
            .any(|frame| frame.covers_value(&key))
            || matches!(self, Environment::Body(world) if world.formals.covers_value(&key))
    }

    /// The relation bound to an open recursive frontier, by the frontier's
    /// exact instance identity — never by spelling.
    pub(crate) fn frontier_relation(
        &self,
        frontier: &super::instance::DefinitionFrontier,
    ) -> Option<crate::relation::SemanticRelation> {
        let locals = self.locals();
        let bound = |cte: &LocalCte| match cte {
            LocalCte::Frontier {
                relation,
                frontier: open,
            } if open == frontier => Some(*relation),
            _ => None,
        };
        locals
            .body_scope()
            .and_then(|scope| scope.synthetic_ctes.values().find_map(bound))
            .or_else(|| locals.synthetic_ctes.values().find_map(bound))
    }

    /// THE PROOF OF THE CARRIER a structural landing names, for the
    /// mention that reads it: the resolver stands over the proof; the
    /// identity stays inside it.
    pub(crate) fn structural(
        &self,
        pending: crate::relation::StructuralRelation,
    ) -> Option<crate::defuse::carriers::CompilerRow> {
        self.carriers_holding(pending)
            .and_then(|record| record.compiler_row(pending))
    }

    /// THE RECORD THAT HOLDS A LANDING as a formal in view here — what a
    /// call forwarding the carrier as its own formal inherits from.
    pub(crate) fn carriers_holding(
        &self,
        pending: crate::relation::StructuralRelation,
    ) -> Option<&crate::defuse::carriers::CarrierRecord> {
        let locals = self.locals();
        locals
            .body_scope()
            .map(|scope| &scope.carriers)
            .filter(|record| record.holds_formal(pending))
            .or_else(|| Some(&locals.carriers).filter(|record| record.holds_formal(pending)))
    }

    pub(crate) fn materialized(
        &self,
        name: &delightql_types::SqlIdentifier,
    ) -> Option<crate::relation::SemanticRelation> {
        self.locals().materialized.get(name).copied()
    }

    /// A query alias noted while resolving. Inside a CHOE body only the
    /// body's own aliases answer: the caller's are its row's, which the
    /// body cannot see.
    fn alias_target(
        &self,
        name: &delightql_types::SqlIdentifier,
    ) -> Option<delightql_types::SqlIdentifier> {
        let locals = self.locals();
        match locals.body_scope() {
            Some(scope) => scope.aliases.get(name).cloned(),
            None => locals.aliases.get(name).cloned(),
        }
    }
}

/// An open instantiation lease: the frame stands on the world for exactly
/// this value's lifetime. Dropping it — on every path, unwinding included
/// — removes exactly the frame it installed.
pub(in crate::defuse) struct Instantiated<'e> {
    world: &'e mut Environment,
}

impl Instantiated<'_> {
    /// The world with the frame standing on it, for the resolution that
    /// spends the frame.
    pub(in crate::defuse) fn world(&mut self) -> &mut Environment {
        self.world
    }
}

impl Drop for Instantiated<'_> {
    fn drop(&mut self) {
        self.world.pop_frame();
    }
}

/// ONE DECLARATION STANDING over a world held behind a `RefCell`. Pushed at
/// construction and popped on drop, each under a borrow lasting only that
/// long, so the walk it governs may borrow the world in between.
///
/// Constructed only by the scoped-definition authority, from a site it holds
/// — never from a site a caller supplies.
pub(in crate::defuse::environment) struct DeclaredArms<'w> {
    world: &'w std::cell::RefCell<Environment>,
}

impl<'w> DeclaredArms<'w> {
    pub(in crate::defuse::environment) fn open(
        world: &'w std::cell::RefCell<Environment>,
        site: LexicalSite,
    ) -> Self {
        let mut open = world.borrow_mut();
        let locals = open.locals_mut();
        let opened_over = locals.query_names.len();
        locals
            .declarations
            .push(ActiveDeclaration { site, opened_over });
        drop(open);
        DeclaredArms { world }
    }
}

impl Drop for DeclaredArms<'_> {
    fn drop(&mut self) {
        self.world.borrow_mut().pop_declaration();
    }
}

/// ONE AUTHORED QUERY-LOCAL BLOCK, DECLARED on a world held behind a
/// `RefCell` for the extent of the body it governs.
///
/// An effect body is one block and many statements. Its claims and its
/// definitions are declared HERE, once, so every statement — and every
/// directive demand between statements — reads the same declaration set;
/// nothing re-derives it and no walk carries a copy of it. What each
/// statement still carries is its relation bindings, which resolve afresh
/// as the plan's world changes.
pub(in crate::defuse::environment) struct DeclaredBlock<'w> {
    world: &'w std::cell::RefCell<Environment>,
}

impl<'w> DeclaredBlock<'w> {
    pub(in crate::defuse::environment) fn open(
        world: &'w std::cell::RefCell<Environment>,
        locals: crate::pipeline::asts::core::QueryLocals<crate::pipeline::asts::core::Unresolved>,
    ) -> Self {
        let (names, cfes, hos, ctes) = locals.spend();
        let mut open = world.borrow_mut();
        open.push_query_names(names);
        open.locals_mut()
            .query_names
            .last_mut()
            .expect("the frame just pushed")
            .bindings = ctes
            .iter()
            .filter(|cte| !cte.subject().declares_effect())
            .cloned()
            .collect();
        for cfe in cfes {
            open.register_query_local(QueryLocalRegistration::Value(cfe));
        }
        for ho in hos {
            open.register_query_local(QueryLocalRegistration::HigherOrder(ho));
        }
        for cte in ctes {
            // Only the effect-marked bindings are manifestations of this
            // block: a pure binding resolves in each statement that reads
            // it, because the world it reads is the plan's, not the text's.
            if cte.subject().declares_effect() {
                if let Some(name) = cte.subject().authored_name().cloned() {
                    open.register_query_local(QueryLocalRegistration::EffectRelation {
                        name,
                        arm: cte,
                    });
                }
            }
        }
        drop(open);
        DeclaredBlock { world }
    }
}

impl Drop for DeclaredBlock<'_> {
    fn drop(&mut self) {
        self.world.borrow_mut().pop_query_names();
    }
}

/// The same lease over a world held behind a `RefCell` (an effect plan's
/// world): the frame is pushed at construction and popped on drop, each
/// under a borrow that lasts only for the push or the pop, so the plan's
/// own statement resolutions may borrow the world in between.
pub(in crate::defuse::environment) struct SharedInstantiated<'w> {
    world: &'w std::cell::RefCell<Environment>,
}

impl<'w> SharedInstantiated<'w> {
    /// AN EFFECT CHOE'S BODY OPENS AT ITS OWN DECLARATION SITE — the site
    /// its registration stamped, read from the carrier the selection
    /// produced, exactly as a pure CHOE's body opens. The effect road holds
    /// no separate visibility rule: what this body may see is what the
    /// declaration standing here admits.
    pub(in crate::defuse::environment) fn opened_body(
        world: &'w std::cell::RefCell<Environment>,
        formals: FormalBindings,
        definition: &ScopedHo,
    ) -> Self {
        let mut open = world.borrow_mut();
        let locals = open.locals_mut();
        let opened_over = locals.query_names.len();
        locals.declarations.push(ActiveDeclaration {
            site: definition.declaration_site(),
            opened_over,
        });
        open.push_frame(Environment::body_frame(
            formals,
            &crate::defuse::carriers::CarrierRecord::default(),
        ));
        drop(open);
        SharedInstantiated { world }
    }
}

impl Drop for SharedInstantiated<'_> {
    fn drop(&mut self) {
        self.world.borrow_mut().pop_frame();
    }
}

#[cfg(test)]
mod query_local_selection_tests {
    use super::*;

    fn cfe(name: &delightql_types::SqlIdentifier) -> crate::pipeline::asts::core::CfeDefinition {
        crate::pipeline::asts::core::CfeDefinition::unbounded(
            name.clone(),
            crate::pipeline::asts::core::CfeFormals::from_role_groups([], []),
            crate::pipeline::asts::core::ContextMode::None,
            crate::pipeline::asts::core::DomainExpression::Application(
                crate::pipeline::asts::core::FunctionApplication::Ground(
                    crate::pipeline::asts::core::LiteralValue::Null,
                ),
            ),
        )
    }

    #[test]
    fn a_horizon_selects_the_latest_cfe_declared_within_it() {
        let name = delightql_types::SqlIdentifier::new("f");
        // Both definitions are admitted through the one minting door, so
        // each carries the horizon its own claim's position minted.
        let mut block = crate::pipeline::asts::core::QueryLocalBlock::default();
        block.admit_cfe(cfe(&name)).expect("earlier CFE");
        block.admit_cfe(cfe(&name)).expect("later CFE");
        let locals = block.seal().expect("the block seals");
        let earlier_horizon = locals.cfes()[0].horizon();
        let later_horizon = locals.cfes()[1].horizon();
        let mut environment = Environment::Use(UseEnvironment::detached());
        environment.push_query_names(locals.names().clone());

        let selected = |environment: &Environment| match environment
            .select_query_local(
                &name,
                crate::pipeline::asts::core::QueryLocalDemand::Value,
                None,
            )
            .expect("selection")
            .expect("visible CFE")
        {
            QueryLocalSelection::Value(scoped) => scoped,
            _ => unreachable!("value demand returns a value"),
        };

        // The earlier definition's carrier, taken from THIS world — the
        // only place one can come from, since a site is not a value.
        environment.register_query_local(QueryLocalRegistration::Value(locals.cfes()[0].clone()));
        let earlier = selected(&environment);
        assert_eq!(earlier.declared_horizon(), earlier_horizon);

        // With both registered and no declaration open, the whole block is
        // in view and the LATEST claim of the spelling answers.
        environment.register_query_local(QueryLocalRegistration::Value(locals.cfes()[1].clone()));
        assert_eq!(selected(&environment).declared_horizon(), later_horizon);

        // Standing in the EARLIER definition's own body, the later
        // declaration is not yet visible.
        environment.push_scoped_declaration(&earlier);
        assert_eq!(selected(&environment).declared_horizon(), earlier_horizon);
        environment.pop_declaration();
        assert_eq!(selected(&environment).declared_horizon(), later_horizon);
    }

    /// LEXICAL REACH IS THE DECLARATION'S FRAME, NOT A DEPTH.
    ///
    /// A definition declared in the outermost frame stays visible however
    /// many frames stand between it and the body being resolved — which is
    /// what a stack-length convention cannot express, because the number it
    /// reads grows with the caller's nesting rather than with the
    /// declaration's position.
    #[test]
    fn a_declaration_reaches_its_body_at_any_nesting() {
        let name = delightql_types::SqlIdentifier::new("f");
        let mut block = crate::pipeline::asts::core::QueryLocalBlock::default();
        block.admit_cfe(cfe(&name)).expect("the only CFE");
        let locals = block.seal().expect("the block seals");
        let mut environment = Environment::Use(UseEnvironment::detached());
        environment.push_query_names(locals.names().clone());
        environment.register_query_local(QueryLocalRegistration::Value(locals.cfes()[0].clone()));

        let selected = match environment
            .select_query_local(
                &name,
                crate::pipeline::asts::core::QueryLocalDemand::Value,
                None,
            )
            .expect("selection")
            .expect("visible CFE")
        {
            QueryLocalSelection::Value(scoped) => scoped,
            _ => unreachable!("value demand returns a value"),
        };

        // Nine intervening query frames — every one of them a body opened
        // between the declaration and this resolution.
        for _ in 0..9 {
            environment.push_query_names(crate::pipeline::asts::core::QueryLocalNames::default());
        }
        environment.push_scoped_declaration(&selected);
        assert!(
            environment
                .select_query_local(
                    &name,
                    crate::pipeline::asts::core::QueryLocalDemand::Value,
                    None,
                )
                .expect("selection")
                .is_some(),
            "the declaration's own frame is still in view at any nesting"
        );
    }

    /// A SITE WHOSE FRAME THIS WORLD DOES NOT HOLD IS NOT AN ANSWER.
    ///
    /// The pairing is broken, and a broken pairing refuses rather than
    /// silently hiding or revealing names — the enumeration that decides
    /// visibility has to be able to point at the frame it enumerated.
    #[test]
    fn a_site_whose_frame_left_the_world_refuses() {
        let name = delightql_types::SqlIdentifier::new("f");
        let mut block = crate::pipeline::asts::core::QueryLocalBlock::default();
        block.admit_cfe(cfe(&name)).expect("the only CFE");
        let locals = block.seal().expect("the block seals");
        let mut environment = Environment::Use(UseEnvironment::detached());
        environment.push_query_names(locals.names().clone());
        environment.register_query_local(QueryLocalRegistration::Value(locals.cfes()[0].clone()));
        let selected = match environment
            .select_query_local(
                &name,
                crate::pipeline::asts::core::QueryLocalDemand::Value,
                None,
            )
            .expect("selection")
            .expect("visible CFE")
        {
            QueryLocalSelection::Value(scoped) => scoped,
            _ => unreachable!("value demand returns a value"),
        };

        // The frame the carrier names leaves; the world pushes another
        // holding the very same claims. Frame identities are minted per
        // push and never reused, so the replacement is a DIFFERENT frame
        // however identical its contents.
        environment.pop_query_names();
        environment.push_query_names(locals.names().clone());
        environment.push_scoped_declaration(&selected);
        assert!(
            environment
                .select_query_local(
                    &name,
                    crate::pipeline::asts::core::QueryLocalDemand::Value,
                    None,
                )
                .is_err(),
            "a declaration naming a frame this world does not hold refuses"
        );
    }
}

#[cfg(test)]
mod formal_inventory_tests {
    use super::*;

    fn value_one() -> crate::pipeline::asts::resolved::DomainExpression {
        crate::pipeline::asts::resolved::DomainExpression::Application(
            crate::pipeline::asts::resolved::FunctionApplication::Ground(
                crate::pipeline::asts::core::LiteralValue::Number("1".into()),
            ),
        )
    }

    /// A spelling the family never declared has no lawful binding: the
    /// inventory refuses it. A design that mints identities AFTER pairing
    /// cannot satisfy this — it would accept any name.
    #[test]
    fn an_undeclared_spelling_refuses_to_bind() {
        let mut inventory = FormalInventory::declared([(
            delightql_types::SqlIdentifier::new("x"),
            FormalRole::Value,
        )]);
        let refusal = inventory
            .bind_named(&delightql_types::SqlIdentifier::new("y"), value_one())
            .expect_err("an undeclared formal must refuse");
        assert!(format!("{refusal}").contains("not a declared parameter"));
    }

    /// A positional binding is TOTAL: too many or too few actuals refuse
    /// before anything binds, in release builds as in debug. A `zip`
    /// would silently drop the third actual here.
    #[test]
    fn a_positional_cardinality_mismatch_refuses() {
        let declared = || {
            FormalInventory::declared([
                (delightql_types::SqlIdentifier::new("a"), FormalRole::Value),
                (delightql_types::SqlIdentifier::new("b"), FormalRole::Value),
            ])
        };
        let too_many = declared()
            .bind_positional(FormalRole::Value, [value_one(), value_one(), value_one()])
            .expect_err("three actuals over two formals refuse");
        assert!(format!("{too_many}").contains("declares 2 value parameters; 3 actuals"));
        let too_few = declared()
            .bind_positional(FormalRole::Value, [value_one()])
            .expect_err("one actual over two formals refuses");
        assert!(format!("{too_few}").contains("declares 2 value parameters; 1 actual"));
        declared()
            .bind_positional(FormalRole::Value, [value_one(), value_one()])
            .expect("two actuals over two formals bind");
    }

    /// Two frames declaring the SAME spelling hold nothing of each other:
    /// a frame sealed by one inventory answers only what that inventory
    /// bound. Identities never leave a frame, so there is no API through
    /// which frame B could answer with frame A's binding.
    #[test]
    fn same_spelling_in_two_frames_never_crosses() {
        let name = delightql_types::SqlIdentifier::new("x");
        let mut bound = FormalInventory::declared([(name.clone(), FormalRole::Value)]);
        bound.bind_named(&name, value_one()).expect("declared");
        let bound = bound.sealed();
        let unbound = FormalInventory::declared([(name.clone(), FormalRole::Value)]).sealed();
        assert!(bound.value(&name).is_some());
        assert!(unbound.value(&name).is_none());
        assert!(!unbound.covers_value(&name));
    }
}
