// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::{Chain, DomainExpression, Phase, Unresolved};
use crate::diagnostic::{Constraint, DelightQLError, Internal, Resolution};
use crate::{lispy::ToLispy, ToLispy};
use std::fmt;

// ============================================================================
// Emit Types
// ============================================================================

/// An emit specification — a forked sub-query that fans out rows to a named
/// sink. The main pipeline continues unchanged; the emit body is compiled
/// independently to a separate SQL query that the host executes and routes.
///
// ============================================================================
// Danger Gate Types
// ============================================================================

/// Toggle state for a danger gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DangerState {
    /// Dangerous behavior is enabled
    On,
    /// Dangerous behavior is disabled (safe default)
    Off,
    /// Compiler may use the dangerous path if needed but is not required to
    Allow,
    /// Graduated severity level (1-9) for host-defined policies
    Severity(u8),
}

impl fmt::Display for DangerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DangerState::On => write!(f, "ON"),
            DangerState::Off => write!(f, "OFF"),
            DangerState::Allow => write!(f, "ALLOW"),
            DangerState::Severity(n) => write!(f, "{}", n),
        }
    }
}

/// A danger gate specification — a per-query override for a named safety boundary.
///
/// Created by the builder when it encounters `(~~danger://path STATE~~)` in the CST.
/// The URI identifies the danger; the state controls it.
#[derive(Debug, Clone, PartialEq)]
pub struct DangerSpec {
    /// The canonical danger URI (e.g. "delightql-danger://cardinality/cartesian")
    pub uri: String,
    /// The toggle state for this query
    pub state: DangerState,
}

// ============================================================================
// Option Types
// ============================================================================

/// Toggle state for an option (strategy/preference selection).
/// Same values as DangerState — ON, OFF, ALLOW, or graduated severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionState {
    /// Option is enabled
    On,
    /// Option is disabled (default)
    Off,
    /// Compiler may use the option if beneficial
    Allow,
    /// Graduated preference level (1-9) for host-defined behavior
    Severity(u8),
}

impl fmt::Display for OptionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OptionState::On => write!(f, "ON"),
            OptionState::Off => write!(f, "OFF"),
            OptionState::Allow => write!(f, "ALLOW"),
            OptionState::Severity(n) => write!(f, "{}", n),
        }
    }
}

/// An option specification — a per-query strategy/preference override.
///
/// Created by the builder when it encounters `(~~option://path STATE~~)` in the CST.
/// The URI identifies the option; the state controls it.
#[derive(Debug, Clone, PartialEq)]
pub struct OptionSpec {
    /// The canonical config URI (e.g. "delightql-config://generation/rule/inlining/view")
    pub uri: String,
    /// The toggle state for this query
    pub state: OptionState,
}

// ============================================================================
// Inline DDL Types
// ============================================================================

/// An inline DDL block from a `(~~ddl ... ~~)` annotation.
///
/// The body is TYPED definition content, parsed and normalized with the
/// enclosing submission — never inline text. Registration remains a
/// consultation-time act: which namespace the block lands in, collisions,
/// redefinition, and rollback are judged against live session state.
#[derive(Debug, Clone)]
pub struct InlineDdlSpec {
    /// The block's definition content. Empty for `(~~ddl ~~)` — the lawful
    /// empty block, which declares nothing and creates nothing.
    pub body: InlineDdlBody,
    /// Optional namespace name for the definitions. `Some("chz")` routes to the
    /// scratch child `home::chz`; `None` (unnamed block) lands the entities directly
    /// in `home`.
    pub namespace: Option<String>,
}

/// A block's body is FILE-SHAPED: many subjects and nested blocks, not one
/// definition. Clauses stay unassembled — sibling agreement is the
/// consultation-time assembler's judgment (`DefinitionGroup::assemble`),
/// exactly as for a consulted file.
#[derive(Debug, Clone, Default)]
pub struct InlineDdlBody {
    /// One clause per authored definition, in authored order.
    pub definitions: Vec<crate::pipeline::asts::ddl::ClauseDecl>,
    /// Nested blocks, same carrier, subordinate to this block's namespace.
    pub ddl_blocks: Vec<InlineDdlSpec>,
}

impl InlineDdlBody {
    /// Whether the block declares anything at all.
    pub fn is_empty(&self) -> bool {
        self.definitions.is_empty() && self.ddl_blocks.is_empty()
    }
}

/// THE BLOCKS ONE STATEMENT CARRIES, by where they stand in it.
///
/// A statement is one step of a linear program. A block written in its
/// preamble LEADS it and is admitted before the statement runs; a block
/// written after its head began TRAILS it and is admitted after the
/// statement's effects, so an `enlist!` or `alias!` it follows is in its
/// lexical world. The grammar attaches a block written between two
/// statements to the earlier one; that attachment is where the block
/// stands, not permission to admit it first.
#[derive(Debug, Clone, Default)]
pub struct StatementBlocks {
    /// Written before the statement's body.
    pub leading: Vec<InlineDdlSpec>,
    /// Written after the statement's head.
    pub trailing: Vec<InlineDdlSpec>,
}

impl StatementBlocks {
    /// Whether the statement carries any block at all.
    pub fn is_empty(&self) -> bool {
        self.leading.is_empty() && self.trailing.is_empty()
    }
}

/// THE LEXICAL HORIZON of a query-scoped body: the last authored declaration
/// position the body may see. The query-local name authority assigns the
/// positions while it reads the block, so visibility is an ordering fact and
/// never reconstructed from whichever per-kind collection a consumer has.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct LexicalHorizon(usize);

impl LexicalHorizon {
    pub(crate) fn through(position: usize) -> Self {
        LexicalHorizon(position)
    }

    pub(crate) fn all() -> Self {
        LexicalHorizon(usize::MAX)
    }

    pub(crate) fn admits(&self, position: usize) -> bool {
        position <= self.0
    }

}

impl ToLispy for LexicalHorizon {
    fn to_lispy(&self) -> String {
        self.0.to_string()
    }
}

/// The manifestation that owns one query-local spelling. Pure and effect
/// faces are distinct capabilities even where they share one syntax family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueryLocalKind {
    Relation,
    Value,
    HigherOrder,
    Sigma,
    EffectRelation,
    EffectHigherOrder,
}

impl QueryLocalKind {
    pub(crate) fn description(self) -> &'static str {
        match self {
            QueryLocalKind::Relation => "common table expression",
            QueryLocalKind::Value => "common function expression",
            QueryLocalKind::HigherOrder => "common higher-order expression",
            QueryLocalKind::Sigma => "common sigma expression",
            QueryLocalKind::EffectRelation => "effect common table expression",
            QueryLocalKind::EffectHigherOrder => "effect common higher-order expression",
        }
    }
}

/// The position asking to spend a query-local name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueryLocalDemand {
    Relation,
    Value,
    HigherOrder,
    Sigma,
    Effect,
}

impl QueryLocalDemand {
    pub(crate) fn description(self) -> &'static str {
        match self {
            QueryLocalDemand::Relation => "relation position",
            QueryLocalDemand::Value => "value-call position",
            QueryLocalDemand::HigherOrder => "parameterized relation position",
            QueryLocalDemand::Sigma => "sigma position",
            QueryLocalDemand::Effect => "effect position",
        }
    }

    fn admits(self, kind: QueryLocalKind) -> bool {
        matches!(
            (self, kind),
            (QueryLocalDemand::Relation, QueryLocalKind::Relation)
                | (QueryLocalDemand::Value, QueryLocalKind::Value)
                | (QueryLocalDemand::HigherOrder, QueryLocalKind::HigherOrder)
                | (QueryLocalDemand::Sigma, QueryLocalKind::Relation)
                | (QueryLocalDemand::Sigma, QueryLocalKind::Sigma)
                | (QueryLocalDemand::Effect, QueryLocalKind::EffectRelation)
                | (QueryLocalDemand::Effect, QueryLocalKind::EffectHigherOrder)
        )
    }
}

/// The exhaustive answer for one query-local spelling. `Absent` is the only
/// answer that licenses a consulted or catalog lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueryLocalJudgment {
    Lawful(QueryLocalKind),
    WrongKind(QueryLocalKind),
    NotYetVisible(QueryLocalKind),
    Absent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueryLocalClaim {
    kind: QueryLocalKind,
    first_position: usize,
}

/// ONE CONSTRUCTION-OWNED QUERY-LOCAL NAME FACT. It is populated in authored
/// order and travels with the unresolved query; resolution spends it with the
/// definitions, and no later phase carries query-local spelling.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QueryLocalNames {
    claims: std::collections::HashMap<delightql_types::SqlIdentifier, QueryLocalClaim>,
    next_position: usize,
}

impl ToLispy for QueryLocalNames {
    fn to_lispy(&self) -> String {
        let mut claims = self
            .claims
            .iter()
            .map(|(name, claim)| {
                format!(
                    "(claim {} {:?} {})",
                    name.to_lispy(),
                    claim.kind,
                    claim.first_position
                )
            })
            .collect::<Vec<_>>();
        claims.sort();
        format!("(query_local_names {})", claims.join(" "))
    }
}

impl QueryLocalNames {
    pub(crate) fn declare(
        &mut self,
        name: delightql_types::SqlIdentifier,
        kind: QueryLocalKind,
    ) -> crate::error::Result<LexicalHorizon> {
        let position = self.next_position;
        self.next_position += 1;
        match self.claims.get(&name) {
            Some(claim) if claim.kind != kind => {
                return Err(crate::pipeline::bindings::one_query_local_name(&name));
            }
            Some(_) => {}
            None => {
                self.claims.insert(
                    name,
                    QueryLocalClaim {
                        kind,
                        first_position: position,
                    },
                );
            }
        }
        Ok(LexicalHorizon::through(position))
    }

    /// Whether this block CLAIMS a spelling at all — the question a reader
    /// asks when it needs to know that a mention is lexical rather than a
    /// reference to something outside the query, and has no position to
    /// judge visibility from.
    pub fn declares(&self, name: &delightql_types::SqlIdentifier) -> bool {
        self.claims.contains_key(name)
    }

    /// The kind this block claims a spelling as, whatever position asks and
    /// wherever its horizon stands: a claim decides a bare mention even
    /// where resolution will refuse it for kind or visibility.
    pub(crate) fn claim(&self, name: &delightql_types::SqlIdentifier) -> Option<QueryLocalKind> {
        self.claims.get(name).map(|claim| claim.kind)
    }

    pub(crate) fn judge(
        &self,
        name: &delightql_types::SqlIdentifier,
        horizon: LexicalHorizon,
        demand: QueryLocalDemand,
    ) -> QueryLocalJudgment {
        let Some(claim) = self.claims.get(name) else {
            return QueryLocalJudgment::Absent;
        };
        if !horizon.admits(claim.first_position) {
            return QueryLocalJudgment::NotYetVisible(claim.kind);
        }
        if demand.admits(claim.kind) {
            QueryLocalJudgment::Lawful(claim.kind)
        } else {
            QueryLocalJudgment::WrongKind(claim.kind)
        }
    }

    pub(crate) fn select(
        &self,
        name: &delightql_types::SqlIdentifier,
        horizon: LexicalHorizon,
        demand: QueryLocalDemand,
    ) -> crate::error::Result<Option<QueryLocalKind>> {
        match self.judge(name, horizon, demand) {
            QueryLocalJudgment::Lawful(kind) => Ok(Some(kind)),
            QueryLocalJudgment::Absent => Ok(None),
            QueryLocalJudgment::WrongKind(kind) => {
                Err(query_local_position_refusal(name, kind, demand, false))
            }
            QueryLocalJudgment::NotYetVisible(kind) => {
                Err(query_local_position_refusal(name, kind, demand, true))
            }
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.claims.is_empty()
    }
}

fn query_local_position_refusal(
    name: &delightql_types::SqlIdentifier,
    kind: QueryLocalKind,
    demand: QueryLocalDemand,
    later: bool,
) -> crate::error::DelightQLError {
    let reason = if later {
        format!(
            "the query-local {} '{name}' is declared after this body's lexical horizon",
            kind.description()
        )
    } else {
        format!(
            "query-local name '{name}' denotes a {}, which is not lawful in {}",
            kind.description(),
            demand.description()
        )
    };
    DelightQLError::from(Resolution::CallableUnknown {
        message: format!(
            "{reason}. A claimed query-local name never falls through to a consulted, catalog, or target definition"
        ),
    })
}

/// A COMMON HIGHER-ORDER EXPRESSION — the query-scoped parameterized rule,
/// the third common expression beside the CTE and the CFE: the consulted
/// `ho_rule`'s (or `effect_rule`'s) head with the SHADOW-NECK. Its clauses
/// are ONE assembled definition group, judged by the same assembler every
/// consulted definition crosses (one subject, one arity, head agreement),
/// and its body is the query's own text: a pure clause holds its body
/// DEFERRED — the authored characters, normalized again at each use with
/// that use's bindings, exactly as a parameterized consulted body is — and
/// an effect clause holds its normalized chain, bound at the demand walk
/// as an effect rule's is. The resolver SPENDS the definition at its call
/// sites; it never enters the catalog and never survives the query.
#[derive(Debug, Clone)]
pub struct HoDefinition {
    /// The subject AS AUTHORED, bare — the `!` of an effect mirror is the
    /// effect declaration's, not the name's.
    name: delightql_types::SqlIdentifier,
    effect: CteEffectDeclaration,
    group: crate::pipeline::asts::ddl::DefinitionGroup,
    horizon: LexicalHorizon,
}

/// A COMMON SIGMA EXPRESSION — the query-scoped truth-rule family. Its
/// clauses are assembled by the same definition-group authority as a
/// consulted sigma family, while the stamped horizon keeps its body in the
/// query-local world that declared it.
#[derive(Debug, Clone)]
pub struct SigmaDefinition {
    name: delightql_types::SqlIdentifier,
    group: crate::pipeline::asts::ddl::DefinitionGroup,
    horizon: LexicalHorizon,
}

impl SigmaDefinition {
    /// THE ONE DOOR: repeated local sigma clauses become one disjoining
    /// definition group, and the first clause's horizon governs every body.
    pub fn assemble(
        name: delightql_types::SqlIdentifier,
        family: crate::pipeline::asts::ddl::ClauseFamily,
        horizon: LexicalHorizon,
    ) -> crate::error::Result<Self> {
        let group = crate::pipeline::asts::ddl::DefinitionGroup::assemble(family)?;
        if group.kind() != crate::pipeline::asts::ddl::DefKind::Sigma {
            return Err(Internal::invariant(
                "query_local_sigma",
                format!(
                    "a common sigma expression '{}' assembled a non-sigma definition",
                    name
                ),
            ));
        }
        Ok(Self {
            name,
            group,
            horizon,
        })
    }

    pub fn name(&self) -> &delightql_types::SqlIdentifier {
        &self.name
    }

    pub fn group(&self) -> &crate::pipeline::asts::ddl::DefinitionGroup {
        &self.group
    }

    pub fn horizon(&self) -> LexicalHorizon {
        self.horizon
    }
}

impl PartialEq for SigmaDefinition {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.horizon == other.horizon
            && self.group.clauses().len() == other.group.clauses().len()
            && self
                .group
                .clauses()
                .iter()
                .zip(other.group.clauses())
                .all(|(a, b)| a.full_source == b.full_source)
    }
}

impl ToLispy for SigmaDefinition {
    fn to_lispy(&self) -> String {
        let clauses = self
            .group
            .clauses()
            .iter()
            .map(|clause| format!("{:?}", clause.full_source))
            .collect::<Vec<_>>()
            .join(" ");
        format!(
            "(sigma_definition (name {}) (horizon {}) (clauses {}))",
            self.name.to_lispy(),
            self.horizon.to_lispy(),
            clauses
        )
    }
}

impl HoDefinition {
    /// THE ONE DOOR: one subject's clauses, in authored order, assembled by
    /// the assembler every consulted definition crosses, so a head law or an
    /// effect-body law refuses under its own identity on either neck.
    pub fn assemble(
        name: delightql_types::SqlIdentifier,
        effect: CteEffectDeclaration,
        family: crate::pipeline::asts::ddl::ClauseFamily,
        horizon: LexicalHorizon,
    ) -> crate::error::Result<Self> {
        let group = crate::pipeline::asts::ddl::DefinitionGroup::assemble(family)?;
        // THE NAME AND KIND ARE THE CLAUSES' OWN: a pure CHOE's clauses are
        // parameterized views under its subject; an effect mirror's are
        // effect rules under the marked subject `name!`.
        let (kind, subject) = match effect {
            CteEffectDeclaration::Pure => {
                (crate::pipeline::asts::ddl::DefKind::HoView, name.clone())
            }
            CteEffectDeclaration::DemandsDirective => (
                crate::pipeline::asts::ddl::DefKind::Effect,
                delightql_types::SqlIdentifier::new(format!("{}!", name.as_str())),
            ),
        };
        if group.kind() != kind || group.name_identifier() != Some(&subject) {
            return Err(Internal::invariant(
                "query_local_choe",
                format!(
                    "a common higher-order expression '{name}' assembled clauses of {:?} '{}'",
                    group.kind(),
                    group.name()
                ),
            ));
        }
        Ok(HoDefinition {
            name,
            effect,
            group,
            horizon,
        })
    }

    pub fn name(&self) -> &delightql_types::SqlIdentifier {
        &self.name
    }

    pub fn effect(&self) -> CteEffectDeclaration {
        self.effect
    }

    /// Whether the definition is the effect mirror: a query-local
    /// parameterized EFFECT rule, opened only by its demand.
    pub fn declares_effect(&self) -> bool {
        self.effect == CteEffectDeclaration::DemandsDirective
    }

    pub fn group(&self) -> &crate::pipeline::asts::ddl::DefinitionGroup {
        &self.group
    }

    pub fn horizon(&self) -> &LexicalHorizon {
        &self.horizon
    }
}

/// Two definitions are the same definition when they are the same
/// authored text under the same name: the assembled group is derived from
/// the clauses' characters, and the horizon from the block around them.
impl PartialEq for HoDefinition {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.effect == other.effect
            && self.horizon == other.horizon
            && self.group.clauses().len() == other.group.clauses().len()
            && self
                .group
                .clauses()
                .iter()
                .zip(other.group.clauses())
                .all(|(a, b)| a.full_source == b.full_source)
    }
}

impl ToLispy for HoDefinition {
    fn to_lispy(&self) -> String {
        let clauses = self
            .group
            .clauses()
            .iter()
            .map(|clause| format!("{:?}", clause.full_source))
            .collect::<Vec<_>>()
            .join(" ");
        format!(
            "(ho_definition (name {}) {} (clauses {}))",
            self.name.to_lispy(),
            self.effect.to_lispy(),
            clauses
        )
    }
}

/// EVERY QUERY-LOCAL BINDING OF ONE QUERY, WITH THE NAME FACT THAT
/// GOVERNS THEM — one value, minted together and moved together.
///
/// The ledger's positions ARE the horizons stamped on these bindings: the
/// act that records a claim is the act that hands the binding it governs
/// its declaration horizon, and no constructor takes a ledger beside
/// manifestations. So a block cannot be assembled from a ledger and a set
/// of bindings that were separately chosen, and there is nothing to infer
/// later from the per-kind collections — inference cannot recover authored
/// interleaving, so a rebuilt position is free to disagree with the
/// horizon the authored position minted.
///
/// A block with no claim carries no authored binding to claim: that is a
/// compiler-built query, said by [`QueryLocals::none`] — never by an empty
/// ledger a later phase would read as a request to reconstruct one.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryLocals<P: Phase = Unresolved> {
    pub(crate) clause_formals: super::definitions::ClauseFormals,
    names: P::QueryLocalNames,
    cfes: P::CfeBindings,
    hos: P::HoBindings,
    sigmas: P::SigmaBindings,
    ctes: Vec<CteBinding<P>>,
}

impl<P: Phase> QueryLocals<P> {
    /// NO QUERY-LOCAL BINDING OF ANY KIND, and therefore no claim.
    pub fn none() -> Self {
        QueryLocals {
            clause_formals: Default::default(),
            names: P::no_query_local_names(),
            cfes: P::no_cfe_bindings(),
            hos: P::no_ho_bindings(),
            sigmas: P::no_sigma_bindings(),
            ctes: Vec::new(),
        }
    }

    /// The CTE bindings, one ordered collection in authored order.
    pub fn ctes(&self) -> &[CteBinding<P>] {
        &self.ctes
    }

    /// The query-scoped CFE definitions this phase still holds — empty
    /// where the phase has spent them.
    pub fn cfes(&self) -> &[CfeDefinition] {
        P::cfe_bindings(&self.cfes)
    }

    /// The query-scoped CHOE definitions this phase still holds, under the
    /// same law.
    pub fn hos(&self) -> &[HoDefinition] {
        P::ho_bindings(&self.hos)
    }

    /// The query-scoped sigma families this phase still holds — empty where
    /// the phase has spent them.
    pub fn sigmas(&self) -> &[SigmaDefinition] {
        P::sigma_bindings(&self.sigmas)
    }

    /// The one name/visibility fact for every binding above.
    pub(crate) fn names(&self) -> &P::QueryLocalNames {
        &self.names
    }

    /// Whether the block binds nothing at all.
    pub fn is_empty(&self) -> bool {
        self.ctes.is_empty()
            && P::query_local_names_is_empty(&self.names)
            && P::cfe_bindings(&self.cfes).is_empty()
            && P::ho_bindings(&self.hos).is_empty()
            && P::sigma_bindings(&self.sigmas).is_empty()
    }

    /// CROSS A PHASE BOUNDARY AS ONE BLOCK. Whether the claims and the
    /// definitions survive is the phases' decision; the walker is handed
    /// the CTE bindings and nothing else, so it has no hook for pairing a
    /// ledger with manifestations it did not govern.
    pub(crate) fn crossed<Q, F>(self, walk: &mut F) -> crate::error::Result<QueryLocals<Q>>
    where
        Q: Phase,
        F: crate::pipeline::ast_transform::AstTransform<P, Q> + ?Sized,
    {
        Ok(QueryLocals {
            clause_formals: self.clause_formals,
            names: super::phases::carry_query_local_names::<P, Q>(self.names)?,
            cfes: super::phases::carry_cfe_bindings::<P, Q>(self.cfes)?,
            hos: super::phases::carry_ho_bindings::<P, Q>(self.hos)?,
            sigmas: super::phases::carry_sigma_bindings::<P, Q>(self.sigmas)?,
            ctes: self
                .ctes
                .into_iter()
                .map(|cte| cte.folded(walk))
                .collect::<crate::error::Result<Vec<_>>>()?,
        })
    }
}

/// A phase that has SPENT its query-local definitions holds no slot for a
/// claim, a CFE or a CHOE. Its CTE bindings answer to nothing, so they are
/// admitted and rearranged freely: there is no ledger left to disagree with.
impl<P> QueryLocals<P>
where
    P: Phase<QueryLocalNames = (), CfeBindings = (), HoBindings = (), SigmaBindings = ()>,
{
    pub fn spent(ctes: Vec<CteBinding<P>>) -> Self {
        QueryLocals {
            clause_formals: Default::default(),
            names: (),
            cfes: (),
            hos: (),
            sigmas: (),
            ctes,
        }
    }

    pub fn ctes_mut(&mut self) -> &mut Vec<CteBinding<P>> {
        &mut self.ctes
    }

    pub fn into_ctes(self) -> Vec<CteBinding<P>> {
        self.ctes
    }
}

/// The blocks the effect road reshapes. Both doors are the block's own:
/// the claims are never lifted off the manifestations they answer for.
impl QueryLocals<Unresolved> {
    /// HAND EVERY RELATION BINDING TO ONE AUTHORITY and take back what it
    /// returns, IN ORDER — a pass that spends heads or rewrites bodies
    /// without touching what any binding is called. The door refuses a
    /// result that is not the same authored subjects in the same order, so
    /// nothing is added, dropped or reordered and the ledger still answers
    /// for exactly these manifestations.
    pub(crate) fn restate_ctes(
        &mut self,
        spend: impl FnOnce(
            Vec<CteBinding<Unresolved>>,
        ) -> crate::error::Result<Vec<CteBinding<Unresolved>>>,
    ) -> crate::error::Result<()> {
        let subjects = |bindings: &[CteBinding<Unresolved>]| {
            bindings
                .iter()
                .map(|cte| cte.subject().authored_name().cloned())
                .collect::<Vec<_>>()
        };
        let standing = subjects(&self.ctes);
        let spent = spend(std::mem::take(&mut self.ctes))?;
        if subjects(&spent) != standing {
            return Err(Internal::invariant(
                "query_local_block",
                "spending the heads of a query-local block's relation bindings answered with \
                 different subjects: the block's claims answer for the bindings it was minted \
                 with, and a replacement list is a second authority beside them",
            ));
        }
        self.ctes = spent;
        Ok(())
    }

    /// RESTATE THE RELATION BINDINGS ONE AT A TIME, each handed THIS BLOCK
    /// carrying only the bindings already restated — the scope a binding's
    /// own body stands in when a pass rewrites the bindings in order.
    ///
    /// The claims never move: which names the query declares is the
    /// block's fact, not a function of how far the pass has got, so a name
    /// declared later is still refused there rather than falling through
    /// to an outer definition. As with [`Self::restate_ctes`], a binding
    /// that comes back under another subject refuses.
    pub(crate) fn restate_ctes_in_order(
        &mut self,
        mut restate: impl FnMut(
            CteBinding<Unresolved>,
            &QueryLocals<Unresolved>,
        ) -> crate::error::Result<CteBinding<Unresolved>>,
    ) -> crate::error::Result<()> {
        let standing = std::mem::take(&mut self.ctes);
        let mut reached = QueryLocals {
            clause_formals: Default::default(),
            names: self.names.clone(),
            cfes: self.cfes.clone(),
            hos: self.hos.clone(),
            sigmas: self.sigmas.clone(),
            ctes: Vec::new(),
        };
        for cte in standing {
            let subject = cte.subject().authored_name().cloned();
            let restated = restate(cte, &reached)?;
            if restated.subject().authored_name().cloned() != subject {
                return Err(Internal::invariant(
                    "query_local_block",
                    "restating a query-local block's relation binding answered with a \
                     different subject: the block's claims answer for the binding it was \
                     minted with",
                ));
            }
            reached.ctes.push(restated);
        }
        self.ctes = reached.ctes;
        Ok(())
    }

}

/// One CHOE's clauses as the block reads them: every clause admitted under
/// one claimed name, in authored order, under the horizon the block stood at
/// when the FIRST clause was written. A query-local definition is its claim,
/// so the claim partitions the block's clauses; each partition is gathered
/// into its one family when the block seals.
#[derive(Debug)]
struct HoClauseGroup {
    name: delightql_types::SqlIdentifier,
    effect: CteEffectDeclaration,
    decls: Vec<crate::pipeline::asts::ddl::ClauseDecl>,
    horizon: LexicalHorizon,
}

/// One local sigma family's clauses, gathered in authored order. A repeated
/// subject is one truth family, so its clauses are later assembled and
/// disjoined together rather than selected as a value-function fallback.
#[derive(Debug)]
struct SigmaClauseGroup {
    name: delightql_types::SqlIdentifier,
    decls: Vec<crate::pipeline::asts::ddl::ClauseDecl>,
    horizon: LexicalHorizon,
}

/// The one family a claim's clauses gather into. A group exists because a
/// clause was admitted under its claim, so it is never empty.
fn claimed_family(
    site: &str,
    decls: Vec<crate::pipeline::asts::ddl::ClauseDecl>,
) -> crate::error::Result<crate::pipeline::asts::ddl::ClauseFamily> {
    crate::pipeline::asts::ddl::ClauseFamily::gather_one(decls)?
        .ok_or_else(|| Internal::invariant(site, "a claimed group holds at least one clause"))
}

/// ONE AUTHORED CLAUSE of a query-scoped value function, as the normalizer
/// reads a `cfe` head: its signature, the guard its scalar parameters
/// stated (several guards conjoin — each filters its own argument, so the
/// clause fires only when all hold), and the body it computes. A clause is
/// not a definition: repeated heads under one name are ordered clauses of
/// ONE value function, and only the block that admits them assembles it.
#[derive(Debug, Clone, PartialEq)]
pub struct CfeClause {
    pub name: delightql_types::SqlIdentifier,
    pub formals: CfeFormals,
    pub context_mode: ContextMode,
    pub guard: Option<super::expressions::TruthExpression<Unresolved>>,
    pub body: DomainExpression<Unresolved>,
}

impl CfeClause {
    /// The clause's signature as the one head assembler reads it: each
    /// formal's role at its position, and the context capture. A guard is
    /// the clause's own filter, not part of the signature.
    fn head(&self) -> super::definitions::Head {
        super::definitions::Head::signature(
            self.formals
                .iter()
                .map(|formal| super::definitions::HoParam::Scalar {
                    name: formal.name.clone(),
                    guard: None,
                    callable: formal.role == CfeFormalRole::Callable,
                })
                .collect(),
        )
        .with_context(self.context_mode.clone())
    }
}

/// One value function's clauses as the block reads them: gathered by NAME
/// in authored order, each with the head it declared, under the positional
/// signature the first clause declared and the horizon the block stood at
/// when it was written.
#[derive(Debug)]
struct CfeClauseFamily {
    name: delightql_types::SqlIdentifier,
    formals: CfeFormals,
    context_mode: ContextMode,
    horizon: LexicalHorizon,
    /// Every clause's head, in authored order: nonempty by construction.
    heads: Vec<super::definitions::Head>,
    /// The clause that opened the family, then every clause that joined:
    /// nonempty by construction, in authored order.
    first: super::ClauseArm<Unresolved>,
    rest: Vec<super::ClauseArm<Unresolved>>,
}

impl CfeClauseFamily {
    /// A LATER CLAUSE JOINS THE FAMILY. What the clauses must agree on is
    /// judged over all of them at once, when the family is assembled.
    fn join(&mut self, clause: CfeClause) {
        self.heads.push(clause.head());
        self.rest.push(super::ClauseArm {
            formals: super::definitions::ClauseFormals::cfe(&clause.formals),
            guard: clause.guard,
            result: clause.body,
        });
    }

    /// THE FAMILY, ASSEMBLED. Its clauses' signatures travel with it, for
    /// the one judgment of a family's parameter row where it is declared.
    /// Its body is what the one family-body door denotes for its clauses —
    /// the same door a consulted value function's clauses cross, so a lone
    /// unguarded clause is its body, a lone guarded clause selects, and the
    /// ordered-clause law is judged once, here.
    fn assemble(self) -> crate::error::Result<CfeDefinition> {
        let CfeClauseFamily {
            name,
            formals,
            context_mode,
            horizon,
            heads,
            first,
            rest,
        } = self;
        let body = super::ClauseSelection::family_body(
            crate::pipeline::asts::vocabulary::Vec1::with_tail(first, rest),
        )
        .map_err(|fault| fault.refusal(name.as_str()))?;
        Ok(CfeDefinition {
            name,
            formals,
            context_mode,
            horizon,
            body,
            clause_signatures: ClauseSignatures(heads),
        })
    }
}

/// THE ONE DOOR A QUERY-LOCAL BLOCK IS MINTED THROUGH.
///
/// Bindings are admitted in the order they were written. Admitting one is
/// what claims its name, and the claim's position is what the binding's
/// horizon is stamped from — the caller supplies neither. A subject that
/// declares no authored spelling (a compiler-generated clause carrier, a
/// recursive frontier, a structural read) claims nothing and is admitted
/// claimless, which is the deliberate statement that no authored name
/// answers to it.
#[derive(Debug, Default)]
pub struct QueryLocalBlock {
    names: QueryLocalNames,
    families: Vec<CfeClauseFamily>,
    hos: Vec<HoDefinition>,
    sigmas: Vec<SigmaDefinition>,
    ctes: Vec<CteBinding<Unresolved>>,
    groups: Vec<HoClauseGroup>,
    sigma_groups: Vec<SigmaClauseGroup>,
}

impl QueryLocalBlock {
    /// Admit one relation binding. An authored subject claims its
    /// spelling and receives the block's declaration horizon; every other
    /// subject stands claimless.
    pub(crate) fn admit_relation(
        &mut self,
        binding: CteBinding<Unresolved>,
    ) -> crate::error::Result<()> {
        let binding = if let Some(name) = binding.subject().authored_name().cloned() {
            let kind = if binding.subject().declares_effect() {
                QueryLocalKind::EffectRelation
            } else {
                QueryLocalKind::Relation
            };
            let horizon = self.names.declare(name, kind)?;
            binding.with_horizon(horizon)
        } else {
            binding
        };
        self.ctes.push(binding);
        Ok(())
    }

    /// Admit one clause of a value function. Every clause claims the name;
    /// a repeated name joins the family the first clause opened, which
    /// keeps the horizon that clause minted — the family is ONE definition
    /// with one declaration site. Its clauses' agreement is judged when the
    /// block seals.
    pub(crate) fn admit_cfe(&mut self, clause: CfeClause) -> crate::error::Result<()> {
        let horizon = self
            .names
            .declare(clause.name.clone(), QueryLocalKind::Value)?;
        if let Some(family) = self
            .families
            .iter_mut()
            .find(|family| family.name == clause.name)
        {
            family.join(clause);
            return Ok(());
        }
        let head = clause.head();
        let CfeClause {
            name,
            formals,
            context_mode,
            guard,
            body,
        } = clause;
        let clause_formals = super::definitions::ClauseFormals::cfe(&formals);
        let formals = CfeFormals::in_binding_order(
            formals
                .iter()
                .enumerate()
                .map(|(position, formal)| CfeFormal {
                    name: super::definitions::argument_name(position),
                    role: formal.role,
                })
                .collect(),
        )?;
        self.families.push(CfeClauseFamily {
            name,
            formals,
            context_mode,
            horizon,
            heads: vec![head],
            first: super::ClauseArm {
                formals: clause_formals,
                guard,
                result: body,
            },
            rest: Vec::new(),
        });
        Ok(())
    }

    /// Admit one clause of a parameterized definition. Every clause claims
    /// the subject; the group keeps the horizon the first one minted,
    /// because that is where the subject became visible.
    pub(crate) fn admit_ho_clause(
        &mut self,
        name: delightql_types::SqlIdentifier,
        effect: CteEffectDeclaration,
        decl: crate::pipeline::asts::ddl::ClauseDecl,
    ) -> crate::error::Result<()> {
        let kind = if effect == CteEffectDeclaration::DemandsDirective {
            QueryLocalKind::EffectHigherOrder
        } else {
            QueryLocalKind::HigherOrder
        };
        let horizon = self.names.declare(name.clone(), kind)?;
        if let Some(group) = self.groups.iter_mut().find(|group| group.name == name) {
            group.decls.push(decl);
            return Ok(());
        }
        self.groups.push(HoClauseGroup {
            horizon,
            name,
            effect,
            decls: vec![decl],
        });
        Ok(())
    }

    /// Admit one truth-rule clause. Repeated clauses join one sigma family;
    /// the definition group, not clause order, supplies its disjunction.
    pub(crate) fn admit_sigma_clause(
        &mut self,
        name: delightql_types::SqlIdentifier,
        decl: crate::pipeline::asts::ddl::ClauseDecl,
    ) -> crate::error::Result<()> {
        let horizon = self.names.declare(name.clone(), QueryLocalKind::Sigma)?;
        if let Some(group) = self
            .sigma_groups
            .iter_mut()
            .find(|group| group.name == name)
        {
            group.decls.push(decl);
            return Ok(());
        }
        self.sigma_groups.push(SigmaClauseGroup {
            name,
            decls: vec![decl],
            horizon,
        });
        Ok(())
    }

    /// The finished block: every clause family assembled into its one
    /// definition, in the order the families opened.
    pub(crate) fn seal(self) -> crate::error::Result<QueryLocals<Unresolved>> {
        let QueryLocalBlock {
            names,
            families,
            mut hos,
            mut sigmas,
            ctes,
            groups,
            sigma_groups,
        } = self;
        let cfes = families
            .into_iter()
            .map(CfeClauseFamily::assemble)
            .collect::<crate::error::Result<Vec<_>>>()?;
        for group in groups {
            hos.push(HoDefinition::assemble(
                group.name,
                group.effect,
                claimed_family("query_local_choe", group.decls)?,
                group.horizon,
            )?);
        }
        for group in sigma_groups {
            sigmas.push(SigmaDefinition::assemble(
                group.name,
                claimed_family("query_local_sigma", group.decls)?,
                group.horizon,
            )?);
        }
        Ok(QueryLocals {
            clause_formals: Default::default(),
            names,
            cfes,
            hos,
            sigmas,
            ctes,
        })
    }
}

/// The root of any DelightQL query: ONE query-local block and ONE body.
///
/// The block is a field, not a wrapper — a query cannot nest a second
/// query around itself to smuggle ordering or ownership, and every
/// consumer handles the one shape instead of selecting among carriers.
/// Compiler-built and authored queries use this same carrier.
#[derive(Debug, Clone, PartialEq)]
pub struct Query<P: Phase = Unresolved> {
    /// Every query-local binding and the one name fact that governs them.
    pub locals: QueryLocals<P>,
    /// The one body the query runs.
    pub body: Chain<P>,
}

impl<P: Phase> ToLispy for Query<P> {
    fn to_lispy(&self) -> String {
        format!(
            "(query (local_names {}) (cfes {}) (hos {}) (sigmas {}) (ctes {}) (body {}))",
            self.locals.names.to_lispy(),
            self.locals.cfes.to_lispy(),
            self.locals.hos.to_lispy(),
            self.locals.sigmas.to_lispy(),
            self.locals.ctes.to_lispy(),
            self.body.to_lispy(),
        )
    }
}

impl Query<Unresolved> {
    /// The one name fact governing this query's own bindings — the claims
    /// that make a mention of one of them lexical rather than a reference
    /// to something the query does not declare.
    pub fn local_names(&self) -> &QueryLocalNames {
        self.locals.names()
    }
}

impl<P: Phase> Query<P> {
    /// A bare body: no bindings of any kind.
    pub fn relational(body: Chain<P>) -> Self {
        Query {
            locals: QueryLocals::none(),
            body,
        }
    }

    /// A body under one already-minted block.
    pub fn binding(locals: QueryLocals<P>, body: Chain<P>) -> Self {
        Query { locals, body }
    }

    /// Whether the query is its body alone — no binding of any kind.
    pub fn is_bare(&self) -> bool {
        self.locals.is_empty()
    }

    /// The CTE bindings, one ordered collection in authored order.
    pub fn ctes(&self) -> &[CteBinding<P>] {
        self.locals.ctes()
    }

    /// The query-scoped CFE definitions this phase still holds.
    pub fn cfes(&self) -> &[CfeDefinition] {
        self.locals.cfes()
    }

    /// The query-scoped CHOE definitions this phase still holds.
    pub fn hos(&self) -> &[HoDefinition] {
        self.locals.hos()
    }

    /// The query-scoped sigma families this phase still holds.
    pub fn sigmas(&self) -> &[SigmaDefinition] {
        self.locals.sigmas()
    }

    /// The body of a query that carries no bindings; the caller keeps the
    /// whole query otherwise.
    pub fn into_bare_body(self) -> std::result::Result<Chain<P>, Box<Query<P>>> {
        if self.is_bare() {
            Ok(self.body)
        } else {
            Err(Box::new(self))
        }
    }
}

/// What a CTE label DECLARES about its expression's relationship to effects.
///
/// A bare label asserts a pure expression; a `!`-marked label asserts the
/// expression demands a directive. The declaration is an assertion, never a
/// coercion: the effect authority judges it against the body once — a mark
/// on a pure body refuses (`effect/cte/pure_mark`) before a marked binding
/// can be constructed, and an effectful body under a bare label has no
/// derivation, so the parse refuses it (`parse/effect/label`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, ToLispy)]
pub enum CteEffectDeclaration {
    /// A bare label: the expression is asserted pure.
    #[lispy("effect:pure")]
    Pure,
    /// A `!`-marked label or head: the expression is asserted to demand a
    /// directive. Read from the CST's `effect_marker` field by the builder
    /// (pinned by `effect_cte_marker_is_read_by_builder`).
    #[lispy("effect:demands_directive")]
    DemandsDirective,
}

/// The subject a CTE binding stands on before resolution spends it.
///
/// One closed carrier: authored bindings keep authored spelling and effect
/// declaration; generated bindings carry only compiler spelling; recursive
/// frontiers add the exact open definition instance; structural bindings
/// stand directly on their pending carrier. No case can borrow evidence from
/// another.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum AuthoredCteSubject {
    /// An authored label or head: `expression : name`, `name(*) : body`.
    #[lispy("subject:authored")]
    Authored {
        /// The spelling as written. Grouping, registration, and duplicate
        /// judgment compare by the identifier law — an unstropped spelling
        /// folds, a stropped one keeps its authored bytes.
        #[lispy("name")]
        name: delightql_types::SqlIdentifier,
        /// The label's effect declaration.
        #[lispy("effect")]
        effect: CteEffectDeclaration,
    },
    /// A compiler-built binding that must still ANSWER TO A NAME, because
    /// its equally generated body references it by this spelling. The
    /// spelling is the compiler's — no author wrote it — so it carries no
    /// effect declaration (a compiler asserts nothing about effects) and
    /// grouping never merges it with an authored clause set.
    #[lispy("subject:generated")]
    Generated {
        /// The generated spelling the generated body references.
        #[lispy("name")]
        name: delightql_types::SqlIdentifier,
    },
}

/// The harmless subject facts generic unresolved-tree consumers may inspect.
/// The frontier arm deliberately carries no evidence.
pub enum CteSubjectView<'a> {
    Authored {
        name: &'a delightql_types::SqlIdentifier,
        effect: &'a CteEffectDeclaration,
    },
    Generated {
        name: &'a delightql_types::SqlIdentifier,
    },
    Frontier,
}

impl<'a> CteSubjectView<'a> {
    pub fn authored_name(self) -> Option<&'a delightql_types::SqlIdentifier> {
        match self {
            CteSubjectView::Authored { name, .. } => Some(name),
            CteSubjectView::Generated { .. } | CteSubjectView::Frontier => None,
        }
    }

    pub fn declares_effect(self) -> bool {
        matches!(
            self,
            CteSubjectView::Authored {
                effect: CteEffectDeclaration::DemandsDirective,
                ..
            }
        )
    }
}

#[cfg(test)]
mod query_local_name_tests {
    use super::{QueryLocalDemand, QueryLocalJudgment, QueryLocalKind, QueryLocalNames};
    use delightql_types::SqlIdentifier;

    #[test]
    fn one_fact_exhaustively_distinguishes_selection_outcomes() {
        let mut names = QueryLocalNames::default();
        let earlier = names
            .declare(SqlIdentifier::new("earlier"), QueryLocalKind::Relation)
            .expect("first declaration");
        names
            .declare(
                SqlIdentifier::new("later"),
                QueryLocalKind::EffectHigherOrder,
            )
            .expect("second declaration");

        assert_eq!(
            names.judge(
                &SqlIdentifier::new("earlier"),
                earlier,
                QueryLocalDemand::Relation,
            ),
            QueryLocalJudgment::Lawful(QueryLocalKind::Relation)
        );
        assert_eq!(
            names.judge(
                &SqlIdentifier::new("earlier"),
                earlier,
                QueryLocalDemand::Effect,
            ),
            QueryLocalJudgment::WrongKind(QueryLocalKind::Relation)
        );
        assert_eq!(
            names.judge(
                &SqlIdentifier::new("later"),
                earlier,
                QueryLocalDemand::Effect,
            ),
            QueryLocalJudgment::NotYetVisible(QueryLocalKind::EffectHigherOrder)
        );
        assert_eq!(
            names.judge(
                &SqlIdentifier::new("absent"),
                earlier,
                QueryLocalDemand::HigherOrder,
            ),
            QueryLocalJudgment::Absent
        );
    }
}

#[cfg(test)]
mod query_local_block_tests {
    use super::{
        AuthoredCteSubject, CfeClause, CfeFormals, ContextMode, CteAuthority, CteBinding,
        CteEffectDeclaration, DomainExpression, LexicalHorizon, QueryLocalBlock, QueryLocalDemand,
        QueryLocalJudgment, QueryLocalKind, Unresolved,
    };
    use delightql_types::SqlIdentifier;

    fn cfe(name: &str) -> CfeClause {
        guarded_cfe(name, None)
    }

    fn cfe_with(name: &str, formals: CfeFormals) -> CfeClause {
        CfeClause {
            formals,
            ..guarded_cfe(name, None)
        }
    }

    fn guarded_cfe(
        name: &str,
        guard: Option<super::super::expressions::TruthExpression<Unresolved>>,
    ) -> CfeClause {
        CfeClause {
            name: SqlIdentifier::new(name),
            formals: CfeFormals::from_role_groups([], []),
            context_mode: ContextMode::None,
            guard,
            body: DomainExpression::Application(super::super::FunctionApplication::Ground(
                super::super::LiteralValue::Null,
            )),
        }
    }

    /// A guard for tests: `null = null`, one comparison, no reads.
    fn a_guard() -> super::super::expressions::TruthExpression<Unresolved> {
        let null = || {
            DomainExpression::Application(super::super::FunctionApplication::Ground(
                super::super::LiteralValue::Null,
            ))
        };
        super::super::expressions::TruthExpression::Comparison(
            super::super::expressions::Comparison {
                operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
                left: Box::new(null()),
                right: Box::new(null()),
            },
        )
    }

    /// REPEATED HEADS UNDER ONE NAME ARE ONE FAMILY: the block seals one
    /// definition, its body the ordered selection over the clauses, under
    /// the horizon the FIRST clause minted.
    #[test]
    fn repeated_cfe_heads_seal_as_one_ordered_family() {
        let mut block = QueryLocalBlock::default();
        block
            .admit_cfe(guarded_cfe("f", Some(a_guard())))
            .expect("guarded clause");
        block.admit_relation(authored("mid")).expect("mid CTE");
        block.admit_cfe(cfe("f")).expect("fallback clause");
        let locals = block.seal().expect("the block seals");

        assert_eq!(locals.cfes().len(), 1, "one family, not two definitions");
        let family = &locals.cfes()[0];
        match &family.body {
            DomainExpression::Application(super::super::FunctionApplication::ClauseSelection(
                selection,
            )) => {
                assert_eq!(selection.arms().len(), 2);
                assert!(selection.arms().first().guard.is_some());
                assert!(selection.arms().get(1).expect("two arms").guard.is_none());
            }
            other => panic!("expected the ordered selection, got {other:?}"),
        }
        // The family's horizon is the first clause's: `mid`, declared
        // between the clauses, is not visible from the family's body.
        assert_eq!(
            locals.names().judge(
                &SqlIdentifier::new("mid"),
                family.horizon(),
                QueryLocalDemand::Relation
            ),
            QueryLocalJudgment::NotYetVisible(QueryLocalKind::Relation)
        );
    }

    /// One unguarded clause is that clause's body outright; a guarded
    /// clause alone still selects, because its guard may not hold.
    #[test]
    fn a_lone_clause_selects_only_when_guarded() {
        let mut block = QueryLocalBlock::default();
        block.admit_cfe(cfe("plain")).expect("plain clause");
        block
            .admit_cfe(guarded_cfe("guarded", Some(a_guard())))
            .expect("guarded clause");
        let locals = block.seal().expect("the block seals");
        assert!(matches!(
            locals.cfes()[0].body,
            DomainExpression::Application(super::super::FunctionApplication::Ground(_))
        ));
        assert!(matches!(
            locals.cfes()[1].body,
            DomainExpression::Application(super::super::FunctionApplication::ClauseSelection(_))
        ));
    }

    /// THE ORDERED-CLAUSE LAW REACHES THE FAMILY: two unguarded clauses,
    /// or an unguarded clause before a guarded one, refuse at the seal
    /// under the `ddl/head` leaf naming the rule, as a consulted family
    /// does.
    #[test]
    fn a_family_refuses_a_misplaced_or_repeated_fallback() {
        let mut block = QueryLocalBlock::default();
        block.admit_cfe(cfe("f")).expect("first unguarded");
        block.admit_cfe(cfe("f")).expect("second unguarded admits");
        let err = block.seal().expect_err("two fallbacks refuse");
        assert_eq!(
            err.error_uri(),
            "delightql-error://semantic/ddl/head/unguarded_multiplicity"
        );
        assert!(
            format!("{err}").contains("found 2 unguarded clauses"),
            "{err}"
        );

        let mut block = QueryLocalBlock::default();
        block.admit_cfe(cfe("g")).expect("fallback first");
        block
            .admit_cfe(guarded_cfe("g", Some(a_guard())))
            .expect("guarded after admits");
        let err = block.seal().expect_err("a fallback before a guard refuses");
        assert_eq!(
            err.error_uri(),
            "delightql-error://semantic/ddl/head/unguarded_position"
        );
        assert!(
            format!("{err}").contains("unguarded clause is at position 1"),
            "{err}"
        );
    }

    /// A family's clauses carry their signatures out of the seal, each
    /// clause's own, for the one judgment of the parameter row where the
    /// family is declared; scalar spellings are each clause's own.
    #[test]
    fn a_family_carries_its_clause_signatures() {
        let mut block = QueryLocalBlock::default();
        block
            .admit_cfe(CfeClause {
                guard: Some(a_guard()),
                ..cfe_with("f", CfeFormals::from_role_groups([], [SqlIdentifier::new("x")]))
            })
            .expect("a clause admits");
        block
            .admit_cfe(cfe_with(
                "f",
                CfeFormals::from_role_groups([], [SqlIdentifier::new("x"), SqlIdentifier::new("y")]),
            ))
            .expect("a clause admits");
        let locals = block.seal().expect("the seal judges no parameter row");
        assert_eq!(locals.cfes()[0].clause_signatures.0.len(), 2);

        let mut block = QueryLocalBlock::default();
        block
            .admit_cfe(CfeClause {
                guard: Some(a_guard()),
                ..cfe_with(
                    "g",
                    CfeFormals::from_role_groups([], [SqlIdentifier::new("x")]),
                )
            })
            .expect("first clause");
        block
            .admit_cfe(cfe_with(
                "g",
                CfeFormals::from_role_groups([], [SqlIdentifier::new("y")]),
            ))
            .expect("second clause");
        block.seal().expect("scalar spelling is clause-local");
    }

    fn relation(subject: AuthoredCteSubject) -> CteBinding {
        CteBinding::authored(
            super::Chain::read(
                super::super::Relation::Ground {
                    mention: super::super::GroundMention::Named {
                        identifier: super::super::QualifiedName {
                            namespace_path: super::super::NamespacePath::empty(),
                            name: "t".into(),
                        },
                        alias: None,
                        mutation_target: false,
                        passthrough: false,
                    },
                },
                super::super::Access::All,
            ),
            subject,
            CteAuthority {
                horizon: LexicalHorizon::all(),
                head: super::super::definitions::Head::glob(),
                origin: super::super::provenance::CteOrigin::CompilerGenerated,
            },
        )
    }

    fn authored(name: &str) -> CteBinding {
        relation(AuthoredCteSubject::Authored {
            name: SqlIdentifier::new(name),
            effect: CteEffectDeclaration::Pure,
        })
    }

    /// ADMITTING A BINDING IS WHAT CLAIMS ITS NAME. The horizon a
    /// definition carries is the one its own claim's position minted, so a
    /// body reaches what was written before it and nothing after.
    #[test]
    fn a_minted_block_stamps_each_definition_at_its_own_claim() {
        let mut block = QueryLocalBlock::default();
        block.admit_cfe(cfe("early")).expect("early CFE");
        block.admit_relation(authored("mid")).expect("mid CTE");
        block.admit_cfe(cfe("late")).expect("late CFE");
        let locals = block.seal().expect("the block seals");

        let early = locals.cfes()[0].horizon();
        let late = locals.cfes()[1].horizon();
        let names = locals.names();
        assert_eq!(
            names.judge(
                &SqlIdentifier::new("mid"),
                early,
                QueryLocalDemand::Relation
            ),
            QueryLocalJudgment::NotYetVisible(QueryLocalKind::Relation)
        );
        assert_eq!(
            names.judge(&SqlIdentifier::new("mid"), late, QueryLocalDemand::Relation),
            QueryLocalJudgment::Lawful(QueryLocalKind::Relation)
        );
    }

    /// Restating the relation bindings is for spending heads and rewriting
    /// bodies. A pass that answers with a different subject list is a
    /// second authority beside the claims, and refuses.
    #[test]
    fn restating_refuses_a_different_subject_list() {
        let mut block = QueryLocalBlock::default();
        block.admit_relation(authored("kept")).expect("kept");
        let mut locals = block.seal().expect("the block seals");
        assert!(locals.clone().restate_ctes(Ok).is_ok());
        assert!(locals
            .restate_ctes(|_| Ok(vec![authored("other")]))
            .is_err());
    }
}

/// What resolution consumes of an authored CTE binding beyond its subject:
/// the head it groups and spends, and the two provenance judgments that pick
/// its naming hint and its resolution scope. SPENT WHOLE at resolution — the
/// phase system deletes the slot afterwards, so no bound phase can carry a
/// spent copy.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub struct CteAuthority {
    /// The query-local declarations visible where this clause body was
    /// authored. Compiler-built bindings use the unrestricted horizon.
    #[lispy("horizon")]
    pub horizon: LexicalHorizon,
    /// The authored head, in the SAME type the `:-` neck carries.
    /// A glob head passes the body's heading through; a listed head is the
    /// closed contract the one assembler enforces across the subject's
    /// clauses. `body : name` is `name(*) : body`, so the labeling
    /// shorthand and a compiler-built binding both glob. The head carries
    /// its badge unjudged: whether this binding is a fixpoint at all is not
    /// knowable until the self-reference binds.
    #[lispy("head")]
    pub head: crate::pipeline::asts::core::definitions::Head,
    /// TYPED provenance: who authored this CTE — set at CONSTRUCTION,
    /// never inferred from the name (a user may legally write `_ho_*`
    /// identifiers). The squished-weave scope override keys on this.
    #[lispy("origin")]
    pub origin: crate::pipeline::asts::core::provenance::CteOrigin,
}

pub use crate::pipeline::bindings::CteBinding;

impl crate::pipeline::asts::core::definitions::HeadedClause for CteBinding<Unresolved> {
    fn head(&self) -> &crate::pipeline::asts::core::definitions::Head {
        &self.authority().head
    }

    fn body_publishes_names(&self) -> bool {
        crate::pipeline::asts::core::definitions::chain_publishes_names(self.body())
    }

    fn spend_head(
        self,
        items: &[crate::pipeline::asts::core::definitions::HeadItem],
        canonical_names: &[delightql_types::SqlIdentifier],
    ) -> Self {
        self.projected_through_head(items, canonical_names)
    }
}

/// CFE (Common Function Expression) definition from parser
/// Example: double:(x) : (x * 2)
/// Context mode for CCAFE (Context-Aware CFE) support
/// Determines how a CFE handles column references beyond its declared parameters
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum ContextMode {
    /// Regular CFE - no context capture, parameters only
    /// Syntax: name:(params) : body
    /// Any non-parameter Lvar in body is an ERROR
    #[lispy("context:none")]
    None,

    /// Implicit context - auto-discover context params from body
    /// Syntax: name:(.., params) : body
    /// Free body names capture from the caller's row at the call site.
    /// Can only be called context-aware: name:(.., args)
    #[lispy("context:implicit")]
    Implicit,

    /// Explicit context - declared context params
    /// Syntax: name:(..{ctx1, ctx2}, params) : body
    /// Only declared context params + parameters allowed in body
    /// Can be called context-aware OR positionally: name:(.., args) or name:(ctx1, ctx2, args)
    #[lispy("context:explicit")]
    Explicit(Vec<delightql_types::SqlIdentifier>),
}

/// What a value definition's formal RECEIVES at a call site.
///
/// The role rides the formal itself, so no consumer reconstructs it from
/// which of two lists a name sat in, and a formal cannot land in a carrier
/// that disagrees with its role: a `Callable` formal fills the frame's
/// callables, a `Scalar` formal its values, and the payload types make the
/// other assignment unwritable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ToLispy)]
pub enum CfeFormalRole {
    /// A data parameter: the call site supplies a value.
    #[lispy("role:scalar")]
    Scalar,
    /// A curried code parameter (the first list of an HO-CFE): the call
    /// site supplies a callable — a mention, a lambda, or a template.
    #[lispy("role:callable")]
    Callable,
}

/// One declared formal of a value definition: the authored spelling — strop
/// bit intact, agreement by the identifier law — and its exact role.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub struct CfeFormal {
    #[lispy("name")]
    pub name: delightql_types::SqlIdentifier,
    #[lispy("role")]
    pub role: CfeFormalRole,
}

/// The declared formals of a value definition, in BINDING order: every
/// callable formal precedes every scalar one, because a call site supplies
/// code first.
///
/// The order is the TYPE's guarantee, not a producer convention: the inner
/// vector is private, `from_role_groups` cannot build a misordered value,
/// and `in_binding_order` refuses one in every build — so a role can never
/// disagree with the binding region a consumer reads it from, whoever the
/// next producer is.
#[derive(Debug, Clone, PartialEq)]
pub struct CfeFormals(Vec<CfeFormal>);

impl crate::lispy::ToLispy for CfeFormals {
    fn to_lispy(&self) -> String {
        self.0.to_lispy()
    }
}

impl CfeFormals {
    /// The infallible door: the two role groups, code first. A caller
    /// holding the groups separately cannot express a misordering.
    pub fn from_role_groups(
        callable: impl IntoIterator<Item = delightql_types::SqlIdentifier>,
        scalar: impl IntoIterator<Item = delightql_types::SqlIdentifier>,
    ) -> Self {
        Self(
            callable
                .into_iter()
                .map(|name| CfeFormal {
                    name,
                    role: CfeFormalRole::Callable,
                })
                .chain(scalar.into_iter().map(|name| CfeFormal {
                    name,
                    role: CfeFormalRole::Scalar,
                }))
                .collect(),
        )
    }

    /// The checked door for a caller holding one ordered list: refuses a
    /// callable formal standing after a scalar one, in every build.
    pub fn in_binding_order(formals: Vec<CfeFormal>) -> crate::error::Result<Self> {
        let mut scalar_seen = false;
        for formal in &formals {
            match formal.role {
                CfeFormalRole::Scalar => scalar_seen = true,
                CfeFormalRole::Callable if scalar_seen => {
                    return Err(DelightQLError::from(Constraint::General {
                        message: format!(
                            "the callable formal '{}' stands after a scalar one: a call \
                             site supplies code first, so binding order is \
                             callable-then-scalar",
                            formal.name
                        ),
                    }));
                }
                CfeFormalRole::Callable => {}
            }
        }
        Ok(Self(formals))
    }

    /// The formals split at the binding boundary: the callable prefix,
    /// then the scalar rest. Total, because the constructors are the only
    /// producers and both uphold the order.
    pub fn split(&self) -> (&[CfeFormal], &[CfeFormal]) {
        let boundary = self
            .0
            .iter()
            .position(|formal| formal.role == CfeFormalRole::Scalar)
            .unwrap_or(self.0.len());
        self.0.split_at(boundary)
    }

    /// The curried (code) formals: the leading `Callable` run.
    pub fn callable(&self) -> &[CfeFormal] {
        self.split().0
    }

    /// The data formals: everything after the callable prefix.
    pub fn scalar(&self) -> &[CfeFormal] {
        self.split().1
    }

    /// Every declared formal, in binding order.
    pub fn iter(&self) -> std::slice::Iter<'_, CfeFormal> {
        self.0.iter()
    }
}

/// Higher-order example: apply_transform:(transform)(value) : value >> transform:()
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub struct CfeDefinition {
    /// The name of the function, AS AUTHORED — the strop bit rides with the
    /// characters, so agreement is the identifier law's, not `==` on text.
    #[lispy("name")]
    pub name: delightql_types::SqlIdentifier,
    /// The declared formals, in the carrier whose type owns binding order.
    #[lispy("formals")]
    pub formals: CfeFormals,
    /// Context mode for CCAFE support
    #[lispy("context_mode")]
    pub context_mode: ContextMode,
    /// The declarations visible where this body was authored. STAMPED BY
    /// THE BLOCK THAT CLAIMED THE NAME, in the same act — a caller cannot
    /// choose a definition's horizon, so it cannot choose one the ledger
    /// does not answer for.
    #[lispy("horizon")]
    horizon: LexicalHorizon,
    /// What the body COMPUTES: the one value the rule denotes.
    #[lispy("body")]
    pub body: DomainExpression<Unresolved>,
    /// Each clause's signature, in authored order: what the family's
    /// clauses must agree on where it is declared.
    #[lispy("clause_signatures")]
    pub clause_signatures: ClauseSignatures,
}

/// A value function family's clause signatures, in authored order.
#[derive(Debug, Clone, PartialEq)]
pub struct ClauseSignatures(pub Vec<super::definitions::Head>);

impl ToLispy for ClauseSignatures {
    fn to_lispy(&self) -> String {
        format!("(clauses {})", self.0.len())
    }
}

impl CfeDefinition {
    /// An authored definition BEFORE any block claimed its name: it reaches
    /// every declaration until the block that admits it says which ones.
    pub fn unbounded(
        name: delightql_types::SqlIdentifier,
        formals: CfeFormals,
        context_mode: ContextMode,
        body: DomainExpression<Unresolved>,
    ) -> Self {
        let signature = super::definitions::Head::signature(
            formals
                .iter()
                .map(|formal| super::definitions::HoParam::Scalar {
                    name: formal.name.clone(),
                    guard: None,
                    callable: formal.role == CfeFormalRole::Callable,
                })
                .collect(),
        )
        .with_context(context_mode.clone());
        CfeDefinition {
            name,
            formals,
            context_mode,
            horizon: LexicalHorizon::all(),
            body,
            clause_signatures: ClauseSignatures(vec![signature]),
        }
    }

    /// The declarations visible where this body was authored.
    pub(crate) fn horizon(&self) -> LexicalHorizon {
        self.horizon
    }

    /// The formals split at the binding boundary.
    pub fn split_formals(&self) -> (&[CfeFormal], &[CfeFormal]) {
        self.formals.split()
    }

    /// The curried (code) formals: the leading `Callable` run.
    pub fn callable_formals(&self) -> &[CfeFormal] {
        self.formals.callable()
    }

    /// The data formals: everything after the callable prefix.
    pub fn scalar_formals(&self) -> &[CfeFormal] {
        self.formals.scalar()
    }
}

#[cfg(test)]
mod cfe_formal_tests {
    use super::{CfeFormal, CfeFormalRole, CfeFormals};
    use delightql_types::SqlIdentifier;

    fn formal(name: &str, role: CfeFormalRole) -> CfeFormal {
        CfeFormal {
            name: SqlIdentifier::new(name),
            role,
        }
    }

    /// The binding boundary splits the callable prefix from the scalar
    /// rest — the role rides each formal, so the split reads roles, never
    /// list membership.
    #[test]
    fn formals_split_at_the_binding_boundary() {
        let formals = CfeFormals::from_role_groups(
            [SqlIdentifier::new("f"), SqlIdentifier::new("g")],
            [SqlIdentifier::new("x")],
        );
        let (callable, scalar) = formals.split();
        assert_eq!(callable.len(), 2);
        assert_eq!(scalar.len(), 1);
        assert!(callable.iter().all(|f| f.role == CfeFormalRole::Callable));
        assert!(scalar.iter().all(|f| f.role == CfeFormalRole::Scalar));

        let none = CfeFormals::from_role_groups([], [SqlIdentifier::new("x")]);
        assert!(none.callable().is_empty());
        let all = CfeFormals::from_role_groups([SqlIdentifier::new("f")], []);
        assert!(all.scalar().is_empty());
    }

    /// A misordered list cannot become a carrier in ANY build: the checked
    /// door refuses a callable formal standing after a scalar one, and the
    /// group door cannot express the shape at all.
    #[test]
    fn a_callable_after_a_scalar_is_refused_at_construction() {
        let refused = CfeFormals::in_binding_order(vec![
            formal("x", CfeFormalRole::Scalar),
            formal("f", CfeFormalRole::Callable),
        ]);
        assert!(refused.is_err());

        let ordered = CfeFormals::in_binding_order(vec![
            formal("f", CfeFormalRole::Callable),
            formal("x", CfeFormalRole::Scalar),
        ])
        .expect("callable-then-scalar is the binding order");
        assert_eq!(ordered.callable().len(), 1);
        assert_eq!(ordered.scalar().len(), 1);
    }
}
