// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::{Chain, DomainExpression, Phase, Unresolved};
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

/// THE LEXICAL HORIZON of a query-scoped body: the last authored declaration
/// position the body may see. The query-local name authority assigns the
/// positions while it reads the block, so visibility is an ordering fact and
/// never reconstructed from whichever per-kind collection a consumer has.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LexicalHorizon(usize);

impl LexicalHorizon {
    pub(crate) fn through(position: usize) -> Self {
        LexicalHorizon(position)
    }

    pub(crate) fn all() -> Self {
        LexicalHorizon(usize::MAX)
    }

    pub(crate) fn is_all(self) -> bool {
        self.0 == usize::MAX
    }

    pub(crate) fn admits(&self, position: usize) -> bool {
        position <= self.0
    }

    pub(crate) fn contains(&self, declaration: LexicalHorizon) -> bool {
        declaration.0 <= self.0
    }

    /// This horizon read in a block whose positions all moved later by
    /// `offset`. A horizon reaching every declaration still reaches every
    /// declaration; a bounded one keeps exactly the run it bounded, because
    /// the claims it bounds moved the same distance.
    pub(crate) fn shifted(self, offset: usize) -> Self {
        if self.is_all() {
            self
        } else {
            LexicalHorizon(self.0 + offset)
        }
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
    EffectRelation,
    EffectHigherOrder,
}

impl QueryLocalKind {
    pub(crate) fn description(self) -> &'static str {
        match self {
            QueryLocalKind::Relation => "common table expression",
            QueryLocalKind::Value => "common function expression",
            QueryLocalKind::HigherOrder => "common higher-order expression",
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
    Effect,
}

impl QueryLocalDemand {
    pub(crate) fn description(self) -> &'static str {
        match self {
            QueryLocalDemand::Relation => "relation position",
            QueryLocalDemand::Value => "value-call position",
            QueryLocalDemand::HigherOrder => "parameterized relation position",
            QueryLocalDemand::Effect => "effect position",
        }
    }

    fn admits(self, kind: QueryLocalKind) -> bool {
        matches!(
            (self, kind),
            (QueryLocalDemand::Relation, QueryLocalKind::Relation)
                | (QueryLocalDemand::Value, QueryLocalKind::Value)
                | (QueryLocalDemand::HigherOrder, QueryLocalKind::HigherOrder)
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

    /// TAKE ANOTHER BLOCK'S CLAIMS INTO THIS ONE, at the position this
    /// block has reached, and answer the distance they moved.
    ///
    /// The claims keep their authored order relative to one another: a
    /// block absorbed second stands wholly after a block absorbed first,
    /// exactly as its text stood after that text. The caller moves the
    /// manifestations the same distance in the same act, which is why the
    /// distance is the answer and not a number the caller chose.
    ///
    /// A spelling both blocks declare under DIFFERENT kinds refuses here,
    /// where one name space closes over the merged block. Under the same
    /// kind the earlier claim keeps the position — the earlier declaration
    /// is where the name became visible.
    fn absorb(&mut self, other: QueryLocalNames) -> crate::error::Result<usize> {
        let offset = self.next_position;
        for (name, claim) in other.claims {
            match self.claims.get(&name) {
                Some(standing) if standing.kind != claim.kind => {
                    return Err(crate::pipeline::bindings::one_query_local_name(&name));
                }
                Some(_) => {}
                None => {
                    self.claims.insert(
                        name,
                        QueryLocalClaim {
                            kind: claim.kind,
                            first_position: claim.first_position + offset,
                        },
                    );
                }
            }
        }
        self.next_position += other.next_position;
        Ok(offset)
    }

    /// Whether this block claims the spelling at all, at any position.
    fn claims(&self, name: &delightql_types::SqlIdentifier) -> bool {
        self.claims.contains_key(name)
    }

    /// These claims WITHOUT the ones a nearer block makes. Positions do not
    /// move — a shadowed declaration is gone, and the ones around it stood
    /// where they stood.
    fn shadowed_by(&self, nearer: &QueryLocalNames) -> Self {
        QueryLocalNames {
            claims: self
                .claims
                .iter()
                .filter(|(name, _)| !nearer.claims(name))
                .map(|(name, claim)| (name.clone(), claim.clone()))
                .collect(),
            next_position: self.next_position,
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
    crate::error::DelightQLError::validation_error_categorized(
        crate::uri_registry::subcat::RESOLUTION_CALLABLE_UNKNOWN,
        format!(
            "{reason}. A claimed query-local name never falls through to a consulted, catalog, or target definition"
        ),
        "a taken query-local name is not a local miss",
    )
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

impl HoDefinition {
    /// THE ONE DOOR: one subject's clauses, in authored order, assembled.
    /// A refusal of the assembler's head laws is the CHOE's own
    /// head-agreement identity: the clauses are query text, not DDL.
    pub fn assemble(
        name: delightql_types::SqlIdentifier,
        effect: CteEffectDeclaration,
        decls: Vec<crate::pipeline::asts::ddl::ClauseDecl>,
        horizon: LexicalHorizon,
    ) -> crate::error::Result<Self> {
        let group =
            crate::pipeline::asts::ddl::DefinitionGroup::assemble(decls).map_err(|error| {
                match error {
                    crate::error::DelightQLError::ValidationError {
                        subcategory: Some(sub),
                        message,
                        context,
                    } if sub.starts_with("ddl/head/") => {
                        crate::error::DelightQLError::validation_error_categorized(
                            crate::uri_registry::subcat::RESOLUTION_CHOE_HEAD_AGREEMENT,
                            format!(
                                "the clauses of the common higher-order expression '{name}' \
                             do not agree: {message}"
                            ),
                            context,
                        )
                    }
                    other => other,
                }
            })?;
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

    /// This definition read in a block whose positions all moved later by
    /// `offset`; only the block's own absorption calls it, in the same act
    /// that moved the claims.
    fn shifted(mut self, offset: usize) -> Self {
        self.horizon = self.horizon.shifted(offset);
        self
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
/// compiler-built query, said by [`QueryLocals::none`] and by
/// [`QueryLocals::compiler_built`], which admits only subjects no authored
/// spelling answers to — never by an empty ledger a later phase would read
/// as a request to reconstruct one.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryLocals<P: Phase = Unresolved> {
    names: P::QueryLocalNames,
    cfes: P::CfeBindings,
    hos: P::HoBindings,
    ctes: Vec<CteBinding<P>>,
}

impl<P: Phase> QueryLocals<P> {
    /// NO QUERY-LOCAL BINDING OF ANY KIND, and therefore no claim.
    pub fn none() -> Self {
        QueryLocals {
            names: P::no_query_local_names(),
            cfes: P::no_cfe_bindings(),
            hos: P::no_ho_bindings(),
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
            names: super::phases::carry_query_local_names::<P, Q>(self.names)?,
            cfes: super::phases::carry_cfe_bindings::<P, Q>(self.cfes)?,
            hos: super::phases::carry_ho_bindings::<P, Q>(self.hos)?,
            ctes: self
                .ctes
                .into_iter()
                .map(|cte| walk.transform_cte_binding(cte))
                .collect::<crate::error::Result<Vec<_>>>()?,
        })
    }
}

/// A phase that has SPENT its query-local definitions holds no slot for a
/// claim, a CFE or a CHOE. Its CTE bindings answer to nothing, so they are
/// admitted and rearranged freely: there is no ledger left to disagree with.
impl<P> QueryLocals<P>
where
    P: Phase<QueryLocalNames = (), CfeBindings = (), HoBindings = ()>,
{
    pub fn spent(ctes: Vec<CteBinding<P>>) -> Self {
        QueryLocals {
            names: (),
            cfes: (),
            hos: (),
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
            return Err(crate::error::DelightQLError::transformation_error(
                "spending the heads of a query-local block's relation bindings answered with \
                 different subjects: the block's claims answer for the bindings it was minted \
                 with, and a replacement list is a second authority beside them",
                "query_local_block",
            ));
        }
        self.ctes = spent;
        Ok(())
    }

    /// A BLOCK OF COMPILER-BUILT CARRIERS ALONE: relations the compiler
    /// wrote, under generated, frontier or structural subjects, which no
    /// authored name answers to. Deliberately claimless — an authored
    /// subject here refuses, because an authored spelling that no claim
    /// covers would fall through to a consulted or catalog definition.
    pub(crate) fn compiler_built(ctes: Vec<CteBinding<Unresolved>>) -> crate::error::Result<Self> {
        if let Some(name) = ctes
            .iter()
            .find_map(|cte| cte.subject().authored_name().cloned())
        {
            return Err(crate::error::DelightQLError::transformation_error(
                format!(
                    "a compiler-built query bound the authored name '{name}': an authored \
                     spelling is claimed by the block that declares it, and a claimless \
                     binding is not a query-local name"
                ),
                "query_local_block",
            ));
        }
        Ok(QueryLocals {
            names: QueryLocalNames::default(),
            cfes: Vec::new(),
            hos: Vec::new(),
            ctes,
        })
    }

    /// SPEND THE BLOCK: resolution takes the claims and the definitions
    /// they govern out together, registers each definition where its claim
    /// says it stands, and leaves nothing behind. There is no way back —
    /// the parts are consumed, never re-paired.
    pub(crate) fn spend(
        self,
    ) -> (
        QueryLocalNames,
        Vec<CfeDefinition>,
        Vec<HoDefinition>,
        Vec<CteBinding<Unresolved>>,
    ) {
        (self.names, self.cfes, self.hos, self.ctes)
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
            names: self.names.clone(),
            cfes: self.cfes.clone(),
            hos: self.hos.clone(),
            ctes: Vec::new(),
        };
        for cte in standing {
            let subject = cte.subject().authored_name().cloned();
            let restated = restate(cte, &reached)?;
            if restated.subject().authored_name().cloned() != subject {
                return Err(crate::error::DelightQLError::transformation_error(
                    "restating a query-local block's relation binding answered with a \
                     different subject: the block's claims answer for the binding it was \
                     minted with",
                    "query_local_block",
                ));
            }
            reached.ctes.push(restated);
        }
        self.ctes = reached.ctes;
        Ok(())
    }

    /// THIS BLOCK UNDER A NEARER ONE: every spelling the nearer block
    /// claims leaves this one, claim and manifestation together.
    ///
    /// Shadowing is a DELETION, not an override. Keeping the outer claim
    /// while the nearer manifestation answers for it is the disagreement
    /// this carrier exists to forbid — and which of two same-spelled
    /// bindings a map happened to keep is not a scoping law.
    pub(crate) fn shadowed_by(&self, nearer: &QueryLocals<Unresolved>) -> Self {
        let gone = |name: Option<&delightql_types::SqlIdentifier>| {
            name.is_some_and(|name| nearer.names.claims(name))
        };
        QueryLocals {
            names: self.names.shadowed_by(&nearer.names),
            cfes: self
                .cfes
                .iter()
                .filter(|cfe| !gone(Some(&cfe.name)))
                .cloned()
                .collect(),
            hos: self
                .hos
                .iter()
                .filter(|ho| !gone(Some(ho.name())))
                .cloned()
                .collect(),
            ctes: self
                .ctes
                .iter()
                .filter(|cte| !gone(cte.subject().authored_name()))
                .cloned()
                .collect(),
        }
    }

    /// THIS BLOCK AS ONE HORIZON SEES IT: every relation and parameterized
    /// manifestation the horizon does not admit is dropped, every CLAIM is
    /// kept. The claims are what answer a name, and a claim the horizon
    /// refuses must still refuse it — dropping the claim with the binding
    /// would turn a not-yet-visible declaration into a local miss. Value
    /// definitions stay whole: one spelling may hold several clauses at
    /// several horizons, and selection already picks among them by the
    /// horizon each was declared at.
    pub(crate) fn visible_at(&self, horizon: LexicalHorizon) -> Self {
        let admits = |name: &delightql_types::SqlIdentifier, demand| {
            matches!(
                self.names.judge(name, horizon, demand),
                QueryLocalJudgment::Lawful(_)
            )
        };
        QueryLocals {
            names: self.names.clone(),
            cfes: self.cfes.clone(),
            hos: self
                .hos
                .iter()
                .filter(|ho| {
                    let demand = if ho.declares_effect() {
                        QueryLocalDemand::Effect
                    } else {
                        QueryLocalDemand::HigherOrder
                    };
                    admits(ho.name(), demand)
                })
                .cloned()
                .collect(),
            ctes: self
                .ctes
                .iter()
                .filter(|cte| {
                    cte.subject().authored_name().is_some_and(|name| {
                        let demand = if cte.subject().declares_effect() {
                            QueryLocalDemand::Effect
                        } else {
                            QueryLocalDemand::Relation
                        };
                        admits(name, demand)
                    })
                })
                .cloned()
                .collect(),
        }
    }

    /// THE PURE FACE OF THIS BLOCK: every effect-marked relation binding
    /// dropped, every claim kept. An effect manifestation is opened by its
    /// demand and never by a pure statement, so a pure position must read
    /// a WRONG KIND there rather than a local miss that falls through to
    /// the catalog.
    pub(crate) fn pure(&self) -> Self {
        QueryLocals {
            names: self.names.clone(),
            cfes: self.cfes.clone(),
            hos: self.hos.clone(),
            ctes: self
                .ctes
                .iter()
                .filter(|cte| !cte.subject().declares_effect())
                .cloned()
                .collect(),
        }
    }
}

/// One CHOE's clauses as the block reads them: gathered by SUBJECT in
/// authored order, under the horizon the block stood at when the FIRST
/// clause was written.
#[derive(Debug)]
struct HoClauseGroup {
    name: delightql_types::SqlIdentifier,
    effect: CteEffectDeclaration,
    decls: Vec<crate::pipeline::asts::ddl::ClauseDecl>,
    horizon: LexicalHorizon,
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
    cfes: Vec<CfeDefinition>,
    hos: Vec<HoDefinition>,
    ctes: Vec<CteBinding<Unresolved>>,
    groups: Vec<HoClauseGroup>,
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

    /// Admit one value definition.
    pub(crate) fn admit_cfe(&mut self, mut cfe: CfeDefinition) -> crate::error::Result<()> {
        cfe.horizon = self
            .names
            .declare(cfe.name.clone(), QueryLocalKind::Value)?;
        self.cfes.push(cfe);
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

    /// TAKE ONE WHOLE BLOCK INTO THIS ONE, claims and manifestations in the
    /// same act.
    ///
    /// An internal rebuilding road — the parameterized expansion that
    /// squishes a definition's clause bodies into one query — must move the
    /// authored fact, not re-derive it: the collections it hoists no longer
    /// record which name was written before which. Absorption is the move.
    /// The absorbed claims stand after everything already admitted, and
    /// every manifestation absorbed with them travels the same distance, so
    /// each contributing block keeps exactly the visibility its text had.
    pub(crate) fn absorb(
        &mut self,
        locals: QueryLocals<Unresolved>,
    ) -> crate::error::Result<usize> {
        let QueryLocals {
            names,
            cfes,
            hos,
            ctes,
        } = locals;
        let offset = self.names.absorb(names)?;
        self.cfes
            .extend(cfes.into_iter().map(|cfe| cfe.shifted(offset)));
        self.hos
            .extend(hos.into_iter().map(|ho| ho.shifted(offset)));
        self.ctes
            .extend(ctes.into_iter().map(|cte| cte.shifted(offset)));
        Ok(offset)
    }

    /// The finished block.
    pub(crate) fn seal(self) -> crate::error::Result<QueryLocals<Unresolved>> {
        let QueryLocalBlock {
            names,
            cfes,
            mut hos,
            ctes,
            groups,
        } = self;
        for group in groups {
            hos.push(HoDefinition::assemble(
                group.name,
                group.effect,
                group.decls,
                group.horizon,
            )?);
        }
        Ok(QueryLocals {
            names,
            cfes,
            hos,
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
            "(query (local_names {}) (cfes {}) (hos {}) (ctes {}) (body {}))",
            self.locals.names.to_lispy(),
            self.locals.cfes.to_lispy(),
            self.locals.hos.to_lispy(),
            self.locals.ctes.to_lispy(),
            self.body.to_lispy(),
        )
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

/// ER-context specification: identifies which context to use for & and && operators
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("er_context_spec")]
pub struct ErContextSpec {
    /// Optional namespace qualification (e.g., "lib::er_grounded")
    pub namespace: Option<String>,
    /// Context name (e.g., "normal", "audit")
    pub context_name: String,
}

/// What a CTE label DECLARES about its expression's relationship to effects.
///
/// A bare label asserts a pure expression; a `!`-marked label asserts the
/// expression demands a directive. The declaration is an assertion, never a
/// coercion: the effect authority judges it against the body once — a mark
/// on a pure body refuses (`effect/cte/pure_mark`) before a marked binding
/// can be constructed, and an effectful body without the mark refuses where
/// the rule registers (`effect/cte/label`).
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
        AuthoredCteSubject, CfeDefinition, CfeFormals, ContextMode, CteAuthority, CteBinding,
        CteEffectDeclaration, DomainExpression, LexicalHorizon, QueryLocalBlock, QueryLocalDemand,
        QueryLocalJudgment, QueryLocalKind, QueryLocals,
    };
    use delightql_types::SqlIdentifier;

    fn cfe(name: &str) -> CfeDefinition {
        CfeDefinition::unbounded(
            SqlIdentifier::new(name),
            CfeFormals::from_role_groups([], []),
            ContextMode::None,
            DomainExpression::Application(super::super::FunctionApplication::Ground(
                super::super::LiteralValue::Null,
            )),
        )
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
                    outer: false,
                },
                super::super::Access::All,
            ),
            subject,
            CteAuthority {
                horizon: LexicalHorizon::all(),
                head: super::super::definitions::Head::glob(),
                origin: super::super::provenance::CteOrigin::CompilerGenerated,
                fixpoint: super::super::super::vocabulary::Fixpoint::Bag,
            },
        )
    }

    fn authored(name: &str) -> CteBinding {
        relation(AuthoredCteSubject::Authored {
            name: SqlIdentifier::new(name),
            effect: CteEffectDeclaration::Pure,
        })
    }

    fn generated(name: &str) -> CteBinding {
        relation(AuthoredCteSubject::Generated {
            name: SqlIdentifier::new(name),
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

    /// ABSORPTION MOVES A WHOLE BLOCK, claims and manifestations together:
    /// each contributor keeps exactly the visibility its own text had, and
    /// the definition that was written first is still the one a later
    /// declaration cannot be seen from.
    #[test]
    fn absorbing_two_blocks_keeps_each_ones_own_visibility() {
        let mut first = QueryLocalBlock::default();
        first.admit_cfe(cfe("a")).expect("a");
        first.admit_relation(authored("b")).expect("b");
        let first = first.seal().expect("first seals");

        let mut second = QueryLocalBlock::default();
        second.admit_cfe(cfe("c")).expect("c");
        second.admit_relation(authored("d")).expect("d");
        let second = second.seal().expect("second seals");

        let mut merged = QueryLocalBlock::default();
        merged.absorb(first).expect("absorb the first");
        merged.absorb(second).expect("absorb the second");
        let merged = merged.seal().expect("the merged block seals");

        let names = merged.names();
        let at_a = merged.cfes()[0].horizon();
        let at_c = merged.cfes()[1].horizon();
        // Within its own block, `a` still cannot see `b`.
        assert_eq!(
            names.judge(&SqlIdentifier::new("b"), at_a, QueryLocalDemand::Relation),
            QueryLocalJudgment::NotYetVisible(QueryLocalKind::Relation)
        );
        // Nor can `c` see `d`, though everything of the first block moved
        // to earlier positions than either of them.
        assert_eq!(
            names.judge(&SqlIdentifier::new("d"), at_c, QueryLocalDemand::Relation),
            QueryLocalJudgment::NotYetVisible(QueryLocalKind::Relation)
        );
        assert_eq!(
            names.judge(&SqlIdentifier::new("b"), at_c, QueryLocalDemand::Relation),
            QueryLocalJudgment::Lawful(QueryLocalKind::Relation)
        );
    }

    /// A compiler-built query binds carriers no authored name answers to.
    /// An authored subject there REFUSES rather than standing claimless,
    /// because a claimless authored spelling falls through to a consulted
    /// or catalog definition.
    #[test]
    fn a_compiler_built_block_refuses_an_authored_subject() {
        assert!(QueryLocals::compiler_built(vec![generated("gen")]).is_ok());
        assert!(QueryLocals::compiler_built(vec![authored("named")]).is_err());
    }

    /// A nearer block SHADOWS an outer one: the outer spelling leaves the
    /// block entirely, so nothing claims a name whose binding is gone and
    /// nothing answers a name whose claim is gone.
    #[test]
    fn a_shadowed_spelling_leaves_claim_and_binding_together() {
        let mut outer = QueryLocalBlock::default();
        outer.admit_relation(authored("shared")).expect("outer");
        outer.admit_relation(authored("kept")).expect("kept");
        let outer = outer.seal().expect("outer seals");

        let mut nearer = QueryLocalBlock::default();
        nearer.admit_relation(authored("shared")).expect("nearer");
        let nearer = nearer.seal().expect("nearer seals");

        let under = outer.shadowed_by(&nearer);
        assert_eq!(under.ctes().len(), 1);
        assert_eq!(
            under.ctes()[0]
                .subject()
                .authored_name()
                .map(|n| n.as_str()),
            Some("kept")
        );
        assert_eq!(
            under.names().judge(
                &SqlIdentifier::new("shared"),
                LexicalHorizon::all(),
                QueryLocalDemand::Relation,
            ),
            QueryLocalJudgment::Absent
        );
        assert_eq!(
            under.names().judge(
                &SqlIdentifier::new("kept"),
                LexicalHorizon::all(),
                QueryLocalDemand::Relation,
            ),
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
    /// shorthand and a compiler-built binding both glob.
    #[lispy("head")]
    pub head: crate::pipeline::asts::core::definitions::Head,
    /// TYPED provenance: who authored this CTE — set at CONSTRUCTION,
    /// never inferred from the name (a user may legally write `_ho_*`
    /// identifiers). The squished-weave scope override keys on this.
    #[lispy("origin")]
    pub origin: crate::pipeline::asts::core::provenance::CteOrigin,
    /// The fixpoint flavor the authored head badged (`… : c%`). Carried
    /// UNJUDGED: whether this binding is a fixpoint at all is not knowable
    /// until the self-reference binds, so the badge travels to the one
    /// recursion decision and is spent there with the authority the phase
    /// deletes.
    #[lispy("fixpoint")]
    pub fixpoint: crate::pipeline::asts::vocabulary::Fixpoint,
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
                    return Err(crate::error::DelightQLError::transformation_error(
                        format!(
                            "the callable formal '{}' stands after a scalar one: a call \
                             site supplies code first, so binding order is \
                             callable-then-scalar",
                            formal.name
                        ),
                        "cfe_formals",
                    ));
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

/// Higher-order example: apply_transform:(transform)(value) : value /-> transform:()
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
        CfeDefinition {
            name,
            formals,
            context_mode,
            horizon: LexicalHorizon::all(),
            body,
        }
    }

    /// The declarations visible where this body was authored.
    pub(crate) fn horizon(&self) -> LexicalHorizon {
        self.horizon
    }

    /// This definition read in a block whose positions all moved later by
    /// `offset`; only the block's own absorption calls it.
    fn shifted(mut self, offset: usize) -> Self {
        self.horizon = self.horizon.shifted(offset);
        self
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
