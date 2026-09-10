// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::diagnostic::{Constraint, Er, Internal, Resolution, Semantic};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::ColumnOccurrence;
use delightql_types::error::{DelightQLError, Result};
use std::collections::HashMap;

mod string_templates;

/// The closed PIPE FORM inventory (fundamentals) over the normalized
/// carriers, and the one crossing every pipe form's output takes.
mod pipe_form;

/// What publication decides, across the phases: which occurrence an item
/// publishes, whether it publishes one at all, and what it answers to.
#[cfg(test)]
mod publication_boundary_tests;

/// Exact pipe-output reuse, read where it is recorded: the resolved
/// member's correspondence names the pipe's exact port, a fresh binding
/// yields a stated Cartesian, and recursive SQL carries the condition.
#[cfg(test)]
mod pipe_reuse_tests;

#[cfg(test)]
mod merge_lowering_tests;
/// An ordering and the bound that consumes it are one node and one SQL
/// scope; a relation actual is admitted closed; a correspondence is spelled
/// from its slots.
#[cfg(test)]
mod ordered_bound_tests;
#[cfg(test)]
mod relation_actual_tests;

/// Probe tests: plan-note schema injection via the query-local
/// registry (test code only — see the module header for the guarantee).
#[cfg(test)]
mod plan_note_injection_tests;

/// SQL-shape pins for argumentative semi/anti-join correlation: the `+rel(col)` guard must
/// compare the OUTER column to the fact column, never `_fact` to itself.
#[cfg(test)]
mod semijoin_correlation_tests;

/// Classification pins for bare guards on enlisted tables / consulted rules
/// (the torture--99 blocker): a
/// guard functor resolvable through enlistment or the Some(ns) resolution
/// scope must classify as table-as-sigma, never fall to PredicateRewrite.
#[cfg(test)]
mod enlisted_guard_classification_tests;

/// Scope pins for sigma-predicate rule guards: a sigma rule visible in the
/// Some(ns) consulted scope must expand to its boolean body — scope first,
/// enlisted-into-main as fallback.
#[cfg(test)]
mod sigma_guard_scope_tests;

/// Shadowing pins: temp shadows main for UNQUALIFIED names only;
/// qualified reads reach the physical entity; the shadow is a resolution
/// preference, never a catalog delete.
#[cfg(test)]
mod session_shadow_tests;

/// The catalog is current per statement: a statement's read is one state,
/// and the next statement follows a completed replacement through every
/// live definition road; a failed replacement leaves the prior state whole.
#[cfg(test)]
mod catalog_current_state_tests;

/// The higher-order occurrence is recorded, never chosen among equals: the
/// formal lands on the position that CONTINUES the caller's actual.
#[cfg(test)]
mod ho_occurrence_tests;
#[cfg(test)]
mod relation_formal_tests;

/// EVERYTHING a pattern slot needs to instantiate a definition, or
/// nothing: the resolver core, the ONE lexical world the slot stands in,
/// and the compilation's allowance travel together, so a road that can
/// instantiate cannot lack the bound and cannot choose a world of its own.
#[derive(Clone, Copy)]
pub(crate) struct SlotInstantiation<'a, 'db> {
    pub(in crate::pipeline::resolver) core: &'a crate::resolution::ResolverCore<'db>,
    pub(in crate::pipeline::resolver) env: &'a crate::defuse::environment::Environment,
    pub(in crate::pipeline::resolver) instances: &'a crate::defuse::instance::InstanceTable,
    /// A SCOPED body's own world: the frame its formal references spend and
    /// the declaration position its names are judged at, as ONE value the
    /// selected definition minted. There is no second field to disagree
    /// with it and no site to supply beside it. A declaration body reads
    /// the site the world's own open declarations name, and this is `None`.
    pub(in crate::pipeline::resolver) scoped:
        Option<&'a crate::defuse::environment::ScopedSlotWorld>,
}

impl<'a, 'db> SlotInstantiation<'a, 'db> {
    /// Read-only doors for the definition-use authority; construction
    /// stays inside the resolver, so an allowance is never assembled from
    /// loose pieces at a use position.
    pub(crate) fn core(&self) -> &'a crate::resolution::ResolverCore<'db> {
        self.core
    }
    pub(crate) fn instances(&self) -> &'a crate::defuse::instance::InstanceTable {
        self.instances
    }
    pub(crate) fn env(&self) -> &'a crate::defuse::environment::Environment {
        self.env
    }
    pub(crate) fn select_query_local(
        &self,
        name: &delightql_types::SqlIdentifier,
        demand: crate::pipeline::asts::core::QueryLocalDemand,
    ) -> crate::error::Result<Option<crate::defuse::environment::QueryLocalSelection>> {
        self.env.select_query_local(name, demand, self.scoped)
    }

    /// An allowance standing in an OPENED consulted world — the world a
    /// sealed slot body was sealed with, held by the definition-use
    /// authority's own converting operation. The core and the instance
    /// table carry over from this allowance; nothing mints them from loose
    /// parts.
    pub(crate) fn in_opened<'b>(
        &self,
        env: &'b crate::defuse::environment::Environment,
    ) -> SlotInstantiation<'b, 'db>
    where
        'a: 'b,
    {
        SlotInstantiation {
            core: self.core,
            env,
            instances: self.instances,
            scoped: None,
        }
    }

    /// An allowance for a SCOPED body: its own frame standing on this
    /// allowance's world.
    pub(crate) fn in_scoped<'b>(
        &self,
        scoped: &'b crate::defuse::environment::ScopedSlotWorld,
    ) -> SlotInstantiation<'b, 'db>
    where
        'a: 'b,
    {
        SlotInstantiation {
            core: self.core,
            env: self.env,
            instances: self.instances,
            scoped: Some(scoped),
        }
    }
}

/// Configuration for TVF resolution behavior
#[derive(Debug, Clone)]
pub struct ResolutionConfig {
    /// Allow unknown TVFs to pass through with Unknown schema
    pub permissive: bool,
    /// SERVE BOOTSTRAP READS (materialization-law §2). While a
    /// materialization source resolves, a relation on the bootstrap
    /// connection is answered as a literal snapshot read at plan build —
    /// so connection 1 never enters the attribution set (exemption is
    /// ABSENCE, not a tie-break) and the compiled source executes whole on
    /// whatever connection attribution selects.
    pub serve_bootstrap_reads: bool,
    /// When true, outer_context provides reachable columns for validation
    /// but does NOT trigger deferred (skip) validation mode. Used for
    /// EXISTS/semi-join/anti-join subqueries where the full column set
    /// (outer + inner) is known and validation is safe.
    pub validate_in_correlation: bool,
    /// The one recursion detector: the compilation-local definition
    /// instance table (shared across clones). Instantiation installs the
    /// instance before opening the family body; a same-key self-use
    /// re-enters the active fixpoint and a changed-key self-use refuses
    /// terminally.
    pub instances: crate::defuse::instance::InstanceTable,
    /// The danger gates in force for this compilation. Scope activation
    /// reads `scope/duplicate` here, so the duplicate-answering judgment
    /// honors the same acknowledgment surface every other gate uses.
    pub danger_gates: crate::pipeline::danger_gates::DangerGateMap,
    /// Whether this resolution reads an AUTHORED environment. The
    /// duplicate-answering judgment runs only where an author composed the
    /// co-live relations: an instantiated definition body and a
    /// compiler-synthesized query are replays — a diamond call feeding one
    /// relation to two formals is ruled lawful, and the compiler's own
    /// repeated reads are its own business.
    pub authored_environment: bool,
}

impl Default for ResolutionConfig {
    fn default() -> Self {
        Self {
            permissive: true, // Default to permissive mode
            serve_bootstrap_reads: false,
            validate_in_correlation: false,
            instances: crate::defuse::instance::InstanceTable::default(),
            danger_gates: crate::pipeline::danger_gates::DangerGateMap::with_defaults(),
            authored_environment: true,
        }
    }
}

pub mod unification;
use unification::ColumnReference;

pub(crate) mod helpers;
use self::helpers::*;
mod bubbling;
mod caller_row;
mod lexical;
use self::bubbling::*;
pub(super) mod cte_validation;
pub(crate) mod resolving;
mod type_conversion;

pub(crate) mod grounding;
pub(crate) mod relation_resolver;
pub(crate) mod resolver_fold;
mod tvf;
use crate::pipeline::asts::core::{
    Comparison, Existence, GroundForm, Membership, RelationalMembership, SigmaApplication,
};
use crate::pipeline::asts::core::{NamedReference, Reference};
use resolver_fold::ResolverFold;

pub(crate) use caller_row::CallerRow;
pub(crate) use lexical::Terminal;
pub(crate) use lexical::{
    BornPosition, Judged, JudgedBirth, PatternOperand, PatternOwner, Position, Reach,
    ResolvedQuery, ResolvedRelation, RowRead, StrictPhaseConverter,
};

// Re-export DatabaseSchema from delightql-types: it lives in the types
// crate, not core, to avoid circular dependencies.
pub use delightql_types::schema::DatabaseSchema;

/// Result of query resolution including connection routing information
pub struct ResolvedQueryResult {
    /// The resolved query AST
    pub query: ast_resolved::Query,
    /// The single connection_id if all tables are on the same connection,
    /// or None if no tables were resolved (pure literal query).
    /// Cross-connection queries will have already errored during resolution.
    pub connection_id: Option<i64>,
}

/// Run one subject's clause heads through the one assembler and project
/// each clause body through its own head.
///
/// A glob group comes back untouched — a glob head publishes the body's
/// heading, names and order as they are. A listed group comes back with
/// every head spent: the contract has been applied to the bodies, so what
/// leaves here is what the subject publishes.
/// Every clause of a definition publishes the same heading.
///
/// The refusal names the clause and both headings, because a glob head
/// declares nothing for the author to compare against: what disagrees is one
/// body against another, and only the compiler has both in front of it.
pub(super) fn clauses_publish_one_heading(
    name: &delightql_types::SqlIdentifier,
    schemas: &[crate::relation::SemanticRelation],
    identities: &crate::relation::Planning,
) -> Result<()> {
    let spell = |relation: &crate::relation::SemanticRelation| -> Result<Vec<String>> {
        Ok(crate::relation::published_ports(identities, relation)?
            .into_iter()
            .map(|port| {
                let mut text = String::new();
                match identities.published(port.column()) {
                    Some(spelling) => {
                        identities.write(spelling, &mut crate::names::Teaching(&mut text));
                    }
                    None => text.push('_'),
                }
                text
            })
            .collect())
    };
    let Some(first) = schemas.first() else {
        return Ok(());
    };
    let expected = spell(first)?;
    for (index, schema) in schemas.iter().enumerate().skip(1) {
        let published = spell(schema)?;
        // A HEADING IS NAMES IN ORDER. Two clauses agreeing only on width
        // accumulate positionally and publish the first clause's names over
        // the second's cells, which is the silent answer heads-law refuses
        // in the same breath as the NULL-padding one.
        if published == expected {
            continue;
        }
        return Err(DelightQLError::from(Semantic::HeadsClauseDisagreement {
            message: format!(
                "the clauses of '{name}' publish different headings: clause 1 \
                 publishes ({first}) and clause {clause} publishes ({other}). \
                 A glob head takes its schema from the bodies, so every clause \
                 must publish one heading — declare the shared heading in the \
                 head, or project each body to it.",
                first = expected.join(", "),
                clause = index + 1,
                other = published.join(", "),
            ),
        }));
    }
    Ok(())
}

pub(super) fn apply_group_head(
    name: &str,
    group: Vec<ast_unresolved::CteBinding>,
) -> Result<Vec<ast_unresolved::CteBinding>> {
    use crate::pipeline::asts::core::definitions::{assemble, spend_heads};

    let heads: Vec<&crate::pipeline::asts::core::definitions::Head> =
        group.iter().map(|cte| &cte.authority().head).collect();
    let assembly = assemble(
        name,
        &heads,
        crate::pipeline::asts::core::definitions::GroundNaming::Refuse,
    )?;
    spend_heads(group, &assembly, name)
}

/// Trait abstracting CTE resolution + registration so that `resolve_cte_bindings`
/// resolves every binding through the ONE lexical world the resolver stands in.
pub(crate) trait CteResolver {
    /// Resolve an unresolved relational expression in the resolver's own
    /// world. There is no owner argument: a CTE resolves where it is
    /// written, and a body world receives its caller's carriers already
    /// resolved.
    fn resolve_cte_expression(
        &mut self,
        expr: ast_unresolved::Chain,
        horizon: crate::pipeline::asts::core::LexicalHorizon,
    ) -> Result<ast_resolved::Chain>;

    /// Register a resolved query-local manifestation through the lexical
    /// world's single registration road.
    fn register_query_local(
        &mut self,
        registration: crate::defuse::environment::QueryLocalRegistration,
    );

    fn register_frontier(
        &mut self,
        frontier: crate::defuse::FrontierGroup,
        relation: crate::relation::SemanticRelation,
    );

    /// The arena every relation in this resolution was derived in.
    fn identities(&self) -> &crate::relation::Planning;

    fn crossing_carriers(&self) -> &[crate::relation::PortId];
}

/// `ResolverFold` as a CTE resolver — used by the top-level `resolve_query`.
impl CteResolver for ResolverFold<'_, '_> {
    fn resolve_cte_expression(
        &mut self,
        expr: ast_unresolved::Chain,
        horizon: crate::pipeline::asts::core::LexicalHorizon,
    ) -> Result<ast_resolved::Chain> {
        if !horizon.is_all() {
            self.env.push_binding_declaration(horizon);
        }
        let resolved = self
            .resolve_relational(expr)
            .map(|resolved| resolved.into_body());
        if !horizon.is_all() {
            self.env.pop_declaration();
        }
        resolved
    }

    fn register_query_local(
        &mut self,
        registration: crate::defuse::environment::QueryLocalRegistration,
    ) {
        self.env.register_query_local(registration);
    }

    fn crossing_carriers(&self) -> &[crate::relation::PortId] {
        &self.crossing_carriers
    }

    fn register_frontier(
        &mut self,
        frontier: crate::defuse::FrontierGroup,
        relation: crate::relation::SemanticRelation,
    ) {
        frontier.register(self.env, relation);
    }

    fn identities(&self) -> &crate::relation::Planning {
        self.core.identities
    }
}

/// Resolve a full Query (which may contain CTEs) at the SESSION — the use
/// world rooted at `scope_fq` (`home` at the prompt; the namespace a
/// consulted goal is a form of).
///
/// Returns the resolved query along with connection routing information.
/// If tables from multiple connections are referenced, returns an error.
pub fn resolve_query(
    query: ast_unresolved::Query,
    schema: &dyn DatabaseSchema,
    system: Option<&crate::system::DelightQLSystem>,
    config: &ResolutionConfig,
    identities: &crate::relation::Planning,
    scope_fq: &str,
) -> Result<ResolvedQueryResult> {
    let mut core = if let Some(sys) = system {
        crate::resolution::ResolverCore::new_with_system(schema, sys, identities)
    } else {
        crate::resolution::ResolverCore::new(schema, identities)
    };
    // THE USE WORLD: one owned value, built for this compilation and never
    // handed to a body.
    let mut env = crate::defuse::environment::Environment::Use(
        crate::defuse::environment::UseEnvironment::session(&core.consult, scope_fq)?,
    );
    let mut fold = ResolverFold::new(&mut core, &mut env, config.clone());
    let mut resolved_query = resolve_query_with(&mut fold, query)?.into_query();

    // Validate that all resolved tables belong to the same connection
    let connection_id = core.validate_single_connection()?;

    // THE INCHOATE LAW, applied over the whole resolved tree: an unaccessed
    // inchoate occurrence yields zero rows under its opaque displayed
    // heading, a name reaching a latent dimension refuses, and a positional
    // reach was its activation.
    apply_inchoate_law(&mut resolved_query, identities)?;

    Ok(ResolvedQueryResult {
        query: resolved_query,
        connection_id,
    })
}

/// THE ONE ROAD FROM A QUERY TO ITS RESOLUTION, in whatever world the fold
/// stands in. Query-scoped definitions are registered as authored and spent
/// at their call sites; the bindings resolve in order; the body resolves
/// with every binding registered. A body world's fold is constructed only
/// by the definition-use authority, so this road cannot be entered with a
/// consulted body beside a world the authority did not choose.
/// ONE STATEMENT OF A BODY WHOSE BLOCK ALREADY STANDS. An effect body is a
/// sequence of statements under ONE authored block: its claims and its
/// definitions are declared on the world once, when the body is entered, and
/// every statement resolves inside that one declaration. What each statement
/// still carries is its relation BINDINGS — they resolve afresh here because
/// the world they read changes as the plan creates relations — so nothing is
/// re-declared and no second name fact is minted for the same block.
pub(crate) fn resolve_statement_in_standing_block(
    fold: &mut ResolverFold,
    query: ast_unresolved::Query,
) -> Result<ResolvedQuery> {
    let ast_unresolved::Query { locals, body } = query;
    let (names, cfes, hos, ctes) = locals.spend();
    debug_assert!(
        names.is_empty() && cfes.is_empty() && hos.is_empty(),
        "a statement of a standing block carries bindings only"
    );
    let resolved_ctes = if ctes.is_empty() {
        Vec::new()
    } else {
        crate::pipeline::bindings::resolve_cte_bindings(ctes, fold)?
    };
    Ok(fold.resolve_relational(body)?.into_query(|body| {
        ast_resolved::Query::binding(
            crate::pipeline::asts::core::QueryLocals::spent(resolved_ctes),
            body,
        )
    }))
}

pub(crate) fn resolve_query_with(
    fold: &mut ResolverFold,
    query: ast_unresolved::Query,
) -> Result<ResolvedQuery> {
    let ast_unresolved::Query { locals, body } = query;
    // THE BLOCK ARRIVES WHOLE. Its claims were minted by the same act that
    // stamped these bindings' horizons — the authored construction, or the
    // absorption an internal expansion moved it through — so there is
    // nothing here to reconstruct and nothing that could be reconstructed:
    // separated per-kind collections no longer record which name was
    // written before which.
    let (names, cfes, hos, ctes) = locals.spend();

    fold.env.push_query_names(names);

    let resolved = (|| {
        for cfe in cfes {
            refuse_empty_explicit_context(&cfe)?;
            fold.env.register_query_local(
                crate::defuse::environment::QueryLocalRegistration::Value(cfe),
            );
        }
        for ho in hos {
            fold.env.register_query_local(
                crate::defuse::environment::QueryLocalRegistration::HigherOrder(ho),
            );
        }

        let resolved_ctes = if ctes.is_empty() {
            Vec::new()
        } else {
            crate::pipeline::bindings::resolve_cte_bindings(ctes, fold)?
        };

        Ok(fold.resolve_relational(body)?.into_query(|body| {
            ast_resolved::Query::binding(
                crate::pipeline::asts::core::QueryLocals::spent(resolved_ctes),
                body,
            )
        }))
    })();
    fold.env.pop_query_names();
    resolved
}

/// THE INCHOATE LOWERING's resolution half (RULINGS sitting 26, amended by
/// POSITION REACHES WHAT NAMES CANNOT).
///
/// An occurrence written `R()` starts with every dimension latent. An
/// access-site act — a slot group, `(*)`, a later access step in its own
/// chain — activates it totally; a positional reference into it activates
/// it (recorded where ordinals resolve); a NAME reaches nothing, because a
/// latent dimension has none. What nothing activated is marked for the
/// zero-row lowering, and its columns are depublished so the displayed
/// heading spells the mints of dimensions nobody named.
fn apply_inchoate_law(
    query: &mut ast_resolved::Query,
    identities: &crate::relation::Planning,
) -> Result<()> {
    use crate::pipeline::ast_visit::{walk_visit_query, AstVisit, Descent};

    struct Unaccessed<'r> {
        identities: &'r crate::names::Registry,
        latent: std::collections::HashSet<crate::names::ScopeId>,
    }
    impl AstVisit<crate::pipeline::asts::core::Resolved> for Unaccessed<'_> {
        fn enter_relational(&mut self, chain: &ast_resolved::Chain) -> Result<Descent> {
            use crate::pipeline::asts::core::{Access, Continuation, Relation};
            let GroundForm::Reference(Relation::Ground { .. }) = chain.head().form() else {
                return Ok(Descent::Continue);
            };
            let result = chain.head().result();
            let mut steps = chain.forms();
            let Some(Continuation::Access {
                access: Access::Unasked,
                ..
            }) = steps.next()
            else {
                return Ok(Descent::Continue);
            };
            // A later access step in the occurrence's own chain is an
            // access-site act: `users() *` and `users() .(id)` are total
            // activation, spelled after the read.
            if steps.any(|step| matches!(step, Continuation::Access { .. })) {
                return Ok(Descent::Continue);
            }
            // A positional reach activated it where the ordinal resolved.
            if self.identities.ordinal_reached(result.scope()) {
                return Ok(Descent::Continue);
            }
            // Only an enumerable heading has dimensions to depublish; an
            // opaque passthrough keeps its own contract.
            if !crate::relation::any_interface_opaque(
                self.identities,
                std::slice::from_ref(result),
            )? {
                self.latent.insert(result.scope());
            }
            Ok(Descent::Continue)
        }
    }

    let mut unaccessed = Unaccessed {
        identities,
        latent: std::collections::HashSet::new(),
    };
    walk_visit_query(&mut unaccessed, query)?;
    if unaccessed.latent.is_empty() {
        return Ok(());
    }
    let latent = unaccessed.latent;

    // A NAME REACHES NOTHING LATENT. Any resolved occurrence of a latent
    // scope's column outside its own read was bound by name — positional
    // reaches activated their scope above — so it refuses with the teaching
    // rather than binding a dimension nobody activated.
    struct NameReach<'r> {
        identities: &'r crate::names::Registry,
        latent: &'r std::collections::HashSet<crate::names::ScopeId>,
        reached: Option<crate::relation::PortId>,
    }
    impl AstVisit<crate::pipeline::asts::core::Resolved> for NameReach<'_> {
        fn enter_domain(&mut self, expression: &ast_resolved::DomainExpression) -> Result<Descent> {
            use crate::pipeline::asts::core::{NamedReference, Reference};
            if let ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
                occurrence,
            ))) = expression
            {
                if self
                    .latent
                    .contains(&crate::relation::owner(self.identities, occurrence.column)?)
                {
                    self.reached.get_or_insert(occurrence.column);
                }
            }
            Ok(Descent::Continue)
        }
    }
    let mut names = NameReach {
        identities,
        latent: &latent,
        reached: None,
    };
    walk_visit_query(&mut names, query)?;
    if let Some(column) = names.reached {
        let mut text = String::new();
        identities.describe(
            crate::relation::owner(identities, column)?,
            &mut crate::names::Teaching(&mut text),
        );
        return Err(DelightQLError::from(Semantic::InchoateLatentName {
            message: format!(
                "the dimension is latent: '{text}()' names no columns until the \
                 occurrence is accessed"
            ),
        }));
    }

    for scope in &latent {
        identities.note_annihilated(*scope);
    }

    // THE DISPLAYED HEADING SPELLS MINTS. A terminal latent read — the
    // chain that is nothing but the read — publishes its dimensions under
    // née mints: a synthesized projection republishes each column into a
    // fresh unnamed occurrence, and baptism names what nobody did.
    republish_latent_terminals(query, &latent, identities);
    Ok(())
}

/// Append the née republication to every chain that IS a latent read and
/// nothing else.
fn republish_latent_terminals(
    query: &mut ast_resolved::Query,
    latent: &std::collections::HashSet<crate::names::ScopeId>,
    identities: &crate::relation::Planning,
) {
    for cte in query.locals.ctes_mut() {
        for part in cte.parts_mut() {
            republish_latent_terminal_chain(part, latent, identities);
        }
    }
    republish_latent_terminal_chain(&mut query.body, latent, identities);
}

fn republish_latent_terminal_chain(
    chain: &mut ast_resolved::Chain,
    latent: &std::collections::HashSet<crate::names::ScopeId>,
    identities: &crate::relation::Planning,
) {
    use crate::pipeline::asts::core::{Access, Continuation, Relation};
    let GroundForm::Reference(Relation::Ground { .. }) = chain.head().form() else {
        return;
    };
    let scope = *chain.head().result();
    if !latent.contains(&scope.scope()) {
        return;
    }
    let forms: Vec<&Continuation<_>> = chain.forms().collect();
    let [Continuation::Access {
        access: Access::Unasked,
        ..
    }] = forms.as_slice()
    else {
        return;
    };
    let Ok(columns) = crate::relation::published_ports(identities, &scope) else {
        return;
    };
    // The export and the projection that names its positions are ONE act:
    // every item carries the occurrence the export just minted for it.
    let Ok(published) = identities.authority().extend(
        std::mem::replace(chain, ast_resolved::Chain::ground(chain.head().clone())),
        crate::relation::builder::StepOp::Republish {
            of: crate::relation::builder::Republishing::Export(crate::relation::form::ExportSpec {
                input: scope,
                why: crate::relation::form::ExportWhy::EmissionAlias,
            }),
            sources: columns,
        },
    ) else {
        return;
    };
    *chain = published;
}

/// `..{}` declares a capture of nothing, which is a regular definition
/// wearing a context marker; the marker must go.
fn refuse_empty_explicit_context(cfe: &ast_unresolved::CfeDefinition) -> Result<()> {
    if let crate::pipeline::asts::core::ContextMode::Explicit(captures) = &cfe.context_mode {
        if captures.is_empty() {
            return Err(DelightQLError::from(Constraint::General {
                message: format!(
                    "CFE '{}' declares empty explicit context '..{{}}' but this is unnecessary. \
                 Remove the context marker entirely: {}:({}): ...",
                    cfe.name,
                    cfe.name,
                    cfe.formals
                        .iter()
                        .map(|formal| formal.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            }));
        }
    }
    Ok(())
}

/// Collect the columns that sibling EXISTS subqueries in this scope publish,
/// read off the resolution that survives.
///
/// Interdependent EXISTS reference each other's tables — in
/// `+orders(...), +order_items(, orders.id = order_items.order_id)` the second
/// subquery's `orders.id` names a relation that only exists inside the first.
/// Those columns have to reach the correlation context, and the context must
/// carry the very occurrences the emitted statement establishes: resolving the
/// unresolved sibling a second time to read its heading mints a parallel set,
/// binds the reference to it, and then discards the relation that owned it,
/// which is how `orders_2.id` came to name a FROM entry no statement contains.
///
/// SCOPE-LOCAL: walks this scope's Filter spine and reaches each EXISTS's own
/// innermost source. It does not descend into arbitrary nested subquery scopes,
/// and it resolves nothing.
/// The relations sibling truth witnesses have already established on this
/// source. Interdependent existence — `+orders(...), +order_items(,
/// orders.id = order_items.order_id)` — addresses a sibling witness BY
/// NAME, so each one has to enter the correlation as a qualifier scope and
/// not merely as a bag of loose columns.
fn exists_witness_relations(
    expr: &ast_resolved::Chain,
    found: &mut Vec<crate::relation::SemanticRelation>,
) {
    for continuation in expr.forms() {
        // A restriction's condition, and a correlated restriction's: the
        // probe an existence names is a witness whichever join evaluates
        // the condition around it.
        let condition = match continuation {
            ast_resolved::Continuation::Restrict { condition, .. } => Some(condition),
            ast_resolved::Continuation::Correlated(correlated) => Some(correlated.condition()),
            _ => None,
        };
        if let Some(ast_resolved::TruthExpression::Existence(Existence {
            relation: subquery,
            ..
        })) = condition
        {
            let relation = resolved_innermost_source(subquery).semantic_relation();
            if !found.contains(&relation) {
                found.push(relation);
            }
        }
        match continuation {
            ast_resolved::Continuation::Restrict { .. }
            | ast_resolved::Continuation::Correlated(_) => {}
            ast_resolved::Continuation::Member { rhs, .. } => {
                exists_witness_relations(rhs, found);
            }
            ast_resolved::Continuation::BagOp { arm, .. } => {
                exists_witness_relations(arm, found);
            }
            ast_resolved::Continuation::Access { .. }
            | ast_resolved::Continuation::Bound { .. }
            | ast_resolved::Continuation::Correlate { .. }
            | ast_resolved::Continuation::Destructure { .. }
            | ast_resolved::Continuation::Pipe { .. }
            | ast_resolved::Continuation::Structural(_) => {}
            ast_resolved::Continuation::ErJoin(_) => {
                unreachable!("ER chains are expanded during resolution")
            }
        }
    }
}

/// The innermost source of a resolved subquery: peel `Filter` only, and stop at
/// any node that is itself a boundary. The resolved twin of
/// `extract_innermost_source`, and it stops in the same places for the same
/// reason — a `Pipe` publishes its own heading, so descending past one would
/// answer with a heading the subquery's table does not have.
fn resolved_innermost_source(expr: &ast_resolved::Chain) -> ast_resolved::Chain {
    let mut peeled = expr.continuations();
    while let Some((last, rest)) = peeled.split_last() {
        if !matches!(
            last.form(),
            ast_resolved::Continuation::Restrict { .. }
                | ast_resolved::Continuation::Correlated(_)
                | ast_resolved::Continuation::Bound { .. }
                | ast_resolved::Continuation::Destructure { .. }
        ) {
            break;
        }
        peeled = rest;
    }
    expr.prefix(peeled.len())
}

/// HOW A FOLD'S CORRELATIONS ARE EVALUATED.
///
/// An interior relation in join position is realized by HOISTING: a
/// restriction that reads the enclosing row is evaluated at the enclosing
/// join, so the relation it stands on owes the interior occurrences it
/// reads until the interior boundary. Every other interior — an existence,
/// a membership, a scalar subquery — is evaluated IN PLACE as a correlated
/// subquery of the target's own, and owes nothing. The fold that opens an
/// interior states which, and only the join-position interior states
/// `Hoisted`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Correlations {
    InPlace,
    Hoisted,
}

/// Which dequalifying run a correlation is answering — `.(cols)` names the
/// shared columns, `.*` asks for every one there is — borrowed from the
/// access that spelled it for the one act that performs it.
#[derive(Clone, Copy)]
pub(crate) enum CorrelatingRun<'a> {
    Named(&'a [delightql_types::SqlIdentifier]),
    All,
}

impl<'a> CorrelatingRun<'a> {
    /// The run a head's own access spells, if it dequalifies.
    pub(crate) fn of_access(access: &'a ast_unresolved::Access) -> Option<Self> {
        match access {
            ast_unresolved::Access::Dequalify(columns) => Some(CorrelatingRun::Named(columns)),
            ast_unresolved::Access::DequalifyAll => Some(CorrelatingRun::All),
            ast_unresolved::Access::Unasked
            | ast_unresolved::Access::All
            | ast_unresolved::Access::Slots(_) => None,
        }
    }
}

// ============================================================================
// ER-Rule Expansion
// ============================================================================

/// The context for an ER expression, from its operator symbols: a chain
/// names ONE context. Every step carries one (an omitted symbol was
/// written as `normal` at normalization), so a step's context is never
/// inferred from its neighbours.
pub(crate) fn er_chain_context(contexts: &[String]) -> Result<ast_unresolved::ErContextSpec> {
    let Some(first) = contexts.first() else {
        return Err(Internal::invariant(
            "resolver::er_chain_context",
            "an ER chain has at least one step",
        ));
    };
    if contexts.iter().any(|context| context != first) {
        return Err(DelightQLError::from(Er::MixedContexts {
            message: format!(
                "one chain, one context — this chain names multiple contexts including ::{first}"
            ),
        }));
    }
    Ok(ast_unresolved::ErContextSpec {
        namespace: None,
        context_name: first.clone(),
    })
}

/// THE ONE AUTHORITY on which columns a pivot's keys come from.
///
/// It runs after resolution because that is the earliest point at which the
/// question can be asked at all: a pivot key is matched by PUBLISHED symbol,
/// and an ordinal has no name until the heading is known. Asking earlier as
/// well, and merging, gave the two addressings two different laws — `|2| in
/// (…)` and `subject in (…)` over the same column then disagreed about
/// whether an `IN` beneath an `or` supplies the columns.
///
/// A pivot's keys become the output HEADING, so only a genuinely exhaustive
/// constraint may supply them: an `IN` under `Or` leaves the other arm free
/// to admit rows outside the key set, and one under `Not` or a negated `IN`
/// excludes rather than enumerates. Those shapes yield nothing here, and the
/// pivot refuses for want of a matching predicate rather than silently
/// dropping the columns they would have omitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PivotInWitness {
    ColumnNames(Vec<String>),
    UnnameableValues,
}

pub(crate) type PivotInWitnesses = HashMap<crate::names::Sym, PivotInWitness>;

fn extract_in_predicate_values_from_resolved(
    source: &ast_resolved::Chain,
    identities: &crate::relation::Planning,
) -> PivotInWitnesses {
    let mut result = HashMap::new();
    scan_resolved_for_in_predicates(source, &mut result, identities);
    result
}

#[stacksafe::stacksafe]
fn scan_resolved_for_in_predicates(
    expr: &ast_resolved::Chain,
    result: &mut PivotInWitnesses,
    identities: &crate::relation::Planning,
) {
    for continuation in expr.forms() {
        match continuation {
            ast_resolved::Continuation::Restrict { condition, .. } => {
                extract_in_from_resolved_boolean(condition, result, identities);
            }
            ast_resolved::Continuation::Correlated(correlated) => {
                extract_in_from_resolved_boolean(correlated.condition(), result, identities);
            }
            ast_resolved::Continuation::Member { rhs, .. } => {
                scan_resolved_for_in_predicates(rhs, result, identities);
            }
            ast_resolved::Continuation::BagOp { arm, .. } => {
                scan_resolved_for_in_predicates(arm, result, identities);
            }
            ast_resolved::Continuation::Access { .. }
            | ast_resolved::Continuation::Bound { .. }
            | ast_resolved::Continuation::Correlate { .. }
            | ast_resolved::Continuation::Destructure { .. }
            | ast_resolved::Continuation::Pipe { .. }
            | ast_resolved::Continuation::Structural(_) => {}
            ast_resolved::Continuation::ErJoin(_) => {
                unreachable!("ER chains should be resolved before IN predicate scanning")
            }
        }
    }
}

#[stacksafe::stacksafe]
fn extract_in_from_resolved_boolean(
    expr: &ast_resolved::TruthExpression,
    result: &mut PivotInWitnesses,
    identities: &crate::relation::Planning,
) {
    match expr {
        // ONE construct, one carrier: `in` is the same relational membership
        // before and after resolution, so this post-resolution scan matches
        // the form the author wrote. It used to need both spellings, and
        // matching only one of them found nothing.
        ast_resolved::TruthExpression::RelationalMembership(RelationalMembership {
            probe,
            relation: subquery,
            negated: false,
            ..
        }) => {
            // Extract the resolved column name from LHS
            let column = match probe.sole_value() {
                Some(ast_resolved::DomainExpression::Reference(Reference::Named(
                    NamedReference(ColumnOccurrence { column, .. }),
                ))) => Some(*column),
                // Non-column LHS (function call, literal, parenthesized, etc.) — can't
                // provide a column name for pivot optimization. Dispensation: any new
                // DomainExpression variant would also not be a bare column reference.
                _ => None,
            };
            if let Some(column) = column {
                // Walk through Pipe/Qualify wrappers to find the anonymous table
                let inner = unwrap_resolved_pipe(subquery.as_ref());
                if let Some(rows) = extract_literal_rows_from_resolved(&inner) {
                    if !rows.is_empty() {
                        if let Some(name) = identities.published_sym(column.column()) {
                            result.insert(name, PivotInWitness::ColumnNames(rows));
                        }
                    }
                }
            }
        }
        // THE SET SPELLING, READ AFTER RESOLUTION. `c in ("a"; "b")` carries
        // its values inline, and the unresolved scan already reads it — but
        // only when the left side is a NAME. A column addressed by position
        // has no name until the heading is known, so `|2| in ("a"; "b")`
        // reached the pivot with no values and refused. Both addressings name
        // the same occurrence here, which is the whole point of resolving
        // first; the pivot key is matched by published symbol either way.
        ast_resolved::TruthExpression::Membership(Membership {
            probe,
            rows,
            negated: false,
            ..
        }) => {
            let Some(ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
                ColumnOccurrence { column, .. },
            )))) = probe.sole_value()
            else {
                return;
            };
            // A pivot's heading witness is a one-column IN, so only the rows
            // that carry exactly one ground value contribute.
            let values = rows
                .iter()
                .map(|row| match row.0.clone().into_vec().as_slice() {
                    [ast_resolved::DomainExpression::Application(
                        ast_resolved::FunctionApplication::Ground(
                            ast_resolved::LiteralValue::String(text),
                        ),
                    )] => Some(text.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if let Some(name) = identities.published_sym(column.column()) {
                let witness = values
                    .into_iter()
                    .collect::<Option<Vec<_>>>()
                    .filter(|values| !values.is_empty())
                    .map(PivotInWitness::ColumnNames)
                    .unwrap_or(PivotInWitness::UnnameableValues);
                result.insert(name, witness);
            }
        }
        ast_resolved::TruthExpression::Conjunction(parts) => {
            for part in parts.iter() {
                extract_in_from_resolved_boolean(part, result, identities);
            }
        }
        // Negated: pivot only uses positive IN predicates. Both spellings,
        // for the same reason the positive arm names both.
        ast_resolved::TruthExpression::RelationalMembership(RelationalMembership {
            negated: true,
            ..
        })
        | ast_resolved::TruthExpression::Membership(Membership { negated: true, .. }) => {}
        // Or: IN predicates inside OR branches change semantics — don't extract.
        // Not: negation wrapper — no positive IN to extract.
        ast_resolved::TruthExpression::Disjunction(_)
        | ast_resolved::TruthExpression::Not { .. } => {}
        // Remaining boolean expressions: no InRelational predicates inside.
        ast_resolved::TruthExpression::Comparison(Comparison { .. })
        | ast_resolved::TruthExpression::Existence(Existence { .. })
        | ast_resolved::TruthExpression::Sigma(SigmaApplication { .. }) => {}
    }
}

/// The chain with its trailing pipes peeled: the relation the pipes shaped.
fn unwrap_resolved_pipe(expr: &ast_resolved::Chain) -> ast_resolved::Chain {
    let mut peeled = expr.continuations();
    while let Some((step, rest)) = peeled.split_last() {
        if !matches!(step.form(), ast_resolved::Continuation::Pipe { .. }) {
            break;
        }
        peeled = rest;
    }
    expr.prefix(peeled.len())
}

/// Classifications of pipe operators in the chain before a DML terminal.
/// Used for DML shape validation.
#[derive(Debug)]
enum DmlPipeKind {
    Transform,
    ProjectOut,
    Rename,
    TupleOrdering,
    Group,
    General,
}

/// Classify a single unresolved operator into a DmlPipeKind.
/// Used by linearized pipe resolution to build DML pipe ops from collected segments.
fn classify_single_dml_op(op: &ast_unresolved::PipeOp) -> DmlPipeKind {
    match op {
        ast_unresolved::PipeOp::Transform { .. } => DmlPipeKind::Transform,
        ast_unresolved::PipeOp::Project(_) | ast_unresolved::PipeOp::Embed(_) => {
            DmlPipeKind::General
        }
        ast_unresolved::PipeOp::ProjectOut(_) => DmlPipeKind::ProjectOut,
        ast_unresolved::PipeOp::Rename(_) => DmlPipeKind::Rename,
        ast_unresolved::PipeOp::Group(_) => DmlPipeKind::Group,
        ast_unresolved::PipeOp::MapCover { .. } | ast_unresolved::PipeOp::EmbedMapCover { .. } => {
            DmlPipeKind::General
        }
    }
}

fn extract_literal_rows_from_resolved(expr: &ast_resolved::Chain) -> Option<Vec<String>> {
    if let (ast_resolved::GroundForm::Literal(anon), true) =
        (expr.head().form(), expr.continuations().is_empty())
    {
        let rows = &anon.table.body.rows;
        let values: Vec<String> = rows
            .iter()
            .filter_map(|row| {
                if row.len() == 1 {
                    if let ast_resolved::DomainExpression::Application(
                        ast_resolved::FunctionApplication::Ground(
                            ast_resolved::LiteralValue::String(s),
                        ),
                    ) = row.0.first().value()
                    {
                        return Some(s.clone());
                    }
                }
                None
            })
            .collect();
        Some(values)
    } else {
        None
    }
}

/// A reference stood over a relation whose dimensions the target does not
/// publish, so the search that would have found it never happened.
pub(crate) fn opaque_reference_refusal() -> DelightQLError {
    DelightQLError::from(Resolution::Schema {
        message: "a relation in view has a heading the target does not publish, so this \
         reference cannot be settled against it"
            .to_string(),
    })
}
