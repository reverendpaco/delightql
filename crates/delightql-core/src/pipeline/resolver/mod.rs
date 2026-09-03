// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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

/// EVERYTHING a pattern slot needs to instantiate a definition, or
/// nothing: the resolver core, the ONE lexical world the slot stands in,
/// and the compilation's allowance travel together, so a road that can
/// instantiate cannot lack the bound and cannot choose a world of its own.
#[derive(Clone, Copy)]
pub(crate) struct SlotInstantiation<'a, 'db> {
    pub(in crate::pipeline::resolver) core: &'a crate::resolution::ResolverCore<'db>,
    pub(in crate::pipeline::resolver) env: &'a crate::defuse::environment::Environment,
    pub(in crate::pipeline::resolver) instances: &'a crate::defuse::instance::InstanceTable,
    /// A SCOPED body's own formal bindings ride the instantiation itself —
    /// one value carries the world and the frame, so they cannot be paired
    /// independently. A declaration body's formals live on its world and
    /// this is `None`.
    pub(in crate::pipeline::resolver) formals:
        Option<&'a crate::defuse::environment::FormalBindings>,
    pub(in crate::pipeline::resolver) horizon: Option<crate::pipeline::asts::core::LexicalHorizon>,
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
        self.env.select_query_local(name, demand, self.horizon)
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
            formals: None,
            horizon: None,
        }
    }

    /// An allowance for a SCOPED body: its own frame standing on this
    /// allowance's world.
    pub(crate) fn in_scoped<'b>(
        &self,
        frame: &'b crate::defuse::environment::FormalBindings,
        horizon: crate::pipeline::asts::core::LexicalHorizon,
    ) -> SlotInstantiation<'b, 'db>
    where
        'a: 'b,
    {
        SlotInstantiation {
            core: self.core,
            env: self.env,
            instances: self.instances,
            formals: Some(frame),
            horizon: Some(horizon),
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
    Comparison, Existence, GroundForm, Membership, RelationalMembership, SigmaApplication, Step,
};
use crate::pipeline::asts::core::{NamedReference, Reference};
use resolver_fold::ResolverFold;

pub(crate) use caller_row::CallerRow;
pub(crate) use lexical::Terminal;
pub(crate) use lexical::{
    AnonRouting, PatternOperand, PatternOwner, Position, RowRead,
    Reach, ResolvedQuery, ResolvedRelation, StrictPhaseConverter, Witness,
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
        return Err(DelightQLError::validation_error_categorized(
            "heads/clause_disagreement",
            format!(
                "the clauses of '{name}' publish different headings: clause 1 \
                 publishes ({first}) and clause {clause} publishes ({other}). \
                 A glob head takes its schema from the bodies, so every clause \
                 must publish one heading — declare the shared heading in the \
                 head, or project each body to it.",
                first = expected.join(", "),
                clause = index + 1,
                other = published.join(", "),
            ),
            "clause accumulation",
        ));
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
            self.env.push_horizon(horizon);
        }
        let resolved = self
            .resolve_relational(expr)
            .map(|resolved| resolved.into_body());
        if !horizon.is_all() {
            self.env.pop_horizon();
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
        return Err(DelightQLError::validation_error_categorized(
            "inchoate/latent_name",
            format!(
                "the dimension is latent: '{text}()' names no columns until the \
                 occurrence is accessed"
            ),
            "access the relation — write `(*)` or a slot group — or reach the \
             dimension by position (`|N|`)",
        ));
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
            return Err(DelightQLError::parse_error(format!(
                "CFE '{}' declares empty explicit context '..{{}}' but this is unnecessary. \
                 Remove the context marker entirely: {}:({}): ...",
                cfe.name,
                cfe.name,
                cfe.formals
                    .iter()
                    .map(|formal| formal.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
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
        match continuation {
            ast_resolved::Continuation::Restrict { condition, .. } => {
                if let ast_resolved::TruthExpression::Existence(Existence {
                    relation: subquery,
                    ..
                }) = condition
                {
                    let relation = resolved_innermost_source(subquery).semantic_relation();
                    if !found.contains(&relation) {
                        found.push(relation);
                    }
                }
            }
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
                | ast_resolved::Continuation::Bound { .. }
                | ast_resolved::Continuation::Destructure { .. }
        ) {
            break;
        }
        peeled = rest;
    }
    expr.prefix(peeled.len())
}

/// Which dequalifying run a correlation is answering, borrowed for the call.
#[derive(Clone, Copy)]
pub(crate) enum CorrelatingRun<'a> {
    Named(&'a [delightql_types::SqlIdentifier]),
    All,
}

/// The same, owned, while the interior is resolved and the access is spent.
pub(super) enum OwnedCorrelatingRun {
    Named(Vec<delightql_types::SqlIdentifier>),
    All,
}

impl OwnedCorrelatingRun {
    pub(super) fn borrow(&self) -> CorrelatingRun<'_> {
        match self {
            OwnedCorrelatingRun::Named(columns) => CorrelatingRun::Named(columns),
            OwnedCorrelatingRun::All => CorrelatingRun::All,
        }
    }
}

// ============================================================================
// ER-Rule Expansion
// ============================================================================

/// Extract the table name from an unresolved Relation.
/// The context for an ER expression, from its operator symbols. A bare
/// operator (the removed `under` dialect's spelling) refuses with the
/// symbol-form teaching; a chain names ONE context.
pub(crate) fn er_chain_context(
    contexts: &[Option<String>],
) -> Result<ast_unresolved::ErContextSpec> {
    let named: Vec<&String> = contexts.iter().flatten().collect();
    if named.is_empty() || contexts.iter().any(|c| c.is_none()) {
        return Err(DelightQLError::validation_error_categorized(
            "grounding/er/bare_operator",
            "the ER operators take their context as a symbol on the operator: \
             &(::your_context) for a direct edge, &&(::your_context) for the \
             transitive walk"
                .to_string(),
            "contexts are symbols; the edge set per context is finite and declared",
        ));
    }
    let first = named[0];
    if named.iter().any(|context| *context != first) {
        return Err(DelightQLError::validation_error_categorized(
            "grounding/er/mixed_contexts",
            format!(
                "one chain, one context — this chain names multiple contexts including ::{first}"
            ),
            "split the chain, or declare the edges in one context",
        ));
    }
    Ok(ast_unresolved::ErContextSpec {
        namespace: None,
        context_name: first.clone(),
    })
}

/// The endpoint's (table name, user alias) — the alias is OUTSIDE the
/// term: selection used the spelling, exports answer to the alias.
/// Whether an authored edge peer carries the outer mark on its access.
fn er_peer_outer(read: &ast_unresolved::Chain) -> bool {
    matches!(
        read.as_read_relation(),
        Some(ast_unresolved::Relation::Ground { outer: true, .. })
    )
}

/// Mark the edge body's own read of the stated endpoint outer, so the
/// authored `?` on the peer keeps every left row through the expanded join.
/// The mark rides the ACCESS, and the body's access of the endpoint is the
/// one place the expanded join can honor it.
pub(crate) fn mark_er_endpoint_outer(
    mut expr: ast_unresolved::Chain,
    endpoint: &str,
) -> Result<ast_unresolved::Chain> {
    fn marked_relation(
        rel: ast_unresolved::Relation,
        endpoint: &str,
        marked: &mut usize,
    ) -> ast_unresolved::Relation {
        match rel {
            ast_unresolved::Relation::Ground {
                mention:
                    ast_unresolved::GroundMention::Named {
                        identifier,
                        alias,
                        mutation_target,
                        passthrough,
                    },
                ..
            } if delightql_types::SqlIdentifier::str_eq(identifier.name.as_str(), endpoint) => {
                *marked += 1;
                ast_unresolved::Relation::Ground {
                    mention: ast_unresolved::GroundMention::Named {
                        identifier,
                        alias,
                        mutation_target,
                        passthrough,
                    },
                    outer: true,
                }
            }
            other => other,
        }
    }
    let mut marked = 0usize;
    *expr.continuations_mut() = std::mem::take(expr.continuations_mut())
        .into_iter()
        .map(|step| {
            ast_unresolved::Step::authored(match step.into_form() {
                ast_unresolved::Continuation::Member {
                    mut rhs,
                    correlation,
                    join_type,
                } => {
                    if rhs.as_read_relation().is_some() {
                        *rhs.head_mut() =
                            ast_unresolved::Grelex::authored(match rhs.head_mut().form().clone() {
                                ast_unresolved::GroundForm::Reference(rel) => {
                                    ast_unresolved::GroundForm::Reference(marked_relation(
                                        rel,
                                        endpoint,
                                        &mut marked,
                                    ))
                                }
                                literal => literal,
                            });
                    }
                    ast_unresolved::Continuation::Member {
                        rhs,
                        correlation,
                        join_type,
                    }
                }
                other => other,
            })
        })
        .collect();
    if marked == 0 {
        *expr.head_mut() = ast_unresolved::Grelex::authored(match expr.head_mut().form().clone() {
            ast_unresolved::GroundForm::Reference(rel) => {
                ast_unresolved::GroundForm::Reference(marked_relation(rel, endpoint, &mut marked))
            }
            literal => literal,
        });
    }
    match marked {
        1 => Ok(expr),
        0 => Err(DelightQLError::validation_error_categorized(
            "grounding/er/outer_endpoint",
            format!(
                "the outer mark reaches the edge body's read of '{endpoint}',                  and this body does not read it directly"
            ),
            "an edge peer's `?` marks the body's own access of that endpoint",
        )),
        _ => Err(DelightQLError::validation_error_categorized(
            "grounding/er/outer_endpoint",
            format!("the edge body reads '{endpoint}' more than once, so the outer mark is ambiguous"),
            "an edge peer's `?` marks the body's own access of that endpoint",
        )),
    }
}

fn er_endpoint(read: &ast_unresolved::Chain) -> (String, Option<delightql_types::SqlIdentifier>) {
    match read.as_read_relation() {
        Some(ast_unresolved::Relation::Ground {
            mention:
                ast_unresolved::GroundMention::Named {
                    identifier, alias, ..
                },
            ..
        }) => (identifier.name.to_string(), alias.clone()),
        _ => (String::new(), None),
    }
}

/// Publish an edge's boundary: of everything the body provides, keep the two
/// endpoints' columns, in endpoint order, each answering to its endpoint.
/// Returns the name of an endpoint the body publishes nothing for.
///
/// The body is a body like any other — resolved as written, its own law, no
/// ER dialect. The narrowing is NOT a projection the compiler writes over it:
/// spelling it as `|> (a.*, b.*)` appended to the body makes the compiler
/// author `a(*) … |> (a.*) |> (a.*)` whenever the author already projected,
/// which is the one shape the language refuses. Which endpoint a column
/// belongs to is a fact the arena already holds, so the boundary asks the
/// arena instead of asking by name past a boundary that consumed the name.
///
/// Which endpoint a column answers to is a fact about the COLUMN. One hop
/// exports both of its endpoints into a single boundary scope, so deciding
/// per scope can record only one of the two, and the other endpoint's columns
/// come out answering to the wrong name — which is invisible until a second
/// hop tries to pair on the shared endpoint and finds nothing answering to it.
/// The scope a column was born in is what tells the two apart; the scope it
/// sits in now is shared by both and tells them apart from nothing.
/// An edge whose body does not publish one of its endpoints, taught as the
/// pair-set violation it is rather than as whatever the narrowing tripped on.
fn er_pair_schema_error(missing: &str, context: &str, subject: String) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "grounding/er/pair_schema",
        format!(
            "{subject} in '::{context}' does not publish '{missing}' — an edge \
             is a PAIR-SET: its body may derive the pairs freely (filter, \
             helper joins, computed keys, aggregates) but its final heading \
             must carry both endpoints' columns; they are the edge's published \
             schema. Rename and narrow at the call site, after selection, not \
             inside the edge"
        ),
        "the published schema of an edge is schema(A) + schema(B); \
         the boundary exports those columns and hides the rest",
    )
}

/// Which of a path's endpoints one published port answers for.
///
/// ONE question, asked wherever an ER composition needs it. The
/// construction-recorded semantic owner is the endpoint's published schema; a
/// derived endpoint may legitimately name its columns differently from its
/// storage source, so that owner wins. Where a projection consumed the
/// lexical qualifier while carrying the exact port, the recorded ancestry
/// says which earlier port reached this output — a rename is excluded because
/// it changed the published spelling, not because a value search failed.
fn er_endpoint_of(
    identities: &crate::relation::Planning,
    input: &crate::relation::SemanticRelation,
    column: crate::relation::PortId,
    endpoints: &[crate::names::Sym],
) -> Option<crate::names::Sym> {
    let authority = identities.authority();
    if let Some(answer) = authority
        .owner(column)
        .ok()
        .and_then(|scope| identities.answers_to(scope))
    {
        if endpoints.contains(&answer) {
            return Some(answer);
        }
    }
    // The ANSWERS are compared, not the ancestors: a port carried through
    // several boundaries has several ancestors, and every one of them
    // naming the same endpoint is agreement rather than ambiguity. Two
    // DIFFERENT endpoints is the ambiguity, and it refuses.
    let mut answers: Vec<_> = authority
        .ancestors_into(input, column)
        .unwrap_or_default()
        .into_iter()
        .filter(|ancestor| {
            identities.published_sym(ancestor.column()) == identities.published_sym(column.column())
        })
        .filter_map(|ancestor| {
            authority
                .owner(ancestor)
                .ok()
                .and_then(|scope| identities.answers_to(scope))
        })
        .filter(|endpoint| endpoints.contains(endpoint))
        .collect();
    answers.sort_unstable();
    answers.dedup();
    match answers.as_slice() {
        [endpoint] => Some(*endpoint),
        [] | [..] => None,
    }
}

/// Whether a relation publishes a column for every named endpoint.
///
/// An edge is a PAIR-SET: its body derives the pairs freely, but its final
/// heading has to carry both endpoints, because that heading IS the edge's
/// published schema. A body that renamed or projected an endpoint away
/// publishes no column born under it, and there is nothing to export.
///
/// A CHECK ONLY. The composed-path road already stands on its own boundary
/// and asks this of it; deriving a second one there would publish a
/// qualifier scope the chain does not carry.
fn er_missing_endpoint(
    published: &[String],
    identities: &crate::relation::Planning,
    input: crate::relation::SemanticRelation,
) -> Option<String> {
    let mut endpoints = Vec::with_capacity(published.len());
    for name in published {
        let Some(endpoint) = identities.known_sym(name, false) else {
            return Some(name.clone());
        };
        endpoints.push(endpoint);
    }
    let Ok(provided) = crate::relation::published_ports(identities, &input) else {
        return published.first().cloned();
    };
    for (name, endpoint) in published.iter().zip(&endpoints) {
        if !provided.iter().any(|column| {
            er_endpoint_of(identities, &input, *column, &endpoints) == Some(*endpoint)
        }) {
            return Some(name.clone());
        }
    }
    None
}

/// STAND THE EDGE'S BOUNDARY OVER ITS RESOLVED BODY.
///
/// The exports are decided, the boundary is derived from them, and the
/// projection that carries each endpoint into the position that derivation
/// minted is built in the SAME act — so the boundary relation is never a
/// value standing beside a projection somebody else wrote.
///
/// Built resolved rather than appended as `|> (a.*, b.*)` before
/// resolution. The written form makes the compiler author
/// `a(*) … |> (a.*) |> (a.*)` whenever the body already projected — the one
/// shape the language refuses — and then needs a qualifier to survive a
/// projection so its own output can compile. Which occurrence belongs to
/// which endpoint is a fact the arena holds; asking it directly costs no
/// licence.
///
/// `Err` is an endpoint the body publishes nothing for; `Ok` is the body
/// with its boundary over it.
fn er_export_endpoints(
    resolved: ResolvedRelation,
    published: &[String],
    aliases: &[Option<delightql_types::SqlIdentifier>],
    identities: &crate::relation::Planning,
    missing: &mut Option<String>,
) -> Result<ResolvedRelation> {
    // AN ENDPOINT EXPORT REPUBLISHES: schema(A) + schema(B) is a new
    // publication the prior spellings do not reach around, so what answers
    // over the result is derived from the boundary this writes.
    resolved.republished_as_er_boundary(identities, |expr| {
        er_export_endpoints_chain(published, aliases, identities, expr, missing)
    })
}

fn er_export_endpoints_chain(
    published: &[String],
    aliases: &[Option<delightql_types::SqlIdentifier>],
    identities: &crate::relation::Planning,
    expr: ast_resolved::Chain,
    missing: &mut Option<String>,
) -> Result<(ast_resolved::Chain, Vec<crate::relation::form::ErExport>)> {
    let input = expr.semantic_relation();
    if let Some(absent) = er_missing_endpoint(published, identities, input) {
        *missing = Some(absent);
        return Ok((expr, Vec::new()));
    }
    let mut endpoints = Vec::with_capacity(published.len());
    for name in published {
        let Some(endpoint) = identities.known_sym(name, false) else {
            *missing = Some(name.clone());
            return Ok((expr, Vec::new()));
        };
        endpoints.push(endpoint);
    }
    // THE ALIAS IS THE ENDPOINT'S NEW ANSWER, AND THE BOUNDARY IS WHERE IT
    // LANDS. An alias written outside the term names the EDGE's endpoint, not
    // the base table the body read: the body is resolved against the declared
    // spellings, and the boundary is the first relation the caller addresses.
    // Threading the alias afterwards has nothing to rename — the boundary is a
    // wrap and answers to no name at all — so the answering channel each
    // position already carries is the one the alias replaces.
    let answers: Vec<crate::names::Sym> = endpoints
        .iter()
        .zip(aliases.iter().chain(std::iter::repeat(&None)))
        .map(|(endpoint, alias)| match alias {
            Some(alias) => {
                identities.canonical(identities.intern(alias.as_str(), alias.is_stropped()))
            }
            None => *endpoint,
        })
        .collect();
    let endpoint_of =
        |column: crate::relation::PortId| er_endpoint_of(identities, &input, column, &endpoints);
    let answer_for = |endpoint: crate::names::Sym| {
        endpoints
            .iter()
            .position(|declared| *declared == endpoint)
            .map_or(endpoint, |position| answers[position])
    };
    let Ok(provided) = crate::relation::published_ports(identities, &input) else {
        *missing = published.first().cloned();
        return Ok((expr, Vec::new()));
    };
    // ONE boundary, because an edge publishes ONE heading: schema(A) +
    // schema(B). Minting a boundary per input scope leaves the two endpoints'
    // columns with no scope in common, so nothing downstream can name the
    // edge's result — and the alias a caller wrote outside the term lands on
    // the base table instead of on the edge.
    //
    // A helper join's columns reach here too; the body may join whatever it
    // likes. Only the endpoints' columns cross, which is what "the boundary
    // exports those columns and hides the rest" means — the rest are dropped
    // by not being republished.
    let exports: Vec<_> = provided
        .iter()
        .filter_map(|column| {
            endpoint_of(*column).map(|endpoint| crate::relation::form::ErExport {
                source: *column,
                endpoint: answer_for(endpoint),
            })
        })
        .collect();
    // An edge exports its endpoints, so the empty case is unreachable; it
    // answers with the body unchanged rather than an itemless projection.
    if exports.is_empty() {
        return Ok((expr, Vec::new()));
    }
    let sources: Vec<crate::relation::PortId> =
        exports.iter().map(|export| export.source).collect();
    let bounded = identities.authority().extend(
        expr,
        crate::relation::builder::StepOp::Republish {
            of: crate::relation::builder::Republishing::ErBoundary(
                crate::relation::form::ErBoundarySpec {
                    input,
                    exports: &exports,
                },
            ),
            sources,
        },
    )?;
    Ok((bounded, exports))
}

pub(crate) fn er_table_name(
    read: &ast_unresolved::Chain,
) -> Result<delightql_types::SqlIdentifier> {
    match read.as_read_relation() {
        Some(ast_unresolved::Relation::Ground {
            mention: ast_unresolved::GroundMention::Named { identifier, .. },
            ..
        }) => Ok(identifier.name.clone()),
        _ => Err(DelightQLError::validation_error(
            "ER-join operands must be table references (e.g., users_t(*))",
            "Invalid ER-join operand",
        )),
    }
}

/// Expand a direct edge run by looking up ER-rules for each consecutive pair
/// and compiling their bodies through the pipeline.
///
/// For simple pairs (`A & B`): expands the single rule body directly.
///
/// For chains (`A & B & C`): parses each pair's rule body into an unresolved AST,
/// flattens them into (relations, conditions), deduplicates shared intermediate
/// tables, combines into a single unresolved expression, and resolves once.
/// This avoids the duplicate-intermediate-table problem that arises from resolving
/// each pair's body independently.
fn expand_er_join_chain(
    relations: Vec<ast_unresolved::Chain>,
    spellings: &[String],
    context: &ast_unresolved::ErContextSpec,
    fold: &mut ResolverFold,
    endpoints_only: Option<Vec<String>>,
) -> Result<ResolvedRelation> {
    if relations.len() < 2 || spellings.len() != relations.len() {
        return Err(DelightQLError::validation_error(
            "ER-join chain requires at least two relations",
            "Invalid ER-join chain",
        ));
    }

    // A self-pair edge publishes the same table twice: every column name
    // collides with its twin, the endpoint globs bind one operand twice,
    // and the rows come back silently self-paired. Refuse until the
    // boundary can mask the two sides apart.
    if let Some(published) = &endpoints_only {
        let mut seen: Vec<&String> = Vec::new();
        for name in published {
            if seen
                .iter()
                .any(|s| delightql_types::SqlIdentifier::str_eq(s, name))
            {
                return Err(DelightQLError::validation_error_categorized(
                    "grounding/er/self_pair",
                    format!(
                        "the edge publishes '{name}' at two endpoints — a \
                         self-pair edge's sides share every column name and \
                         cannot yet be masked apart, so the pairs would come \
                         back silently self-joined. Spell one side as a \
                         renamed rule view (boss(*) :- employees(*)) and \
                         declare the edge over the distinct terms"
                    ),
                    "an edge's published schema is schema(A) + schema(B); \
                     the two sides must be distinguishable",
                ));
            }
            seen.push(name);
        }
    }

    // An outer-marked peer keeps every left row across ONE edge; a longer
    // walk has no ruled composition for the mark yet.
    if relations.len() > 2 && relations.iter().any(er_peer_outer) {
        return Err(DelightQLError::validation_error_categorized(
            "grounding/er/outer_endpoint",
            "an outer-marked peer stands in an edge chain; the mark composes              across one edge only",
            "mark the peer of a single edge, or join the marked access              separately",
        ));
    }

    // The alias is OUTSIDE the term: selection used the spellings;
    // exports answer to the endpoint aliases, threaded after resolution.
    let (left_endpoint_name, left_endpoint_alias) = er_endpoint(&relations[0]);
    let (right_endpoint_name, right_endpoint_alias) =
        er_endpoint(relations.last().expect("len checked"));

    // THE TERMS OF THE PATH: each term's canonical spelling selects an
    // edge rule; the table its read names is the endpoint a composed chain
    // shares with its neighbor. One value per term, so an edge never
    // stands beside terms other than the ones that selected it.
    let terms: Vec<crate::defuse::er::ErTerm> = relations
        .iter()
        .zip(spellings)
        .map(|(read, spelling)| {
            crate::defuse::er::ErTerm::of(
                spelling,
                delightql_types::SqlIdentifier::new(er_endpoint(read).0),
            )
        })
        .collect();

    // For the simple pair case (A & B), just expand the single rule body
    if relations.len() == 2 {
        let resolved_expr = expand_single_er_pair(
            terms[0].clone(),
            terms[1].clone(),
            context,
            fold,
            er_peer_outer(&relations[1]).then_some(right_endpoint_name.as_str()),
        )?;
        // The boundary stands BEFORE the aliases are threaded: an alias a
        // caller wrote outside the term names the EDGE, and threading it into
        // a body with no boundary over it renames the base table instead.
        let resolved_expr = match &endpoints_only {
            Some(published) => {
                let mut missing = None;
                let bounded = er_export_endpoints(
                    resolved_expr,
                    published,
                    &[left_endpoint_alias.clone(), right_endpoint_alias.clone()],
                    &fold.core.identities,
                    &mut missing,
                )?;
                if let Some(missing) = missing {
                    return Err(er_pair_schema_error(
                        &missing,
                        &context.context_name,
                        format!("the edge body for ({}, {})", spellings[0], spellings[1]),
                    ));
                }
                bounded
            }
            None => resolved_expr,
        };
        return er_thread_endpoint_aliases(
            resolved_expr,
            (&left_endpoint_name, &left_endpoint_alias),
            (&right_endpoint_name, &right_endpoint_alias),
            &fold.core.identities,
        );
    }

    // For chains (A & B & C & ...), the authority admits and opens EVERY
    // consecutive pair's rule, links the admitted edges (adjacency and the
    // shared endpoint derived from the terms each edge carries), merges
    // the opened bodies — the ER consumer's own law — and resolves the
    // combined body under the first pair's declared grounding.
    let resolved_query = crate::defuse::er::use_er_chain(fold, &context.context_name, &terms)?;

    match resolved_query.into_relational_body() {
        Ok(expr) => {
            let expr = match &endpoints_only {
                Some(published) => {
                    let mut missing = None;
                    let bounded = er_export_endpoints(
                        expr,
                        published,
                        &[left_endpoint_alias.clone(), right_endpoint_alias.clone()],
                        &fold.core.identities,
                        &mut missing,
                    )?;
                    if let Some(missing) = missing {
                        return Err(er_pair_schema_error(
                            &missing,
                            &context.context_name,
                            "the composed chain".to_string(),
                        ));
                    }
                    bounded
                }
                None => expr,
            };
            er_thread_endpoint_aliases(
                expr,
                (&left_endpoint_name, &left_endpoint_alias),
                (&right_endpoint_name, &right_endpoint_alias),
                &fold.core.identities,
            )
        }
        Err(_) => Err(DelightQLError::validation_error(
            format!(
                "ER-chain body in context '{}' resolved to a non-relational query",
                context.context_name,
            ),
            "Invalid ER-chain body",
        )),
    }
}

/// Rename endpoint tables to their user aliases throughout a resolved
/// ER result (exports answer to the alias; selection already happened
/// by spelling).
fn er_thread_endpoint_aliases(
    resolved: ResolvedRelation,
    left: (&str, &Option<delightql_types::SqlIdentifier>),
    right: (&str, &Option<delightql_types::SqlIdentifier>),
    identities: &crate::relation::Planning,
) -> Result<ResolvedRelation> {
    // The renames touch the base tables INSIDE the chain; the boundary
    // standing outermost — and the endpoint routes bound on it, already
    // spelled with the aliases the exports were derived under — publish
    // nothing new, so what answers over the result stays what answered.
    resolved.republished_within(None, identities, |mut expr| {
        if let (name, Some(alias)) = left {
            expr = rename_in_resolved_expr(expr, name, alias, identities)?;
        }
        if let (name, Some(alias)) = right {
            expr = rename_in_resolved_expr(expr, name, alias, identities)?;
        }
        Ok(expr)
    })
}

/// `&&` composes RELATIONS, not syntax: each hop
/// of the walked path resolves WHOLE through the ordinary direct-edge
/// road (its body free per the pair-set ruling, its boundary export
/// publishing schema(X) + schema(Y)), the hops join on the shared
/// endpoint's full heading (null-safe, row identity by value), and the
/// result publishes the outer endpoints only. Bodies never merge, so
/// nothing is flattened, restricted, or deduplicated.
fn compose_er_chain_relational(
    path: &[String],
    hop_tables: &[String],
    endpoint_aliases: (
        &Option<delightql_types::SqlIdentifier>,
        &Option<delightql_types::SqlIdentifier>,
    ),
    context: &ast_unresolved::ErContextSpec,
    fold: &mut ResolverFold,
) -> Result<ResolvedRelation> {
    use ast_resolved::Chain as RE;
    let identity_arena = fold.core.identities;
    // Reads the name, never interns it: this asks a question per column per
    // hop, and interning appends a spelling every time it is asked. The
    // question itself is `er_endpoint_of`'s — the same one the edge boundary
    // asks — so a path and its own hops cannot disagree about which endpoint
    // a column belongs to.
    let belongs_to = |column: crate::relation::PortId,
                      relation: &crate::relation::SemanticRelation,
                      table: &str| {
        let Some(endpoint) = identity_arena.known_sym(table, false) else {
            return false;
        };
        er_endpoint_of(&identity_arena, relation, column, &[endpoint]).is_some()
    };
    let mut composed: Option<RE> = None;
    let mut all_columns: Vec<crate::relation::PortId> = Vec::new();
    let mut all_relation: Option<crate::relation::SemanticRelation> = None;

    for i in 0..path.len() - 1 {
        let hop_expr = expand_single_er_pair(
            crate::defuse::er::ErTerm::of(
                &path[i],
                delightql_types::SqlIdentifier::new(hop_tables[i].clone()),
            ),
            crate::defuse::er::ErTerm::of(
                &path[i + 1],
                delightql_types::SqlIdentifier::new(hop_tables[i + 1].clone()),
            ),
            context,
            fold,
            // A transitive walk's peers cannot carry the mark; the
            // normalizer refuses the spelling before this road runs.
            None,
        )?;
        // The answering channel is the pairing key: stamp each hop's
        // columns with their endpoint names (the len==2 road's caller
        // does this; here we are the caller).
        //
        // A hop that publishes no column for an endpoint refuses here, as the
        // direct road does. The export builds nothing when it answers a
        // missing endpoint, so a caller that dropped the answer would compose
        // the un-narrowed, un-stamped body and carry a path whose outer
        // endpoint quietly went missing — a wrong answer where the same edge
        // written directly is a refusal.
        if let Some(missing) = er_missing_endpoint(
            &[hop_tables[i].clone(), hop_tables[i + 1].clone()],
            &fold.core.identities,
            hop_expr.semantic_relation(),
        ) {
            return Err(er_pair_schema_error(
                &missing,
                &context.context_name,
                format!(
                    "the edge body for ({}, {})",
                    hop_tables[i],
                    hop_tables[i + 1]
                ),
            ));
        }
        // The hop stands on the body it just resolved. Exporting a later hop
        // over the FIRST body's relation would publish the first hop's
        // positions under every hop's name, so the path's far endpoint would
        // have no column to keep.
        let hop_rel = RE::ground(fold.core.identities.authority().exporting_head(
            GroundForm::Reference(ast_resolved::Relation::InnerRelation {
                pattern: ast_resolved::InnerRelationPattern::UncorrelatedDerivedTable {
                    identifier: ast_resolved::QualifiedName {
                        namespace_path: ast_resolved::NamespacePath::empty(),
                        name: hop_tables[i].clone().into(),
                    },
                    subquery: Box::new(hop_expr.into_body()),
                    is_consulted_view: false,
                },
                alias: None,
                outer: false,
            }),
            crate::relation::form::ExportWhy::ErHop { hop: i as u16 },
        )?);
        let hop_scope = hop_rel.semantic_relation();

        let hop_columns = crate::relation::published_ports(&fold.core.identities, &hop_scope)?;

        if let Some(acc) = composed.take() {
            let shared = &hop_tables[i];
            let mut conditions = Vec::new();
            for right in hop_columns
                .iter()
                .copied()
                .filter(|column| belongs_to(*column, &hop_scope, shared))
            {
                let name = fold.core.identities.published_sym(right.column());
                let matches: Vec<_> = all_columns
                    .iter()
                    .copied()
                    .filter(|column| {
                        all_relation.is_some_and(|acc| belongs_to(*column, &acc, shared))
                            && fold.core.identities.published_sym(column.column()) == name
                    })
                    .collect();
                let [left] = matches.as_slice() else {
                    crate::probe::probing!(er, {
                        crate::probe::probe!(
                            er,
                            "hop {i} shared {shared:?}: {right:?} published={name:?} \
                             found {} candidates among {} accumulated",
                            matches.len(),
                            all_columns.len()
                        );
                        for column in &all_columns {
                            crate::probe::probe!(
                                er,
                                "  acc {column:?} published={:?} addressing={:?} answers_to={}",
                                fold.core.identities.published_sym(column.column()),
                                fold.core.identities.addressing(column.column()),
                                all_relation.is_some_and(|acc| belongs_to(*column, &acc, shared))
                            );
                        }
                    });
                    return Err(DelightQLError::validation_error(
                        "ER chain composition cannot uniquely pair a shared-endpoint column",
                        "Invalid ER-chain composition",
                    ));
                };
                let reference = |column| {
                    Box::new(ast_resolved::DomainExpression::Reference(Reference::Named(
                        NamedReference(ColumnOccurrence::engine(column)),
                    )))
                };
                conditions.push(ast_resolved::TruthExpression::Comparison(Comparison {
                    operator: crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual,
                    left: reference(*left),
                    right: reference(right),
                }));
            }
            let authority = fold.core.identities.authority();
            let left_relation = acc.semantic_relation();
            let right_relation = hop_rel.semantic_relation();
            let _ = left_relation;
            let mut joined_expr = authority.extend(
                acc,
                crate::relation::builder::StepOp::Join {
                    rhs: hop_rel,
                    // The hop pairing is attached as restrictions just
                    // below; the step itself merges nothing.
                    correlation: ast_resolved::MemberCorrelation::Cartesian(()),
                    join_type: None,
                    right: right_relation,
                    kind: crate::relation::form::JoinKind::Inner,
                    merged: &[],
                },
            )?;
            let join_scope = joined_expr.semantic_relation();
            for condition in conditions {
                joined_expr = joined_expr.transparently(ast_resolved::Transparent::Restrict {
                    condition,
                    origin: crate::pipeline::asts::core::FilterOrigin::Generated,
                });
            }
            all_columns = crate::relation::published_ports(&fold.core.identities, &join_scope)?;
            all_relation = Some(join_scope);
            composed = Some(joined_expr);
        } else {
            all_columns = hop_columns;
            all_relation = Some(hop_scope);
            composed = Some(hop_rel);
        }
    }

    let expr = composed.expect("path has at least two spellings");
    let composed_relation = all_relation.expect("a composed path stands on a relation");
    let first_table = &hop_tables[0];
    let last_table = hop_tables.last().expect("nonempty");
    // The composed path publishes the OUTER endpoints only, and each kept
    // position keeps answering to the endpoint it belongs to — the same
    // boundary law a direct edge exports under, so a path and a pair reach
    // their columns the same way.
    let endpoint_named = |table: &str| {
        identity_arena.known_sym(table, false).filter(|endpoint| {
            all_columns.iter().any(|column| {
                er_endpoint_of(&identity_arena, &composed_relation, *column, &[*endpoint]).is_some()
            })
        })
    };
    let mut exports = Vec::new();
    let mut kept = Vec::new();
    for (table, alias) in [
        (first_table, endpoint_aliases.0),
        (last_table, endpoint_aliases.1),
    ] {
        let Some(endpoint) = endpoint_named(table) else {
            continue;
        };
        // THE ALIAS IS THE ENDPOINT'S NEW ANSWER. A path's boundary is the
        // first relation the caller addresses, so an alias written outside
        // the term replaces the answering channel here; there is no scope
        // afterwards for a rename to land on.
        let answer = match alias {
            Some(alias) => fold
                .core
                .identities
                .canonical(identity_arena.intern(alias.as_str(), alias.is_stropped())),
            None => endpoint,
        };
        for column in all_columns.iter().copied() {
            if er_endpoint_of(&identity_arena, &composed_relation, column, &[endpoint]).is_none() {
                continue;
            }
            if kept.contains(&column) {
                continue;
            }
            kept.push(column);
            exports.push(crate::relation::form::ErExport {
                source: column,
                endpoint: answer,
            });
        }
    }
    let input = expr.semantic_relation();
    // Each kept endpoint column republishes itself: the composed chain names
    // nothing anew, so every item carries the occurrence just minted for it —
    // and the boundary and its projection are derived in ONE act.
    let expr = fold.core.identities.authority().extend(
        expr,
        crate::relation::builder::StepOp::Republish {
            of: crate::relation::builder::Republishing::ErBoundary(
                crate::relation::form::ErBoundarySpec {
                    input,
                    exports: &exports,
                },
            ),
            sources: kept,
        },
    )?;
    ResolvedRelation::er_boundary(expr, &exports, &fold.core.identities)
}

/// Flatten an unresolved relational expression into a list of relations and conditions.
/// Walks the Join/Filter tree and collects all leaf Relation nodes and all Filter conditions.
/// Transitive composition (&&) merges edge bodies BEFORE resolution, so a
/// body that carries anything beyond join/filter normal form — a pipe
/// stage, a set operation, a nested edge call — cannot be merged without
/// discarding its semantics; it refuses instead (dropped semantics or a
/// downstream panic is not an admissible fallback).
pub(crate) fn flatten_unresolved_body(
    expr: ast_unresolved::Chain,
    pair_desc: &str,
) -> Result<(
    Vec<ast_unresolved::Chain>,
    Vec<ast_unresolved::TruthExpression>,
)> {
    let mut reads = Vec::new();
    let mut conditions = Vec::new();
    flatten_unresolved_body_inner(expr, &mut reads, &mut conditions, pair_desc)?;
    Ok((reads, conditions))
}

/// A body's READS, not its bare relations: a mention travels with the access
/// its own parens asked for, so merging two bodies cannot leave one holding a
/// relation nobody parameterized.
fn flatten_unresolved_body_inner(
    expr: ast_unresolved::Chain,
    reads: &mut Vec<ast_unresolved::Chain>,
    conditions: &mut Vec<ast_unresolved::TruthExpression>,
    pair_desc: &str,
) -> Result<()> {
    let refuse = |what: &str| -> DelightQLError {
        DelightQLError::validation_error_categorized(
            "grounding/er/chain_normal_form",
            format!(
                "the edge body for {pair_desc} carries {what} — a transitive \
                     chain (&&) merges its edge bodies into one join before \
                     resolution, so each body must be join/filter normal form: \
                     relations and conditions only. Restructure the edge body, \
                     or call the edge directly with &"
            ),
            "transitive composition is structural: bodies merge before resolution",
        )
    };

    if !matches!(expr.head().form(), ast_unresolved::GroundForm::Reference(_)) {
        return Err(refuse("an anonymous table"));
    }
    let (read, steps) = expr.split_read();
    reads.push(read);
    for continuation in steps {
        match continuation.into_form() {
            ast_unresolved::Continuation::Member { rhs, .. } => {
                flatten_unresolved_body_inner(rhs, reads, conditions, pair_desc)?;
            }
            ast_unresolved::Continuation::Restrict { condition, .. } => {
                conditions.push(condition);
            }
            // A second access is a step on the read's RESULT — it reshapes,
            // which normal form does not admit.
            ast_unresolved::Continuation::Access { .. } => {
                return Err(refuse("a further dimension access"));
            }
            ast_unresolved::Continuation::Bound { .. } => {
                return Err(refuse("a row bound (#<n)"));
            }
            ast_unresolved::Continuation::Correlate { .. } => {
                return Err(refuse("a whole-heading correlation"));
            }
            ast_unresolved::Continuation::Destructure { .. } => {
                return Err(refuse("a destructure (~=)"));
            }
            ast_unresolved::Continuation::Pipe { .. } => {
                return Err(refuse("a pipe stage (|>)"));
            }
            ast_unresolved::Continuation::Structural(step) => {
                return Err(refuse(match &step.form {
                    ast_unresolved::StructuralForm::Ordering { .. } => "an ordering (#(…))",
                    ast_unresolved::StructuralForm::Reposition { .. } => "a reposition (*[…])",
                    ast_unresolved::StructuralForm::Meta => "a meta-ize (^)",
                    ast_unresolved::StructuralForm::Witness { .. } => "a witness (+/\\+)",
                    ast_unresolved::StructuralForm::SignedWitness => "a signed witness (+-)",
                    ast_unresolved::StructuralForm::Drill { .. } => "an interior drill (.col(…))",
                    ast_unresolved::StructuralForm::Narrow { .. } => {
                        "a narrowing destructure (.col{…})"
                    }
                }));
            }
            ast_unresolved::Continuation::BagOp { .. } => {
                return Err(refuse("a set operation"));
            }
            ast_unresolved::Continuation::ErJoin(_) => {
                return Err(refuse("a nested edge call"));
            }
        }
    }
    Ok(())
}

/// Rebuild a flat unresolved expression from a list of relations and conditions.
/// Produces a left-deep Join tree of all relations, then wraps with Filter layers
/// for each condition.
pub(crate) fn rebuild_flat_expression(
    reads: Vec<ast_unresolved::Chain>,
    conditions: Vec<ast_unresolved::TruthExpression>,
) -> Result<ast_unresolved::Chain> {
    // Build left-deep join tree from the reads
    let mut iter = reads.into_iter();
    let mut expr = iter.next().ok_or_else(|| {
        DelightQLError::validation_error(
            "ER chain composed to zero relations — the normal-form and \
                 shared-endpoint refusals should have caught this earlier; \
                 this is a dql bug",
            "Invalid ER-join chain",
        )
    })?;
    for read in iter {
        expr = expr.then(Step::authored(ast_unresolved::Continuation::Member {
            rhs: read,
            correlation: None,
            join_type: None,
        }));
    }

    // Wrap with filter layers for each condition
    for cond in conditions {
        expr = expr.then(Step::authored(ast_unresolved::Continuation::Restrict {
            condition: cond,
            origin: crate::pipeline::asts::core::FilterOrigin::UserWritten,
        }));
    }

    Ok(expr)
}

/// Expand a single ER pair (A, B) by looking up the rule and compiling its body.
fn expand_single_er_pair(
    left: crate::defuse::er::ErTerm,
    right: crate::defuse::er::ErTerm,
    context: &ast_unresolved::ErContextSpec,
    fold: &mut ResolverFold,
    outer_endpoint: Option<&str>,
) -> Result<ResolvedRelation> {
    let left_name = left.spelling.clone();
    let right_name = right.spelling.clone();
    let left_name = left_name.as_str();
    let right_name = right_name.as_str();
    // ONE ADMITTED edge use: the body opens, composes (self-aliases so
    // qualified references like `users_t.id` keep working; the outer
    // endpoint mark), and RESOLVES inside the authority under the
    // declaration's own grounding.
    let body_bubbled = crate::defuse::er::use_er_edge(fold, &context.context_name, left, right)?
        .compose_standard(outer_endpoint)?
        .resolve(fold, |e| {
            DelightQLError::database_error(
                format!(
                    "Error resolving ER-rule body for ({}, {}) in context '{}': {}",
                    left_name, right_name, context.context_name, e
                ),
                e.to_string(),
            )
        })?;

    // Extract the relational expression from the resolved query.
    match body_bubbled.into_relational_body() {
        Ok(resolved) => Ok(resolved),
        Err(_) => Err(DelightQLError::validation_error(
            format!(
                "ER-rule body for ({}, {}) in context '{}' resolved to a non-relational query (CTEs in ER-rule bodies are not supported)",
                left_name, right_name, context.context_name,
            ),
            "Invalid ER-rule body",
        )),
    }
}

/// Add self-aliases to Ground relations in a query that don't already have aliases.
/// Transforms `table(*)` into `table(*) as table`. This ensures ConsultedView expansion
/// preserves the original table name as the SQL alias, so qualified references
/// (like `table.col`) in predicates continue to resolve correctly.
pub(crate) fn add_self_aliases_to_query(mut query: ast_unresolved::Query) -> ast_unresolved::Query {
    query.body = add_self_aliases_to_expr(query.body);
    query
}

/// Self-aliasing reaches the joined relations and stops where the chain
/// stops being a plain conjunction: a pipe publishes its own heading, so a
/// relation under one is no longer a self-reference to baptize.
fn add_self_aliases_to_expr(mut expr: ast_unresolved::Chain) -> ast_unresolved::Chain {
    // The trailing run of conjunctive steps is what self-aliasing reaches.
    // Rebuilding it BY VALUE is what lets the run be rewritten without a
    // stand-in relation standing in the slot for a statement: an unresolved
    // ground read must say how it is addressed, and there is no honest thing
    // for a placeholder to say.
    // The head's own access is not a step: it says what the read asks for,
    // so the run reaches past it to the relation it names.
    let span = expr.head_span();
    let stop = expr.continuations()[span..]
        .iter()
        .rposition(|step| {
            !matches!(
                step.form(),
                ast_unresolved::Continuation::Member { .. }
                    | ast_unresolved::Continuation::Restrict { .. }
                    | ast_unresolved::Continuation::Bound { .. }
                    | ast_unresolved::Continuation::Destructure { .. }
            )
        })
        .map_or(span, |index| span + index + 1);
    let reached_head = stop == span;
    *expr.continuations_mut() = std::mem::take(expr.continuations_mut())
        .into_iter()
        .enumerate()
        .map(|(index, step)| {
            ast_unresolved::Step::authored(match step.into_form() {
                ast_unresolved::Continuation::Member {
                    rhs,
                    correlation,
                    join_type,
                } if index >= stop => ast_unresolved::Continuation::Member {
                    rhs: add_self_aliases_to_expr(rhs),
                    correlation,
                    join_type,
                },
                other => other,
            })
        })
        .collect();
    if reached_head {
        *expr.head_mut() = ast_unresolved::Grelex::authored(match expr.head_mut().form().clone() {
            ast_unresolved::GroundForm::Reference(rel) => {
                ast_unresolved::GroundForm::Reference(add_self_alias_to_relation(rel))
            }
            literal => literal,
        });
    }
    expr
}

fn add_self_alias_to_relation(rel: ast_unresolved::Relation) -> ast_unresolved::Relation {
    match rel {
        ast_unresolved::Relation::Ground {
            mention:
                ast_unresolved::GroundMention::Named {
                    identifier,
                    alias: None,
                    mutation_target,
                    passthrough,
                },
            outer,
        } => ast_unresolved::Relation::Ground {
            mention: ast_unresolved::GroundMention::Named {
                alias: Some(identifier.name.clone()),
                identifier,
                mutation_target,
                passthrough,
            },
            outer,
        },
        other => other,
    }
}

#[cfg(test)]
mod self_alias_tests {
    use super::*;

    fn ground(name: &str) -> ast_unresolved::Chain {
        ast_unresolved::Chain::read(
            ast_unresolved::Relation::Ground {
                mention: ast_unresolved::GroundMention::named(ast_unresolved::QualifiedName {
                    namespace_path: ast_unresolved::NamespacePath::empty(),
                    name: name.into(),
                }),
                outer: false,
            },
            ast_unresolved::Access::All,
        )
    }

    fn member(chain: ast_unresolved::Chain, rhs: ast_unresolved::Chain) -> ast_unresolved::Chain {
        chain.then(Step::authored(ast_unresolved::Continuation::Member {
            rhs,
            correlation: None,
            join_type: None,
        }))
    }

    fn qualify(chain: ast_unresolved::Chain) -> ast_unresolved::Chain {
        chain.then(Step::authored(ast_unresolved::Continuation::Access {
            access: ast_unresolved::Access::All,
            named: None,
        }))
    }

    /// The aliases the walk baptized, head first, `None` where it left the
    /// relation unnamed.
    fn aliases(expr: &ast_unresolved::Chain) -> Vec<Option<String>> {
        let mut out = vec![head_alias(&expr.head())];
        for continuation in expr.forms() {
            if let ast_unresolved::Continuation::Member { rhs, .. } = continuation {
                out.extend(aliases(rhs));
            }
        }
        out
    }

    fn head_alias(head: &ast_unresolved::Grelex) -> Option<String> {
        match head.form() {
            ast_unresolved::GroundForm::Reference(ast_unresolved::Relation::Ground {
                mention,
                ..
            }) => mention.alias().map(|alias| alias.to_string()),
            _ => None,
        }
    }

    /// Every relation of a plain conjunction is baptized. The types cannot
    /// say this: the walk decides which steps it reaches.
    #[test]
    fn a_plain_conjunction_baptizes_every_relation() {
        let expr = member(member(ground("a"), ground("b")), ground("c"));
        assert_eq!(
            aliases(&add_self_aliases_to_expr(expr)),
            vec![
                Some("a".to_string()),
                Some("b".to_string()),
                Some("c".to_string())
            ]
        );
    }

    /// A pipe publishes its own heading, so the relation under one is no
    /// longer a self-reference to baptize — and the relations AFTER the pipe
    /// still are. This is the boundary the walk draws, and the one the
    /// by-value rewrite has to keep drawing in the same place.
    #[test]
    fn a_pipe_stops_the_baptism_and_the_steps_after_it_resume() {
        let expr = member(qualify(member(ground("a"), ground("b"))), ground("c"));
        assert_eq!(
            aliases(&add_self_aliases_to_expr(expr)),
            vec![None, None, Some("c".to_string())]
        );
    }

    /// An alias the author wrote is not overwritten.
    #[test]
    fn an_authored_alias_survives() {
        let mut written = ground("a");
        if let ast_unresolved::GroundForm::Reference(ast_unresolved::Relation::Ground {
            mention: ast_unresolved::GroundMention::Named { alias, .. },
            ..
        }) = written.head_mut().form_mut()
        {
            *alias = Some("mine".into());
        }
        assert_eq!(
            aliases(&add_self_aliases_to_expr(written)),
            vec![Some("mine".to_string())]
        );
    }
}

/// Expand a transitive edge by building a graph of all ER-rules in the
/// context, finding a path from left to right, and expanding that path as a
/// direct edge run.
fn expand_er_transitive_join(
    left: ast_unresolved::Chain,
    right: ast_unresolved::Chain,
    left_spelling: &str,
    right_spelling: &str,
    context: &ast_unresolved::ErContextSpec,
    fold: &mut ResolverFold,
) -> Result<ResolvedRelation> {
    // Extract table names (and alias/access) from endpoints
    let (left_name, left_alias) = match left.as_read_relation() {
        Some(ast_unresolved::Relation::Ground {
            mention:
                ast_unresolved::GroundMention::Named {
                    identifier, alias, ..
                },
            ..
        }) => (identifier.name.to_string(), alias.clone()),
        _ => {
            return Err(DelightQLError::validation_error(
                "Left side of && must be a table reference",
                "Invalid ER-transitive-join operand",
            ))
        }
    };
    let (right_name, right_alias) = match right.as_read_relation() {
        Some(ast_unresolved::Relation::Ground {
            mention:
                ast_unresolved::GroundMention::Named {
                    identifier, alias, ..
                },
            ..
        }) => (identifier.name.to_string(), alias.clone()),
        _ => {
            return Err(DelightQLError::validation_error(
                "Right side of && must be a table reference",
                "Invalid ER-transitive-join operand",
            ))
        }
    };

    // Build graph from all ER-rules in context (scoped to namespace if qualified).
    // ER-rules from non-enlisted namespaces are NOT visible at the call site —
    // the caller must enlist!() the namespace to access its ER-rules.
    let rules = crate::defuse::er::er_context_edges(
        fold,
        &context.context_name,
        left_spelling,
        right_spelling,
    )?;

    // Build adjacency list (undirected graph — rules are symmetric)
    let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();
    for (left_t, right_t) in &rules {
        adjacency
            .entry(left_t.clone())
            .or_default()
            .push(right_t.clone());
        adjacency
            .entry(right_t.clone())
            .or_default()
            .push(left_t.clone());
    }

    // BFS over SPELLINGS: the graph's nodes are canonical spellings —
    // an endpoint participates exactly when its written spelling is a
    // declared edge term.
    let path = bfs_path(&adjacency, left_spelling, right_spelling)?;

    // Convert the spelling path to chain relations. Endpoints keep the
    // caller's relations (alias threading); interior hops are entity
    // boundaries whose Relation is only a carrier — the pair bodies
    // supply the real joined relations. A hop's table name is its
    // spelling's functor head.
    let chain_relations: Vec<ast_unresolved::Chain> = path
        .iter()
        .enumerate()
        .map(|(i, spelling)| {
            if i == 0 && left.as_read_relation().is_some() {
                return left.clone();
            }
            if i == path.len() - 1 && right.as_read_relation().is_some() {
                return right.clone();
            }
            let head = spelling.split('(').next().unwrap_or(spelling).trim();
            ast_unresolved::Chain::read(
                ast_unresolved::Relation::Ground {
                    mention: ast_unresolved::GroundMention::named(ast_unresolved::QualifiedName {
                        namespace_path: ast_unresolved::NamespacePath::empty(),
                        name: head.into(),
                    }),
                    outer: false,
                },
                ast_unresolved::Access::All,
            )
        })
        .collect();

    // Endpoints only: intermediate hops contribute nothing to the
    // schema.
    if path.len() > 2 {
        // Relational composition: each hop resolves whole, hops join on
        // the shared endpoint's heading, outer endpoints publish.
        let hop_tables: Vec<String> = path
            .iter()
            .map(|spelling| {
                spelling
                    .split('(')
                    .next()
                    .unwrap_or(spelling)
                    .trim()
                    .to_string()
            })
            .collect();
        let expr = compose_er_chain_relational(
            &path,
            &hop_tables,
            (&left_alias, &right_alias),
            context,
            fold,
        )?;
        if let Some(missing) = er_missing_endpoint(
            &[left_name.clone(), right_name.clone()],
            &fold.core.identities,
            expr.semantic_relation(),
        ) {
            return Err(er_pair_schema_error(
                &missing,
                &context.context_name,
                "the composed chain".to_string(),
            ));
        }
        let expr = er_thread_endpoint_aliases(
            expr,
            (&left_name, &left_alias),
            (&right_name, &right_alias),
            &fold.core.identities,
        )?;
        return Ok(expr);
    }
    // Adjacent pair: the direct road, endpoints only.
    expand_er_join_chain(
        chain_relations,
        &path,
        context,
        fold,
        Some(vec![left_name.clone(), right_name.clone()]),
    )
}

/// Rename a table's alias and all qualifier references throughout a resolved
/// expression tree. Takes ownership and returns the modified expression.
/// Used to apply user aliases from `&&` endpoints after the chain is resolved.
fn rename_in_resolved_expr(
    expr: ast_resolved::Chain,
    old_name: &str,
    new_name: &delightql_types::SqlIdentifier,
    identities: &crate::relation::Planning,
) -> Result<ast_resolved::Chain> {
    // Renaming reaches the conjoined relations and stops where the chain
    // stops being a plain conjunction: past a pipe the heading is the
    // pipe's own, and nothing there answers to the old name.
    let mut reached_head = true;
    let mut prefix_len = 0;
    for (index, continuation) in expr.continuations().iter().enumerate().rev() {
        match continuation.form() {
            ast_resolved::Continuation::Member { .. }
            | ast_resolved::Continuation::Restrict { .. } => {}
            _ => {
                reached_head = false;
                prefix_len = index + 1;
                break;
            }
        }
    }
    let old = identities.canonical(identities.intern(old_name, false));
    let answer = identities.intern(new_name.as_str(), new_name.is_stropped());
    let authority = identities.authority();
    let expr = authority.realias_tail(expr, prefix_len, old, answer, |form| {
        let ast_resolved::Continuation::Member {
            rhs,
            correlation,
            join_type,
        } = form
        else {
            unreachable!("only a member is handed here")
        };
        Ok(ast_resolved::Continuation::Member {
            rhs: rename_in_resolved_expr(rhs, old_name, new_name, identities)?,
            correlation,
            join_type,
        })
    })?;
    let mut expr = expr;
    if reached_head {
        let renames_head = match expr.head().form() {
            ast_resolved::GroundForm::Reference(ast_resolved::Relation::Ground { .. }) => true,
            ast_resolved::GroundForm::Reference(ast_resolved::Relation::ConsultedView {
                ..
            }) => identities.answers_to(expr.head().result().scope()) == Some(old),
            ast_resolved::GroundForm::Reference(ast_resolved::Relation::InnerRelation {
                alias,
                ..
            }) => alias.as_ref().map(|a| a.to_string()).unwrap_or_default() == old_name,
            ast_resolved::GroundForm::Reference(ast_resolved::Relation::FunctorCall { .. })
            | ast_resolved::GroundForm::Literal(_) => false,
        };
        if renames_head {
            let renamed = match expr.head().form().clone() {
                ast_resolved::GroundForm::Reference(ast_resolved::Relation::InnerRelation {
                    pattern,
                    alias: _,
                    outer,
                }) => Some(ast_resolved::GroundForm::Reference(
                    ast_resolved::Relation::InnerRelation {
                        pattern,
                        alias: Some(new_name.clone()),
                        outer,
                    },
                )),
                _ => None,
            };
            authority.realias_head(&mut expr, renamed, old, answer)?;
        }
    }
    Ok(expr)
}

/// A stand-in head used only while a member's chain is moved out for
/// rewriting; it never survives the statement that creates it.
/// Path-finding in the ER graph: enumerate ALL simple paths between the
/// endpoints. Exactly one → that path; zero → no-path error; two or
/// more → the ambiguity error, regardless of relative length — the
/// contract is "if multiple paths exist, the query fails", so a direct
/// edge never silently outranks a longer business path. Enumeration
/// must be exhaustive: a search that stops early (at the shortest, or
/// with a global visited set that suppresses paths sharing an
/// intermediate node) refuses some competitor shapes and silently
/// selects through others, which is worse than either consistent rule.
fn bfs_path(adjacency: &HashMap<String, Vec<String>>, from: &str, to: &str) -> Result<Vec<String>> {
    if from == to {
        return Err(DelightQLError::validation_error(
            "ER-transitive join endpoints must be different tables",
            "Same-table transitive join",
        ));
    }

    // ER contexts are hand-authored and small; simple-path enumeration is
    // cheap there. The expansion cap is a refuse-loudly backstop for a
    // pathologically dense context — uniqueness that cannot be verified
    // is reported, never assumed.
    const MAX_EXPANSIONS: usize = 100_000;
    let mut expansions = 0usize;

    let mut found_paths: Vec<Vec<String>> = Vec::new();
    let mut stack: Vec<Vec<String>> = vec![vec![from.to_string()]];

    while let Some(path) = stack.pop() {
        let current = path.last().unwrap();
        if let Some(neighbors) = adjacency.get(current.as_str()) {
            for neighbor in neighbors {
                expansions += 1;
                if expansions > MAX_EXPANSIONS {
                    return Err(DelightQLError::validation_error(
                        format!(
                            "ER-context too dense to verify a unique join path \
                             from '{}' to '{}'; spell the join explicitly with `&`.",
                            from, to,
                        ),
                        "ER path search cap",
                    ));
                }
                if neighbor == to {
                    let mut p = path.clone();
                    p.push(neighbor.clone());
                    found_paths.push(p);
                } else if !path.contains(neighbor) {
                    let mut p = path.clone();
                    p.push(neighbor.clone());
                    stack.push(p);
                }
            }
        }
    }

    // Deterministic order (shortest first) — the adjacency map is a
    // HashMap, so discovery order is not stable across runs.
    found_paths.sort_by(|a, b| a.len().cmp(&b.len()).then_with(|| a.cmp(b)));

    match found_paths.len() {
        0 => Err(DelightQLError::validation_error(
            format!(
                "No path from '{}' to '{}' in ER-context. \
                 Check that ER-rules connect these tables (directly or transitively).",
                from, to,
            ),
            "No ER path",
        )),
        1 => Ok(found_paths.into_iter().next().unwrap()),
        _ => {
            let path_strs: Vec<String> = found_paths.iter().map(|p| p.join(" -> ")).collect();
            Err(DelightQLError::validation_error(
                format!(
                    "Ambiguous: {} paths from '{}' to '{}':\n  {}",
                    found_paths.len(),
                    from,
                    to,
                    path_strs.join("\n  "),
                ),
                "Ambiguous ER path",
            ))
        }
    }
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

/// Insert correlation filters at the base of a pipe chain, directly above
/// the innermost non-Pipe expression (typically a Ground relation).
/// This ensures the filter's qualifiers match the Ground table name.
#[stacksafe::stacksafe]
fn insert_filters_at_base(
    expr: ast_resolved::Chain,
    filters: Vec<ast_resolved::TruthExpression>,
    identities: &crate::relation::Planning,
) -> Result<ast_resolved::Chain> {
    if filters.is_empty() {
        return Ok(expr);
    }
    // Filters land BELOW the pipes: a pipe publishes its own heading, so a
    // filter written against the base cannot address what the pipe made.
    let (mut expr, trailing) = expr
        .peel_while(|form| matches!(form, ast_resolved::Continuation::Pipe { .. }))
        .into_parts();
    // A filter publishes what it filters, so the pipes that came off still
    // stand on the relation they stood on.

    for filter in filters {
        expr = expr.transparently(ast_resolved::Transparent::Restrict {
            condition: filter,
            origin: ast_resolved::FilterOrigin::Generated,
        });
    }
    identities.authority().reland_all(expr, trailing)
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
    DelightQLError::validation_error_categorized(
        crate::uri_registry::subcat::RESOLUTION_SCHEMA,
        "a relation in view has a heading the target does not publish, so this \
         reference cannot be settled against it",
        "declare the dimensions at the mention — `f(...)(a, b)` names one slot per \
         dimension of the full width",
    )
}
