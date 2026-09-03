// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The ER-EDGE use road of the definition-use authority: one door selects
//! the rule for a pair over the world's reach (the ruled miss teachings
//! included), opens its body, and resolves it in the rule's OWN world —
//! as ONE act. The chain road links atomic admitted edges, each carrying
//! the pair that selected it, and derives adjacency and the shared
//! endpoint from the members themselves. No caller looks a rule up and
//! pairs its pieces by hand, and no chain is assembled from parallel
//! arrays.

pub(crate) use super::admitted::ErEdgeUse;
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_unresolved;
use crate::pipeline::resolver::resolver_fold::ResolverFold;

/// One TERM of an ER path: the canonical spelling that selects an edge
/// rule, and the table the term's read names — the endpoint a composed
/// chain shares with its neighbor.
#[derive(Debug, Clone)]
pub(crate) struct ErTerm {
    pub(crate) spelling: String,
    pub(crate) table: delightql_types::SqlIdentifier,
}

impl ErTerm {
    pub(crate) fn of(spelling: &str, table: delightql_types::SqlIdentifier) -> Self {
        ErTerm {
            spelling: spelling.to_string(),
            table,
        }
    }
}

/// Use the edge (left, right) of `context` — selection over the world's
/// reach, the ruled miss teachings, the ADMISSION of the rule's instance,
/// and the body opening with its error dress, in one act. The admitted
/// edge retains the context and the pair that selected it.
pub(crate) fn use_er_edge<'db>(
    fold: &ResolverFold<'_, 'db>,
    context_name: &str,
    left: ErTerm,
    right: ErTerm,
) -> Result<ErEdgeUse<'db>> {
    let left_name = left.spelling.as_str();
    let right_name = right.spelling.as_str();
    let rule = fold
        .core
        .consult
        .lookup_er_rule(context_name, left_name, right_name, fold.env.reach())?
        .ok_or_else(|| er_edge_miss_error(fold, context_name, left_name, right_name))?;

    let bound = match super::bound_use::bind_definition_use(
        &fold.config.instances,
        rule,
        super::bound_use::NoActuals,
    )? {
        super::bound_use::BoundAdmission::Fresh(bound) => bound,
        super::bound_use::BoundAdmission::Cycle { chain } => {
            return Err(super::bound_use::mutual_recursion_refusal(chain));
        }
        super::bound_use::BoundAdmission::Reenter
        | super::bound_use::BoundAdmission::Widening { .. } => {
            return Err(DelightQLError::validation_error_categorized(
                crate::uri_registry::subcat::RECURSION_CONSULTED_CLAUSE_ORDER,
                format!(
                    "circular consulted-definition expansion: the edge body for \
                     {left_name} & {right_name} in '::{context_name}' is already \
                     being expanded. If the cycle runs through another view or \
                     edge, break the cycle."
                ),
                "resolver::consulted_view_expansion",
            ));
        }
    };

    // The rule body opens in its own world, grounded or not; free
    // references bind through the data-hole law. Nothing rewrites the
    // body's names. The edge PAIRING (this bound use with the body it
    // opened and the terms that selected it) is the admitted authority's
    // own act.
    let dress = |e: DelightQLError| {
        DelightQLError::database_error(
            format!(
                "Error expanding ER-rule body for ({}, {}) in context '{}': {}",
                left.spelling, right.spelling, context_name, e
            ),
            e.to_string(),
        )
    };
    super::admitted::er_edge(bound, context_name, left.clone(), right.clone()).map_err(dress)
}

/// Use a WHOLE chain of edges: the first pair's rule is admitted and
/// opened, and every further term EXTENDS the chain — the chain itself
/// selects, admits, and opens the edge from its last edge's own right term
/// to the next term, so adjacency is one shared token by construction and
/// the common declaration is proven at each link. The composed body
/// resolves inside the chain's one consuming operation in the FIRST edge's
/// declaration world.
pub(crate) fn use_er_chain(
    fold: &mut ResolverFold<'_, '_>,
    context_name: &str,
    terms: &[ErTerm],
) -> Result<crate::pipeline::resolver::ResolvedQuery> {
    let [first_left, first_right, rest @ ..] = terms else {
        return Err(DelightQLError::validation_error(
            "ER-join chain requires at least two relations",
            "Invalid ER-join chain",
        ));
    };
    let mut chain = super::admitted::AdmittedErChain::link(use_er_edge(
        fold,
        context_name,
        first_left.clone(),
        first_right.clone(),
    )?);
    for right in rest {
        chain = chain.then(fold, right.clone())?;
    }
    chain.resolve(fold).map_err(|e| {
        DelightQLError::database_error(
            format!(
                "Error resolving ER-chain body in context '{}': {}",
                context_name, e
            ),
            e.to_string(),
        )
    })
}

/// The context's declared edges as SPELLINGS, for the chain road's graph.
/// The world's reach supplies the edge set; a context declared in more
/// than one reachable namespace refuses, so the walked graph belongs to
/// one declaring namespace.
pub(crate) fn er_context_edges(
    fold: &ResolverFold<'_, '_>,
    context_name: &str,
    left_spelling: &str,
    right_spelling: &str,
) -> Result<Vec<(String, String)>> {
    let rules = fold
        .core
        .consult
        .lookup_er_rules_in_context(context_name, fold.env.reach())?;
    if rules.is_empty() {
        return Err(er_edge_miss_error(
            fold,
            context_name,
            left_spelling,
            right_spelling,
        ));
    }
    // Check for cross-namespace ambiguity
    let namespaces: std::collections::HashSet<String> = rules
        .iter()
        .map(|(_, _, entity)| super::bound_use::family_display_namespace(entity).to_string())
        .collect();
    if namespaces.len() > 1 {
        let ns_list: Vec<String> = namespaces.into_iter().collect();
        return Err(DelightQLError::validation_error(
            format!(
                "Ambiguous ER-context '{}': rules found in multiple namespaces ({}). \
                 Engage exactly one namespace or use qualified access (ns.view(*)).",
                context_name,
                ns_list.join(", "),
            ),
            "Ambiguous ER-context across namespaces",
        ));
    }
    Ok(rules
        .into_iter()
        .map(|(left, right, _)| (left, right))
        .collect())
}

/// The edge-selection failure, in two teachings: an unknown context is
/// its own error (the edge set per context is finite and declared); a
/// known context without the requested pair enumerates what IS declared.
pub(crate) fn er_edge_miss_error(
    fold: &ResolverFold<'_, '_>,
    context_name: &str,
    left_spelling: &str,
    right_spelling: &str,
) -> DelightQLError {
    let known = fold
        .core
        .consult
        .er_context_known(context_name, fold.env.reach())
        .unwrap_or(false);
    if !known {
        let contexts = fold
            .core
            .consult
            .list_er_contexts(fold.env.reach())
            .unwrap_or_default();
        let listing = if contexts.is_empty() {
            "no contexts have declared edges in the enlisted scope".to_string()
        } else {
            format!(
                "contexts with declared edges: {}",
                contexts
                    .iter()
                    .map(|c| format!("::{c}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        return DelightQLError::validation_error_categorized(
            "grounding/er/unknown_context",
            format!("unknown context '::{context_name}' — {listing}"),
            "a context exists exactly where an edge declares it",
        );
    }
    let edges = fold
        .core
        .consult
        .lookup_er_rules_in_context(context_name, fold.env.reach())
        .unwrap_or_default();
    let listing = edges
        .iter()
        .map(|(l, r, _)| format!("{l} & {r}"))
        .collect::<Vec<_>>()
        .join("; ");
    DelightQLError::validation_error_categorized(
        "grounding/er/edge_miss",
        format!(
            "no edge declared for {left_spelling} & {right_spelling} in \
             '::{context_name}' — a term selects an edge by its exact canonical \
             spelling, and emptiness by absent declaration is an error, not a \
             result. Declared edges: {listing}"
        ),
        "restriction is downstream: select a declared edge, then filter its \
         relation",
    )
}

/// One link of a chain being composed: an admitted edge's opened body
/// beside the exact terms that selected it. Built only by the admitted
/// chain's own resolve, from its members.
pub(in crate::defuse) struct ErLink {
    pub(in crate::defuse) body: ast_unresolved::Query,
    pub(in crate::defuse) left: ErTerm,
    pub(in crate::defuse) right: ErTerm,
}

/// THE CHAIN COMPOSITION LAW: adjacent edge bodies merge into one
/// join/filter normal form, sharing EXACTLY their common endpoint — the
/// authority's own finite act over its members' opened bodies. The shared
/// endpoint of every link after the first is the link's own left term,
/// which the chain proved adjacent to its predecessor's right term.
pub(in crate::defuse) fn compose_chain(
    links: Vec<ErLink>,
    context_name: &str,
) -> Result<ast_unresolved::Query> {
    let mut all_relations: Vec<ast_unresolved::Chain> = Vec::new();
    let mut all_conditions: Vec<ast_unresolved::TruthExpression> = Vec::new();
    let mut seen_table_names: std::collections::HashSet<delightql_types::SqlIdentifier> =
        std::collections::HashSet::new();

    for (i, link) in links.into_iter().enumerate() {
        let ErLink { body, left, right } = link;
        let left_name = &left.spelling;
        let right_name = &right.spelling;

        let body_expr = match body.into_bare_body() {
            Ok(expr) => expr,
            Err(_) => {
                return Err(DelightQLError::validation_error(
                    format!(
                        "ER-rule body for ({}, {}) in context '{}' contains CTEs \
                         (not supported in chains)",
                        left_name, right_name, context_name,
                    ),
                    "Invalid ER-rule body",
                ))
            }
        };

        let pair_desc = format!("{left_name} & {right_name} in '::{context_name}'");
        let (body_rels, body_conds) =
            crate::pipeline::resolver::flatten_unresolved_body(body_expr, &pair_desc)?;

        // Merge relations. Adjacent bodies share EXACTLY their common
        // endpoint (this body's left term, introduced by the previous
        // body): that one occurrence deduplicates, once. Any OTHER repeat
        // cannot be aliased apart during composition, and dropping it
        // silently rewrites the join, so it refuses.
        let shared_table = &left.table;
        let mut shared_endpoint_budget = if i > 0 { 1usize } else { 0 };
        for read in body_rels {
            if let Ok(name) = crate::pipeline::resolver::er_table_name(&read) {
                if seen_table_names.insert(name.clone()) {
                    all_relations.push(read);
                } else if shared_endpoint_budget > 0 && name == *shared_table {
                    shared_endpoint_budget -= 1;
                } else {
                    return Err(DelightQLError::validation_error_categorized(
                        "grounding/er/chain_shared_repeat",
                        format!(
                            "composing the chain repeats relation '{name}' beyond \
                             the shared endpoint — the edge body for {pair_desc} \
                             reintroduces it after an earlier body (or the same \
                             body) already did. Adjacent edge bodies share only \
                             their common endpoint; other repeats cannot be \
                             aliased apart during composition. Restructure the \
                             bodies, or call the edges directly with &"
                        ),
                        "a chain merges adjacent bodies on their shared endpoint only",
                    ));
                }
            } else {
                all_relations.push(read);
            }
        }
        all_conditions.extend(body_conds);
    }

    let combined_expr =
        crate::pipeline::resolver::rebuild_flat_expression(all_relations, all_conditions)?;
    Ok(crate::pipeline::resolver::add_self_aliases_to_query(
        ast_unresolved::Query::relational(combined_expr),
    ))
}
