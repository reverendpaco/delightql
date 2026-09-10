// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The ER road of the definition-use authority. One door selects the rule
//! for a pair of terms over the world's reach (the ruled miss teachings
//! included), admits it, and opens its body; the chain road links atomic
//! admitted edges, each carrying the pair that selected it, so adjacency
//! is one shared term by construction. AN EDGE BODY IS SIMPLE (`shape`):
//! two endpoint reads, at least one connection, filters, nothing else —
//! judged on the text when the edge is declared, re-judged at use, and
//! judged again on the resolved body under its endpoint roles (`body`),
//! where a bare column has an owner and a named function stands for its
//! body — the same judgment on every consuming road. A CHAIN READS EACH
//! TERM ONCE: the chain's scans, each body resolved alone in its own
//! world, its judged conditions rebound over the scans (the admitted
//! chain's `compose`); the walk's path is `walk`'s. The boundary each
//! result stands under is the authority's own (`boundary`).
//!
//! A caller supplies only what it authored: a term's canonical spelling
//! and the alias or outer mark written outside it. The term's read, its
//! relation name, the edge it selects, the occurrence the bodies share,
//! and the heading the result publishes are all derived here.

pub(in crate::defuse) mod body;
mod boundary;
pub(in crate::defuse) mod shape;
mod walk;

pub(in crate::defuse) use boundary::{add_self_aliases_to_query, mark_endpoint_outer};

use super::admitted::{AdmittedErChain, ErEdgeUse};
use crate::diagnostic::{Constraint, Er, Recursion, Resolution};
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_unresolved;
use crate::pipeline::resolver::resolver_fold::ResolverFold;
use crate::pipeline::resolver::ResolvedRelation;
use delightql_types::SqlIdentifier;

/// THE OMITTED CONTEXT. A bare `&`, `&&`, or edge declaration means this
/// context and no other: the normalizer writes it where the symbol was
/// omitted, so a bare and an explicitly normal spelling are one identity
/// before selection ever sees them. Omission never searches other
/// contexts.
pub(crate) const DEFAULT_CONTEXT: &str = "normal";

/// One TERM of an ER path: the canonical spelling that selects an edge
/// rule, and the READ that spelling denotes — the one read a body lends
/// when it spells the term, and the relation whose name the term's
/// endpoint answers to. Both are derived from the spelling; nothing
/// supplies a table beside it.
#[derive(Debug, Clone)]
pub(in crate::defuse) struct ErTerm {
    spelling: String,
    read: ast_unresolved::Chain,
}

impl ErTerm {
    /// The term a canonical spelling denotes. The spelling is the one
    /// canonicalizer's output — a catalog row or a normalized operand — so
    /// it parses as a single relation-access read; anything else is not a
    /// term.
    pub(in crate::defuse) fn spelled(spelling: &str) -> Result<ErTerm> {
        let read = term_read(spelling).ok_or_else(|| {
            DelightQLError::from(Er::OperandTerm {
                message: format!(
                    "'{spelling}' is not a relation-access term — an edge term is \
                     a single read such as people(*) or people(, age >= 18), with \
                     the alias and the outer mark outside it"
                ),
            })
        })?;
        Ok(ErTerm {
            spelling: spelling.to_string(),
            read,
        })
    }

    pub(in crate::defuse) fn spelling(&self) -> &str {
        &self.spelling
    }

    /// The relation the term reads, as written: the name its endpoint
    /// answers to, under the namespace path the term spelled.
    pub(in crate::defuse) fn identifier(&self) -> &ast_unresolved::QualifiedName {
        match self.read.as_read_relation() {
            Some(ast_unresolved::Relation::Ground {
                mention: ast_unresolved::GroundMention::Named { identifier, .. },
                ..
            })
            | Some(ast_unresolved::Relation::InnerRelation {
                pattern: ast_unresolved::InnerRelationPattern::Indeterminate { identifier, .. },
                ..
            }) => identifier,
            _ => unreachable!("a term's read names its relation by construction"),
        }
    }

    /// The relation's name: what the term's endpoint answers to.
    pub(in crate::defuse) fn name(&self) -> &SqlIdentifier {
        &self.identifier().name
    }

    /// The read the spelling denotes, for the composition's judgment.
    pub(in crate::defuse) fn read(&self) -> &ast_unresolved::Chain {
        &self.read
    }
}

/// Normalize a canonical spelling back into the read it denotes: one
/// named relation access standing alone, unaliased and unmarked — a bare
/// ground read (`people(*)`, `ns.people(*)`), or the shaped read a slot row
/// with a constraint normalizes to (`people(, age >= 18)`), which names its
/// relation the same way. Every term the canonicalizer admits is admitted
/// here; this reads the term back, it does not judge it again.
fn term_read(spelling: &str) -> Option<ast_unresolved::Chain> {
    let tree = crate::pipeline::parse::query_sequence(spelling).ok()?;
    let registry = std::rc::Rc::new(crate::names::Registry::new(&[]));
    let normalized = crate::pipeline::normalize::query_sequence(&tree, registry).ok()?;
    let mut queries = normalized.into_queries();
    if queries.len() != 1 {
        return None;
    }
    let body = queries.remove(0).query.into_bare_body().ok()?;
    is_bare_read(&body).then_some(body)
}

/// Whether a chain is one named relation read standing alone: unaliased,
/// unmarked, neither a mutation target nor a passthrough. What a term's
/// read is, and what a body's read of a term must be before its spelling
/// is compared — the alias and the mark stand outside the read's bytes.
pub(super) fn is_bare_read(body: &ast_unresolved::Chain) -> bool {
    matches!(
        body.as_read_relation(),
        Some(ast_unresolved::Relation::Ground {
            mention: ast_unresolved::GroundMention::Named {
                alias: None,
                mutation_target: false,
                passthrough: false,
                ..
            },
            outer: false,
        }) | Some(ast_unresolved::Relation::InnerRelation {
            pattern: ast_unresolved::InnerRelationPattern::Indeterminate { .. },
            alias: None,
            outer: false,
        })
    )
}

/// An AUTHORED operand of `&` or `&&`: the term's canonical spelling (the
/// normalizer's, from the same read) and what the author wrote outside the
/// term — the alias its exports answer to and the outer mark on the peer.
pub(crate) struct ErOperand {
    spelling: String,
    alias: Option<SqlIdentifier>,
    outer: bool,
}

impl ErOperand {
    /// The operand an authored read stands for. A read that is not a named
    /// relation access names no term and refuses.
    pub(crate) fn authored(spelling: String, read: &ast_unresolved::Chain) -> Result<ErOperand> {
        match read.as_read_relation() {
            Some(ast_unresolved::Relation::Ground {
                mention: ast_unresolved::GroundMention::Named { alias, .. },
                outer,
            })
            | Some(ast_unresolved::Relation::InnerRelation {
                pattern: ast_unresolved::InnerRelationPattern::Indeterminate { .. },
                alias,
                outer,
            }) => Ok(ErOperand {
                spelling,
                alias: alias.clone(),
                outer: *outer,
            }),
            _ => Err(DelightQLError::from(Er::OperandTerm {
                message: "ER-join operands must be table references (e.g., users_t(*))".to_string(),
            })),
        }
    }
}

/// THE DECLARATION'S JUDGMENT: an edge body outside the simple shape
/// refuses when it is declared, naming the offending part. The
/// normalizer's edge arm asks this of every body it admits.
pub(crate) fn judge_declared_body(
    body: &ast_unresolved::Query,
    reads: &crate::term_spec::BodyReads,
    left_spelling: &str,
    right_spelling: &str,
    context: &str,
) -> Result<()> {
    let left = ErTerm::spelled(left_spelling)?;
    let right = ErTerm::spelled(right_spelling)?;
    let pair = format!("{left_spelling} & {right_spelling} in '::{context}'");
    shape::judge(body, reads, &left, &right, &pair).map(|_| ())
}

/// Use the edge (left, right) of `context` — selection over the world's
/// reach, the ruled miss teachings, the ADMISSION of the rule's instance,
/// and the body opening with its error dress, in one act. The admitted
/// edge retains the context and the pair that selected it.
pub(in crate::defuse) fn use_er_edge<'db>(
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
        .ok_or_else(|| edge_miss_error(fold, context_name, left_name, right_name))?;

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
            return Err(DelightQLError::from(Recursion::ConsultedClauseOrder {
                message: format!(
                    "circular consulted-definition expansion: the edge body for \
                     {left_name} & {right_name} in '::{context_name}' is already \
                     being expanded. If the cycle runs through another view or \
                     edge, break the cycle."
                ),
            }));
        }
    };

    // The rule body opens in its own world, grounded or not; free
    // references bind through the data-hole law. Nothing rewrites the
    // body's names. The edge PAIRING (this bound use with the body it
    // opened and the terms that selected it) is the admitted authority's
    // own act.
    // The body's own refusal is the answer: an edge miss, a data hole, a
    // parse failure of the stored rule each keep their identity.
    super::admitted::er_edge(bound, context_name, left, right)
}

/// The context's declared edges as SPELLINGS, for the walk's graph. The
/// world's reach supplies the edge set; a context declared in more than
/// one reachable namespace refuses, so the walked graph belongs to one
/// declaring namespace.
fn context_edges(
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
        return Err(edge_miss_error(
            fold,
            context_name,
            left_spelling,
            right_spelling,
        ));
    }
    let namespaces: std::collections::HashSet<String> = rules
        .iter()
        .map(|(_, _, entity)| super::bound_use::family_display_namespace(entity).to_string())
        .collect();
    if namespaces.len() > 1 {
        let ns_list: Vec<String> = namespaces.into_iter().collect();
        return Err(DelightQLError::from(Resolution::Ambiguous {
            message: format!(
                "Ambiguous ER-context '{}': rules found in multiple namespaces ({}). \
                 Engage exactly one namespace or use qualified access (ns.view(*)).",
                context_name,
                ns_list.join(", "),
            ),
        }));
    }
    Ok(rules
        .into_iter()
        .map(|(left, right, _)| (left, right))
        .collect())
}

/// The edge-selection failure, in two teachings: an unknown context is
/// its own error (the edge set per context is finite and declared); a
/// known context without the requested pair enumerates what IS declared.
fn edge_miss_error(
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
        return DelightQLError::from(Er::UnknownContext {
            message: format!("unknown context '::{context_name}' — {listing}"),
        });
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
    DelightQLError::from(Er::EdgeMiss {
        message: format!(
            "no edge declared for {left_spelling} & {right_spelling} in \
             '::{context_name}' — a term selects an edge by its exact canonical \
             spelling, and emptiness by absent declaration is an error, not a \
             result. Declared edges: {listing}"
        ),
    })
}

/// A chain reads each term once and publishes each term's columns under
/// its name, so two of its terms cannot name one relation.
fn refuse_repeated_term(terms: &[ErTerm]) -> Result<()> {
    for (index, term) in terms.iter().enumerate() {
        if terms[..index]
            .iter()
            .any(|earlier| earlier.name() == term.name())
        {
            return Err(DelightQLError::from(Er::Endpoint {
                message: format!(
                    "the chain reads '{}' at two of its terms — a chain reads each term \
                     once and publishes each under its own name. Declare one side as a \
                     named rule and spell the chain over distinct terms",
                    term.name()
                ),
            }));
        }
    }
    Ok(())
}

/// STAND THE BOUNDARY: the relation publishes exactly the given terms'
/// columns, each answering to its term (or the alias written outside it),
/// and the aliases are threaded to the tables inside.
fn publish(
    fold: &ResolverFold<'_, '_>,
    relation: ResolvedRelation,
    terms: &[&ErTerm],
    aliases: &[Option<SqlIdentifier>],
    context_name: &str,
    subject: impl FnOnce() -> String,
) -> Result<ResolvedRelation> {
    let published: Vec<String> = terms
        .iter()
        .map(|term| term.name().as_str().to_string())
        .collect();
    let mut missing = None;
    let (bounded, _exports) = boundary::export_endpoints(
        relation,
        &published,
        aliases,
        fold.core.identities,
        &mut missing,
    )?;
    if let Some(missing) = missing {
        return Err(crate::diagnostic::Internal::invariant(
            "er composition",
            format!(
                "{} in '::{context_name}' publishes no column of '{missing}', which a simple \
                 edge body cannot fail to read",
                subject()
            ),
        ));
    }
    let endpoints: Vec<(&SqlIdentifier, &Option<SqlIdentifier>)> = terms
        .iter()
        .map(|term| term.name())
        .zip(aliases.iter().chain(std::iter::repeat(&None)))
        .collect();
    boundary::thread_endpoint_aliases(bounded, &endpoints, fold.core.identities)
}

/// The one edge of a two-term position, resolved whole in its own world:
/// self-aliased, the peer's outer mark honored on the body's read of it.
fn resolve_edge(
    fold: &mut ResolverFold<'_, '_>,
    context_name: &str,
    left: &ErTerm,
    right: &ErTerm,
    outer_peer: bool,
) -> Result<ResolvedRelation> {
    let outer = outer_peer.then(|| right.name().as_str().to_string());
    use_er_edge(fold, context_name, left.clone(), right.clone())?
        .compose_standard(outer.as_deref())?
        .resolve(fold, |e| e)
}

/// Link every consecutive pair of terms into one admitted chain.
fn link_chain<'db>(
    fold: &ResolverFold<'_, 'db>,
    context_name: &str,
    terms: &[ErTerm],
) -> Result<AdmittedErChain<'db>> {
    let [first_left, first_right, rest @ ..] = terms else {
        return Err(DelightQLError::from(Constraint::General {
            message: "ER-join chain requires at least two relations".to_string(),
        }));
    };
    let mut chain = AdmittedErChain::link(use_er_edge(
        fold,
        context_name,
        first_left.clone(),
        first_right.clone(),
    )?);
    for right in rest {
        chain = chain.then(fold, right.clone())?;
    }
    Ok(chain)
}

/// A DIRECT RUN, `a & b & c …`: every consecutive pair's edge is admitted,
/// the chain shares each interior term as one read, and the result
/// publishes EVERY written term's exports, each answering to its own
/// alias. A two-term run is the edge itself, with the peer's outer mark
/// honored.
pub(crate) fn resolve_run(
    fold: &mut ResolverFold<'_, '_>,
    context_name: &str,
    operands: &[ErOperand],
) -> Result<ResolvedRelation> {
    if operands.len() < 2 {
        return Err(DelightQLError::from(Constraint::General {
            message: "ER-join chain requires at least two relations".to_string(),
        }));
    }
    let terms: Vec<ErTerm> = operands
        .iter()
        .map(|operand| ErTerm::spelled(&operand.spelling))
        .collect::<Result<_>>()?;
    refuse_repeated_term(&terms)?;
    // An outer-marked peer keeps every left row across ONE edge; a longer
    // run has no ruled composition for the mark yet.
    if operands.len() > 2 && operands.iter().any(|operand| operand.outer) {
        return Err(DelightQLError::from(Er::OuterEndpoint {
            message: "an outer-marked peer stands in an edge chain; the mark composes \
                      across one edge only"
                .to_string(),
        }));
    }
    let aliases: Vec<Option<SqlIdentifier>> = operands
        .iter()
        .map(|operand| operand.alias.clone())
        .collect();
    let relation = if let [left, right] = terms.as_slice() {
        resolve_edge(fold, context_name, left, right, operands[1].outer)?
    } else {
        let chain = link_chain(fold, context_name, &terms)?;
        let (relation, _) = chain.compose(fold)?;
        relation
    };
    // EVERY WRITTEN TERM PUBLISHES, each answering to its own alias.
    let written: Vec<&ErTerm> = terms.iter().collect();
    publish(fold, relation, &written, &aliases, context_name, || {
        if let [left, right] = terms.as_slice() {
            format!(
                "the edge body for ({}, {})",
                left.spelling(),
                right.spelling()
            )
        } else {
            "the composed chain".to_string()
        }
    })
}

/// A TRANSITIVE WALK, `a && c`: the one simple path through the context's
/// declared edges is admitted as a chain, the chain reads each term once
/// with every edge's conditions attached over its reads, and the result
/// publishes the two outer endpoints only, each answering to its alias.
pub(crate) fn resolve_walk(
    fold: &mut ResolverFold<'_, '_>,
    context_name: &str,
    from: &ErOperand,
    to: &ErOperand,
) -> Result<ResolvedRelation> {
    let edges = context_edges(fold, context_name, &from.spelling, &to.spelling)?;
    // Undirected: an edge is selected by its pair, in either order.
    let mut adjacency: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for (left, right) in &edges {
        adjacency
            .entry(left.clone())
            .or_default()
            .push(right.clone());
        adjacency
            .entry(right.clone())
            .or_default()
            .push(left.clone());
    }
    // The graph's nodes are canonical spellings — an endpoint participates
    // exactly when its written spelling is a declared edge term.
    let path = walk::bfs_path(&adjacency, &from.spelling, &to.spelling)?;
    let terms: Vec<ErTerm> = path
        .iter()
        .map(|spelling| ErTerm::spelled(spelling))
        .collect::<Result<_>>()?;
    let (Some(first), Some(last)) = (terms.first(), terms.last()) else {
        return Err(crate::diagnostic::Internal::invariant(
            "er composition",
            "a walked path has two endpoints",
        ));
    };
    refuse_repeated_term(&terms)?;
    let aliases = [from.alias.clone(), to.alias.clone()];
    if let [left, right] = terms.as_slice() {
        // A walk's peers cannot carry the mark; the normalizer refuses the
        // spelling before this road runs.
        let relation = resolve_edge(fold, context_name, left, right, false)?;
        return publish(
            fold,
            relation,
            &[left, right],
            &aliases,
            context_name,
            || {
                format!(
                    "the edge body for ({}, {})",
                    left.spelling(),
                    right.spelling()
                )
            },
        );
    }
    let chain = link_chain(fold, context_name, &terms)?;
    let (relation, _) = chain.compose(fold)?;
    // THE OUTER ENDPOINTS PUBLISH, each answering to its alias.
    publish(
        fold,
        relation,
        &[first, last],
        &aliases,
        context_name,
        || "the composed chain".to_string(),
    )
}
