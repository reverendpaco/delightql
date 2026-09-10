// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE EDGE BOUNDARY: what a resolved edge body publishes. An edge is a
//! pair-set, so its boundary keeps exactly its endpoints' columns, each
//! answering to its endpoint (or the alias a caller wrote outside the
//! term), and hides everything else the body joined or computed. The
//! self-alias and outer-mark shaping of an opened body, and the alias
//! threading over a resolved one, live beside it: they are the only
//! rewrites an ER position performs on a body, and no road outside the
//! authority performs them.

use crate::diagnostic::Er;
use crate::error::{DelightQLError, Result};
use crate::pipeline::resolver::ResolvedRelation;
use crate::pipeline::{ast_resolved, ast_unresolved};
use delightql_types::SqlIdentifier;

/// Mark the edge body's own read of the stated endpoint outer, so the
/// authored `?` on the peer keeps every left row through the expanded join.
/// The mark rides the ACCESS, and the body's access of the endpoint is the
/// one place the expanded join can honor it.
pub(in crate::defuse) fn mark_endpoint_outer(
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
            } if SqlIdentifier::str_eq(identifier.name.as_str(), endpoint) => {
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
        0 => Err(DelightQLError::from(Er::OuterEndpoint {
    message: format!(
                "the outer mark reaches the edge body's read of '{endpoint}',                  and this body does not read it directly"
            ),
})),
        _ => Err(DelightQLError::from(Er::OuterEndpoint {
    message: format!("the edge body reads '{endpoint}' more than once, so the outer mark is ambiguous"),
})),
    }
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
pub(in crate::defuse) fn endpoint_of(
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
pub(in crate::defuse) fn missing_endpoint(
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
        if !provided
            .iter()
            .any(|column| endpoint_of(identities, &input, *column, &endpoints) == Some(*endpoint))
        {
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
///
/// The exports come back beside the bounded relation, in the boundary's own
/// port order: a composed walk pairs its segments through them, so the
/// endpoint a hop's column belongs to is read off the boundary that
/// published it and never recovered from a name afterwards.
pub(in crate::defuse) fn export_endpoints(
    resolved: ResolvedRelation,
    published: &[String],
    aliases: &[Option<SqlIdentifier>],
    identities: &crate::relation::Planning,
    missing: &mut Option<String>,
) -> Result<(ResolvedRelation, Vec<crate::relation::form::ErExport>)> {
    // AN ENDPOINT EXPORT REPUBLISHES: schema(A) + schema(B) is a new
    // publication the prior spellings do not reach around, so what answers
    // over the result is derived from the boundary this writes.
    let mut exported = Vec::new();
    let bounded = resolved.republished_as_er_boundary(identities, |expr| {
        let (chain, exports) =
            export_endpoints_chain(published, aliases, identities, expr, missing)?;
        exported.extend(exports.iter().copied());
        Ok((chain, exports))
    })?;
    Ok((bounded, exported))
}

fn export_endpoints_chain(
    published: &[String],
    aliases: &[Option<SqlIdentifier>],
    identities: &crate::relation::Planning,
    expr: ast_resolved::Chain,
    missing: &mut Option<String>,
) -> Result<(ast_resolved::Chain, Vec<crate::relation::form::ErExport>)> {
    let input = expr.semantic_relation();
    if let Some(absent) = missing_endpoint(published, identities, input) {
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
        |column: crate::relation::PortId| endpoint_of(identities, &input, column, &endpoints);
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

/// Rename endpoint tables to their aliases throughout a resolved ER result
/// (exports answer to the alias; selection already happened by spelling).
/// One alias per written term: a run's interior term answers to its own
/// alias exactly as an endpoint does.
pub(in crate::defuse) fn thread_endpoint_aliases(
    resolved: ResolvedRelation,
    endpoints: &[(&SqlIdentifier, &Option<SqlIdentifier>)],
    identities: &crate::relation::Planning,
) -> Result<ResolvedRelation> {
    // The renames touch the base tables INSIDE the chain; the boundary
    // standing outermost — and the endpoint routes bound on it, already
    // spelled with the aliases the exports were derived under — publish
    // nothing new, so what answers over the result stays what answered.
    resolved.republished_within(None, identities, |mut expr| {
        for (name, alias) in endpoints {
            if let Some(alias) = alias {
                expr = rename_in_resolved_expr(expr, name.as_str(), alias, identities)?;
            }
        }
        Ok(expr)
    })
}

/// Rename a table's alias and all qualifier references throughout a resolved
/// expression tree. Takes ownership and returns the modified expression.
/// Used to apply user aliases from `&&` endpoints after the chain is resolved.
pub(in crate::defuse) fn rename_in_resolved_expr(
    expr: ast_resolved::Chain,
    old_name: &str,
    new_name: &SqlIdentifier,
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
            | ast_resolved::Continuation::Restrict { .. }
            | ast_resolved::Continuation::Correlated(_) => {}
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

/// Add self-aliases to Ground relations in a query that don't already have aliases.
/// Transforms `table(*)` into `table(*) as table`. This ensures ConsultedView expansion
/// preserves the original table name as the SQL alias, so qualified references
/// (like `table.col`) in predicates continue to resolve correctly.
pub(in crate::defuse) fn add_self_aliases_to_query(
    mut query: ast_unresolved::Query,
) -> ast_unresolved::Query {
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
    use crate::pipeline::asts::core::Step;

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
