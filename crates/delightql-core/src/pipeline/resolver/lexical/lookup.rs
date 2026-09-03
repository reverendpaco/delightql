// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE ADDRESS ALGORITHM, over ingredients only the lexical authority
//! assembles. Nothing outside `lexical` can call it: a caller that could
//! hand it its own ports and relations would be writing a second lookup.

use super::Binding;
use crate::names::{Addressing, Sym};
use crate::pipeline::asts::core::literals::column_ordinal_text;
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::resolver::unification::{ColumnReference, Refusal, UnificationResult};
use crate::relation::{PortId, SemanticRelation};
use delightql_types::SqlIdentifier;

/// The name a written reference means, interned as WRITTEN.
///
/// A strop is not decoration on a spelling: it is what makes the name
/// case-sensitive, so a reference interned without it canonicalizes to a
/// different name than the stropped one that published the column. The
/// carrier holds `SqlIdentifier` for exactly this reason — a bare `String`
/// here can no longer say whether the author wrote the backticks.
pub(super) fn written_name(name: &SqlIdentifier, registry: &crate::relation::Planning) -> Sym {
    registry.canonical(registry.intern(name.as_str(), name.is_stropped()))
}

/// THE EXACTLY-ONE LIVE BARE POSITION A SPELLING REUSES, out of a row — the
/// one bare-reuse law, for a pattern's binder and an anonymous member's
/// header alike. A live bare lvar is matched by its COMPLETE name: a
/// position published under a relation name is `r.name`, not `name`, so a
/// bare spelling neither reuses it nor collides with it, and a strop is
/// part of the name. Zero candidates answer `None` — the spelling binds
/// fresh. Several candidates that each stand in a relation answering to an
/// authored name are each reachable qualified — the spelling binds fresh
/// and the writer addresses the one they mean. An AMBIENT bare candidate
/// among several refuses: no spelling selects between two live bare lvars.
pub(super) fn live_bare_reuse(
    row: &[PortId],
    name: &SqlIdentifier,
    registry: &crate::relation::Planning,
) -> Result<Option<PortId>, crate::error::DelightQLError> {
    let wanted = written_name(name, registry);
    let candidates: Vec<PortId> = row
        .iter()
        .copied()
        .filter(|port| {
            matches!(
                registry.addressing(port.column()),
                Addressing::Bare | Addressing::BareStage
            ) && registry.published_sym(port.column()) == Some(wanted)
        })
        .collect();
    crate::probe::probing!(using, {
        for port in row {
            crate::probe::probe!(
                using,
                "  candidate {port:?} pub={:?} addr={:?}",
                registry.published_sym(port.column()),
                registry.addressing(port.column())
            );
        }
        crate::probe::probe!(using, "  declared={candidates:?} for {wanted:?}");
    });
    match candidates.as_slice() {
        [] => Ok(None),
        [one] => Ok(Some(*one)),
        _ if candidates.iter().all(|port| {
            crate::relation::owner(registry, *port)
                .is_ok_and(|owner| registry.answers_to(owner).is_some())
        }) =>
        {
            Ok(None)
        }
        _ => Err(crate::error::DelightQLError::validation_error_categorized(
            "resolution/ambiguous",
            format!("the spelling '{name}' has more than one live bare candidate to reuse"),
            "name one candidate with `as` or address one qualified",
        )),
    }
}

/// The scopes and columns one reference is decided over.
///
/// `available` is the exhaustive column set at this boundary. A bare
/// reference must not also search lexical source scopes: those contain the
/// pre-boundary occurrences which `available` has already republished. An
/// AUTHORED QUALIFIER reaches the lexical bindings too, because they remain
/// addressable by name — and that is true of the qualifier itself, not of
/// the spelling that follows it, so a named reference and an ordinal are
/// decided over the same set. Gathering only: whether the search these feed
/// was whole is `Registry::address`'s question.
fn gather_search(
    available: &[PortId],
    visible: &[Binding],
    qualified: bool,
    registry: &crate::relation::Planning,
) -> Result<(Vec<Binding>, Vec<PortId>), Refusal> {
    let mut relations = Vec::new();
    let mut candidates = Vec::new();
    for port in available {
        if !crate::relation::is_higher_order_support(registry, *port) && !candidates.contains(port)
        {
            candidates.push(*port);
        }
    }
    for binding in visible {
        if !relations.iter().any(|seen: &Binding| {
            seen.relation == binding.relation && seen.answer == binding.answer
        }) {
            relations.push(binding.clone());
        }
        if qualified {
            let interface = registry
                .authority()
                .interface(&binding.relation)
                .map_err(|error| Refusal {
                    subcategory: "resolution/scope/stale",
                    message: error.to_string(),
                    context: "the live semantic relation environment",
                })?;
            for port in interface.ports() {
                if !crate::relation::is_higher_order_support(registry, *port)
                    && !candidates.contains(port)
                {
                    candidates.push(*port);
                }
            }
        }
    }
    Ok((relations, candidates))
}

pub(super) fn unify_single_column(
    reference: ColumnReference,
    available: &[PortId],
    visible: &[Binding],
    registry: &crate::relation::Planning,
) -> UnificationResult {
    match reference {
        ColumnReference::Named { name, qualifier } => {
            let wanted = written_name(&name, registry);
            let qualifier_sym = qualifier
                .as_ref()
                .map(|qualifier| written_name(qualifier, registry));
            let name = name.into_inner();
            let qualifier = qualifier.map(SqlIdentifier::into_inner);
            let (relations, candidates) =
                match gather_search(available, visible, qualifier_sym.is_some(), registry) {
                    Ok(search) => search,
                    Err(refusal) => return UnificationResult::Refused(refusal),
                };
            // Report the set the decision is made over, not the set the
            // caller offered: for a qualified reference they differ by the
            // lexical headings folded in above, and a probe that prints the
            // smaller one shows a single match where the refusal saw two.
            crate::probe::probing!(resolve, {
                crate::probe::probe!(
                    resolve,
                    "address {}{name:?} over {} scopes",
                    qualifier.as_deref().unwrap_or("<bare>"),
                    relations.len()
                );
                for binding in &relations {
                    crate::probe::probe!(
                        resolve,
                        "  scope {scope:?} answers_to={:?} origin={:?}",
                        binding.answer,
                        registry.kind_of(binding.relation.scope()),
                        scope = binding.relation.scope(),
                    );
                }
                for candidate in &candidates {
                    let column = candidate.column();
                    crate::probe::probe!(
                        resolve,
                        "  candidate {candidate:?}@{:?} published={:?} matches_name={} \
                         addressing={:?}",
                        crate::relation::owner(registry, *candidate),
                        registry.published_sym(column),
                        registry.published_sym(column) == Some(wanted),
                        registry.addressing(column)
                    );
                }
            });
            let hits: Vec<PortId> = match qualifier_sym {
                None => {
                    if relations.iter().any(|binding| {
                        registry
                            .authority()
                            .interface(&binding.relation)
                            .is_ok_and(|interface| interface.is_opaque())
                    }) {
                        return UnificationResult::Opaque;
                    }
                    candidates
                        .iter()
                        .copied()
                        .filter(|port| answers_for(*port, registry) == Some(wanted))
                        .collect()
                }
                Some(qualifier_sym) => {
                    match qualified_candidates(qualifier_sym, &relations, registry) {
                        Ok(qualified) => qualified
                            .into_iter()
                            .filter(|port| answers_for(*port, registry) == Some(wanted))
                            .collect(),
                        Err(QualifiedError::NoScope) => {
                            return missing_scope(
                                &name,
                                qualifier.as_deref().expect("a qualified lookup"),
                                &relations,
                                registry,
                            )
                        }
                        Err(error) => return error.with_name(&name),
                    }
                }
            };
            match collapse_republications(hits, registry).as_slice() {
                [port] => UnificationResult::Resolved(ColumnOccurrence::addressed(
                    *port,
                    qualifier.is_some(),
                    super::Terminal::judged(),
                )),
                [] => UnificationResult::Unresolved(match &qualifier {
                    Some(qualifier) => format!("{qualifier}.{name}"),
                    None => name,
                }),
                _ => UnificationResult::Ambiguous {
                    column: name,
                    tables: describe_relations(&relations, registry),
                },
            }
        }
        ColumnReference::Ordinal {
            position,
            reverse,
            qualifier,
        } => {
            let qualifier_sym = qualifier
                .as_ref()
                .map(|qualifier| written_name(qualifier, registry));
            // A qualified ordinal is a qualified glob narrowed by position —
            // the same tiers, over the same set a qualified NAME is decided
            // over, or `u|1|` and `u.age` reach columns in different scopes
            // and a join stops being a join.
            let (relations, searched) =
                match gather_search(available, visible, qualifier_sym.is_some(), registry) {
                    Ok(search) => search,
                    Err(refusal) => return UnificationResult::Refused(refusal),
                };
            let candidates: Vec<PortId> = match qualifier_sym {
                Some(qualifier_id) => {
                    match qualified_candidates(qualifier_id, &relations, registry) {
                        Ok(candidates) => candidates,
                        Err(QualifiedError::NoScope) => {
                            return missing_scope(
                                &column_ordinal_text(position, reverse),
                                qualifier.as_ref().expect("a qualified ordinal").as_str(),
                                &relations,
                                registry,
                            )
                        }
                        Err(error) => {
                            return error.with_name(&column_ordinal_text(position, reverse))
                        }
                    }
                }
                None => searched,
            }
            .into_iter()
            .filter(|port| !crate::relation::is_higher_order_support(registry, *port))
            .collect();

            let miss = || column_ordinal_text(position, reverse);
            if candidates.is_empty() {
                return UnificationResult::Unresolved(miss());
            }
            let index = if reverse {
                if position == 0 || position as usize > candidates.len() {
                    return UnificationResult::Unresolved(miss());
                }
                candidates.len() - position as usize
            } else {
                if position == 0 || position as usize > candidates.len() {
                    return UnificationResult::Unresolved(miss());
                }
                position as usize - 1
            };
            // POSITION REACHES WHAT NAMES CANNOT: for an inchoate
            // occurrence, this reach is its activation.
            if let Ok(owner) = crate::relation::owner(registry, candidates[index]) {
                registry.note_ordinal_reach(owner);
            }
            UnificationResult::Resolved(ColumnOccurrence::addressed(
                candidates[index],
                qualifier.is_some(),
                super::Terminal::judged(),
            ))
        }
    }
}

fn missing_scope(
    name: &str,
    qualifier: &str,
    relations: &[Binding],
    registry: &crate::relation::Planning,
) -> UnificationResult {
    let visible = describe_relations(relations, registry);
    UnificationResult::Unresolved(if visible.is_empty() {
        format!("{qualifier}.{name} — '{qualifier}' is not a scope here (nothing in scope answers a qualifier)")
    } else {
        format!(
            "{qualifier}.{name} — '{qualifier}' is not a scope here (in scope: {})",
            visible.join(", ")
        )
    })
}

/// The spelling a position answers to, by its own publication. There is
/// no per-column route by which a QUALIFIER reaches a position: which
/// relations a qualifier names is the lexical frontier's fact.
fn answers_for(port: PortId, registry: &crate::relation::Planning) -> Option<Sym> {
    let column = port.column();
    match registry.addressing(column) {
        Addressing::Published
        | Addressing::Bare
        | Addressing::BareUnder
        | Addressing::BareStage => registry
            .published(column)
            .map(|name| registry.canonical(name)),
        Addressing::Hygienic | Addressing::Latent => None,
    }
}

enum QualifiedError {
    NoScope,
    Opaque,
    NoUnnamedPipe,
    TwoUnnamedPipes,
}

impl QualifiedError {
    fn with_name(self, name: &str) -> UnificationResult {
        match self {
            QualifiedError::NoScope => UnificationResult::Unresolved(name.to_owned()),
            QualifiedError::Opaque => UnificationResult::Opaque,
            QualifiedError::NoUnnamedPipe => UnificationResult::Refused(Refusal {
                subcategory: "resolution/pipe/no_unnamed_pipe",
                message: format!("`_.{name}`: there is no unnamed pipe here for `_` to select"),
                context: "the deictic `_`",
            }),
            QualifiedError::TwoUnnamedPipes => UnificationResult::Refused(Refusal {
                subcategory: "resolution/pipe/two_unnamed_pipes",
                message: format!(
                    "`_.{name}`: two unnamed pipes are in scope and `_` names neither"
                ),
                context: "the deictic `_`",
            }),
        }
    }
}

/// ONE COLUMN OFFERED AT TWO LEVELS IS ONE ANSWER.
///
/// A qualified reference is decided over the heading standing here AND the
/// lexical bindings it can still reach. Where this heading republished a
/// binding's position — a join carries its operands' columns, an alias
/// carries the relation it names — both are offered and both answer to the
/// same qualified spelling. The position standing here is the one the
/// reference means; the one it was carried from is the same column one level
/// down. Construction recorded the carry, so this asks the record. Sibling
/// positions carrying one source are unrelated by it, so a genuine
/// two-answer heading still refuses.
fn collapse_republications(hits: Vec<PortId>, registry: &crate::relation::Planning) -> Vec<PortId> {
    if hits.len() < 2 {
        return hits;
    }
    let kept: Vec<PortId> = hits
        .iter()
        .copied()
        .filter(|candidate| {
            !hits.iter().any(|other| {
                *other != *candidate && crate::relation::stands_where(registry, *other, *candidate)
            })
        })
        .collect();
    if kept.is_empty() {
        hits
    } else {
        kept
    }
}

fn qualified_candidates(
    qualifier: Sym,
    relations: &[Binding],
    registry: &crate::relation::Planning,
) -> Result<Vec<PortId>, QualifiedError> {
    let named: Vec<&Binding> = relations
        .iter()
        .filter(|binding| binding.answer == Some(qualifier))
        .collect();
    if !named.is_empty() {
        let mut ports = Vec::new();
        for binding in named {
            let interface = registry
                .authority()
                .interface(&binding.relation)
                .map_err(|_| QualifiedError::NoScope)?;
            if interface.is_opaque() {
                return Err(QualifiedError::Opaque);
            }
            match &binding.ports {
                Some(reached) => ports.extend_from_slice(reached),
                None => ports.extend_from_slice(interface.ports()),
            }
        }
        return Ok(ports);
    }
    if Some(qualifier) != registry.known_sym("_", false) {
        return Err(QualifiedError::NoScope);
    }
    // A STAGE IS WHAT A PIPE PRODUCED. An authored `as n` mints the stage
    // birth; an unnamed step is the projection or reduction the operator
    // derived, and `_` selects it by the same law — the deictic names the
    // one visible pipe output, not one spelling of it.
    let stages: Vec<SemanticRelation> = relations
        .iter()
        .filter(|binding| binding.answer.is_none())
        .map(|binding| binding.relation)
        .filter(|relation| {
            matches!(
                registry.kind_of(relation.scope()),
                crate::names::ScopeKind::PipeStage
                    | crate::names::ScopeKind::Wrap {
                        why: crate::names::WrapReason::Projection
                            | crate::names::WrapReason::Aggregate
                            | crate::names::WrapReason::Distinct
                    }
            )
        })
        .collect();
    match stages.as_slice() {
        [] => Err(QualifiedError::NoUnnamedPipe),
        [stage] => registry
            .authority()
            .interface(stage)
            .map(|interface| interface.ports().to_vec())
            .map_err(|_| QualifiedError::NoScope),
        _ => Err(QualifiedError::TwoUnnamedPipes),
    }
}

pub(super) fn qualify_ports(
    qualifier: &SqlIdentifier,
    visible: &[Binding],
    registry: &crate::relation::Planning,
) -> Result<Vec<PortId>, crate::error::DelightQLError> {
    let written = qualifier;
    let qualifier = written_name(qualifier, registry);
    let (relations, _) =
        gather_search(&[], visible, true, registry).map_err(Refusal::into_error)?;
    qualified_candidates(qualifier, &relations, registry).map_err(|error| match error {
        // A qualifier that names no relation in view is the same absence
        // a qualified name meets: the scope is not here.
        QualifiedError::NoScope => match missing_scope("*", written.as_str(), &relations, registry)
        {
            UnificationResult::Unresolved(text) => {
                crate::error::DelightQLError::column_not_found_error(
                    text,
                    "in qualified enumeration",
                )
            }
            other => unreachable!("a missing scope is reported as unresolved: {other:?}"),
        },
        other => match other.with_name("*") {
            UnificationResult::Refused(refusal) => refusal.into_error(),
            UnificationResult::Opaque => crate::pipeline::resolver::opaque_reference_refusal(),
            _ => crate::error::DelightQLError::column_not_found_error(
                "qualified enumeration",
                "in qualified enumeration",
            ),
        },
    })
}

fn describe_relations(relations: &[Binding], registry: &crate::relation::Planning) -> Vec<String> {
    let mut descriptions = Vec::new();
    for binding in relations {
        let mut text = String::new();
        registry.describe(
            binding.relation.scope(),
            &mut crate::names::Teaching(&mut text),
        );
        if !descriptions.contains(&text) {
            descriptions.push(text);
        }
    }
    descriptions
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The algorithm over raw ingredients — a test of the judgment, which
    /// production reaches only through a `Position`.
    fn unify_columns(
        references: Vec<ColumnReference>,
        available: &[PortId],
        visible: &[SemanticRelation],
        registry: &crate::relation::Planning,
    ) -> Vec<UnificationResult> {
        let visible: Vec<Binding> = visible
            .iter()
            .map(|relation| Binding {
                relation: *relation,
                answer: registry.answers_to(relation.scope()),
                ports: None,
            })
            .collect();
        references
            .into_iter()
            .map(|reference| unify_single_column(reference, available, &visible, registry))
            .collect()
    }

    fn relation(
        registry: &crate::relation::Planning,
        answer: Option<&str>,
        names: &[(&str, bool)],
    ) -> (SemanticRelation, Vec<PortId>) {
        let slots: Vec<_> = names
            .iter()
            .enumerate()
            .map(
                |(position, (name, stropped))| crate::relation::form::SourceSlot {
                    position: position as u32,
                    named: Some(registry.intern(name, *stropped)),
                    declared_type: None,
                },
            )
            .collect();
        let entity = registry.mint_entity(registry.intern("test-source", false));
        let relation = registry
            .authority()
            .derive(crate::relation::RelForm::Source(
                crate::relation::form::SourceSpec {
                    origin: crate::relation::form::SourceOrigin::Catalog { entity },
                    slots: &slots,
                    answers_to: answer.map(|name| registry.intern(name, false)),
                },
            ))
            .unwrap();
        let ports = crate::relation::published_ports(registry, &relation).unwrap();
        (relation, ports)
    }

    fn named(name: &str, qualifier: Option<&str>) -> ColumnReference {
        ColumnReference::Named {
            name: SqlIdentifier::new(name),
            qualifier: qualifier.map(SqlIdentifier::new),
        }
    }

    fn stropped(name: &str, qualifier: Option<&str>) -> ColumnReference {
        ColumnReference::Named {
            name: SqlIdentifier::stropped(name),
            qualifier: qualifier.map(SqlIdentifier::stropped),
        }
    }

    /// A STROP IS PART OF THE NAME. `\`Mixed\`` published and `\`Mixed\``
    /// written are one name; the bare spelling folds and is another. A
    /// reference carrier that could not say which was written would answer
    /// both the same way, and the case-sensitive column would be reachable
    /// by nothing.
    ///
    /// All three addressing routes are paired against ONE publication: the
    /// name, the ordinal's qualifier, and the dequalifying step are the same
    /// law, and a hole in any one of them is the same hole.
    #[test]
    fn a_stropped_reference_reaches_the_stropped_publication() {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let (source, ports) = relation(&registry, Some("source"), &[("Mixed", true)]);
        let column = ports[0];

        let reached = unify_columns(
            vec![stropped("Mixed", None)],
            &[column],
            &[source],
            &registry,
        )
        .pop()
        .expect("one reference has one result");
        assert!(
            matches!(&reached, UnificationResult::Resolved(found) if found.column == column),
            "the stropped spelling reaches the column it published: {reached:?}"
        );

        // The same through an ORDINAL: a qualifier chooses the scope the
        // position is counted within, so folding it there reaches a scope
        // nobody named — one character apart from the one that was.
        let by_position = |qualifier: SqlIdentifier| ColumnReference::Ordinal {
            position: 1,
            reverse: false,
            qualifier: Some(qualifier),
        };
        let reached = unify_columns(
            vec![by_position(SqlIdentifier::stropped("source"))],
            &[column],
            &[source],
            &registry,
        )
        .pop()
        .expect("one reference has one result");
        assert!(
            matches!(&reached, UnificationResult::Resolved(found) if found.column == column),
            "a stropped qualifier reaches the scope it names: {reached:?}"
        );

        for miss in [stropped("mixed", None), named("Mixed", None)] {
            let result = unify_columns(vec![miss], &[column], &[source], &registry)
                .pop()
                .expect("one reference has one result");
            assert!(
                matches!(result, UnificationResult::Unresolved(_)),
                "only the written spelling reaches a stropped column: {result:?}"
            );
        }
    }

    /// TWO AMBIENT BARE CANDIDATES REFUSE. The surface cannot spell the
    /// state — same-named ambient bare lvars unify at the join that would
    /// have carried them together — so the refusal arm is witnessed here,
    /// over ports the authority minted: one candidate reuses, none binds
    /// fresh, and two of one name refuse rather than guess.
    #[test]
    fn two_ambient_bare_candidates_refuse_reuse() {
        let planning = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let first = crate::relation::named_port(&planning, "m");
        let second = crate::relation::named_port(&planning, "m");
        let name = SqlIdentifier::new("m");
        assert_eq!(live_bare_reuse(&[], &name, &planning).unwrap(), None);
        assert_eq!(
            live_bare_reuse(&[first], &name, &planning).unwrap(),
            Some(first)
        );
        assert!(live_bare_reuse(&[first, second], &name, &planning).is_err());
    }

    #[test]
    fn bare_pipe_need_selects_the_available_boundary_occurrence() {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let (source, ports) = relation(&registry, Some("source"), &[("returned", false)]);
        let before = ports[0];
        let stage = registry
            .authority()
            .derive(crate::relation::RelForm::Export(
                crate::relation::form::ExportSpec {
                    input: source,
                    why: crate::relation::form::ExportWhy::Stage,
                },
            ))
            .unwrap();
        let after = crate::relation::published_ports(&registry, &stage).unwrap()[0];

        let result = unify_columns(
            vec![named("returned", None)],
            &[after],
            &[source],
            &registry,
        )
        .pop()
        .expect("one reference has one result");

        assert!(matches!(result, UnificationResult::Resolved(found) if found.column == after));
        assert_ne!(before, after);
    }

    #[test]
    fn bare_lookup_still_refuses_two_available_occurrences() {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let (_, left) = relation(&registry, None, &[("returned", false)]);
        let (_, right) = relation(&registry, None, &[("returned", false)]);

        let result = unify_columns(
            vec![named("returned", None)],
            &[left[0], right[0]],
            &[],
            &registry,
        )
        .pop()
        .expect("one reference has one result");

        assert!(matches!(result, UnificationResult::Ambiguous { .. }));
    }

    #[test]
    fn bare_lookup_does_not_expand_available_to_the_whole_scope() {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let (_, ports) = relation(&registry, None, &[("available", false), ("hidden", false)]);
        let available = ports[0];

        let result = unify_columns(vec![named("hidden", None)], &[available], &[], &registry)
            .pop()
            .expect("one reference has one result");

        assert!(matches!(result, UnificationResult::Unresolved(name) if name == "hidden"));
    }

    /// A consulted view lands its columns in a scope answering to nothing —
    /// the name the query wrote lives on the COLUMNS. A join republishes them
    /// keeping that mark, so the qualified lookup is offered the view's
    /// occurrence and the join's copy of it. One value, two boundaries.
    #[test]
    fn qualified_lookup_prefers_the_arm_over_the_joins_copy_of_it() {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let (view, view_ports) = relation(&registry, Some("emp2"), &[("eid", false)]);
        let arm = view_ports[0];
        let (other, _) = relation(&registry, None, &[]);
        let join = registry
            .authority()
            .derive(crate::relation::RelForm::Join(
                crate::relation::form::JoinSpec {
                    left: view,
                    right: other,
                    kind: crate::relation::form::JoinKind::Inner,
                    merged: &[],
                },
            ))
            .unwrap();
        let exported = crate::relation::published_ports(&registry, &join).unwrap()[0];

        let result = unify_columns(
            vec![named("eid", Some("emp2"))],
            &[exported],
            &[view],
            &registry,
        )
        .pop()
        .expect("one reference has one result");

        assert!(matches!(result, UnificationResult::Resolved(found) if found.column == arm));
    }

    /// Only a shared chain collapses. Two arms that separately answer under one
    /// name are rivals however alike they look, and the reference naming both
    /// still has no single column to mean.
    #[test]
    fn qualified_lookup_still_refuses_two_unrelated_carriers() {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let (left_relation, left) = relation(&registry, Some("emp2"), &[("eid", false)]);
        let (right_relation, right) = relation(&registry, Some("emp2"), &[("eid", false)]);

        let result = unify_columns(
            vec![named("eid", Some("emp2"))],
            &[left[0], right[0]],
            &[left_relation, right_relation],
            &registry,
        )
        .pop()
        .expect("one reference has one result");

        assert!(matches!(result, UnificationResult::Ambiguous { .. }));
    }

    /// A DECLARED ROW IS BORN AND STOOD OVER IN ONE ACT, from spellings:
    /// its positions are the declaration's, and what answers over it is
    /// the declaration's own answer — a name when it declared one, and
    /// nothing otherwise, so no qualifier reaches an undeclared row.
    #[test]
    fn a_declared_row_answers_by_its_declaration_alone() {
        use super::super::{Position, Reach, ResolvedRelation};
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let mut witness = super::super::Witness::default();
        let slot = |name: &str| crate::relation::form::AnonymousSlot::Binder {
            position: 0,
            named: registry.intern(name, false),
            declared_type: None,
            shape: crate::names::ValueShape::Unknown,
        };
        let declared = |name: &str, answer: Option<&str>| {
            let slots = vec![slot(name)];
            ResolvedRelation::declared_row(
                crate::relation::form::AnonymousSpec {
                    shape: crate::relation::form::AnonymousShape::ArgumentRow,
                    slots: &slots,
                    answers_to: answer.map(|answer| registry.intern(answer, false)),
                },
                &registry,
            )
            .unwrap()
        };

        let named_row = declared("x", Some("source"));
        let port =
            crate::relation::published_ports(&registry, &named_row.semantic_relation()).unwrap()[0];
        let mut position = Position::root();
        position.enter(named_row, Reach::Row);
        let qualified = position
            .address(named("x", Some("source")), false, &mut witness, &registry)
            .unwrap();
        assert!(
            matches!(&qualified, UnificationResult::Resolved(found) if found.column == port),
            "the declared answer reaches the row's own position: {qualified:?}"
        );
        let bare = position
            .address(named("x", None), false, &mut witness, &registry)
            .unwrap();
        assert!(matches!(&bare, UnificationResult::Resolved(found) if found.column == port));

        let unnamed_row = declared("y", None);
        let port = crate::relation::published_ports(&registry, &unnamed_row.semantic_relation())
            .unwrap()[0];
        let mut position = Position::root();
        position.enter(unnamed_row, Reach::Row);
        let bare = position
            .address(named("y", None), false, &mut witness, &registry)
            .unwrap();
        assert!(matches!(&bare, UnificationResult::Resolved(found) if found.column == port));
        let qualified = position
            .address(named("y", Some("anything")), false, &mut witness, &registry)
            .unwrap();
        assert!(
            matches!(qualified, UnificationResult::Unresolved(_)),
            "a row declared under no name answers no qualifier: {qualified:?}"
        );
    }

    /// THE STAGE ENDS THE SCOPE IT CONSUMED. What answers over a stage's
    /// far side is born from the stage relation alone, so the position a
    /// consumer stands at has no route by which the pre-boundary
    /// qualifier reaches the carried position — while the bare
    /// republication still does.
    #[test]
    fn a_pre_boundary_qualifier_does_not_reach_past_the_stage() {
        use super::super::{Position, Reach, ResolvedRelation};
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let (source, _) = relation(&registry, Some("source"), &[("returned", false)]);
        let stage = registry
            .authority()
            .derive(crate::relation::RelForm::Export(
                crate::relation::form::ExportSpec {
                    input: source,
                    why: crate::relation::form::ExportWhy::Stage,
                },
            ))
            .unwrap();
        let after = crate::relation::published_ports(&registry, &stage).unwrap()[0];
        let chain = registry
            .authority()
            .ground_read(crate::pipeline::ast_resolved::Access::All, false, stage)
            .unwrap();
        let mut position = Position::root();
        position.enter(ResolvedRelation::answering_for_itself(chain), Reach::Row);
        let mut witness = super::super::Witness::default();

        let qualified = position
            .address(
                named("returned", Some("source")),
                false,
                &mut witness,
                &registry,
            )
            .unwrap();
        assert!(
            matches!(qualified, UnificationResult::Unresolved(_)),
            "the consumed scope's qualifier is dead on the far side: {qualified:?}"
        );

        let bare = position
            .address(named("returned", None), false, &mut witness, &registry)
            .unwrap();
        assert!(matches!(bare, UnificationResult::Resolved(found) if found.column == after));
    }
}
