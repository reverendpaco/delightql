// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Column resolution through the compilation registry.

use crate::names::{AddressError, ColId, Reference, Registry, ScopeEnv, ScopeId, Sym};
use crate::pipeline::asts::core::literals::column_ordinal_text;
use delightql_types::SqlIdentifier;

#[derive(Debug)]
pub enum UnificationResult {
    Resolved(ColId),
    Unresolved(String),
    Ambiguous {
        column: String,
        tables: Vec<String>,
    },
    /// A relation in view publishes dimensions the target never described.
    /// The reference is neither resolved nor absent: it was not searched.
    Opaque,
    /// The addressing refused with a teaching of its own, which every
    /// consumer surfaces unchanged rather than restating as "column not
    /// found". Treated as an absence by the widening a narrowed search does:
    /// what is missing HERE may still be answered outside.
    Refused(Refusal),
}

/// A refusal decided by the addressing, carried whole to the caller that
/// turns it into an error.
#[derive(Debug, Clone)]
pub struct Refusal {
    pub subcategory: &'static str,
    pub message: String,
    pub context: &'static str,
}

impl Refusal {
    pub fn into_error(self) -> crate::error::DelightQLError {
        crate::error::DelightQLError::validation_error_categorized(
            self.subcategory,
            self.message,
            self.context,
        )
    }
}

#[derive(Debug, Clone)]
pub enum ColumnReference {
    Named {
        name: SqlIdentifier,
        qualifier: Option<SqlIdentifier>,
    },
    Ordinal {
        position: u16,
        reverse: bool,
        qualifier: Option<SqlIdentifier>,
    },
}

/// The name a written reference means, interned as WRITTEN.
///
/// A strop is not decoration on a spelling: it is what makes the name
/// case-sensitive, so a reference interned without it canonicalizes to a
/// different name than the stropped one that published the column. The
/// carrier holds `SqlIdentifier` for exactly this reason — a bare `String`
/// here can no longer say whether the author wrote the backticks.
fn written_name(name: &SqlIdentifier, registry: &Registry) -> Sym {
    registry.canonical(registry.intern(name.as_str(), name.is_stropped()))
}

pub fn unify_columns(
    references: Vec<ColumnReference>,
    available: &[ColId],
    visible: &[ScopeId],
    registry: &Registry,
) -> Vec<UnificationResult> {
    references
        .into_iter()
        .map(|reference| unify_single_column(reference, available, visible, registry))
        .collect()
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
    available: &[ColId],
    visible: &[ScopeId],
    qualified: bool,
    registry: &Registry,
) -> (Vec<ScopeId>, Vec<ColId>) {
    let mut scopes = Vec::new();
    let mut candidates = Vec::new();
    for column in available {
        let scope = registry.scope_of(*column);
        if !scopes.contains(&scope) {
            scopes.push(scope);
        }
        if !candidates.contains(column) {
            candidates.push(*column);
        }
    }
    if qualified {
        for scope in visible {
            if !scopes.contains(scope) {
                scopes.push(*scope);
            }
            for column in registry.heading(*scope).columns_seen() {
                if !candidates.contains(&column) {
                    candidates.push(column);
                }
            }
        }
    }
    (scopes, candidates)
}

fn unify_single_column(
    reference: ColumnReference,
    available: &[ColId],
    visible: &[ScopeId],
    registry: &Registry,
) -> UnificationResult {
    match reference {
        ColumnReference::Named { name, qualifier } => {
            let wanted = written_name(&name, registry);
            let qualifier_sym = qualifier
                .as_ref()
                .map(|qualifier| written_name(qualifier, registry));
            let name = name.into_inner();
            let qualifier = qualifier.map(SqlIdentifier::into_inner);
            let (scopes, candidates) =
                gather_search(available, visible, qualifier_sym.is_some(), registry);
            // Report the set the decision is made over, not the set the
            // caller offered: for a qualified reference they differ by the
            // lexical headings folded in above, and a probe that prints the
            // smaller one shows a single match where the refusal saw two.
            crate::probe::probing!(resolve, {
                crate::probe::probe!(
                    resolve,
                    "address {}{name:?} over {} scopes",
                    qualifier.as_deref().unwrap_or("<bare>"),
                    scopes.len()
                );
                for scope in &scopes {
                    crate::probe::probe!(
                        resolve,
                        "  scope {scope:?} answers_to={:?} origin={:?}",
                        registry.answers_to(*scope),
                        registry.origin_of(*scope)
                    );
                }
                for candidate in &candidates {
                    crate::probe::probe!(
                        resolve,
                        "  candidate {candidate:?}@{:?} published={:?} matches_name={} \
                         addressing={:?}",
                        registry.scope_of(*candidate),
                        registry.published_sym(*candidate),
                        registry.published_sym(*candidate) == Some(wanted),
                        registry.addressing(*candidate)
                    );
                }
            });
            let env = ScopeEnv::among(scopes, candidates);
            let reference = Reference {
                qualifier: qualifier_sym,
                name: wanted,
            };
            match registry.address(reference, &env) {
                Ok(column) => UnificationResult::Resolved(column),
                // Say back what was written. A qualifier is not decoration
                // here — it CHOSE the set that was searched, so reporting the
                // bare name describes a search nobody asked for and reads as
                // though the qualifier had been understood and dropped.
                Err(AddressError::NotFound) => UnificationResult::Unresolved(match &qualifier {
                    Some(qualifier) => format!("{qualifier}.{name}"),
                    None => name,
                }),
                Err(AddressError::NoSuchScope) => {
                    let qualifier = qualifier.expect("qualified lookup has a qualifier");
                    UnificationResult::Unresolved(format!(
                        "{qualifier}.{name} — '{qualifier}' is not a scope here{}",
                        describe_live_scopes(&env, registry),
                    ))
                }
                // `_` is deixis: it points at the one unnamed pipe output in
                // view. Neither refusal is a missing column — one has
                // nothing to point at, the other too much — so neither may
                // be reported as one.
                Err(AddressError::NoUnnamedPipe) => UnificationResult::Refused(Refusal {
                    subcategory: "resolution/pipe/no_unnamed_pipe",
                    message: format!(
                        "`_.{name}`: there is no unnamed pipe here for `_` to select. `_` \
                         points at a pipe's output; it is not a name and looks nothing up. \
                         Name the relation you meant, or pipe first."
                    ),
                    context: "the deictic `_`",
                }),
                Err(AddressError::TwoUnnamedPipes) => UnificationResult::Refused(Refusal {
                    subcategory: "resolution/pipe/two_unnamed_pipes",
                    message: format!(
                        "`_.{name}`: two unnamed pipes are in scope and `_` names neither. \
                         One spelling cannot stand for two relations. Name one of them with \
                         `as` and reach it by that name."
                    ),
                    context: "the deictic `_`",
                }),
                // Not absent, not unique, not ambiguous: not enumerated.
                Err(AddressError::Incomplete) => UnificationResult::Opaque,
                Err(AddressError::Ambiguous) => UnificationResult::Ambiguous {
                    column: name,
                    tables: describe_scope_names(&env, registry),
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
            let (_, searched) =
                gather_search(available, visible, qualifier_sym.is_some(), registry);
            let candidates: Vec<ColId> = match qualifier_sym {
                Some(qualifier) => registry.qualified_glob(qualifier, &searched).to_vec(),
                None => searched,
            };

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
            registry.note_ordinal_reach(registry.scope_of(candidates[index]));
            UnificationResult::Resolved(candidates[index])
        }
    }
}

pub(in crate::pipeline::resolver) fn describe_live_scopes(
    env: &ScopeEnv,
    registry: &Registry,
) -> String {
    let scopes = describe_scope_names(env, registry);
    if scopes.is_empty() {
        String::new()
    } else {
        format!(" (in scope: {})", scopes.join(", "))
    }
}

fn describe_scope_names(env: &ScopeEnv, registry: &Registry) -> Vec<String> {
    let mut descriptions = Vec::new();
    for visible in env.visible() {
        for scope in registry.nameable_scopes(*visible) {
            let mut text = String::new();
            registry.describe(scope, &mut crate::names::Teaching(&mut text));
            if !descriptions.contains(&text) {
                descriptions.push(text);
            }
        }
    }
    descriptions
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::names::{Addressing, ColumnOrigin, Hint, Republish, ScopeOrigin, ValueFacts};

    fn published_column(registry: &Registry, scope: ScopeId, name: &str) -> ColId {
        registry.mint_column(
            scope,
            ColumnOrigin::Bound { position: 0 },
            Some(registry.intern(name, false)),
            Addressing::Published,
            ValueFacts::default(),
        )
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
        let registry = Registry::new(&[]);
        let scope = registry.mint_scope(
            ScopeOrigin::AnonRelation,
            Hint::User(registry.intern("source", false)),
            None,
        );
        let column = registry.mint_column(
            scope,
            ColumnOrigin::Bound { position: 0 },
            Some(registry.intern("Mixed", true)),
            Addressing::Published,
            ValueFacts::default(),
        );

        let reached = unify_columns(
            vec![stropped("Mixed", None)],
            &[column],
            &[scope],
            &registry,
        )
        .pop()
        .expect("one reference has one result");
        assert!(
            matches!(reached, UnificationResult::Resolved(found) if found == column),
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
            &[scope],
            &registry,
        )
        .pop()
        .expect("one reference has one result");
        assert!(
            matches!(reached, UnificationResult::Resolved(found) if found == column),
            "a stropped qualifier reaches the scope it names: {reached:?}"
        );

        for miss in [stropped("mixed", None), named("Mixed", None)] {
            let result = unify_columns(vec![miss], &[column], &[scope], &registry)
                .pop()
                .expect("one reference has one result");
            assert!(
                matches!(result, UnificationResult::Unresolved(_)),
                "only the written spelling reaches a stropped column: {result:?}"
            );
        }
    }

    #[test]
    fn bare_pipe_need_selects_the_available_boundary_occurrence() {
        let registry = Registry::new(&[]);
        let source = registry.mint_scope(
            ScopeOrigin::AnonRelation,
            Hint::User(registry.intern("source", false)),
            None,
        );
        let before = published_column(&registry, source, "returned");
        let stage =
            registry.mint_derived_scope(ScopeOrigin::PipeStage { input: source }, Hint::None);
        let after = *registry
            .republish_heading(source, stage, Republish::Passthrough)
            .in_order()
            .next()
            .unwrap();

        let result = unify_columns(
            vec![named("returned", None)],
            &[after],
            &[source],
            &registry,
        )
        .pop()
        .expect("one reference has one result");

        assert!(matches!(result, UnificationResult::Resolved(column) if column == after));
        assert_ne!(before, after);
    }

    #[test]
    fn bare_lookup_still_refuses_two_available_occurrences() {
        let registry = Registry::new(&[]);
        let left = registry.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
        let right = registry.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
        let left_column = published_column(&registry, left, "returned");
        let right_column = published_column(&registry, right, "returned");

        let result = unify_columns(
            vec![named("returned", None)],
            &[left_column, right_column],
            &[],
            &registry,
        )
        .pop()
        .expect("one reference has one result");

        assert!(matches!(result, UnificationResult::Ambiguous { .. }));
    }

    #[test]
    fn bare_lookup_does_not_expand_available_to_the_whole_scope() {
        let registry = Registry::new(&[]);
        let scope = registry.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
        let available = published_column(&registry, scope, "available");
        let _not_available = published_column(&registry, scope, "hidden");

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
        let registry = Registry::new(&[]);
        let carried = registry.canonical(registry.intern("emp2", false));
        let view = registry.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
        let arm = registry.mint_column(
            view,
            ColumnOrigin::Bound { position: 0 },
            Some(registry.intern("eid", false)),
            Addressing::BareAnswering(carried),
            ValueFacts::default(),
        );
        let other = registry.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
        let join = registry.mint_scope(
            ScopeOrigin::Join {
                left: view,
                right: other,
            },
            Hint::None,
            None,
        );
        let exported = *registry
            .republish_heading(view, join, Republish::Passthrough)
            .in_order()
            .next()
            .unwrap();

        let result = unify_columns(
            vec![named("eid", Some("emp2"))],
            &[exported],
            &[view],
            &registry,
        )
        .pop()
        .expect("one reference has one result");

        assert!(matches!(result, UnificationResult::Resolved(column) if column == arm));
    }

    /// Only a shared chain collapses. Two arms that separately answer under one
    /// name are rivals however alike they look, and the reference naming both
    /// still has no single column to mean.
    #[test]
    fn qualified_lookup_still_refuses_two_unrelated_carriers() {
        let registry = Registry::new(&[]);
        let carried = registry.canonical(registry.intern("emp2", false));
        let carrier = || {
            let scope = registry.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
            registry.mint_column(
                scope,
                ColumnOrigin::Bound { position: 0 },
                Some(registry.intern("eid", false)),
                Addressing::BareAnswering(carried),
                ValueFacts::default(),
            )
        };
        let left = carrier();
        let right = carrier();

        let result = unify_columns(
            vec![named("eid", Some("emp2"))],
            &[left, right],
            &[],
            &registry,
        )
        .pop()
        .expect("one reference has one result");

        assert!(matches!(result, UnificationResult::Ambiguous { .. }));
    }

    #[test]
    fn qualified_lookup_can_reach_the_visible_pre_boundary_scope() {
        let registry = Registry::new(&[]);
        let source = registry.mint_scope(
            ScopeOrigin::AnonRelation,
            Hint::User(registry.intern("source", false)),
            None,
        );
        let before = published_column(&registry, source, "returned");
        let stage =
            registry.mint_derived_scope(ScopeOrigin::PipeStage { input: source }, Hint::None);
        let after = *registry
            .republish_heading(source, stage, Republish::Passthrough)
            .in_order()
            .next()
            .unwrap();

        let result = unify_columns(
            vec![named("returned", Some("source"))],
            &[after],
            &[source],
            &registry,
        )
        .pop()
        .expect("one reference has one result");

        assert!(matches!(result, UnificationResult::Resolved(column) if column == before));
    }
}
