// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The duplicate-scope judgment's vocabulary.
//!
//! Two scopes addressable together never share a canonical answering
//! name. Which scopes are addressable together is the lexical frontier's
//! fact — the relations one position holds in view — and the registry
//! judges that co-visible set from the answer each scope was born under
//! or adopted as a stage owner. Nothing here records liveness: a scope is
//! in view exactly while a frontier holds it, and no index beside the
//! frontier can say otherwise.

/// Why an admission judgment refused.
#[derive(Debug)]
pub enum ScopeActivationRefusal {
    /// Two live scopes cannot share one canonical answering name. Carries
    /// the AUTHORED characters of the refused scope's answer, for the
    /// teaching.
    DuplicateAnswer { spelling: String },
}

impl From<ScopeActivationRefusal> for crate::error::DelightQLError {
    fn from(refusal: ScopeActivationRefusal) -> Self {
        match refusal {
            ScopeActivationRefusal::DuplicateAnswer { spelling } => {
                crate::error::DelightQLError::validation_error_categorized(
                    crate::uri_registry::subcat::SCOPE_DUPLICATE,
                    format!("two live scopes cannot share the name '{spelling}'"),
                    "give one of them its own name with `as` — or acknowledge \
                     delightql-danger://scope/duplicate to admit the ambiguity",
                )
            }
        }
    }
}

/// Whether the ruled danger gate is armed for this judgment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DuplicateScopePolicy {
    /// The default: a duplicate canonical answering name refuses.
    Refuse,
    /// `delightql-danger://scope/duplicate` is acknowledged: the namesakes
    /// stay co-visible.
    Acknowledged,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::names::{Registry, ScopeId};

    fn registry() -> Registry {
        Registry::new(&[])
    }

    fn named_scope(reg: &Registry, name: &str) -> ScopeId {
        let answer = reg.intern(name, false);
        reg.anonymous_scope(Some(answer))
    }

    #[test]
    fn duplicate_canonical_answering_names_refuse_in_one_co_visible_set() {
        let reg = registry();
        let q = named_scope(&reg, "q");
        let fold_equal = named_scope(&reg, "Q");
        let error = reg
            .refuse_shared_names(&[q, fold_equal], DuplicateScopePolicy::Refuse)
            .expect_err("a fold-equal namesake refuses");
        // The refusal names the SECOND spelling — the one that collided.
        assert!(error.to_string().contains("'Q'"), "{error}");
    }

    #[test]
    fn unrelated_occurrences_of_one_relation_do_not_collide() {
        let reg = registry();
        let here = named_scope(&reg, "users");
        let elsewhere = named_scope(&reg, "users");
        // Both exist in the compilation, but they never become addressable
        // together, so neither judgment sees the other.
        for scope in [here, elsewhere] {
            reg.refuse_shared_names(&[scope], DuplicateScopePolicy::Refuse)
                .expect("one scope is not a duplicate");
        }
    }

    #[test]
    fn the_acknowledged_danger_admits_the_namesakes() {
        let reg = registry();
        let first = named_scope(&reg, "q");
        let second = named_scope(&reg, "q");
        reg.refuse_shared_names(&[first, second], DuplicateScopePolicy::Acknowledged)
            .expect("the acknowledged danger admits");
    }

    #[test]
    fn anonymous_scopes_own_no_answering_name() {
        let reg = registry();
        let scopes: Vec<ScopeId> = (0..3).map(|_| reg.anonymous_scope(None)).collect();
        reg.refuse_shared_names(&scopes, DuplicateScopePolicy::Refuse)
            .expect("anonymous scopes have no answering name");
    }
}
