// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
/// A compiler-minted scope. The field is private and there is no public
/// constructor, so a sink that demands `FreshAlias` cannot be fed a
/// hard-coded spelling.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::pipeline::transformer) struct FreshAlias {
    identity: crate::names::ScopeId,
}

impl FreshAlias {
    pub(in crate::pipeline::transformer) fn identity(&self) -> crate::names::ScopeId {
        self.identity
    }
}

/// A scope name with declared provenance, demanded by the `Builder`
/// entry constructors. The `Fresh` arm cannot be forged (see
/// `FreshAlias`); the `Resolved` arm is an explicit claim at the call
/// site that the scope came from the AST — a user alias or a base-table
/// occurrence the resolver decided. A bare string no longer typechecks at
/// those sinks.
#[derive(Clone, Debug)]
pub(in crate::pipeline::transformer) enum ScopeName {
    Resolved(crate::names::ScopeId),
    Fresh(FreshAlias),
}

impl ScopeName {
    pub(in crate::pipeline::transformer) fn into_scope(self) -> crate::names::ScopeId {
        match self {
            ScopeName::Resolved(scope) => scope,
            ScopeName::Fresh(fresh) => fresh.identity(),
        }
    }
}

impl From<FreshAlias> for ScopeName {
    fn from(fresh: FreshAlias) -> Self {
        ScopeName::Fresh(fresh)
    }
}

#[derive(Clone)]
pub(in crate::pipeline) struct NameGenerator {
    identities: std::rc::Rc<crate::names::Registry>,
}

impl std::fmt::Debug for NameGenerator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NameGenerator").finish()
    }
}

impl NameGenerator {
    /// Create a new generator with its own counter starting at 0.
    pub(in crate::pipeline) fn new(identities: std::rc::Rc<crate::names::Registry>) -> Self {
        Self { identities }
    }

    /// Create a generator that shares the same counter as another.
    /// Used when constructing child builders (e.g., join operands)
    /// that must not collide with names from sibling builders.
    pub(in crate::pipeline::transformer) fn fork(&self) -> Self {
        Self {
            identities: std::rc::Rc::clone(&self.identities),
        }
    }

    pub(in crate::pipeline::transformer) fn identities(&self) -> &crate::names::Registry {
        &self.identities
    }

    pub(in crate::pipeline::transformer) fn fresh(
        &self,
        identity: crate::names::ScopeId,
    ) -> FreshAlias {
        FreshAlias { identity }
    }

    pub(in crate::pipeline::transformer) fn anonymous(&self) -> FreshAlias {
        self.fresh(self.identities.anonymous_scope(None))
    }

    pub(in crate::pipeline::transformer) fn wrap(
        &self,
        input: crate::names::ScopeId,
        why: crate::names::WrapReason,
    ) -> FreshAlias {
        self.fresh(self.identities.wrap_scope(input, why))
    }

    pub(in crate::pipeline::transformer) fn set_arm(
        &self,
        input: crate::names::ScopeId,
        arm: u16,
    ) -> FreshAlias {
        self.fresh(self.identities.set_arm_scope(input, arm))
    }

    pub(in crate::pipeline::transformer) fn emission_alias(
        &self,
        input: crate::names::ScopeId,
    ) -> FreshAlias {
        self.fresh(self.identities.emission_alias_scope(input))
    }

    pub(in crate::pipeline::transformer) fn interior_emission(
        &self,
        owner: crate::names::ColId,
    ) -> FreshAlias {
        self.fresh(self.identities.interior_emission_scope(owner))
    }

    pub(in crate::pipeline::transformer) fn join(&self) -> FreshAlias {
        self.fresh(self.identities.join_scope())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generator() -> NameGenerator {
        NameGenerator::new(std::rc::Rc::new(crate::names::Registry::new(&[])))
    }

    fn fresh(generator: &NameGenerator) -> FreshAlias {
        generator.anonymous()
    }

    #[test]
    fn sequential_aliases_are_distinct_occurrences() {
        let gen = generator();
        let first = fresh(&gen).identity();
        let second = fresh(&gen).identity();
        assert_ne!(first, second);
    }

    #[test]
    fn forked_generators_share_the_registry() {
        let gen_a = generator();
        let first = fresh(&gen_a).identity();

        let gen_b = gen_a.fork();
        let second = fresh(&gen_b).identity();
        let third = fresh(&gen_a).identity();
        assert_ne!(first, second);
        assert_ne!(second, third);
        assert_ne!(first, third);
    }
}
