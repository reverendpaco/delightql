// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The definition-use authority.
//!
//! One sealed subsystem owns the relationship between an authored
//! definition family, its declaration environment, and its per-statement
//! use: it selects a current family exhaustively under the statement's one
//! catalog read ([`CatalogRead`]), opens it in its captured declaration
//! environment, and recognizes recursive re-entry by identity.
//!
//! Downstream authorities (relation construction, CTE binding, effects,
//! receipts, grouping, lowering) consume what this module links; they never
//! select a definition, choose its environment, or re-resolve its actuals.

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod admitted;
pub(crate) mod bound_use;
pub(crate) mod carriers;
pub(crate) use bound_use::ClosedRelationActual;
pub(crate) mod callable;
pub(crate) mod environment;
pub(crate) mod er;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod grounded_world;
pub(crate) mod ho;
pub(crate) mod instance;
pub(crate) mod select;

/// One unresolved recursive-frontier binding, minted only while the
/// definition-use authority owns the opened clause and its exact frontier.
/// Other compiler modules may move this value whole into the binding
/// authority, but cannot construct or project it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FrontierCte {
    body: crate::pipeline::ast_unresolved::Chain,
    frontier: instance::DefinitionFrontier,
    authority: crate::pipeline::asts::core::CteAuthority,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct FrontierGroup(instance::DefinitionFrontier);

impl FrontierGroup {
    pub(crate) fn name(&self) -> &delightql_types::SqlIdentifier {
        self.0.name()
    }

    pub(crate) fn fixpoint(&self) -> crate::pipeline::asts::vocabulary::Fixpoint {
        self.0.fixpoint()
    }

    pub(crate) fn register(
        &self,
        environment: &mut environment::Environment,
        relation: crate::relation::SemanticRelation,
    ) {
        environment.register_frontier(self.0.clone(), relation);
    }
}

impl FrontierCte {
    fn new(
        body: crate::pipeline::ast_unresolved::Chain,
        frontier: instance::DefinitionFrontier,
        authority: crate::pipeline::asts::core::CteAuthority,
    ) -> Self {
        FrontierCte {
            body,
            frontier,
            authority,
        }
    }

    pub(crate) fn body(&self) -> &crate::pipeline::ast_unresolved::Chain {
        &self.body
    }

    pub(crate) fn authority(&self) -> &crate::pipeline::asts::core::CteAuthority {
        &self.authority
    }

    pub(crate) fn group(&self) -> FrontierGroup {
        FrontierGroup(self.frontier.clone())
    }

    pub(crate) fn subject_lispy(&self) -> String {
        use crate::lispy::ToLispy;
        format!("(subject:frontier {})", self.frontier.to_lispy())
    }

    pub(crate) fn map_body(
        self,
        map: impl FnOnce(
            crate::pipeline::ast_unresolved::Chain,
        ) -> crate::error::Result<crate::pipeline::ast_unresolved::Chain>,
    ) -> crate::error::Result<Self> {
        Ok(FrontierCte {
            body: map(self.body)?,
            frontier: self.frontier,
            authority: self.authority,
        })
    }

    pub(crate) fn projected_through_head(
        self,
        items: &[crate::pipeline::asts::core::definitions::HeadItem],
        canonical_names: &[delightql_types::SqlIdentifier],
    ) -> Self {
        let body = crate::pipeline::asts::core::definitions::project_body_through_head(
            self.body,
            items,
            canonical_names,
        );
        let mut authority = self.authority;
        authority.head = crate::pipeline::asts::core::definitions::Head::glob();
        FrontierCte {
            body,
            frontier: self.frontier,
            authority,
        }
    }

    pub(crate) fn into_resolution(
        self,
    ) -> (
        crate::pipeline::ast_unresolved::Chain,
        crate::pipeline::asts::core::CteAuthority,
    ) {
        (self.body, self.authority)
    }

    pub(crate) fn folded<Q, F>(self, walk: &mut F) -> crate::error::Result<FrontierCrossing<Q>>
    where
        Q: crate::pipeline::asts::core::Phase,
        F: crate::pipeline::ast_transform::AstTransform<crate::pipeline::asts::core::Unresolved, Q>
            + ?Sized,
    {
        Ok(FrontierCrossing {
            body: walk.transform_relational_action(self.body)?.into_inner(),
            frontier: self.frontier,
            authority: self.authority,
        })
    }
}

/// A recursive-frontier binding crossing a generic AST walk. The body may
/// change phase, but the walk never receives the frontier or authority.
pub struct FrontierCrossing<P: crate::pipeline::asts::core::Phase> {
    body: crate::pipeline::asts::core::Chain<P>,
    frontier: instance::DefinitionFrontier,
    authority: crate::pipeline::asts::core::CteAuthority,
}

impl FrontierCrossing<crate::pipeline::asts::core::Unresolved> {
    pub(crate) fn into_frontier(self) -> FrontierCte {
        FrontierCte {
            body: self.body,
            frontier: self.frontier,
            authority: self.authority,
        }
    }
}

/// THE STATEMENT'S CATALOG READ — the one door through which the
/// definition-use authority reads the definition catalog.
///
/// It is a shared borrow of the system for the statement being compiled:
/// every selection, reach capture, and declared-mode lookup takes one, and
/// the resolver core holds it for the compilation's whole extent. Catalog
/// mutation — `consult!`, `reconsult!`, `unconsult!`, `ground!` — is an
/// exclusive (`&mut`) operation on the same system, so no mutation can
/// interleave with a statement holding this read: the exclusion is the
/// borrow checker's, not a runtime check. A statement therefore selects
/// from the catalog's one current state throughout, and the next
/// statement's read observes every completed replacement. The lazily
/// published system modules are the one thing a read may observe appear
/// (a namespace published after capture is seen at first use); nothing a
/// read already selected is ever replaced beneath it.
#[derive(Clone, Copy)]
pub(crate) struct CatalogRead<'s> {
    system: &'s crate::system::DelightQLSystem,
}

impl std::fmt::Debug for CatalogRead<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CatalogRead")
    }
}

impl<'s> CatalogRead<'s> {
    pub(crate) fn of(system: &'s crate::system::DelightQLSystem) -> Self {
        CatalogRead { system }
    }

    /// The system behind the read, for the questions that are not catalog
    /// rows (lazy module loading, the namespace-authority flag).
    pub(crate) fn system(self) -> &'s crate::system::DelightQLSystem {
        self.system
    }

    /// The bootstrap connection, locked for one catalog question.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn connection(
        self,
        context: &str,
    ) -> crate::error::Result<std::sync::MutexGuard<'s, rusqlite::Connection>> {
        self.system.lock_bootstrap(context)
    }
}
