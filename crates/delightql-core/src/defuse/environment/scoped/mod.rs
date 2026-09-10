// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE SCOPED-DEFINITION AUTHORITY.
//!
//! A query-scoped definition — a CFE or a CHOE — is selected as a carrier
//! that holds the authored definition together with the [`LexicalSite`] its
//! registration stamped. This module owns both halves and every operation
//! that spends the body.
//!
//! **The body never leaves.** Nothing here answers with the definition, its
//! clauses, or its unresolved body except through a value that keeps that
//! definition's declaration standing on the world for the borrow's whole
//! extent. Outside this module no safe signature yields a scoped body at
//! all, so no caller can resolve one in a world where its declaration does
//! not stand — the interpretation and the syntax are inseparable by
//! construction rather than by a push-and-pop convention at each use.
//!
//! What a caller may ask for is METADATA: a name, a context mode, a formal
//! inventory, a head. None of it decides how a body reads.
//!
//! The EFFECT manifestations — the effect-mirror CHOE's clauses and an
//! effect label's arms — are read by exactly one descendant: the
//! effect-body authority in [`effect`], which stands beneath this module so
//! that it reaches those clauses through the private fields above and no
//! accessor has to exist.

pub(crate) mod effect;

use super::{Environment, FormalBindings, LexicalSite};
use crate::error::Result;
use crate::pipeline::resolver::resolver_fold::ResolverFold;

/// A SELECTED QUERY-SCOPED VALUE DEFINITION: its clauses and the site its
/// registration stamped, in one value with no projection that takes them
/// apart.
#[derive(Debug, Clone)]
pub(crate) struct ScopedCfe {
    definition: crate::pipeline::asts::core::CfeDefinition,
    site: LexicalSite,
}

/// A SELECTED QUERY-SCOPED HIGHER-ORDER DEFINITION, under the same law.
#[derive(Debug, Clone)]
pub(crate) struct ScopedHo {
    definition: crate::pipeline::asts::core::HoDefinition,
    site: LexicalSite,
}

impl ScopedCfe {
    /// STAMPED BY REGISTRATION — the one act that knows both halves.
    pub(super) fn stamped(
        definition: crate::pipeline::asts::core::CfeDefinition,
        site: LexicalSite,
    ) -> Self {
        ScopedCfe { definition, site }
    }

    pub(super) fn site(&self) -> LexicalSite {
        self.site
    }

    /// The horizon this definition's claim minted — the metadata the
    /// selection ladder compares. Not a road to the body.
    pub(super) fn declared_horizon(&self) -> crate::pipeline::asts::core::LexicalHorizon {
        self.definition.horizon()
    }

    // ── Metadata: what the binding prologue reads, left of the body ──

    pub(in crate::defuse) fn name(&self) -> &delightql_types::SqlIdentifier {
        &self.definition.name
    }

    pub(in crate::defuse) fn context_mode(&self) -> &crate::pipeline::asts::core::ContextMode {
        &self.definition.context_mode
    }

    pub(in crate::defuse) fn callable_formals(&self) -> &[crate::pipeline::asts::core::CfeFormal] {
        self.definition.callable_formals()
    }

    pub(in crate::defuse) fn scalar_formals(&self) -> &[crate::pipeline::asts::core::CfeFormal] {
        self.definition.scalar_formals()
    }

    // ── The body-spending operations ──

    /// RESOLVE THIS DEFINITION'S BODY AS A SCALAR, in the world it was born
    /// in, at the site its declaration stands in — however deep the mention
    /// that spends it. The declaration is installed and removed by this
    /// operation on every path, and the body is not handed anywhere.
    ///
    /// A definition's body is SEALED: only its formals, and for a
    /// context-aware definition its declared captures, reach the call
    /// site's row. Implicit context is the one deliberate unsealing.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::defuse) fn resolve_scalar(
        &self,
        formals: FormalBindings,
        core: &mut crate::resolution::ResolverCore<'_>,
        env: &mut Environment,
        config: crate::pipeline::resolver::ResolutionConfig,
        grade: crate::defuse::bound_use::CallableGrade,
        row: Option<super::EnclosingRow<'_>>,
        obligation: Option<crate::pipeline::resolver::resolver_fold::WindowObligation>,
    ) -> (
        Result<Option<crate::pipeline::ast_resolved::DomainExpression>>,
        Option<crate::pipeline::resolver::resolver_fold::WindowObligation>,
    ) {
        use crate::pipeline::ast_transform::AstTransform;
        env.push_scoped_declaration(self);
        let mut lease = env.instantiated(formals);
        let mut child = match row {
            Some(row) => ResolverFold::enclosed(
                &mut *core,
                lease.world(),
                config,
                row.0,
                crate::pipeline::resolver::Correlations::InPlace,
            ),
            None => ResolverFold::new(&mut *core, lease.world(), config),
        };
        child.position_grade = grade;
        child.window_obligation = obligation;
        let out = child
            .transform_domain(self.definition.body.clone())
            .map(Some);
        let obligation = child.window_obligation.take();
        drop(lease);
        env.pop_declaration();
        (out, obligation)
    }

    /// The COVER-CELL twin: the same body, the same declaration, resolved
    /// through the caller's own fold.
    pub(in crate::defuse) fn resolve_cover(
        &self,
        formals: FormalBindings,
        fold: &mut ResolverFold<'_, '_>,
    ) -> Result<crate::pipeline::ast_resolved::DomainExpression> {
        use crate::pipeline::ast_transform::AstTransform;
        let config = fold.config.clone();
        let grade = fold.position_grade;
        fold.env.push_scoped_declaration(self);
        let mut lease = fold.env.instantiated(formals);
        let mut child = ResolverFold::new(&mut *fold.core, lease.world(), config);
        child.position_grade = grade;
        let out = child.transform_domain(self.definition.body.clone());
        drop(lease);
        fold.env.pop_declaration();
        out
    }

    /// CLOSE THIS DEFINITION AS A CODE ACTUAL: its body resolves here, with
    /// holes standing for its own scalar formals, so the callee receives
    /// finished code rather than a spelling to look up. The holes are built
    /// from this definition's own declaration, and the declaration stands
    /// while the body resolves.
    pub(in crate::defuse) fn close_curried(
        &self,
        fold: &mut ResolverFold<'_, '_>,
    ) -> Result<(usize, crate::pipeline::ast_resolved::DomainExpression)> {
        let arity = self.definition.scalar_formals().len();
        let formals = self.hole_frame()?;
        let resolved = self.resolve_cover(formals, fold)?;
        Ok((arity, resolved))
    }

    /// THE SLOT ROAD'S WORLD: this body, the frame it spends, and this
    /// definition's declaration site, minted together. The site comes from
    /// `self`, so a slot cannot be given one definition's formals and
    /// another's lexical position, and the body arrives with both.
    pub(in crate::defuse) fn spending(&self, formals: FormalBindings) -> ScopedSlotWorld {
        ScopedSlotWorld {
            formals,
            site: self.site,
            body: self.definition.body.clone(),
        }
    }

    /// A frame of formal HOLES, one per declared scalar formal.
    fn hole_frame(&self) -> Result<FormalBindings> {
        use super::FormalRole;
        let formals = self.definition.scalar_formals();
        let arity = formals.len();
        let mut inventory = super::FormalInventory::declared(
            formals
                .iter()
                .map(|formal| (formal.name.clone(), FormalRole::Value)),
        );
        inventory
            .bind_positional(FormalRole::Value, (0..arity as u32).map(curried_hole))
            .map_err(|error| {
                crate::defuse::admitted::named_binding_refusal(self.definition.name.as_str(), error)
            })?;
        Ok(inventory.sealed())
    }
}

fn curried_hole(index: u32) -> crate::pipeline::asts::resolved::DomainExpression {
    crate::pipeline::asts::resolved::DomainExpression::Application(
        crate::pipeline::asts::resolved::FunctionApplication::Open(
            crate::pipeline::asts::core::FormalHole(index),
        ),
    )
}

/// A SEALED SLOT BODY'S WORLD: the body, the frame it spends, and the
/// declaration site of the definition that owns it. Minted only by
/// [`ScopedCfe::spending`], so all three arrive from one selected
/// definition and none of them is separately supplied.
#[derive(Debug)]
pub(crate) struct ScopedSlotWorld {
    formals: FormalBindings,
    site: LexicalSite,
    body: crate::pipeline::ast_unresolved::DomainExpression,
}

impl ScopedSlotWorld {
    pub(super) fn site(&self) -> LexicalSite {
        self.site
    }

    /// The caller-resolved actuals this body's formal references spend.
    pub(crate) fn formals(&self) -> &FormalBindings {
        &self.formals
    }

    /// CONVERT the sealed body under this world. The allowance stands in
    /// the caller's world with this definition's frame and site; the body
    /// is consumed here and reaches no other road.
    pub(in crate::defuse) fn convert(
        self,
        registry: &crate::names::Registry,
        allowance: crate::pipeline::resolver::SlotInstantiation<'_, '_>,
    ) -> Result<Option<crate::pipeline::asts::resolved::DomainExpression>> {
        use crate::pipeline::ast_transform::AstTransform;
        use crate::pipeline::resolver::StrictPhaseConverter;
        let nested = allowance.in_scoped(&self);
        let mut converter = StrictPhaseConverter::sealed(registry, nested);
        converter.transform_domain(self.body.clone()).map(Some)
    }
}

impl ScopedHo {
    pub(super) fn declaration_site(&self) -> LexicalSite {
        self.site
    }

    /// STAMPED BY REGISTRATION.
    pub(super) fn stamped(
        definition: crate::pipeline::asts::core::HoDefinition,
        site: LexicalSite,
    ) -> Self {
        ScopedHo { definition, site }
    }

    // ── Metadata: the head this definition publishes, never its clauses ──

    pub(in crate::defuse) fn name(&self) -> &delightql_types::SqlIdentifier {
        self.definition.name()
    }

    pub(in crate::defuse) fn params(&self) -> &[crate::pipeline::asts::ddl::HoParam] {
        self.definition.group().params()
    }

    pub(in crate::defuse) fn head_items(
        &self,
    ) -> crate::pipeline::asts::core::definitions::HeadItems {
        self.definition.group().first().head.items.clone()
    }

    /// This definition's positions, analyzed from its assembled heads and
    /// column-named from its clauses — the same analysis a consulted family
    /// without stored positions receives. The analysis happens HERE, on the
    /// definition this carrier holds, so no signature elsewhere takes a
    /// scoped definition in order to describe one.
    pub(in crate::defuse) fn positions(&self) -> Vec<crate::pipeline::asts::ddl::HoPositionInfo> {
        crate::defuse::ho::ensure_position_column_names(
            crate::pipeline::resolver::grounding::build_ho_position_analysis(
                self.definition.group(),
            ),
            self.definition.group().clauses(),
        )
    }

    /// SPEND THIS DEFINITION: the one operation that reads its clauses.
    ///
    /// The body opens FIRST — the formal frame, the caller-resolved relation
    /// carriers and this definition's own declaration stand on the world for
    /// exactly the expansion's extent — and every later act happens inside
    /// that opening: the authored clause text is reconstructed with this
    /// use's bindings, shaped against the analyzed positions, collected into
    /// one query-local block, and RESOLVED in the same world the declaration
    /// stands in. What returns is a resolved query. The clauses never become
    /// a value, so no caller can hold them, clone them, or resolve them
    /// somewhere else; the syntax and the world that interprets it are one
    /// operation rather than two.
    pub(in crate::defuse) fn expand(
        &self,
        spending: ChoeSpending<'_>,
        formals: FormalBindings,
        carriers: &crate::defuse::carriers::CarrierRecord,
        scoped_world: Option<super::ClosedLexicalWorld>,
        caller: &mut ResolverFold<'_, '_>,
    ) -> Result<crate::pipeline::resolver::ResolvedQuery> {
        let ChoeSpending {
            function,
            positions,
            actuals,
            join_input_scope,
            frame,
            crossing_carriers,
        } = spending;
        let facts = crate::defuse::admitted::BodyPositionFacts::authored_of(caller);
        let mut scoped_world = scoped_world.map(super::ClosedLexicalWorld::open);
        let world = match &mut scoped_world {
            Some(world) => world,
            None => &mut *caller.env,
        };
        let mut lease = world.opened_body(formals, carriers, self);
        let group = self.definition.group();
        // A multi-clause CHOE binds its clauses under one frontier so they
        // accumulate as one definition; a self-reference never reaches the
        // frontier — admission refused it before the body opened.
        let frontier = (group.clauses().len() > 1).then(|| frame.frontier(group, Vec::new()));
        let mut block = crate::pipeline::asts::core::QueryLocalBlock::default();
        for clause in group.clauses() {
            let crate::pipeline::asts::ddl::DdlBody::Deferred { source } = &clause.body else {
                return Err(crate::diagnostic::Internal::invariant(
                    "choe",
                    "a common higher-order expression holds its body as authored text; \
                     a clause built any other way cannot be bound",
                ));
            };
            let q = crate::ddl::reconstruct::bound_relex(source, actuals.bindings.clone())?;
            // A CHOE clause always admits the caller: a self-reference was
            // refused at admission, so no clause reads a frontier.
            let q = crate::defuse::admitted::shape_bound_clause(
                q,
                clause.params().to_vec(),
                clause.head_items(),
                positions,
                actuals,
                join_input_scope.clone(),
                crate::defuse::ClauseCaller::Admitted,
                &[],
            );
            crate::defuse::ho::extract_clause_ctes(
                q,
                function,
                frontier.as_ref(),
                crate::defuse::ClauseCaller::Admitted,
                &mut block,
            )?;
        }
        let main_query = crate::pipeline::ast_unresolved::Chain::read(
            crate::pipeline::ast_unresolved::Relation::Ground {
                mention: crate::pipeline::ast_unresolved::GroundMention::Named {
                    identifier: crate::pipeline::ast_unresolved::QualifiedName {
                        namespace_path: crate::pipeline::ast_unresolved::NamespacePath::empty(),
                        name: function.into(),
                    },
                    alias: None,
                    mutation_target: false,
                    passthrough: false,
                },
                outer: false,
            },
            crate::pipeline::ast_unresolved::Access::All,
        );
        let squished = crate::pipeline::ast_unresolved::Query::binding(block.seal()?, main_query);
        let mut fold =
            crate::defuse::admitted::body_fold_in(&mut *caller.core, lease.world(), facts);
        fold.crossing_carriers = crossing_carriers.to_vec();
        crate::pipeline::resolver::resolve_query_with(&mut fold, squished)
    }
}

/// WHAT A USE SUPPLIES to a CHOE expansion: caller-resolved actuals and the
/// metadata analyzed from the declaration. Every field is the use's own; not
/// one of them is, or reaches, a definition body.
pub(in crate::defuse) struct ChoeSpending<'a> {
    pub(in crate::defuse) function: &'a str,
    pub(in crate::defuse) positions: &'a [crate::pipeline::asts::ddl::HoPositionInfo],
    pub(in crate::defuse) actuals: &'a crate::defuse::admitted::HoActuals,
    pub(in crate::defuse) join_input_scope: Option<crate::relation::StructuralRelation>,
    pub(in crate::defuse) frame: &'a crate::defuse::instance::InstanceFrame,
    pub(in crate::defuse) crossing_carriers: &'a [crate::relation::PortId],
}

/// THE ARMS OF ONE EFFECT-MARKED CTE LABEL, with the site that declared
/// them. The label denotes the corresponding union of every arm written
/// under it, so the arms are held together and spent together.
///
/// The arms are unresolved bodies, and like every other body in this module
/// they never become a value outside it: there is no accessor, and the one
/// reader is the effect-body authority beneath this module, which walks
/// them itself in the declaration this carrier's site opens.
#[derive(Debug, Clone)]
pub(crate) struct ScopedEffectArms {
    arms: Vec<crate::pipeline::ast_unresolved::CteBinding>,
    site: LexicalSite,
}

impl ScopedEffectArms {
    pub(super) fn stamped(site: LexicalSite) -> Self {
        ScopedEffectArms {
            arms: Vec::new(),
            site,
        }
    }

    pub(super) fn admit(&mut self, arm: crate::pipeline::ast_unresolved::CteBinding) {
        self.arms.push(arm);
    }
}
