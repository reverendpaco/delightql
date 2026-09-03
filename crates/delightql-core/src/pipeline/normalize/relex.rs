// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The relational line — chains, grelexes, continuations, and the let block.
//!
//! A chain is a HEAD and the continuations that consume it, in authored order.
//! Left-associativity is structural: `continuations[i]` consumes exactly the
//! relation `head ++ continuations[..i]` produces, so nothing here nests a
//! source and nothing downstream reconstructs pipe order.
//!
//! ## What a paren group asks
//!
//! Exactly one authority decides it, and it is [`Normalizer::access_of`]. A
//! mention's own parens, a call's access group and a directive's receipt are
//! the same question asked in three places; a dequalifying run says the same
//! thing the parens do, so `t(*)`, `t(*.(a))` and `t(.*)` fold INTO the
//! access rather than standing beside it. Anything else in the parens is a
//! derived table — the sneaky-parentheses inner relation — and that is where
//! the fold stops.

use super::{gap, Deferred, Normalizer};
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::definitions::Head;
use crate::pipeline::asts::core::expressions::pipes::DestructureMode;
use crate::pipeline::asts::core::expressions::InnerRelationPattern;
use crate::pipeline::asts::core::operators::JoinType;
use crate::pipeline::asts::core::provenance::CteOrigin;
use crate::pipeline::asts::core::Existence;
use crate::pipeline::asts::core::{
    Access, AnonRelation, AnonTable, ArrayPattern, ArrayPatternMember, Chain, Continuation,
    CteBinding, DangerSpec, DangerState, Datum, DomainExpression, ErJoinStep, FilterOrigin, Grelex,
    GroundForm, GroundMention, HeaderItem, InlineDdlSpec, LiteralValue, Membership, NamespacePath,
    OptionSpec, OptionState, PathBinding, PatternTarget, PipeOp, Probe, QualifiedName, Query,
    RecordPattern, RecordPatternMember, Relation, SetOperator, Slot, Step, TabularBody, TabularRow,
    TreePattern, Unresolved, ValueRow,
};
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::vocabulary::FunctorMarks;
use crate::pipeline::asts::vocabulary::{Vec1, Vec2};
use crate::pipeline::syntax::cst;
use delightql_types::SqlIdentifier;
use std::rc::Rc;

/// The heading payload, whichever form names it.
///
/// A first-order rule and a query-scoped binding spell the SAME heading, and
/// the generated CST gives each parent its own two-variant field enum. This is
/// where they meet, so the reading below is written once. The conversions are
/// exhaustive matches: a third heading form becomes a compile error here.
pub(crate) enum HeadingPayload<'t> {
    Argumentative(cst::ArgumentativeHeading<'t>),
    Glob(cst::GlobHeading<'t>),
}

impl<'t> From<cst::StandardCteHead<'t>> for HeadingPayload<'t> {
    fn from(head: cst::StandardCteHead<'t>) -> Self {
        match head {
            cst::StandardCteHead::ArgumentativeHeading(h) => HeadingPayload::Argumentative(h),
            cst::StandardCteHead::GlobHeading(h) => HeadingPayload::Glob(h),
        }
    }
}

impl<'t> From<cst::FoRuleHead<'t>> for HeadingPayload<'t> {
    fn from(head: cst::FoRuleHead<'t>) -> Self {
        match head {
            cst::FoRuleHead::ArgumentativeHeading(h) => HeadingPayload::Argumentative(h),
            cst::FoRuleHead::GlobHeading(h) => HeadingPayload::Glob(h),
        }
    }
}

/// One let-block binding as read: a relation binding (CTE, labelled or
/// effect-marked), or one clause of a common higher-order expression, which
/// waits for its siblings before it is a definition.
pub(crate) enum LetBinding {
    Relation(CteBinding<Unresolved>),
    HigherOrder {
        name: SqlIdentifier,
        effect: crate::pipeline::asts::core::CteEffectDeclaration,
        decl: crate::pipeline::asts::ddl::ClauseDecl,
    },
}

fn nested_preamble_refusal() -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "cte/nested_preamble",
        "only a relation binding may stand in a source's own preamble".to_string(),
        "declare the function, the common higher-order expression, or the DDL block \
         on the statement itself",
    )
}

/// What a heading says: the output head, and the fixpoint flavor the subject
/// badged.
pub(crate) struct Heading {
    pub head: Head,
    pub fixpoint: crate::pipeline::asts::vocabulary::Fixpoint,
}

impl<'t> Normalizer<'t> {
    // -----------------------------------------------------------------
    // Queries
    // -----------------------------------------------------------------

    pub(crate) fn relex_query(&mut self, node: cst::Relex<'t>) -> Result<Query<Unresolved>> {
        let body = self.require(node.body(), "a relex has a body")?;
        let chain = self.let_free_relex(body)?;
        self.wrap_let_block(node.let_block(), chain)
    }

    /// The let block is ONE block: ctes, cfes, choes, effect ctes and inline
    /// DDL intermixed. Which binding collection each kind lands in is decided
    /// here, once, in authored order — and so is each CHOE's LEXICAL
    /// HORIZON: a body sees the bindings declared before it, and the block
    /// is the only place that order is written.
    pub(crate) fn wrap_let_block(
        &mut self,
        block: Option<cst::LetBlock<'t>>,
        chain: Chain<Unresolved>,
    ) -> Result<Query<Unresolved>> {
        use crate::pipeline::asts::core::QueryLocalBlock;

        let mut read = QueryLocalBlock::default();
        let admit = |read: &mut QueryLocalBlock, binding: LetBinding| match binding {
            LetBinding::Relation(binding) => read.admit_relation(binding),
            LetBinding::HigherOrder { name, effect, decl } => {
                read.admit_ho_clause(name, effect, decl)
            }
        };
        // A nested preamble's bindings belong to THIS query: the source that
        // declared them built a chain, and a chain holds no let block.
        for hoisted in std::mem::take(&mut self.hoisted_ctes) {
            admit(&mut read, hoisted)?;
        }
        if let Some(block) = block {
            for child in block.children() {
                match child {
                    // A MEMBER'S OWN SOURCE MAY DECLARE BINDINGS. Draining after
                    // each member — not once before the walk — is what keeps them:
                    // the grammar admits a preamble binding either as this block's
                    // sibling or as the next member's source's own, and the two
                    // derivations must reach the same block. They land BEFORE the
                    // member that carried them, which is where they were written.
                    cst::LetBlockChild::Cte(cte) => {
                        let binding = self.cte(cte)?;
                        for hoisted in std::mem::take(&mut self.hoisted_ctes) {
                            admit(&mut read, hoisted)?;
                        }
                        admit(&mut read, binding)?;
                    }
                    cst::LetBlockChild::EffectCte(cte) => {
                        let binding = self.effect_cte(cte)?;
                        for hoisted in std::mem::take(&mut self.hoisted_ctes) {
                            admit(&mut read, hoisted)?;
                        }
                        admit(&mut read, binding)?;
                    }
                    cst::LetBlockChild::Cfe(cfe) => {
                        let cfe = self.cfe(cfe)?;
                        read.admit_cfe(cfe)?;
                    }
                    cst::LetBlockChild::DdlAnnotation(ddl) => {
                        let spec = self.ddl_annotation(ddl)?;
                        self.features().add_ddl_block(spec);
                    }
                }
            }
        }
        Ok(Query::binding(read.seal()?, chain))
    }

    /// A preamble declared where only a CHAIN can be built. Its bindings wait
    /// for the form that owns the query — the same list `wrap_let_block`
    /// drains, so there is one place a binding can come from.
    pub(crate) fn hoist_let_block(&mut self, block: Option<cst::LetBlock<'t>>) -> Result<()> {
        let Some(block) = block else {
            return Ok(());
        };
        for child in block.children() {
            let binding = match child {
                cst::LetBlockChild::Cte(cte) => self.cte(cte)?,
                cst::LetBlockChild::EffectCte(cte) => self.effect_cte(cte)?,
                cst::LetBlockChild::Cfe(_) | cst::LetBlockChild::DdlAnnotation(_) => {
                    return Err(nested_preamble_refusal())
                }
            };
            self.hoisted_ctes.push(binding);
        }
        Ok(())
    }

    // -----------------------------------------------------------------
    // The chain
    // -----------------------------------------------------------------

    #[stacksafe::stacksafe]
    pub(crate) fn let_free_relex(
        &mut self,
        node: cst::LetFreeRelex<'t>,
    ) -> Result<Chain<Unresolved>> {
        // A LEADING OUTER WAITS FOR ITS PEER. The marker changes ORIENTATION,
        // never its own meaning: the outer-marked head and its completing
        // comma member are one join, and which side is outer is what the
        // positions say.
        let mut chain = match (node.leading_outer(), node.peer()) {
            (Some(leading), Some(peer)) => {
                let mut left = self.leading_outer(leading)?;
                // The name stands on the access it was written beside, before
                // the join exists — after the member the same `as` would name
                // the RIGHT side, which is a different relation.
                if let Some(alias) = node.leading_outer_name() {
                    let name = self.require(alias.name(), "a stage name carries a name")?;
                    left = name_the_stage(left, self.identifier(name))?;
                }
                let member = self.require(peer.member(), "an outer peer is a relation")?;
                let right = self.grelex_like_member(member)?;
                let join_type = if right_is_outer(&right) {
                    JoinType::FullOuter
                } else {
                    JoinType::RightOuter
                };
                left.then(Step::authored(Continuation::Member {
                    rhs: right,
                    correlation: None,
                    join_type: Some(join_type),
                }))
            }
            _ => {
                let grelex = self.require(node.grelex(), "a relex begins with a grelex")?;
                self.grelex(grelex)?
            }
        };
        for child in node.children() {
            match child {
                cst::LetFreeRelexChild::Continuation(continuation) => {
                    chain = self.continuation(continuation, chain)?
                }
                // An annotation stands at any continuation anchor. It
                // decorates a POSITION and never changes the relex around
                // it, so it forks off the chain-so-far and the chain
                // continues unchanged.
                cst::LetFreeRelexChild::Annotation(annotation) => {
                    self.annotation(annotation, &chain)?
                }
            }
        }
        Ok(chain)
    }

    fn leading_outer(&mut self, node: cst::LeadingOuterGrelex<'t>) -> Result<Chain<Unresolved>> {
        match self.require(node.child(), "a leading outer is an outer access")? {
            cst::LeadingOuterGrelexChild::OuterGrelex(outer) => self.outer_grelex(outer),
            cst::LeadingOuterGrelexChild::OuterAnonGrelex(outer) => {
                let body = self.require(outer.child(), "an anonymous table has a body")?;
                let table = self.anon_body(body)?;
                Ok(Chain::authored(GroundForm::Literal(AnonRelation {
                    table,
                    alias: None,
                    outer: true,
                })))
            }
        }
    }

    /// A grelex yields a CHAIN, not a bare head: an interior's shaping
    /// continuations consume the relation its parens named, and the chain is
    /// where a consumed relation goes. `users(*)` is a chain of one.
    pub(crate) fn grelex(&mut self, node: cst::Grelex<'t>) -> Result<Chain<Unresolved>> {
        self.last_term = Some(self.text(node).to_string());
        match node {
            cst::Grelex::NamedGrelex(named) => self.named_grelex(named),
            cst::Grelex::AnonGrelex(anon) => {
                let body = self.require(anon.child(), "an anonymous table has a body")?;
                Ok(Chain::authored(GroundForm::Literal(AnonRelation::plain(
                    self.anon_body(body)?,
                ))))
            }
        }
    }

    /// The bare relation a position needs when it can hold nothing else — an
    /// edge term, whose identity IS its canonical spelling and which
    /// therefore has no interior to consume.
    /// The READ an edge term names: a mention and what its parens asked
    /// for, and nothing standing on the result.
    pub(crate) fn named_read(&mut self, node: cst::NamedGrelex<'t>) -> Result<Chain<Unresolved>> {
        let text = self.text(node).to_string();
        let read = self.named_grelex(node)?;
        if read.has_steps() || !matches!(read.head().form(), GroundForm::Reference(_)) {
            return Err(DelightQLError::validation_error_categorized(
                "grounding/er/endpoint",
                format!("'{text}' shapes its interior, so it names no single term"),
                "an edge selects by the term's exact canonical spelling",
            ));
        }
        Ok(read)
    }

    fn grelex_like_member(&mut self, node: cst::GrelexLikeMember<'t>) -> Result<Chain<Unresolved>> {
        self.last_term = Some(self.text(node).to_string());
        Ok(match node {
            cst::GrelexLikeMember::Grelex(grelex) => self.grelex(grelex)?,
            cst::GrelexLikeMember::OuterGrelex(outer) => self.outer_grelex(outer)?,
            cst::GrelexLikeMember::OuterAnonGrelex(outer) => {
                let body = self.require(outer.child(), "an anonymous table has a body")?;
                let table = self.anon_body(body)?;
                Chain::authored(GroundForm::Literal(AnonRelation {
                    table,
                    alias: None,
                    outer: true,
                }))
            }
            // The existence-marked anonymous table is truth, not a relation.
            // Comma normalization consumes it directly into membership, so a
            // road that asks for a relational member refuses here.
            cst::GrelexLikeMember::ExistsAnonGrelex(probe) => {
                return Err(DelightQLError::parse_error(format!(
                    "'{}' is truth and must stand in comma truth position",
                    self.text(probe)
                )))
            }
        })
    }

    // -----------------------------------------------------------------
    // Named relational forms
    // -----------------------------------------------------------------

    fn named_grelex(&mut self, node: cst::NamedGrelex<'t>) -> Result<Chain<Unresolved>> {
        match node {
            // `t()` — parens written, no dimension named. S08 rules this
            // inchoate where `t(*)` activates, so the two stay distinct
            // values rather than collapsing to one.
            cst::NamedGrelex::InchoateFunctor(functor) => {
                let name = self.require(functor.relation(), "a functor names a relation")?;
                self.ground_read(name, Access::Unasked, false)
            }
            cst::NamedGrelex::ArgumentativeFunctor(functor) => {
                let name = self.require(functor.relation(), "a functor names a relation")?;
                let form =
                    self.require(functor.arguments(), "an argumentative functor has slots")?;
                let access = self.slot_access(form)?;
                match functor.ho_part() {
                    Some(part) => self.higher_order_read(name, part, access, Vec::new()),
                    None => self.ground_read(name, access, false),
                }
            }
            cst::NamedGrelex::InteriorFunctor(functor) => {
                let name = self.require(functor.relation(), "a functor names a relation")?;
                let interior =
                    self.require(functor.interior(), "an interior functor has an interior")?;
                match functor.ho_part() {
                    Some(part) => {
                        let (access, rest) = self.call_group(interior)?;
                        self.higher_order_read(name, part, access, rest)
                    }
                    None => self.interior_read(name, interior, false),
                }
            }
            // THE CATALOG ANSWERS AS DATA: a pure relation, one row for the
            // named namespace. The trailing `::` is the mark that says the
            // subject is a namespace.
            cst::NamedGrelex::CatalogFunctor(functor) => self.catalog_functor(functor),
        }
    }

    fn outer_grelex(&mut self, node: cst::OuterGrelex<'t>) -> Result<Chain<Unresolved>> {
        let name = self.require(node.relation(), "an outer access names a relation")?;
        let interior = self.require(node.interior(), "an outer access has an interior")?;
        let (access, shaping) = match interior {
            cst::OuterGrelexInterior::ArgumentativeForm(form) => {
                (self.slot_access(form)?, Vec::new())
            }
            cst::OuterGrelexInterior::Interior(interior) => match node.ho_part() {
                Some(_) => self.call_group(interior)?,
                None => return self.interior_read(name, interior, true),
            },
        };
        // `?` is written on the ACCESS, and a higher-order access is an
        // access: the marker marks the call's own read outer exactly as it
        // marks a ground one.
        match node.ho_part() {
            Some(part) => {
                let reference = self.relation_reference(name)?;
                self.higher_order_call(
                    reference,
                    part,
                    access,
                    shaping,
                    FunctorMarks::with_evidence(true, false),
                )
            }
            None => self.ground_read(name, access, true),
        }
    }

    fn catalog_functor(&mut self, node: cst::CatalogFunctor<'t>) -> Result<Chain<Unresolved>> {
        let catalog = self.require(node.catalog(), "a catalog functor names a namespace")?;
        let segments = self.namespace_segments(catalog);
        // THE CATALOG WRAPPER IS ADDRESSED BY ITS OWN SPELLING. The relation
        // lives in `sys::meta` and its NAME is the namespace with the trailing
        // `::` kept — that is what the resolver looks up and what the
        // generator view expands. Dropping the marker and qualifying by the
        // namespace instead names a table nobody has.
        let identifier = QualifiedName {
            namespace_path: NamespacePath::from_parts(vec!["sys".to_string(), "meta".to_string()])
                .map_err(|error| {
                    DelightQLError::parse_error(format!("invalid catalog namespace: {error:?}"))
                })?,
            name: SqlIdentifier::new(format!(
                "{}::",
                segments
                    .iter()
                    .map(|segment| segment.as_str())
                    .collect::<Vec<_>>()
                    .join("::")
            )),
        };
        match node.interior() {
            None => Ok(self.mention_read(identifier, false, Access::Unasked, false)),
            Some(cst::CatalogFunctorInterior::ArgumentativeForm(form)) => {
                let access = self.slot_access(form)?;
                Ok(self.mention_read(identifier, false, access, false))
            }
            Some(cst::CatalogFunctorInterior::Interior(interior)) => {
                self.interior_relation_of(identifier, false, interior, false)
            }
        }
    }

    // -----------------------------------------------------------------
    // Ground reads and the access fold
    // -----------------------------------------------------------------

    fn ground_read(
        &mut self,
        name: cst::RelationName<'t>,
        access: Access<Unresolved>,
        outer: bool,
    ) -> Result<Chain<Unresolved>> {
        let (identifier, passthrough) = self.relation_identifier(name)?;
        Ok(self.mention_read(identifier, passthrough, access, outer))
    }

    /// A written ground read, with the call-site substitutions that belong to
    /// the boundary where the name is still characters. A relation FORMAL
    /// resolves to what the call site supplied, and after this line the body
    /// cannot tell it was ever written as a parameter.
    pub(crate) fn mention_read(
        &mut self,
        identifier: QualifiedName,
        passthrough: bool,
        access: Access<Unresolved>,
        outer: bool,
    ) -> Chain<Unresolved> {
        if let Some(bound) = self.bound_relation(&identifier, access.clone(), outer) {
            return bound;
        }
        ground_read(
            GroundMention::Named {
                identifier,
                alias: None,
                mutation_target: false,
                passthrough,
            },
            access,
            outer,
        )
    }

    /// Three bindings reach a ground read:
    ///
    /// - a compiler-owned carrier, read by IDENTITY — an interior CTE the
    ///   invocation materialized reaches the body exactly here, and its
    ///   declared column names become the caller pattern;
    /// - a supplied relation EXPRESSION, which arrives whole;
    /// - a supplied NAME, which swaps the spelling and nothing else.
    fn bound_relation(
        &mut self,
        identifier: &QualifiedName,
        access: Access<Unresolved>,
        outer: bool,
    ) -> Option<Chain<Unresolved>> {
        // A qualified name addresses a namespace, and a formal has none.
        if !identifier.namespace_path.is_empty() {
            return None;
        }
        let bindings = self.bindings()?.clone();
        let formal = identifier.name.as_str();
        if let Some(chain) = bindings.table_scope_relation(formal, access.clone(), None, outer) {
            return Some(chain);
        }
        bindings.table_expr_params.get(formal).cloned()
    }

    fn interior_read(
        &mut self,
        name: cst::RelationName<'t>,
        interior: cst::Interior<'t>,
        outer: bool,
    ) -> Result<Chain<Unresolved>> {
        let (identifier, passthrough) = self.relation_identifier(name)?;
        self.interior_relation_of(identifier, passthrough, interior, outer)
    }

    fn interior_relation_of(
        &mut self,
        identifier: QualifiedName,
        passthrough: bool,
        interior: cst::Interior<'t>,
        outer: bool,
    ) -> Result<Chain<Unresolved>> {
        let (access, rest) = self.fold_interior(interior)?;
        // Nothing but the dequalifying run: the parens said what the mention
        // asks and no derived table is needed.
        if rest.is_empty() {
            return Ok(self.mention_read(identifier, passthrough, access, outer));
        }
        // SNEAKY PARENTHESES: a shaping interior is a derived table, and THE
        // IMPLICIT STAR says an interior continuation always starts
        // realised: `p(C) ≡ p(*) C`, so a leading run that named nothing is
        // asking for everything, never leaving the base read inchoate.
        let access = match access {
            Access::Unasked => Access::All,
            other => other,
        };
        let mut subquery = self.mention_read(identifier.clone(), passthrough, access, false);
        for continuation in rest {
            subquery = self.continuation(continuation, subquery)?;
        }
        Ok(Chain::authored(GroundForm::Reference(
            Relation::InnerRelation {
                pattern: InnerRelationPattern::Indeterminate {
                    identifier,
                    subquery: Box::new(subquery),
                },
                alias: None,
                outer,
            },
        )))
    }

    /// The interior's LEADING dequalifying run, folded into the access it
    /// says, and everything after it.
    ///
    /// `*` activates, `.(cols)` names the shared columns, `.*` dequalifies
    /// every shareable name. Those three say exactly what a mention's parens
    /// say, so they belong to the access. The run stops at the first
    /// continuation that shapes the relation instead of describing the read.
    fn fold_interior(
        &mut self,
        interior: cst::Interior<'t>,
    ) -> Result<(Access<Unresolved>, Vec<cst::Continuation<'t>>)> {
        let mut access: Option<Access<Unresolved>> = None;
        let mut rest = Vec::new();
        let mut folding = true;
        for continuation in interior.children() {
            if folding {
                match access_step(continuation) {
                    Some(step) => {
                        let step = self.access_run_step(step)?;
                        access = Some(fold_access(access, step));
                        continue;
                    }
                    None => folding = false,
                }
            }
            rest.push(continuation);
        }
        Ok((access.unwrap_or(Access::Unasked), rest))
    }

    /// One run step, with its named columns read.
    fn access_run_step(&mut self, node: AccessStep<'t>) -> Result<AccessRunStep> {
        match node {
            AccessStep::Activate => Ok(AccessRunStep::Activate),
            AccessStep::Dequalify(using) => {
                let mut columns = Vec::new();
                for child in using.children() {
                    match child {
                        cst::UsingChild::Reference(reference) => {
                            columns.push(self.dequalified_name(reference)?)
                        }
                        cst::UsingChild::CommaSigil(_) => {}
                    }
                }
                // `.*` names no column: the intersection is computed at the
                // join, which is a different access, not an empty list.
                Ok(if columns.is_empty() {
                    AccessRunStep::DequalifyAll
                } else {
                    AccessRunStep::Dequalify(columns)
                })
            }
        }
    }

    fn dequalified_name(&mut self, node: cst::Reference<'t>) -> Result<SqlIdentifier> {
        match node {
            cst::Reference::NamedReference(reference) => {
                let name = self.require(reference.name(), "a dequalified column has a name")?;
                Ok(self.identifier(name))
            }
            // As above: the law admits a `reference`, the carrier holds a
            // spelling, and the ordinal waits for a carrier that can hold it.
            cst::Reference::PositionalReference(ordinal) => Err(gap(
                Deferred::DequalifyOrdinal,
                format!(
                    "the ordinal {} where a dequalifying access names a column",
                    self.text(ordinal)
                ),
            )),
        }
    }

    /// The relation an inner form names, for the truth-position probes that
    /// carry one. The interior is the same interior an ordinary mention has,
    /// read by the same authority.
    pub(crate) fn interior_relation(
        &mut self,
        callee: cst::RelationName<'t>,
        ho_part: Option<cst::HoPart<'t>>,
        interior: cst::InteriorContinuation<'t>,
    ) -> Result<Chain<Unresolved>> {
        let inner = self.require(interior.child(), "an interior continuation has an interior")?;
        let (access, rest) = self.fold_interior(inner)?;
        // A probe reads the whole relation unless the parens narrowed it:
        // there is nothing to project onto, so an unasked interior activates.
        let access = match access {
            Access::Unasked => Access::All,
            other => other,
        };
        // `ho_part as on every functor`: an inner form NAMES a relation, and a
        // relation the caller parameterizes is the same relation.
        if let Some(part) = ho_part {
            let reference = self.relation_reference(callee)?;
            return self.higher_order_call(reference, part, access, rest, FunctorMarks::default());
        }
        let (identifier, passthrough) = self.relation_identifier(callee)?;
        let mut chain = self.mention_read(identifier, passthrough, access, false);
        for continuation in rest {
            chain = self.continuation(continuation, chain)?;
        }
        Ok(chain)
    }

    fn higher_order_read(
        &mut self,
        name: cst::RelationName<'t>,
        part: cst::HoPart<'t>,
        access: Access<Unresolved>,
        shaping: Vec<cst::Continuation<'t>>,
    ) -> Result<Chain<Unresolved>> {
        let reference = self.relation_reference(name)?;
        self.higher_order_call(reference, part, access, shaping, FunctorMarks::default())
    }

    /// ONE higher-order application, wherever a functor takes an argument
    /// row — a chain head, an outer access, an existence probe, an inner
    /// form. What differs between those positions is the call-site EVIDENCE
    /// the marks carry, never the call.
    pub(crate) fn higher_order_call(
        &mut self,
        reference: crate::pipeline::asts::vocabulary::Ref,
        part: cst::HoPart<'t>,
        access: Access<Unresolved>,
        shaping: Vec<cst::Continuation<'t>>,
        marks: FunctorMarks,
    ) -> Result<Chain<Unresolved>> {
        let ho_arguments = self.ho_arguments(part)?;
        self.higher_order_call_with(reference, ho_arguments, access, shaping, marks)
    }

    /// The same call, its arguments already built — the road a call group
    /// takes wherever it opens without a `ho_part` node of its own.
    pub(crate) fn higher_order_call_with(
        &mut self,
        reference: crate::pipeline::asts::vocabulary::Ref,
        ho_arguments: Vec<crate::pipeline::asts::core::operators::HoArgument<Unresolved>>,
        access: Access<Unresolved>,
        shaping: Vec<cst::Continuation<'t>>,
        marks: FunctorMarks,
    ) -> Result<Chain<Unresolved>> {
        let mut call = crate::pipeline::asts::core::FunctorCall::written(reference, ho_arguments);
        call.marks = marks;
        // THE ACCESS GROUP STANDS WHERE AN ACCESS STANDS. `f(x)(*)` asks the
        // relation the call publishes for its dimensions, exactly as
        // `users(*)` asks the relation a name publishes; a call publishes no
        // relation by itself, so it holds no access of its own.
        let mut chain = Chain::read(
            Relation::FunctorCall {
                alias: None,
                call: crate::pipeline::asts::core::SealedCall::authored(call),
            },
            access,
        );
        for continuation in shaping {
            chain = self.continuation(continuation, chain)?;
        }
        Ok(chain)
    }

    pub(crate) fn relation_identifier(
        &self,
        node: cst::RelationName<'t>,
    ) -> Result<(QualifiedName, bool)> {
        match self.require(node.child(), "a relation name has a spelling")? {
            cst::RelationNameChild::PredicateIdentifier(name) => {
                Ok((self.qualified_reference_name(name)?, false))
            }
            // THE ENGINE'S CATALOG IS THE ENGINE'S: the slash routes past
            // DQL's catalog, so the engine segment travels as the namespace
            // and the read is marked as one DQL never cataloged.
            cst::RelationNameChild::EngineReference(engine) => {
                let namespace =
                    self.require(engine.engine(), "an engine reference names an engine")?;
                let name = self.require(engine.name(), "an engine reference names a relation")?;
                Ok((
                    QualifiedName {
                        namespace_path: NamespacePath::single(
                            self.identifier(namespace).as_str().to_string(),
                        ),
                        name: self.engine_name(name),
                    },
                    true,
                ))
            }
        }
    }

    // -----------------------------------------------------------------
    // Slot rows
    // -----------------------------------------------------------------

    /// A slot never holds a PREDICATE: a truth expression standing here is
    /// the CROSSING's value, and the predicate reading takes the comma road.
    pub(crate) fn slot_access(
        &mut self,
        node: cst::ArgumentativeForm<'t>,
    ) -> Result<Access<Unresolved>> {
        // Each slot is read into the SLOT it is, not into a term a later
        // classifier re-reads. The crossing is built here, so a constrained
        // position says in its own type that a truth stands there.
        let mut slots = Vec::new();
        for slot in self.slot_nodes(node) {
            slots.push(self.slot(slot)?);
        }
        Ok(
            match crate::pipeline::asts::vocabulary::Vec1::try_from_vec(slots) {
                Some(slots) => Access::Slots(slots),
                None => Access::Unasked,
            },
        )
    }

    pub(crate) fn slot_nodes(&self, node: cst::ArgumentativeForm<'t>) -> Vec<cst::Slot<'t>> {
        node.children()
            .filter_map(|child| match child {
                cst::ArgumentativeFormChild::Slot(slot) => Some(slot),
                cst::ArgumentativeFormChild::CommaSigil(_) => None,
            })
            .collect()
    }

    /// ONE slot, read as the slot it is.
    ///
    /// A bare name binds, a qualified name REUSES the enclosing value, `_`
    /// disregards, and a term CONSTRAINS. Each is its own alternative, so no
    /// consumer recovers the distinction from a value it was handed.
    pub(crate) fn slot(&mut self, node: cst::Slot<'t>) -> Result<Slot<Unresolved>> {
        Ok(match node {
            // A SCALAR FORMAL IS CODE, NOT DATA, in a slot as in any other
            // value position: the slot CONSTRAINS the position with the
            // value the caller resolved for the formal, and the body's
            // formal frame answers the reference at resolution. Binding it
            // as a fresh column would publish the parameter's own name.
            cst::Slot::NamedReference(reference)
                if self.is_scalar_formal(&self.authored_column(reference.clone())?) =>
            {
                let column = self.authored_column(reference)?;
                Slot::Constraint(Box::new(DomainExpression::Reference(Reference::Named(
                    NamedReference(column),
                ))))
            }
            other => Slot::classify(self.slot_term(other)?),
        })
    }

    /// Whether an unqualified authored name is a scalar formal of the
    /// definition being normalized with bindings in hand.
    pub(crate) fn is_scalar_formal(
        &self,
        column: &crate::pipeline::asts::core::AuthoredColumn,
    ) -> bool {
        column.qualifier.is_none()
            && column.namespace_path.is_empty()
            && self
                .bindings()
                .is_some_and(|bindings| bindings.scalar_formals.contains(column.name.as_str()))
    }

    pub(crate) fn slot_term(
        &mut self,
        node: cst::Slot<'t>,
    ) -> Result<DomainExpression<Unresolved>> {
        match node {
            cst::Slot::NamedReference(reference) => {
                let column = self.authored_column(reference)?;
                Ok(DomainExpression::Reference(Reference::Named(
                    NamedReference(column),
                )))
            }
            cst::Slot::Disregarded(_) => Ok(DomainExpression::Application(
                crate::pipeline::asts::core::FunctionApplication::Open(
                    crate::pipeline::asts::core::DomainHole::Disregarded,
                ),
            )),
            cst::Slot::ConstraintTerm(cst::ConstraintTerm::FunctionApplication(application)) => {
                self.function_application_expression(application)
            }
            cst::Slot::RenamedSlot(renamed) => Err(self.renamed_slot_refusal(renamed)),
        }
    }

    /// THE WRITTEN NAME IS THE NAMING. A slot binds by POSITION — a bare name
    /// binds a fresh column, a qualified one reuses a value, a term constrains
    /// the column and is consumed — so no slot publishes a name for `as` to
    /// change. Renaming is a projection's job, and the teaching says so.
    pub(crate) fn renamed_slot_refusal(&self, node: cst::RenamedSlot<'t>) -> DelightQLError {
        let alias = node
            .alias()
            .map(|name| self.tree.text(name).to_string())
            .unwrap_or_default();
        DelightQLError::validation_error_categorized(
            crate::uri_registry::subcat::CONSTRAINT_POSITIONAL_ALIAS,
            format!(
                "Alias '{alias}' is not allowed in positional binding — a slot binds by \
                 position and publishes no name to rename"
            ),
            "rename in a projection — `f(…) |> (col as name)`",
        )
    }

    pub(crate) fn function_application_expression(
        &mut self,
        node: cst::FunctionApplication<'t>,
    ) -> Result<DomainExpression<Unresolved>> {
        self.domain_expression(cst::DomainExpression::FunctionApplication(node))
    }

    // -----------------------------------------------------------------
    // Anonymous tables
    // -----------------------------------------------------------------

    /// THE ANON HEADER IS A SLOT ROW — the caller-pattern slot law, verbatim.
    fn anon_body(&mut self, node: cst::AnonBody<'t>) -> Result<AnonTable<Unresolved>> {
        // ONE SHAPE FOR EVERY TABULAR INTERIOR. The heading, the sparse
        // marks, the row assembly and the width judgment are the fact body's
        // too; what differs is what a CELL may be, and each body reads its
        // own cells before handing them here.
        let mut rows = Vec::new();
        let (column_headers, sparse) = self.tabular_heading(node.header())?;
        // An anonymous-header binder uses the same position-valid name
        // admission as every other publication: a bare reserved word
        // requires stropping.
        if let Some(row) = &column_headers {
            for item in row.iter() {
                if let Slot::Bind(binder) = &item.slot {
                    self.admit_published(binder.name.clone())?;
                }
            }
        }
        for child in node.children() {
            if let cst::AnonBodyChild::DataRow(row) = child {
                let (positional, fills) = self.row_parts(row)?;
                rows.push((positional, fills, Vec::new()));
            }
        }
        let rows = self.tabular_rows("anonymous table", None, &column_headers, &sparse, rows)?;
        Ok(AnonTable {
            body: TabularBody {
                header: column_headers,
                rows,
            },
        })
    }

    /// The heading a tabular interior declares, and which of its positions
    /// the author marked sparse. Absent where the body wrote none.
    #[allow(clippy::type_complexity)]
    pub(crate) fn tabular_heading(
        &mut self,
        header: Option<cst::HeaderRow<'t>>,
    ) -> Result<(
        Option<TabularRow<HeaderItem<Unresolved>>>,
        Vec<(usize, SqlIdentifier)>,
    )> {
        match header {
            Some(header) => {
                let (headers, sparse) = self.header_parts(header)?;
                Ok((Some(headers), sparse))
            }
            None => Ok((None, Vec::new())),
        }
    }

    /// A TABLE HAS ONE HEADING, whichever tabular interior wrote it.
    ///
    /// The written heading fixes the width; with none, the first row does. A
    /// row of another width has cells that belong to no column, and every
    /// downstream reader would have to decide which — so the table refuses
    /// here instead. `offers` is the fact side's heading offers, judged
    /// AFTER assembly: an arity refusal is the nearer complaint, and a row
    /// that does not fit its heading has no position for an offer to
    /// conflict at.
    #[allow(clippy::type_complexity)]
    pub(crate) fn tabular_rows(
        &mut self,
        subject: &str,
        offers_owner: Option<&str>,
        column_headers: &Option<TabularRow<HeaderItem<Unresolved>>>,
        sparse: &[(usize, SqlIdentifier)],
        rows: Vec<(
            Vec<DomainExpression<Unresolved>>,
            Vec<(SqlIdentifier, DomainExpression<Unresolved>)>,
            Vec<Option<SqlIdentifier>>,
        )>,
    ) -> Result<Vec1<TabularRow<Datum<Unresolved>>>> {
        let width = column_headers.as_ref().map_or(0, TabularRow::len);
        let mut assembled = Vec::with_capacity(rows.len());
        for (index, (positional, fills, offers)) in rows.into_iter().enumerate() {
            let row = tabular_row(positional, fills, sparse, width)?;
            if let (Some(owner), Some(headers)) = (offers_owner, column_headers.as_ref()) {
                super::definitions::offers_agree_with_header(
                    owner, headers, sparse, &offers, index,
                )?;
            }
            assembled.push(row);
        }
        let declared = column_headers.as_ref().map(|_| width);
        if let Some(expected) = declared.or_else(|| assembled.first().map(TabularRow::len)) {
            if let Some(row) = assembled.iter().find(|row| row.len() != expected) {
                return Err(DelightQLError::parse_error_categorized(
                    "anon",
                    format!(
                        "a row of this {subject} carries {} cell(s); {} carries {expected}",
                        row.len(),
                        if declared.is_some() {
                            "its heading"
                        } else {
                            "its first row"
                        }
                    ),
                ));
            }
        }
        Vec1::try_from_vec(assembled).ok_or_else(|| {
            DelightQLError::parse_error_categorized("anon", format!("a {subject} body has no rows"))
        })
    }

    /// A row with no sparse column: every datum is a cell, in order.
    pub(crate) fn data_row(
        &mut self,
        node: cst::DataRow<'t>,
    ) -> Result<TabularRow<Datum<Unresolved>>> {
        let (positional, fills) = self.row_parts(node)?;
        tabular_row(positional, fills, &[], 0)
    }

    /// The heading a tabular interior declares, and which of its positions the
    /// author marked sparse.
    ///
    /// ONE SHAPE FOR EVERY TABULAR INTERIOR: `fact_body` reuses `header_row`,
    /// so a `?` means the same thing in a fact as in an anonymous table and is
    /// read in exactly one place.
    #[allow(clippy::type_complexity)]
    pub(crate) fn header_parts(
        &mut self,
        header: cst::HeaderRow<'t>,
    ) -> Result<(
        TabularRow<HeaderItem<Unresolved>>,
        Vec<(usize, SqlIdentifier)>,
    )> {
        let mut headers = Vec::new();
        let mut sparse: Vec<(usize, SqlIdentifier)> = Vec::new();
        for child in header.children() {
            let cst::HeaderRowChild::HeaderItem(item) = child else {
                continue;
            };
            let mut term = None;
            let mut marked = false;
            for part in item.children() {
                match part {
                    cst::HeaderItemChild::Slot(slot) => term = Some(self.slot_term(slot)?),
                    cst::HeaderItemChild::SparseMark(_) => marked = true,
                }
            }
            let term = self.require(term, "a header item names a column")?;
            if marked {
                let DomainExpression::Reference(Reference::Named(NamedReference(column))) = &term
                else {
                    return Err(DelightQLError::validation_error_categorized(
                        "anon/sparse_header",
                        "a sparse column is filled by name, so the header must be one".to_string(),
                        "drop the `?`, or write the column's name",
                    ));
                };
                sparse.push((headers.len(), column.name.clone()));
            }
            headers.push(HeaderItem {
                slot: Slot::classify(term),
                sparse: marked,
            });
        }
        let headers = Vec1::try_from_vec(headers)
            .map(|row| TabularRow(Box::new(row)))
            .ok_or_else(|| DelightQLError::parse_error("a tabular header names a column"))?;
        // SPARSE COLUMNS FORM A SUFFIX: positional omission is unambiguous
        // only when the omittable columns come last.
        if let Some(&(first_sparse, _)) = sparse.first() {
            if (first_sparse..headers.len()).any(|at| !sparse.iter().any(|(p, _)| *p == at)) {
                return Err(DelightQLError::validation_error_categorized(
                    "anon/sparse_suffix",
                    "a required column cannot follow a sparse column".to_string(),
                    "move the sparse columns to the end of the heading, or mark \
                     the trailing columns sparse too",
                ));
            }
        }
        Ok((headers, sparse))
    }

    /// A row's written cells, split by what they are: values in order, and
    /// fills paired with the column each names.
    #[allow(clippy::type_complexity)]
    fn row_parts(
        &mut self,
        node: cst::DataRow<'t>,
    ) -> Result<(
        Vec<DomainExpression<Unresolved>>,
        Vec<(SqlIdentifier, DomainExpression<Unresolved>)>,
    )> {
        let mut values = Vec::new();
        let mut fills = Vec::new();
        for child in node.children() {
            let cst::DataRowChild::Datum(datum) = child else {
                continue;
            };
            match datum {
                cst::Datum::DomainExpression(expression) => {
                    // A ROW IS A POSITIONAL PREFIX, THEN NAMED FILLS. A
                    // value standing after a fill would leave the written
                    // order no longer saying which position got which value.
                    if !fills.is_empty() {
                        return Err(DelightQLError::validation_error_categorized(
                            "anon/sparse_fill_position",
                            "a sparse fill must follow every positional value in its row"
                                .to_string(),
                            "write the positional values first, then the named fills",
                        ));
                    }
                    values.push(self.domain_expression(expression)?)
                }
                cst::Datum::SparseFill(fill) => fills.extend(self.sparse_fill_parts(fill)?),
            }
        }
        Ok((values, fills))
    }

    /// ONE FILL, ONE CELL PER COLUMN IT NAMES.
    pub(crate) fn sparse_fill_parts(
        &mut self,
        fill: cst::SparseFill<'t>,
    ) -> Result<Vec<(SqlIdentifier, DomainExpression<Unresolved>)>> {
        let columns: Vec<_> = fill.column().collect();
        let supplied: Vec<_> = fill.value().collect();
        if columns.len() != supplied.len() {
            return Err(DelightQLError::validation_error_categorized(
                "anon/sparse_arity",
                format!(
                    "a fill names {} column(s) and supplies {} value(s)",
                    columns.len(),
                    supplied.len()
                ),
                "give every named column exactly one value",
            ));
        }
        let mut fills = Vec::with_capacity(columns.len());
        for (column, ground) in columns.into_iter().zip(supplied) {
            let name = self.identifier(column);
            let value = self.ground(ground)?;
            fills.push((
                name.clone(),
                DomainExpression::Application(
                    crate::pipeline::asts::core::FunctionApplication::Ground(value),
                ),
            ));
        }
        Ok(fills)
    }

    // -----------------------------------------------------------------
    // Continuations
    // -----------------------------------------------------------------

    #[stacksafe::stacksafe]
    pub(crate) fn continuation(
        &mut self,
        node: cst::Continuation<'t>,
        chain: Chain<Unresolved>,
    ) -> Result<Chain<Unresolved>> {
        match node {
            cst::Continuation::OperatorContinuation(operator) => {
                self.operator_continuation(operator, chain)
            }
            cst::Continuation::BinaryContinuation(binary) => {
                self.binary_continuation(binary, chain)
            }
        }
    }

    fn operator_continuation(
        &mut self,
        node: cst::OperatorContinuation<'t>,
        chain: Chain<Unresolved>,
    ) -> Result<Chain<Unresolved>> {
        match node {
            cst::OperatorContinuation::PipeContinuation(pipe) => {
                let mut form = None;
                for child in pipe.children() {
                    match child {
                        cst::PipeContinuationChild::PostPipeForm(node) => form = Some(node),
                        cst::PipeContinuationChild::PipeOperator(_) => {}
                    }
                }
                let form = self.require(form, "a pipe has a right side")?;
                self.post_pipe_form(form, chain)
            }
            cst::OperatorContinuation::PostfixOperator(postfix) => {
                if let Some(step) = postfix_access_step(postfix) {
                    let step = self.access_run_step(step)?;
                    return Ok(apply_access_run(chain, step));
                }
                let step = self.postfix_operator(postfix)?;
                Ok(chain.then(Step::authored(step)))
            }
            // `as f` names a stage's output and removes it from `_`'s deictic
            // domain. On a bare head there is no stage yet, so the name is
            // the mention's alias.
            cst::OperatorContinuation::StageName(stage) => {
                let name = self.require(stage.name(), "a stage name carries a name")?;
                let name = self.identifier(name);
                // Admission, not decoration: the spelling becomes an
                // answering name only through the position law (exact `_`
                // and bare reserved words refuse; a strop stays an exact
                // name), and admission reserves it against the compilation's
                // invented names.
                let name = self.admit_stage(name)?;
                name_the_stage(chain, name)
            }
            // `as f(slots)` names AND patterns: one act at the one occurrence
            // plain `as f` would name.
            cst::OperatorContinuation::ArgumentativeStage(stage) => {
                let name = self.require(stage.name(), "an argumentative stage carries a name")?;
                let name = self.identifier(name);
                let name = self.admit_stage(name)?;
                let slots =
                    self.require(stage.slots(), "an argumentative stage carries a slot row")?;
                let access = self.slot_access(slots)?;
                pattern_the_stage(chain, name, access)
            }
            // THE SINGLETON PIPE — sugar for the zero-key group. ONE road:
            // it builds the same group operator `%( ~> item)` builds, so the
            // two spellings cannot drift apart.
            cst::OperatorContinuation::SingletonReduction(reduction) => {
                let operator = self.singleton_reduction(reduction)?;
                Ok(chain.pipe(operator))
            }
        }
    }

    pub(crate) fn singleton_reduction(
        &mut self,
        node: cst::SingletonReduction<'t>,
    ) -> Result<PipeOp<Unresolved>> {
        use crate::pipeline::asts::core::{GroupSpec, ReductionItem};

        let mut reductions = Vec::new();
        for child in node.children() {
            match child {
                cst::SingletonReductionChild::OutItem(item) => {
                    reductions.push(ReductionItem::Out(self.out_item(item)?))
                }
                cst::SingletonReductionChild::MetadataGroup(group) => {
                    let (group, naming) = self.metadata_group(group)?;
                    reductions.push(ReductionItem::Metadata(
                        crate::pipeline::asts::core::MetadataOut::authored(group, naming),
                    ))
                }
                cst::SingletonReductionChild::ReductionSigil(_) => {}
            }
        }
        Ok(PipeOp::Group(GroupSpec::Reduce {
            keys: Vec::new(),
            reductions: self.require(
                crate::pipeline::asts::vocabulary::Vec1::try_from_vec(reductions),
                "at least one reduction item",
            )?,
            plan: crate::pipeline::asts::core::ReductionPlan::empty(),
        }))
    }

    fn post_pipe_form(
        &mut self,
        node: cst::PostPipeForm<'t>,
        chain: Chain<Unresolved>,
    ) -> Result<Chain<Unresolved>> {
        match node {
            cst::PostPipeForm::PipeOperation(operation) => {
                let operator = self.pipe_operation(operation)?;
                Ok(chain.pipe(operator))
            }
            cst::PostPipeForm::PipeStructural(structural) => {
                let mut chain = chain;
                for step in self.pipe_structural(structural)? {
                    chain = chain.then(Step::authored(step));
                }
                Ok(chain)
            }
            // Substitution, not combination: the piped source becomes the
            // call's final argument, and the landing is SPENT here — piped
            // and direct spellings are indistinguishable afterwards.
            cst::PostPipeForm::PureInvocation(invocation) => {
                self.pure_invocation(invocation, chain)
            }
        }
    }

    /// ONE SUBSTITUTION LAW: the authored arguments bind a complete left
    /// prefix and the flowing operand lands in the FINAL place after them;
    /// a written `@` names an exceptional non-final one. The act is the
    /// landing authority's — this position supplies the relation, not the
    /// place it goes.
    ///
    /// The call HEADS the chain it publishes. A source kept as an operand
    /// beside a call that already holds it is the same relation named twice,
    /// and only whichever consumer collapses the pair first decides which one
    /// counts — a body no collapser walks reaches lowering with a call still standing in operator
    /// position.
    pub(crate) fn pure_invocation(
        &mut self,
        node: cst::PureInvocation<'t>,
        source: Chain<Unresolved>,
    ) -> Result<Chain<Unresolved>> {
        let callee = self.require(node.callee(), "an invocation names a relation")?;
        let reference = self.relation_reference(callee)?;
        let access = self.require(node.access(), "an invocation has an access group")?;
        let (access, shaping) = self.access_of(access)?;

        let mut arguments = match node.ho_part() {
            Some(part) => self.ho_arguments(part)?,
            None => Vec::new(),
        };
        super::landing::land_relation(&mut arguments, source)?;
        let call = crate::pipeline::asts::core::FunctorCall::written(reference, arguments);
        let mut chain = Chain::read(
            Relation::FunctorCall {
                alias: None,
                call: crate::pipeline::asts::core::SealedCall::authored(call),
            },
            access,
        );
        // The group's shaping belongs to what the call PUBLISHES, exactly as
        // it does when the same call is read directly.
        for continuation in shaping {
            chain = self.continuation(continuation, chain)?;
        }
        Ok(chain)
    }

    /// ONE access authority. A call's access group asks the same question a
    /// mention's parens ask, so it is answered in the same place.
    pub(crate) fn access_of(
        &mut self,
        node: cst::Access<'t>,
    ) -> Result<(Access<Unresolved>, Vec<cst::Continuation<'t>>)> {
        match self.require(node.child(), "an access group has an interior")? {
            cst::AccessChild::ArgumentativeForm(form) => Ok((self.slot_access(form)?, Vec::new())),
            cst::AccessChild::Interior(interior) => self.call_group(interior),
        }
    }

    /// A CALL's paren group. A call PUBLISHES a relation, so what the group
    /// asks is the access and what shapes it becomes continuations of the
    /// chain the call heads — there is no second carrier to invent.
    ///
    /// THE IMPLICIT STAR: a shaping interior starts REALISED, so a group whose
    /// leading run named nothing but still shapes is asking for everything.
    pub(crate) fn call_group(
        &mut self,
        interior: cst::Interior<'t>,
    ) -> Result<(Access<Unresolved>, Vec<cst::Continuation<'t>>)> {
        let (access, rest) = self.fold_interior(interior)?;
        let access = match (access, rest.is_empty()) {
            (Access::Unasked, false) => Access::All,
            (access, _) => access,
        };
        Ok((access, rest))
    }

    /// ONE argument of a call group, wherever the group opens.
    pub(crate) fn one_ho_argument(
        &mut self,
        argument: cst::HoArgument<'t>,
    ) -> Result<crate::pipeline::asts::core::operators::HoArgument<Unresolved>> {
        use crate::pipeline::asts::core::operators::HoArgument;
        Ok(match argument {
            cst::HoArgument::ResidualDesignator(designator) => {
                let name =
                    self.require(designator.relation(), "a residual designator names a rule")?;
                let part = self.require(
                    designator.ho_part(),
                    "a configured residual carries its prefix row",
                )?;
                HoArgument::Rule(self.higher_order_read(name, part, Access::Unasked, Vec::new())?)
            }
            // ONE relation carrier among ho_arguments: whether a grelex
            // binds a relation parameter or stands in a scalar slot is
            // judged against the callee's descriptor at resolution, never
            // here.
            cst::HoArgument::Grelex(grelex) => HoArgument::Relation(self.grelex(grelex)?),
            cst::HoArgument::Ground(ground) => HoArgument::Value(
                crate::pipeline::asts::core::ArgumentValue::plain(self.ground_expression(ground)?),
            ),
            // AN ARGUMENT THAT ADDRESSES A COLUMN REACHES AS FAR AS ANY
            // REFERENCE — by name or by position.
            cst::HoArgument::HoArgumentReference(reference) => {
                let reference =
                    self.require(reference.child(), "an argument addresses a column")?;
                HoArgument::Value(crate::pipeline::asts::core::ArgumentValue::plain(
                    self.reference_expression(reference)?,
                ))
            }
            // THE RELATION HOLES ARE ROW STRUCTURE, not values: the landing
            // is the formal a piped relation fills, and the skip is a
            // position the descriptor judges. Neither can stand where a
            // value stands.
            cst::HoArgument::RelationHole(hole) => match hole {
                cst::RelationHole::Landing(_) => {
                    HoArgument::Landing(crate::pipeline::asts::core::AtSign)
                }
                cst::RelationHole::Skipped(_) => HoArgument::Skip,
            },
        })
    }

    pub(crate) fn ho_arguments(
        &mut self,
        node: cst::HoPart<'t>,
    ) -> Result<Vec<crate::pipeline::asts::core::operators::HoArgument<Unresolved>>> {
        use crate::pipeline::asts::core::operators::HoArgument;

        let mut arguments = Vec::new();
        for child in node.children() {
            match child {
                cst::HoPartChild::HoArgument(argument) => {
                    arguments.push(self.one_ho_argument(argument)?)
                }
                // THE LIFT'S COST: `&` bounds arguments and `;` separates
                // lifted rows. Both glyphs are CST-only — the lifted rows
                // dissolve into one anonymous-table argument.
                cst::HoPartChild::LiftSigil(_) | cst::HoPartChild::CommaSigil(_) => {}
            }
        }
        let lift = |rows: Vec<TabularRow<Datum<Unresolved>>>,
                    arguments: &mut Vec<HoArgument<Unresolved>>| {
            if let Some(rows) = Vec1::try_from_vec(rows) {
                arguments.push(HoArgument::Relation(Chain::authored(GroundForm::Literal(
                    AnonRelation::plain(AnonTable {
                        body: TabularBody { header: None, rows },
                    }),
                ))));
            }
        };
        let lifted: Vec<TabularRow<Datum<Unresolved>>> = node
            .lifted()
            .map(|row| self.data_row(row))
            .collect::<Result<_>>()?;
        lift(lifted, &mut arguments);
        // Rows right of `&` after a row-set are the SECOND lifted relation.
        let second: Vec<TabularRow<Datum<Unresolved>>> = node
            .second()
            .map(|row| self.data_row(row))
            .collect::<Result<_>>()?;
        lift(second, &mut arguments);
        Ok(arguments)
    }

    fn binary_continuation(
        &mut self,
        node: cst::BinaryContinuation<'t>,
        chain: Chain<Unresolved>,
    ) -> Result<Chain<Unresolved>> {
        match node {
            cst::BinaryContinuation::CommaContinuation(comma) => {
                let member = self.require(comma.member(), "a comma continuation has a member")?;
                self.comma_member(member, chain)
            }
            cst::BinaryContinuation::UnionLikeContinuation(union) => {
                let (operator, arm) = match union {
                    cst::UnionLikeContinuation::CorrespondingUnionContinuation(node) => (
                        SetOperator::UnionCorresponding,
                        node.children().find_map(|child| match child {
                            cst::CorrespondingUnionContinuationChild::Grelex(arm) => Some(arm),
                            cst::CorrespondingUnionContinuationChild::CorrespondingUnionSigil(
                                _,
                            ) => None,
                        }),
                    ),
                    cst::UnionLikeContinuation::SmartUnionContinuation(node) => (
                        SetOperator::SmartUnionAll,
                        node.children().find_map(|child| match child {
                            cst::SmartUnionContinuationChild::Grelex(arm) => Some(arm),
                            cst::SmartUnionContinuationChild::SmartUnionSigil(_) => None,
                        }),
                    ),
                    cst::UnionLikeContinuation::PositionalUnionContinuation(node) => (
                        SetOperator::UnionAllPositional,
                        node.children().find_map(|child| match child {
                            cst::PositionalUnionContinuationChild::Grelex(arm) => Some(arm),
                            cst::PositionalUnionContinuationChild::PositionalUnionSigil(_) => None,
                        }),
                    ),
                };
                let arm = self.require(arm, "a union has an arm")?;
                let arm = self.grelex(arm)?;
                Ok(chain.bag_op(operator, arm, ()))
            }
            cst::BinaryContinuation::MinusContinuation(minus) => {
                let arm = minus.children().find_map(|child| match child {
                    cst::MinusContinuationChild::Grelex(arm) => Some(arm),
                    cst::MinusContinuationChild::MinusSigil(_) => None,
                });
                let arm = self.require(arm, "a minus has an arm")?;
                let arm = self.grelex(arm)?;
                Ok(chain.bag_op(SetOperator::MinusCorresponding, arm, ()))
            }
            cst::BinaryContinuation::EdgeContinuation(edge) => self.edge(edge, chain),
        }
    }

    fn comma_member(
        &mut self,
        node: cst::CommaContinuationMember<'t>,
        chain: Chain<Unresolved>,
    ) -> Result<Chain<Unresolved>> {
        match node {
            cst::CommaContinuationMember::GrelexLikeMember(
                cst::GrelexLikeMember::ExistsAnonGrelex(probe),
            ) => {
                let mut body = None;
                let mut opener = None;
                for child in probe.children() {
                    match child {
                        cst::ExistsAnonGrelexChild::AnonBody(node) => body = Some(node),
                        cst::ExistsAnonGrelexChild::ExistsAnonOpen(node) => opener = Some(node),
                    }
                }
                let table =
                    self.anon_body(self.require(body, "an anonymous membership has a body")?)?;
                let opener = self.require(opener, "an anonymous membership carries polarity")?;
                let header = table.body.header.ok_or_else(|| {
                    DelightQLError::validation_error_categorized(
                        "resolution/anon/witness_shape",
                        "a witness anonymous table is a membership test and needs headers"
                            .to_string(),
                        "provide a probe and candidate rows, or drop the witness marker",
                    )
                })?;
                let mut probes = header
                    .into_vec()
                    .into_iter()
                    .map(|item| {
                        item.slot.into_term().ok_or_else(|| {
                            DelightQLError::parse_error(
                                "an anonymous membership header has a value",
                            )
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let probe = if probes.len() == 1 {
                    Probe::Value(Box::new(probes.pop().expect("one probe")))
                } else {
                    Probe::Row(Vec2::try_from_vec(probes).ok_or_else(|| {
                        DelightQLError::parse_error("an anonymous membership has a probe")
                    })?)
                };
                let rows = table
                    .body
                    .rows
                    .map(|row| ValueRow((*row.0).map(Datum::into_value)));
                Ok(chain.then(Step::authored(Continuation::Restrict {
                    condition: crate::pipeline::asts::core::TruthExpression::Membership(
                        Membership {
                            probe,
                            negated: self.text(opener).starts_with('\\'),
                            rows,
                            source: crate::pipeline::asts::core::MembershipSource::WitnessAnon,
                        },
                    ),
                    origin: FilterOrigin::UserWritten,
                })))
            }
            cst::CommaContinuationMember::GrelexLikeMember(member) => {
                let outer = matches!(
                    member,
                    cst::GrelexLikeMember::OuterGrelex(_)
                        | cst::GrelexLikeMember::OuterAnonGrelex(_)
                );
                let rhs = self.grelex_like_member(member)?;
                Ok(chain.then(Step::authored(Continuation::Member {
                    rhs,
                    correlation: None,
                    join_type: outer.then_some(JoinType::LeftOuter),
                })))
            }
            // In comma position a truth RESTRICTS the current relation.
            // Existence is a truth like any other here: semi/antijoin is a
            // lowering strategy, never a relational carrier.
            cst::CommaContinuationMember::TruthExpression(truth) => {
                // A whole-heading correlation written at this member's top
                // level is its OWN comma kind. `and` here means what two
                // comma members mean, so a correlation conjoined with a
                // predicate becomes the two continuations it already was.
                let (wholes, condition) = self.comma_truth(truth)?;
                let mut chain = chain;
                for whole in wholes {
                    chain = chain.then(Step::authored(Continuation::Correlate { whole }));
                }
                if let Some(condition) = condition {
                    chain = chain.then(Step::authored(Continuation::Restrict {
                        condition,
                        origin: FilterOrigin::UserWritten,
                    }));
                }
                Ok(chain)
            }
            cst::CommaContinuationMember::DestructureRelex(destructure) => {
                Ok(chain.then(Step::authored(self.destructure(destructure)?)))
            }
            // ORDER IS CONSUMED: the AST stores an Ordering, not the
            // comma-versus-pipe origin it was written with.
            cst::CommaContinuationMember::Ordering(ordering) => {
                let specs = self.ordering_specs(ordering)?;
                Ok(chain.then(Step::authored(Continuation::Structural(
                    crate::pipeline::asts::core::StructuralStep {
                        form: crate::pipeline::asts::core::StructuralForm::Ordering {
                            specs,
                            bound: None,
                        },
                        named: Default::default(),
                    },
                ))))
            }
            // A BOUND CONSUMES THE ORDERING IT STANDS BESIDE: the chain folds
            // it into that ordering's node, and only a bound no ordering
            // precedes stands as a step of its own.
            cst::CommaContinuationMember::RowBound(bound) => {
                Ok(chain.bounding(self.row_bound(bound)?))
            }
        }
    }

    /// A destructure occupies predicate position but is not a predicate: it
    /// EXPANDS. Its pattern is a static heading witness — declared, never
    /// evaluated.
    fn destructure(&mut self, node: cst::DestructureRelex<'t>) -> Result<Continuation<Unresolved>> {
        let source = self.require(node.source(), "a destructure has a source")?;
        let mode = self.require(node.mode(), "a destructure has a mode")?;
        let pattern = self.require(node.pattern(), "a destructure has a pattern")?;
        let iterates = mode
            .children()
            .any(|child| matches!(child, cst::DestructureModeChild::ReductionSigil(_)));
        Ok(Continuation::Destructure {
            source: Box::new(self.domain_expression(source)?),
            pattern: match pattern {
                cst::DestructureRelexPattern::TreePattern(pattern) => self.tree_pattern(pattern)?,
                // A member standing alone IS the pattern.
                cst::DestructureRelexPattern::MetadataBinding(binding) => {
                    TreePattern::Record(RecordPattern {
                        members: Vec1::new(
                            self.pattern_member(cst::PatternMember::MetadataBinding(binding))?,
                        ),
                    })
                }
            },
            mode: if iterates {
                DestructureMode::Aggregate
            } else {
                DestructureMode::Scalar
            },
            schema: (),
        })
    }

    /// `&` holds only DECLARED edges and selects by the term's exact
    /// canonical spelling; `&&` composes edge relations. The context is a
    /// light mention riding on the operator.
    fn edge(
        &mut self,
        node: cst::EdgeContinuation<'t>,
        chain: Chain<Unresolved>,
    ) -> Result<Chain<Unresolved>> {
        let operator = self.require(node.operator(), "an edge has an operator")?;
        let transitive = matches!(
            operator,
            cst::EdgeContinuationOperator::TransitiveEdgeSigil(_)
        );
        let context = match node.context() {
            Some(context) => {
                let symbol = self.require(context.child(), "an edge context is a symbol")?;
                Some(self.text(symbol).trim_start_matches("::").to_string())
            }
            None => None,
        };
        let term = self.require(node.term(), "an edge names a term")?;
        // THE OUTER MARK IS ON THE ACCESS, NOT IN THE TERM: `orders_t?(*)`
        // selects the same declared edge as `orders_t(*)` and keeps every
        // left row. The selection key is therefore the UNMARKED spelling.
        let (term_text, rhs) = match term {
            cst::EdgeContinuationTerm::NamedGrelex(named) => {
                (self.text(named).to_string(), self.named_read(named)?)
            }
            cst::EdgeContinuationTerm::OuterGrelex(outer) => {
                if transitive {
                    return Err(DelightQLError::validation_error_categorized(
                        "grounding/er/transitive_outer",
                        format!(
                            "'{}' marks a composed walk's peer outer; the walk \
                             cannot yet compose an outer step",
                            self.text(outer)
                        ),
                        "mark a direct edge peer, or compose the walk and join \
                         the marked access separately",
                    ));
                }
                let name = self.require(outer.relation(), "an outer access names a relation")?;
                let interior = self.require(outer.interior(), "an outer access has an interior")?;
                let text = format!("{}({})", self.text(name), self.text(interior));
                let read = self.outer_grelex(outer)?;
                if read.has_steps() || !matches!(read.head().form(), GroundForm::Reference(_)) {
                    return Err(DelightQLError::validation_error_categorized(
                        "grounding/er/endpoint",
                        format!("'{text}' shapes its interior, so it names no single term"),
                        "an edge selects by the term's exact canonical spelling",
                    ));
                }
                (text, read)
            }
        };
        // IDENTITY IS THE CANONICAL SPELLING: the selection keys are the
        // terms' canonical bytes, produced by the one canonicalizer. The LEFT
        // key is the chain's own endpoint, which is why the walk carries the
        // authored spelling forward — canonicalization never normalizes
        // semantics, so `people(, 18 <= age)` is a DIFFERENT term from
        // `people(, age >= 18)` and only the bytes can say which was written.
        let right_spelling = crate::term_spec::canonicalize_term(&term_text)?;
        let left_spelling = self.edge_endpoint()?;
        self.last_term = Some(term_text);
        Ok(chain.then(Step::authored(Continuation::ErJoin(ErJoinStep {
            transitive,
            context,
            left_spelling,
            right_spelling,
            rhs,
        }))))
    }

    /// The endpoint of the chain-so-far, as its canonical spelling. A
    /// three-relation walk is two steps whose spellings meet in the middle,
    /// so each step names both of its own ends.
    fn edge_endpoint(&self) -> Result<String> {
        let Some(term) = self.last_term.as_deref() else {
            return Err(DelightQLError::validation_error_categorized(
                "grounding/er/endpoint",
                "an edge operator joins two DECLARED terms; the left side is not one",
                "write the edge between two relation accesses, e.g. `a(*) &(::ctx) b(*)`",
            ));
        };
        crate::term_spec::canonicalize_term(term)
    }

    // -----------------------------------------------------------------
    // Destructure patterns
    // -----------------------------------------------------------------

    /// MIRROR LAW: the pattern grammar mirrors the constructor grammar member
    /// for member. It gets its own carriers because it MEANS the other
    /// direction — these members bind a heading, they do not build a value.
    pub(crate) fn tree_pattern(
        &mut self,
        node: cst::TreePattern<'t>,
    ) -> Result<TreePattern<Unresolved>> {
        match node {
            cst::TreePattern::RecordPattern(record) => {
                let mut members = Vec::new();
                for member in self.pattern_members(record) {
                    members.push(self.pattern_member(member)?);
                }
                Ok(TreePattern::Record(RecordPattern {
                    members: self.require(
                        Vec1::try_from_vec(members),
                        "a record pattern has at least one member",
                    )?,
                }))
            }
            cst::TreePattern::ArrayPattern(array) => {
                let mut members = Vec::new();
                for child in array.children() {
                    match child {
                        cst::ArrayPatternChild::IndexedBinding(binding) => {
                            let index =
                                self.require(binding.index(), "an indexed binding has an index")?;
                            let text = self.text(index);
                            let value = text.parse::<i64>().map_err(|_| {
                                DelightQLError::parse_error(format!("'{text}' is not an index"))
                            })?;
                            let mut steps =
                                vec![crate::pipeline::asts::core::PathStep::Index(value)];
                            if let Some(reach) = binding.reach() {
                                steps.extend(self.path_steps(reach)?);
                            }
                            // A member that reaches PUBLISHES the flattened
                            // spelling of what it reached, as a record's path
                            // binding does; a bare index keeps whatever the
                            // array member was already called.
                            let naming = match (binding.alias(), binding.reach()) {
                                (Some(alias), _) => {
                                    Some(self.admit_published(self.identifier(alias))?)
                                }
                                (None, Some(reach)) => Some(SqlIdentifier::new(format!(
                                    "{text}_{}",
                                    self.flattened_path(reach)?
                                ))),
                                (None, None) => None,
                            };
                            members.push(ArrayPatternMember {
                                path: crate::pipeline::asts::core::Path::try_from_steps(steps)
                                    .expect("an indexed binding opens on its own index"),
                                naming,
                            });
                        }
                        cst::ArrayPatternChild::CommaSigil(_) => {}
                    }
                }
                Ok(TreePattern::Array(ArrayPattern {
                    members: self.require(
                        Vec1::try_from_vec(members),
                        "an array pattern has at least one member",
                    )?,
                }))
            }
        }
    }

    pub(crate) fn pattern_members(
        &self,
        node: cst::RecordPattern<'t>,
    ) -> Vec<cst::PatternMember<'t>> {
        node.children()
            .filter_map(|child| match child {
                cst::RecordPatternChild::PatternMember(member) => Some(member),
                cst::RecordPatternChild::CommaSigil(_) => None,
            })
            .collect()
    }

    fn pattern_member(
        &mut self,
        node: cst::PatternMember<'t>,
    ) -> Result<RecordPatternMember<Unresolved>> {
        match node {
            cst::PatternMember::Binder(binder) => {
                let name = self.require(binder.child(), "a binder is an identifier")?;
                Ok(RecordPatternMember::Binder(self.written_binder(name)))
            }
            // Rename: the key is the JSON key, the identifier is the column
            // it publishes.
            cst::PatternMember::KeyedBinding(binding) => {
                let key = self.require(binding.child(), "a keyed binding has a key")?;
                let name = self.require(binding.name(), "a keyed binding names a column")?;
                Ok(RecordPatternMember::Keyed {
                    key: self.pattern_key(key)?,
                    binder: self.written_binder(name),
                })
            }
            // `"k": {…}` nests; `"k": ~> {…}` iterates. One marker, two
            // cardinalities.
            cst::PatternMember::NestedPattern(nested) => {
                let mut key = None;
                let mut inner: Option<TreePattern<Unresolved>> = None;
                let mut iteration = false;
                for child in nested.children() {
                    match child {
                        cst::NestedPatternChild::Key(node) => key = Some(node),
                        cst::NestedPatternChild::TreePattern(pattern) => {
                            inner = Some(self.tree_pattern(pattern)?)
                        }
                        cst::NestedPatternChild::Iteration(node) => {
                            iteration = true;
                            for part in node.children() {
                                match part {
                                    cst::IterationChild::TreePattern(pattern) => {
                                        inner = Some(self.tree_pattern(pattern)?)
                                    }
                                    // FN.22 (amended): a metadata group may
                                    // stand as an induced member's body —
                                    // `"k": ~> g:~> {…}` is the braced
                                    // nesting `"k": {g:~> {…}}`. The `~>` is
                                    // the induction's own spelling; the
                                    // binding iterates the keyed OBJECT, so
                                    // no array iteration stands between.
                                    cst::IterationChild::MetadataBinding(binding) => {
                                        iteration = false;
                                        inner = Some(TreePattern::Record(RecordPattern {
                                            members: Vec1::new(self.metadata_binding(binding)?),
                                        }))
                                    }
                                    cst::IterationChild::ReductionSigil(_) => {}
                                }
                            }
                        }
                    }
                }
                let key = self.require(key, "a nested pattern has a key")?;
                let inner = self.require(inner, "a nested pattern has a body")?;
                Ok(RecordPatternMember::Nested {
                    key: self.pattern_key(key)?,
                    iteration,
                    pattern: Box::new(inner),
                })
            }
            // Reach without matching. A path binding publishes the
            // underscore-flattened spelling; `as` renames.
            cst::PatternMember::PathBinding(binding) => {
                let mut path = None;
                for child in binding.children() {
                    match child {
                        cst::PathBindingChild::Path(node) => path = Some(node),
                        cst::PathBindingChild::AsKeyword(_) => {}
                    }
                }
                let path = self.require(path, "a path binding has a path")?;
                let naming = match binding.alias() {
                    Some(alias) => Some(self.admit_published(self.identifier(alias))?),
                    None => Some(SqlIdentifier::new(self.flattened_path(path)?)),
                };
                Ok(RecordPatternMember::Path(PathBinding {
                    path: self.path(path)?,
                    naming,
                }))
            }
            // KEYS become column values; `g: ~> _` binds keys and disregards
            // contents.
            cst::PatternMember::MetadataBinding(binding) => self.metadata_binding(binding),
            // Sole-member only, and the grammar is what enforces that: the
            // anaphor iterates the interior binding nothing.
            cst::PatternMember::Disregarded(_) => Ok(RecordPatternMember::Disregarded),
        }
    }

    /// One metadata level of a PATTERN, and the levels under it.
    ///
    /// MIRROR LAW: the construction side chains through `meta_target`, so this
    /// side chains the same way — a nested level is another metadata member,
    /// and an absent target is `g:~> _`, which binds keys and disregards
    /// contents.
    fn metadata_binding(
        &mut self,
        node: cst::MetadataBinding<'t>,
    ) -> Result<RecordPatternMember<Unresolved>> {
        let key_column =
            self.require(node.key_column(), "a metadata binding names its key column")?;
        let key_column = self.require(key_column.child(), "a key column is a reference")?;
        let mut target = None;
        for child in node.children() {
            match child {
                cst::MetadataBindingChild::TreePattern(pattern) => {
                    target = Some(PatternTarget::Pattern(Box::new(
                        self.tree_pattern(pattern)?,
                    )))
                }
                cst::MetadataBindingChild::MetadataBinding(nested) => {
                    target = Some(PatternTarget::Pattern(Box::new(TreePattern::Record(
                        RecordPattern {
                            members: Vec1::new(self.metadata_binding(nested)?),
                        },
                    ))))
                }
                cst::MetadataBindingChild::Disregarded(_) => {
                    target = Some(PatternTarget::Disregarded)
                }
                cst::MetadataBindingChild::MetadataSigil(_) => {}
            }
        }
        let key = self.authored_column(key_column)?;
        Ok(RecordPatternMember::Metadata {
            key: crate::pipeline::asts::core::WrittenBinder {
                name: key.name,
                namespace_path: key.namespace_path,
            },
            target: self.require(target, "a metadata binding has a target")?,
        })
    }

    fn pattern_key(&self, node: cst::Key<'t>) -> Result<String> {
        let string = self.require(node.child(), "a key is a string")?;
        Ok(super::ground::string_interior(self.text(string)).to_string())
    }

    /// A path publishes the underscore-flattened spelling: `.a.b` is `a_b`.
    pub(crate) fn flattened_path(&self, node: cst::Path<'t>) -> Result<String> {
        Ok(self.path_spellings(node)?.join("_"))
    }

    /// The path's steps as SPELLINGS, read through the one path reader so a
    /// name published from a reach cannot drift from the reach itself.
    fn path_spellings(&self, node: cst::Path<'t>) -> Result<Vec<String>> {
        Ok(self
            .path_steps(node)?
            .iter()
            .map(crate::pipeline::asts::core::PathStep::spelling)
            .collect())
    }

    // -----------------------------------------------------------------
    // Let-block bindings
    // -----------------------------------------------------------------

    fn cte(&mut self, node: cst::Cte<'t>) -> Result<LetBinding> {
        Ok(LetBinding::Relation(match node {
            cst::Cte::HoCte(ho) => return self.ho_cte(ho),
            // A query-scoped label is a BARE name, and `body : name` IS
            // `name(*) : body` — one glob head, so the shorthand and a
            // compiler-built binding say the same thing.
            cst::Cte::LabelCte(label) => {
                let body = self.require(label.body(), "a label binds a body")?;
                let name = self.require(label.name(), "a label carries a name")?;
                // `!!` IS EVIDENCE ABOUT THE RELATION: the mark is on the
                // chain the label names, and the terminal that consumes the
                // name finds it exactly where the direct spelling would.
                let expression = match body {
                    cst::LabelCteBody::LetFreeRelex(relex) => self.let_free_relex(relex)?,
                    cst::LabelCteBody::MutationSource(source) => {
                        self.dml_form(cst::DmlForm::MutationSource(source))?
                    }
                };
                self.binding(
                    expression,
                    self.identifier(name),
                    Head::glob(),
                    crate::pipeline::asts::vocabulary::Fixpoint::from_badge(
                        label.child().is_some(),
                    ),
                    crate::pipeline::asts::core::CteEffectDeclaration::Pure,
                )?
            }
            cst::Cte::StandardCte(standard) => {
                let name = self.require(standard.name(), "a binding names its subject")?;
                let name = self.require(name.name(), "a subject has a name")?;
                let head = self.require(standard.head(), "a binding has a head")?;
                let body = self.require(standard.body(), "a binding has a body")?;
                let expression = self.let_free_relex(body)?;
                let Heading { head, fixpoint } = self.heading(head.into())?;
                self.binding(
                    expression,
                    self.identifier(name),
                    head,
                    fixpoint,
                    crate::pipeline::asts::core::CteEffectDeclaration::Pure,
                )?
            }
        }))
    }

    /// `name(params)(head) : body` — one clause of a COMMON HIGHER-ORDER
    /// EXPRESSION. The head is read exactly as the consulted `ho_rule`'s is
    /// (the same `ho_param` and `head_term` readers), and the body is HELD
    /// AS AUTHORED: a parameterized body is normalized at each use, with
    /// that use's bindings in hand, because substitution is a CST-to-AST
    /// judgment — a formal in relation position becomes the supplied
    /// relation, a formal in a bound becomes the supplied integer — and the
    /// bindings are not here yet. The clauses meet at the assembler once the
    /// block has been read.
    fn ho_cte(&mut self, node: cst::HoCte<'t>) -> Result<LetBinding> {
        use crate::pipeline::asts::core::definitions::{HeadItems, HoParam};
        use crate::pipeline::asts::ddl::{DdlBody, DefKind, DefSubject};

        let name = self.require(node.name(), "a binding names its subject")?;
        let name = self.require(name.name(), "a subject has a name")?;
        let body = self.require(node.body(), "a binding has a body")?;
        let name = self.admit_cte(self.identifier(name))?;
        let params: Vec<HoParam> = node
            .children()
            .filter_map(|child| match child {
                cst::HoCteChild::HoParam(param) => Some(param),
                cst::HoCteChild::CommaSigil(_) => None,
            })
            .map(|param| self.ho_param(param))
            .collect::<Result<_>>()?;
        let mut items = Vec::new();
        let mut glob = false;
        for item in node.head() {
            match item {
                cst::HoCteHead::HeadTerm(term) => items.push(self.head_term(term)?),
                cst::HoCteHead::Glob(_) => glob = true,
                cst::HoCteHead::CommaSigil(_) => {}
            }
        }
        let head = Head::higher_order(
            params,
            if glob {
                HeadItems::Glob
            } else {
                HeadItems::Listed(items)
            },
        );
        let decl = self.clause(
            DefKind::HoView,
            DefSubject::Named(name.clone()),
            head,
            crate::pipeline::asts::vocabulary::Fixpoint::Bag,
            DdlBody::Deferred {
                source: self.text(body).to_string(),
            },
            self.text(node),
            None,
        );
        Ok(LetBinding::HigherOrder {
            name,
            effect: crate::pipeline::asts::core::CteEffectDeclaration::Pure,
            decl,
        })
    }

    /// The heading payload's reading. ONE decoder: a rule's head and a
    /// query-scoped binding's head are the same production, so the item list
    /// and the badge are read in one place — and the SUBJECT is not read here
    /// at all, because it stands on the form that owns the heading.
    ///
    /// The badge travels out rather than being acted on: whether the subject
    /// is a fixpoint at all is not knowable here, so the flavor rides the
    /// binding to the one recursion decision (THE BADGE CHOOSES THE UNION).
    pub(crate) fn heading(&mut self, node: HeadingPayload<'t>) -> Result<Heading> {
        match node {
            HeadingPayload::Glob(head) => {
                let badged = head
                    .children()
                    .any(|child| matches!(child, cst::GlobHeadingChild::FixpointBadge(_)));
                Ok(Heading {
                    head: Head::glob(),
                    fixpoint: crate::pipeline::asts::vocabulary::Fixpoint::from_badge(badged),
                })
            }
            HeadingPayload::Argumentative(head) => {
                let mut items = Vec::new();
                let mut badged = false;
                for child in head.children() {
                    match child {
                        cst::ArgumentativeHeadingChild::HeadTerm(term) => {
                            items.push(self.head_term(term)?)
                        }
                        cst::ArgumentativeHeadingChild::FixpointBadge(_) => badged = true,
                        cst::ArgumentativeHeadingChild::CommaSigil(_) => {}
                    }
                }
                Ok(Heading {
                    head: Head::listed(items),
                    fixpoint: crate::pipeline::asts::vocabulary::Fixpoint::from_badge(badged),
                })
            }
        }
    }

    /// A ground term SUPPLIES a constant — SUPPLY IS ELABORATION, one law for
    /// the `:` and `:-` necks. An unlabeled ground term abstains from naming
    /// its position; a label makes it name one.
    pub(crate) fn head_term(
        &mut self,
        node: cst::HeadTerm<'t>,
    ) -> Result<crate::pipeline::asts::core::definitions::HeadItem> {
        use crate::pipeline::asts::core::definitions::{HeadItem, Supply};

        let mut supply = None;
        for child in node.children() {
            match child {
                cst::HeadTermChild::Identifier(name) => {
                    supply = Some(Supply::Ref(self.identifier(name)))
                }
                cst::HeadTermChild::Ground(ground) => {
                    supply = Some(Supply::Ground(self.ground(ground)?))
                }
                cst::HeadTermChild::AsKeyword(_) => {}
            }
        }
        Ok(HeadItem {
            supply: self.require(supply, "a head term supplies a value")?,
            label: node
                .alias()
                .map(|alias| self.admit_published(self.identifier(alias)))
                .transpose()?,
        })
    }

    pub(crate) fn binding(
        &self,
        expression: Chain<Unresolved>,
        name: SqlIdentifier,
        head: Head,
        fixpoint: crate::pipeline::asts::vocabulary::Fixpoint,
        effect: crate::pipeline::asts::core::CteEffectDeclaration,
    ) -> Result<CteBinding<Unresolved>> {
        // A binding name is a naming position: the admission law runs, and
        // the spelling is reserved against the compilation's invented names.
        let name = self.admit_cte(name)?;
        Ok(CteBinding::authored(
            expression,
            crate::pipeline::asts::core::AuthoredCteSubject::Authored { name, effect },
            crate::pipeline::asts::core::CteAuthority {
                horizon: crate::pipeline::asts::core::LexicalHorizon::all(),
                head,
                origin: CteOrigin::UserDefined,
                fixpoint,
            },
        ))
    }

    /// One list is a query-scoped function; two make an HO-CFE, and the
    /// FIRST list holds the curried (function-valued) parameters.
    fn cfe(&mut self, node: cst::Cfe<'t>) -> Result<crate::pipeline::asts::core::CfeDefinition> {
        use crate::pipeline::asts::core::{CfeDefinition, ContextMode};

        let name = self.require(node.name(), "a query function has a name")?;
        let body = self.require(node.body(), "a query function has a body")?;
        let first = node.first_params();
        let second = node.second_params();
        let higher_order = second.is_some();

        let declared =
            |normalizer: &Self, params: Option<cst::CfeParams<'t>>| -> Vec<cst::CfeParam<'t>> {
                let Some(params) = params else {
                    return Vec::new();
                };
                let _ = normalizer;
                params
                    .children()
                    .filter_map(|child| match child {
                        cst::CfeParamsChild::CfeParam(param) => Some(param),
                        cst::CfeParamsChild::CommaSigil(_) => None,
                    })
                    .collect()
            };

        let first_params = declared(self, first);
        let second_params = declared(self, second);

        // The capture mode is declared by the marker's SHAPE: `..` captures
        // implicitly, `..{…}` declares what it captures — including nothing.
        // THE MARKER LEADS, and one signature declares one capture: the same
        // judgments the consulted head makes.
        let mut context_mode = ContextMode::None;
        let positioned = first_params
            .iter()
            .enumerate()
            .map(|(index, param)| (index == 0, param))
            .chain(second_params.iter().map(|param| (false, param)));
        for (leads, param) in positioned {
            if let cst::CfeParam::ContextMarker(marker) = param {
                if context_mode != ContextMode::None {
                    return Err(DelightQLError::validation_error_categorized(
                        "ddl/head/duplicate_context_marker",
                        "a signature declares its capture once — a second context \
                         marker has nothing to add and would silently replace the \
                         first. Keep one marker",
                        "one context capture per signature",
                    ));
                }
                if !leads {
                    return Err(Self::context_marker_position_refusal());
                }
                context_mode = match marker.child() {
                    None => ContextMode::Implicit,
                    Some(capture) => ContextMode::Explicit(
                        capture
                            .children()
                            .filter_map(|child| match child {
                                cst::ContextCaptureChild::Identifier(name) => {
                                    Some(self.identifier(name))
                                }
                                cst::ContextCaptureChild::CommaSigil(_) => None,
                            })
                            .collect(),
                    ),
                };
            }
        }

        let mut callable_names: Vec<SqlIdentifier> = Vec::new();
        let mut scalar_names: Vec<SqlIdentifier> = Vec::new();
        for param in &first_params {
            match param {
                cst::CfeParam::CallableParam(callable) => {
                    let name = self.require(callable.name(), "a callable parameter has a name")?;
                    callable_names.push(self.identifier(name));
                }
                cst::CfeParam::PlainParam(plain) => {
                    let name = self.require(plain.child(), "a parameter has a name")?;
                    if higher_order {
                        callable_names.push(self.identifier(name));
                    } else {
                        scalar_names.push(self.identifier(name));
                    }
                }
                cst::CfeParam::ContextMarker(_) => {}
            }
        }
        for param in &second_params {
            match param {
                cst::CfeParam::CallableParam(callable) => {
                    let name = self.require(callable.name(), "a callable parameter has a name")?;
                    scalar_names.push(self.identifier(name));
                }
                cst::CfeParam::PlainParam(plain) => {
                    let name = self.require(plain.child(), "a parameter has a name")?;
                    scalar_names.push(self.identifier(name));
                }
                cst::CfeParam::ContextMarker(_) => {}
            }
        }
        let formals =
            crate::pipeline::asts::core::CfeFormals::from_role_groups(callable_names, scalar_names);

        // A duplicate formal makes its earlier namesake unreachable — every
        // binding to either lands on one frame slot. Agreement is the
        // identifier law's: an unstropped spelling folds, a stropped one
        // keeps its authored bytes. The declared captures share the frame,
        // so they enter the same judgment.
        let name = self.admit_definition(self.identifier(name))?;
        let mut declared_names: Vec<&SqlIdentifier> =
            formals.iter().map(|formal| &formal.name).collect();
        if let ContextMode::Explicit(captures) = &context_mode {
            declared_names.extend(captures.iter());
        }
        let mut seen: std::collections::HashSet<&SqlIdentifier> = std::collections::HashSet::new();
        for declared in declared_names {
            if !seen.insert(declared) {
                return Err(crate::error::DelightQLError::validation_error_categorized(
                    "cfe/parameter/duplicate",
                    format!(
                        "'{name}' declares '{declared}' twice; every binding to either \
                         occurrence would land on one slot, leaving the other unreachable"
                    ),
                    "give each parameter and declared capture a distinct name",
                ));
            }
        }

        Ok(CfeDefinition::unbounded(
            name,
            formals,
            context_mode,
            self.domain_expression(body)?,
        ))
    }

    // -----------------------------------------------------------------
    // Annotations
    // -----------------------------------------------------------------

    /// THE SET IS CLOSED. Each member has its own carrier and its own
    /// collector; a generic `(~~name …~~)` has no derivation at all.
    ///
    pub(crate) fn annotation(
        &mut self,
        node: cst::Annotation<'t>,
        _anchor: &Chain<Unresolved>,
    ) -> Result<()> {
        match node {
            // Reserved room, recognized so the refusal can teach rather than
            // read as a typo.
            cst::Annotation::ReservedAnnotation(_) => {
                return Err(DelightQLError::validation_error_categorized(
                    "annotation/reserved",
                    "emit annotations are reserved room and do nothing yet",
                    "route rows with a directive: `q |> emit!(\"sink\")(*)` when one exists",
                ))
            }
            cst::Annotation::DefinitionAnnotation(annotation) => {
                self.definition_annotation(annotation)?
            }
        }
        Ok(())
    }

    /// The annotations that need NO relation, and so stand wherever a position
    /// is decorated — including a definition's doc slot, which precedes the
    /// body an assertion would have needed.
    pub(crate) fn definition_annotation(
        &mut self,
        node: cst::DefinitionAnnotation<'t>,
    ) -> Result<()> {
        match node {
            cst::DefinitionAnnotation::DangerAnnotation(danger) => {
                let uri = self.require(danger.uri(), "a danger annotation names a gate")?;
                let uri = crate::pipeline::danger_gates::canonical_danger_uri(&self.uri_path(uri));
                if crate::pipeline::danger_gates::known_danger_hierarchies()
                    .iter()
                    .all(|known| uri != crate::pipeline::danger_gates::canonical_danger_uri(known))
                {
                    return Err(DelightQLError::parse_error_categorized(
                        "danger/unknown",
                        format!(
                            "unknown danger gate '{}'. Known gates: {}",
                            uri.trim_start_matches(
                                crate::pipeline::danger_gates::DANGER_URI_SCHEME
                            ),
                            crate::pipeline::danger_gates::known_danger_hierarchies().join(", ")
                        ),
                    ));
                }
                // A danger gate takes the URI ALONE: acknowledging it beside
                // the query IS the acknowledgment, so there is no state word
                // to read and none to get wrong.
                let spec = DangerSpec {
                    uri,
                    state: DangerState::On,
                };
                self.features().add_danger(spec);
            }
            cst::DefinitionAnnotation::ConfigAnnotation(config) => {
                let uri = self.require(config.uri(), "a config annotation names an option")?;
                let uri = crate::pipeline::option_map::canonical_config_uri(&self.uri_path(uri));
                if crate::pipeline::option_map::known_config_hierarchies()
                    .iter()
                    .all(|known| uri != crate::pipeline::option_map::canonical_config_uri(known))
                {
                    return Err(DelightQLError::parse_error_categorized(
                        "config/unknown",
                        format!(
                            "unknown config option '{}'. Known options: {}",
                            uri,
                            crate::pipeline::option_map::known_config_hierarchies().join(", ")
                        ),
                    ));
                }
                let state = match config.value() {
                    None => OptionState::On,
                    Some(ground) => option_state(self.ground(ground)?)?,
                };
                let spec = OptionSpec { uri, state };
                self.features().add_option(spec);
            }
            cst::DefinitionAnnotation::DdlAnnotation(ddl) => {
                let spec = self.ddl_annotation(ddl)?;
                self.features().add_ddl_block(spec);
            }
            // The error hook DECLARES what the submission expects to fail
            // with. It contributes nothing to the relation — but it is not
            // nothing: the runner compares the refusal against it, so it is
            // collected here rather than re-scanned from raw nodes.
            cst::DefinitionAnnotation::ErrorAnnotation(hook) => {
                let expected = crate::pipeline::verdict::ExpectedError {
                    uri_segments: match hook.uri() {
                        None => Vec::new(),
                        Some(uri) => uri
                            .children()
                            .map(|segment| self.text(segment).to_string())
                            .collect(),
                    },
                };
                if self.pending_error.is_some() {
                    return Err(DelightQLError::parse_error_categorized(
                        "error_hook/repeated",
                        "one goal declares one expected error; this one declares two",
                    ));
                }
                self.pending_error = Some(expected);
            }
        }
        Ok(())
    }

    fn uri_path(&self, node: cst::AnnotationUri<'t>) -> String {
        node.children()
            .map(|segment| self.text(segment))
            .collect::<Vec<_>>()
            .join("/")
    }

    pub(crate) fn ddl_annotation(&mut self, node: cst::DdlAnnotation<'t>) -> Result<InlineDdlSpec> {
        let namespace = node
            .namespace()
            .map(|name| super::ground::string_interior(self.text(name)).to_string());
        // An absent body is an EMPTY body: `(~~ddl ~~)` declares nothing, and
        // the grammar spells that by leaving the field off.
        let body = match node.body() {
            None => crate::pipeline::asts::core::InlineDdlBody::default(),
            // The content is the BLOCK's, not the enclosing form's: a fresh
            // sub-normalizer keeps everything an inner definition declares —
            // features, hooks, hoisted bindings — out of the enclosing
            // submission's sidecars, exactly as a consulted file's interior
            // stays the file's. The tree, arena, and call-site bindings are
            // shared: one submission, one identity arena, one substitution
            // environment.
            Some(content) => {
                let mut inner = match self.bindings() {
                    Some(bindings) => {
                        Normalizer::bound(self.tree, Rc::clone(&self.registry), bindings.clone())
                    }
                    None => Normalizer::new(self.tree, Rc::clone(&self.registry)),
                };
                // A block addressed to a system-owned `_` child carries the
                // companion vocabulary: its subjects are operations, not
                // authored names, and the admission law does not judge them.
                inner.system_child_block = namespace
                    .as_deref()
                    .is_some_and(|child| child.starts_with('_'));
                inner.ddl_content(content)?
            }
        };
        Ok(InlineDdlSpec { body, namespace })
    }

    /// A block's typed body: clauses and nested blocks, in authored order.
    ///
    /// Clauses stay unassembled — agreement is the consultation-time
    /// assembler's judgment. A doc-slot `(~~ddl … ~~)` on an inner definition
    /// reaches the collector like any other definition annotation, and it
    /// belongs to THIS body, so it is drained here rather than left to leak.
    fn ddl_content(
        &mut self,
        content: cst::DdlContent<'t>,
    ) -> Result<crate::pipeline::asts::core::InlineDdlBody> {
        let mut body = crate::pipeline::asts::core::InlineDdlBody::default();
        for child in content.children() {
            match child {
                cst::DdlContentChild::EntityDefinition(definition) => {
                    let clause = self.entity_definition(definition)?;
                    body.definitions.push(clause);
                    body.ddl_blocks.extend(self.features().take_ddl_blocks());
                }
                cst::DdlContentChild::DdlAnnotation(nested) => {
                    let spec = self.ddl_annotation(nested)?;
                    body.ddl_blocks.push(spec);
                }
            }
        }
        Ok(body)
    }
}

/// The leading dequalifying run's steps. Anything else ends the run.
enum AccessStep<'t> {
    Activate,
    Dequalify(cst::Using<'t>),
}

fn access_step(node: cst::Continuation<'_>) -> Option<AccessStep<'_>> {
    match node {
        cst::Continuation::OperatorContinuation(cst::OperatorContinuation::PostfixOperator(
            postfix,
        )) => postfix_access_step(postfix),
        cst::Continuation::OperatorContinuation(_) | cst::Continuation::BinaryContinuation(_) => {
            None
        }
    }
}

fn postfix_access_step(node: cst::PostfixOperator<'_>) -> Option<AccessStep<'_>> {
    match node {
        cst::PostfixOperator::DomainActivate(_) => Some(AccessStep::Activate),
        cst::PostfixOperator::Using(using) => Some(AccessStep::Dequalify(using)),
        cst::PostfixOperator::Meta(_)
        | cst::PostfixOperator::Witness(_)
        | cst::PostfixOperator::SignedWitness(_)
        | cst::PostfixOperator::Drill(_) => None,
    }
}

/// One step of a dequalifying run, read off its node.
///
/// `.()` names no column, so it is the every-shareable-name form; a run's
/// later `.(cols)` extends what the run has already named.
enum AccessRunStep {
    Activate,
    DequalifyAll,
    Dequalify(Vec<SqlIdentifier>),
}

/// The run's step applied to the access carried so far.
///
/// THE ONE FOLD. `users(*.(a))`, `users()*.(a)` and `users() * .(a)` are the
/// same query, so the run inside the parens and the run after them must reach
/// the same access — two folds are two answers waiting to disagree.
fn fold_access(carried: Option<Access<Unresolved>>, step: AccessRunStep) -> Access<Unresolved> {
    match step {
        // ACTIVATION IS AN ACCESS-SITE ACT: over the unasked mention the
        // star is total activation, and over an asked one it re-affirms
        // what the access already asked.
        AccessRunStep::Activate => match carried {
            None | Some(Access::Unasked) => Access::All,
            Some(other) => other,
        },
        AccessRunStep::DequalifyAll => Access::DequalifyAll,
        AccessRunStep::Dequalify(columns) => match carried {
            Some(Access::Dequalify(mut named)) => {
                named.extend(columns);
                Access::Dequalify(named)
            }
            _ => Access::Dequalify(columns),
        },
    }
}

/// Whether an access a mention already carries can absorb a further run step.
///
/// ONE ACCESS PER PARENS. A caller pattern has already said which dimensions
/// the mention asks for, so `users(name, age, _).(name)` is that positional
/// access and then a USING step on its result — folding would overwrite the
/// pattern with the step.
fn absorbs_run(access: &Access<Unresolved>) -> bool {
    match access {
        Access::Unasked | Access::All | Access::Dequalify(_) | Access::DequalifyAll => true,
        Access::Slots(_) => false,
    }
}

/// A dequalifying run step applied where the relation it describes stands.
///
/// `,` puts a mention in member position, and a run written after it is that
/// member's own — `a(*), b() .(c)` is `a(*), b(.(c))`, the same relation asked
/// the same way. Reading the run at the outer chain instead would leave the
/// member unasked and hand the step a heading in which the shared name already
/// occurs twice.
///
/// At the relation the run reaches, the step folds into the mention's access
/// when that mention can still hold one, and is a step on the relation's
/// result otherwise. Both readings are the law's — the first is `users(*.(a))`,
/// the second is the USING step after a caller pattern — and which applies is
/// the access the mention already carries.
#[stacksafe::stacksafe]
fn apply_access_run(mut chain: Chain<Unresolved>, step: AccessRunStep) -> Chain<Unresolved> {
    if let Some(Continuation::Member { rhs, .. }) = chain
        .continuations_mut()
        .last_mut()
        .map(|step| step.form_mut())
    {
        *rhs = apply_access_run(rhs.clone(), step);
        return chain;
    }
    if let Some(access) = bare_access(&mut chain).filter(|access| absorbs_run(access)) {
        let carried = std::mem::replace(access, Access::Unasked);
        *access = fold_access(Some(carried), step);
        return chain;
    }
    // THE SAME ACCESS, one step later. When the mention cannot absorb the run
    // the step stands on the mention's RESULT — but it is the same value of
    // the same type in the same carrier, so one authority answers for both
    // positions and nothing downstream re-derives which spelling was written.
    chain.then(Step::authored(Continuation::Access {
        access: fold_access(None, step),
        named: None,
    }))
}

/// The mention's own access, when the chain is still just that read.
fn bare_access(chain: &mut Chain<Unresolved>) -> Option<&mut Access<Unresolved>> {
    if chain.has_steps()
        || !matches!(
            chain.head().form(),
            GroundForm::Reference(Relation::Ground { .. })
        )
    {
        return None;
    }
    match chain
        .continuations_mut()
        .first_mut()
        .map(|step| step.form_mut())
    {
        Some(Continuation::Access { access, .. }) => Some(access),
        _ => None,
    }
}

/// A written ground read: the mention, and the access its parens asked for
/// standing where every consumer looks for it.
fn ground_read(
    mention: GroundMention,
    access: Access<Unresolved>,
    outer: bool,
) -> Chain<Unresolved> {
    Chain::read(Relation::Ground { mention, outer }, access)
}

/// `a?(*), b?(*)` is FULL outer: both marked. The completing member's own
/// marker is what says so.
fn right_is_outer(chain: &Chain<Unresolved>) -> bool {
    match chain.as_read_relation() {
        Some(Relation::Ground { outer, .. })
        | Some(Relation::InnerRelation { outer, .. })
        | Some(Relation::ConsultedView { outer, .. }) => *outer,
        Some(Relation::FunctorCall { .. }) | None => match chain.head().form() {
            GroundForm::Literal(table) => table.outer,
            GroundForm::Reference(_) => false,
        },
    }
}

/// `as f` names the OUTPUT of the stage it stands after.
///
/// A pipe stage has a name slot of its own; a member's output is the member's
/// own relation, and a bare head's is the mention. The alias REPLACES the
/// anonymous form rather than standing beside it, so a named stage is no
/// longer something the deictic `_` can point at.
///
/// AN ACCESS IS TRANSPARENT TO `as`. `users(*)` and `users()*` are the same
/// query, so `as u` must name the same relation after either spelling: it
/// names what the access asked OF. Reading the access as a namable stage of
/// its own gave the two spellings two different answers — the absorbed one
/// named the mention, the postfix one named a stage.
#[stacksafe::stacksafe]
/// `as f` NAMES THE OCCURRENCE STANDING HERE — the one latest nameable
/// occurrence, whatever produced it: the stage a pipe or structural step
/// published, the relation an access published, the member or arm to the
/// right, the relation an existence probe reads, or the bare head. ONE
/// judgment: the argumentative stage patterns and names the same
/// occurrence through it.
fn name_the_stage(mut chain: Chain<Unresolved>, alias: SqlIdentifier) -> Result<Chain<Unresolved>> {
    // A head's own access IS the read, and the read's name is the
    // mention's: `users(*) as u` names the occurrence `users(*)` reads.
    if chain.head_span() == 1 && chain.continuations().len() == 1 {
        return Ok(alias_head(chain, alias));
    }
    match chain
        .continuations_mut()
        .last_mut()
        .map(|step| step.form_mut())
    {
        // An access past the head's own read publishes an occurrence of its
        // own, and the name is that occurrence's — never the operand's.
        Some(Continuation::Access { named, .. }) | Some(Continuation::Pipe { named, .. }) => {
            *named = Some(alias);
            Ok(chain)
        }
        Some(Continuation::Member { rhs, .. }) => {
            *rhs = name_the_stage(rhs.clone(), alias)?;
            Ok(chain)
        }
        Some(Continuation::BagOp { arm, .. }) => {
            *arm = name_the_stage(arm.clone(), alias)?;
            Ok(chain)
        }
        // An existence probe DOES publish something to name: the relation it
        // probes, which a correlation in the same chain then addresses. The
        // name lands on the relation the probe reads — its head, whatever
        // restrictions the probe states over it.
        Some(Continuation::Restrict {
            condition:
                crate::pipeline::asts::core::TruthExpression::Existence(Existence {
                    relation: subquery,
                    ..
                }),
            ..
        }) => {
            **subquery = alias_head((**subquery).clone(), alias);
            Ok(chain)
        }
        // An edge step publishes its right-hand term, and the alias stands
        // OUTSIDE the term: the selection key is the term's own canonical
        // bytes, so naming it changes nothing about which edge was selected.
        Some(Continuation::ErJoin(step)) => {
            step.rhs = alias_head(step.rhs.clone(), alias);
            Ok(chain)
        }
        // Every other comma member publishes no stage: `as` after one names
        // nothing, and naming nothing refuses rather than being dropped.
        Some(Continuation::Restrict {
            condition:
                crate::pipeline::asts::core::TruthExpression::Membership(Membership {
                    source: crate::pipeline::asts::core::MembershipSource::WitnessAnon,
                    ..
                }),
            ..
        }) => Err(witness_alias_refusal()),
        // A structural form is a run step and publishes a stage exactly as
        // a pipe operator does; its name slot is the same slot.
        Some(Continuation::Structural(step)) => {
            step.named = Some(alias);
            Ok(chain)
        }
        // A restriction, a bound, a correlation or a destructure publishes
        // no stage: a name after one names nothing addressable, so it
        // refuses rather than being dropped.
        Some(
            Continuation::Restrict { .. }
            | Continuation::Correlate { .. }
            | Continuation::Bound { .. }
            | Continuation::Destructure { .. },
        ) => Err(names_no_stage(&alias)),
        None => Ok(alias_head(chain, alias)),
    }
}

/// `as f(slots)` PATTERNS AND NAMES THE OCCURRENCE STANDING HERE in one
/// act, at exactly the occurrence `as f` would name: the slot row is an
/// access step over that occurrence and the name is that step's. Nothing
/// here selects a relation beside the row — the step stands on whatever the
/// chain publishes where it is placed.
fn pattern_the_stage(
    mut chain: Chain<Unresolved>,
    alias: SqlIdentifier,
    access: Access<Unresolved>,
) -> Result<Chain<Unresolved>> {
    match chain
        .continuations_mut()
        .last_mut()
        .map(|step| step.form_mut())
    {
        Some(Continuation::Member { rhs, .. }) => {
            *rhs = pattern_the_stage(rhs.clone(), alias, access)?;
            Ok(chain)
        }
        Some(Continuation::BagOp { arm, .. }) => {
            *arm = pattern_the_stage(arm.clone(), alias, access)?;
            Ok(chain)
        }
        Some(Continuation::Restrict {
            condition:
                crate::pipeline::asts::core::TruthExpression::Existence(Existence {
                    relation: subquery,
                    ..
                }),
            ..
        }) => {
            **subquery = pattern_the_stage((**subquery).clone(), alias, access)?;
            Ok(chain)
        }
        // An edge composes its endpoints by their catalog headings; a slot
        // row over an endpoint would compose a relation the edge does not
        // name. The boundary the edge publishes is the occurrence to pattern.
        Some(Continuation::ErJoin(_)) => Err(DelightQLError::validation_error_categorized(
            "resolution/pipe/no_unnamed_pipe",
            format!(
                "`as {alias}(…)` here patterns an edge endpoint, which is read whole by the edge"
            ),
            "pattern the relation the edge publishes: `… && … as x |> … as u(…)`",
        )),
        Some(Continuation::Restrict {
            condition:
                crate::pipeline::asts::core::TruthExpression::Membership(Membership {
                    source: crate::pipeline::asts::core::MembershipSource::WitnessAnon,
                    ..
                }),
            ..
        }) => Err(witness_alias_refusal()),
        Some(
            Continuation::Restrict { .. }
            | Continuation::Correlate { .. }
            | Continuation::Bound { .. }
            | Continuation::Destructure { .. },
        ) => Err(names_no_stage(&alias)),
        Some(
            Continuation::Access { .. } | Continuation::Pipe { .. } | Continuation::Structural(_),
        )
        | None => Ok(chain.then(Step::authored(Continuation::Access {
            access,
            named: Some(alias),
        }))),
    }
}

fn witness_alias_refusal() -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "resolution/anon/membership_alias",
        "a witness anonymous table (+_ or \\+_) is a membership test and exports no columns",
        "drop the alias; predicates may refer to columns from the outer relation",
    )
}

fn names_no_stage(alias: &SqlIdentifier) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "resolution/pipe/no_unnamed_pipe",
        format!("`as {alias}` here names nothing: this operator publishes no pipe stage"),
        "naming a pipe",
    )
}

/// The alias a bare head carries. A head is not a stage — there is none yet
/// for the name to replace — so the name lands on the mention.
fn alias_head(mut chain: Chain<Unresolved>, alias: SqlIdentifier) -> Chain<Unresolved> {
    match chain.head().clone().into_form() {
        GroundForm::Reference(relation) => {
            *chain.head_mut() =
                Grelex::authored(GroundForm::Reference(alias_relation(relation, alias)))
        }
        GroundForm::Literal(mut occurrence) => {
            occurrence.alias = Some(alias);
            *chain.head_mut() = Grelex::authored(GroundForm::Literal(occurrence));
        }
    }
    chain
}

/// The interior's base read, renamed to the name the mention was given.
///
/// Only the read the parens opened: a relation piped in beside it keeps its
/// own name, so the rename follows the head rather than every mention the
/// interior happens to hold.
fn name_interior_read(subquery: &mut Chain<Unresolved>, alias: &SqlIdentifier) {
    if let GroundForm::Reference(Relation::Ground {
        mention: GroundMention::Named { alias: slot, .. },
        ..
    }) = subquery.head_mut().form_mut()
    {
        if slot.is_none() {
            *slot = Some(alias.clone());
        }
    }
}

fn alias_relation(
    mut relation: Relation<Unresolved>,
    alias: SqlIdentifier,
) -> Relation<Unresolved> {
    match &mut relation {
        // THE NAME IS THE READ'S, whatever the read is. A plan carrier is
        // addressed by IDENTITY and keeps the authored formal its read
        // carries, but an alias written on it still names the scope the body
        // will qualify by — dropping it left `T(*) as t, t.id` addressing a
        // scope nothing opened.
        Relation::Ground { mention, .. } => match mention {
            GroundMention::Named { alias: slot, .. } => *slot = Some(alias),
            GroundMention::Receipt { alias: slot, .. } => *slot = Some(alias),
            // A scratch read is compiler-built after normalization; no
            // authored text carries one, so none is here to alias.
            GroundMention::Scratch { .. } => {
                unreachable!("a scratch read is compiler-built and never aliased in authored text")
            }
            GroundMention::Structural { alias: slot, .. } => *slot = Some(alias),
        },
        // THE NAME IS THE RELATION'S, NOT THE WRAPPER'S. Sneaky parentheses
        // make a derived table out of the mention the interior reads, and
        // that mention is the relation the alias renames — so the interior's
        // own continuations address the CURRENT heading through the new name
        // (`orders(, o.user_id = … ) as o`), exactly as they would through
        // the reference when no alias was written.
        Relation::InnerRelation {
            alias: slot,
            pattern,
            ..
        } => {
            if let InnerRelationPattern::Indeterminate { subquery, .. } = pattern {
                name_interior_read(subquery, &alias);
            }
            *slot = Some(alias);
        }
        // A callable read is named where it stands, exactly as a ground
        // read is. The call it stands on carries no name of its own.
        Relation::FunctorCall { alias: slot, .. } => *slot = Some(alias),
        Relation::ConsultedView { .. } => {}
    }
    relation
}

/// ON, OFF, ALLOW, or a graduated 1–9. The value arrives as a ground term, so
/// the vocabulary is read off the VALUE and never off characters in a slot
/// the grammar left untyped.
fn option_state(value: LiteralValue) -> Result<OptionState> {
    match &value {
        LiteralValue::Symbol(name) | LiteralValue::String(name) => match name.as_str() {
            "ON" | "on" => Ok(OptionState::On),
            "OFF" | "off" => Ok(OptionState::Off),
            "ALLOW" | "allow" => Ok(OptionState::Allow),
            other => Err(DelightQLError::parse_error(format!(
                "invalid option state '{other}'; expected ON, OFF, ALLOW, or 1-9"
            ))),
        },
        LiteralValue::Number(number) => match number.parse::<u8>() {
            Ok(level @ 1..=9) => Ok(OptionState::Severity(level)),
            _ => Err(DelightQLError::parse_error(format!(
                "invalid option level '{number}'; expected 1-9"
            ))),
        },
        LiteralValue::Boolean(true) => Ok(OptionState::On),
        LiteralValue::Boolean(false) => Ok(OptionState::Off),
        LiteralValue::Null | LiteralValue::Mention(_) => Err(DelightQLError::parse_error(format!(
            "invalid option state '{value}'; expected ON, OFF, ALLOW, or 1-9"
        ))),
    }
}

/// One row of a tabular interior, assembled against the heading's sparse
/// positions.
///
/// The positional cells fill the dense positions IN ORDER, each fill lands at
/// the column it NAMES, and an unfilled sparse column is null. The row's width
/// is the heading's, however few cells were written.
///
/// With no sparse position declared there is nothing to address by name, so a
/// fill is refused rather than silently placed. THE ALGORITHM IS ONE: an
/// anonymous table and a fact declare their columns with the same `header_row`
/// and are assembled here, not twice.
pub(crate) fn tabular_row(
    positional: Vec<DomainExpression<Unresolved>>,
    fills: Vec<(SqlIdentifier, DomainExpression<Unresolved>)>,
    sparse: &[(usize, SqlIdentifier)],
    width: usize,
) -> Result<TabularRow<Datum<Unresolved>>> {
    if sparse.is_empty() {
        if let Some((name, _)) = fills.first() {
            return Err(DelightQLError::validation_error_categorized(
                "anon/sparse_fill_position",
                format!("'{name}' is filled where no column is sparse"),
                "mark the column sparse in the header (`{name}?`), or write the value in place",
            ));
        }
        return Vec1::try_from_vec(positional.into_iter().map(Datum::Value).collect())
            .map(|row| TabularRow(Box::new(row)))
            .ok_or_else(|| DelightQLError::parse_error("a tabular row has a datum"));
    }
    let dense = width - sparse.len();
    // A POSITIONAL VALUE MAY FILL A SPARSE COLUMN: optional means the
    // column may be omitted, never that it cannot be supplied. Positions
    // fill left to right — the dense prefix first, then as far into the
    // sparse suffix as the row wrote.
    if positional.len() < dense || positional.len() > width {
        return Err(DelightQLError::validation_error_categorized(
            "anon/sparse_arity",
            format!(
                "a row of this table writes {} positional cell(s); the heading \
                 takes {dense} required and up to {width}",
                positional.len()
            ),
            "every column without `?` is written in every row; positions fill \
             left to right",
        ));
    }
    for (name, _) in &fills {
        if !sparse.iter().any(|(_, column)| column == name) {
            return Err(DelightQLError::validation_error_categorized(
                "anon/sparse_fill_position",
                format!("'{name}' is filled but is not a sparse column of this table"),
                "mark it sparse in the header, or write its value in place",
            ));
        }
    }
    let filled_by_position = positional.len();
    let mut positional = positional.into_iter();
    let mut values = Vec::with_capacity(width);
    for position in 0..width {
        let Some((_, column)) = sparse.iter().find(|(at, _)| *at == position) else {
            values.push(Datum::Value(
                positional
                    .next()
                    .expect("the dense count was checked against the heading"),
            ));
            continue;
        };
        // ONE COLUMN, ONE SUPPLY. A position the row already filled cannot
        // also be filled by name: two values and no choosing rule.
        if position < filled_by_position {
            if fills.iter().any(|(name, _)| name == column) {
                return Err(DelightQLError::validation_error_categorized(
                    "anon/sparse_duplicate",
                    format!(
                        "Duplicate sparse fill for column '{column}': a column filled \
                         twice in one row has two values and no rule for choosing"
                    ),
                    "each sparse column takes at most one fill per row",
                ));
            }
            values.push(Datum::Value(
                positional
                    .next()
                    .expect("the positional count was checked against the width"),
            ));
            continue;
        }
        // TWO FILLS FOR ONE COLUMN CONTRADICT. A first-match reader would drop
        // the later value in silence, so the row refuses instead.
        let mut found = fills.iter().filter(|(name, _)| name == column);
        let filled = found.next();
        if found.next().is_some() {
            return Err(DelightQLError::validation_error_categorized(
                "anon/sparse_duplicate",
                format!(
                    "Duplicate sparse fill for column '{column}': a column filled \
                     twice in one row has two values and no rule for choosing"
                ),
                "each sparse column takes at most one fill per row",
            ));
        }
        let fallback = match filled {
            Some((
                _,
                DomainExpression::Application(
                    crate::pipeline::asts::core::FunctionApplication::Ground(value),
                ),
            )) => value.clone(),
            Some(_) => unreachable!("a sparse fill is admitted only from a ground term"),
            None => crate::pipeline::asts::core::LiteralValue::Null,
        };
        values.push(Datum::SparseFill {
            column: column.clone(),
            fallback,
        });
    }
    Ok(TabularRow(Box::new(
        Vec1::try_from_vec(values).expect("a sparse heading has at least one position"),
    )))
}

/// The slot carrier a caller pattern classifies into. Named here so the
/// classification's home is where paren groups are read.
type _Slot = Slot<Unresolved>;
