// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE RESOLVED EDGE BODY, under its endpoint roles. The text's judgment
//! (`shape`) admits a body; here the body has resolved alone in its own
//! world, and every column a condition reads is placed under one of the
//! two endpoints: a read's own port, or a position the body publishes
//! over that read (a bare column resolves to the body's published
//! position, never to the read's). Every fact the resolved body carries
//! is accounted for — its restrictions, and whatever relates its member
//! to its head — so that nothing a single edge honors is lost when a
//! chain rebuilds from these parts. On those roles the two facts the text
//! cannot settle are judged, the same way for every edge consumer — a
//! single edge, a run, a walk: every condition is simple AS RESOLVED, a
//! named function standing for its body, and a connection stands between
//! the two endpoints. The judged conditions are what a chain rebinds onto
//! its own read of the same terms, position for position.

use std::collections::HashMap;

use crate::diagnostic::Internal;
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_transform::{
    same_phase_payload_folds, walk_transform_reference, AstTransform,
};
use crate::pipeline::ast_visit::{walk_visit_boolean, walk_visit_domain, AstVisit, Descent};
use crate::pipeline::asts::core::{FilterOrigin, NamedReference, Reference, Resolved};
use crate::relation::{published_ports, Planning, PortId, SemanticRelation};

use super::shape::{self, LeftAt};

/// Which endpoint a column belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Endpoint {
    Left,
    Right,
}

/// Every port a resolved body's conditions may read, under its endpoint.
struct Roles {
    left: Vec<PortId>,
    right: Vec<PortId>,
    /// A position the body publishes over one read: the port, and the
    /// read port it descends from.
    published: Vec<(PortId, PortId)>,
}

impl Roles {
    /// The read port a column stands for: itself, or the read position
    /// a published position descends from.
    fn read_of(&self, port: PortId) -> PortId {
        self.published
            .iter()
            .find(|(published, _)| *published == port)
            .map_or(port, |(_, source)| *source)
    }

    fn endpoint_of(&self, port: PortId) -> Option<Endpoint> {
        let read = self.read_of(port);
        if self.left.contains(&read) {
            Some(Endpoint::Left)
        } else if self.right.contains(&read) {
            Some(Endpoint::Right)
        } else {
            None
        }
    }
}

/// A simple body, resolved alone, taken apart and judged: its roles and
/// its resolved conditions.
pub(in crate::defuse) struct ResolvedBody {
    roles: Roles,
    conditions: Vec<ast_resolved::TruthExpression>,
}

fn invariant(what: &str) -> DelightQLError {
    Internal::invariant("er composition", what)
}

impl ResolvedBody {
    /// Take a resolved simple body apart — its two reads and its
    /// restrictions, in authored order; the text said which read is the
    /// left term's — and judge what only the resolved body shows: that
    /// every condition is simple as resolved, and that a connection
    /// stands between the two endpoints.
    pub(in crate::defuse) fn of(
        chain: &ast_resolved::Chain,
        body: &SemanticRelation,
        left_at: LeftAt,
        pair: &str,
        identities: &Planning,
    ) -> Result<ResolvedBody> {
        let head_ports = published_ports(identities, chain.head().result())?;
        let mut member = None;
        let mut conditions = Vec::new();
        for step in chain.steps() {
            match step.form() {
                ast_resolved::Continuation::Member {
                    rhs, correlation, ..
                } => {
                    if member.is_some() {
                        return Err(invariant(
                            "a simple edge body resolves to exactly two reads",
                        ));
                    }
                    member = Some(published_ports(identities, &rhs.semantic_relation())?);
                    // THE MEMBER'S OWN RELATIONSHIP is accounted for, never
                    // dropped: a decided crossing is the simple shape; a
                    // condition stated on the member is a condition of the
                    // body; a correspondence synthesized from the two reads
                    // directs the join and publishes one column for two,
                    // which is not the pair schema.
                    match correlation {
                        ast_resolved::MemberCorrelation::Cartesian(_) => {}
                        ast_resolved::MemberCorrelation::Condition(condition) => {
                            conditions.push(condition.clone())
                        }
                        ast_resolved::MemberCorrelation::Correspond(_) => {
                            return Err(shape::refuse(
                                pair,
                                "reads its two endpoints in correspondence by position — a \
                                 correspondence directs the join and publishes one column \
                                 for two, which is not the pair schema; bind the positions \
                                 to distinct names and spell the equality"
                                    .to_string(),
                            ))
                        }
                    }
                }
                ast_resolved::Continuation::Restrict { condition, origin } => match origin {
                    FilterOrigin::UserWritten => conditions.push(condition.clone()),
                    // A READ'S OWN RESTRICTION — a name the read binds
                    // twice, a slot's constraint — is applied where the
                    // read's slots are bound, over the read's own ports,
                    // and is reproduced wherever the term is read again.
                    // It is the read's, not the body's; a chain's scan of
                    // the term carries it already.
                    FilterOrigin::PositionalLiteral { .. } => {}
                    FilterOrigin::Generated | FilterOrigin::HoGroundScalar => {
                        return Err(invariant(
                            "a simple edge body's restrictions are authored or its reads' own",
                        ))
                    }
                },
                _ => {
                    return Err(invariant(
                        "a simple edge body resolves to reads and restrictions only",
                    ))
                }
            }
        }
        let member_ports =
            member.ok_or_else(|| invariant("a simple edge body resolves to two reads"))?;
        let (left, right) = match left_at {
            LeftAt::Head => (head_ports, member_ports),
            LeftAt::Member => (member_ports, head_ports),
        };
        let authority = identities.authority();
        let mut published = Vec::new();
        for port in published_ports(identities, body)? {
            let ancestors = authority.ancestors_into(body, port)?;
            if let Some(source) = ancestors
                .iter()
                .copied()
                .find(|ancestor| left.contains(ancestor) || right.contains(ancestor))
            {
                published.push((port, source));
            }
        }
        let roles = Roles {
            left,
            right,
            published,
        };
        for condition in &conditions {
            simple(condition, pair)?;
        }
        judge_connection(&conditions, &roles, pair)?;
        Ok(ResolvedBody { roles, conditions })
    }

    /// The conditions rebound onto another read of the same two terms —
    /// the body's read ports each onto the given read's, position for
    /// position (the same read, resolved in the same world), and the
    /// body's published positions with them. Total over a judged body:
    /// every reference is a column of one of the two reads.
    pub(in crate::defuse) fn rebound_onto(
        self,
        left_read: &[PortId],
        right_read: &[PortId],
    ) -> Result<Vec<ast_resolved::TruthExpression>> {
        let ResolvedBody { roles, conditions } = self;
        if roles.left.len() != left_read.len() || roles.right.len() != right_read.len() {
            return Err(invariant(
                "a body's read of a term and the chain's read of it publish one heading",
            ));
        }
        let onto: HashMap<PortId, PortId> = roles
            .left
            .iter()
            .copied()
            .zip(left_read.iter().copied())
            .chain(roles.right.iter().copied().zip(right_read.iter().copied()))
            .collect();
        struct Rebind<'a> {
            roles: &'a Roles,
            onto: &'a HashMap<PortId, PortId>,
        }
        impl AstTransform<Resolved, Resolved> for Rebind<'_> {
            same_phase_payload_folds!(Resolved);
            fn transform_reference(
                &mut self,
                r: Reference<Resolved>,
            ) -> Result<Reference<Resolved>> {
                match r {
                    Reference::Named(NamedReference(occurrence)) => {
                        let column = *self
                            .onto
                            .get(&self.roles.read_of(occurrence.column))
                            .ok_or_else(|| {
                                invariant("a judged condition references only the body's two reads")
                            })?;
                        Ok(Reference::Named(NamedReference(occurrence.rebound(column))))
                    }
                    other => walk_transform_reference(self, other),
                }
            }
        }
        let mut rebind = Rebind {
            roles: &roles,
            onto: &onto,
        };
        conditions
            .into_iter()
            .map(|condition| rebind.transform_boolean(condition))
            .collect()
    }
}

/// A resolved condition reaches no relation. The text was judged as
/// written; the resolved condition is the fact — a named function stands
/// for its body, and a body that probes a relation is that probe. A truth
/// crossed into a value is judged as the truth it is.
fn simple(condition: &ast_resolved::TruthExpression, pair: &str) -> Result<()> {
    struct Simple<'a> {
        pair: &'a str,
        refusal: Option<DelightQLError>,
    }
    impl Simple<'_> {
        fn stop(&mut self, what: &str) -> Result<Descent> {
            self.refusal = Some(shape::refuse(
                self.pair,
                format!(
                    "carries {what} in a condition as it resolves (a named function stands \
                     for its body)"
                ),
            ));
            Ok(Descent::Break)
        }
    }
    impl AstVisit<Resolved> for Simple<'_> {
        fn enter_relational(&mut self, _: &ast_resolved::Chain) -> Result<Descent> {
            self.stop("a relation")
        }
        fn enter_boolean(&mut self, e: &ast_resolved::TruthExpression) -> Result<Descent> {
            match e {
                ast_resolved::TruthExpression::Existence(_) => self.stop("an existence probe"),
                ast_resolved::TruthExpression::Membership(_)
                | ast_resolved::TruthExpression::RelationalMembership(_) => {
                    self.stop("a membership test")
                }
                ast_resolved::TruthExpression::Sigma(_) => self.stop("a sigma predicate"),
                ast_resolved::TruthExpression::Comparison(_)
                | ast_resolved::TruthExpression::Conjunction(_)
                | ast_resolved::TruthExpression::Disjunction(_)
                | ast_resolved::TruthExpression::Not { .. } => Ok(Descent::Continue),
            }
        }
        fn enter_function(&mut self, f: &ast_resolved::FunctionApplication) -> Result<Descent> {
            match f {
                ast_resolved::FunctionApplication::Standard(application)
                    if application.window.is_some() =>
                {
                    self.stop("a window")
                }
                ast_resolved::FunctionApplication::Scalarized(_) => self.stop("a subquery"),
                ast_resolved::FunctionApplication::Enclyph(_) => self.stop("a constructed value"),
                ast_resolved::FunctionApplication::Crossed(_)
                | ast_resolved::FunctionApplication::Ground(_)
                | ast_resolved::FunctionApplication::Open(_)
                | ast_resolved::FunctionApplication::Standard(_)
                | ast_resolved::FunctionApplication::Infix(_)
                | ast_resolved::FunctionApplication::Template(_)
                | ast_resolved::FunctionApplication::Case(_)
                | ast_resolved::FunctionApplication::FieldSelect(_)
                | ast_resolved::FunctionApplication::ClauseSelection(_)
                | ast_resolved::FunctionApplication::JsonAccess(_) => Ok(Descent::Continue),
            }
        }
    }
    let mut visit = Simple {
        pair,
        refusal: None,
    };
    walk_visit_boolean(&mut visit, condition)?;
    match visit.refusal {
        Some(refusal) => Err(refusal),
        None => Ok(()),
    }
}

/// THE CONNECTION, confirmed on the roles: some top-level conjunct is a
/// comparison whose sides read the two endpoints, one each.
fn judge_connection(
    conditions: &[ast_resolved::TruthExpression],
    roles: &Roles,
    pair: &str,
) -> Result<()> {
    use ast_resolved::TruthExpression;
    /// The one endpoint every column of a side belongs to.
    fn side(expr: &ast_resolved::DomainExpression, roles: &Roles) -> Option<Endpoint> {
        let mut endpoints = column_ports(expr)
            .into_iter()
            .map(|port| roles.endpoint_of(port));
        let first = endpoints.next()??;
        endpoints
            .all(|endpoint| endpoint == Some(first))
            .then_some(first)
    }
    fn conjuncts<'a>(expr: &'a TruthExpression, out: &mut Vec<&'a TruthExpression>) {
        match expr {
            TruthExpression::Conjunction(parts) => {
                for part in parts.iter() {
                    conjuncts(part, out);
                }
            }
            other => out.push(other),
        }
    }
    let mut top = Vec::new();
    for condition in conditions {
        conjuncts(condition, &mut top);
    }
    let connected = top.iter().any(|conjunct| match conjunct {
        TruthExpression::Comparison(comparison) => matches!(
            (
                side(&comparison.left, roles),
                side(&comparison.right, roles)
            ),
            (Some(Endpoint::Left), Some(Endpoint::Right))
                | (Some(Endpoint::Right), Some(Endpoint::Left))
        ),
        _ => false,
    });
    if connected {
        Ok(())
    } else {
        Err(shape::no_connection(pair))
    }
}

/// The column occurrences a resolved expression reads.
struct Ports(Vec<PortId>);

impl AstVisit<Resolved> for Ports {
    fn enter_domain(&mut self, e: &ast_resolved::DomainExpression) -> Result<Descent> {
        if let ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            occurrence,
        ))) = e
        {
            self.0.push(occurrence.column);
        }
        Ok(Descent::Continue)
    }
}

fn column_ports(expr: &ast_resolved::DomainExpression) -> Vec<PortId> {
    let mut ports = Ports(Vec::new());
    let _ = walk_visit_domain(&mut ports, expr);
    ports.0
}
