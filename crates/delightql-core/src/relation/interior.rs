// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE INTERIOR A RECORD CONSTRUCTION PUBLISHES, attached by the authority
//! that mints the record's ports.
//!
//! A record's members name the interior's columns in written order, and
//! every member states, in the same act that mints its port, which
//! semantic child that port owns: none, the exact child its source port
//! already owned, or the child a literally nested record constructs. The
//! port and its child disposition are one value; no member is minted first
//! and paired with a child later, and no caller outside this authority can
//! hand an owner a body of its choosing.

use super::form::{AnonymousShape, AnonymousSlot, AnonymousSpec, InteriorSpec, RelForm};
use super::{PortId, SemanticBuilder, SemanticRelation};
use crate::diagnostic::Internal;
use crate::error::Result;
use crate::pipeline::ast_resolved;
use crate::pipeline::asts::core::{Enclyph, NamedReference, RecordMember, Reference};

/// WHICH SEMANTIC CHILD A NEWLY MINTED MEMBER OWNS. Closed: every resolved
/// member is classified into exactly one arm, and a present child is an
/// authority-minted relation, never a syntactic shape.
enum Child {
    /// No statically known child: a scalar, a metadata group, a tuple.
    None,
    /// The exact child the member's source port already owns, continued.
    Continue {
        source: PortId,
        child: SemanticRelation,
    },
    /// The child a nested record constructs, already derived.
    Construct(SemanticRelation),
}

/// ONE MEMBER, MINTED: its slot in the record's heading and the child its
/// port owns, decided together.
struct Member {
    slot: AnonymousSlot,
    child: Child,
}

impl SemanticBuilder<'_> {
    /// ATTACH THE INTERIOR HEADING a published value owns.
    ///
    /// A record's members name the interior's columns, in written order,
    /// each carrying the child its own port owns. A tuple collects by
    /// position and names nothing, so it publishes a nested payload with
    /// no heading to attach. A published value that is not a construction
    /// has neither. Answers whether an interior was attached.
    pub(crate) fn attach_record_interior(
        &self,
        owner: PortId,
        expression: &ast_resolved::DomainExpression,
    ) -> Result<bool> {
        let record = match expression {
            ast_resolved::DomainExpression::Application(
                ast_resolved::FunctionApplication::Enclyph(Enclyph::Record(record)),
            ) => record,
            ast_resolved::DomainExpression::Application(
                ast_resolved::FunctionApplication::Enclyph(Enclyph::Tuple(_)),
            ) => {
                self.names().mark_nested_payload(owner.column());
                return Ok(true);
            }
            _ => return Ok(false),
        };
        let body = self.record_relation(record)?;
        self.derive(RelForm::Interior(InteriorSpec { owner, body }))?;
        Ok(true)
    }

    /// THE RELATION A RECORD CONSTRUCTS: one exhaustive judgment over its
    /// resolved members mints the heading and every member's child
    /// disposition as one value; the ports are then derived and each child
    /// attached to the port its member minted.
    fn record_relation(&self, record: &ast_resolved::Record) -> Result<SemanticRelation> {
        let mut members = Vec::with_capacity(record.members.len());
        for (position, member) in record.members.iter().enumerate() {
            let (published, child) = match member {
                // A reference donates its name and CONTINUES its port: the
                // child that port owns is the child this member owns.
                RecordMember::SelfKeyed(NamedReference(occurrence)) => (
                    self.names().published(occurrence.column.column()),
                    self.continued_child(occurrence.column)?,
                ),
                // A key over a bare reference renames the position; the
                // child is the source port's. A key over any other value
                // publishes a value with no static child.
                RecordMember::Keyed { key, value } => (
                    Some(self.names().intern(key, false)),
                    match value.as_ref() {
                        ast_resolved::DomainExpression::Reference(Reference::Named(
                            NamedReference(occurrence),
                        )) => self.continued_child(occurrence.column)?,
                        _ => Child::None,
                    },
                ),
                // A metadata group yields a record keyed by data: a nested
                // payload whose heading is not static.
                RecordMember::Metadata { key, .. } => {
                    (Some(self.names().intern(key, false)), Child::None)
                }
                RecordMember::Induced { key, value } => (
                    Some(self.names().intern(key, false)),
                    match value.as_ref() {
                        // The nested level is constructed FIRST, so the
                        // member carries a derived relation, never syntax.
                        Enclyph::Record(nested) => Child::Construct(self.record_relation(nested)?),
                        // A tuple publishes by position and names nothing,
                        // so it contributes no interior heading.
                        Enclyph::EmptyRecord(_) | Enclyph::Tuple(_) => Child::None,
                    },
                ),
                RecordMember::Spread(spread) => spread.expanded(),
            };
            members.push(Member {
                slot: AnonymousSlot::Declared {
                    position: position as u32,
                    named: published,
                },
                child,
            });
        }
        let slots: Vec<AnonymousSlot> = members.iter().map(|member| member.slot.clone()).collect();
        let relation = self.derive(RelForm::Anonymous(AnonymousSpec::plain(
            AnonymousShape::Tabular,
            &slots,
            None,
        )))?;
        let ports = self.interface(&relation)?.ports().to_vec();
        if ports.len() != members.len() {
            return Err(Internal::invariant(
                "record interior",
                "a record's heading publishes one port per member",
            ));
        }
        for (owner, member) in ports.into_iter().zip(members) {
            match member.child {
                Child::None => {}
                Child::Continue { source, child } => self.continue_child(owner, source, child)?,
                Child::Construct(body) => {
                    self.derive(RelForm::Interior(InteriorSpec { owner, body }))?;
                }
            }
        }
        Ok(relation)
    }

    /// The child a member continues from its source port: the port's own
    /// recorded interior, by exact identity.
    fn continued_child(&self, source: PortId) -> Result<Child> {
        Ok(match super::interior(self.names(), source)? {
            Some(child) => Child::Continue { source, child },
            None => Child::None,
        })
    }

    /// THE CHILD CONTINUES onto the port the record minted for its member,
    /// exactly as a carried position continues its source's interior — the
    /// same relation, and the source's conflict mark with it. The minted
    /// port owns nothing yet; owning one already would mean two acts
    /// attached to one port.
    fn continue_child(&self, owner: PortId, source: PortId, child: SemanticRelation) -> Result<()> {
        if super::interior(self.names(), owner)?.is_some() {
            return Err(Internal::invariant(
                "record interior",
                "a freshly minted record member already owns an interior",
            ));
        }
        self.names().relations().record_interior(owner, child);
        if self.names().relations().interior_conflict(source) {
            self.names().relations().record_interior_conflict(owner);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! THE STORE-LEVEL WITNESS: the fresh outer member owns its child the
    //! moment the record is constructed, before any drill asks for it. A
    //! row that never drills cannot see this; the store can.

    use super::*;
    use crate::pipeline::asts::core::{ColumnOccurrence, DomainExpression, FunctionApplication};
    use crate::pipeline::asts::vocabulary::Vec1;
    use crate::relation::Planning;

    fn declared(authority: &SemanticBuilder<'_>, names: &[&str]) -> SemanticRelation {
        let slots: Vec<AnonymousSlot> = names
            .iter()
            .enumerate()
            .map(|(position, name)| AnonymousSlot::Declared {
                position: position as u32,
                named: Some(authority.names().intern(name, false)),
            })
            .collect();
        authority
            .derive(RelForm::Anonymous(AnonymousSpec::plain(
                AnonymousShape::Tabular,
                &slots,
                None,
            )))
            .expect("an anonymous relation derives")
    }

    fn port(authority: &SemanticBuilder<'_>, relation: &SemanticRelation, index: usize) -> PortId {
        authority.interface(relation).expect("an interface").ports()[index]
    }

    fn self_keyed(port: PortId) -> RecordMember<crate::pipeline::asts::core::Resolved> {
        RecordMember::SelfKeyed(NamedReference(ColumnOccurrence::engine(port)))
    }

    fn record(
        members: Vec<RecordMember<crate::pipeline::asts::core::Resolved>>,
    ) -> DomainExpression<crate::pipeline::asts::core::Resolved> {
        DomainExpression::Application(FunctionApplication::Enclyph(Enclyph::Record(
            ast_resolved::Record::plain(Vec1::try_from_vec(members).expect("members")),
        )))
    }

    /// `%(a, b ~> {c} as inner)` then `%(a ~> {b, inner} as outer)`: the
    /// staged spelling. The outer record's `inner` member is a reference
    /// to a port that already owns `(c)`; the member the record mints
    /// owns that exact child before anything drills.
    #[test]
    fn a_referenced_member_continues_its_source_ports_child_at_construction() {
        let planning = Planning::open(crate::names::Registry::new(&[]));
        let authority = planning.authority();
        // The first stage: (a, b, inner) where inner owns (c).
        let stage = declared(&authority, &["a", "b", "inner"]);
        let inner_body = declared(&authority, &["c"]);
        let stage_inner = port(&authority, &stage, 2);
        authority
            .derive(RelForm::Interior(InteriorSpec {
                owner: stage_inner,
                body: inner_body,
            }))
            .expect("the first stage's interior attaches");
        // The second stage publishes (a, outer); outer's value is the
        // record {b, inner} over the first stage's ports.
        let second = declared(&authority, &["a", "outer"]);
        let outer = port(&authority, &second, 1);
        let attached = authority
            .attach_record_interior(
                outer,
                &record(vec![
                    self_keyed(port(&authority, &stage, 1)),
                    self_keyed(stage_inner),
                ]),
            )
            .expect("the record's interior attaches");
        assert!(attached);

        let outer_body = super::super::interior(authority.names(), outer)
            .expect("read")
            .expect("outer owns its record's relation");
        let outer_ports = authority
            .interface(&outer_body)
            .expect("interface")
            .ports()
            .to_vec();
        assert_eq!(outer_ports.len(), 2, "the record publishes b and inner");
        let fresh_inner = outer_ports[1];
        assert_ne!(
            fresh_inner, stage_inner,
            "the record mints a fresh port for its member"
        );
        let source_child = super::super::interior(authority.names(), stage_inner)
            .expect("read")
            .expect("the source port owns its interior");
        assert_eq!(
            super::super::interior(authority.names(), fresh_inner).expect("read"),
            Some(source_child),
            "the fresh member owns the exact child its source port owned"
        );
        assert_eq!(
            super::super::interior(authority.names(), outer_ports[0]).expect("read"),
            None,
            "a scalar member owns no child"
        );
    }

    /// `{"renamed": inner}`: an explicit key over a bare reference renames
    /// the position and continues the same child.
    #[test]
    fn a_keyed_reference_member_continues_its_child_under_the_new_name() {
        let planning = Planning::open(crate::names::Registry::new(&[]));
        let authority = planning.authority();
        let stage = declared(&authority, &["inner"]);
        let inner_body = declared(&authority, &["c"]);
        let stage_inner = port(&authority, &stage, 0);
        authority
            .derive(RelForm::Interior(InteriorSpec {
                owner: stage_inner,
                body: inner_body,
            }))
            .expect("attaches");
        let second = declared(&authority, &["outer"]);
        let outer = port(&authority, &second, 0);
        authority
            .attach_record_interior(
                outer,
                &record(vec![RecordMember::Keyed {
                    key: "renamed".to_string(),
                    value: Box::new(DomainExpression::Reference(Reference::Named(
                        NamedReference(ColumnOccurrence::engine(stage_inner)),
                    ))),
                }]),
            )
            .expect("attaches");
        let outer_body = super::super::interior(authority.names(), outer)
            .expect("read")
            .expect("owns");
        let renamed = port(&authority, &outer_body, 0);
        assert_eq!(
            authority.names().published_sym(renamed.column()),
            Some(
                authority
                    .names()
                    .canonical(authority.names().intern("renamed", false))
            )
        );
        assert_eq!(
            super::super::interior(authority.names(), renamed).expect("read"),
            super::super::interior(authority.names(), stage_inner).expect("read"),
            "the renamed member owns the exact child its source port owned"
        );
    }

    /// A nested record constructs its child; a reference to a port with no
    /// interior owns none. Depth is ordinary recursion, not a case.
    #[test]
    fn a_nested_record_constructs_its_child_and_a_plain_reference_owns_none() {
        let planning = Planning::open(crate::names::Registry::new(&[]));
        let authority = planning.authority();
        let stage = declared(&authority, &["b", "c"]);
        let second = declared(&authority, &["outer"]);
        let outer = port(&authority, &second, 0);
        let nested = ast_resolved::Record::plain(
            Vec1::try_from_vec(vec![self_keyed(port(&authority, &stage, 1))]).expect("members"),
        );
        authority
            .attach_record_interior(
                outer,
                &record(vec![
                    self_keyed(port(&authority, &stage, 0)),
                    RecordMember::Induced {
                        key: "level2".to_string(),
                        value: Box::new(Enclyph::Record(nested)),
                    },
                ]),
            )
            .expect("attaches");
        let outer_body = super::super::interior(authority.names(), outer)
            .expect("read")
            .expect("owns");
        let ports = authority
            .interface(&outer_body)
            .expect("interface")
            .ports()
            .to_vec();
        assert_eq!(
            super::super::interior(authority.names(), ports[0]).expect("read"),
            None
        );
        let level2 = super::super::interior(authority.names(), ports[1])
            .expect("read")
            .expect("the induced member owns the constructed child");
        let level2_ports = authority
            .interface(&level2)
            .expect("interface")
            .ports()
            .to_vec();
        assert_eq!(level2_ports.len(), 1);
        assert_eq!(
            authority.names().published_sym(level2_ports[0].column()),
            Some(
                authority
                    .names()
                    .canonical(authority.names().intern("c", false))
            )
        );
    }
}
