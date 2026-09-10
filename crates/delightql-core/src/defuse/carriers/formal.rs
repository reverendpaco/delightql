// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE RELATION FORMALS ONE USE BOUND — a relation and the interface its
//! receiving formal appointed, as ONE value.
//!
//! A relation formal is bound by the carrier authority alone
//! ([`super::bind_relation_formals`]): the actual is resolved in its lawful
//! world, the formal's face is applied to it — an open face preserves the
//! actual's interface, an appointed face spends the declared positional
//! pattern — and only then is the carrier bound and its landing recorded
//! here. What the body reads when it names the formal is therefore already
//! the receiving interface; no reader reconstructs it from a second map,
//! and no forwarding road can move the relation without it.
//!
//! Private fields throughout. A bound formal is minted only in this
//! module's parent; the collection grows only through the mint, and an
//! entry is addressed by the formal's own name, so nothing can pair a
//! carrier with a formal it was not bound at.

use crate::diagnostic::Internal;
use crate::error::Result;
use crate::pipeline::ast_unresolved::{Access, Chain, GroundForm, GroundMention, Relation};
use crate::pipeline::asts::core::definitions::HeadItem;
use crate::pipeline::asts::core::{AuthoredColumn, NamedReference, Reference};
use crate::pipeline::asts::ddl::{HeadItems, HoParam};
use crate::relation::StructuralRelation;

/// THE COMPLETE POSITIONAL PATTERN an appointed face spends: every declared
/// name, in order, as the caller pattern the one pattern authority reads —
/// exact width, repeated-name equality and every other admitted formal
/// shape are that authority's judgment, made where the pattern is applied.
pub(super) fn appointed_pattern(items: &[HeadItem]) -> Access {
    Access::from_terms(
        items
            .iter()
            .map(|item| {
                crate::pipeline::ast_unresolved::DomainExpression::Reference(Reference::Named(
                    NamedReference(AuthoredColumn {
                        name: item.supply.spelling().into(),
                        qualifier: None,
                        namespace_path: crate::pipeline::ast_unresolved::NamespacePath::empty(),
                    }),
                ))
            })
            .collect(),
    )
}

/// THE FACE APPLIED AS THE ONE ARGUMENTATIVE READ, for a road that stages
/// its actual before the body opens — an effect rule's input, snapshotted
/// into plan scratch at the demand site. An appointed formal's pattern is
/// spent over the actual as the argumentative read the resolver judges, so
/// what is staged already publishes the receiving interface; an open
/// formal stages the actual as it is. The formal's own name is the read's
/// identifier, exactly as `T(k, v)` written over a relation would be.
pub(in crate::defuse) fn faced_input(formal: &HoParam, relation: Chain) -> Chain {
    match formal {
        HoParam::Relation {
            name,
            cols: HeadItems::Listed(items),
        } => Chain::read(
            Relation::InnerRelation {
                pattern: crate::pipeline::ast_unresolved::InnerRelationPattern::Indeterminate {
                    identifier: crate::pipeline::ast_unresolved::QualifiedName {
                        namespace_path: crate::pipeline::ast_unresolved::NamespacePath::empty(),
                        name: name.clone(),
                    },
                    subquery: Box::new(relation),
                },
                alias: None,
                outer: false,
            },
            appointed_pattern(items),
        ),
        HoParam::Relation {
            cols: HeadItems::Glob,
            ..
        }
        | HoParam::Scalar { .. }
        | HoParam::Rule { .. }
        | HoParam::Ground { .. } => relation,
    }
}

/// What a formal reads as: a carrier the body addresses by landing, or the
/// rows of an inline lift standing in the body under the declared names.
#[derive(Clone, Debug)]
enum Binding {
    /// A carrier bound by the authority. Its relation already publishes the
    /// receiving body's interface.
    Carrier(StructuralRelation),
    /// The rows of an inline scalar lift or a headerless literal, headed by
    /// the declared names: a value every reader must see the cells of, so it
    /// stands in the body whole rather than behind a carrier.
    Inline(GroundForm),
}

/// A RELATION FORMAL'S IDENTITY: the position the declaration issued it at.
/// Private and issued only over the declared row, so a binding can only be
/// made AT an issued formal, never under a spelling some binding code
/// supplied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RelationFormalId(u32);

/// A RELATION FORMAL, BOUND. Made only by the carrier authority, at the
/// identity the declaration issued.
#[derive(Clone, Debug)]
pub struct BoundRelationFormal {
    name: delightql_types::SqlIdentifier,
    binding: Binding,
}

/// WHAT A FORMAL IS BOUND TO — minted only in this module's parent, and
/// spent by [`RelationFormals::bind`] at an issued identity.
pub struct FormalBinding(Binding);

impl FormalBinding {
    pub(super) fn carrier(landing: StructuralRelation) -> Self {
        FormalBinding(Binding::Carrier(landing))
    }

    /// An inline relation whose heading the DECLARATION supplied: the face
    /// is applied by the construction of the literal, before it arrives here.
    pub(super) fn inline(rows: Chain) -> Result<Self> {
        let head = rows.into_bare_head().ok_or_else(|| {
            Internal::invariant(
                "relation formal binding",
                "an inline lift is a bare literal head",
            )
        })?;
        Ok(FormalBinding(Binding::Inline(head.into_form())))
    }
}

impl BoundRelationFormal {
    /// The landing the body addresses this formal's carrier by; `None` for
    /// an inline relation, which stands in the body itself.
    pub fn landing(&self) -> Option<StructuralRelation> {
        match &self.binding {
            Binding::Carrier(landing) => Some(*landing),
            Binding::Inline(_) => None,
        }
    }

    /// THE BODY'S READ OF THIS FORMAL, under the access the body wrote. The
    /// access passes through as written: what the formal publishes is
    /// already the receiving interface, so a whole read publishes it and a
    /// body-authored pattern is applied over it like over any relation. The
    /// read answers to the DECLARED identifier, strop and all.
    pub fn read(
        &self,
        access: Access,
        alias: Option<delightql_types::SqlIdentifier>,
        outer: bool,
    ) -> Chain {
        match &self.binding {
            Binding::Carrier(landing) => Chain::read(
                Relation::Ground {
                    mention: GroundMention::Structural {
                        pending: *landing,
                        authored_name: Some(self.name.clone()),
                        alias,
                    },
                    outer,
                },
                access,
            ),
            Binding::Inline(head) => {
                if access.is_whole() {
                    Chain::authored(head.clone())
                } else {
                    Chain::read_head(head.clone(), access)
                }
            }
        }
    }

    /// The semantic identity of what was bound, for the instance key: the
    /// landing for a carrier, the literal's own text for an inline lift.
    pub(crate) fn instance_key(&self) -> String {
        use crate::lispy::ToLispy;
        match &self.binding {
            Binding::Carrier(landing) => format!("relation:{landing:?}"),
            Binding::Inline(head) => format!("relation:inline:{}", head.to_lispy()),
        }
    }
}

/// THE RELATION FORMALS ONE DECLARATION ISSUED, and what one use bound each
/// to. Issued over the declared row BEFORE any actual is paired; bound only
/// at an issued identity, by declared position; read only through the
/// declared identifier under the language's own identity law — an
/// unstropped spelling folds, a stropped one is exact — never through a
/// raw string.
#[derive(Clone, Debug, Default)]
pub struct RelationFormals {
    issued: Vec<(delightql_types::SqlIdentifier, RelationFormalId)>,
    bound: Vec<(RelationFormalId, BoundRelationFormal)>,
    landed: Option<RelationFormalId>,
}

impl RelationFormals {
    /// ISSUE the relation formals of one declared row: every relation
    /// parameter, at the position the declaration gives it.
    pub(super) fn issued(params: &[HoParam]) -> Self {
        Self::issued_where(params, |param| matches!(param, HoParam::Relation { .. }))
    }

    fn issued_where(params: &[HoParam], admits: impl Fn(&HoParam) -> bool) -> Self {
        RelationFormals {
            issued: params
                .iter()
                .enumerate()
                .filter(|(_, param)| admits(param))
                .map(|(position, param)| (param.name().clone(), RelationFormalId(position as u32)))
                .collect(),
            bound: Vec::new(),
            landed: None,
        }
    }

    fn issued_at(
        &self,
        position: usize,
    ) -> Option<(&delightql_types::SqlIdentifier, RelationFormalId)> {
        self.issued
            .iter()
            .find(|(_, id)| id.0 as usize == position)
            .map(|(name, id)| (name, *id))
    }

    /// The bound formal the DECLARED identifier names, if any.
    pub fn get(&self, name: &delightql_types::SqlIdentifier) -> Option<&BoundRelationFormal> {
        let (_, id) = self.issued.iter().find(|(declared, _)| declared == name)?;
        self.bound
            .iter()
            .find(|(bound, _)| bound == id)
            .map(|(_, formal)| formal)
    }

    /// The landing of the formal the pipe landed at, when the use was piped.
    pub fn landed_source(&self) -> Option<StructuralRelation> {
        let id = self.landed?;
        self.bound
            .iter()
            .find(|(bound, _)| *bound == id)
            .and_then(|(_, formal)| formal.landing())
    }

    /// BIND THE FORMAL AT ONE DECLARED POSITION. The position must be an
    /// issued relation formal, and a formal is bound once per use.
    pub(super) fn bind(
        &mut self,
        position: usize,
        binding: FormalBinding,
        landed: bool,
    ) -> Result<()> {
        let Some((name, id)) = self
            .issued_at(position)
            .map(|(name, id)| (name.clone(), id))
        else {
            return Err(Internal::invariant(
                "relation formal binding",
                format!("position {position} is not an issued relation formal"),
            ));
        };
        if self.bound.iter().any(|(bound, _)| *bound == id) {
            return Err(Internal::invariant(
                "relation formal binding",
                format!("relation formal '{name}' is bound twice in one use"),
            ));
        }
        if landed {
            if self.landed.is_some() {
                return Err(Internal::invariant(
                    "relation formal binding",
                    "one use has one landed formal",
                ));
            }
            self.landed = Some(id);
        }
        self.bound.push((
            id,
            BoundRelationFormal {
                name,
                binding: binding.0,
            },
        ));
        Ok(())
    }

    /// A LANDING STANDS IN ANOTHER'S PLACE: every formal bound at `from` is
    /// addressed at `to` hereafter. Answers whether any was.
    pub(super) fn reland(&mut self, from: StructuralRelation, to: StructuralRelation) -> bool {
        let mut replaced = false;
        for (_, bound) in &mut self.bound {
            if let Binding::Carrier(landing) = &mut bound.binding {
                if *landing == from {
                    *landing = to;
                    replaced = true;
                }
            }
        }
        replaced
    }

    /// A RESIDUAL'S SEALED PREFIX JOINS THE SUFFIX SPEND: both halves were
    /// issued over the same declaration and bound by the authority, and a
    /// formal is bound in exactly one of them.
    pub(in crate::defuse) fn extend(&mut self, prefix: RelationFormals) -> Result<()> {
        if self.issued.is_empty() {
            self.issued = prefix.issued;
        } else if self.issued != prefix.issued {
            return Err(Internal::invariant(
                "relation formal binding",
                "a residual's prefix and suffix were issued over different declarations",
            ));
        }
        for (id, bound) in prefix.bound {
            if self.bound.iter().any(|(existing, _)| *existing == id) {
                return Err(Internal::invariant(
                    "relation formal binding",
                    format!("relation formal '{}' is bound twice in one use", bound.name),
                ));
            }
            self.bound.push((id, bound));
        }
        Ok(())
    }

    /// CONSULT-TIME PLACEHOLDERS for a definition parsed before any call
    /// supplies actuals: an open formal reads a proffer landing no carrier
    /// answers to, an appointed one reads one null row under the declared
    /// names, and a scalar formal reads a proffer too, so a body that names
    /// it in relation position still parses. Analysis only; no relation is
    /// paired with anything here.
    pub(crate) fn proffered(
        head: &crate::pipeline::asts::ddl::Head,
        identities: &crate::relation::Planning,
    ) -> Result<RelationFormals> {
        let params = head.ho_params.as_deref().unwrap_or_default();
        let mut formals = Self::issued_where(params, |param| {
            matches!(param, HoParam::Relation { .. } | HoParam::Scalar { .. })
        });
        for (position, param) in params.iter().enumerate() {
            let binding = match param {
                HoParam::Relation {
                    cols: HeadItems::Glob,
                    ..
                }
                | HoParam::Scalar { .. } => {
                    FormalBinding::carrier(identities.authority().reserve_proffer())
                }
                HoParam::Relation {
                    cols: HeadItems::Listed(items),
                    ..
                } => {
                    let columns: Vec<String> =
                        items.iter().map(|item| item.supply.spelling()).collect();
                    let null_row: Vec<crate::pipeline::asts::core::LiteralValue> = columns
                        .iter()
                        .map(|_| crate::pipeline::asts::core::LiteralValue::Null)
                        .collect();
                    match crate::pipeline::resolver::grounding::lift_scalars_to_anonymous_table(
                        &columns,
                        &[null_row],
                    ) {
                        Ok(rows) => FormalBinding::inline(rows)?,
                        Err(_) => FormalBinding::carrier(identities.authority().reserve_proffer()),
                    }
                }
                HoParam::Rule { .. } | HoParam::Ground { .. } => continue,
            };
            formals.bind(position, binding, false)?;
        }
        Ok(formals)
    }
}
