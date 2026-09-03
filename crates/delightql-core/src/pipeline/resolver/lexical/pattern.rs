// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE SLOT ROW, JUDGED — the mechanism behind the one whole-relation
//! argumentative operation.
//!
//! Private to the lexical authority. Nothing here is reachable from
//! outside `lexical`: the operand, the owner, the row in view a binder
//! may reuse, and the terminal judgment a qualified slot consumes are all
//! taken from the position the operation holds, never supplied beside
//! each other by a caller. The row this produces is sealed
//! ([`crate::relation::pending::SlotRow`]) with the proof only this module
//! mints, so the authority binds exactly the row the judgment made.
//!
//! Every name here is a [`SqlIdentifier`]: an owner, a binder and a
//! qualified slot are interned AS WRITTEN, strop included, up to the
//! spelling the export answers to and the port each binder publishes.

use super::{PatternOwner, Position, Terminal};
use crate::error::{DelightQLError, Result};
use crate::names::Registry;
use crate::pipeline::ast_transform::{walk_transform_boolean, walk_transform_domain, AstTransform};
use crate::pipeline::asts::core::{AuthoredColumn, ColumnOccurrence};
use crate::pipeline::asts::core::{Comparison, Existence, RelationalMembership, SigmaApplication};
use crate::pipeline::asts::core::{DomainExpression, Resolved, TruthExpression, Unresolved};
use crate::pipeline::asts::core::{NamedReference, Reference as AstReference};
use crate::pipeline::asts::unresolved::LiteralValue;
use crate::pipeline::asts::vocabulary::Vec1;
use crate::pipeline::asts::{resolved as ast_resolved, unresolved as ast_unresolved};
use crate::pipeline::resolver::unification::{ColumnReference, UnificationResult};
use crate::relation::{PortId, SemanticRelation};
use delightql_types::SqlIdentifier;

enum NormalizedColumnSpec {
    All,
    AllWithUsing(Vec<SqlIdentifier>),
    Explicit(Vec<ColumnSelection>),
}

struct ColumnSelection {
    source_position: usize,
    /// The name this slot binds, AS WRITTEN — a binder or a qualified
    /// reference's column. `None` for a slot that names nothing.
    binds: Option<SqlIdentifier>,
    constraint: Option<PatternConstraint>,
    /// The slot as the caller wrote it, WHOLE. Kept so the resolved relation
    /// can be rebuilt slot-for-slot: a slot that bound a name is replaced by
    /// the occurrence it bound, and every other slot keeps its authored
    /// form, which names nothing and so carries no spelling past resolution.
    authored: ast_unresolved::Slot,
}

enum PatternConstraint {
    Literal(LiteralValue),
    /// `(t.k)` — the column the author addressed, as written.
    Reference(AuthoredColumn),
    Skip,
    SelfUnify {
        first_position: usize,
    },
    Expression(Box<ast_unresolved::DomainExpression>),
}

/// WHAT A SLOT ROW RESOLVED TO.
///
/// The ACCESS STEP is one value — the dimensions the row selects and the
/// interface those dimensions publish, written by the authority in one
/// act — and nothing hands out either half: one row's interface cannot end
/// up standing beside another row's slots.
pub(super) struct SlotRead {
    step: ast_resolved::Step,
    /// A WHOLE-HEADING READ (`t(*)`, `t(.*)`, `t(.(a, b))`) asks for every
    /// dimension the relation already published and is not argumentative:
    /// the relation it read stays reachable by its name. A slot row is.
    whole: bool,
    where_constraints: Vec<ast_resolved::TruthExpression>,
    using_columns: Option<Vec<SqlIdentifier>>,
}

impl SlotRead {
    /// What the row publishes, for the act stating its lexical answer.
    pub(super) fn publishes(&self) -> SemanticRelation {
        *self.step.result()
    }

    /// Whether this read is a whole-heading read rather than a slot row.
    pub(super) fn is_whole_read(&self) -> bool {
        self.whole
    }

    /// The columns a dequalifying access named, when it named any.
    pub(super) fn using_columns(&self) -> Option<&[SqlIdentifier]> {
        self.using_columns.as_deref()
    }

    /// THE READ THIS ROW RESOLVES TO: the ground relation and the access
    /// its own parens asked for, landed together.
    pub(super) fn ground_read(
        self,
        outer: bool,
        identities: &crate::relation::Planning,
    ) -> Result<(ast_resolved::Chain, Vec<ast_resolved::TruthExpression>)> {
        let authority = identities.authority();
        let head = authority.reading(crate::relation::builder::ReadHead::Ground {
            outer,
            published: *self.step.result(),
        })?;
        let read = authority.reland(ast_resolved::Chain::ground(head), self.step)?;
        Ok((read, self.where_constraints))
    }

    /// THE ACCESS STEP, for a row applied over a body that is already
    /// resolved. It lands on that body through the authority, which checks
    /// the row was resolved over it.
    pub(super) fn applied_to(
        self,
        expr: ast_resolved::Chain,
        identities: &crate::relation::Planning,
    ) -> Result<(ast_resolved::Chain, Vec<ast_resolved::TruthExpression>)> {
        let landed = identities.authority().reland(expr, self.step)?;
        Ok((landed, self.where_constraints))
    }
}

pub(super) fn apply_local_constraints(
    source: ast_resolved::Chain,
    constraints: Vec<ast_resolved::TruthExpression>,
    output_scope: SemanticRelation,
) -> ast_resolved::Chain {
    let Some(condition) = ast_resolved::TruthExpression::all(constraints) else {
        return source;
    };
    source.transparently(ast_resolved::Transparent::Restrict {
        condition,
        origin: ast_resolved::FilterOrigin::PositionalLiteral {
            source: output_scope.scope(),
        },
    })
}

/// The name a refusal teaches the row by: the owner the author wrote, or
/// the operand as the registry describes it.
fn teaching_name(
    owner: &PatternOwner,
    operand: SemanticRelation,
    registry: &crate::relation::Planning,
) -> String {
    match owner {
        PatternOwner::Authored(name) => name.to_string(),
        PatternOwner::Unqualified => {
            let mut text = String::new();
            registry.describe(operand.scope(), &mut crate::names::Teaching(&mut text));
            text
        }
    }
}

/// THE ROW'S RESULT, over the relation it reads.
///
/// The operand is stated, not recovered: which relation a row stands on is
/// the read's, and a scope reconstructed from whichever columns were
/// offered cannot tell one occurrence of a table from another. The
/// interface is derived from the operand HERE; the row a binder may reuse
/// and the judgment a qualified slot consumes are the position's own.
pub(super) fn resolve(
    access: &ast_unresolved::Access,
    operand: SemanticRelation,
    owner: &PatternOwner,
    position: &Position<'_>,
    instantiation: crate::pipeline::resolver::SlotInstantiation<'_, '_>,
    registry: &crate::relation::Planning,
) -> Result<SlotRead> {
    let ports = crate::relation::published_ports(registry, &operand)?;
    // THE PATTERN ADDRESSES THE DECLARED HEADING. Hygienic support
    // positions — correlation carriers, injected discriminators — are not
    // part of it: they are split off HERE, once, and ride the read as
    // dependencies, so every row means the same thing over an interface
    // that carries them.
    let mut visible = Vec::with_capacity(ports.len());
    let mut carriers = Vec::new();
    let mut crossing = Vec::new();
    for port in ports {
        if crate::relation::is_higher_order_support(registry, port) {
            if registry.authority().residual_row_token(port).is_some()
                || registry.authority().residual_capture_value(port).is_some()
            {
                crossing.push(port);
            } else {
                carriers.push(port);
            }
        } else {
            visible.push(port);
        }
    }
    let spelled = teaching_name(owner, operand, registry);
    // EXACT WIDTH, JUDGED ONCE. A slot row is total over the declared
    // heading: one slot per visible position, whatever the relation.
    if let ast_unresolved::Access::Slots(slots) = access {
        if slots.len() != visible.len() {
            return Err(DelightQLError::validation_error(
                format!(
                    "Positional pattern incomplete - table '{}' has {} columns but pattern specifies {} elements",
                    spelled,
                    visible.len(),
                    slots.len()
                ),
                "Positional pattern validation".to_string(),
            ));
        }
    }
    match normalize_pattern(access, &visible, &spelled, registry)? {
        // A GLOB SHAPE HAS NO SLOTS, so it publishes what it was handed:
        // the access states which whole-heading form was written and the
        // step restates the operand's own relation.
        NormalizedColumnSpec::All => Ok(SlotRead {
            step: whole_heading_step(access, operand, registry)?,
            whole: true,
            where_constraints: Vec::new(),
            using_columns: None,
        }),
        NormalizedColumnSpec::AllWithUsing(columns) => {
            // The correspondence is settled by whoever consumes the USING
            // columns — the join road's `Correspond` carrier and the
            // existence road's synthesis, each against the heading of the
            // relation the step continues. Deriving conditions here read
            // the whole outer context instead, so a sibling existence
            // scope could make an unambiguous name refuse as ambiguous —
            // and every consumer discarded the conditions.
            Ok(SlotRead {
                step: whole_heading_step(access, operand, registry)?,
                whole: true,
                where_constraints: Vec::new(),
                using_columns: Some(columns),
            })
        }
        NormalizedColumnSpec::Explicit(selections) => resolve_explicit_selections(
            selections,
            operand,
            &visible,
            carriers,
            crossing,
            owner,
            position,
            instantiation,
            registry,
        ),
    }
}

fn normalize_pattern(
    pattern: &ast_unresolved::Access,
    table_schema: &[PortId],
    spelled: &str,
    registry: &crate::relation::Planning,
) -> Result<NormalizedColumnSpec> {
    match pattern {
        ast_unresolved::Access::All
        | ast_unresolved::Access::Unasked
        | ast_unresolved::Access::DequalifyAll => Ok(NormalizedColumnSpec::All),
        ast_unresolved::Access::Dequalify(columns) => {
            // As written: the dequalifying step names the column the
            // author spelled, and this is the read that decides whether
            // it is there.
            for column in columns {
                let name = super::lookup::written_name(column, registry);
                let exists = table_schema
                    .iter()
                    .any(|port| registry.published_sym(port.column()) == Some(name));
                if !exists {
                    return Err(DelightQLError::column_not_found_error(
                        column.clone(),
                        format!("USING column '{column}' not found in table '{spelled}'"),
                    ));
                }
            }
            Ok(NormalizedColumnSpec::AllWithUsing(columns.clone()))
        }
        ast_unresolved::Access::Slots(slots) => Ok(NormalizedColumnSpec::Explicit(
            positional_to_selections(slots, registry),
        )),
    }
}

fn positional_to_selections(
    slots: &Vec1<ast_unresolved::Slot>,
    registry: &crate::relation::Planning,
) -> Vec<ColumnSelection> {
    let mut selections = Vec::new();
    let mut first_slot_of: Vec<(crate::names::Sym, usize)> = Vec::new();
    for (position, slot) in slots.iter().enumerate() {
        // A slot that BINDS carries the name it binds: the binder slots and
        // the qualified references. Every other slot names nothing, and the
        // two are told apart by that absence downstream.
        let (binds, constraint) = match slot {
            ast_unresolved::Slot::Bind(binder) => {
                // THE SAME VARIABLE TWICE is the same complete name: a
                // stropped `A` and a bare `a` are two variables.
                let name = super::lookup::written_name(&binder.name, registry);
                match first_slot_of.iter().find(|(seen, _)| *seen == name) {
                    Some(&(_, first_position)) => (
                        Some(binder.name.clone()),
                        Some(PatternConstraint::SelfUnify { first_position }),
                    ),
                    None => {
                        first_slot_of.push((name, position));
                        (Some(binder.name.clone()), None)
                    }
                }
            }
            ast_unresolved::Slot::Anon => (None, Some(PatternConstraint::Skip)),
            ast_unresolved::Slot::Reuse(NamedReference(authored)) => (
                Some(authored.name.clone()),
                Some(PatternConstraint::Reference(authored.clone())),
            ),
            ast_unresolved::Slot::Constraint(term) => match &**term {
                ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Ground(value),
                ) => (None, Some(PatternConstraint::Literal(value.clone()))),
                other => (
                    None,
                    Some(PatternConstraint::Expression(Box::new(other.clone()))),
                ),
            },
        };
        selections.push(ColumnSelection {
            source_position: position,
            binds,
            constraint,
            authored: slot.clone(),
        });
    }
    selections
}

/// THE OCCURRENCE A QUALIFIED SLOT REUSES: the frontier's terminal judgment
/// over the position the row stands in — never a search of the left row by
/// name.
fn addressed_reuse(
    authored: &AuthoredColumn,
    position: &Position<'_>,
    registry: &crate::relation::Planning,
) -> Result<ColumnOccurrence> {
    let spelled = match &authored.qualifier {
        Some(table) => format!("{table}.{}", authored.name),
        None => authored.name.to_string(),
    };
    let reference = ColumnReference::Named {
        name: authored.name.clone(),
        qualifier: authored.qualifier.clone(),
    };
    let mut witness = super::Witness::default();
    match position.address(reference, false, &mut witness, registry)? {
        UnificationResult::Resolved(occurrence) => Ok(occurrence),
        UnificationResult::Unresolved(_) => Err(DelightQLError::column_not_found_error(
            spelled,
            "in positional join pattern",
        )),
        UnificationResult::Ambiguous { .. } => Err(DelightQLError::validation_error_categorized(
            "resolution/ambiguous",
            "Positional join reference matches more than one left-hand column",
            "qualify the reference with one visible relation",
        )),
        UnificationResult::Opaque => Err(crate::pipeline::resolver::opaque_reference_refusal()),
        UnificationResult::Refused(refusal) => Err(refusal.into_error()),
    }
}

#[allow(clippy::too_many_arguments)]
fn resolve_explicit_selections(
    selections: Vec<ColumnSelection>,
    operand: SemanticRelation,
    table_schema: &[PortId],
    carriers: Vec<PortId>,
    crossing: Vec<PortId>,
    owner: &PatternOwner,
    position: &Position<'_>,
    instantiation: crate::pipeline::resolver::SlotInstantiation<'_, '_>,
    registry: &crate::relation::Planning,
) -> Result<SlotRead> {
    // THE ROW A BINDER MAY REUSE is the row in view — every position the
    // position's frames and their enclosure publish — when any row does.
    let row_in_view = if position.encloses_a_row() {
        Some(position.ports_in_view(registry)?)
    } else {
        None
    };
    let mut positions = Vec::with_capacity(selections.len());
    let mut where_constraints = Vec::new();
    let mut reused: Vec<crate::names::Sym> = Vec::new();

    for selection in selections {
        let source = table_schema[selection.source_position];
        let publishes = selection.constraint.is_none();
        if matches!(selection.constraint, Some(PatternConstraint::Skip)) {
            positions.push(crate::relation::pending::PatternPosition::Skips {
                stored: resolve_authored_slot(
                    &selection.authored,
                    table_schema,
                    registry,
                    instantiation,
                )?,
            });
            continue;
        }

        let qualified_reference = matches!(selection.authored, ast_unresolved::Slot::Reuse(_));
        let stored = if selection.binds.is_none() {
            // Names nothing, so there is no occurrence to point at: a
            // literal or a computed slot constrains the source column but
            // does not bind it to a caller-written name. Its interior is
            // still resolved against this relation's own heading — the
            // same heading the constraint beside it is resolved against.
            Some(resolve_authored_slot(
                &selection.authored,
                table_schema,
                registry,
                instantiation,
            )?)
        } else {
            None
        };
        // A BINDING SPELLING REUSES THE EXACTLY-ONE LIVE BARE PORT of its
        // name, decided by the one bare-reuse judgment while the complete
        // live bare interface is in hand. A positional lvar unifies with a
        // BARE lvar of the same complete name and with nothing else:
        // qualification is part of an lvar's complete name, so a glob's
        // `lt.k` and a positional `k` are different names and their
        // relations cross. Each name reuses at most once in one row.
        let reuses = match (&row_in_view, publishes, &stored, &selection.binds) {
            (Some(row), true, None, Some(name)) => {
                let complete = super::lookup::written_name(name, registry);
                if reused.contains(&complete) {
                    None
                } else {
                    let found = super::lookup::live_bare_reuse(row, name, registry)?;
                    if found.is_some() {
                        reused.push(complete);
                    }
                    found
                }
            }
            _ => None,
        };

        positions.push(match (publishes, stored, selection.binds) {
            // A CALLER PATTERN BINDS; IT DOES NOT RENAME. The written name
            // is the author's for this call, so the position answers to it
            // BARE — a qualified reference reaches the source through the
            // carry chain, and positional unification recognizes the
            // binding as an lvar. What the tree stores at it is the port the
            // read mints, so the binding and the position it stands at
            // cannot disagree. The name is interned AS WRITTEN.
            (true, None, Some(name)) => crate::relation::pending::PatternPosition::Binds {
                source,
                naming: crate::relation::form::Naming::Bound(
                    registry.intern(name.as_str(), name.is_stropped()),
                ),
                qualified: qualified_reference,
                reuses,
            },
            (true, None, None) => unreachable!("a slot that publishes and stores nothing binds"),
            (true, Some(stored), _) => {
                crate::relation::pending::PatternPosition::Publishes { source, stored }
            }
            (false, Some(stored), _) => {
                crate::relation::pending::PatternPosition::Constrains { source, stored }
            }
            // A qualified reference names somebody else's column: it
            // constrains this position, it does not offer a name for it.
            (false, None, _) => crate::relation::pending::PatternPosition::Constrains {
                source,
                stored: if qualified_reference {
                    ast_resolved::Slot::Reuse(NamedReference(ColumnOccurrence::engine_qualified(
                        source,
                    )))
                } else {
                    // A bare name bound. The slot stays a binding slot; the
                    // phase changed what it holds, not what it is.
                    ast_resolved::Slot::Bind(source)
                },
            },
        });

        match selection.constraint {
            Some(PatternConstraint::Literal(value)) => {
                where_constraints.push(create_term_constraint(
                    source,
                    ast_resolved::DomainExpression::Application(
                        ast_resolved::FunctionApplication::Ground(value),
                    ),
                ));
            }
            Some(PatternConstraint::Reference(authored)) => {
                // A QUALIFIED SLOT REUSES THE EXISTING LOGICAL VALUE: it
                // constrains this position against the addressed column and
                // publishes nothing (FN.13), so there is no name for a USING
                // settlement to find. The correspondence is stated over the
                // addressed occurrence and this exact port, and it is the
                // equality that decides which rows correspond.
                let addressed = addressed_reuse(&authored, position, registry)?;
                where_constraints.push(create_correspondence_condition(addressed, source));
            }
            Some(PatternConstraint::SelfUnify { first_position }) => {
                where_constraints.push(create_self_unification_condition(
                    table_schema[first_position],
                    source,
                ));
            }
            Some(PatternConstraint::Expression(expression)) => {
                where_constraints.push(create_term_constraint(
                    source,
                    convert_unresolved_to_resolved_expression(
                        &expression,
                        table_schema,
                        registry,
                        instantiation,
                    )?,
                ));
            }
            Some(PatternConstraint::Skip) => unreachable!("skip handled before binding"),
            None => {}
        }
    }

    for source in crossing {
        positions.push(crate::relation::pending::PatternPosition::Binds {
            source,
            naming: crate::relation::form::Naming::Hygienic,
            qualified: false,
            reuses: None,
        });
    }

    // THE OWNER THE ROW ANSWERS TO, as written: an authored name is
    // interned with its strop; a row nobody named answers to nothing.
    let answers_to = match owner {
        PatternOwner::Authored(name) => Some(registry.intern(name.as_str(), name.is_stropped())),
        PatternOwner::Unqualified => None,
    };
    // ONE DESCRIPTION, SEALED. The row says what each written slot does;
    // the authority asks the dimensions, exports them under the written
    // owner, and stores the ports it minted at the positions that bind.
    let (step, _) = registry
        .authority()
        .bind(crate::relation::pending::Pending::CallerPattern(
            crate::relation::pending::SlotRow::judged(
                operand,
                answers_to,
                positions,
                carriers,
                Terminal::judged(),
            ),
        ))?;
    Ok(SlotRead {
        step,
        whole: false,
        where_constraints,
        using_columns: None,
    })
}

/// THE ACCESS STEP OF A PATTERN THAT SELECTS NO SLOT.
///
/// `(*)`, `()`, `.*` and `.(a, b)` name a whole heading: they publish what
/// they were handed, so the step restates its operand's relation and
/// nothing is derived. Which whole-heading form the tree stores is the
/// AUTHORED one; a pattern with slots does not reach here.
fn whole_heading_step(
    authored: &ast_unresolved::Access,
    input: SemanticRelation,
    registry: &crate::relation::Planning,
) -> Result<ast_resolved::Step> {
    let access = match authored {
        ast_unresolved::Access::All => ast_resolved::Access::All,
        ast_unresolved::Access::Unasked => ast_resolved::Access::Unasked,
        ast_unresolved::Access::Dequalify(columns) => {
            ast_resolved::Access::Dequalify(columns.clone())
        }
        ast_unresolved::Access::DequalifyAll => ast_resolved::Access::DequalifyAll,
        ast_unresolved::Access::Slots(_) => {
            return Err(DelightQLError::validation_error(
                "A positional pattern reached resolution without bound occurrences",
                "Positional pattern resolution",
            ))
        }
    };
    let (step, _) = registry
        .authority()
        .bind(crate::relation::pending::Pending::Requalify { input, access })?;
    Ok(step)
}

fn resolved_ref(column: PortId) -> ast_resolved::DomainExpression {
    ast_resolved::DomainExpression::Reference(AstReference::Named(NamedReference(
        ColumnOccurrence::engine(column),
    )))
}

/// A slot term unifies with its physical position. Literal and computed terms
/// reach this one comparison, whose equality is null-safe by the slot law:
/// the term is read against this relation's own heading, so the answer
/// selects a row and cannot multiply rows.
fn create_term_constraint(
    column: PortId,
    term: ast_resolved::DomainExpression,
) -> ast_resolved::TruthExpression {
    ast_resolved::TruthExpression::Comparison(Comparison {
        operator: crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual,
        left: Box::new(resolved_ref(column)),
        right: Box::new(term),
    })
}

/// THE AUTHORED SLOT, RESOLVED — slot-for-slot, kind for kind.
fn resolve_authored_slot(
    slot: &ast_unresolved::Slot,
    table_schema: &[PortId],
    registry: &crate::relation::Planning,
    instantiation: crate::pipeline::resolver::SlotInstantiation<'_, '_>,
) -> Result<ast_resolved::Slot> {
    Ok(match slot {
        // The anonymous slot is STRUCTURE, not a value: it crosses as
        // itself, never through the value road — its authored term is the
        // open leaf, which resolution spends only at an applying position.
        ast_unresolved::Slot::Anon => ast_resolved::Slot::Anon,
        other => {
            let term = other
                .term()
                .expect("only the anonymous slot has no term road, and it is handled above");
            ast_resolved::Slot::classify(convert_unresolved_to_resolved_expression(
                &term,
                table_schema,
                registry,
                instantiation,
            )?)
        }
    })
}

/// A REPEATED BINDER WITHIN ONE PATTERN: the same variable twice, in one
/// row. Rows cannot multiply, so the equality is the language's null-safe
/// one and a both-null row instances the pattern (equality-law row 13).
fn create_self_unification_condition(left: PortId, right: PortId) -> ast_resolved::TruthExpression {
    ast_resolved::TruthExpression::Comparison(Comparison {
        operator: crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual,
        left: Box::new(resolved_ref(left)),
        right: Box::new(resolved_ref(right)),
    })
}

/// A QUALIFIED SLOT NAMES ANOTHER RELATION'S COLUMN, and this is the act
/// that establishes the cross-row correspondence: the addressed occurrence
/// stands in the row the pattern looks left into, so the two ports stand in
/// two relations the join multiplies. Correspondence equality is the
/// target's own (equality-law row 3) — null is absence here, and absence
/// makes no correspondence.
///
/// Stated HERE and nowhere later: a downstream reader that recovered the
/// role from the predicate's references, its bucket, or the clause it was
/// finally emitted in would be answering a different question than the act
/// that built the join.
fn create_correspondence_condition(
    left: ColumnOccurrence,
    right: PortId,
) -> ast_resolved::TruthExpression {
    ast_resolved::TruthExpression::Comparison(Comparison {
        operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
        left: Box::new(ast_resolved::DomainExpression::Reference(
            AstReference::Named(NamedReference(left)),
        )),
        right: Box::new(resolved_ref(right)),
    })
}

/// THE CONVERTER A COMPUTED SLOT'S INTERIOR RESOLVES THROUGH: the slot
/// sees exactly its own relation's heading, and a definition it invokes
/// instantiates under the allowance it carries.
///
/// The heading is PRIVATE and set by the row's own judgment; outside the
/// lexical authority the converter exists only [`Self::sealed`] — over no
/// heading at all, for a definition body that sees its formals and
/// nothing else. No caller pairs a heading with an allowance.
pub(crate) struct StrictPhaseConverter<'a, 'db> {
    heading: &'a [PortId],
    registry: &'a Registry,
    /// The resolver core, the one lexical world, the compilation's ONE
    /// allowance, and (for a scoped body) the body's own formal bindings —
    /// together, always: a slot that meets a definition call instantiates
    /// under this allowance, and there is no converter without one.
    instantiation: crate::pipeline::resolver::SlotInstantiation<'a, 'db>,
}

impl<'a, 'db> StrictPhaseConverter<'a, 'db> {
    /// A converter over NO heading — a sealed definition body, which binds
    /// its formals and refuses every other bare name.
    pub(crate) fn sealed(
        registry: &'a Registry,
        instantiation: crate::pipeline::resolver::SlotInstantiation<'a, 'db>,
    ) -> Self {
        StrictPhaseConverter {
            heading: &[],
            registry,
            instantiation,
        }
    }

    /// The allowance this converter instantiates under.
    pub(crate) fn instantiation(&self) -> crate::pipeline::resolver::SlotInstantiation<'a, 'db> {
        self.instantiation
    }

    pub(crate) fn registry(&self) -> &'a Registry {
        self.registry
    }
}

impl AstTransform<Unresolved, Resolved> for StrictPhaseConverter<'_, '_> {
    crate::pipeline::ast_transform::position_is_resolved_against_a_heading!();
    fn fold_entity(
        &mut self,
        entity: crate::pipeline::asts::vocabulary::Ref,
    ) -> crate::error::Result<crate::names::CallableId> {
        Ok(entity.written_call_identity(self.registry))
    }
    crate::pipeline::ast_transform::column_is_bound_where_it_is_resolved!();
    crate::pipeline::ast_transform::binder_is_bound_where_the_pattern_is_resolved!();
    crate::pipeline::ast_transform::a_landing_is_consumed_where_the_pipe_is_applied!();
    crate::pipeline::ast_transform::a_context_marker_is_consumed_where_the_call_instantiates!();
    crate::pipeline::ast_transform::scope_is_minted_where_it_is_resolved!();
    crate::pipeline::ast_transform::minted_where_it_is_decided!(
        fold_output -> crate::relation::PortId: "an expression's output port",
        fold_scalar_output -> crate::relation::PortId: "a scalarized relation's column",
        fold_destructure -> Vec<crate::pipeline::asts::core::DestructureMapping>: "a destructuring pattern's columns",
    );
    fn fold_open_leaf(
        &mut self,
        _: crate::pipeline::asts::core::DomainHole,
    ) -> crate::error::Result<crate::pipeline::asts::core::FormalHole> {
        Err(crate::error::DelightQLError::validation_error_categorized(
            "value/open/unapplied",
            "a composition input stands outside any callable applying it",
            "the position that applies an open body spends its slot",
        ))
    }

    fn fold_cover_callable(
        &mut self,
        _: crate::pipeline::asts::core::Callable<crate::pipeline::asts::core::Unresolved>,
    ) -> crate::error::Result<()> {
        Err(crate::error::DelightQLError::transformation_error(
            "a cover's callable is applied where its operator resolves, and this fold is not that place",
            "phase_payload",
        ))
    }

    fn fold_rename_target(
        &mut self,
        _: crate::pipeline::asts::core::NameTarget,
    ) -> crate::error::Result<crate::names::Spelling> {
        Err(crate::error::DelightQLError::transformation_error(
            "a rename target is expanded where the rename resolves, and this fold is not that place",
            "phase_payload",
        ))
    }
    fn fold_drill(
        &mut self,
        _: crate::pipeline::asts::core::operators::AuthoredDrill,
    ) -> crate::error::Result<crate::pipeline::asts::core::operators::BoundDrill> {
        Err(crate::error::DelightQLError::transformation_error(
            "an interior drill binds where its operator resolves, and this fold is not that place",
            "phase_payload",
        ))
    }

    #[stacksafe::stacksafe]
    fn transform_domain(
        &mut self,
        expression: DomainExpression<Unresolved>,
    ) -> Result<DomainExpression<Resolved>> {
        // A slot expression may invoke a query-scoped value definition, and
        // it is spent HERE like at any other site: arguments resolve against
        // this relation's heading, the body against the formals alone.
        if let DomainExpression::Application(
            crate::pipeline::asts::core::FunctionApplication::Standard(application),
        ) = &expression
        {
            if let Some(inlined) = crate::defuse::callable::instantiate_slot(self, application)? {
                return Ok(inlined);
            }
        }
        match expression {
            DomainExpression::Reference(AstReference::Ordinal(_)) => Err(
                DelightQLError::parse_error("Column ordinals not supported in pattern constraints"),
            ),
            // A computed slot's interior sees exactly its own relation's
            // heading: the constraint compares this relation's column to an
            // expression over this relation's row. A bare name binds there;
            // anything the heading does not publish is refused here, before
            // the authored text can travel on unresolved.
            DomainExpression::Reference(AstReference::Named(NamedReference(AuthoredColumn {
                name,
                qualifier: None,
                namespace_path,
            }))) => {
                // A formal of the open instantiation is already spent: it
                // stands for the caller's resolved argument and never binds
                // to this relation's heading, so the definition's own probes
                // cannot capture it.
                if namespace_path.is_empty() {
                    if let Some(resolved) = self
                        .instantiation
                        .formals
                        .and_then(|formals| formals.value(&name))
                        .cloned()
                        .or_else(|| self.instantiation.env.formal_value(&name))
                    {
                        return Ok(resolved);
                    }
                }
                let wanted = self
                    .registry
                    .canonical(self.registry.intern(&name, name.is_stropped()));
                let hits: Vec<_> = self
                    .heading
                    .iter()
                    .copied()
                    .filter(|port| self.registry.published_sym(port.column()) == Some(wanted))
                    .collect();
                match hits.as_slice() {
                    [column] => Ok(DomainExpression::Reference(AstReference::Named(
                        NamedReference(ColumnOccurrence::engine(*column)),
                    ))),
                    [] => Err(DelightQLError::column_not_found_error(
                        name.to_string(),
                        "in a computed pattern slot",
                    )),
                    _ => Err(DelightQLError::validation_error(
                        format!(
                            "column '{}' is published more than once in this \
                             relation's heading",
                            name
                        ),
                        "in a computed pattern slot",
                    )),
                }
            }
            DomainExpression::Reference(AstReference::Named(NamedReference(AuthoredColumn {
                ..
            }))) => Err(DelightQLError::parse_error(
                "a qualified reference inside a computed pattern slot cannot bind — \
                 the slot sees only its own relation's heading",
            )),
            other => walk_transform_domain(self, other),
        }
    }

    fn transform_boolean(
        &mut self,
        expression: TruthExpression<Unresolved>,
    ) -> Result<TruthExpression<Resolved>> {
        match expression {
            TruthExpression::Existence(Existence { .. }) => Err(DelightQLError::parse_error(
                "EXISTS expressions not supported in pattern constraints",
            )),
            TruthExpression::RelationalMembership(RelationalMembership { .. }) => {
                Err(DelightQLError::parse_error(
                    "IN subquery expressions not supported in pattern constraints",
                ))
            }
            TruthExpression::Sigma(SigmaApplication { .. }) => {
                Err(DelightQLError::not_implemented(
                    "Sigma predicates in pattern destructuring not yet supported",
                ))
            }
            other => walk_transform_boolean(self, other),
        }
    }
}

fn convert_unresolved_to_resolved_expression(
    expression: &ast_unresolved::DomainExpression,
    table_schema: &[PortId],
    registry: &crate::relation::Planning,
    instantiation: crate::pipeline::resolver::SlotInstantiation<'_, '_>,
) -> Result<ast_resolved::DomainExpression> {
    StrictPhaseConverter {
        heading: table_schema,
        registry,
        instantiation,
    }
    .transform_domain(expression.clone())
}
