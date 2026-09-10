// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! AN ANONYMOUS RELATION'S HEADER ROW, JUDGED AND BORN — the slot-row law
//! the caller pattern already meets, applied to `_(a, q.b, f:(c) @ …)`.
//!
//! Private to the lexical authority. The row a bare header may reuse and
//! the terminal judgment a qualified header consumes are both taken from
//! the position the relation is resolved at — the row it is composed with
//! — never supplied beside the headers by a caller.
//!
//! THE JUDGMENT IS THE BIRTH. Its product is one affine [`AnonymousBirth`]
//! that owns everything the relation's construction depends on: the name
//! the relation answers to (interned at the judgment, because it decided
//! what a bare header may reuse), the planning authority the reuse ports
//! belong to, the grid's own type and shape facts per position, each
//! position's role and, beside it, the live port it reuses. Its fields are
//! private, it is neither `Clone` nor `Copy`, and its ONE operation,
//! [`AnonymousBirth::born`], consumes it and derives the relation on the
//! authority it holds. Nothing yields a reuse edge, accepts one beside a
//! slot, renames the relation after the judgment, supplies another
//! authority at the birth, or installs one position's facts at another:
//! there is no signature through which those ingredients travel apart.
//! The proof [`JudgedBirth`] that the relation authority's judged-birth
//! entrance spends is minted only here, in that one operation.
//!
//! NAMING DECIDES THE INTERFACE BEFORE CORRESPONDENCE. A bare header of an
//! unnamed relation is a bare logical variable and reuses the exactly-one
//! live bare port of its name; under an alias the same header is the
//! alias's own (`q.a`) and reuses nothing. A qualified header addresses an
//! existing position of the row and reuses it; it cannot introduce its
//! qualifier. What the composition then IS — a join, multiplying by every
//! matching candidate — is the comma's; no count of reused headers
//! converts the relation into an existence test.
//!
//! A HEADER'S IDENTITY IS ITS COMPLETE OCCURRENCE: the qualifier and the
//! leaf, each interned as written with its strop. `q.a`, `` `Q`.a `` and
//! `a` are three variables. Every judgment here that asks whether two
//! spellings are one variable — the repeat, and the header-versus-row
//! collision — compares that identity and never its characters.

use super::lookup::{live_bare_reuse, written_name};
use super::{Position, Terminal};
use crate::diagnostic::{AnonBinding, DelightQLError, Internal, Resolution};
use crate::error::Result;
use crate::names::{Spelling, Sym};
use crate::pipeline::asts::core::{AuthoredColumn, NamedReference, Reference, Slot, TabularRow};
use crate::pipeline::asts::vocabulary::Vec1;
use crate::pipeline::asts::{resolved as ast_resolved, unresolved as ast_unresolved};
use crate::pipeline::resolver::relation_resolver::{
    infer_anon_column_shapes, infer_anon_column_types,
};
use crate::pipeline::resolver::unification::{ColumnReference, UnificationResult};
use crate::relation::form::{AnonymousSlot, AnonymousSpec};
use crate::relation::{Planning, PortId, SemanticRelation};
use delightql_types::SqlIdentifier;

/// One authored column occurrence's identity: the qualifier and the leaf,
/// each as written — a strop is part of the name.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Occurrence {
    qualifier: Option<Sym>,
    name: Sym,
}

impl Occurrence {
    fn of(column: &AuthoredColumn, registry: &Planning) -> Self {
        Occurrence {
            qualifier: column
                .qualifier
                .as_ref()
                .map(|qualifier| written_name(qualifier, registry)),
            name: written_name(&column.name, registry),
        }
    }
}

fn spelled(column: &AuthoredColumn) -> String {
    match &column.qualifier {
        Some(qualifier) => format!("{qualifier}.{}", column.name),
        None => column.name.to_string(),
    }
}

/// The complete occurrence a data cell reads, when it reads one.
fn cell_occurrence(datum: &ast_unresolved::Datum) -> Option<&AuthoredColumn> {
    match datum {
        ast_unresolved::Datum::Value(ast_unresolved::DomainExpression::Reference(
            Reference::Named(NamedReference(column)),
        )) => Some(column),
        ast_unresolved::Datum::Value(_) | ast_unresolved::Datum::SparseFill { .. } => None,
    }
}

/// What one header position was judged to do, with the edge it decided.
enum Judgment {
    Binds {
        named: Spelling,
        reuses: Option<PortId>,
    },
    Repeats {
        first: usize,
    },
    Disregards,
    /// A ground or computed term, still authored: the birth resolves it
    /// over the row, in position order, and classifies the slot from what
    /// it resolved to.
    Constrains(ast_unresolved::DomainExpression),
}

/// THE PROOF THE JUDGED-BIRTH ENTRANCE SPENDS. One private field, minted
/// only by [`AnonymousBirth::born`].
pub(crate) struct JudgedBirth(());

/// THE HEADER ROW, JUDGED, AWAITING ITS ONE BIRTH. Private fields; affine.
pub(crate) struct AnonymousBirth<'r> {
    planning: &'r Planning,
    answers_to: Option<Spelling>,
    positions: Vec<Judgment>,
    declared_types: Vec<Option<String>>,
    shapes: Vec<crate::names::ValueShape>,
}

/// What one position of the born relation does, as the caller may read it
/// after the birth: the role and, for a computed header, the term it
/// resolved to. No reuse edge is among them.
pub(crate) enum BornPosition {
    Binds,
    /// The same variable again: the position that first bound it.
    Repeats {
        first: usize,
    },
    /// `_` — consumed, constrained by nothing, published as nothing.
    Disregards,
    /// A ground or computed term, resolved over the row.
    Constrains(ast_resolved::DomainExpression),
}

/// The relation one birth minted, with what each of its positions does.
pub(crate) struct Born {
    pub(crate) relation: SemanticRelation,
    pub(crate) positions: Vec<BornPosition>,
}

impl AnonymousBirth<'_> {
    /// THE ONE BIRTH. Consumes the judgment; resolves each constraining
    /// term through `resolve` in position order; mints every slot beside
    /// the reuse edge judged for it; derives the relation on the authority
    /// that judged the reuse, under the name the judgment answered to.
    pub(crate) fn born(
        self,
        mut resolve: impl FnMut(
            ast_unresolved::DomainExpression,
        ) -> Result<ast_resolved::DomainExpression>,
    ) -> Result<Born> {
        let AnonymousBirth {
            planning,
            answers_to,
            positions,
            declared_types,
            shapes,
        } = self;
        let mut slots = Vec::with_capacity(positions.len());
        let mut born = Vec::with_capacity(positions.len());
        for (index, judgment) in positions.into_iter().enumerate() {
            let position = index as u32;
            let declared_type = declared_types.get(index).cloned().flatten();
            let shape = shapes.get(index).copied().unwrap_or_default();
            match judgment {
                Judgment::Binds { named, reuses } => {
                    slots.push((
                        AnonymousSlot::Binder {
                            position,
                            named,
                            declared_type,
                            shape,
                        },
                        reuses,
                    ));
                    born.push(BornPosition::Binds);
                }
                // A repeated binder and `_` are consumed: physical,
                // constrained by what the analyzer writes from the stored
                // term, published as nothing.
                Judgment::Repeats { first } => {
                    slots.push((
                        AnonymousSlot::Constraint {
                            position,
                            declared_type,
                            shape,
                        },
                        None,
                    ));
                    born.push(BornPosition::Repeats { first });
                }
                Judgment::Disregards => {
                    slots.push((
                        AnonymousSlot::Constraint {
                            position,
                            declared_type,
                            shape,
                        },
                        None,
                    ));
                    born.push(BornPosition::Disregards);
                }
                // A computed header names a column of the ENCLOSING row —
                // `_(upper:(description) @ …)` probes the outer relation's
                // `description` — and resolves against the same context the
                // data rows do. What it RESOLVED TO classifies the slot: a
                // ground term is an unnamed output position by the slot
                // vocabulary's own law; anything else constrains and is
                // consumed.
                Judgment::Constrains(term) => {
                    let resolved = resolve(term)?;
                    let slot = match &resolved {
                        ast_resolved::DomainExpression::Application(
                            ast_resolved::FunctionApplication::Ground(_),
                        ) => AnonymousSlot::Literal {
                            position,
                            declared_type,
                            shape,
                        },
                        ast_resolved::DomainExpression::Application(_) => {
                            AnonymousSlot::Constraint {
                                position,
                                declared_type,
                                shape,
                            }
                        }
                        other => {
                            return Err(Internal::invariant(
                                "resolver::lexical::anonymous",
                                format!(
                                    "a computed anonymous header resolved to a non-application: {other:?}"
                                ),
                            ))
                        }
                    };
                    slots.push((slot, None));
                    born.push(BornPosition::Constrains(resolved));
                }
            }
        }
        let relation = AnonymousSpec::born_judged(planning, slots, answers_to, JudgedBirth(()))?;
        Ok(Born {
            relation,
            positions: born,
        })
    }
}

impl Position<'_> {
    /// THE HEADER ROW, JUDGED HERE. `header` is the authored header row
    /// (`_` is the disregarded slot), `rows` the authored grid beneath it
    /// and `resolved_rows` the same grid resolved, `alias` the relation's
    /// own name when it has one, `planning` the authority every reuse port
    /// belongs to. Every reuse edge in the answer was decided by this
    /// position's own lookup over the row standing under the reader's
    /// finger, and the answer is born only on that authority under that
    /// name.
    pub(crate) fn judge_anonymous_header<'r>(
        &self,
        header: &TabularRow<ast_unresolved::HeaderItem>,
        rows: &Vec1<TabularRow<ast_unresolved::Datum>>,
        resolved_rows: &Vec1<TabularRow<ast_resolved::Datum>>,
        alias: Option<&SqlIdentifier>,
        planning: &'r Planning,
    ) -> Result<AnonymousBirth<'r>> {
        let answers_to = alias.map(|alias| planning.intern(alias.as_str(), alias.is_stropped()));
        // THE ROW A HEADER MAY REUSE is the relation this one is composed
        // with — the frame the join entered before resolving its member.
        // A relation standing first, or in a closed world, composes with
        // nothing and reuses nothing.
        let row = self.local_ports(planning)?;
        let mut seen: Vec<(Occurrence, usize)> = Vec::new();
        let mut positions = Vec::with_capacity(header.len());
        for (index, item) in header.iter().enumerate() {
            if matches!(item.slot, Slot::Anon) {
                positions.push(Judgment::Disregards);
                continue;
            }
            let term = item.term().ok_or_else(|| {
                Internal::invariant(
                    "resolver::lexical::anonymous",
                    "a tabular header slot has a domain term",
                )
            })?;
            let ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                column,
            ))) = &term
            else {
                positions.push(Judgment::Constrains(term));
                continue;
            };
            let occurrence = Occurrence::of(column, planning);
            // A HEADER IS NOT A CELL OF ITS OWN GRID: the header names a
            // position, a cell reading the same complete occurrence would
            // make that position's constraint vacuous. Judged by identity,
            // so `q.a` beside a cell `r.a` is two variables and stands.
            if rows.iter().any(|row| {
                row.iter()
                    .filter_map(cell_occurrence)
                    .any(|cell| Occurrence::of(cell, planning) == occurrence)
            }) {
                return Err(DelightQLError::from(AnonBinding::HeaderRowLvar {
                    message: format!(
                        "lvar '{}' appears both as a header and in the data rows of the same anonymous table",
                        spelled(column)
                    ),
                }));
            }
            // A BINDER IS A VARIABLE, and only variables unify: a stropped
            // leaf is an authored NAME, so repeating one is a name
            // collision (minted apart), not a self-unification. The
            // variable's identity is its complete occurrence.
            if !column.name.is_stropped() {
                if let Some(&(_, first)) = seen.iter().find(|(spelt, _)| *spelt == occurrence) {
                    positions.push(Judgment::Repeats { first });
                    continue;
                }
                seen.push((occurrence, index));
            }
            let reuses = match &column.qualifier {
                // A qualified header REUSES an existing position of the
                // row, decided by the one address judgment every qualified
                // spelling meets; a spelling the row does not answer names
                // no owner, and a header cannot create one.
                Some(_) => Some(self.qualified_header_reuse(column, &row, planning)?),
                // A bare header of an UNNAMED relation reuses by the one
                // bare-reuse law. Under an alias the header is the alias's
                // own qualified variable: nothing bare to reuse.
                None if answers_to.is_none() => live_bare_reuse(&row, &column.name, planning)?,
                None => None,
            };
            positions.push(Judgment::Binds {
                named: planning.intern(column.name.as_str(), column.name.is_stropped()),
                reuses,
            });
        }
        // The lookups above are the terminal judgments this birth spends;
        // the proof is minted where they were made.
        let _judged: Terminal = Terminal::judged();
        Ok(AnonymousBirth {
            planning,
            answers_to,
            positions,
            // Literal-grid type inference: the rows are the columns'
            // declaration, read here so no caller assembles the facts.
            declared_types: infer_anon_column_types(resolved_rows),
            shapes: infer_anon_column_shapes(resolved_rows),
        })
    }

    /// THE EXACTLY-ONE POSITION OF THE ROW A QUALIFIED HEADER REUSES. The
    /// address judgment answers the occurrence; the landing is judged over
    /// the complete row — the position standing where the answer stood,
    /// and refusing when none does or more than one does.
    fn qualified_header_reuse(
        &self,
        column: &AuthoredColumn,
        row: &[PortId],
        registry: &Planning,
    ) -> Result<PortId> {
        let no_owner = || {
            DelightQLError::from(AnonBinding::Qualifier {
                message: format!(
                    "anonymous-table header '{}' names no column in scope",
                    spelled(column)
                ),
            })
        };
        let not_exact = |what: &str| {
            DelightQLError::from(Resolution::CorrespondenceNotExact {
                message: format!("anonymous-table header '{}' {what}", spelled(column)),
            })
        };
        let reference = ColumnReference::Named {
            name: column.name.clone(),
            qualifier: column.qualifier.clone(),
        };
        match self.address(reference, false, registry)? {
            UnificationResult::Resolved(addressed) => {
                // The answer must stand IN THE ROW this relation is
                // composed with: a position an enclosing row publishes is
                // not one this composition can merge.
                let landed: Vec<PortId> = row
                    .iter()
                    .copied()
                    .filter(|port| {
                        *port == addressed.column
                            || crate::relation::stands_where(registry, *port, addressed.column)
                    })
                    .collect();
                match landed.as_slice() {
                    [] => Err(no_owner()),
                    [one] => Ok(*one),
                    _ => Err(not_exact(
                        "stands at more than one position of the row it is composed with",
                    )),
                }
            }
            UnificationResult::Unresolved(_) => Err(no_owner()),
            UnificationResult::Ambiguous { .. } => {
                Err(not_exact("does not select exactly one position"))
            }
            UnificationResult::Opaque => Err(crate::pipeline::resolver::opaque_reference_refusal()),
            UnificationResult::Refused(refusal) => Err(refusal),
        }
    }
}

#[cfg(test)]
mod tests {
    //! THE BIRTH OWNS ITS CONTEXT. What ownership already states — one
    //! consuming `born(self)`, no `Clone`, no signature that takes a
    //! judged reuse beside a slot, an alias after the judgment, or a second
    //! authority at the birth — the compiler enforces; these witnesses pin
    //! what the born relation records under each context the carrier
    //! owns: the reuse edge under a bare judgment, its absence under an
    //! alias, the grid's facts at their own positions, and the slot a
    //! computed header takes from what it resolved to.
    use super::*;
    use crate::names::Addressing;
    use crate::pipeline::asts::core::{
        AnonTable, DomainExpression, FunctionApplication, LiteralValue, Resolved, Unresolved,
    };
    use crate::relation::form::AnonymousShape;

    fn planning() -> Planning {
        Planning::open(crate::names::Registry::new(&[]))
    }

    fn reference(name: &str) -> DomainExpression<Unresolved> {
        DomainExpression::Reference(Reference::Named(NamedReference(AuthoredColumn {
            name: SqlIdentifier::new(name),
            qualifier: None,
            namespace_path: Default::default(),
        })))
    }

    fn number<P: crate::pipeline::asts::core::Phase>(text: &str) -> DomainExpression<P> {
        DomainExpression::Application(FunctionApplication::Ground(LiteralValue::Number(
            text.to_string(),
        )))
    }

    fn text<P: crate::pipeline::asts::core::Phase>(value: &str) -> DomainExpression<P> {
        DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(
            value.to_string(),
        )))
    }

    /// A row publishing bare `a`, standing under the reader's finger.
    fn standing_over_bare_a(planning: &Planning) -> (Position<'static>, SemanticRelation) {
        let slots = [AnonymousSlot::Binder {
            position: 0,
            named: planning.intern("a", false),
            declared_type: None,
            shape: crate::names::ValueShape::Unknown,
        }];
        let row = super::super::ResolvedRelation::declared_row(
            AnonymousSpec::plain(AnonymousShape::Tabular, &slots, None),
            planning,
        )
        .expect("a declared row derives");
        let relation = row.semantic_relation();
        let mut position = Position::root();
        position.enter(row, super::super::Reach::Row);
        (position, relation)
    }

    fn resolve_ground(term: DomainExpression<Unresolved>) -> Result<DomainExpression<Resolved>> {
        match term {
            DomainExpression::Application(FunctionApplication::Ground(value)) => Ok(
                DomainExpression::Application(FunctionApplication::Ground(value)),
            ),
            other => panic!("the witness resolves ground terms only: {other:?}"),
        }
    }

    fn born(
        planning: &Planning,
        position: &Position<'_>,
        headers: Vec<DomainExpression<Unresolved>>,
        cells: Vec<DomainExpression<Unresolved>>,
        alias: Option<&str>,
    ) -> Born {
        let resolved_cells: Vec<DomainExpression<Resolved>> = cells
            .iter()
            .cloned()
            .map(|cell| resolve_ground(cell).expect("a ground cell resolves"))
            .collect();
        let authored = AnonTable::<Unresolved>::from_values(Some(headers), vec![cells])
            .expect("a nonempty grid");
        let resolved = AnonTable::<Resolved>::from_values(None, vec![resolved_cells])
            .expect("a nonempty grid");
        let alias = alias.map(SqlIdentifier::new);
        position
            .judge_anonymous_header(
                authored.header().expect("headers were written"),
                authored.rows(),
                resolved.rows(),
                alias.as_ref(),
                planning,
            )
            .expect("the header judges")
            .born(resolve_ground)
            .expect("the judged header is born")
    }

    /// A bare header of an UNNAMED relation reuses the live bare port, and
    /// the born relation's mint recorded exactly that edge.
    #[test]
    fn a_bare_header_is_born_reusing_the_live_bare_port() {
        let planning = planning();
        let (position, row) = standing_over_bare_a(&planning);
        let born = born(
            &planning,
            &position,
            vec![reference("a")],
            vec![number("2")],
            None,
        );
        let pairs = crate::relation::recorded_correspondence(&planning, &row, &born.relation)
            .expect("the record reads");
        assert_eq!(pairs.len(), 1, "one reuse edge, the judged one");
        let ports = crate::relation::published_ports(&planning, &born.relation).unwrap();
        assert_eq!(pairs[0].right, ports[0]);
        assert_eq!(planning.addressing(ports[0].column()), Addressing::Bare);
        assert!(matches!(born.positions.as_slice(), [BornPosition::Binds]));
    }

    /// THE NAME IS THE JUDGMENT'S: judged under an alias, the same header
    /// reuses nothing and is born qualifier-owned — there is no later act
    /// at which a name could be chosen for a bare judgment.
    #[test]
    fn an_aliased_header_is_born_qualified_and_reuses_nothing() {
        let planning = planning();
        let (position, row) = standing_over_bare_a(&planning);
        let born = born(
            &planning,
            &position,
            vec![reference("a")],
            vec![number("2")],
            Some("q"),
        );
        let pairs = crate::relation::recorded_correspondence(&planning, &row, &born.relation)
            .expect("the record reads");
        assert!(
            pairs.is_empty(),
            "an alias qualifies the header before any reuse"
        );
        let ports = crate::relation::published_ports(&planning, &born.relation).unwrap();
        assert_eq!(
            planning.addressing(ports[0].column()),
            Addressing::BareUnder
        );
        assert_eq!(
            planning.answers_to(born.relation.scope()),
            Some(planning.canonical(planning.intern("q", false)))
        );
    }

    /// THE GRID'S FACTS STAND AT THEIR OWN POSITIONS: the birth reads them
    /// from the rows it was judged over, so no caller can install one
    /// position's type at another.
    #[test]
    fn the_grids_facts_are_born_at_their_own_positions() {
        let planning = planning();
        let position = Position::root();
        let born = born(
            &planning,
            &position,
            vec![reference("n"), reference("s")],
            vec![number("2"), text("x")],
            None,
        );
        let ports = crate::relation::published_ports(&planning, &born.relation).unwrap();
        assert_eq!(
            planning.facts(ports[0].column()).declared_type.as_deref(),
            Some("INTEGER")
        );
        assert_eq!(
            planning.facts(ports[1].column()).declared_type.as_deref(),
            Some("TEXT")
        );
    }

    /// A CONSTRAINING HEADER TAKES ITS SLOT FROM WHAT IT RESOLVED TO: a
    /// ground term is an unnamed output position; the classification is
    /// the birth's, made from the resolved term, not a fact a caller
    /// states beside the slot.
    #[test]
    fn a_ground_header_is_born_as_an_unnamed_output_position() {
        let planning = planning();
        let position = Position::root();
        let born = born(
            &planning,
            &position,
            vec![reference("n"), number("3")],
            vec![number("2"), number("3")],
            None,
        );
        let ports = crate::relation::published_ports(&planning, &born.relation).unwrap();
        assert_eq!(
            planning.addressing(ports[1].column()),
            Addressing::Published
        );
        assert!(planning.published(ports[1].column()).is_none());
        assert!(matches!(
            born.positions.as_slice(),
            [BornPosition::Binds, BornPosition::Constrains(_)]
        ));
    }
}
