// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `form`: the one function that computes a heading, with one arm per
//! heading-forming construction (W5 #6). A heading is a function of its
//! operands' headings; a carried position copies its name state, so a lost
//! name stays lost and is never judged again.

use super::{Binding, Heading, Interior, Known, Layout, MintOrigin, Name, NameState, Origin, Position, Visibility};
use crate::pipeline::middle::core::ids::RelId;
use crate::pipeline::middle::core::node::HeaderSlot;
use crate::pipeline::middle::core::refuse::{self, Refusal};

/// The inputs of one heading-forming construction.
pub(crate) enum Formed<'a> {
    /// A catalog relation's columns, in catalog order, each with the
    /// structure its declaration gives its values.
    Catalog { columns: &'a [Name], structures: &'a [Interior] },
    /// A read of a source heading under an access.
    Read {
        source: &'a Heading,
        access: ReadForm<'a>,
    },
    /// An anonymous table: its header's slots, and each column's structure
    /// over its rows. A repeated binder publishes nothing.
    Lit {
        header: &'a [HeaderSlot],
        aliased: bool,
        structures: &'a [Interior],
    },
    /// One made record or tuple: its members in written order, a record's
    /// each under its key, with its structure.
    One {
        layout: Layout,
        members: &'a [(Option<Name>, Interior)],
    },
    /// An act's receipt: its declared columns in order, each a constant of
    /// the act or a relation it carries as an interior value, whose heading
    /// is that position's interior.
    Receipt { columns: &'a [(&'a Name, Option<(&'a Heading, RelId)>)] },
    /// A definition family's clauses accumulated: every clause publishes
    /// one heading (heads-law: CLAUSE AGREEMENT; never first-wins).
    /// `closed`: every clause's head declares its positions (a listed head,
    /// a fact), so a disagreement is of arity or of name offers.
    Family {
        name: &'a str,
        clauses: &'a [&'a Heading],
        closed: bool,
    },
    /// A closed head's declared positions, read from the head alone (no
    /// body): a fact's header or a headerless fact row, or a listed rule
    /// head. A head is ordinary projection (heads-law SUPPLY IS
    /// ELABORATION), so each position is named once or abstains, and no
    /// two positions publish one name.
    Declared { name: &'a str, positions: &'a [Declared<'a>] },
    /// What one member contributes to its run: its relation's heading,
    /// under the member's binding.
    Member {
        rel: &'a Heading,
        binding: MemberBinding,
    },
    /// A run's output: each member's contributed heading, less the
    /// positions a merge consumed, with collisions marked.
    Run { members: &'a [RunMember<'a>] },
    /// A projection-like stage: the items' names in order, then the hidden
    /// positions it carries unchanged. `projection` says whether the stage
    /// is a projection (or an embed) rather than a reduction, a distinct or
    /// a collection, which only its refusal names.
    Items {
        items: &'a [ItemForm<'a>],
        carried: &'a [Position],
        projection: bool,
    },
    /// A stage appending passengers as hidden positions.
    Carry {
        input: &'a Heading,
        passengers: &'a [super::super::ids::PassengerId],
    },
    /// A read of a stored table marked as a mutation's source: the read's
    /// positions, then each part of its row locator as a hidden position.
    Marked {
        read: &'a Heading,
        locator: &'a [super::super::ids::PassengerId],
    },
    /// A cover: its input's positions, the covered ones now holding the
    /// values it wrote under their own names.
    /// A cover: the input's positions, each covered one holding the shape
    /// of the value that covers it.
    Cover {
        input: &'a Heading,
        covered: &'a [(usize, Interior)],
    },
    /// A stage that republishes its input unchanged except for dropped
    /// positions.
    Without {
        input: &'a Heading,
        dropped: &'a [usize],
    },
    /// Its input's heading, unchanged.
    Same { input: &'a Heading },
    /// A signed witness: its operand's displayed positions, then `met`.
    Witnessed { input: &'a Heading },
    /// A definition instance's output: its body's heading across the
    /// instance's read boundary (json-substrate-law: a definition carries a
    /// scalar cell, not an extracted value's kind evidence).
    Instance { body: &'a Heading },
    /// An act's staging of a relation its continuation or property reads:
    /// its input's heading, every position judged at the staging boundary.
    Staged { input: &'a Heading },
    /// A set operation's result.
    SetOp {
        left: &'a Heading,
        right: &'a Heading,
        alignment: &'a [(Option<usize>, Option<usize>)],
    },
    /// A reflection's fixed heading.
    Meta,
    /// The rows of a structured value: its interior's positions, reached
    /// through the drill's qualifier.
    Unnest { interior: &'a Heading },
}

pub(crate) enum ReadForm<'a> {
    All,
    Unasked,
    Slots(&'a [Option<Name>]),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MemberBinding {
    /// Keep the relation's own binding (a read or an anonymous table).
    Own,
    /// Positions are reached through the member's qualifier.
    Qualified,
    /// Positions are bare binders.
    Bare,
}

pub(crate) struct RunMember<'a> {
    pub(crate) heading: &'a Heading,
    pub(crate) merged_away: &'a [usize],
    /// The member's positions a merged key stands at, each with the
    /// structure the merge decided.
    pub(crate) met: &'a [(usize, Interior)],
}

pub(crate) enum ItemForm<'a> {
    /// `expr as name`: the value published under the name.
    Baptized(&'a Name),
    /// A reference: the referenced position's name state is carried.
    Carried(&'a Position),
    /// A glob's position: carried, and a collision with another glob
    /// position poisons both rather than refusing.
    Globbed(&'a Position),
    /// A computed value nobody named.
    Computed,
    /// A head's ground term that abstains from naming its position.
    Abstained,
    /// A value with a decided structure, and the passenger carrying its
    /// node where the stage carries one.
    Structured {
        name: Option<&'a Name>,
        interior: Interior,
        node: Option<super::super::ids::PassengerId>,
    },
}

/// A fact-only family's heading with each displayed position nobody names
/// published under the canonical fact name `f|N|`, `N` its displayed
/// ordinal.
pub(crate) fn fact_named(heading: &Heading, family: &str) -> Heading {
    let mut ordinal = 0;
    Heading::of(
        heading
            .positions()
            .iter()
            .map(|p| {
                if matches!(p.visibility, Visibility::Hidden(_)) {
                    return p.clone();
                }
                ordinal += 1;
                match p.name {
                    NameState::Minted(MintOrigin::Abstained) => Position {
                        name: NameState::Authored(Name::new(format!("{family}|{ordinal}|"))),
                        ..p.clone()
                    },
                    _ => p.clone(),
                }
            })
            .collect(),
    )
}

/// Compute the heading of one construction.
pub(crate) fn form(formed: Formed<'_>) -> Result<Heading, Refusal> {
    match formed {
        Formed::Catalog { columns, structures } => Ok(Heading::of(
            columns
                .iter()
                .zip(structures)
                .map(|(name, structure)| Position {
                    interior: structure.clone(),
                    ..Position::published(NameState::Catalog(name.clone()), Binding::Qualified)
                })
                .collect(),
        )),
        Formed::Read { source, access } => read(source, access),
        Formed::Lit {
            header,
            aliased,
            structures,
        } => lit(header, aliased, structures),
        Formed::One { layout, members } => Ok(Heading::of(
            members
                .iter()
                .map(|(key, structure)| Position {
                    interior: structure.clone(),
                    ..Position::published(
                        match (layout, key) {
                            (Layout::Record, Some(key)) => NameState::Authored(key.clone()),
                            (Layout::Record, None) | (Layout::Tuple, _) => NameState::Minted(MintOrigin::Expr),
                        },
                        Binding::Bare,
                    )
                })
                .collect(),
        )),
        Formed::Receipt { columns } => Ok(Heading::of(
            columns
                .iter()
                .map(|(name, carried)| Position {
                    interior: match carried {
                        Some((heading, rel)) => {
                            Interior::made_with(Known::Shape(Box::new((*heading).clone()), Origin::Carried(*rel)))
                        }
                        None => Interior::flat(),
                    },
                    ..Position::published(NameState::Authored((*name).clone()), Binding::Bare)
                })
                .collect(),
        )),
        Formed::Family { name, clauses, closed } => family(name, clauses, closed),
        Formed::Declared { name, positions } => declared(name, positions),
        Formed::Member { rel, binding } => Ok(Heading::of(
            rel.positions()
                .iter()
                .map(|p| Position {
                    binding: match binding {
                        MemberBinding::Own => p.binding,
                        MemberBinding::Qualified => Binding::Qualified,
                        MemberBinding::Bare => Binding::Bare,
                    },
                    ..p.clone()
                })
                .collect(),
        )),
        Formed::Run { members } => Ok(run(members)),
        Formed::Items {
            items,
            carried,
            projection,
        } => {
            let mut positions = items_heading(items, projection)?;
            positions.extend(carried.iter().cloned());
            Ok(Heading::of(positions))
        }
        Formed::Carry { input, passengers } => {
            let mut positions: Vec<Position> = input.positions().iter().map(dequalified).collect();
            for p in passengers {
                positions.push(Position {
                    visibility: Visibility::Hidden(*p),
                    ..Position::published(NameState::Minted(MintOrigin::Expr), Binding::Bare)
                });
            }
            Ok(Heading::of(positions))
        }
        Formed::Marked { read, locator } => {
            let mut positions = read.positions().to_vec();
            positions.extend(locator.iter().map(|part| Position {
                visibility: Visibility::Hidden(*part),
                ..Position::published(NameState::Minted(MintOrigin::Expr), Binding::Bare)
            }));
            Ok(Heading::of(positions))
        }
        Formed::Cover { input, covered } => Ok(Heading::of(
            input
                .positions()
                .iter()
                .enumerate()
                .map(|(i, p)| match covered.iter().find(|(target, _)| *target == i) {
                    Some((_, interior)) => Position {
                        interior: interior.clone(),
                        node: None,
                        ..dequalified(p)
                    },
                    None => dequalified(p),
                })
                .collect(),
        )),
        Formed::Without { input, dropped } => Ok(Heading::of(
            input
                .positions()
                .iter()
                .enumerate()
                .filter(|(i, _)| !dropped.contains(i))
                .map(|(_, p)| dequalified(p))
                .collect(),
        )),
        Formed::Same { input } => Ok(Heading::of(
            input.positions().iter().map(dequalified).collect(),
        )),
        Formed::Witnessed { input } => Ok(Heading::of(
            input
                .displayed()
                .map(|(_, p)| dequalified(p))
                .chain(std::iter::once(Position::published(
                    NameState::Authored(Name::new("met")),
                    Binding::Bare,
                )))
                .collect(),
        )),
        Formed::Instance { body } => Ok(Heading::of(
            body.positions().iter().map(|p| dequalified(&severed(p))).collect(),
        )),
        Formed::SetOp {
            left,
            right,
            alignment,
        } => Ok(set_op(left, right, alignment)),
        Formed::Meta => Ok(Heading::of(
            ["scope", "column_name", "ordinal"]
                .into_iter()
                .map(|n| Position::published(NameState::Authored(Name::new(n)), Binding::Bare))
                .collect(),
        )),
        Formed::Unnest { interior } => Ok(Heading::of(
            interior
                .positions()
                .iter()
                .map(|p| Position {
                    binding: Binding::Qualified,
                    ..p.clone()
                })
                .collect(),
        )),
        Formed::Staged { input } => {
            let mut positions = Vec::with_capacity(input.len());
            for p in input.positions() {
                positions.push(Position {
                    interior: crate::pipeline::middle::core::decide::document::staged(&p.interior)?,
                    ..dequalified(p)
                });
            }
            Ok(Heading::of(positions))
        }
    }
}

/// A pipe form publishes a dequalified result: every position becomes a
/// bare binder under its own name state.
fn dequalified(p: &Position) -> Position {
    Position {
        binding: Binding::Bare,
        ..p.clone()
    }
}

/// A read is a read boundary: a value read across it keeps its cell, and
/// what is known of its kind is the boundary's judgment; no node crosses.
fn severed(p: &Position) -> Position {
    Position {
        interior: crate::pipeline::middle::core::decide::document::read(&p.interior),
        node: None,
        ..p.clone()
    }
}

fn read(source: &Heading, access: ReadForm<'_>) -> Result<Heading, Refusal> {
    let source = &Heading::of(source.positions().iter().map(severed).collect());
    match access {
        ReadForm::All => Ok(Heading::of(
            source
                .positions()
                .iter()
                .map(|p| Position {
                    binding: Binding::Qualified,
                    visibility: match p.visibility {
                        Visibility::Latent => Visibility::Published,
                        Visibility::Published => Visibility::Published,
                        Visibility::Hidden(h) => Visibility::Hidden(h),
                    },
                    ..p.clone()
                })
                .collect(),
        )),
        ReadForm::Unasked => Ok(Heading::of(
            source
                .positions()
                .iter()
                .map(|p| Position {
                    binding: Binding::Qualified,
                    visibility: match p.visibility {
                        Visibility::Hidden(h) => Visibility::Hidden(h),
                        Visibility::Published | Visibility::Latent => Visibility::Latent,
                    },
                    ..p.clone()
                })
                .collect(),
        )),
        ReadForm::Slots(slots) => {
            let displayed: Vec<&Position> = source.displayed().map(|(_, p)| p).collect();
            if slots.len() != displayed.len() {
                return Err(refuse::arity(slots.len(), displayed.len()));
            }
            let mut positions: Vec<Position> = Vec::new();
            let mut bound: Vec<&Name> = Vec::new();
            // A slot republishes the position under the slot's name, with
            // the shape the position holds; a name bound again unifies, and
            // its position holds both positions' structures met.
            for (slot, source_position) in slots.iter().zip(&displayed) {
                let Some(name) = slot else {
                    continue;
                };
                match bound.iter().position(|b| *b == name) {
                    Some(first) => {
                        positions[first].interior = Interior::meet(&positions[first].interior, &source_position.interior);
                    }
                    None => {
                        bound.push(name);
                        positions.push(Position {
                            interior: source_position.interior.clone(),
                            ..Position::published(NameState::Authored(name.clone()), Binding::Bare)
                        });
                    }
                }
            }
            // A positional access reads its source's rows one for one, so
            // the passengers they carry travel with them.
            positions.extend(
                source
                    .positions()
                    .iter()
                    .filter(|p| matches!(p.visibility, Visibility::Hidden(_)))
                    .cloned(),
            );
            Ok(Heading::of(positions))
        }
    }
}

fn lit(header: &[HeaderSlot], aliased: bool, structures: &[Interior]) -> Result<Heading, Refusal> {
    let binding = if aliased {
        Binding::Qualified
    } else {
        Binding::Bare
    };
    // A repeated binder unifies with its first column, whose position holds
    // both columns' structures met.
    let mut structures: Vec<Interior> = (0..header.len())
        .map(|k| structures.get(k).cloned().unwrap_or_else(Interior::flat))
        .collect();
    for (k, slot) in header.iter().enumerate() {
        if let HeaderSlot::Reuse { first, .. } = slot {
            structures[*first] = Interior::meet(&structures[*first], &structures[k]);
        }
    }
    let mut positions = Vec::new();
    for (k, slot) in header.iter().enumerate() {
        let interior = structures[k].clone();
        match slot {
            HeaderSlot::Reuse { .. } | HeaderSlot::Disregard | HeaderSlot::Constraint { .. } => {}
            HeaderSlot::Bind(name) => {
                positions.push(Position {
                    interior,
                    ..Position::published(NameState::Authored(name.clone()), binding)
                });
            }
            HeaderSlot::Anon => positions.push(Position {
                interior,
                ..Position::published(NameState::Minted(MintOrigin::Anon), binding)
            }),
        }
    }
    Ok(Heading::of(positions))
}

/// The spelling a position publishes for clause agreement: its answering
/// name, or nothing (a heading is names in order; a position answering to
/// no name publishes none).
fn published(p: &Position) -> String {
    p.answering_name().map(|n| n.to_string()).unwrap_or_else(|| "_".to_string())
}

/// A name as its author wrote it, the strop kept, for teaching.
fn written(name: &Name) -> String {
    match name.is_stropped() {
        true => format!("`{name}`"),
        false => name.to_string(),
    }
}

/// What one position of a closed head declares, as the head wrote it.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Declared<'a> {
    /// A name: a reference, or a label.
    Names(&'a Name),
    /// An unlabeled ground term: it supplies and abstains from naming.
    Abstains,
    /// A slot that neither names nor supplies (`_`, a constraint term): in
    /// a projection it would leave its position nameless and valueless.
    NamesNothing,
}

/// A closed head's heading from its declared positions alone: a slot that
/// names nothing refuses; two positions naming one identifier refuse (a
/// heading names each column once, by the identifier law).
fn declared(name: &str, positions: &[Declared<'_>]) -> Result<Heading, Refusal> {
    let mut out: Vec<Position> = Vec::with_capacity(positions.len());
    for (k, position) in positions.iter().enumerate() {
        let state = match position {
            Declared::NamesNothing => return Err(refuse::head_names_nothing(name, k + 1)),
            Declared::Abstains => NameState::Minted(MintOrigin::Abstained),
            Declared::Names(n) => {
                if let Some(earlier) = positions[..k].iter().position(|p| matches!(p, Declared::Names(e) if *e == *n)) {
                    return Err(refuse::head_name_collision(name, earlier + 1, k + 1, &written(n)));
                }
                NameState::Authored((*n).clone())
            }
        };
        out.push(Position::published(state, Binding::Bare));
    }
    Ok(Heading::of(out))
}

/// A family's heading: every clause publishes the same names in order, or
/// the family refuses. Positions no name answers to agree with each other,
/// and a clause's ground term that abstains agrees with the name another
/// clause offers there, which names the position (one name offered, others
/// abstain: the offered name). A hidden position survives only where every
/// clause carries the same one; a position's interior is per clause where
/// the clauses' differ.
fn family(name: &str, clauses: &[&Heading], closed: bool) -> Result<Heading, Refusal> {
    let Some(first) = clauses.first() else {
        return Ok(Heading::default());
    };
    let abstains = |p: &Position| p.name == NameState::Minted(MintOrigin::Abstained);
    let spell = |h: &Heading| -> Vec<String> { h.displayed().map(|(_, p)| published(p)).collect() };
    let offer = |p: &Position| p.answering_name().map(written).unwrap_or_else(|| "_".to_string());
    let width = first.displayed().count();
    // The name each position publishes: the one its offering clauses agree
    // on, else none.
    let mut offered: Vec<Option<&Position>> = vec![None; width];
    for (index, clause) in clauses.iter().enumerate() {
        let positions: Vec<&Position> = clause.displayed().map(|(_, p)| p).collect();
        if positions.len() != width {
            return Err(match closed {
                true => refuse::head_arity(name, width, index + 1, positions.len()),
                false => refuse::clause_disagreement(name, &spell(first), index + 1, &spell(clause)),
            });
        }
        for (k, p) in positions.into_iter().enumerate() {
            if abstains(p) {
                continue;
            }
            // Offers agree by identifier (identifier law), not by spelling.
            match offered[k] {
                Some(earlier) if earlier.answering_name() != p.answering_name() => {
                    return Err(match closed {
                        true => refuse::head_name_conflict(name, k + 1, &offer(earlier), &offer(p), index + 1),
                        false => refuse::clause_disagreement(name, &spell(first), index + 1, &spell(clause)),
                    });
                }
                Some(_) => {}
                None => offered[k] = Some(p),
            }
        }
    }
    let expected: Vec<String> = (0..width)
        .map(|k| offered[k].map(published).unwrap_or_else(|| "_".to_string()))
        .collect();
    let hidden_agree = clauses.iter().all(|c| {
        c.positions()
            .iter()
            .filter(|p| matches!(p.visibility, Visibility::Hidden(_)))
            .map(|p| &p.visibility)
            .eq(first.positions().iter().filter(|p| matches!(p.visibility, Visibility::Hidden(_))).map(|p| &p.visibility))
    });
    let displayed: Vec<Vec<&Position>> = clauses.iter().map(|c| c.displayed().map(|(_, p)| p).collect()).collect();
    let mut positions: Vec<Position> = (0..expected.len())
        .map(|i| {
            let arms: Vec<&Position> = displayed.iter().map(|d| d[i]).collect();
            let interior = arms_interior(&arms.iter().map(|p| &p.interior).collect::<Vec<_>>());
            let base = offered[i].unwrap_or(arms[0]);
            let naming: Vec<&Position> = arms.iter().copied().filter(|p| !abstains(p)).collect();
            Position {
                name: if naming.iter().all(|p| p.name == base.name) {
                    base.name.clone()
                } else {
                    match base.answering_name() {
                        Some(n) => NameState::Authored(n.clone()),
                        None => NameState::Minted(MintOrigin::Expr),
                    }
                },
                interior,
                ..dequalified(base)
            }
        })
        .collect();
    if hidden_agree {
        positions.extend(first.positions().iter().filter(|p| matches!(p.visibility, Visibility::Hidden(_))).cloned());
    }
    Ok(Heading::of(positions))
}

/// A relation-shaped known heading as one arm's (or several arms')
/// headings with their origins; an arm knowing none holds none. A union's
/// rows are nobody's rows: an arm's interior carries no passenger.
fn per_arm(known: &Known) -> Vec<(Heading, Option<Origin>)> {
    let unpassengered = |h: &Heading| {
        Heading::of(h.positions().iter().filter(|p| !matches!(p.visibility, Visibility::Hidden(_))).cloned().collect())
    };
    match known {
        Known::Shape(h, origin) => vec![(unpassengered(h), Some(*origin))],
        Known::PerArm(hs) => hs.iter().map(|(h, o)| (unpassengered(h), *o)).collect(),
        Known::None | Known::One(..) | Known::Keyed(_) => vec![(Heading::default(), None)],
    }
}

/// The structure a position holds where several arms (a set's, a family's
/// clauses) each supply it: the join of the arms' structures, every fact
/// kept; where relation-shaped headings differ and every other arm is an
/// ordinary value, one heading per arm.
fn arms_interior(arms: &[&Interior]) -> Interior {
    let Some(first) = arms.first() else {
        return Interior::flat();
    };
    let joined = arms.iter().skip(1).fold((*first).clone(), |acc, i| Interior::join(&acc, i));
    let relation = |i: &Interior| matches!(i.known, Known::Shape(..) | Known::PerArm(_));
    if joined.known == Known::None
        && arms.iter().any(|i| relation(i))
        && arms.iter().all(|i| relation(i) || i.null || **i == Interior::flat())
    {
        return Interior {
            known: Known::PerArm(arms.iter().flat_map(|i| per_arm(&i.known)).collect()),
            ..joined
        };
    }
    joined
}

fn run(members: &[RunMember<'_>]) -> Heading {
    let positions = members
        .iter()
        .flat_map(|m| {
            m.heading
                .positions()
                .iter()
                .enumerate()
                .filter(|(i, _)| !m.merged_away.contains(i))
                .map(|(i, p)| match m.met.iter().find(|(at, _)| *at == i) {
                    Some((_, structure)) => Position {
                        interior: structure.clone(),
                        ..p.clone()
                    },
                    None => p.clone(),
                })
        })
        .collect();
    Heading::of(mark_collisions(positions))
}

/// COLLIDING OUTPUTS POISON BOTH: every published position whose name
/// another published position also answers to loses it.
fn mark_collisions(mut positions: Vec<Position>) -> Vec<Position> {
    let names: Vec<Option<Name>> = positions.iter().map(|p| p.answering_name().cloned()).collect();
    for (i, p) in positions.iter_mut().enumerate() {
        if let Some(name) = &names[i] {
            let clashes = names
                .iter()
                .enumerate()
                .any(|(j, other)| j != i && other.as_ref() == Some(name));
            if clashes {
                p.name = NameState::Lost(name.clone());
            }
        }
    }
    positions
}

fn items_heading(items: &[ItemForm<'_>], projection: bool) -> Result<Vec<Position>, Refusal> {
    let mut positions = Vec::with_capacity(items.len());
    let mut authored: Vec<(usize, Name)> = Vec::new();
    let mut globbed: Vec<Name> = Vec::new();
    for item in items {
        let p = match item {
            ItemForm::Baptized(name) => {
                authored.push((positions.len(), (*name).clone()));
                Position::published(NameState::Authored((*name).clone()), Binding::Bare)
            }
            ItemForm::Carried(p) => {
                if let Some(name) = p.answering_name() {
                    authored.push((positions.len(), name.clone()));
                }
                dequalified_published(p)
            }
            ItemForm::Globbed(p) => {
                if let Some(name) = p.answering_name() {
                    globbed.push(name.clone());
                }
                dequalified_published(p)
            }
            ItemForm::Computed => {
                Position::published(NameState::Minted(MintOrigin::Expr), Binding::Bare)
            }
            ItemForm::Abstained => {
                Position::published(NameState::Minted(MintOrigin::Abstained), Binding::Bare)
            }
            ItemForm::Structured { name, interior, node } => Position {
                name: match name {
                    Some(name) => {
                        authored.push((positions.len(), (*name).clone()));
                        NameState::Authored((*name).clone())
                    }
                    None => NameState::Minted(MintOrigin::Expr),
                },
                visibility: Visibility::Published,
                binding: Binding::Bare,
                interior: interior.clone(),
                node: *node,
            },
        };
        positions.push(p);
    }
    for (i, (_, name)) in authored.iter().enumerate() {
        if authored[i + 1..].iter().any(|(_, other)| other == name) {
            return Err(refuse::duplicate_name(name, projection));
        }
        if globbed.contains(name) {
            return Err(refuse::glob_collision(name));
        }
    }
    Ok(mark_collisions(positions))
}

/// A carried position in a projection: its name state travels; a latent
/// dimension an ordinal reached publishes its origin name.
fn dequalified_published(p: &Position) -> Position {
    Position {
        binding: Binding::Bare,
        visibility: match p.visibility {
            Visibility::Hidden(h) => Visibility::Hidden(h),
            Visibility::Published | Visibility::Latent => Visibility::Published,
        },
        ..p.clone()
    }
}

fn set_op(left: &Heading, right: &Heading, alignment: &[(Option<usize>, Option<usize>)]) -> Heading {
    Heading::of(
        alignment
            .iter()
            .map(|(l, r)| {
                let lp = l.map(|i| &left.positions()[i]);
                let rp = r.map(|i| &right.positions()[i]);
                let base = lp.or(rp).expect("an aligned position has an arm");
                let interior = arms_interior(&[lp.map(|p| &p.interior), rp.map(|p| &p.interior)].into_iter().flatten().collect::<Vec<_>>());
                Position {
                    interior,
                    ..dequalified(base)
                }
            })
            .collect(),
    )
}
