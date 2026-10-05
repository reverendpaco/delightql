// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Structured values: what a declared column may hold, what a path
//! addresses, what an expansion binds, what a constructor may take as a
//! member, what crosses a read boundary, and which comparisons of
//! structures the law answers. Each judgment reads the decided structures
//! (`Interior`) of its operands and the facts its caller states, nothing
//! else.

use crate::pipeline::middle::core::heading::{Evidence, Heading, Interior, Known, Layout, Made, Name, Origin};
use crate::pipeline::middle::core::node::Consumer;
use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::facade::{CmpOp, DeclaredClass, Path, PathStep};

/// THE DOCUMENT DENYLIST: the structure a catalog column's declaration
/// gives its values. A declaration the target's type vocabulary classes
/// numeric, date or boolean holds no document; everything else (textual,
/// unknown, undeclared) may hold one. A column the catalog records as
/// holding a stored nested relation holds the document the language made
/// (a snapshot is read like any table, and identity never changes a value);
/// its heading is not recorded, and no stored write admits a minted key.
pub(crate) fn declared(class: Option<DeclaredClass>, stored: bool) -> Interior {
    match stored {
        true => Interior::made_with(Known::None),
        false => Interior::declared(class.is_some()),
    }
}

/// THE READ BOUNDARY (dynamic structure does not survive an untyped read
/// boundary): what is known of a value read across a definition's or CTE's
/// body read, a relation formal's actual, a recursive frontier, a relation
/// definition's value formal, or an act's staging. The value crosses; an
/// extracted value's kind does not, and nothing else changes.
pub(crate) fn read(structure: &Interior) -> Interior {
    Interior {
        evidence: match structure.evidence {
            Evidence::Decided | Evidence::Member => structure.evidence,
            Evidence::Reached | Evidence::Extracted | Evidence::Severed => Evidence::Severed,
        },
        ..structure.clone()
    }
}

/// An act's staging that its continuation or its property reads: a read
/// boundary, conservative at the boundary itself, so a value whose kind no
/// evidence proves is not admitted.
pub(crate) fn staged(structure: &Interior) -> Result<Interior, Refusal> {
    match structure.evidence {
        Evidence::Extracted | Evidence::Severed => Err(refuse::dynamic_staging()),
        Evidence::Decided | Evidence::Member | Evidence::Reached => Ok(read(structure)),
    }
}

/// The unruled question of what a binding of a member the language made
/// reads.
fn member_binding_unruled() -> Refusal {
    refuse::unruled(
        "whether an expansion binds a member of a document the language made as the value it was or as a node \
         of the document",
    )
}

/// A structured consumer of a value of this structure (a path step, a
/// pattern, an iteration): the read boundary, the member-binding question and the denylist refuse
/// here, before SQL.
pub(crate) fn consumed(source: &Interior, consumer: &str) -> Result<(), Refusal> {
    match source.evidence {
        Evidence::Severed => return Err(refuse::dynamic_continuity(consumer)),
        Evidence::Member => return Err(member_binding_unruled()),
        Evidence::Decided | Evidence::Reached | Evidence::Extracted => {}
    }
    if source.denied {
        return Err(refuse::scalar_column(consumer));
    }
    Ok(())
}

/// What a path reaches.
enum Reached {
    /// A member of a known structure (or the source itself, for no step).
    Member(Interior),
    /// A node of a document whose shape is data: the structure it is read
    /// out of.
    Node(Interior),
    /// A step the known structure does not have.
    Absent,
}

fn walk(source: &Interior, path: &Path) -> Result<Reached, Refusal> {
    let mut at = source.clone();
    for step in path.steps() {
        consumed(&at, "a path")?;
        if at.known == Known::None {
            return Ok(Reached::Node(at));
        }
        match step_into(&at, step)? {
            Some(member) => at = member,
            None => return Ok(Reached::Absent),
        }
    }
    Ok(Reached::Member(at))
}

/// What a path addresses in a value of structure `source`: the structure
/// of what it yields. Whatever a path reads out of a document is a node of
/// that document, so it is an extracted value whose kind each row observes,
/// unless the node is a structure the language made, which stays decided.
/// A step a known structure does not have reaches nothing: the path yields
/// NULL.
pub(crate) fn reach(source: &Interior, path: &Path) -> Result<Interior, Refusal> {
    Ok(match walk(source, path)? {
        Reached::Absent => Interior::flat(),
        Reached::Member(member) if member.always_made() => Interior {
            evidence: member.evidence.max(Evidence::Reached),
            ..member
        },
        Reached::Member(member) => member.node_in(),
        Reached::Node(from) => from.node_in(),
    })
}

/// What a pattern binds at `path` of an element of structure `element`:
/// over data, a node; over a known structure, the member it reaches, bound.
pub(crate) fn bind(element: &Interior, path: &Path) -> Result<Interior, Refusal> {
    Ok(match walk(element, path)? {
        Reached::Absent => Interior::flat(),
        Reached::Node(from) => from.node_in(),
        Reached::Member(member) => bound(member, &element.known),
    })
}

/// A member an expansion binds out of `over`, the known structure it reads
/// (the drill by the member's position, a pattern by its key). A column of
/// the rows a receipt carries (every arm's, where arms meet) is a staged
/// column, no document: it binds as it stands. A member of a document the
/// language made: a made member and a member bound before keep their
/// structure; an extracted member is a node; an ordinary member is the
/// value it went in as under the drill's reading and a node of the
/// document under the pattern's, which the member-binding question asks.
pub(crate) fn bound(member: Interior, over: &Known) -> Interior {
    let staged = match over {
        Known::Shape(_, origin) => matches!(origin, Origin::Carried(_)),
        Known::PerArm(arms) => arms.iter().all(|(_, o)| matches!(o, Some(Origin::Carried(_)))),
        Known::None | Known::One(..) | Known::Keyed(_) => false,
    };
    if staged {
        return member;
    }
    match (member.made, member.evidence) {
        (Made::Always, _) | (_, Evidence::Member) => member,
        (Made::Never, Evidence::Decided) => Interior {
            evidence: Evidence::Member,
            ..member
        },
        (Made::Never | Made::Sometimes, Evidence::Decided | Evidence::Reached | Evidence::Extracted | Evidence::Severed) => {
            member.node_in()
        }
    }
}

/// One step into a value of known structure `at`: the member's structure,
/// or `None` for a member it does not have.
fn step_into(at: &Interior, step: &PathStep) -> Result<Option<Interior>, Refusal> {
    let known = &at.known;
    match (known, step) {
        (Known::One(heading, Layout::Record), PathStep::Key(key)) => {
            let key = Name::new(key.clone());
            Ok(heading
                .positions()
                .iter()
                .find(|p| p.answering_name() == Some(&key))
                .map(|p| p.interior.clone()))
        }
        (Known::One(heading, Layout::Record), PathStep::Index(index)) => {
            // Unruled: whether `.0` also reaches a key spelled "0".
            let spelled = Name::new(index.to_string());
            if heading.positions().iter().any(|p| p.answering_name() == Some(&spelled)) {
                return Err(refuse::unruled(
                    "whether a numeric path step on a record whose key is that number's spelling reaches the key",
                ));
            }
            Ok(None)
        }
        (Known::One(heading, Layout::Tuple), PathStep::Index(index)) => Ok(position(heading.len(), *index)
            .and_then(|i| heading.positions().get(i))
            .map(|p| p.interior.clone())),
        (Known::One(_, Layout::Tuple), PathStep::Key(_)) => Ok(None),
        (Known::Shape(heading, origin), PathStep::Index(_)) => {
            Ok(Some(Interior::made_with(Known::One(heading.clone(), origin.layout()))))
        }
        (Known::PerArm(_), PathStep::Index(_)) => Ok(Some(Interior {
            minted: at.minted,
            ..Interior::made_with(Known::None)
        })),
        (Known::Shape(..) | Known::PerArm(_), PathStep::Key(_)) => Ok(None),
        (Known::Keyed(inner), PathStep::Key(_)) => Ok(Some((**inner).clone())),
        (Known::Keyed(_), PathStep::Index(_)) => Ok(None),
        (Known::None, _) => Ok(Some(at.node_in())),
    }
}

/// A position counted from the start, or from the end when negative.
fn position(len: usize, index: i64) -> Option<usize> {
    if index >= 0 {
        usize::try_from(index).ok().filter(|i| *i < len)
    } else {
        len.checked_sub(usize::try_from(index.unsigned_abs()).ok()?)
    }
}

/// Whether a value of this structure may be a member of a structure the
/// language makes (a record or tuple in value position, or collected). A
/// crossed truth is the target's own value there (the CROSSING LAW); an
/// extracted value read back across a read boundary carries no kind.
pub(crate) fn member(structure: &Interior) -> Result<(), Refusal> {
    match structure.evidence {
        Evidence::Severed => Err(refuse::dynamic_continuity("a constructor member")),
        Evidence::Decided | Evidence::Member | Evidence::Reached | Evidence::Extracted => Ok(()),
    }
}

/// A consumer that reads a value's bytes: a stored write, a call or cast,
/// an operator, a comparison, a grouping, distinct or ordering key, a
/// metadata key. A value that may hold a minted key has no bytes an answer
/// may depend on (MINTED SPELLINGS ARE INVARIANT). A consumer that reads
/// members by position or by an authored key (a drill, a path, a pattern, a
/// constructor nesting it) is not one, and neither is a release: a mint
/// inside a released value is data.
pub(crate) fn observed(structure: &Interior, consumer: &str) -> Result<(), Refusal> {
    if structure.minted {
        return Err(refuse::minted_bytes(consumer));
    }
    Ok(())
}

/// How set identity compares the values meeting at one position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Membership {
    /// The values are of one kind, so their bytes answer, a NULL equal to a
    /// NULL.
    Bytes,
    /// A structure the language made meets a value that is not one: they
    /// are never one member, whatever their bytes; only two NULLs are.
    Apart,
}

/// SET IDENTITY (equality-law rows 7–10): a grouping key, a distinct item
/// and the two sides of a minus step's aligned position compare values as
/// set members, NULL a value. Values of one kind compare by their bytes (a
/// value that may hold a minted key has none to compare). JSON-looking text
/// remains text in equality (json-substrate-law), so a made structure is
/// never the member a text value is: across a minus pair the two are
/// apart; within one position whose rows hold structures beside ordinary
/// values the bytes cannot tell them apart, which is not covered. A value
/// declared non-document (THE DOCUMENT DENYLIST) holds no text to confuse.
pub(crate) fn set_member(meeting: &[&Interior], consumer: &str) -> Result<Membership, Refusal> {
    for structure in meeting {
        observed(structure, consumer)?;
        if structure.made == Made::Sometimes {
            return Err(refuse::outside(
                "set identity over values that are made structures in some rows and ordinary values in others",
            ));
        }
    }
    let made = meeting.iter().any(|s| s.made == Made::Always);
    let text = meeting.iter().any(|s| s.made == Made::Never && !s.denied && s.evidence <= Evidence::Member);
    Ok(if made && text { Membership::Apart } else { Membership::Bytes })
}

/// What a compared value states of the NULLs inside it.
#[derive(Clone, Debug)]
pub(crate) enum Nulls {
    /// The NULL literal: the comparison asks whether the other is NULL.
    Null,
    /// Never NULL: a literal.
    Never,
    /// A structure made in the comparison itself: its members' NULLs, in
    /// its member heading's order.
    Members(Vec<Nulls>),
    /// Nothing stated: any member may be NULL.
    Unknown,
}

/// One operand of a comparison: its decided structure and its NULLs.
pub(crate) struct Compared<'a> {
    pub(crate) structure: &'a Interior,
    pub(crate) nulls: Nulls,
}

/// Whether the law answers a comparison of two values, wherever it stands
/// (a written comparison, a unification, a slot, a header or a case arm).
/// Values none of whose rows hold a structure compare as values. A
/// structure compared with a value that is not one depends on whether a
/// structure equals a non-structure; two records with the same keys in a
/// different order on whether key order is part of a record's value; a
/// member that may be NULL on whether structure equality is null-safe,
/// where the readings differ: for `=` consumed as a filter
/// and for the null-safe forms, where both sides may be NULL at one member;
/// elsewhere, where either may. An ordering of structures has no reading.
pub(crate) fn comparable(left: Compared<'_>, right: Compared<'_>, op: CmpOp, consumer: Consumer) -> Result<(), Refusal> {
    if matches!(left.nulls, Nulls::Null) || matches!(right.nulls, Nulls::Null) {
        return Ok(());
    }
    observed(left.structure, "a comparison")?;
    observed(right.structure, "a comparison")?;
    if left.structure.made == Made::Never && right.structure.made == Made::Never {
        return Ok(());
    }
    if !matches!(
        op,
        CmpOp::Equal | CmpOp::NotEqual | CmpOp::NullSafeEqual | CmpOp::NullSafeNotEqual
    ) {
        return Err(refuse::outside("an ordering comparison of structures"));
    }
    let both = matches!(op, CmpOp::NullSafeEqual | CmpOp::NullSafeNotEqual)
        || (op == CmpOp::Equal && matches!(consumer, Consumer::Filter | Consumer::Correlation));
    pair(left.structure, &left.nulls, right.structure, &right.nulls, both)
}

/// Judge one pair of compared values (whole operands, or members paired
/// by key or position).
fn pair(l: &Interior, ln: &Nulls, r: &Interior, rn: &Nulls, both: bool) -> Result<(), Refusal> {
    match (l.made, r.made) {
        (Made::Never, Made::Never) => {
            let depends = if both { may_null(ln) && may_null(rn) } else { may_null(ln) || may_null(rn) };
            return if depends { Err(null_member_unruled()) } else { Ok(()) };
        }
        (Made::Always, Made::Always) => {}
        _ => {
            return Err(refuse::unruled(
                "whether a structure equals a value that is not one, such as JSON-looking text",
            ))
        }
    }
    let (lh, ll, rh, rl) = match (&l.known, &r.known) {
        (Known::One(lh, ll), Known::One(rh, rl)) => (lh, *ll, rh, *rl),
        // A structure whose members no heading names: any of its members
        // may be NULL, and a record's keys may stand in any order. The
        // readings of null-safe structure equality differ only where the other side may hold a NULL
        // member too (or, where `both` is not asked, anywhere); otherwise a
        // record on the other side asks only whether key order is part of
        // its value.
        (Known::One(h, Layout::Record), Known::None) if both && !holds_null(h, ln) => return Err(key_order_unruled()),
        (Known::None, Known::One(h, Layout::Record)) if both && !holds_null(h, rn) => return Err(key_order_unruled()),
        _ => return Err(null_member_unruled()),
    };
    if ll != rl {
        return Ok(());
    }
    let lm = members(lh, ln);
    let rm = members(rh, rn);
    let pairs: Vec<(usize, usize)> = match ll {
        Layout::Tuple => {
            if lm.len() != rm.len() {
                return Ok(());
            }
            (0..lm.len()).map(|i| (i, i)).collect()
        }
        Layout::Record => {
            let keys = |h: &Heading| h.positions().iter().map(|p| p.answering_name().cloned()).collect::<Vec<_>>();
            let (lk, rk) = (keys(lh), keys(rh));
            let mut ls = lk.clone();
            let mut rs = rk.clone();
            ls.sort_by_key(|k| k.as_ref().map(|n| n.to_string()));
            rs.sort_by_key(|k| k.as_ref().map(|n| n.to_string()));
            if ls != rs {
                return Ok(());
            }
            if lk != rk {
                return Err(key_order_unruled());
            }
            (0..lk.len()).map(|i| (i, i)).collect()
        }
    };
    for (i, j) in pairs {
        pair(&lm[i].0, &lm[i].1, &rm[j].0, &rm[j].1, both)?;
    }
    Ok(())
}

/// A known structure's members with their NULLs: as the comparison states
/// them, or unknown.
fn members(heading: &Heading, nulls: &Nulls) -> Vec<(Interior, Nulls)> {
    heading
        .positions()
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let n = match nulls {
                Nulls::Members(ms) => ms.get(i).cloned().unwrap_or(Nulls::Unknown),
                Nulls::Null | Nulls::Never | Nulls::Unknown => Nulls::Unknown,
            };
            (p.interior.clone(), n)
        })
        .collect()
}

/// Whether a member value may be NULL.
fn may_null(n: &Nulls) -> bool {
    matches!(n, Nulls::Null | Nulls::Unknown)
}

/// Whether a NULL may stand at some member of a known structure, at any
/// depth its side states, as that side states its NULLs.
fn holds_null(heading: &Heading, nulls: &Nulls) -> bool {
    members(heading, nulls).iter().any(|(s, n)| match (&s.known, n) {
        (Known::One(h, _), Nulls::Members(_)) => holds_null(h, n),
        _ => may_null(n),
    })
}

fn key_order_unruled() -> Refusal {
    refuse::unruled("whether a record's key order is part of its value")
}

fn null_member_unruled() -> Refusal {
    refuse::unruled("whether equality of structures is null-safe member by member when a member is NULL")
}
