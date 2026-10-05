// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Headings: the ordered positions a relation displays, each with its name
//! state. A heading is computed by [`form::form`] alone and stored in the
//! node it describes; name correspondence and structural compatibility are
//! two separate readings of stored headings.

pub(crate) mod compatibility;
pub(crate) mod correspondence;
pub(crate) mod form;
pub(crate) mod selector;

use super::ids::{ExprId, PassengerId, RelId};
use crate::pipeline::middle::facade::SqlIdentifier;

pub(crate) type Name = SqlIdentifier;

/// What a position publishes, and whether it still answers to it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum NameState {
    /// A name the author wrote: a baptism, a slot binder, a header name.
    Authored(Name),
    /// A catalog column's own spelling.
    Catalog(Name),
    /// A name lost by collision. It answers to nothing; its display is a
    /// mint that keeps the lost name.
    Lost(Name),
    /// No name was ever given.
    Minted(MintOrigin),
}

/// Where an unnamed position's value came from, for its display.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MintOrigin {
    /// A computed value.
    Expr,
    /// An unnamed cell of an anonymous table.
    Anon,
    /// An unlabeled ground term of a head: it supplies a value and
    /// abstains from naming the position, which a clause offering a name
    /// for it names.
    Abstained,
}

/// Whether the position is part of the published heading.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Visibility {
    Published,
    /// A dimension of an inchoate read: displayed and reachable by position
    /// only; no name reaches it.
    Latent,
    /// A passenger carried beside the published positions.
    Hidden(PassengerId),
}

/// How a position unifies with a same-named position of another member of
/// its run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Binding {
    /// A bare binder: a same-named bare binder of another member unifies
    /// with it.
    Bare,
    /// Reached through its member's qualifier; unifies only by an explicit
    /// merge.
    Qualified,
}

/// The decided structure of a position's value: independent facts, each
/// decided where the position is formed. Where two structures meet in one
/// position, `join` and `meet` keep every fact; each consumer reads the
/// fact it needs and no other.
#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) struct Interior {
    /// The heading every row's value is known to have.
    pub(crate) known: Known,
    /// Whether the language made the value.
    pub(crate) made: Made,
    /// What is known of the kind of a value from data.
    pub(crate) evidence: Evidence,
    /// Whether the value is declared non-document: a column declared
    /// numeric, date or boolean (THE DOCUMENT DENYLIST), in every row.
    pub(crate) denied: bool,
    /// Whether a row's value may be a truth crossed into a value.
    pub(crate) truth: bool,
    /// Whether a row's value may hold a key the compiler minted: a member
    /// of a record the language made that no name answers to, at any
    /// depth. Its spelling is drawn per compilation, so the value's bytes
    /// are not an answer.
    pub(crate) minted: bool,
    /// Whether every row's value is NULL: a NULL literal, and what holds
    /// only NULLs. Such a value supplies no structure where values meet.
    pub(crate) null: bool,
}

/// The heading a value is known to have. A relation-shaped heading names
/// what formed it: two headings one origin formed hold the same binder
/// columns position by position.
#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) enum Known {
    /// No heading is known for every row.
    #[default]
    None,
    /// Rows with a known heading: a collection, or the relation a receipt
    /// carries.
    Shape(Box<Heading>, Origin),
    /// After a set operation or across clauses: one heading per arm.
    PerArm(Vec<(Heading, Option<Origin>)>),
    /// One record or tuple the language made: its members are the
    /// heading's positions, a record's by key, a tuple's by position.
    One(Box<Heading>, Layout),
    /// A record keyed by data (a metadata collection): every key holds
    /// `inner`.
    Keyed(Box<Interior>),
}

/// Whether the language made the value, by row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) enum Made {
    #[default]
    Never,
    /// In some rows: a made document beside ordinary values.
    Sometimes,
    /// In every row: a constructor nests it rather than quoting its bytes.
    Always,
}

/// What is known of the kind (record, tuple, scalar or text) of a value,
/// from the most to the least: a join keeps the least either side knows.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub(crate) enum Evidence {
    /// Decided: an ordinary value, or a document the language made.
    #[default]
    Decided,
    /// An ordinary value the language made a member of a document, bound
    /// back out by an expansion: the value as it went in under the drill's
    /// reading, a node of the document under the pattern's (unruled).
    /// Under both it is no record or tuple, so a read boundary changes
    /// nothing of it; only a structured consumer reads the two differently.
    Member,
    /// A member a path reached in a document the language made: its kind
    /// is known while the path and its consumer stand in one statement,
    /// but it is a path's result, so a read boundary severs it as it does
    /// any extracted value (json-substrate-law: an extracted value may not
    /// cross a read boundary and then enter a structured consumer).
    Reached,
    /// A node a path read out of a document: its kind is observed per row.
    Extracted,
    /// An extracted value read back across a read boundary: no kind
    /// evidence survives it.
    Severed,
}

/// How a made value's members are addressed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Layout {
    /// By key: a record.
    Record,
    /// By position: a tuple.
    Tuple,
}

impl Interior {
    /// An ordinary value: no structure decided.
    pub(crate) fn flat() -> Self {
        Interior::default()
    }

    /// A value the language made, with the heading every row has. Whether
    /// it holds a minted key is read from that heading, at every depth.
    pub(crate) fn made_with(known: Known) -> Self {
        Interior {
            minted: known.holds_minted(),
            known,
            made: Made::Always,
            ..Interior::default()
        }
    }

    /// A node a path read out of a document.
    pub(crate) fn extracted() -> Self {
        Interior {
            evidence: Evidence::Extracted,
            ..Interior::default()
        }
    }

    /// A node of a document read out of a value of this structure: an
    /// extracted value, which may hold whatever minted key the value may.
    pub(crate) fn node_in(&self) -> Self {
        Interior {
            minted: self.minted,
            ..Interior::extracted()
        }
    }

    /// A catalog column's value, denied a document or not.
    pub(crate) fn declared(denied: bool) -> Self {
        Interior {
            denied,
            ..Interior::default()
        }
    }

    /// A value that is NULL in every row.
    pub(crate) fn null() -> Self {
        Interior {
            null: true,
            ..Interior::default()
        }
    }

    /// A truth crossed into a value.
    pub(crate) fn truth() -> Self {
        Interior {
            truth: true,
            ..Interior::default()
        }
    }

    /// Whether every row's value is a document the language made.
    pub(crate) fn always_made(&self) -> bool {
        self.made == Made::Always
    }

    /// The structure of a column whose rows (or arms) each hold a value of
    /// one of these two structures: case arms, an anonymous table's rows,
    /// a set operation's arms, a family's clauses. A value NULL in every
    /// row holds every structure, so it supplies none.
    pub(crate) fn join(a: &Interior, b: &Interior) -> Interior {
        match (a.null, b.null) {
            (true, _) => b.clone(),
            (false, true) => a.clone(),
            (false, false) => combine(a, b, Meeting::Either),
        }
    }

    /// The structure of a value that is both of these wherever both are
    /// present: a merged key (unified binders, `USING`, natural), a
    /// repeated slot or header binder.
    pub(crate) fn meet(a: &Interior, b: &Interior) -> Interior {
        combine(a, b, Meeting::Both)
    }
}

/// How two structures meet in one position.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Meeting {
    /// Each row holds one side's value.
    Either,
    /// Each row's value is both sides' value.
    Both,
}

/// THE COMBINATION OF TWO STRUCTURES, every fact kept. What is known of the
/// value is what both sides know: a heading only where both know the same
/// one, made only where both agree (by row otherwise), the least kind
/// evidence either keeps. What may hold of one side's value may hold of
/// the position's: a truth, a minted key. Declared non-document holds of
/// every row only where both sides declare it when each row holds one
/// side, and where either does when each row's value is both.
fn combine(a: &Interior, b: &Interior, meeting: Meeting) -> Interior {
    Interior {
        known: if a.known == b.known { a.known.clone() } else { Known::None },
        made: if a.made == b.made { a.made } else { Made::Sometimes },
        evidence: a.evidence.max(b.evidence),
        denied: match meeting {
            Meeting::Either => a.denied && b.denied,
            Meeting::Both => a.denied || b.denied,
        },
        truth: a.truth || b.truth,
        minted: a.minted || b.minted,
        null: a.null && b.null,
    }
}

impl Known {
    /// Whether a value with this heading holds a key the compiler minted:
    /// a record member no name answers to, or a member holding one.
    fn holds_minted(&self) -> bool {
        let members = |h: &Heading, layout: Layout| {
            h.displayed().any(|(_, p)| {
                p.interior.minted || (layout == Layout::Record && p.answering_name().is_none())
            })
        };
        match self {
            Known::None => false,
            Known::Shape(h, origin) => members(h, origin.layout()),
            Known::PerArm(arms) => arms
                .iter()
                .any(|(h, origin)| members(h, origin.map_or(Layout::Record, |o| o.layout()))),
            Known::One(h, layout) => members(h, *layout),
            Known::Keyed(inner) => inner.minted,
        }
    }
}

/// What formed an interior: a collection, or the relation a receipt
/// carries at that position (every row holding the interior holds exactly
/// that relation's rows, staged at the act).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Origin {
    Collection(ExprId),
    /// A collection whose rows are tuples: its members have no names.
    Tuples(ExprId),
    Carried(RelId),
}

impl Origin {
    /// How the rows an origin forms address their members.
    pub(crate) fn layout(self) -> Layout {
        match self {
            Origin::Tuples(_) => Layout::Tuple,
            Origin::Collection(_) | Origin::Carried(_) => Layout::Record,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Position {
    pub(crate) name: NameState,
    pub(crate) visibility: Visibility,
    pub(crate) binding: Binding,
    pub(crate) interior: Interior,
    /// The hidden position of the same heading that carries this
    /// position's node (the extracted value with its kind). A heading
    /// keeps it only while it carries that passenger.
    pub(crate) node: Option<PassengerId>,
}

impl Position {
    pub(crate) fn published(name: NameState, binding: Binding) -> Self {
        Position {
            name,
            visibility: Visibility::Published,
            binding,
            interior: Interior::flat(),
            node: None,
        }
    }

    /// The name the position answers to, when it answers to one.
    pub(crate) fn answering_name(&self) -> Option<&Name> {
        match (&self.visibility, &self.name) {
            (Visibility::Published, NameState::Authored(name) | NameState::Catalog(name)) => {
                Some(name)
            }
            (
                Visibility::Published | Visibility::Latent | Visibility::Hidden(_),
                NameState::Authored(_)
                | NameState::Catalog(_)
                | NameState::Lost(_)
                | NameState::Minted(_),
            ) => None,
        }
    }

    /// The display spelling: a name, or a mint marker keeping what it can.
    pub(crate) fn display(&self) -> String {
        match (&self.visibility, &self.name) {
            (Visibility::Latent, NameState::Authored(n) | NameState::Catalog(n)) => {
                format!("{n}\u{22a5}nee")
            }
            (_, NameState::Authored(n) | NameState::Catalog(n)) => n.to_string(),
            (_, NameState::Lost(n)) => format!("{n}\u{22a5}"),
            (_, NameState::Minted(MintOrigin::Expr)) => "\u{22a5}expr".to_string(),
            (_, NameState::Minted(MintOrigin::Anon)) => "\u{22a5}anon".to_string(),
            (_, NameState::Minted(MintOrigin::Abstained)) => "\u{22a5}abstained".to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) struct Heading {
    positions: Vec<Position>,
}

impl Heading {
    /// A heading of these positions. A position's node link survives only
    /// where the heading carries that passenger.
    fn of(mut positions: Vec<Position>) -> Self {
        let carried: Vec<PassengerId> = positions
            .iter()
            .filter_map(|p| match p.visibility {
                Visibility::Hidden(h) => Some(h),
                Visibility::Published | Visibility::Latent => None,
            })
            .collect();
        for p in &mut positions {
            if p.node.is_some_and(|n| !carried.contains(&n)) {
                p.node = None;
            }
        }
        Heading { positions }
    }

    pub(crate) fn positions(&self) -> &[Position] {
        &self.positions
    }

    pub(crate) fn len(&self) -> usize {
        self.positions.len()
    }

    /// The displayed positions (published and latent), in order: the
    /// positions an ordinal counts.
    pub(crate) fn displayed(&self) -> impl Iterator<Item = (usize, &Position)> {
        self.positions
            .iter()
            .enumerate()
            .filter(|(_, p)| !matches!(p.visibility, Visibility::Hidden(_)))
    }

    /// The same positions, each holding `f` of its structure.
    pub(crate) fn map_interiors(&self, f: impl Fn(&Interior) -> Interior) -> Heading {
        Heading::of(
            self.positions
                .iter()
                .map(|p| Position {
                    interior: f(&p.interior),
                    ..p.clone()
                })
                .collect(),
        )
    }

    /// The display spelling of the displayed heading.
    pub(crate) fn display(&self) -> Vec<String> {
        self.displayed().map(|(_, p)| p.display()).collect()
    }
}
