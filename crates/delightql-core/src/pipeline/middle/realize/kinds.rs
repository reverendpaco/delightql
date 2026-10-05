// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The value kinds realization reads to choose a witness's faithful key: a
//! function of the graph and the target, recomputed per realization and
//! never stored in the core.
//!
//! A catalog column's declared type is evidence of what it holds only when
//! the backend serving it answers that its storage guarantees its declared
//! types, and only in that backend's own terms: `ReadSource::Catalog.typed`
//! records both (a view, a virtual table, a relation served in another
//! dialect than the target's, or one no introspection can answer for, is
//! not typed, and its column has no kind).
//!
//! A call's result has no kind: what a target function returns is a fact
//! of the target no product fan-out states. A cast has its type word's.
//!
//! On SQLite the one question a kind answers is whether a column can hold
//! `-0.0`: a typed column of INTEGER, REAL, NUMERIC or TEXT affinity cannot;
//! an untyped one can. An integer or text value cannot; any other value can.
//! A declared type is read once, by its affinity (SQLite's rule, read
//! through the dialect data door), for both the kind and the
//! compound-affinity judgment. On DuckDB and
//! PostgreSQL it is whether the value's type is one whose faithful key was
//! measured: a typed column's declared type is the target's own type, and a
//! computed kind's types (integers, text, doubles and decimals, booleans,
//! documents) are all measured ones.

use super::frame::{Key, Private};
use crate::pipeline::middle::core::graph::{Arena, Graph};
use crate::pipeline::middle::core::node::provenance::{self, Source};
use crate::pipeline::middle::core::ids::{BinderId, ExprId, RelId};
use crate::pipeline::middle::core::node::{Arg, Cell, ExprKind, Qual, ReadSource, RelKind};
use crate::pipeline::middle::facade::{BinOp, LiteralValue, Output, SqlDialect, SqliteAffinity};
use std::cell::RefCell;
use std::collections::BTreeMap;

/// A value's kind, as far as the key choice needs it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    Int,
    Text,
    Null,
    /// A document the language made: text on every target.
    Json,
    /// A column of REAL or NUMERIC affinity, read directly: it never holds
    /// `-0.0` (REAL affinity stores it as `0.0`, NUMERIC as the integer 0).
    StoredReal,
    Real,
    Unknown,
}

impl Kind {
    fn join(self, other: Kind) -> Kind {
        match (self, other) {
            (a, b) if a == b => a,
            (Kind::Null, x) | (x, Kind::Null) => x,
            (Kind::Int | Kind::Real | Kind::StoredReal, Kind::Int | Kind::Real | Kind::StoredReal) => Kind::Real,
            _ => Kind::Unknown,
        }
    }

    /// A computed value over this kind loses the stored representation.
    fn computed(self) -> Kind {
        match self {
            Kind::StoredReal => Kind::Real,
            k => k,
        }
    }
}

/// A SQLite type affinity a position's values carry from a declared
/// column type (datatype3 section 3.1), where they are a plain read of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Affinity {
    Integer,
    Text,
    Real,
    Numeric,
}

/// The affinity a position carries: none (a literal, a computed value, a
/// column declared with none), a declared one, the fixpoint's own (a
/// frontier read), one whose value is a declared column's but whose
/// spelling carries none (a delegate's pick: `Lost`), or one the table
/// cannot trace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Carried {
    None,
    Declared(Affinity),
    Lost(Affinity),
    Fixpoint,
    Unknown,
}

/// The kind table of one realization.
pub(super) struct KindTable {
    target: SqlDialect,
    members: BTreeMap<BinderId, RelId>,
    /// Each catalog column's affinity, by its read and its catalog
    /// position: SQLite's rule over its declared type, read through the
    /// target's dialect data.
    affinities: BTreeMap<(RelId, usize), Carried>,
    memo: RefCell<BTreeMap<(RelId, u16), Kind>>,
    carried_memo: RefCell<BTreeMap<(RelId, u16), Carried>>,
}

impl KindTable {
    pub(super) fn of(graph: &Graph, rels: impl Iterator<Item = RelId>, out: &Output<'_>) -> Self {
        let mut members = BTreeMap::new();
        let mut affinities = BTreeMap::new();
        for id in rels {
            match graph.rel(id).kind() {
                RelKind::Run(run) => {
                    for q in run.quals() {
                        if let Qual::Member(m) = q {
                            members.insert(m.binder(), m.rel());
                        }
                    }
                }
                RelKind::Read {
                    source: ReadSource::Catalog { columns, .. },
                    ..
                } => {
                    for (k, column) in columns.iter().enumerate() {
                        affinities.insert((id, k), carried_affinity(out.declared_affinity(column.declared.as_deref())));
                    }
                }
                _ => {}
            }
        }
        KindTable {
            target: out.dialect(),
            members,
            affinities,
            memo: RefCell::new(BTreeMap::new()),
            carried_memo: RefCell::new(BTreeMap::new()),
        }
    }

    /// A catalog column's affinity; one this table did not read cannot be
    /// established.
    fn affinity(&self, rel: RelId, k: usize) -> Carried {
        self.affinities.get(&(rel, k)).copied().unwrap_or(Carried::Unknown)
    }

    /// The SQLite affinity position `i` of a relation's heading carries: a
    /// declared column read unchanged; `None` for a computed value.
    pub(super) fn declared_affinity(&self, graph: &Graph, rel: RelId, i: u16) -> Option<Affinity> {
        match self.carried(graph, rel, i) {
            Carried::Declared(affinity) => Some(affinity),
            Carried::None | Carried::Lost(_) | Carried::Fixpoint | Carried::Unknown => None,
        }
    }

    /// The affinity position `i` of a relation holds by its value but not by
    /// its spelling: a delegate's pick of a declared column.
    pub(super) fn lost_affinity(&self, graph: &Graph, rel: RelId, i: u16) -> Option<Affinity> {
        match self.carried(graph, rel, i) {
            Carried::Lost(affinity) => Some(affinity),
            Carried::None | Carried::Declared(_) | Carried::Fixpoint | Carried::Unknown => None,
        }
    }

    /// Whether the column a key names can hold `-0.0` on SQLite.
    pub(super) fn may_hold_negative_zero(&self, graph: &Graph, key: Key) -> bool {
        matches!(self.key(graph, key), Kind::Real | Kind::Unknown)
    }

    /// Whether every type the column a key names can have on the target is
    /// one whose faithful key was measured.
    pub(super) fn measured(&self, graph: &Graph, key: Key) -> bool {
        self.key(graph, key) != Kind::Unknown
    }

    /// A catalog column's kind on the target, from its declared type when
    /// the relation is typed (its storage guarantees the declared types, in
    /// the target's own terms); an untyped column has none.
    fn catalog_column(&self, rel: RelId, k: usize, declared: Option<&str>, typed: bool) -> Kind {
        if !typed {
            return Kind::Unknown;
        }
        match self.target {
            SqlDialect::SQLite => stored_kind(self.affinity(rel, k)),
            target => declared.map_or(Kind::Unknown, |t| measured_type(target, t)),
        }
    }

    fn key(&self, graph: &Graph, key: Key) -> Kind {
        match key {
            Key::Col(b, i) => match self.members.get(&b) {
                Some(rel) => self.position(graph, *rel, i),
                None => Kind::Unknown,
            },
            Key::Merged(m) => {
                let site = graph.merge(m);
                let left = self.cell(graph, site.left());
                let (rb, ri) = site.right();
                left.join(self.key(graph, Key::Col(rb, ri)))
            }
            Key::Passenger(p) => match graph.passenger(p) {
                crate::pipeline::middle::core::node::Passenger::Configured { value, .. } => self.expr(graph, *value),
                crate::pipeline::middle::core::node::Passenger::RowLocator(_) => Kind::Int,
                crate::pipeline::middle::core::node::Passenger::Node { .. } => Kind::Json,
            },
            Key::Value(e) => self.expr(graph, e),
            Key::Pos(rel, i) => self.position(graph, rel, i),
            Key::Private(_, Private::Witness | Private::Flag | Private::Matched | Private::Rank) => Kind::Int,
            Key::Private(_, Private::Order(_)) | Key::Lifted(_) => Kind::Unknown,
            Key::Level(..) | Key::KeyedRows(..) | Key::Node(..) | Key::BindNode(..) | Key::ColNode(..) => Kind::Json,
            Key::Pick(..) | Key::KeyPick(..) => Kind::Int,
        }
    }

    fn cell(&self, graph: &Graph, cell: Cell) -> Kind {
        match cell {
            Cell::Col(b, i) => self.key(graph, Key::Col(b, i)),
            Cell::Merged(m) => self.key(graph, Key::Merged(m)),
        }
    }

    /// The kind of position `i` of a relation's heading.
    #[stacksafe::stacksafe]
    pub(super) fn position(&self, graph: &Graph, rel: RelId, i: u16) -> Kind {
        if let Some(k) = self.memo.borrow().get(&(rel, i)) {
            return *k;
        }
        self.memo.borrow_mut().insert((rel, i), Kind::Unknown);
        let kind = self.compute(graph, rel, i as usize);
        self.memo.borrow_mut().insert((rel, i), kind);
        kind
    }

    fn compute(&self, graph: &Graph, rel: RelId, i: usize) -> Kind {
        match provenance::of(graph, rel, i) {
            Source::Passenger(p) => self.key(graph, Key::Passenger(p)),
            Source::Catalog { read, column } => match graph.rel(read).kind() {
                RelKind::Read {
                    source: ReadSource::Catalog { columns, typed, .. },
                    ..
                } => match columns.get(column) {
                    Some(c) => self.catalog_column(read, column, c.declared.as_deref(), *typed),
                    None => Kind::Int,
                },
                _ => Kind::Unknown,
            },
            Source::Position(below, j) => self.position(graph, below, j as u16),
            Source::Cell { cell, .. } => self.cell(graph, cell),
            Source::Value { expr, .. } => self.expr(graph, expr),
            Source::Rows(values) => values.iter().map(|e| self.expr(graph, *e)).fold(Kind::Null, Kind::join),
            Source::Arms(arms) => arms
                .iter()
                .map(|arm| arm.map_or(Kind::Null, |(r, j)| self.position(graph, r, j as u16)))
                .fold(Kind::Null, Kind::join),
            Source::Clauses(bodies) => {
                bodies.iter().map(|b| self.position(graph, *b, i as u16)).fold(Kind::Null, Kind::join)
            }
            Source::Fixpoint { anchors, steps } => anchors
                .iter()
                .chain(&steps)
                .map(|b| self.position(graph, *b, i as u16))
                .fold(Kind::Null, Kind::join),
            Source::Frontier | Source::Unknown => Kind::Unknown,
            Source::Reflection(at) => {
                if at == 2 {
                    Kind::Int
                } else {
                    Kind::Text
                }
            }
            Source::Interior => Kind::Json,
            Source::Verdict => Kind::Int,
        }
    }

    #[stacksafe::stacksafe]
    fn expr(&self, graph: &Graph, e: ExprId) -> Kind {
        match graph.expr(e).kind() {
            ExprKind::Col(b, i) => self.key(graph, Key::Col(*b, *i)),
            ExprKind::Merged(m) => self.key(graph, Key::Merged(*m)),
            ExprKind::Passenger(p) => self.key(graph, Key::Passenger(*p)),
            ExprKind::Const(v) => literal(v),
            ExprKind::Call { callee, args, .. } | ExprKind::Window { callee, args, .. } => {
                // A call's result kind is a fact of the target's function,
                // which no product fan-out states (the aggregates targeting
                // table and the dialect pack carry none): no kind is
                // assumed. A cast's is the language's type word's.
                match callee.name.as_str() {
                    "cast" => match cast_type(graph, args) {
                        Some("integer" | "boolean") => Kind::Int,
                        Some("text") => Kind::Text,
                        Some("real" | "numeric") => Kind::Real,
                        _ => Kind::Unknown,
                    },
                    _ => Kind::Unknown,
                }
            }
            ExprKind::Infix(op, l, r) => {
                let (lk, rk) = (self.expr(graph, *l).computed(), self.expr(graph, *r).computed());
                match op {
                    BinOp::Concat => Kind::Text,
                    BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Mod | BinOp::Div => match (lk, rk) {
                        (Kind::Int, Kind::Int) => Kind::Int,
                        (Kind::Null, _) | (_, Kind::Null) => Kind::Null,
                        (Kind::Int | Kind::Real, Kind::Int | Kind::Real) => Kind::Real,
                        _ => Kind::Unknown,
                    },
                }
            }
            ExprKind::Case { arms, default, .. } => {
                let mut k = default.map(|d| self.expr(graph, d)).unwrap_or(Kind::Null);
                for (_, result) in arms {
                    k = k.join(self.expr(graph, *result));
                }
                k
            }
            ExprKind::Crossed(_) => Kind::Int,
            ExprKind::Scalar { rel, .. } => match graph.rel(*rel).heading().displayed().next() {
                Some((first, _)) => self.position(graph, *rel, first as u16),
                None => Kind::Unknown,
            },
            ExprKind::Collect { .. } | ExprKind::Construct { .. } | ExprKind::Metadata { .. } => Kind::Json,
            ExprKind::Pick { value, .. } => self.expr(graph, *value),
            // A document read: what the target's read returns is its own.
            ExprKind::Path { .. } => Kind::Unknown,
            ExprKind::Across(actual) | ExprKind::Argument { value: actual, .. } => self.expr(graph, *actual),
        }
    }
}

impl KindTable {
    /// Whether a SQLite compound (a UNION ALL spine, a recursive
    /// accumulation) over these arms' positions could change a value: SQLite
    /// gives the compound column the affinity of one arm, so it converts
    /// another arm's value unless every arm carries that affinity or holds
    /// values it keeps. `None` for an arm stands for a written NULL.
    pub(super) fn compound_changes(&self, graph: &Graph, arms: &[Option<(RelId, u16)>]) -> bool {
        let mut declared: Option<Affinity> = None;
        let mut plain: Vec<Kind> = Vec::new();
        for arm in arms {
            let Some((rel, i)) = arm else {
                continue;
            };
            match self.carried(graph, *rel, *i) {
                Carried::Unknown => return true,
                Carried::Fixpoint => {}
                // A pick's lost affinity is its value's, taken back by every
                // comparison: it counts as the column's own.
                Carried::Declared(a) | Carried::Lost(a) => match declared {
                    Some(b) if b != a => return true,
                    _ => declared = Some(a),
                },
                Carried::None => plain.push(self.position(graph, *rel, *i)),
            }
        }
        let Some(affinity) = declared else {
            return false;
        };
        plain.into_iter().any(|kind| !keeps(affinity, kind))
    }

    /// The affinity position `i` of a relation carries.
    #[stacksafe::stacksafe]
    fn carried(&self, graph: &Graph, rel: RelId, i: u16) -> Carried {
        if let Some(c) = self.carried_memo.borrow().get(&(rel, i)) {
            return *c;
        }
        self.carried_memo.borrow_mut().insert((rel, i), Carried::Unknown);
        let c = self.carried_of(graph, rel, i as usize);
        self.carried_memo.borrow_mut().insert((rel, i), c);
        c
    }

    fn carried_of(&self, graph: &Graph, rel: RelId, i: usize) -> Carried {
        use crate::pipeline::middle::core::node::Passenger;
        match provenance::of(graph, rel, i) {
            Source::Passenger(p) => match graph.passenger(p) {
                Passenger::Configured { value, .. } => self.carried_expr(graph, *value),
                Passenger::RowLocator(_) | Passenger::Node { .. } => Carried::None,
            },
            Source::Catalog { read, column } => match graph.rel(read).kind() {
                RelKind::Read {
                    source: ReadSource::Catalog { columns, .. },
                    ..
                } if column < columns.len() => self.affinity(read, column),
                _ => Carried::None,
            },
            Source::Position(below, j) => self.carried(graph, below, j as u16),
            Source::Cell { cell, .. } => self.carried_cell(graph, cell),
            Source::Value { expr, .. } => self.carried_expr(graph, expr),
            Source::Rows(values) => values.iter().map(|e| self.carried_expr(graph, *e)).fold(Carried::None, agree),
            Source::Arms(arms) => arms
                .iter()
                .map(|arm| arm.map_or(Carried::None, |(r, j)| self.carried(graph, r, j as u16)))
                .reduce(agree)
                .unwrap_or(Carried::Unknown),
            // Every clause (anchor) carries the position's affinity, or the
            // clauses carry none they agree on.
            Source::Clauses(bodies) => {
                bodies.iter().map(|b| self.carried(graph, *b, i as u16)).reduce(agree).unwrap_or(Carried::None)
            }
            Source::Fixpoint { anchors, .. } => {
                anchors.iter().map(|b| self.carried(graph, *b, i as u16)).reduce(agree).unwrap_or(Carried::None)
            }
            Source::Frontier => Carried::Fixpoint,
            Source::Reflection(_) | Source::Interior | Source::Verdict => Carried::None,
            Source::Unknown => Carried::Unknown,
        }
    }

    fn carried_col(&self, graph: &Graph, b: BinderId, i: u16) -> Carried {
        match self.members.get(&b) {
            Some(rel) => self.carried(graph, *rel, i),
            None => Carried::Unknown,
        }
    }

    fn carried_cell(&self, graph: &Graph, cell: Cell) -> Carried {
        match cell {
            Cell::Col(b, i) => self.carried_col(graph, b, i),
            Cell::Merged(m) => {
                let (b, j) = graph.merge(m).right();
                agree(self.carried_cell(graph, graph.merge(m).left()), self.carried_col(graph, b, j))
            }
        }
    }

    /// A plain reference carries its column's affinity; every other
    /// expression carries none, except a call the table does not know.
    fn carried_expr(&self, graph: &Graph, e: ExprId) -> Carried {
        match graph.expr(e).kind() {
            ExprKind::Col(b, i) => self.carried_col(graph, *b, *i),
            ExprKind::Merged(m) => self.carried_cell(graph, Cell::Merged(*m)),
            ExprKind::Passenger(p) => match graph.passenger(*p) {
                crate::pipeline::middle::core::node::Passenger::Configured { value, .. } => self.carried_expr(graph, *value),
                crate::pipeline::middle::core::node::Passenger::RowLocator(_) | crate::pipeline::middle::core::node::Passenger::Node { .. } => Carried::None,
            },
            // A CAST has the affinity its type would give a column.
            ExprKind::Call { callee, args, .. } if callee.name == "cast" => match cast_type(graph, args) {
                Some("integer") => Carried::Declared(Affinity::Integer),
                Some("text") => Carried::Declared(Affinity::Text),
                Some("real") => Carried::Declared(Affinity::Real),
                Some("numeric" | "boolean") => Carried::Declared(Affinity::Numeric),
                _ => Carried::Unknown,
            },
            ExprKind::Scalar { .. } => Carried::Unknown,
            ExprKind::Across(actual) => self.carried_expr(graph, *actual),
            // A pick is its chosen row's value, spelled through an aggregate
            // that carries no affinity.
            ExprKind::Pick { value, .. } => match self.carried_expr(graph, *value) {
                Carried::Declared(affinity) | Carried::Lost(affinity) => Carried::Lost(affinity),
                other => other,
            },
            ExprKind::Argument { .. } => Carried::None,
            ExprKind::Const(_)
            | ExprKind::Call { .. }
            | ExprKind::Window { .. }
            | ExprKind::Infix(..)
            | ExprKind::Case { .. }
            | ExprKind::Crossed(_)
            | ExprKind::Collect { .. }
            | ExprKind::Metadata { .. }
            | ExprKind::Construct { .. }
            | ExprKind::Path { .. } => Carried::None,
        }
    }
}

/// The type symbol a cast converts to.
fn cast_type<'g>(graph: &'g Graph, args: &[Arg]) -> Option<&'g str> {
    match args {
        [_, Arg::Value { expr, .. }] => match graph.expr(*expr).kind() {
            ExprKind::Const(LiteralValue::Symbol(t)) => Some(t.as_str()),
            _ => None,
        },
        _ => None,
    }
}

/// Two alternatives' affinities as one position's: equal ones agree; any
/// disagreement is untraced.
fn agree(a: Carried, b: Carried) -> Carried {
    match (a, b) {
        (x, y) if x == y => x,
        (Carried::Fixpoint, x) | (x, Carried::Fixpoint) => x,
        _ => Carried::Unknown,
    }
}

/// What a column carries of SQLite's affinity of its declared type: a
/// column declared with none (or BLOB) carries none; one whose affinity the
/// catalog cannot establish carries an untraced one.
fn carried_affinity(affinity: Option<SqliteAffinity>) -> Carried {
    match affinity {
        None => Carried::Unknown,
        Some(SqliteAffinity::Blob) => Carried::None,
        Some(SqliteAffinity::Integer) => Carried::Declared(Affinity::Integer),
        Some(SqliteAffinity::Text) => Carried::Declared(Affinity::Text),
        Some(SqliteAffinity::Real) => Carried::Declared(Affinity::Real),
        Some(SqliteAffinity::Numeric) => Carried::Declared(Affinity::Numeric),
    }
}

/// Whether an affinity leaves a value of this kind as it is.
fn keeps(affinity: Affinity, kind: Kind) -> bool {
    matches!(
        (affinity, kind),
        (_, Kind::Null) | (Affinity::Integer | Affinity::Numeric, Kind::Int) | (Affinity::Text, Kind::Text)
    )
}

fn literal(v: &LiteralValue) -> Kind {
    match v {
        LiteralValue::Number(n) if n.is_integer() => Kind::Int,
        LiteralValue::Number(_) => Kind::Real,
        LiteralValue::String(_) | LiteralValue::Symbol(_) | LiteralValue::Mention(_) => Kind::Text,
        LiteralValue::Boolean(_) => Kind::Int,
        LiteralValue::Null => Kind::Null,
    }
}

/// The kind of a target type whose faithful key R2F-KEYS measured; any
/// other type is `Unknown`.
fn measured_type(target: SqlDialect, name: &str) -> Kind {
    match (target, name) {
        (SqlDialect::DuckDB, "INTEGER" | "BIGINT" | "HUGEINT") | (SqlDialect::PostgreSQL, "integer" | "bigint") => {
            Kind::Int
        }
        (SqlDialect::DuckDB, "VARCHAR" | "VARCHAR COLLATE NOCASE") | (SqlDialect::PostgreSQL, "text") => Kind::Text,
        (SqlDialect::DuckDB, "DOUBLE") | (SqlDialect::PostgreSQL, "double precision" | "numeric") => Kind::Real,
        (SqlDialect::DuckDB, "JSON") | (SqlDialect::PostgreSQL, "json" | "jsonb") => Kind::Json,
        _ => Kind::Unknown,
    }
}

/// The kind a stored column's affinity gives the values read from it: an
/// INTEGER or TEXT affinity holds integers or text as written; a REAL or
/// NUMERIC affinity stores a real that is exactly an integer as one (datatype3
/// §3), so it never holds `-0.0`; a column of no affinity holds what it is
/// given.
fn stored_kind(carried: Carried) -> Kind {
    match carried {
        Carried::Declared(Affinity::Integer) | Carried::Lost(Affinity::Integer) => Kind::Int,
        Carried::Declared(Affinity::Text) | Carried::Lost(Affinity::Text) => Kind::Text,
        Carried::Declared(Affinity::Real | Affinity::Numeric) | Carried::Lost(Affinity::Real | Affinity::Numeric) => {
            Kind::StoredReal
        }
        Carried::None | Carried::Fixpoint | Carried::Unknown => Kind::Unknown,
    }
}
