// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! An effect's act and its receipt. The act is the directive's facets as
//! its descriptor declares them and the law judges them: what it writes,
//! when its receipt holds a row, which relations its receipt carries as
//! interiors, and whether it prints its input; with the relation it
//! consumes. The act is decided when the receipt node is built and never
//! written again: the receipt node holds it, so an act exists exactly where
//! elaboration reached a directive, once per occurrence.

use crate::pipeline::middle::core::decide::mutation as law;
use crate::pipeline::middle::core::graph::{Arena, Builder, Judging};
use crate::pipeline::middle::core::heading::form::{self, Formed};
use crate::pipeline::middle::core::heading::{Heading, Name};
use crate::pipeline::middle::core::ids::{ExprId, PassengerId, RelId};
use crate::pipeline::middle::core::node::walk::{self, Child};
use crate::pipeline::middle::core::node::{ReadSource, RelKind};
use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::facade::{ObjectResidence, ObjectShape, Placement};

/// What an act does to storage.
pub(crate) enum Write {
    /// Nothing: a utility.
    None,
    /// Rows of a stored table, read by `target`.
    Rows { target: RelId, rows: RowWrite },
    /// An object created from the source.
    Object(Created),
    /// The session's world: the runtime performs the directive once for
    /// each row of the source, each row its arguments in order. `report` is
    /// the receipt's carried relation the act fills with the rows it
    /// reports when it runs.
    Session { directive: String, report: Option<RelId> },
    /// A terminal disposition, reached when the source has a row: a
    /// graceful stop, or an abort under its label.
    Terminal(Terminal),
}

/// A terminal disposition (TERMINAL DISPOSITIONS AND ASSERTION).
pub(crate) enum Terminal {
    Exit,
    Abort { label: String },
}

/// How an act writes a stored table's rows.
pub(crate) enum RowWrite {
    /// Every source row inserted: `(target position, source position)` for
    /// each target column the source names, in target order. A target
    /// column the source does not name takes NULL.
    Insert { columns: Vec<(u16, u16)> },
    /// Every row the marked occurrence's locator reaches replaced by its
    /// incoming row: `(target position, source position)` for every
    /// column, in target order.
    Replace { arrival: law::Arrival, columns: Vec<(u16, u16)> },
    /// Every row the marked occurrence's locator reaches removed.
    Remove { arrival: law::Arrival },
}

/// An object a creation directive makes: where the runtime places and
/// registers it (the placement carries its name, and what the directive
/// materializes, shape and residence), and the shape of the same-owner
/// session object holding its name, which the creation replaces (NAME
/// CLASH: by name, not kind).
pub(crate) struct Created {
    pub(crate) placement: Placement,
    pub(crate) replaces: Option<ObjectShape>,
    /// Each read of the source that the object's own session name will
    /// shadow once it is created, with the schema spelling that reaches the
    /// durable relation past the session pool: a view keeps reading its
    /// source, never itself.
    pub(crate) past_session: Vec<(RelId, String)>,
}

impl Created {
    pub(crate) fn shape(&self) -> ObjectShape {
        self.placement.creation().shape()
    }
    pub(crate) fn residence(&self) -> ObjectResidence {
        self.placement.creation().residence()
    }
}

/// The written parts of a creation and the catalog facts beside them:
/// whether the durable name exists in the target namespace.
pub(crate) struct CreatedSpec {
    pub(crate) placement: Placement,
    pub(crate) durable_exists: bool,
    pub(crate) past_session: Vec<(RelId, String)>,
}

/// When the receipt holds its row.
pub(crate) enum Verdict {
    /// A stored row was reached: inserted, replaced or removed.
    Affected,
    /// The act was reached: a creation succeeds or aborts; a utility
    /// answers when reached.
    Reached,
    /// The witness has a row. An empty witness aborts the run under the
    /// authored label, or, unlabelled, under the occurrence's own name.
    Witness { witness: RelId, label: Option<String> },
    /// The relation has a row: a user rule's ledger of its clause receipts.
    Any(RelId),
}

/// One position of a receipt: a constant of the act, or a relation the
/// receipt carries as an interior value.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ReceiptCell {
    Const(ExprId),
    Interior(RelId),
}

pub(crate) struct EffectAct {
    operation: String,
    source: Option<RelId>,
    write: Write,
    verdict: Verdict,
    prints: bool,
    inputs: Vec<RelId>,
}

impl EffectAct {
    /// The directive's name as written: the receipt's `operation`.
    pub(crate) fn operation(&self) -> &str {
        &self.operation
    }
    /// The relation the act consumes, when it consumes one.
    pub(crate) fn source(&self) -> Option<RelId> {
        self.source
    }
    pub(crate) fn write(&self) -> &Write {
        &self.write
    }
    pub(crate) fn verdict(&self) -> &Verdict {
        &self.verdict
    }
    /// Whether the act ships its input to the client.
    pub(crate) fn prints(&self) -> bool {
        self.prints
    }
    /// The relations an invocation's formals are bound to: each is the
    /// call's argument, one relation however many clauses read it.
    pub(crate) fn inputs(&self) -> &[RelId] {
        &self.inputs
    }
    /// The relations the act reads besides its source: its target, its
    /// witness, the ledger its verdict reads.
    pub(crate) fn reads(&self) -> Vec<RelId> {
        let mut out: Vec<RelId> = self.source.into_iter().collect();
        out.extend(self.inputs.iter().copied());
        if let Write::Rows { target, .. } = &self.write {
            out.push(*target);
        }
        match &self.verdict {
            Verdict::Witness { witness, .. } => out.push(*witness),
            Verdict::Any(rel) => out.push(*rel),
            Verdict::Affected | Verdict::Reached => {}
        }
        out
    }
}

/// How a row write is asked for, before the law judges it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RowMode {
    Insert,
    Replace,
    Remove,
}

/// The written parts of a write.
pub(crate) enum WriteSpec {
    None,
    Rows { target: RelId, mode: RowMode },
    Object(CreatedSpec),
    Session { directive: String, report: Option<RelId> },
    Terminal(Terminal),
}

/// The written parts of an act: what the directive's descriptor and its
/// call supply.
pub(crate) struct EffectSpec {
    pub(crate) operation: String,
    pub(crate) source: Option<RelId>,
    pub(crate) write: WriteSpec,
    pub(crate) verdict: Verdict,
    pub(crate) prints: bool,
    /// The relations an invocation's formals are bound to.
    pub(crate) inputs: Vec<RelId>,
    /// The receipt's columns, in the descriptor's order.
    pub(crate) receipt: Vec<(Name, ReceiptCell)>,
}

/// The stored table a catalog read reads: its catalog row when the catalog
/// rows it, else its namespace and name.
#[derive(PartialEq)]
enum Table {
    Row(i64),
    Named(String, Name),
    None,
}

impl Table {
    fn of(arena: &impl Judging, read: RelId) -> Table {
        match arena.rel(read).kind() {
            RelKind::Read {
                source:
                    ReadSource::Catalog {
                        entity: Some(id), ..
                    },
                ..
            } => Table::Row(*id),
            RelKind::Read {
                source: ReadSource::Catalog { name, namespace, .. },
                ..
            } => Table::Named(namespace.clone(), name.clone()),
            _ => Table::None,
        }
    }
}

/// A read's table as the author would write it, for a refusal.
fn spelled(arena: &impl Judging, read: RelId) -> String {
    match arena.rel(read).kind() {
        RelKind::Read {
            source: ReadSource::Catalog { name, namespace, .. },
            ..
        } => format!("{namespace}.{name}"),
        _ => String::new(),
    }
}

impl Builder {
    /// Build an act and the receipt that stands where its directive is
    /// written. A row write is judged by the mutation contract: an insert
    /// forbids a marked occurrence under its source and pairs the source's
    /// columns with the target's by name; a replace or remove reaches the
    /// rows of exactly one marked occurrence of the target, with no grouping
    /// and no bound on the descent from it, its row locator arriving at the
    /// terminal; a replace's incoming heading is the target's by name.
    pub(crate) fn effect(&mut self, spec: EffectSpec) -> Result<RelId, Refusal> {
        let verb = spec.operation.clone();
        // A write stores its source's bytes. A printing act releases them,
        // and a mint inside a released value is data.
        let stores = match &spec.write {
            WriteSpec::Object(_) | WriteSpec::Rows { mode: RowMode::Insert | RowMode::Replace, .. } => {
                Some("a stored write")
            }
            WriteSpec::Session { .. } => Some("a session act's arguments"),
            WriteSpec::Rows { mode: RowMode::Remove, .. } | WriteSpec::None | WriteSpec::Terminal(_) => None,
        };
        if let (Some(source), Some(consumer)) = (spec.source, stores) {
            for (_, p) in self.rel(source).heading().displayed() {
                crate::pipeline::middle::core::decide::document::observed(&p.interior, consumer)?;
            }
        }
        let write = match spec.write {
            WriteSpec::None => Write::None,
            WriteSpec::Session { directive, report } => {
                spec.source
                    .ok_or_else(|| refuse::elaboration_contract("a session act with no arguments"))?;
                if let Some(report) = report {
                    if !spec.receipt.iter().any(|(_, c)| matches!(c, ReceiptCell::Interior(r) if *r == report)) {
                        return Err(refuse::elaboration_contract("an act report the receipt does not carry"));
                    }
                }
                Write::Session { directive, report }
            }
            WriteSpec::Terminal(terminal) => {
                // Reached when its input has a row, or, with no input, when
                // its step runs.
                match (spec.source, &spec.verdict) {
                    (Some(source), Verdict::Any(rel)) if *rel == source => {}
                    (None, Verdict::Reached) => {}
                    _ => {
                        return Err(refuse::elaboration_contract(
                            "a terminal disposition reached otherwise than by its input or by its step",
                        ))
                    }
                }
                Write::Terminal(terminal)
            }
            WriteSpec::Object(created) => {
                let source = spec
                    .source
                    .ok_or_else(|| refuse::elaboration_contract("a creation with no source"))?;
                same_connection(self, &verb, source, &created.placement)?;
                Write::Object(clash(&verb, created)?)
            }
            WriteSpec::Rows { target, mode } => {
                let source = spec
                    .source
                    .ok_or_else(|| refuse::elaboration_contract("a row write with no source"))?;
                let rows = self.row_write(&verb, target, source, mode)?;
                Write::Rows { target, rows }
            }
        };
        let (header, cells): (Vec<Name>, Vec<ReceiptCell>) = spec.receipt.into_iter().unzip();
        // The act stages every relation its receipt carries, and what reads
        // the receipt reads that staging.
        for cell in &cells {
            if let ReceiptCell::Interior(rel) = cell {
                for (_, p) in self.rel(*rel).heading().displayed() {
                    crate::pipeline::middle::core::decide::document::staged(&p.interior)?;
                }
            }
        }
        let heading = form::form(Formed::Receipt {
            columns: &receipt_columns(self, &header, &cells),
        })?;
        let act = EffectAct {
            operation: spec.operation,
            source: spec.source,
            write,
            verdict: spec.verdict,
            prints: spec.prints,
            inputs: spec.inputs,
        };
        Ok(self.push_relation(
            RelKind::Receipt {
                act: Box::new(act),
                header,
                cells,
            },
            heading,
        ))
    }

    fn row_write(&mut self, verb: &str, target: RelId, source: RelId, mode: RowMode) -> Result<RowWrite, Refusal> {
        let marks = law::marks(self, source);
        match mode {
            RowMode::Insert => {
                if !marks.is_empty() {
                    return Err(refuse::marker_forbidden(verb));
                }
                let computed = law::computed(self, target);
                let columns = law::insertion(self.rel(target).heading(), self.rel(source).heading(), &computed)?;
                Ok(RowWrite::Insert { columns })
            }
            RowMode::Replace => {
                let (arrival, mark) = self.reached(verb, target, source, &marks)?;
                let columns = law::replaced(self, target, source, mark)?;
                Ok(RowWrite::Replace { arrival, columns })
            }
            RowMode::Remove => {
                let (arrival, mark) = self.reached(verb, target, source, &marks)?;
                law::published(self, target, source, mark)?;
                Ok(RowWrite::Remove { arrival })
            }
        }
    }

    /// The rows a replace or remove reaches: those of exactly one marked
    /// occurrence of the target, with no grouping and no bound on the
    /// descent from it, its row locator arriving at the terminal once.
    fn reached(
        &self,
        verb: &str,
        target: RelId,
        source: RelId,
        marks: &[(Vec<PassengerId>, RelId)],
    ) -> Result<(law::Arrival, RelId), Refusal> {
        let (locator, mark) = match marks {
            [(locator, mark)] => (locator.as_slice(), *mark),
            [] => return Err(refuse::marker_missing(verb)),
            _ => return Err(refuse::marker_multiple()),
        };
        let table = Table::of(self, target);
        if table == Table::None || Table::of(self, mark) != table {
            return Err(refuse::marker_mismatch(verb, &spelled(self, mark), &spelled(self, target)));
        }
        let path = law::marked_path(self, source, mark);
        if path.grouped_on_path {
            return Err(refuse::source_aggregate(verb));
        }
        if path.bound_on_path {
            return Err(refuse::bounded_mutation(verb));
        }
        if path.bound_elsewhere {
            return Err(refuse::outside("a bound among a mutation source's other relations"));
        }
        Ok((law::arrival(self.rel(source).heading(), locator, verb)?, mark))
    }
}

/// NAME CLASH: a durable creation whose name exists refuses; a session
/// creation replaces a holder of its physical temp name owned by the same
/// durable namespace, whatever its kind, and refuses one owned by another.
fn clash(verb: &str, spec: CreatedSpec) -> Result<Created, Refusal> {
    let replaces = match spec.placement.creation().residence() {
        ObjectResidence::Durable if spec.durable_exists => {
            return Err(refuse::durable_clash(verb, &spec.placement.target_path()))
        }
        ObjectResidence::Durable => None,
        ObjectResidence::SessionShadow => {
            if let Some(held) = spec
                .placement
                .holders()
                .iter()
                .find(|h| h.owner.as_deref() != Some(spec.placement.namespace()))
            {
                return Err(refuse::temp_name_held(
                    verb,
                    &spec.placement.target_path(),
                    &spec.placement.created_path(),
                    held.owner.as_deref(),
                ));
            }
            spec.placement.holders().first().map(|h| h.shape)
        }
    };
    Ok(Created {
        placement: spec.placement,
        replaces,
        past_session: spec.past_session,
    })
}

/// THE CREATION CONNECTION (materialization-law §2): the target selects the
/// connection; the connections of the physical reads the source reaches are
/// a compatibility check. A source with no physical read creates on the
/// target's connection; one that reads another connection refuses, since
/// creation never transports rows between connections.
fn same_connection(arena: &impl Judging, verb: &str, source: RelId, placement: &Placement) -> Result<(), Refusal> {
    let target = placement.connection();
    let other = walk::reachable(arena, &[Child::Rel(source)]).rels.into_iter().find_map(|r| match arena.rel(r).kind() {
        RelKind::Read {
            source: ReadSource::Catalog { physical, namespace, .. },
            ..
        } if physical.connection != target => Some((physical.connection, namespace.clone())),
        _ => None,
    });
    match other {
        // Where `sys::` rows are physically visible to a creation is
        // docketed (materialization-law §2, requirement 4): counting the
        // connection that serves them settles nothing.
        Some((_, namespace)) if namespace == "sys" || namespace.starts_with("sys::") => {
            Err(refuse::creation_sys_source(verb, &placement.target_path(), &namespace))
        }
        Some((connection, _)) => Err(refuse::creation_connection(verb, &placement.target_path(), target, connection)),
        None => Ok(()),
    }
}

/// A receipt's columns as its heading is formed from them: each name, and
/// for a carried interior its relation's heading and the relation.
pub(crate) fn receipt_columns<'a>(
    arena: &'a impl Judging,
    header: &'a [Name],
    cells: &[ReceiptCell],
) -> Vec<(&'a Name, Option<(&'a Heading, RelId)>)> {
    header
        .iter()
        .zip(cells)
        .map(|(name, cell)| match cell {
            ReceiptCell::Const(_) => (name, None),
            ReceiptCell::Interior(rel) => (name, Some((arena.rel(*rel).heading(), *rel))),
        })
        .collect()
}

/// The parts of the row locator a row write reaches its rows by.
pub(crate) fn locator(rows: &RowWrite) -> Vec<PassengerId> {
    match rows {
        RowWrite::Replace { arrival, .. } | RowWrite::Remove { arrival } => arrival.locator(),
        RowWrite::Insert { .. } => Vec::new(),
    }
}

/// The act's derived facts, again from their deciders over the frozen
/// graph: the one mark, what stands on its path, the stored arrival, and
/// the pairing of columns.
#[cfg(debug_assertions)]
pub(crate) fn recheck(arena: &impl Judging, act: &EffectAct) -> bool {
    let Write::Rows { target, rows } = &act.write else {
        return true;
    };
    let Some(source) = act.source else {
        return false;
    };
    let marks = law::marks(arena, source);
    match rows {
        RowWrite::Insert { columns } => {
            marks.is_empty()
                && law::insertion(arena.rel(*target).heading(), arena.rel(source).heading(), &law::computed(arena, *target))
                    .ok()
                    .as_deref()
                    == Some(columns.as_slice())
        }
        RowWrite::Replace { arrival, .. } | RowWrite::Remove { arrival } => {
            let [(found, mark)] = marks.as_slice() else {
                return false;
            };
            let path = law::marked_path(arena, source, *mark);
            let arrives = law::arrival(arena.rel(source).heading(), found, &act.operation).ok().as_ref() == Some(arrival);
            let columns_agree = match rows {
                RowWrite::Replace { columns, .. } => {
                    law::replaced(arena, *target, source, *mark).ok().as_deref() == Some(columns.as_slice())
                }
                RowWrite::Remove { .. } => law::published(arena, *target, source, *mark).is_ok(),
                RowWrite::Insert { .. } => true,
            };
            !path.bound_on_path
                && !path.bound_elsewhere
                && !path.grouped_on_path
                && arrives
                && columns_agree
        }
    }
}
