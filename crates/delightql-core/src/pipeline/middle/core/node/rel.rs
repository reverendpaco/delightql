// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Relation constructors other than the comma run. Each forms the node's
//! heading through `heading::form`, computes its free binders and effect
//! class, and pushes the node.

use super::walk::Child;
use super::{
    Bound, Cell, ExprKind, HeaderSlot, Item, Naming, OrderKey, PipeOp, Qual,
    ReadAccess, ReadSource, RelKind, RelNode, SetOpKind, Slot,
};
use crate::pipeline::middle::core::decide;
use crate::pipeline::middle::core::graph::{Arena, Builder, Judging};
use crate::pipeline::middle::core::heading::form::{self, Formed, ItemForm, ReadForm};
use crate::pipeline::middle::core::heading::{Evidence, Heading, Interior, Known, Layout, MintOrigin, Name, NameState, Origin, Position};
use crate::pipeline::middle::core::ids::{BinderId, ExprId, InstanceId, RelId, TruthId};
use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::core::switches::Switches;
use crate::pipeline::middle::facade::CmpOp;

/// A written slot of a positional access.
pub(crate) enum SlotSpec {
    Bind(Name),
    Anon,
    Constraint(ExprId),
}

pub(crate) enum AccessSpec {
    All,
    Unasked,
    Slots(Vec<SlotSpec>),
}

/// A written position of an anonymous table's header.
pub(crate) enum HeaderSpec {
    Bind(Name),
    /// A column of a table written with no header.
    Anon,
    /// `_` in a written header.
    Disregard,
    /// A ground term: a constraint on the column.
    Constraint(ExprId),
    /// A qualified name: a constraint on the column by the position it
    /// reuses, whose name the header writes.
    Reuse { value: ExprId, name: Name },
}

/// One clause of a definition family as its constructor receives it: its
/// dispatch guard, its body (projected through its head), and whether its
/// head wears the fixpoint badge.
pub(crate) struct ClauseSpec {
    pub(crate) guard: Option<TruthId>,
    pub(crate) body: RelId,
    pub(crate) badged: bool,
    /// Whether the clause's head declares its positions (a listed head, a
    /// fact) rather than publishing its body's heading (a glob head).
    pub(crate) closed: bool,
    /// Whether the clause is a fact's.
    pub(crate) fact: bool,
}


/// The receipt column that carries a directive's payload (receipt-algebra:
/// the conventional `returned` payload).
const RETURNED: &str = "returned";
impl Builder {
    pub(in crate::pipeline::middle::core::node) fn push_relation(&mut self, kind: RelKind, heading: Heading) -> RelId {
        let fv = rel_fv(self, &kind);
        self.push_rel(RelNode { kind, heading, fv })
    }

    /// A read of a catalog table or view.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn catalog_read(
        &mut self,
        name: Name,
        namespace: String,
        entity: Option<i64>,
        columns: Vec<super::CatalogColumn>,
        physical: super::Physical,
        typed: bool,
        access: AccessSpec,
        switches: &Switches,
    ) -> Result<RelId, Refusal> {
        let names: Vec<Name> = columns.iter().map(|c| c.name.clone()).collect();
        let structures: Vec<Interior> = columns.iter().map(|c| decide::document::declared(c.class, c.stored)).collect();
        let source = form::form(Formed::Catalog {
            columns: &names,
            structures: &structures,
        })?;
        let (access, heading) = read_access(self, &source, access, switches)?;
        Ok(self.push_relation(
            RelKind::Read {
                source: ReadSource::Catalog {
                    name,
                    namespace,
                    entity,
                    columns,
                    locator: Vec::new(),
                    physical,
                    typed,
                },
                access,
            },
            heading,
        ))
    }

    /// A read of the object the creation whose receipt is `receipt` makes,
    /// under the access its parens ask for.
    pub(crate) fn created_read(
        &mut self,
        receipt: RelId,
        name: Name,
        columns: Vec<super::CatalogColumn>,
        physical: super::Physical,
        access: AccessSpec,
        switches: &Switches,
    ) -> Result<RelId, Refusal> {
        let names: Vec<Name> = columns.iter().map(|c| c.name.clone()).collect();
        let structures: Vec<Interior> = columns.iter().map(|c| decide::document::declared(c.class, c.stored)).collect();
        let source = form::form(Formed::Catalog {
            columns: &names,
            structures: &structures,
        })?;
        let (access, heading) = read_access(self, &source, access, switches)?;
        Ok(self.push_relation(
            RelKind::Read {
                source: ReadSource::Created {
                    receipt,
                    name,
                    columns,
                    physical,
                },
                access,
            },
            heading,
        ))
    }

    /// A target table function applied to its arguments, under the access
    /// its parens ask for: one slot per column the function delivers, by
    /// position, as any read's.
    pub(crate) fn function_read(
        &mut self,
        name: Name,
        args: Vec<ExprId>,
        columns: Vec<super::CatalogColumn>,
        physical: super::Physical,
        access: AccessSpec,
        switches: &Switches,
    ) -> Result<RelId, Refusal> {
        self.observe(args.iter().copied(), "a table function's argument")?;
        let names: Vec<Name> = columns.iter().map(|c| c.name.clone()).collect();
        let structures: Vec<Interior> = columns.iter().map(|c| decide::document::declared(c.class, c.stored)).collect();
        let source = form::form(Formed::Catalog {
            columns: &names,
            structures: &structures,
        })?;
        let (access, heading) = read_access(self, &source, access, switches)?;
        Ok(self.push_relation(
            RelKind::Read {
                source: ReadSource::Function {
                    name,
                    args,
                    columns,
                    physical,
                },
                access,
            },
            heading,
        ))
    }

    /// A read of a stored table marked as a mutation's source (`!!`): the
    /// read, and its row locator born with it as hidden positions (W5 #13,
    /// A5), holding the table's row identity decided here from `identity`.
    /// The locator is a passenger no reference names; the forms that keep
    /// their operand's rows carry it, and every other form drops it.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn marked_catalog_read(
        &mut self,
        name: Name,
        namespace: String,
        entity: Option<i64>,
        columns: Vec<super::CatalogColumn>,
        identity: &crate::pipeline::middle::facade::RowIdentityFacts,
        physical: super::Physical,
        typed: bool,
        access: AccessSpec,
        switches: &Switches,
    ) -> Result<RelId, Refusal> {
        let reached = decide::mutation::row_locator(&name, &columns, identity)?;
        let names: Vec<Name> = columns.iter().map(|c| c.name.clone()).collect();
        let structures: Vec<Interior> = columns.iter().map(|c| decide::document::declared(c.class, c.stored)).collect();
        let source = form::form(Formed::Catalog {
            columns: &names,
            structures: &structures,
        })?;
        let (access, read) = read_access(self, &source, access, switches)?;
        let locator: Vec<crate::pipeline::middle::core::ids::PassengerId> = reached
            .into_iter()
            .map(|part| self.push_passenger(super::Passenger::RowLocator(part)))
            .collect();
        let heading = form::form(Formed::Marked {
            read: &read,
            locator: &locator,
        })?;
        Ok(self.push_relation(
            RelKind::Read {
                source: ReadSource::Catalog {
                    name,
                    namespace,
                    entity,
                    columns,
                    locator,
                    physical,
                    typed,
                },
                access,
            },
            heading,
        ))
    }

    /// A read of a relation built in this graph: a query-local or
    /// clause-local binding's body, or a relation formal's actual.
    pub(crate) fn local_read(&mut self, source: RelId, access: AccessSpec, switches: &Switches) -> Result<RelId, Refusal> {
        let (access, heading) = read_access(self, self.rel(source).heading(), access, switches)?;
        Ok(self.push_relation(
            RelKind::Read {
                source: ReadSource::Local(source),
                access,
            },
            heading,
        ))
    }

    /// The act's staging of a relation it consumes: the relation's rows,
    /// evaluated once at the act, under its own heading.
    pub(crate) fn staged(&mut self, source: RelId) -> Result<RelId, Refusal> {
        let heading = form::form(Formed::Staged {
            input: self.rel(source).heading(),
        })?;
        Ok(self.push_relation(
            RelKind::Read {
                source: ReadSource::Staged(source),
                access: ReadAccess::All,
            },
            heading,
        ))
    }

    /// An anonymous table. A repeated header binder constrains its cell
    /// equal to the earlier one's within each row and publishes nothing;
    /// the equality's class is decided here. A stropped header name is an
    /// authored name, not a variable: it never repeats, and a header that
    /// would publish one name twice (stropped or not) refuses.
    pub(crate) fn lit(
        &mut self,
        header: Vec<HeaderSpec>,
        rows: Vec<Vec<ExprId>>,
        aliased: bool,
        switches: &Switches,
    ) -> Result<RelId, Refusal> {
        let mut slots: Vec<HeaderSlot> = Vec::with_capacity(header.len());
        // The names a qualified header writes, with the position each
        // reuses: the same occurrence again repeats it within the row; a
        // binder or another occurrence may not write the name again.
        let mut reused: Vec<(Name, Option<Cell>, usize)> = Vec::new();
        for spec in header {
            slots.push(match spec {
                HeaderSpec::Anon => HeaderSlot::Anon,
                HeaderSpec::Disregard => HeaderSlot::Disregard,
                HeaderSpec::Constraint(value) => HeaderSlot::Constraint {
                    value,
                    class: constraint_class(self, value, switches),
                },
                HeaderSpec::Reuse { value, name } => {
                    let cell = referenced_cell(self, value);
                    match reused.iter().find(|(n, _, _)| *n == name) {
                        Some((_, earlier, first)) if *earlier == cell && cell.is_some() && !name.is_stropped() => {
                            slots.push(HeaderSlot::Reuse {
                                first: *first,
                                class: within_row_class(switches),
                            });
                            continue;
                        }
                        Some(_) => return Err(refuse::duplicate_header_name(&name)),
                        None => {}
                    }
                    if slots.iter().any(|s| matches!(s, HeaderSlot::Bind(n) if *n == name)) {
                        return Err(refuse::duplicate_header_name(&name));
                    }
                    // One occurrence in both roles leaves the header's
                    // constraint vacuous.
                    if cell.is_some() && rows.iter().flatten().any(|e| referenced_cell(self, *e) == cell) {
                        return Err(refuse::header_row_lvar(&name));
                    }
                    reused.push((name, cell, slots.len()));
                    HeaderSlot::Constraint {
                        value,
                        class: constraint_class(self, value, switches),
                    }
                }
                HeaderSpec::Bind(name) if reused.iter().any(|(n, _, _)| *n == name) => {
                    return Err(refuse::duplicate_header_name(&name))
                }
                HeaderSpec::Bind(name) => match slots
                    .iter()
                    .position(|s| matches!(s, HeaderSlot::Bind(n) if *n == name))
                {
                    Some(first) => match &slots[first] {
                        HeaderSlot::Bind(earlier) if !earlier.is_stropped() && !name.is_stropped() => {
                            HeaderSlot::Reuse {
                                first,
                                class: within_row_class(switches),
                            }
                        }
                        _ => return Err(refuse::duplicate_header_name(&name)),
                    },
                    None => HeaderSlot::Bind(name),
                },
            });
        }
        let structures = lit_structures(self, slots.len(), &rows)?;
        // A repeated binder and a constraint compare the column's cells.
        for (slot, column) in slots.iter().zip(&structures) {
            match slot {
                HeaderSlot::Reuse { first, .. } => unify(&structures[*first], column)?,
                HeaderSlot::Constraint { value, .. } => constrain(self, column, *value)?,
                HeaderSlot::Bind(_) | HeaderSlot::Anon | HeaderSlot::Disregard => {}
            }
        }
        let heading = form::form(Formed::Lit {
            header: &slots,
            aliased,
            structures: &structures,
        })?;
        Ok(self.push_relation(
            RelKind::Lit {
                header: slots,
                rows,
                aliased,
            },
            heading,
        ))
    }

    /// A pipe stage over a closed run. A reduction's items are judged
    /// here: each stands for its group (W5 #11). A grouping key and a
    /// distinct item are compared by their bytes.
    pub(crate) fn pipe(&mut self, input: RelId, op: PipeOp) -> Result<RelId, Refusal> {
        match &op {
            PipeOp::Group { keys, .. } => self.identify(keys.iter().map(|k| k.expr), "a grouping key")?,
            PipeOp::Distinct(items) => self.identify(items.iter().map(|i| i.expr), "a distinct item")?,
            // A removal's or cover's positions belong to the input they were
            // resolved against.
            PipeOp::ProjectOut(selection) if selection.input() != input => {
                return Err(refuse::elaboration_contract("a removal resolved against another input"))
            }
            PipeOp::Cover(selection) if selection.input() != input => {
                return Err(refuse::elaboration_contract("a cover resolved against another input"))
            }
            PipeOp::Project(_)
            | PipeOp::Embed(_)
            | PipeOp::ProjectOut(_)
            | PipeOp::Cover(_)
            | PipeOp::Carry(_) => {}
        }
        // An extraction a projection or embed publishes carries its node
        // beside it: one the stage makes (a path, a case over paths), or one
        // an expansion bound beside the position the item reads.
        if let PipeOp::Project(items) | PipeOp::Embed(items) = &op {
            for item in items {
                if self.node_carrier(item.expr).is_some()
                    || structure_of(self, Some(input), item.expr)?.evidence != Evidence::Extracted
                {
                    continue;
                }
                let carries = match self.expr(item.expr).kind() {
                    ExprKind::Path { .. } | ExprKind::Case { .. } => has_node(self, item.expr)?,
                    ExprKind::Col(..) => node_road(self, item.expr) == Some(NodeRoad::Bound),
                    _ => false,
                };
                if carries {
                    self.push_passenger(super::Passenger::Node { extraction: item.expr });
                }
            }
        }
        let heading = pipe_heading(self, input, &op)?;
        if let Some(item) = per_row_item(self, input, &op, &heading) {
            return Err(refuse::per_row_in_reduction(&item));
        }
        Ok(self.push_relation(RelKind::Pipe { input, op }, heading))
    }

    /// A removal over a closed run: the position of the run each selector
    /// reads, resolved here against that run, removed.
    pub(crate) fn remove(&mut self, input: RelId, selectors: Vec<ExprId>) -> Result<RelId, Refusal> {
        let selection = select(self, input, selectors.into_iter().map(|s| (s, ())).collect())
            .ok_or_else(|| refuse::column("?", "the selector names no output of the stage's input"))?;
        self.pipe(input, PipeOp::ProjectOut(selection))
    }

    /// A cover over a closed run: the position of the run each target reads,
    /// resolved here against that run, takes its paired value.
    pub(crate) fn cover(&mut self, input: RelId, pairs: Vec<(ExprId, ExprId)>) -> Result<RelId, Refusal> {
        let selection = select(self, input, pairs)
            .ok_or_else(|| refuse::column("?", "the cover target names no output of the stage's input"))?;
        self.pipe(input, PipeOp::Cover(selection))
    }

    /// An ordering, with the bound that consumes it, over a closed run.
    pub(crate) fn order(
        &mut self,
        input: RelId,
        keys: Vec<OrderKey>,
        bound: Option<Bound>,
    ) -> Result<RelId, Refusal> {
        self.observe(keys.iter().map(|k| k.expr), "an ordering key")?;
        let heading = form::form(Formed::Same {
            input: self.rel(input).heading(),
        })?;
        Ok(self.push_relation(RelKind::Order { input, keys, bound }, heading))
    }

    /// A reflection of its input's heading as data.
    pub(crate) fn meta(&mut self, input: RelId) -> Result<RelId, Refusal> {
        let heading = form::form(Formed::Meta)?;
        Ok(self.push_relation(RelKind::Meta { input }, heading))
    }

    /// THE SIGNED WITNESS over `input` (`+-`): every row of its operand
    /// widened with `met = 1`, or one all-NULL row with `met = 0` where it
    /// has none — THE TOTAL LEDGER's rule, row by row.
    pub(crate) fn signed_witness(&mut self, input: RelId) -> Result<RelId, Refusal> {
        self.witnessed(input)
    }

    /// The rows of `input` witnessed (THE TOTAL LEDGER): each widened with
    /// `met = 1`, or, where it has none, one proxy row with `met = 0` whose
    /// positions are NULL except the conventional `returned` payload, which
    /// holds the empty relation.
    pub(crate) fn witnessed(&mut self, input: RelId) -> Result<RelId, Refusal> {
        let empty = self
            .rel(input)
            .heading()
            .displayed()
            .enumerate()
            .filter(|(_, (_, p))| {
                matches!(p.interior.known, Known::Shape(_, Origin::Carried(_)))
                    && p.answering_name().is_some_and(|n| n.as_str() == RETURNED)
            })
            .map(|(k, _)| k)
            .collect();
        let heading = form::form(Formed::Witnessed {
            input: self.rel(input).heading(),
        })?;
        Ok(self.push_relation(RelKind::Witnessed { input, empty }, heading))
    }

    /// An application of a definition instance: a read boundary.
    pub(crate) fn apply(&mut self, instance: InstanceId) -> Result<RelId, Refusal> {
        let body = self.instance(instance).body();
        let heading = form::form(Formed::Instance {
            body: self.rel(body).heading(),
        })?;
        Ok(self.push_relation(RelKind::Apply { instance }, heading))
    }

    /// A union step. Corresponding and smart unions align by answered name;
    /// a position whose name is lost or minted aligns with nothing. A
    /// correlation written for the step is decided here, once: the pairs it
    /// matches (`decide::setop::correlation`), each pair's sides as set
    /// identity compares them, the class of the match (set identity), and
    /// how the step keeps what it matches under `gate`.
    pub(crate) fn set_op(
        &mut self,
        left: RelId,
        right: RelId,
        op: SetOpKind,
        written: Option<Vec<super::setop::Atom>>,
        gate: decide::setop::Gate,
    ) -> Result<RelId, Refusal> {
        let alignment = align(self.rel(left).heading(), self.rel(right).heading(), op)?;
        let heading = form::form(Formed::SetOp {
            left: self.rel(left).heading(),
            right: self.rel(right).heading(),
            alignment: &alignment,
        })?;
        let correlation = match written {
            None => None,
            Some(atoms) => Some(union_correlation(self, left, right, op, &alignment, atoms, gate)?),
        };
        Ok(self.push_relation(
            RelKind::SetOp {
                left,
                right,
                op,
                alignment,
                correlation,
            },
            heading,
        ))
    }
}

/// A union step's correlation under the gate, from its written atom: its
/// pairs, their membership and its class.
pub(crate) fn union_correlation(
    arena: &impl Judging,
    left: RelId,
    right: RelId,
    op: SetOpKind,
    alignment: &[(Option<usize>, Option<usize>)],
    atoms: Vec<super::setop::Atom>,
    gate: decide::setop::Gate,
) -> Result<super::Correlation, Refusal> {
    let mode = match op {
        SetOpKind::Positional => decide::setop::Mode::Ordinals,
        SetOpKind::Corresponding | SetOpKind::Smart => decide::setop::Mode::Names,
    };
    let pairs = decide::setop::correlation(mode, arena.rel(left).heading(), arena.rel(right).heading(), &atoms)?;
    let membership = pair_membership(arena, left, right, &pairs, "a set-operation correlation")?;
    decide::setop::gated(gate, alignment, &pairs)?;
    Ok(super::Correlation::of(pairs, membership, decide::equality::set_identity(), atoms))
}

/// One clause of a family as CLAUSE AGREEMENT reads it: the heading it
/// publishes, whether its head is closed, whether it is a fact's.
pub(crate) struct ClauseHeading<'a> {
    pub(crate) heading: &'a Heading,
    pub(crate) closed: bool,
    pub(crate) fact: bool,
}

/// Open and closed heads do not mix in one family. A head's form is read
/// from the head alone, so a family is judged here before any clause body is
/// read. Answers whether the family is closed.
pub(crate) fn judge_forms(name: &str, closed: impl IntoIterator<Item = bool>) -> Result<bool, Refusal> {
    let (mut any_closed, mut any_open) = (false, false);
    for c in closed {
        if c {
            any_closed = true;
        } else {
            any_open = true;
        }
    }
    if any_closed && any_open {
        return Err(refuse::head_mixed_forms(name));
    }
    Ok(!any_open)
}

/// CLAUSE AGREEMENT (heads-law), the one judgment of a family's clauses,
/// whether their headings come from elaborated bodies or, where a family is
/// declared, from closed heads alone: open and closed heads do not mix; the
/// clauses publish one heading (closed heads by arity and by name offers);
/// THE GROUND-POSITION RULE binds a family that is not facts alone, and a
/// fact-only position nobody names is judged where it is read.
pub(crate) fn judge_family(name: &str, family: &str, clauses: &[ClauseHeading<'_>]) -> Result<Heading, Refusal> {
    let closed = judge_forms(name, clauses.iter().map(|c| c.closed))?;
    let headings: Vec<&Heading> = clauses.iter().map(|c| c.heading).collect();
    let mut heading = form::form(Formed::Family {
        name,
        clauses: &headings,
        closed,
    })?;
    let abstained: Vec<usize> = heading
        .displayed()
        .enumerate()
        .filter(|(_, (_, p))| p.name == NameState::Minted(MintOrigin::Abstained))
        .map(|(k, _)| k)
        .collect();
    match (clauses.iter().all(|c| c.fact), abstained.first()) {
        (_, None) => {}
        (false, Some(k)) => return Err(refuse::unnamed_ground_position(name, k + 1)),
        // A fact-only family's position nobody names publishes the
        // canonical fact name `f|N|` (RULINGS 2026-08-05).
        (true, Some(_)) => heading = form::fact_named(&heading, family),
    }
    Ok(heading)
}

impl Builder {
    /// Values compared as set members, each judged by the one set-identity
    /// judgment.
    fn identify(&self, values: impl IntoIterator<Item = ExprId>, consumer: &str) -> Result<(), Refusal> {
        for value in values {
            decide::document::set_member(&[&structure_of(self, None, value)?], consumer)?;
        }
        Ok(())
    }

    /// A minus step (THE MINUS LAW): its arms aligned by name, each aligned
    /// pair's two sides judged together as set members, as grouping and
    /// distinct judge theirs, its match class set identity; it publishes the
    /// left arm's heading. A correlation written for the step narrows the
    /// match to the pairs it names (`decide::setop::correlation`); the
    /// same-names demand stands.
    pub(crate) fn minus(&mut self, left: RelId, right: RelId) -> Result<RelId, Refusal> {
        let pairs = minus_pairs(self.rel(left).heading(), self.rel(right).heading())?;
        let membership = pair_membership(self, left, right, &pairs, "a minus step")?;
        let heading = minus_heading(self.rel(left).heading())?;
        Ok(self.push_relation(
            RelKind::Minus {
                left,
                right,
                pairs,
                membership,
                class: decide::equality::set_identity(),
            },
            heading,
        ))
    }
}

/// Each pair of a set step as set identity compares its two sides
/// (`decide::document::set_member`).
pub(crate) fn pair_membership(
    arena: &impl Judging,
    left: RelId,
    right: RelId,
    pairs: &[(usize, usize)],
    consumer: &str,
) -> Result<Vec<decide::document::Membership>, Refusal> {
    pairs
        .iter()
        .map(|(l, r)| {
            decide::document::set_member(
                &[
                    &arena.rel(left).heading().positions()[*l].interior,
                    &arena.rel(right).heading().positions()[*r].interior,
                ],
                consumer,
            )
        })
        .collect()
}

/// THE MINUS ALIGNMENT: every displayed position of the left arm by the
/// name it answers to, against the one right position answering to it, and
/// the right arm publishing no other. A minted or lost name aligns with
/// nothing (A MINT ALIGNS WITH NOTHING), so the step refuses, teaching the
/// rename or baptism.
pub(crate) fn minus_pairs(left: &Heading, right: &Heading) -> Result<Vec<(usize, usize)>, Refusal> {
    use crate::pipeline::middle::core::heading::correspondence::answers_to;
    let mut pairs = Vec::new();
    for (l, p) in left.displayed() {
        let name = p.answering_name().ok_or_else(refuse::minus_names)?;
        match answers_to(right, name).as_slice() {
            [r] => pairs.push((l, *r)),
            _ => return Err(refuse::minus_names()),
        }
    }
    if right.displayed().count() != pairs.len() {
        return Err(refuse::minus_names());
    }
    Ok(pairs)
}

/// A minus step's heading: the left arm's displayed positions, dequalified.
pub(crate) fn minus_heading(left: &Heading) -> Result<Heading, Refusal> {
    let hidden: Vec<usize> = left
        .positions()
        .iter()
        .enumerate()
        .filter(|(_, p)| matches!(p.visibility, crate::pipeline::middle::core::heading::Visibility::Hidden(_)))
        .map(|(i, _)| i)
        .collect();
    form::form(Formed::Without { input: left, dropped: &hidden })
}

/// Two cells one binder names twice (a repeated header binder or slot): a
/// within-row comparison, judged as any comparison of structures.
fn unify(earlier: &Interior, later: &Interior) -> Result<(), Refusal> {
    use decide::document::{comparable, Compared, Nulls};
    comparable(
        Compared {
            structure: earlier,
            nulls: Nulls::Unknown,
        },
        Compared {
            structure: later,
            nulls: Nulls::Unknown,
        },
        CmpOp::NullSafeEqual,
        super::Consumer::Filter,
    )
}

/// A column constrained to equal a value (a ground term or a qualified
/// reference), judged as any comparison of structures.
pub(crate) fn constrain(arena: &impl Judging, column: &Interior, value: ExprId) -> Result<(), Refusal> {
    use decide::document::{comparable, Compared, Nulls};
    let structure = structure_of(arena, None, value)?;
    comparable(
        Compared {
            structure: column,
            nulls: Nulls::Unknown,
        },
        super::expr::compared(arena, &structure, value),
        CmpOp::NullSafeEqual,
        super::Consumer::Filter,
    )
}

/// The equality class of a comparison within one row of one occurrence,
/// consumed as a filter: a repeated binder, a repeated slot, a ground slot.
fn within_row_class(switches: &Switches) -> super::EqClass {
    decide::equality::class(CmpOp::NullSafeEqual, 1, true, super::Consumer::Filter, switches)
}

/// The equality class of a slot's constraint: the slot's column, one row
/// of this read, compared with the value, which may read other row
/// occurrences (a qualified slot reuses another occurrence's column). A
/// constraint relating two occurrences is correspondence; a ground one is a
/// within-row test.
/// The class of a constraint's equality: the constrained cell against the
/// value, which reads the occurrences it names, or, naming none, the rows of
/// the relation that supplies it (equality-law Membership: the probe's
/// source decides, and a value some relation supplies is no ground value).
pub(crate) fn constraint_class(arena: &impl Judging, value: ExprId, switches: &Switches) -> super::EqClass {
    let named = arena.expr(value).occurrences().len();
    let read = if named == 0 && super::expr::relation_supplied(arena, value) { 1 } else { named };
    decide::equality::class(CmpOp::NullSafeEqual, 1 + read, true, super::Consumer::Filter, switches)
}

pub(crate) fn read_access(
    arena: &impl Judging,
    source: &Heading,
    access: AccessSpec,
    switches: &Switches,
) -> Result<(ReadAccess, Heading), Refusal> {
    match access {
        AccessSpec::All => Ok((
            ReadAccess::All,
            form::form(Formed::Read {
                source,
                access: ReadForm::All,
            })?,
        )),
        AccessSpec::Unasked => Ok((
            ReadAccess::Unasked,
            form::form(Formed::Read {
                source,
                access: ReadForm::Unasked,
            })?,
        )),
        AccessSpec::Slots(specs) => {
            let names: Vec<Option<Name>> = specs
                .iter()
                .map(|s| match s {
                    SlotSpec::Bind(n) => Some(n.clone()),
                    SlotSpec::Anon | SlotSpec::Constraint(_) => None,
                })
                .collect();
            let heading = form::form(Formed::Read {
                source,
                access: ReadForm::Slots(&names),
            })?;
            // The read refused a slot count other than the displayed width.
            let displayed: Vec<&Interior> = source.displayed().map(|(_, p)| &p.interior).collect();
            let mut slots = Vec::with_capacity(specs.len());
            for (i, spec) in specs.into_iter().enumerate() {
                match &spec {
                    SlotSpec::Bind(name) => {
                        if let Some(first) = names[..i].iter().position(|earlier| earlier.as_ref() == Some(name)) {
                            unify(displayed[first], displayed[i])?;
                        }
                    }
                    SlotSpec::Constraint(value) => constrain(arena, displayed[i], *value)?,
                    SlotSpec::Anon => {}
                }
                slots.push(match spec {
                    SlotSpec::Bind(name) => match names[..i]
                        .iter()
                        .position(|earlier| earlier.as_ref() == Some(&name))
                    {
                        Some(first) => Slot::Reuse {
                            first,
                            class: within_row_class(switches),
                        },
                        None => Slot::Bind(name),
                    },
                    SlotSpec::Anon => Slot::Anon,
                    SlotSpec::Constraint(value) => Slot::Constraint {
                        value,
                        class: constraint_class(arena, value, switches),
                    },
                });
            }
            Ok((ReadAccess::Slots(slots), heading))
        }
    }
}

/// The names a stored positional access binds, slot by slot, as its
/// heading was formed from them.
pub(crate) fn slot_names(slots: &[Slot]) -> Vec<Option<Name>> {
    let mut out: Vec<Option<Name>> = Vec::with_capacity(slots.len());
    for slot in slots {
        let name = match slot {
            Slot::Bind(n) => Some(n.clone()),
            Slot::Reuse { first, .. } => out.get(*first).cloned().flatten(),
            Slot::Anon | Slot::Constraint { .. } => None,
        };
        out.push(name);
    }
    out
}

/// The position a reference reads, when the reference is a column of a
/// member or a merged key.
pub(crate) fn referenced_position(arena: &impl Judging, expr: ExprId) -> Option<Position> {
    match arena.expr(expr).kind() {
        ExprKind::Col(binder, i) => arena
            .binder(*binder)
            .heading()
            .positions()
            .get(*i as usize)
            .cloned(),
        ExprKind::Merged(m) => cell_position(arena, Cell::Merged(*m)),
        // A value formal reads its actual's position across the definition's
        // boundary: the boundary's judgment of it, no node.
        ExprKind::Across(actual) => {
            let p = referenced_position(arena, *actual)?;
            Some(Position {
                interior: decide::document::read(&p.interior),
                node: None,
                ..p
            })
        }
        ExprKind::Const(_)
        | ExprKind::Construct { .. }
        | ExprKind::Path { .. }
        | ExprKind::Call { .. }
        | ExprKind::Window { .. }
        | ExprKind::Infix(..)
        | ExprKind::Case { .. }
        | ExprKind::Crossed(_)
        | ExprKind::Scalar { .. }
        | ExprKind::Passenger(_)
        | ExprKind::Collect { .. }
        | ExprKind::Metadata { .. }
        | ExprKind::Pick { .. }
        | ExprKind::Argument { .. } => None,
    }
}

/// The run cell a reference reads, when it reads one.
pub(crate) fn referenced_cell(arena: &impl Judging, expr: ExprId) -> Option<Cell> {
    match arena.expr(expr).kind() {
        ExprKind::Col(b, i) => Some(Cell::Col(*b, *i)),
        ExprKind::Merged(m) => Some(Cell::Merged(*m)),
        ExprKind::Across(actual) => referenced_cell(arena, *actual),
        _ => None,
    }
}

/// Each reference's output position of a stage's input (a closed run): the
/// positions a removal drops or a cover writes, each with what it pairs, as a
/// selection of that input. `None` when a reference names no output of it.
pub(crate) fn select<T>(arena: &impl Judging, input: RelId, references: Vec<(ExprId, T)>) -> Option<super::Selection<T>> {
    let RelKind::Run(run) = arena.rel(input).kind() else {
        return None;
    };
    let items = references
        .into_iter()
        .map(|(e, paired)| {
            let cell = referenced_cell(arena, e)?;
            let at = run.outputs().iter().position(|c| *c == cell)?;
            Some((super::Selected::at(e, u16::try_from(at).ok()?), paired))
        })
        .collect::<Option<Vec<_>>>()?;
    Some(super::Selection::of(input, items))
}

/// The position a run output cell holds: a merged key keeps its left
/// operand's name state and holds the structure its merge decided.
pub(crate) fn cell_position(arena: &impl Judging, cell: Cell) -> Option<Position> {
    match cell {
        Cell::Col(binder, i) => arena
            .binder(binder)
            .heading()
            .positions()
            .get(i as usize)
            .cloned(),
        Cell::Merged(m) => Some(Position {
            interior: arena.merge(m).structure().clone(),
            ..cell_position(arena, arena.merge(m).left())?
        }),
    }
}

/// One entry of a heading-forming item list: a stage item, or a nested
/// collection level.
enum Entry<'a> {
    Item(&'a Item),
    Nested(&'a Name, crate::pipeline::middle::core::heading::Layout, &'a [super::CollectMember]),
    Metadata(&'a Name, &'a super::MetaLevel),
}

/// Whether an item reads a position it publishes as written. A hidden
/// position (a configured value's passenger, read through its formal) has
/// no name to carry: read as an item, it is a value like a computed one.
fn referenced(arena: &impl Judging, expr: ExprId) -> bool {
    referenced_position(arena, expr).is_some_and(|p| !matches!(p.visibility, crate::pipeline::middle::core::heading::Visibility::Hidden(_)))
}

fn fallback_position() -> Position {
    use crate::pipeline::middle::core::heading::{Binding, MintOrigin, NameState};
    Position::published(NameState::Minted(MintOrigin::Expr), Binding::Bare)
}

fn abstained_position() -> Position {
    use crate::pipeline::middle::core::heading::{Binding, MintOrigin, NameState};
    Position::published(NameState::Minted(MintOrigin::Abstained), Binding::Bare)
}

/// The position an unqualified glob item covers: the position of the
/// stage's input the item's cell is, as that input publishes it.
fn covered_position(arena: &impl Judging, input: Option<RelId>, expr: ExprId) -> Option<Position> {
    let cell = referenced_cell(arena, expr)?;
    let input = input?;
    let RelKind::Run(run) = arena.rel(input).kind() else {
        return None;
    };
    let at = run.outputs().iter().position(|c| *c == cell)?;
    arena.rel(input).heading().positions().get(at).cloned()
}

fn items_heading(
    arena: &impl Judging,
    input: Option<RelId>,
    prefix: &[Position],
    items: &[Item],
    carried: &[Position],
    projection: bool,
) -> Result<Heading, Refusal> {
    let entries: Vec<Entry<'_>> = items.iter().map(Entry::Item).collect();
    entries_heading(arena, input, prefix, &entries, carried, None, projection)
}

fn entries_heading(
    arena: &impl Judging,
    input: Option<RelId>,
    prefix: &[Position],
    entries: &[Entry<'_>],
    hidden: &[Position],
    enclosing: Option<ExprId>,
    projection: bool,
) -> Result<Heading, Refusal> {
    let mut carried: Vec<Position> = Vec::new();
    let mut interiors: Vec<Interior> = Vec::new();
    let mut nodes: Vec<Option<crate::pipeline::middle::core::ids::PassengerId>> = Vec::new();
    for entry in entries {
        match entry {
            Entry::Item(item) => {
                carried.push(match item.naming {
                    Naming::Glob => covered_position(arena, input, item.expr)
                        .or_else(|| referenced_position(arena, item.expr))
                        .unwrap_or_else(fallback_position),
                    Naming::Reference | Naming::QualifiedGlob | Naming::As(_) => {
                        referenced_position(arena, item.expr).unwrap_or_else(fallback_position)
                    }
                    Naming::Computed => fallback_position(),
                    Naming::Abstain => abstained_position(),
                });
                interiors.push(structure_of(arena, input, item.expr)?);
                // The node the stage carries beside the item, else the one its
                // position carries; a carried position takes the stage's.
                let node = arena.node_carrier(item.expr).or_else(|| node_of(arena, item.expr));
                if let (Some(position), Some(_)) = (carried.last_mut(), node) {
                    position.node = node;
                }
                nodes.push(node);
            }
            Entry::Nested(_, layout, members) => {
                carried.push(fallback_position());
                let origin = enclosing.expect("a nested level stands inside a collection");
                interiors.push(collected_interior(arena, input, *layout, members, origin)?);
                nodes.push(None);
            }
            Entry::Metadata(_, level) => {
                carried.push(fallback_position());
                let origin = enclosing.expect("a metadata level stands inside a collection");
                interiors.push(metadata_interior(arena, input, level, origin)?);
                nodes.push(None);
            }
        }
    }
    let mut forms: Vec<ItemForm<'_>> = prefix.iter().map(ItemForm::Globbed).collect();
    for (i, entry) in entries.iter().enumerate() {
        let structured = interiors[i] != Interior::flat();
        forms.push(match entry {
            Entry::Nested(name, _, _) | Entry::Metadata(name, _) => ItemForm::Structured {
                name: Some(name),
                interior: interiors[i].clone(),
                node: None,
            },
            Entry::Item(item) => match &item.naming {
                Naming::As(name) if structured => ItemForm::Structured {
                    name: Some(name),
                    interior: interiors[i].clone(),
                    node: nodes[i],
                },
                Naming::As(name) => ItemForm::Baptized(name),
                Naming::Reference if referenced(arena, item.expr) => ItemForm::Carried(&carried[i]),
                Naming::Glob | Naming::QualifiedGlob => ItemForm::Globbed(&carried[i]),
                Naming::Computed | Naming::Reference if structured => ItemForm::Structured {
                    name: None,
                    interior: interiors[i].clone(),
                    node: nodes[i],
                },
                Naming::Computed | Naming::Reference => ItemForm::Computed,
                Naming::Abstain => ItemForm::Abstained,
            },
        });
    }
    form::form(Formed::Items {
        items: &forms,
        carried: hidden,
        projection,
    })
}

/// A nested level's interior: the collection of its members' rows, formed
/// by the collection that holds it.
fn collected_interior(
    arena: &impl Judging,
    input: Option<RelId>,
    layout: crate::pipeline::middle::core::heading::Layout,
    members: &[super::CollectMember],
    origin: ExprId,
) -> Result<Interior, Refusal> {
    Ok(Interior::made_with(Known::Shape(
        Box::new(collect_heading(arena, input, members, origin)?),
        match layout {
            crate::pipeline::middle::core::heading::Layout::Record => Origin::Collection(origin),
            crate::pipeline::middle::core::heading::Layout::Tuple => Origin::Tuples(origin),
        },
    )))
}

/// A metadata level's interior: an object keyed by its key's values, each
/// holding what its target holds.
fn metadata_interior(
    arena: &impl Judging,
    input: Option<RelId>,
    level: &super::MetaLevel,
    origin: ExprId,
) -> Result<Interior, Refusal> {
    let held = match &level.target {
        super::MetaTarget::Collect(layout, members) => collected_interior(arena, input, *layout, members, origin)?,
        super::MetaTarget::Group(inner) => metadata_interior(arena, input, inner, origin)?,
    };
    Ok(Interior::made_with(Known::Keyed(Box::new(held))))
}

/// A collected record's heading: its members, a nested level carrying its
/// own interior formed by the same collection.
fn collect_heading(
    arena: &impl Judging,
    input: Option<RelId>,
    members: &[super::CollectMember],
    origin: ExprId,
) -> Result<Heading, Refusal> {
    let entries: Vec<Entry<'_>> = members
        .iter()
        .map(|m| match m {
            super::CollectMember::Item(item) => Entry::Item(item),
            super::CollectMember::Nested(name, layout, inner) => Entry::Nested(name, *layout, inner),
            super::CollectMember::Metadata(name, level) => Entry::Metadata(name, level),
        })
        .collect();
    entries_heading(arena, input, &[], &entries, &[], Some(origin), false)
}

/// An anonymous table's column structures: each the join of its cells'
/// over the rows.
pub(crate) fn lit_structures(arena: &impl Judging, width: usize, rows: &[Vec<ExprId>]) -> Result<Vec<Interior>, Refusal> {
    let mut structures = Vec::with_capacity(width);
    for k in 0..width {
        let mut column: Option<Interior> = None;
        for e in rows.iter().filter_map(|row| row.get(k)) {
            let cell = structure_of(arena, None, *e)?;
            column = Some(match column {
                Some(column) => Interior::join(&column, &cell),
                None => cell,
            });
        }
        structures.push(column.unwrap_or_else(Interior::flat));
    }
    Ok(structures)
}

/// THE STRUCTURE OF A VALUE, decided from the value: a collection's rows,
/// a made record or tuple, what a path addresses, the structure of the
/// position a reference reads (a renamed structured value is still
/// structured), the join of a case's results, a crossed truth, a value
/// formal's actual read across its definition's boundary; any other value
/// is an ordinary one.
pub(crate) fn structure_of(arena: &impl Judging, input: Option<RelId>, expr: ExprId) -> Result<Interior, Refusal> {
    match arena.expr(expr).kind() {
        ExprKind::Collect { layout, members } => Ok(Interior::made_with(Known::Shape(
            Box::new(collect_heading(arena, input, members, expr)?),
            match layout {
                Layout::Record => Origin::Collection(expr),
                Layout::Tuple => Origin::Tuples(expr),
            },
        ))),
        ExprKind::Construct { layout, members } => {
            Ok(Interior::made_with(Known::One(Box::new(one_heading(arena, *layout, members)?), *layout)))
        }
        ExprKind::Metadata { level } => metadata_interior(arena, input, level, expr),
        // A delegate's payload is its chosen row's value.
        ExprKind::Pick { value, .. } => structure_of(arena, input, *value),
        ExprKind::Path { source, path } => decide::document::reach(&structure_of(arena, input, *source)?, path),
        ExprKind::Col(..) | ExprKind::Merged(_) => {
            Ok(referenced_position(arena, expr).map(|p| p.interior).unwrap_or_else(Interior::flat))
        }
        ExprKind::Across(actual) => Ok(decide::document::read(&structure_of(arena, input, *actual)?)),
        ExprKind::Argument { structure, .. } => Ok((**structure).clone()),
        ExprKind::Case { arms, default, .. } => {
            let mut joined: Option<Interior> = None;
            for result in arms.iter().map(|(_, r)| *r).chain(*default) {
                let s = structure_of(arena, input, result)?;
                joined = Some(match joined {
                    Some(joined) => Interior::join(&joined, &s),
                    None => s,
                });
            }
            Ok(joined.unwrap_or_else(Interior::flat))
        }
        // A scalar position holds its relation's one value, with that
        // position's structure.
        ExprKind::Scalar { rel, .. } => Ok(arena
            .rel(*rel)
            .heading()
            .displayed()
            .next()
            .map(|(_, p)| p.interior.clone())
            .unwrap_or_else(Interior::flat)),
        ExprKind::Crossed(_) => Ok(Interior::truth()),
        ExprKind::Const(crate::pipeline::middle::facade::LiteralValue::Null) => Ok(Interior::null()),
        ExprKind::Const(_)
        | ExprKind::Call { .. }
        | ExprKind::Window { .. }
        | ExprKind::Infix(..)
        | ExprKind::Passenger(_) => Ok(Interior::flat()),
    }
}

/// Whether a value's node (the value with its kind) is a value of its
/// own stage: a path's, a NULL's, a document's the language made, and a
/// case's whose every result has one.
pub(crate) fn has_node(arena: &impl Judging, expr: ExprId) -> Result<bool, Refusal> {
    match arena.expr(expr).kind() {
        ExprKind::Path { .. } | ExprKind::Const(crate::pipeline::middle::facade::LiteralValue::Null) => Ok(true),
        ExprKind::Case { arms, default, .. } => {
            for result in arms.iter().map(|(_, r)| *r).chain(*default) {
                if !has_node(arena, result)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        _ => Ok(structure_of(arena, None, expr)?.always_made()),
    }
}

/// Where a structured consumer (a path, a constructor member, an
/// expansion) reads the node of an extracted value: the value with its
/// kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NodeRoad {
    /// A path in the same expression: the node is read in place.
    InPlace,
    /// The passenger the value's stage carries beside it.
    Carried(crate::pipeline::middle::core::ids::PassengerId),
    /// A position an expansion bound, read in the run the expansion is a
    /// member of (or the stage reading that run): the node is read from the
    /// same element as the value.
    Bound,
}

/// THE NODE ROAD of an extracted value; `None` where no stage carries its
/// kind, which a structured consumer refuses.
pub(crate) fn node_road(arena: &impl Judging, expr: ExprId) -> Option<NodeRoad> {
    if let ExprKind::Path { .. } = arena.expr(expr).kind() {
        return Some(NodeRoad::InPlace);
    }
    if let Some(carrier) = node_of(arena, expr) {
        return Some(NodeRoad::Carried(carrier));
    }
    let ExprKind::Col(binder, i) = arena.expr(expr).kind() else {
        return None;
    };
    let RelKind::Unnest { value, expansion } = arena.rel(arena.binder(*binder).rel()?).kind() else {
        return None;
    };
    let mut published = 0;
    for level in &expansion.levels {
        for bind in &level.binds {
            if let super::BindRole::Publish(_) = bind.role {
                if published == *i as usize {
                    return bind_offers_node(arena, *value, level, bind).then_some(NodeRoad::Bound);
                }
                published += 1;
            }
        }
    }
    None
}

/// Whether an expansion's bind offers its node beside its value: a known
/// position holding an extracted value, what a path reaches, an element of
/// an iterating level.
pub(crate) fn bind_offers_node(arena: &impl Judging, value: ExprId, level: &super::Level, bind: &super::Bind) -> bool {
    use super::{BindAt, Reach};
    match &bind.at {
        BindAt::Position(k) => {
            level.reach == Reach::Known
                && structure_of(arena, None, value).is_ok_and(|s| {
                    let heading = match &s.known {
                        Known::Shape(h, _) => Some(&**h),
                        Known::PerArm(arms) => arms.first().map(|(h, _)| h),
                        Known::None | Known::One(..) | Known::Keyed(_) => None,
                    };
                    heading
                        .and_then(|h| h.positions().get(*k as usize))
                        .is_some_and(|p| p.interior.evidence == Evidence::Extracted)
                })
        }
        BindAt::Path(_) => true,
        BindAt::Element => level.reach != Reach::Node,
        BindAt::Key => false,
    }
}

/// A structured consumer of an extracted value refuses where no stage
/// carries the value's kind.
pub(crate) fn node_reached(arena: &impl Judging, expr: ExprId, structure: &Interior) -> Result<(), Refusal> {
    if structure.evidence == Evidence::Extracted && node_road(arena, expr).is_none() {
        return Err(refuse::outside("an extracted value no stage carries the kind of"));
    }
    Ok(())
}

/// The passenger carrying a value's node beside it: the one its stage
/// births for it, or the one the position a reference reads carries.
pub(crate) fn node_of(arena: &impl Judging, expr: ExprId) -> Option<crate::pipeline::middle::core::ids::PassengerId> {
    match arena.expr(expr).kind() {
        ExprKind::Col(..) | ExprKind::Merged(_) => referenced_position(arena, expr).and_then(|p| p.node),
        _ => arena.node_carrier(expr),
    }
}

/// A made record's or tuple's member heading: each member under its key
/// (a record's) with its structure.
fn one_heading(arena: &impl Judging, layout: Layout, members: &[Item]) -> Result<Heading, Refusal> {
    let mut entries = Vec::with_capacity(members.len());
    for item in members {
        let key = match (layout, &item.naming) {
            (Layout::Tuple, _) => None,
            (Layout::Record, Naming::As(key)) => Some(key.clone()),
            (Layout::Record, Naming::Reference | Naming::Glob | Naming::QualifiedGlob) => {
                referenced_position(arena, item.expr).and_then(|p| p.answering_name().cloned())
            }
            (Layout::Record, Naming::Computed | Naming::Abstain) => None,
        };
        entries.push((key, structure_of(arena, None, item.expr)?));
    }
    form::form(Formed::One {
        layout,
        members: &entries,
    })
}

/// A reduction item of a group that does not stand for its group, by the
/// name it publishes; `None` when every item does or the stage is no
/// reduction.
pub(crate) fn per_row_item(arena: &impl Judging, input: RelId, op: &PipeOp, heading: &Heading) -> Option<String> {
    let PipeOp::Group { keys, reductions } = op else {
        return None;
    };
    let cells: Vec<Cell> = keys.iter().filter_map(|k| referenced_cell(arena, k.expr)).collect();
    let own: super::BinderSet = match arena.rel(input).kind() {
        RelKind::Run(run) => run.members().map(|m| m.binder()).collect(),
        _ => super::BinderSet::new(),
    };
    reductions.iter().enumerate().find_map(|(i, item)| {
        let per_row = super::expr::per_row_value(arena, item.expr, &cells, &own);
        decide::grade::reduction_item(per_row).err().map(|_| {
            heading
                .positions()
                .get(keys.len() + i)
                .map(|p| format!("`{}`", p.display()))
                .unwrap_or_else(|| "a reduction item".to_string())
        })
    })
}

/// The heading of a pipe stage.
pub(crate) fn pipe_heading(arena: &impl Judging, input: RelId, op: &PipeOp) -> Result<Heading, Refusal> {
    let input_heading = arena.rel(input).heading();
    let hidden: Vec<Position> = input_heading
        .positions()
        .iter()
        .filter(|p| matches!(p.visibility, crate::pipeline::middle::core::heading::Visibility::Hidden(_)))
        .cloned()
        .collect();
    // The nodes the stage's own extractions carry, as hidden positions.
    let nodes = |items: &[Item]| -> Vec<Position> {
        items
            .iter()
            .filter_map(|item| arena.node_carrier(item.expr))
            .map(|p| Position {
                visibility: crate::pipeline::middle::core::heading::Visibility::Hidden(p),
                ..Position::published(
                    crate::pipeline::middle::core::heading::NameState::Minted(
                        crate::pipeline::middle::core::heading::MintOrigin::Expr,
                    ),
                    crate::pipeline::middle::core::heading::Binding::Bare,
                )
            })
            .collect()
    };
    match op {
        PipeOp::Project(items) => {
            let mut carried = hidden.clone();
            carried.extend(nodes(items));
            items_heading(arena, Some(input), &[], items, &carried, true)
        }
        PipeOp::Embed(items) => items_heading(arena, Some(input), input_heading.positions(), items, &nodes(items), true),
        PipeOp::Carry(passengers) => form::form(Formed::Carry {
            input: input_heading,
            passengers,
        }),
        PipeOp::ProjectOut(selection) => {
            let dropped: Vec<usize> = selection.items().iter().map(|(s, _)| s.position()).collect();
            form::form(Formed::Without {
                input: input_heading,
                dropped: &dropped,
            })
        }
        PipeOp::Cover(cover) => {
            // A covered position holds the value that covers it: its shape
            // is the covering value's, and a replaced structure ends there.
            let mut covered = Vec::with_capacity(cover.items().len());
            for (target, value) in cover.items() {
                covered.push((target.position(), structure_of(arena, Some(input), *value)?));
            }
            form::form(Formed::Cover {
                input: input_heading,
                covered: &covered,
            })
        }
        PipeOp::Group { keys, reductions } => {
            let all: Vec<Item> = keys.iter().chain(reductions).cloned().collect();
            items_heading(arena, Some(input), &[], &all, &[], false)
        }
        PipeOp::Distinct(keys) => items_heading(arena, Some(input), &[], keys, &[], false),
    }
}

/// Corresponding and smart unions align by answered name, positional union
/// by ordinal.
pub(crate) fn align(
    left: &Heading,
    right: &Heading,
    op: SetOpKind,
) -> Result<Vec<(Option<usize>, Option<usize>)>, Refusal> {
    use crate::pipeline::middle::core::heading::correspondence::answers_to;
    let lefts: Vec<usize> = left.displayed().map(|(i, _)| i).collect();
    let rights: Vec<usize> = right.displayed().map(|(i, _)| i).collect();
    match op {
        SetOpKind::Positional => {
            if lefts.len() != rights.len() {
                return Err(refuse::set_width_mismatch(lefts.len(), rights.len()));
            }
            Ok(lefts
                .into_iter()
                .zip(rights)
                .map(|(l, r)| (Some(l), Some(r)))
                .collect())
        }
        SetOpKind::Corresponding | SetOpKind::Smart => {
            let mut out: Vec<(Option<usize>, Option<usize>)> = Vec::new();
            let mut used = vec![false; right.len()];
            for l in lefts {
                let partner = left.positions()[l]
                    .answering_name()
                    .and_then(|name| answers_to(right, name).into_iter().next());
                if let Some(r) = partner {
                    used[r] = true;
                }
                out.push((Some(l), partner));
            }
            for r in rights {
                if !used[r] {
                    out.push((None, Some(r)));
                }
            }
            if op == SetOpKind::Smart && out.iter().any(|(l, r)| l.is_none() || r.is_none()) {
                return Err(refuse::set_name_mismatch());
            }
            Ok(out)
        }
    }
}

/// The free binders of a relation: its children's, less the binders it
/// introduces. A stage over a run consumes that run's scope, so the run's
/// member binders its items read are not free.
pub(crate) fn rel_fv(arena: &impl Judging, kind: &RelKind) -> super::BinderSet {
    let mut children: Vec<super::BinderSet> = Vec::new();
    let mut introduced: Vec<BinderId> = Vec::new();
    if let RelKind::Read {
        source: ReadSource::Frontier(b),
        ..
    } = kind
    {
        let mut own = super::BinderSet::new();
        own.insert(*b);
        children.push(own);
    }
    for child in super::walk::of_rel(arena, kind) {
        children.push(match child {
            Child::Rel(r) => arena.rel(r).fv().clone(),
            Child::Expr(e) => arena.expr(e).fv().clone(),
            Child::Truth(t) => arena.truth(t).fv().clone(),
        });
    }
    match kind {
        RelKind::Run(run) => {
            for qual in run.quals() {
                if let Qual::Member(m) = qual {
                    introduced.push(m.binder());
                }
            }
        }
        RelKind::Pipe { input, .. } | RelKind::Order { input, .. } => {
            introduced.extend(scope_binders(arena, *input));
        }
        RelKind::Fix(fix) => introduced.push(fix.frontier()),
        RelKind::Read { .. }
        | RelKind::Lit { .. }
        | RelKind::Receipt { .. }
        | RelKind::SetOp { .. }
        | RelKind::Minus { .. }
        | RelKind::Meta { .. }
        | RelKind::Witnessed { .. }
        | RelKind::Unnest { .. }
        | RelKind::Family { .. }
        | RelKind::Apply { .. } => {}
    }
    decide::fv::of(children.iter(), &introduced)
}

/// The binders a stage over `input` may read without them being free: the
/// member binders of the run it consumes.
pub(crate) fn scope_binders(arena: &impl Judging, input: RelId) -> Vec<BinderId> {
    match arena.rel(input).kind() {
        RelKind::Run(run) => run
            .quals()
            .iter()
            .filter_map(|q| match q {
                Qual::Member(m) => Some(m.binder()),
                Qual::Guard(_) | Qual::Bound(_) => None,
            })
            .collect(),
        RelKind::Read { .. }
        | RelKind::Lit { .. }
        | RelKind::Receipt { .. }
        | RelKind::Pipe { .. }
        | RelKind::Order { .. }
        | RelKind::SetOp { .. }
        | RelKind::Minus { .. }
        | RelKind::Meta { .. }
        | RelKind::Witnessed { .. }
        | RelKind::Unnest { .. }
        | RelKind::Family { .. }
        | RelKind::Fix(_)
        | RelKind::Apply { .. } => Vec::new(),
    }
}


/// The relations a relation reads directly.
pub(crate) fn child_rels(arena: &impl Judging, kind: &RelKind) -> Vec<RelId> {
    super::walk::of_rel(arena, kind)
        .into_iter()
        .filter_map(|c| match c {
            Child::Rel(r) => Some(r),
            Child::Expr(_) | Child::Truth(_) => None,
        })
        .collect()
}

impl Builder {
    /// Reserve the frontier of a fixpoint about to be defined: a binder
    /// whose occurrence carries the anchor's heading. A self-reference reads
    /// the definition, a boundary no extracted value's node crosses
    /// (json-substrate-law): the anchor's node passengers are not its.
    pub(crate) fn frontier(&mut self, anchor: RelId) -> Result<BinderId, Refusal> {
        let heading = self.rel(anchor).heading();
        let nodes: Vec<usize> = heading
            .positions()
            .iter()
            .enumerate()
            .filter(|(_, p)| match p.visibility {
                crate::pipeline::middle::core::heading::Visibility::Hidden(h) => {
                    matches!(crate::pipeline::middle::core::graph::Arena::passenger(self, h), super::Passenger::Node { .. })
                }
                _ => false,
            })
            .map(|(k, _)| k)
            .collect();
        let heading = match nodes.is_empty() {
            true => heading.clone(),
            false => form::form(Formed::Without {
                input: heading,
                dropped: &nodes,
            })?,
        };
        Ok(self.fresh_binder(super::BinderSite { heading, rel: None }))
    }

    /// A row no caller supplies, of `shape`'s heading: what a definition's
    /// scalar formals read where the definition is declared, before any use
    /// binds them.
    pub(crate) fn declaration_row(&mut self, shape: RelId) -> BinderId {
        let heading = self.rel(shape).heading().clone();
        self.fresh_binder(super::BinderSite { heading, rel: None })
    }

    /// A read of a fixpoint's frontier. A self-reference reads the frontier
    /// whole, under the heading the definition publishes (THE FRONTIER BINDS
    /// BY ITS OWN HEADING): any other access rebinds or constrains it.
    pub(crate) fn frontier_read(&mut self, frontier: BinderId, access: AccessSpec) -> Result<RelId, Refusal> {
        let AccessSpec::All = access else {
            return Err(refuse::argumentative_binding());
        };
        let heading = form::form(Formed::Read {
            source: self.binder(frontier).heading(),
            access: ReadForm::All,
        })?;
        Ok(self.push_relation(
            RelKind::Read {
                source: ReadSource::Frontier(frontier),
                access: ReadAccess::All,
            },
            heading,
        ))
    }

    /// Close a definition family (W5 #15; heads-law CLAUSE AGREEMENT;
    /// recursion-contract-law). Every clause wears one badge; a badge
    /// claims a self-reference; the clauses publish one heading. One
    /// unguarded clause that does not read the frontier is the family's
    /// body; clauses none of which reads it accumulate as a bag; clauses
    /// that read it make a fixpoint over those that do not.
    pub(crate) fn close_family(
        &mut self,
        name: &str,
        family: &str,
        frontier: Option<BinderId>,
        clauses: Vec<ClauseSpec>,
    ) -> Result<RelId, Refusal> {
        let recursive = frontier.is_some_and(|f| clauses.iter().any(|c| self.rel_reads(c.body, f)));
        let badges: Vec<bool> = clauses.iter().map(|c| c.badged).collect();
        let deduplicating = decide::recursion::badge(name, &badges, recursive)?;
        let headings: Vec<Heading> = clauses.iter().map(|c| self.rel(c.body).heading().clone()).collect();
        let judged: Vec<ClauseHeading<'_>> = clauses
            .iter()
            .zip(&headings)
            .map(|(c, heading)| ClauseHeading {
                heading,
                closed: c.closed,
                fact: c.fact,
            })
            .collect();
        let heading = judge_family(name, family, &judged)?;
        match (frontier, recursive) {
            (Some(frontier), true) => {
                if clauses.iter().any(|c| c.guard.is_some()) {
                    return Err(refuse::outside("a recursive family with clause guards"));
                }
                let (steps, anchors): (Vec<RelId>, Vec<RelId>) = clauses
                    .iter()
                    .map(|c| c.body)
                    .partition(|c| self.rel_reads(*c, frontier));
                decide::recursion::judge(&step_facts(self, &steps, frontier))?;
                // A deduplicating fixpoint compares its rows by their bytes.
                if deduplicating {
                    for (_, p) in heading.displayed() {
                        decide::document::observed(&p.interior, "a deduplicating fixpoint")?;
                    }
                }
                let cap = demand_cap(self, &steps);
                Ok(self.push_relation(
                    RelKind::Fix(super::Fix {
                        frontier,
                        deduplicating,
                        anchors,
                        steps,
                        cap,
                    }),
                    heading,
                ))
            }
            (_, _) => match clauses.as_slice() {
                [ClauseSpec {
                    guard: None,
                    body,
                    ..
                }] if self.rel(*body).heading().displayed().map(|(_, p)| &p.name).eq(heading.displayed().map(|(_, p)| &p.name)) => {
                    Ok(*body)
                }
                _ => Ok(self.push_relation(
                    RelKind::Family {
                        facts: clauses.iter().all(|c| c.fact).then(|| family.to_string()),
                        clauses: clauses
                            .into_iter()
                            .map(|c| super::FamilyClause {
                                guard: c.guard,
                                body: c.body,
                            })
                            .collect(),
                    },
                    heading,
                )),
            },
        }
    }

    /// Whether a relation reads a binder it does not bind.
    pub(crate) fn rel_reads(&self, rel: RelId, binder: BinderId) -> bool {
        self.rel(rel).fv().contains(&binder)
    }
}

/// What each recursive clause does with the frontier: reads as a direct
/// source (through relations only), reads inside a truth or value it holds,
/// and reductions over it.
/// A fixpoint's demand cap from its recursive clauses: the bounds each
/// clause writes on its own chain (its stages and the runs they read, never
/// a member's own relation); the least count when every one is `#<N`. A
/// clause chain holding another bound (an offset, an ordered bound) decides
/// no cap: what that bound means in a recursion the law does not state.
pub(crate) fn demand_cap(arena: &impl Judging, steps: &[RelId]) -> Option<super::DemandCap> {
    let mut bounds: Vec<(RelId, Bound)> = Vec::new();
    let mut other = false;
    for step in steps {
        chain_bounds(arena, *step, &mut bounds, &mut other);
    }
    if other || bounds.iter().any(|(_, b)| b.offset.is_some() || b.count.is_none()) {
        return None;
    }
    let count = bounds.iter().filter_map(|(_, b)| b.count).min()?;
    let mut runs: Vec<RelId> = bounds.into_iter().map(|(r, _)| r).collect();
    runs.dedup();
    Some(super::DemandCap { count, runs })
}

fn chain_bounds(arena: &impl Judging, rel: RelId, out: &mut Vec<(RelId, Bound)>, other: &mut bool) {
    match arena.rel(rel).kind() {
        RelKind::Pipe { input, .. } => chain_bounds(arena, *input, out, other),
        RelKind::Order { input, bound, .. } => {
            *other |= bound.is_some();
            chain_bounds(arena, *input, out, other);
        }
        RelKind::Run(run) => {
            for q in run.quals() {
                if let Qual::Bound(b) = q {
                    out.push((rel, b.clone()));
                }
            }
            // The chain continues into the run's first member where that
            // member is the stage before the run, or the run's only member.
            let mut members = run.members();
            if let Some(first) = members.next() {
                let only = members.next().is_none();
                match arena.rel(first.rel()).kind() {
                    RelKind::Pipe { .. } | RelKind::Order { .. } => chain_bounds(arena, first.rel(), out, other),
                    RelKind::Run(_) if only => chain_bounds(arena, first.rel(), out, other),
                    _ => {}
                }
            }
        }
        _ => {}
    }
}

pub(crate) fn step_facts(arena: &impl Judging, steps: &[RelId], frontier: BinderId) -> Vec<decide::recursion::StepFacts> {
    steps
        .iter()
        .map(|step| {
            let mut facts = decide::recursion::StepFacts::default();
            walk_step(arena, Child::Rel(*step), frontier, false, &mut facts);
            facts
        })
        .collect()
}

fn walk_step(
    arena: &impl Judging,
    node: Child,
    frontier: BinderId,
    under_value: bool,
    facts: &mut decide::recursion::StepFacts,
) {
    if let Child::Rel(r) = node {
        match arena.rel(r).kind() {
            RelKind::Read {
                source: ReadSource::Frontier(b),
                ..
            } if *b == frontier => {
                if under_value {
                    facts.subquery_reads += 1;
                } else {
                    facts.frontier_reads += 1;
                }
            }
            RelKind::Pipe {
                input,
                op: PipeOp::Group { reductions, .. },
            } if !reductions.is_empty() && arena.rel(*input).fv().contains(&frontier) => {
                facts.reduces_frontier = true
            }
            _ => {}
        }
    }
    for child in super::walk::of(arena, node) {
        let below = under_value || matches!(child, Child::Expr(_) | Child::Truth(_));
        walk_step(arena, child, frontier, below, facts);
    }
}

/// The surface form of an expansion: what directs it decides what it may
/// expand (KNOWN EXPANDS, DECLARED WITNESSES).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExpansionForm {
    /// `.col(*)`, `.col(a, b)`: the known interior's heading directs it.
    Drill,
    /// `|> .col{…}`: a declared pattern over a sequence, payload only.
    Narrow,
    /// `x ~= …`: a declared pattern.
    Destructure,
}

impl Builder {
    /// One expansion of `value` (THE EXPANSION FAMILY), decided by
    /// `expansion_heading`: what the form may expand, what each published
    /// bind holds, and the heading.
    pub(crate) fn expand(
        &mut self,
        value: ExprId,
        form: ExpansionForm,
        levels: Vec<super::Level>,
        column: &str,
    ) -> Result<RelId, Refusal> {
        let expansion = super::Expansion { levels, form };
        let heading = expansion_heading(self, value, &expansion, column)?;
        Ok(self.push_relation(RelKind::Unnest { value, expansion }, heading))
    }
}

/// The heading of one expansion, and the judgments that admit it: KNOWN
/// EXPANDS (a drill only over a known interior), DECLARED WITNESSES (a
/// pattern over any value but a declared non-document column), a sequence
/// position over a value known to be one record (`narrowing/object_literal`
/// for a narrowing; held for an iterating destructure), a record pattern over
/// a known non-record, two iterations side by side (unruled). A bound
/// member of a known structure is bound by `decide::document::bound`, the
/// drill's positions and the patterns' keys alike.
pub(crate) fn expansion_heading(
    arena: &impl Judging,
    value: ExprId,
    expansion: &super::Expansion,
    column: &str,
) -> Result<Heading, Refusal> {
    use super::{BindAt, BindRole, Reach};
    let form = expansion.form;
    let levels = &expansion.levels;
    let structure = structure_of(arena, None, value)?;
    if form != ExpansionForm::Drill {
        decide::document::consumed(&structure, "a pattern")?;
    }
    side_by_side(levels)?;
    // The element each level's binds read, by its structure.
    let mut elements: Vec<Interior> = Vec::with_capacity(levels.len());
    let mut known: Option<Heading> = None;
    // The known structure a drill reads its members out of.
    let mut over = Known::None;
    for (index, level) in levels.iter().enumerate() {
        let node = match &level.from {
            None => structure.clone(),
            Some((parent, None)) => elements[*parent].clone(),
            Some((parent, Some(path))) => decide::document::reach(&elements[*parent], path)?,
        };
        let element = match level.reach {
            Reach::Known => {
                if let Known::Shape(_, Origin::Tuples(_)) = &node.known {
                    return Err(refuse::unruled(
                        "whether a drill of a tuple collection publishes invented positional names or binds only by position",
                    ));
                }
                let interior = unnest_heading(&node, column)?;
                over = node.known.clone();
                known = Some(interior.clone());
                Interior::made_with(Known::One(Box::new(interior), Layout::Record))
            }
            Reach::Node => {
                let record_pattern = level.binds.iter().any(|b| {
                    matches!(&b.at, BindAt::Path(p) if p.steps().next().is_some_and(|s| matches!(s, crate::pipeline::middle::facade::PathStep::Key(_))))
                });
                if record_pattern
                    && matches!(node.known, Known::Shape(..) | Known::PerArm(_) | Known::One(_, Layout::Tuple))
                {
                    return Err(refuse::unruled("what a record pattern binds over a value that is not a record"));
                }
                node
            }
            Reach::Sequence => match &node.known {
                Known::One(_, Layout::Record) | Known::Keyed(_) if index == 0 && form == ExpansionForm::Narrow => {
                    return Err(refuse::object_literal(column))
                }
                Known::One(_, Layout::Record) | Known::Keyed(_) => return Err(refuse::pattern_object_literal(column)),
                Known::Shape(h, origin) => Interior::made_with(Known::One(h.clone(), origin.layout())),
                Known::None | Known::One(_, Layout::Tuple) | Known::PerArm(_) => node.node_in(),
            },
            Reach::Keys => match &node.known {
                Known::Keyed(inner) => (**inner).clone(),
                // A record's members, one per key: what stands under each
                // is the member's, read per row, and a bound key is the
                // record's key as data.
                Known::One(_, Layout::Record) | Known::None => {
                    if level.binds.iter().any(|b| matches!(b.at, BindAt::Key)) {
                        decide::document::observed(&node, "a metadata pattern's key")?;
                    }
                    node.node_in()
                }
                Known::One(_, Layout::Tuple) | Known::Shape(..) | Known::PerArm(_) => {
                    return Err(refuse::unruled("what a metadata pattern binds over a value that is not keyed by data"))
                }
            },
        };
        elements.push(element);
    }
    let mut published: Vec<(Option<Name>, Interior)> = Vec::new();
    let mut whole = form == ExpansionForm::Drill && levels.len() == 1;
    for (index, level) in levels.iter().enumerate() {
        for (k, bind) in level.binds.iter().enumerate() {
            let held = match &bind.at {
                BindAt::Position(p) => known
                    .as_ref()
                    .and_then(|h| h.positions().get(*p as usize))
                    .map(|p| decide::document::bound(p.interior.clone(), &over))
                    .ok_or_else(|| refuse::elaboration_contract("a bound position past the known interior"))?,
                BindAt::Path(path) => decide::document::bind(&elements[index], path)?,
                BindAt::Element => match &elements[index] {
                    made if made.always_made() => made.clone(),
                    element => element.node_in(),
                },
                BindAt::Key => Interior::flat(),
            };
            if let BindRole::Constrain { value, .. } = &bind.role {
                constrain(arena, &held, *value)?;
            }
            whole &= matches!((&bind.at, &bind.role), (BindAt::Position(p), BindRole::Publish(None)) if *p as usize == k);
            if let BindRole::Publish(name) = &bind.role {
                if let Some(name) = name {
                    if published.iter().any(|(n, _)| n.as_ref() == Some(name)) {
                        return Err(match form {
                            ExpansionForm::Drill => refuse::duplicate_name(name, false),
                            ExpansionForm::Narrow | ExpansionForm::Destructure => refuse::pattern_duplicate(name),
                        });
                    }
                }
                published.push((name.clone(), held));
            }
        }
    }
    // A drill of every position publishes the known interior heading as it
    // stands, lost and minted names included, each member bound.
    let bound = match (&known, whole && known.as_ref().is_some_and(|h| h.len() == published.len())) {
        (Some(interior), true) => interior.map_interiors(|i| decide::document::bound(i.clone(), &over)),
        _ => {
            if published.iter().any(|(n, _)| n.is_none()) {
                return Err(refuse::elaboration_contract("an expansion publishing a bind with no name"));
            }
            form::form(Formed::One {
                layout: Layout::Record,
                members: &published,
            })?
        }
    };
    // A pattern reads the node of the value it expands.
    node_reached(arena, value, &structure)?;
    match form {
        ExpansionForm::Drill => form::form(Formed::Unnest { interior: &bound }),
        ExpansionForm::Narrow | ExpansionForm::Destructure => Ok(bound),
    }
}

/// Two iterating levels of one expansion side by side (neither inside the
/// other): whether their rows pair every element of one with every element
/// of the other is not ruled.
fn side_by_side(levels: &[super::Level]) -> Result<(), Refusal> {
    use super::Reach;
    let iterating = |i: usize| levels[i].reach != Reach::Node;
    // The iterating levels a level stands inside, itself included.
    let within = |mut i: usize| {
        let mut chain = Vec::new();
        loop {
            if iterating(i) {
                chain.push(i);
            }
            match levels[i].from {
                Some((parent, _)) => i = parent,
                None => return chain,
            }
        }
    };
    for i in 0..levels.len() {
        for j in (i + 1)..levels.len() {
            if iterating(i) && iterating(j) && !within(i).contains(&j) && !within(j).contains(&i) {
                return Err(refuse::side_by_side_iteration());
            }
        }
    }
    Ok(())
}

/// The interior heading a drill reads, judged compatible across arms.
pub(crate) fn unnest_heading(interior: &Interior, name: &str) -> Result<Heading, Refusal> {
    use crate::pipeline::middle::core::heading::compatibility::compatible;
    match &interior.known {
        Known::Shape(h, _) => Ok((**h).clone()),
        Known::PerArm(arms) => {
            let (first, origin) = arms.first().cloned().unwrap_or_default();
            for (arm, arm_origin) in &arms[1..] {
                if compatible((&first, origin), (arm, *arm_origin)).is_err() {
                    return Err(refuse::mixed_release(name));
                }
            }
            Ok(first)
        }
        Known::None | Known::One(..) | Known::Keyed(_) => Err(refuse::no_interior(name)),
    }
}

impl Builder {
    /// Birth a passenger: a configured value's captured term over its
    /// construction row (W5 #13).
    pub(crate) fn passenger(
        &mut self,
        value: ExprId,
        construction: BinderId,
    ) -> crate::pipeline::middle::core::ids::PassengerId {
        self.push_passenger(super::Passenger::Configured {
            value,
            construction,
        })
    }
}
