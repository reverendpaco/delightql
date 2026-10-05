// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A PACKAGED RELATION: a relation carried in one row as an interior value,
//! row for row.
//!
//! Every receipt interior is packaged here — a directive's `returned`
//! payload, whether the effect walk or the host builds it, an assertion's
//! `witnesses`, a rule's clause ledger, and the host's `input` echoes.
//! Packaging is the whole-table collection of every
//! column of its source under [`Collection::Packaged`]: each row the source
//! holds is one member, an all-NULL row and its duplicates included, and an
//! empty source packages as the empty interior. Eliding an all-NULL
//! contributor is the law of an authored tree; a receipt transports the
//! relation it was handed, so presence is the source row's and never a
//! judgment of its visible cells.
//!
//! [`Packaged`] has no constructor outside this module: no authored spelling
//! and no column can make a collection packaged, and nothing packaged here
//! can elide.

use super::core::expressions::metadata_types::FilterOrigin;
use super::core::literals::LiteralValue;
use super::core::specs::{GroupSpec, OneOut, OutItem, ReductionItem};
use super::core::{
    AnonRelation, AnonTable, Collection, Comparison, Continuation, DomainExpression, Enclyph,
    FunctionApplication, Glob, GroundForm, Record, RecordMember, ReductionPlan, Spread, Step,
    TruthExpression,
};
use super::unresolved::{Chain, PipeOp};

/// The mark of a collection this module made. Its field is private to this
/// module, so no other code can make one.
#[derive(Debug, Clone, PartialEq)]
pub struct Packaged(());

/// `source ~> {*} as <interior>`, packaged: one row whose one column holds
/// every row of `source`.
pub(crate) fn package(source: Chain, interior: &str) -> Chain {
    package_with(
        source,
        interior,
        vec![RecordMember::Spread(Spread::Glob(Glob::whole()))],
        Vec::new(),
    )
}

/// The same packaging with the members each row is packaged under chosen by
/// the caller, and other reductions over the same rows beside it: one
/// mention of `source` both packages it and answers them.
pub(crate) fn package_with(
    source: Chain,
    interior: &str,
    members: Vec<RecordMember<super::core::Unresolved>>,
    beside: Vec<ReductionItem<super::core::Unresolved>>,
) -> Chain {
    let record = DomainExpression::Application(FunctionApplication::Enclyph(Enclyph::Record(
        Record::plain(members),
    )));
    let reductions = std::iter::once(ReductionItem::Out(OutItem::One(OneOut::authored(
        record,
        Some(interior.into()),
    ))))
    .chain(beside)
    .collect();
    source.then(Step::authored(Continuation::Pipe {
        operator: PipeOp::Group(GroupSpec::Reduce {
            plan: ReductionPlan {
                tree_groups: Vec::new(),
                collection: Collection::Packaged(Packaged(())),
            },
            keys: Vec::new(),
            reductions: crate::pipeline::asts::vocabulary::Vec1::try_from_vec(reductions)
                .expect("a packaging reduces to its interior"),
        }),
        named: None,
    }))
}

/// The relation a host hands over as rows of text cells under `heading`.
/// Zero rows is the empty relation of that heading: the literal body is
/// nonempty by type, so its one NULL row stands behind a false restriction
/// and never reaches a collector.
pub(crate) fn host_rows(heading: &[&str], rows: &[Vec<Option<String>>]) -> Chain {
    let cell = |value: &Option<String>| {
        DomainExpression::Application(FunctionApplication::Ground(match value {
            Some(text) => LiteralValue::String(text.clone()),
            None => LiteralValue::Null,
        }))
    };
    let header = heading
        .iter()
        .map(|name| DomainExpression::lvar_builder(name.to_string()).build())
        .collect();
    let body: Vec<Vec<DomainExpression<super::core::Unresolved>>> = if rows.is_empty() {
        vec![heading.iter().map(|_| cell(&None)).collect()]
    } else {
        rows.iter()
            .map(|row| row.iter().map(cell).collect())
            .collect()
    };
    let table =
        AnonTable::from_values(Some(header), body).expect("a host relation has a nonempty heading");
    let literal = Chain::authored(GroundForm::Literal(AnonRelation::plain(table)));
    if !rows.is_empty() {
        return literal;
    }
    let integer =
        |n| DomainExpression::Application(FunctionApplication::Ground(LiteralValue::integer(n)));
    literal.then(Step::authored(Continuation::Restrict {
        condition: TruthExpression::Comparison(Comparison {
            operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
            left: Box::new(integer(0)),
            right: Box::new(integer(1)),
        }),
        origin: FilterOrigin::Generated,
    }))
}
