// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::expressions::DomainExpression;
use super::ordering::{Limit, OrderTerm};
use super::select_items::{Publishes, SelectItem};
use super::table::TableExpression;

#[derive(Debug, Clone, PartialEq)]
pub enum QueryExpression {
    /// A SELECT statement
    Select(Box<SelectStatement>),

    /// UNION/UNION ALL/INTERSECT/EXCEPT
    SetOperation {
        op: SetOperator,
        left: Box<QueryExpression>,
        right: Box<QueryExpression>,
    },
}

/// The one set operator this AST can spell.
///
/// DelightQL's set operators are ALL-flavored by law, so the deduplicating
/// spellings — `UNION`, `INTERSECT`, `EXCEPT` — have no producer and no
/// variant here: the multiset law is an ABSENT CAPABILITY, not a flag left
/// false. Minus lowers as an anti-semijoin, which is what makes it
/// bag-preserving and null-correct on every target; reintroducing
/// `EXCEPT ALL` where a target offers it is a legalizer optimization over
/// that shape, never a new way to build one.
///
/// A FIXPOINT'S ACCUMULATION IS NOT ONE OF THESE. It is the shape of
/// `CteBody::Fixpoint`, which keeps its anchor and members apart and carries
/// its own flavor — so `UNION` is not a value anything can hold, place, or
/// move.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SetOperator {
    UnionAll,
}

impl QueryExpression {
    /// The occurrence behind each column this expression produces. A set
    /// operation takes its column names from its first branch, and a
    /// `VALUES` list takes the engine's.
    ///
    /// A `*` standing for no occurrence is what a layer publishing nothing
    /// writes, and SQL still answers every column its FROM carries: those
    /// columns are read through it. `None` when some of them come from a
    /// source the compiler never saw the heading of — a relation outside
    /// this statement, or a table function the catalog does not describe —
    /// since their width and names are the source's own.
    #[stacksafe::stacksafe]
    pub fn heading(&self) -> Option<Vec<Option<crate::names::ColId>>> {
        match self {
            QueryExpression::Select(select) => {
                let mut heading = Vec::new();
                for item in &select.select_list {
                    match item.publishes() {
                        Publishes::One(col) => heading.push(Some(col)),
                        Publishes::Run([]) => {
                            for table in select.from.as_deref().unwrap_or_default() {
                                heading.extend(table.heading()?);
                            }
                        }
                        Publishes::Run(cols) => heading.extend(cols.iter().copied().map(Some)),
                        Publishes::Nothing => heading.push(None),
                    }
                }
                Some(heading)
            }
            QueryExpression::SetOperation { left, .. } => left.heading(),
        }
    }
}

impl SetOperator {
    /// The keyword this operator writes.
    pub fn keyword(&self) -> &'static str {
        match self {
            SetOperator::UnionAll => "UNION ALL",
        }
    }
}

/// How a statement groups its whole input as ONE group that exists only
/// when the input has a row. Targets disagree on which SQL says that
/// (`SqlDialect::whole_input_grouping`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WholeInputGrouping {
    /// `GROUP BY NULL`: a key every row shares, which the target reads as a
    /// value. Any result columns may stand beside it.
    SharedKey,
    /// No GROUP BY and `HAVING count(*) > 0`: the standard makes a HAVING
    /// without GROUP BY one group of the whole input.
    InhabitedHaving,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    /// The scope this SELECT produces. Column qualification compares a
    /// reference's owner with this lexical emission point.
    pub(super) at: crate::names::ScopeId,

    /// DISTINCT flag
    pub(super) distinct: bool,

    /// What to select (columns, expressions, *)
    pub(super) select_list: Vec<SelectItem>,

    /// FROM clause - tables, subqueries, joins
    pub(super) from: Option<Vec<TableExpression>>,

    /// WHERE clause
    pub(super) where_clause: Option<DomainExpression>,

    /// GROUP BY clause
    pub(super) group_by: Option<Vec<DomainExpression>>,

    /// HAVING clause. Without GROUP BY it judges the one group the whole
    /// input forms, a shape only `retain_group_keys` writes.
    pub(super) having: Option<DomainExpression>,

    /// ORDER BY clause
    pub(super) order_by: Option<Vec<OrderTerm>>,

    /// LIMIT clause with optional OFFSET
    pub(super) limit: Option<Limit>,
}

impl SelectStatement {
    pub fn builder() -> super::builders::SelectBuilder {
        super::builders::SelectBuilder::new()
    }

    pub fn is_distinct(&self) -> bool {
        self.distinct
    }

    pub fn at(&self) -> crate::names::ScopeId {
        self.at
    }

    pub fn select_list(&self) -> &[SelectItem] {
        &self.select_list
    }

    pub fn from(&self) -> Option<&[TableExpression]> {
        self.from.as_deref()
    }

    /// The select list, to rewrite VALUES in place. A rewrite that changes
    /// which occurrence a position realizes builds a new item and says so.
    pub(in crate::pipeline) fn select_list_mut(&mut self) -> &mut Vec<SelectItem> {
        &mut self.select_list
    }

    pub(in crate::pipeline) fn where_clause_mut(&mut self) -> Option<&mut DomainExpression> {
        self.where_clause.as_mut()
    }

    pub(in crate::pipeline) fn group_by_mut(&mut self) -> Option<&mut [DomainExpression]> {
        self.group_by.as_deref_mut()
    }

    pub(in crate::pipeline) fn having_mut(&mut self) -> Option<&mut DomainExpression> {
        self.having.as_mut()
    }

    pub(in crate::pipeline) fn order_by_mut(&mut self) -> Option<&mut [OrderTerm]> {
        self.order_by.as_deref_mut()
    }

    /// Keep the ordering terms `orders` answers for. An ordering left with
    /// no term is no ordering: SQL has no empty ORDER BY.
    pub(in crate::pipeline) fn retain_order_terms(
        &mut self,
        orders: impl FnMut(&OrderTerm) -> bool,
    ) {
        if let Some(terms) = &mut self.order_by {
            terms.retain(orders);
            if terms.is_empty() {
                self.order_by = None;
            }
        }
    }

    /// Keep the grouping keys `partitions` answers for.
    ///
    /// A grouping is more than its keys. Over an input with no row it has no
    /// group, where an ungrouped aggregate still answers one row; so a
    /// grouping left with no key still groups its whole input as one group
    /// that exists only when the input had a row, written as `whole` says.
    pub(in crate::pipeline) fn retain_group_keys(
        &mut self,
        partitions: impl FnMut(&DomainExpression) -> bool,
        whole: WholeInputGrouping,
    ) {
        let Some(keys) = &mut self.group_by else {
            return;
        };
        keys.retain(partitions);
        if !keys.is_empty() {
            return;
        }
        match whole {
            WholeInputGrouping::SharedKey => {
                keys.push(DomainExpression::literal(
                    crate::pipeline::asts::core::LiteralValue::Null,
                ));
            }
            WholeInputGrouping::InhabitedHaving => {
                self.group_by = None;
                let inhabited = DomainExpression::function("count", vec![DomainExpression::star()])
                    .gt(DomainExpression::literal(
                        crate::pipeline::asts::core::LiteralValue::integer(0),
                    ));
                self.having = Some(match self.having.take() {
                    Some(having) => DomainExpression::and(vec![inhabited, having]),
                    None => inhabited,
                });
            }
        }
    }

    pub fn from_mut(&mut self) -> Option<&mut [TableExpression]> {
        self.from.as_deref_mut()
    }

    pub fn where_clause(&self) -> Option<&DomainExpression> {
        self.where_clause.as_ref()
    }

    pub fn group_by(&self) -> Option<&[DomainExpression]> {
        self.group_by.as_deref()
    }

    pub fn having(&self) -> Option<&DomainExpression> {
        self.having.as_ref()
    }

    pub fn order_by(&self) -> Option<&[OrderTerm]> {
        self.order_by.as_deref()
    }

    pub fn limit(&self) -> Option<&Limit> {
        self.limit.as_ref()
    }

    /// Move a cap onto this statement. A cap says how many rows leave; it
    /// names no column, so the publication this statement was proven to make
    /// is untouched and needs no re-proof.
    pub(in crate::pipeline) fn set_limit(&mut self, limit: Limit) {
        self.limit = Some(limit);
    }

    /// Take the bound off. Only a pass that has put the same bound somewhere
    /// the statement still reads may do this — a bound simply dropped is a
    /// relation the query no longer names.
    pub(in crate::pipeline) fn clear_limit(&mut self) {
        self.limit = None;
    }
}
