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

    /// VALUES clause (for inline data)
    Values { rows: Vec<Vec<DomainExpression>> },

    /// Nested WITH clause (for CTEs within CTEs)
    /// Generates: WITH cte1 AS (...), cte2 AS (...) SELECT ...
    /// This allows tree groups (which generate intermediate CTEs) to be bound as CTEs themselves
    WithCte {
        ctes: Vec<super::Cte>,
        query: Box<QueryExpression>,
    },
}

/// The one set operator this AST can spell.
///
/// DelightQL's set operators are ALL-flavored by law, so the dedupliating
/// spellings — `UNION`, `INTERSECT`, `EXCEPT` — have no producer and no
/// variant here: the multiset law is an ABSENT CAPABILITY, not a flag left
/// false. Minus lowers as an anti-semijoin, which is what makes it
/// bag-preserving and null-correct on every target; reintroducing
/// `EXCEPT ALL` where a target offers it is a legalizer optimization over
/// that shape, never a new way to build one.
#[derive(Debug, Clone, PartialEq)]
pub enum SetOperator {
    UnionAll,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    /// The scope this SELECT produces. Column qualification compares a
    /// reference's owner with this lexical emission point.
    pub(super) at: crate::names::ScopeId,

    /// Evidence that this statement's list was checked against the heading
    /// it claims.
    ///
    /// It travels with the statement so a later rewrite has something to
    /// carry rather than a convention to honour: reconstruction doors take
    /// the evidence off the statement they are reshaping, and there is no
    /// road that mints it outside the publication authority.
    pub(super) checked: crate::pipeline::transformer::builder::publication::Checked,

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

    /// HAVING clause (only valid with GROUP BY)
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

    /// Re-publish this statement into `into`: rewrite what each slot names to
    /// the occurrence given for it, re-stamp the result scope, and record the
    /// fact that is then true of it.
    ///
    /// A statement that becomes a subquery body produces the columns of the
    /// FROM alias naming it, not the ones it produced standing alone — the
    /// alias is what every consumer reads it through. Scope and outputs move
    /// together or the statement says two different things, so there is no
    /// road that moves one: this door moves both, and the evidence with them.
    ///
    /// `target` answers for one output at a time and may refuse; the statement
    /// is left untouched unless every slot is answered, because a body halfway
    /// through this is exactly the state the pairing exists to rule out.
    pub(in crate::pipeline) fn republish(
        &mut self,
        into: crate::names::ScopeId,
        mut target: impl FnMut(crate::names::ColId) -> std::result::Result<crate::names::ColId, String>,
    ) -> std::result::Result<(), String> {
        // A star names its run as surely as an alias names one output, so it
        // is republished too: a star left holding the occurrences it stood
        // for inside is a body claiming a heading its wrapper no longer has.
        let renamed = self
            .select_list
            .iter()
            .map(|item| match item.publishes() {
                Publishes::One(output) => target(output).map(|target| vec![target]),
                Publishes::Run(expansion) => {
                    expansion.iter().map(|output| target(*output)).collect()
                }
                Publishes::Nothing => Ok(Vec::new()),
            })
            .collect::<std::result::Result<Vec<_>, String>>()?;

        for (item, rename) in self.select_list.iter_mut().zip(renamed) {
            match item {
                SelectItem::Expression {
                    alias: Some(alias), ..
                } => {
                    *alias = rename[0];
                }
                SelectItem::Expression { expr, alias } if alias.is_none() => {
                    // A bare column reference publishes under its own name;
                    // republishing it means spelling the new one out.
                    if let Some(target) = rename.first() {
                        let _ = expr;
                        *alias = Some(*target);
                    }
                }
                SelectItem::Expression { .. } => {}
                SelectItem::Star { expansion } => *expansion = rename,
            }
        }
        self.at = into;
        self.checked = crate::pipeline::transformer::builder::publication::Checked::of(
            into,
            &self.select_list,
        );
        Ok(())
    }

    pub fn from(&self) -> Option<&[TableExpression]> {
        self.from.as_deref()
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
