// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::expressions::DomainExpression;
use super::ordering::{Limit, OrderTerm};
use super::query::{QueryExpression, SelectStatement};
use super::select_items::SelectItem;
use super::table::TableExpression;
use crate::pipeline::transformer::builder::publication::Checked;

pub struct SelectBuilder {
    distinct: bool,
    select_list: Vec<SelectItem>,
    from: Option<Vec<TableExpression>>,
    where_clause: Option<DomainExpression>,
    group_by: Option<Vec<DomainExpression>>,
    having: Option<DomainExpression>,
    order_by: Option<Vec<OrderTerm>>,
    limit: Option<Limit>,
}

impl SelectBuilder {
    pub fn new() -> Self {
        SelectBuilder {
            distinct: false,
            select_list: Vec::new(),
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
        }
    }

    pub fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    pub fn select(mut self, item: SelectItem) -> Self {
        self.select_list.push(item);
        self
    }

    pub fn select_all(mut self, items: Vec<SelectItem>) -> Self {
        self.select_list.extend(items);
        self
    }

    pub fn set_select(mut self, items: Vec<SelectItem>) -> Self {
        self.select_list = items;
        self
    }

    pub fn from_subquery(mut self, query: QueryExpression, alias: crate::names::ScopeId) -> Self {
        self.from = Some(vec![TableExpression::subquery(query, alias)]);
        self
    }

    pub fn from_tables(mut self, tables: Vec<TableExpression>) -> Self {
        self.from = Some(tables);
        self
    }

    pub fn where_clause(mut self, expr: DomainExpression) -> Self {
        self.where_clause = Some(expr);
        self
    }

    /// Add an AND condition to WHERE clause
    /// If no WHERE exists, sets it. If WHERE exists, combines with AND.
    pub fn and_where(mut self, expr: DomainExpression) -> Self {
        self.where_clause = match self.where_clause {
            None => Some(expr),
            Some(existing) => Some(DomainExpression::and(vec![existing, expr])),
        };
        self
    }

    pub fn group_by(mut self, exprs: Vec<DomainExpression>) -> Self {
        self.group_by = Some(exprs);
        self
    }

    pub fn having(mut self, expr: DomainExpression) -> Self {
        self.having = Some(expr);
        self
    }

    pub fn order_by(mut self, term: OrderTerm) -> Self {
        self.order_by.get_or_insert_with(Vec::new).push(term);
        self
    }

    /// Carry a cap that already exists, whole: a caller MOVING one never
    /// takes it apart — an offset lost in that round trip is a different
    /// query.
    pub(in crate::pipeline) fn limit_from(mut self, limit: Limit) -> Self {
        self.limit = Some(limit);
        self
    }

    /// The row clause already standing on this select. A caller adding one
    /// asks first, because a second clause is a bound over the FIRST one's
    /// result and cannot replace it.
    pub(in crate::pipeline) fn limit_clause(&self) -> Option<&Limit> {
        self.limit.as_ref()
    }

    /// The list as it stands, for a caller that must state a fact about it
    /// before building.
    pub(in crate::pipeline) fn items(&self) -> &[SelectItem] {
        &self.select_list
    }

    /// Build a statement that produces the publication `checked` states.
    ///
    /// The fact is re-checked against this list, so it is not a badge that
    /// can be lifted off one statement and stamped onto another: a fact
    /// borrowed from elsewhere names a scope and outputs this list does not
    /// produce, and the door refuses. Stating the fact is the authority's,
    /// because only the authority can construct one.
    pub(in crate::pipeline) fn publishing(
        self,
        checked: Checked,
    ) -> Result<SelectStatement, String> {
        checked.verify(&self.select_list)?;
        self.stand_at(checked.at(), checked)
    }

    /// Rebuild `previous` with reshaped clauses, keeping the publication it
    /// was proven to produce.
    ///
    /// The rewriters below the transformer flatten wrappers, merge WHEREs and
    /// move joins; none of that may move an output. The new list is held to
    /// the fact the old statement CARRIES, not to the old list — comparing
    /// lists would let a statement whose fact had already drifted pass on the
    /// drift.
    pub(in crate::pipeline) fn rebuilding(
        self,
        previous: &SelectStatement,
    ) -> Result<SelectStatement, String> {
        let checked = previous.checked.clone();
        checked.verify(&self.select_list)?;
        self.stand_at(checked.at(), checked)
    }

    /// Build a fresh statement standing at `at`, its publication read off
    /// the list it was just given. The door exists for compiler-synthesized
    /// reads whose list IS the publication — nothing upstream holds a prior
    /// fact to rebuild from.
    pub(in crate::pipeline) fn standing_at(
        self,
        at: crate::names::ScopeId,
    ) -> Result<SelectStatement, String> {
        let checked = Checked::of(at, &self.select_list);
        self.stand_at(at, checked)
    }

    /// Restand `previous` at another scope, its unnamed reads re-expressed
    /// over what is now underneath.
    ///
    /// The narrow case `rebuilding` cannot serve: unwrapping a layer moves
    /// the statement down to the scope the wrapper stood on, and an item that
    /// merely READ a wrapper column now reads the column underneath. What may
    /// not move is anything the statement NAMED — every explicit alias, slot
    /// for slot — nor the width of the row. That pair is the transformation,
    /// stated; the fact recorded afterwards is the one the new list makes
    /// true.
    pub(in crate::pipeline) fn restructuring(
        self,
        at: crate::names::ScopeId,
        previous: &SelectStatement,
    ) -> Result<SelectStatement, String> {
        let named = |items: &[SelectItem]| -> Vec<Option<crate::names::ColId>> {
            items
                .iter()
                .map(|item| match item {
                    SelectItem::Expression { alias, .. } => *alias,
                    SelectItem::Star { .. } => None,
                })
                .collect()
        };
        if named(&previous.select_list) != named(&self.select_list) {
            return Err(format!(
                "a restructuring of the statement at {:?} does not name what it named",
                previous.at
            ));
        }
        let width = |items: &[SelectItem]| -> usize {
            items.iter().map(|item| item.publishes().slots()).sum()
        };
        if width(&previous.select_list) != width(&self.select_list) {
            return Err(format!(
                "a restructuring of the statement at {:?} emits {} columns where it emitted {}",
                previous.at,
                width(&self.select_list),
                width(&previous.select_list)
            ));
        }
        let checked = Checked::of(at, &self.select_list);
        self.stand_at(at, checked)
    }

    /// Stamp a scope onto a list. Private, and reachable only through a door
    /// that has established what the list publishes.
    fn stand_at(
        self,
        at: crate::names::ScopeId,
        checked: Checked,
    ) -> Result<SelectStatement, String> {
        if self.select_list.is_empty() {
            return Err("SELECT statement requires at least one select item".to_string());
        }

        if self.having.is_some() && self.group_by.is_none() {
            return Err("HAVING clause requires GROUP BY".to_string());
        }

        Ok(SelectStatement {
            at,
            checked,
            distinct: self.distinct,
            select_list: self.select_list,
            from: self.from,
            where_clause: self.where_clause,
            group_by: self.group_by,
            having: self.having,
            order_by: self.order_by,
            limit: self.limit,
        })
    }
}

impl Default for SelectBuilder {
    fn default() -> Self {
        Self::new()
    }
}
