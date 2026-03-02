use super::expressions::DomainExpression;
use super::ordering::{Limit, OrderTerm};
use super::query::{QueryExpression, SelectStatement};
use super::select_items::SelectItem;
use super::table::TableExpression;

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

    pub fn has_select_items(&self) -> bool {
        !self.select_list.is_empty()
    }

    /// Returns true if the SELECT list contains non-trivial items (not just `SELECT *`).
    /// Used to detect builders from value-covers that need materialization before
    /// another operator can apply its own SELECT.
    pub fn has_cover_select_items(&self) -> bool {
        if self.select_list.is_empty() {
            return false;
        }
        // A trivial SELECT is a single `*` — produced by filter/join builders.
        // Anything else (cover transforms, projections) is non-trivial.
        !(self.select_list.len() == 1 && matches!(self.select_list[0], SelectItem::Star))
    }

    pub fn has_limit(&self) -> bool {
        self.limit.is_some()
    }

    pub fn get_from(&self) -> Option<&Vec<TableExpression>> {
        self.from.as_ref()
    }

    pub fn get_where_clause(&self) -> Option<&DomainExpression> {
        self.where_clause.as_ref()
    }

    pub fn get_select_list(&self) -> &[SelectItem] {
        &self.select_list
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

    pub fn from_subquery(mut self, query: QueryExpression, alias: impl Into<String>) -> Self {
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

    pub fn limit(mut self, count: i64) -> Self {
        self.limit = Some(Limit::new(count));
        self
    }

    pub fn limit_offset(mut self, count: i64, offset: i64) -> Self {
        self.limit = Some(Limit::with_offset(count, offset));
        self
    }

    pub fn build(self) -> Result<SelectStatement, String> {
        // Validation
        if self.select_list.is_empty() {
            return Err("SELECT statement requires at least one select item".to_string());
        }

        if self.having.is_some() && self.group_by.is_none() {
            return Err("HAVING clause requires GROUP BY".to_string());
        }

        Ok(SelectStatement {
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
