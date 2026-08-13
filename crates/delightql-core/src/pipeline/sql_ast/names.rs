// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Exhaustive identity enumeration for late SQL naming.

use super::{
    DomainExpression, JoinCondition, QueryExpression, SelectItem, SqlFrameBound, SqlStatement,
    TableExpression, TvfArgument,
};
use crate::names::{ColId, Registry, ScopeId};

pub(crate) struct NameCollector<'a> {
    identities: &'a Registry,
    scopes: Vec<ScopeId>,
    refs: Vec<ColId>,
}

impl<'a> NameCollector<'a> {
    pub(crate) fn new(identities: &'a Registry) -> Self {
        Self {
            identities,
            scopes: Vec::new(),
            refs: Vec::new(),
        }
    }

    pub(crate) fn scope(&mut self, scope: ScopeId) {
        if !self.scopes.contains(&scope) {
            self.scopes.push(scope);
        }
    }

    pub(crate) fn column(&mut self, column: ColId) {
        self.scope(self.identities.scope_of(column));
        if !self.refs.contains(&column) {
            self.refs.push(column);
        }
    }

    #[stacksafe::stacksafe]
    pub(crate) fn expression(&mut self, expression: &DomainExpression) {
        match expression {
            DomainExpression::Column(column)
            | DomainExpression::PublishedNameLiteral(column)
            | DomainExpression::PublishedJsonPathLiteral(column) => self.column(*column),
            DomainExpression::ScopeNameLiteral(scope) => self.scope(*scope),
            DomainExpression::Literal(_)
            | DomainExpression::JsonPathLiteral(_)
            | DomainExpression::Star => {}
            DomainExpression::Cast { expr, .. }
            | DomainExpression::Unary { expr, .. }
            | DomainExpression::Observation { expr, .. }
            | DomainExpression::Parens(expr) => self.expression(expr),
            DomainExpression::Binary { left, right, .. } => {
                self.expression(left);
                self.expression(right);
            }
            DomainExpression::Function { args, .. }
            | DomainExpression::PredicateRewrite { args, .. } => {
                for argument in args {
                    self.expression(argument);
                }
            }
            DomainExpression::Case {
                expr,
                when_clauses,
                else_clause,
            } => {
                if let Some(expr) = expr {
                    self.expression(expr);
                }
                for arm in when_clauses {
                    self.expression(arm.when());
                    self.expression(arm.then());
                }
                if let Some(expr) = else_clause {
                    self.expression(expr);
                }
            }
            DomainExpression::Exists { query, .. } | DomainExpression::Subquery(query) => {
                self.query(query)
            }
            DomainExpression::WindowFunction {
                args,
                partition_by,
                order_by,
                frame,
                ..
            } => {
                for argument in args {
                    self.expression(argument);
                }
                for partition in partition_by {
                    self.expression(partition);
                }
                for (expression, _) in order_by {
                    self.expression(expression);
                }
                if let Some(frame) = frame {
                    for bound in [&frame.start, &frame.end] {
                        match bound {
                            SqlFrameBound::Preceding(expression)
                            | SqlFrameBound::Following(expression) => self.expression(expression),
                            SqlFrameBound::Unbounded | SqlFrameBound::CurrentRow => {}
                        }
                    }
                }
            }
        }
    }

    #[stacksafe::stacksafe]
    fn table(&mut self, table: &TableExpression) {
        match table {
            TableExpression::Scope(scope) | TableExpression::QualifiedScope { scope, .. } => {
                self.scope(*scope)
            }
            TableExpression::Entity { alias, .. } => {
                if let Some(alias) = alias {
                    self.scope(*alias);
                }
            }
            TableExpression::Subquery { query, alias } => {
                self.query(query);
                self.scope(*alias);
            }
            TableExpression::Join {
                left,
                right,
                join_condition,
                ..
            } => {
                self.table(left);
                self.table(right);
                match join_condition {
                    JoinCondition::On(expression) => self.expression(expression),
                    JoinCondition::Using(columns) => {
                        for column in columns {
                            self.column(*column);
                        }
                    }
                    JoinCondition::Natural => {}
                }
            }
            TableExpression::TVF {
                arguments, alias, ..
            } => {
                for argument in arguments {
                    match argument {
                        TvfArgument::Literal(_) => {}
                        TvfArgument::Column(column) => self.column(*column),
                    }
                }
                self.scope(*alias);
            }
        }
    }

    #[stacksafe::stacksafe]
    pub(crate) fn query(&mut self, query: &QueryExpression) {
        match query {
            QueryExpression::Select(select) => {
                for item in select.select_list() {
                    match item {
                        SelectItem::Star { .. } => {}
                        SelectItem::Expression { expr, alias } => {
                            self.expression(expr);
                            if let Some(alias) = alias {
                                self.column(*alias);
                            }
                        }
                    }
                }
                if let Some(tables) = select.from() {
                    for table in tables {
                        self.table(table);
                    }
                }
                if let Some(expression) = select.where_clause() {
                    self.expression(expression);
                }
                if let Some(expressions) = select.group_by() {
                    for expression in expressions {
                        self.expression(expression);
                    }
                }
                if let Some(expression) = select.having() {
                    self.expression(expression);
                }
                if let Some(terms) = select.order_by() {
                    for term in terms {
                        self.expression(term.expr());
                    }
                }
            }
            QueryExpression::SetOperation { left, right, .. } => {
                self.query(left);
                self.query(right);
            }
            QueryExpression::Values { rows } => {
                for row in rows {
                    for expression in row {
                        self.expression(expression);
                    }
                }
            }
            QueryExpression::WithCte { ctes, query } => {
                for cte in ctes {
                    self.scope(cte.scope());
                    self.query(cte.query());
                }
                self.query(query);
            }
        }
    }

    pub(crate) fn finish(self) -> crate::names::Statement {
        let headings = self
            .scopes
            .iter()
            .map(|scope| self.identities.heading(*scope).columns_seen())
            .filter(|heading| !heading.is_empty())
            .collect();
        crate::names::Statement {
            scopes: self.scopes,
            headings,
            refs: self.refs,
        }
    }
}

pub fn statement_names(statement: &SqlStatement, identities: &Registry) -> crate::names::Statement {
    let mut names = NameCollector::new(identities);
    match statement {
        // The name is the relation's own; there is nothing inside to walk.
        SqlStatement::DropTempTable { table } => names.scope(*table),
        SqlStatement::Query { with_clause, query } => {
            if let Some(ctes) = with_clause {
                for cte in ctes {
                    names.scope(cte.scope());
                    names.query(cte.query());
                }
            }
            names.query(query);
        }
        SqlStatement::CreateTempTable {
            table,
            with_clause,
            query,
        }
        | SqlStatement::CreateTempView {
            view: table,
            with_clause,
            query,
        } => {
            names.scope(*table);
            if let Some(ctes) = with_clause {
                for cte in ctes {
                    names.scope(cte.scope());
                    names.query(cte.query());
                }
            }
            names.query(query);
        }
        SqlStatement::Delete {
            target,
            target_scope,
            with_clause,
            where_clause,
        } => {
            target_names(target, &mut names);
            names.scope(*target_scope);
            cte_names(with_clause, &mut names);
            if let Some(expression) = where_clause {
                names.expression(expression);
            }
        }
        SqlStatement::Update {
            target,
            target_scope,
            with_clause,
            set_clause,
            where_clause,
        } => {
            target_names(target, &mut names);
            names.scope(*target_scope);
            cte_names(with_clause, &mut names);
            for (column, expression) in set_clause {
                names.column(*column);
                names.expression(expression);
            }
            if let Some(expression) = where_clause {
                names.expression(expression);
            }
        }
        SqlStatement::Insert {
            target,
            target_scope,
            columns,
            with_clause,
            source,
        } => {
            target_names(target, &mut names);
            names.scope(*target_scope);
            for column in columns {
                names.column(*column);
            }
            cte_names(with_clause, &mut names);
            names.query(source);
        }
    }
    names.finish()
}

fn cte_names(ctes: &Option<Vec<super::Cte>>, names: &mut NameCollector<'_>) {
    if let Some(ctes) = ctes {
        for cte in ctes {
            names.scope(cte.scope());
            names.query(cte.query());
        }
    }
}

fn target_names(target: &super::statements::RelationTarget, names: &mut NameCollector<'_>) {
    match target {
        super::statements::RelationTarget::Scope(scope)
        | super::statements::RelationTarget::QualifiedScope { scope, .. } => names.scope(*scope),
        super::statements::RelationTarget::Entity(_) => {}
    }
}
