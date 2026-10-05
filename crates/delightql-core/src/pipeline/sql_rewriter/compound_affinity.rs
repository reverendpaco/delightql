// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// compound_affinity.rs — SQLite: a compound column's arms must agree on
// affinity, or the column has none.
//
// SQLite gives each column of a compound SELECT (a UNION ALL spine, a
// VALUES body, a recursive accumulation) the affinity of ONE constituent
// arm, and which arm is indeterminate: the engine documents that the
// choice may differ between two evaluations inside one query. A WHERE
// term pushed down into the arms sees each row's own storage class while
// the same term re-evaluated on the outer loop sees the row after the
// chosen affinity converted it — so one predicate observes two
// denotations of one value, and a row can satisfy neither a filter nor
// its complement. A plain read through such a column converts values the
// arms never converted.
//
// A compound whose arms disagree on a column's affinity is therefore not
// realizable as written. This legalization makes every column's arms
// agree: where they do not, each arm expression that carries an affinity
// is wrapped in unary `+`, which SQLite defines as an expression with no
// affinity. The column then has none, no read converts, and every row
// keeps the storage class its arm produced. Arms that already agree are
// left alone: a homogeneous compound converts nothing and keeps the
// comparison class its arms share.
//
// The judgment reads the emitted shape. A literal, function, operator or
// case has no affinity; the exact operand has its argument's. A CAST has
// its target type's. A column read from a catalog entity has its declared
// type's. A column read from a derived relation has the affinity of the
// expression that published it, which this pass records as it descends in
// emission order. A column it cannot judge — a table-valued function's, a
// scratch relation's — counts as unlike, so the compound it reaches is made
// affinity-free rather than left indeterminate.

use std::collections::{HashMap, HashSet};

use crate::diagnostic::Internal;
use crate::error::{DelightQLError, Result};
use crate::names::{ColId, Registry, ScopeId, ScopeKind};
use crate::pipeline::asts::core::LiteralValue;
use crate::pipeline::generator::SqlDialect;
use crate::pipeline::sql_ast::{
    Cte, DomainExpression, JoinCondition, QueryExpression, SelectItem, SelectStatement,
    SqlFrameBound, SqlStatement, TableExpression, UnaryOperator,
};

/// Only SQLite types the value and not the column; every other target
/// resolves a compound column to one static type before a row exists.
pub fn needs_legalization(dialect: SqlDialect) -> bool {
    matches!(dialect, SqlDialect::SQLite)
}

use crate::pipeline::sqlite_affinity::{declared_affinity, Affinity};

/// What this pass knows about an expression's affinity. `None` is a
/// column the compiler cannot judge, which no arm may be assumed to match.
type Judged = Option<Affinity>;

/// `CAST(e AS t)` has the affinity a column declared `t` would have.
fn cast_affinity(type_name: &str) -> Judged {
    declared_affinity(Some(type_name))
}

fn like(arms: &[Judged]) -> bool {
    let Some(Some(first)) = arms.first() else {
        return arms.is_empty();
    };
    arms.iter().all(|arm| *arm == Some(*first))
}

pub fn legalize_compound_affinity(stmt: &mut SqlStatement, identities: &Registry) -> Result<()> {
    let mut pass = Pass {
        identities,
        env: HashMap::new(),
        physical: HashSet::new(),
    };
    pass.statement(stmt)
}

struct Pass<'a> {
    identities: &'a Registry,
    /// Every published position judged so far, in emission order.
    env: HashMap<ColId, Judged>,
    /// Scopes this statement reads as catalog entities: their columns'
    /// declared types are physical facts.
    physical: HashSet<ScopeId>,
}

impl Pass<'_> {
    fn statement(&mut self, stmt: &mut SqlStatement) -> Result<()> {
        match stmt {
            SqlStatement::DropTempTable { .. } => Ok(()),
            SqlStatement::Query { with_clause, query }
            | SqlStatement::CreateTempTable {
                with_clause, query, ..
            }
            | SqlStatement::Insert {
                with_clause,
                source: query,
                ..
            } => {
                self.ctes(with_clause.as_deref_mut())?;
                self.query(query).map(drop)
            }
            SqlStatement::Delete {
                with_clause,
                where_clause,
                ..
            } => {
                self.ctes(with_clause.as_deref_mut())?;
                if let Some(predicate) = where_clause {
                    self.expr(predicate)?;
                }
                Ok(())
            }
            SqlStatement::Update {
                with_clause,
                set_clause,
                where_clause,
                ..
            } => {
                self.ctes(with_clause.as_deref_mut())?;
                for (_, value) in set_clause {
                    self.expr(value)?;
                }
                if let Some(predicate) = where_clause {
                    self.expr(predicate)?;
                }
                Ok(())
            }
        }
    }

    fn ctes(&mut self, ctes: Option<&mut [Cte]>) -> Result<()> {
        for cte in ctes.into_iter().flatten() {
            self.cte(cte)?;
        }
        Ok(())
    }

    /// A binding publishes what its body's columns carry. A fixpoint is a
    /// compound of its parts: the recursive table has one affinity per
    /// column and SQLite applies it to every accumulated row, so the parts
    /// must agree exactly as a spine's arms must.
    fn cte(&mut self, cte: &mut Cte) -> Result<()> {
        let names = cte.column_names().map(|names| names.to_vec());
        let mut parts = cte.parts_mut();
        let Some((anchor, members)) = parts.split_first_mut() else {
            return Ok(());
        };
        let mut columns = self.query(anchor)?;
        if !members.is_empty() {
            // A member reads the accumulation through the binding's own
            // slots: seed them from the anchor so that read is judged
            // rather than unknown, then settle each column across parts.
            for (position, judged) in columns.iter().enumerate() {
                for slot in published_slots(anchor, position) {
                    self.env.insert(slot, *judged);
                }
            }
            let mut member_columns = Vec::with_capacity(members.len());
            for member in members.iter_mut() {
                member_columns.push(self.query(member)?);
            }
            for position in 0..columns.len() {
                let mut arms = vec![columns[position]];
                for member in &member_columns {
                    arms.push(member.get(position).copied().flatten());
                }
                if !like(&arms) {
                    self.strip(anchor, position)?;
                    for member in members.iter_mut() {
                        self.strip(member, position)?;
                    }
                    columns[position] = Some(Affinity::Blob);
                }
                for part in std::iter::once(&**anchor).chain(members.iter().map(|m| &**m)) {
                    for slot in published_slots(part, position) {
                        self.env.insert(slot, columns[position]);
                    }
                }
            }
        }
        if let Some(names) = names {
            for (slot, judged) in names.into_iter().zip(columns) {
                self.env.insert(slot, judged);
            }
        }
        Ok(())
    }

    /// Judge and legalize one query; the result is the affinity of each
    /// column it publishes, in order.
    #[stacksafe::stacksafe]
    fn query(&mut self, query: &mut QueryExpression) -> Result<Vec<Judged>> {
        match query {
            QueryExpression::Select(select) => self.select(select),
            QueryExpression::SetOperation { left, right, .. } => {
                let left_columns = self.query(left)?;
                let right_columns = self.query(right)?;
                if left_columns.len() != right_columns.len() {
                    return Err(DelightQLError::from(Internal::invariant(
                        "sql_rewriter::compound_affinity",
                        format!(
                            "set operation arms publish {} and {} columns",
                            left_columns.len(),
                            right_columns.len()
                        ),
                    )));
                }
                let mut columns = Vec::with_capacity(left_columns.len());
                for (position, (l, r)) in left_columns.into_iter().zip(right_columns).enumerate() {
                    columns.push(if like(&[l, r]) {
                        l
                    } else {
                        self.strip(left, position)?;
                        self.strip(right, position)?;
                        Some(Affinity::Blob)
                    });
                }
                // The compound's column is what every arm's position now
                // carries, whichever slot a reader addresses it by.
                for (position, judged) in columns.iter().enumerate() {
                    for slot in published_slots(query, position) {
                        self.env.insert(slot, *judged);
                    }
                }
                Ok(columns)
            }
        }
    }

    fn select(&mut self, select: &mut SelectStatement) -> Result<Vec<Judged>> {
        if let Some(from) = select.from_mut() {
            for table in from {
                self.table(table)?;
            }
        }
        if let Some(predicate) = select.where_clause_mut() {
            self.expr(predicate)?;
        }
        if let Some(keys) = select.group_by_mut() {
            for key in keys {
                self.expr(key)?;
            }
        }
        if let Some(predicate) = select.having_mut() {
            self.expr(predicate)?;
        }
        if let Some(terms) = select.order_by_mut() {
            for term in terms {
                self.expr(term.expr_mut())?;
            }
        }
        let mut columns = Vec::new();
        for item in select.select_list_mut() {
            match item {
                SelectItem::Publishing { expr, slot, .. }
                | SelectItem::Scaffolding { expr, slot } => {
                    let judged = self.expr(expr)?;
                    self.env.insert(*slot, judged);
                    columns.push(judged);
                }
                SelectItem::Star { reads, expansion } => {
                    for (read, published) in reads.iter().zip(expansion.iter()) {
                        let judged = self.column(*read);
                        self.env.insert(*published, judged);
                        columns.push(judged);
                    }
                }
            }
        }
        Ok(columns)
    }

    #[stacksafe::stacksafe]
    fn table(&mut self, table: &mut TableExpression) -> Result<()> {
        match table {
            TableExpression::Scope(scope) => {
                if matches!(
                    self.identities.kind_of(*scope),
                    ScopeKind::BaseTable { .. } | ScopeKind::Resolution { .. }
                ) {
                    self.physical.insert(*scope);
                }
                Ok(())
            }
            TableExpression::Entity { alias, .. } => {
                if let Some(alias) = alias {
                    self.physical.insert(*alias);
                }
                Ok(())
            }
            TableExpression::Subquery { query, .. } => self.query(query).map(drop),
            TableExpression::Join {
                left,
                right,
                join_condition,
                ..
            } => {
                self.table(left)?;
                self.table(right)?;
                if let JoinCondition::On(predicate) = join_condition {
                    self.expr(predicate)?;
                }
                Ok(())
            }
            TableExpression::TVF { .. } => Ok(()),
        }
    }

    fn column(&self, column: ColId) -> Judged {
        if let Some(judged) = self.env.get(&column) {
            return *judged;
        }
        let scope = self.identities.scope_of(column);
        let physical = self.physical.contains(&scope)
            || matches!(
                self.identities.kind_of(scope),
                ScopeKind::BaseTable { .. } | ScopeKind::Resolution { .. }
            );
        physical
            .then(|| declared_affinity(self.identities.facts(column).declared_type.as_deref()))
            .flatten()
    }

    /// Judge an expression's affinity, legalizing every query it nests.
    #[stacksafe::stacksafe]
    fn expr(&mut self, expr: &mut DomainExpression) -> Result<Judged> {
        Ok(match expr {
            DomainExpression::Literal(_)
            | DomainExpression::PublishedNameLiteral(_)
            | DomainExpression::PublishedJsonPathLiteral(_)
            | DomainExpression::JsonPathLiteral(_)
            | DomainExpression::ScopeNameLiteral(_)
            | DomainExpression::Star => Some(Affinity::Blob),
            DomainExpression::Column(column) => self.column(*column),
            DomainExpression::Cast { expr, type_name } => {
                self.expr(expr)?;
                cast_affinity(type_name)
            }
            DomainExpression::Parens(inner) => self.expr(inner)?,
            DomainExpression::Unary { expr, .. } => {
                self.expr(expr)?;
                Some(Affinity::Blob)
            }
            DomainExpression::Subquery(query) => {
                let columns = self.query(query)?;
                columns.first().copied().unwrap_or(Some(Affinity::Blob))
            }
            DomainExpression::Exists { query, .. } => {
                self.query(query)?;
                Some(Affinity::Blob)
            }
            DomainExpression::Binary { left, right, .. } => {
                self.expr(left)?;
                self.expr(right)?;
                Some(Affinity::Blob)
            }
            // The exact operand is its argument under a postfix collation,
            // which keeps the argument's affinity.
            DomainExpression::Function {
                name: crate::pipeline::sql_ast::FunctionName::Intrinsic(crate::names::Intrinsic::Exact),
                args,
                ..
            } if args.len() == 1 => self.expr(&mut args[0])?,
            DomainExpression::Function { args, .. }
            | DomainExpression::PredicateRewrite { args, .. } => {
                for arg in args {
                    self.expr(arg)?;
                }
                Some(Affinity::Blob)
            }
            DomainExpression::Case {
                expr,
                when_clauses,
                else_clause,
            } => {
                if let Some(subject) = expr {
                    self.expr(subject)?;
                }
                for clause in when_clauses {
                    self.expr(clause.when_mut())?;
                    self.expr(clause.then_mut())?;
                }
                if let Some(otherwise) = else_clause {
                    self.expr(otherwise)?;
                }
                Some(Affinity::Blob)
            }
            DomainExpression::WindowFunction {
                args,
                partition_by,
                order_by,
                frame,
                ..
            } => {
                for arg in args {
                    self.expr(arg)?;
                }
                for key in partition_by {
                    self.expr(key)?;
                }
                for (key, _) in order_by {
                    self.expr(key)?;
                }
                if let Some(frame) = frame {
                    for bound in [&mut frame.start, &mut frame.end] {
                        if let SqlFrameBound::Preceding(e) | SqlFrameBound::Following(e) = bound {
                            self.expr(e)?;
                        }
                    }
                }
                Some(Affinity::Blob)
            }
            DomainExpression::Observation { expr, .. } => {
                self.expr(expr)?;
                Some(Affinity::Blob)
            }
        })
    }

    /// Make column `position` of every arm under `query` affinity-free.
    #[stacksafe::stacksafe]
    fn strip(&mut self, query: &mut QueryExpression, position: usize) -> Result<()> {
        match query {
            QueryExpression::Select(select) => {
                let items = select.select_list_mut();
                let (index, offset) = item_at(items, position)?;
                if let SelectItem::Star { reads, expansion } = &items[index] {
                    if reads.len() != expansion.len() {
                        return Err(DelightQLError::from(Internal::invariant(
                            "sql_rewriter::compound_affinity",
                            "a star reads and publishes runs of unequal width".to_string(),
                        )));
                    }
                    let expanded: Vec<SelectItem> = reads
                        .iter()
                        .zip(expansion.iter())
                        .map(|(read, published)| {
                            SelectItem::expression_with_alias(
                                DomainExpression::Column(*read),
                                *published,
                            )
                        })
                        .collect();
                    items.splice(index..=index, expanded);
                }
                let item = &mut items[index + offset];
                let slot = match item {
                    SelectItem::Publishing { slot, .. } | SelectItem::Scaffolding { slot, .. } => {
                        *slot
                    }
                    SelectItem::Star { .. } => {
                        unreachable!("the star at this position was expanded")
                    }
                };
                let judged = match item.expr_mut() {
                    Some(expr) => self.expr(expr)?,
                    None => Some(Affinity::Blob),
                };
                if judged != Some(Affinity::Blob) {
                    let freed = match item {
                        SelectItem::Publishing { expr, slot, .. } => SelectItem::Publishing {
                            expr: plus(std::mem::replace(expr, DomainExpression::Star)),
                            slot: *slot,
                            printed: true,
                        },
                        SelectItem::Scaffolding { expr, slot } => SelectItem::Scaffolding {
                            expr: plus(std::mem::replace(expr, DomainExpression::Star)),
                            slot: *slot,
                        },
                        SelectItem::Star { .. } => unreachable!(),
                    };
                    *item = freed;
                }
                self.env.insert(slot, Some(Affinity::Blob));
                Ok(())
            }
            QueryExpression::SetOperation { left, right, .. } => {
                self.strip(left, position)?;
                self.strip(right, position)
            }
        }
    }
}

/// A typed NULL freed of its affinity is NULL: the CAST existed only to
/// give a pad the type a strict target demands, and SQLite is not one.
fn plus(expr: DomainExpression) -> DomainExpression {
    match expr {
        DomainExpression::Cast { expr, .. }
            if matches!(*expr, DomainExpression::Literal(LiteralValue::Null)) =>
        {
            *expr
        }
        expr => DomainExpression::Unary {
            op: UnaryOperator::Plus,
            expr: Box::new(expr),
        },
    }
}

/// The select item occupying emitted column `position`, and the offset
/// into it when the item is a star standing for a run.
fn item_at(items: &[SelectItem], position: usize) -> Result<(usize, usize)> {
    let mut start = 0;
    for (index, item) in items.iter().enumerate() {
        let width = item.publishes().slots();
        if position < start + width {
            return Ok((index, position - start));
        }
        start += width;
    }
    Err(DelightQLError::from(Internal::invariant(
        "sql_rewriter::compound_affinity",
        format!("no select item occupies column {position}"),
    )))
}

/// Every slot the arms of `query` publish at emitted column `position`.
pub(super) fn published_slots(query: &QueryExpression, position: usize) -> Vec<ColId> {
    match query {
        QueryExpression::Select(select) => {
            let mut start = 0;
            for item in select.select_list() {
                let width = item.publishes().slots();
                if position < start + width {
                    return match item {
                        SelectItem::Publishing { slot, .. }
                        | SelectItem::Scaffolding { slot, .. } => {
                            vec![*slot]
                        }
                        SelectItem::Star { expansion, .. } => vec![expansion[position - start]],
                    };
                }
                start += width;
            }
            Vec::new()
        }
        QueryExpression::SetOperation { left, right, .. } => {
            let mut slots = published_slots(left, position);
            slots.extend(published_slots(right, position));
            slots
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arms_are_like_only_when_every_one_is_judged_and_equal() {
        assert!(like(&[]));
        assert!(like(&[Some(Affinity::Real), Some(Affinity::Real)]));
        assert!(!like(&[Some(Affinity::Real), Some(Affinity::Blob)]));
        assert!(!like(&[Some(Affinity::Blob), None]));
        assert!(!like(&[None, None]));
    }
}
