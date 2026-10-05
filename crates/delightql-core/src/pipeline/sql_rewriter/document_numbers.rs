// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// document_numbers.rs — a value read out of a document carries no category
// on a target whose document read does not state one; every operation that
// needs a category must be shown one.
//
// A DelightQL NUMBER carries its category (integer, decimal, approximate)
// to the target as a direct literal. Admission into a target document is a
// different boundary: PostgreSQL's native construction writes `1.25e2` as
// `125`, `jsonb` canonicalizes it, and the scalar read publishes every
// document number as TEXT (`#>>`; DuckDB's `json_extract_string` alike) or
// as a document value (`#>` jsonb). From there a numeric consumer is refused
// by the target (`sum(text)`), a comparison mixes types (`jsonb > integer`),
// and an ORDERED consumer — `min`, `max`, an ORDER BY key — answers in
// LEXICAL order (`'10' < '2'`) without any error at all. The compiler may
// not guess a category from the runtime bytes, and it may not cast every
// document number to one type, which would promote the integers and
// decimals beside the approximates.
//
// THE ACCOUNT IS TOTAL AND FAILS CLOSED. Every expression has evidence:
// either it is a DOCUMENT value — a document read, or something that merely
// selects among document values — or it is TYPED with a kind the target or
// the author established (a literal, a catalog column, a cast, the result
// of an operation over typed operands). Every operation states, by an
// explicit contract, what it does with evidence: it DEMANDS a category of
// an operand (a numeric one for `sum` and arithmetic; any stated one for a
// comparison, `min`/`max`, an ordering or grouping key), it PRESERVES a
// document value (a nested read, a CASE arm, the compiler's own structured
// constructors, `count`, a NULL test), or it ESTABLISHES a kind (a cast —
// exactly the kind it names, so a cast to text discharges no numeric
// demand). An operation with no contract — any callable the compiler did
// not spell itself — receives a document value only by refusing: not
// knowing what a callable needs is not proof that it needs nothing. The
// remedy is always the same and is named in the refusal: an authored cast
// stating the category the author means.
//
// Only a target whose read is TYPED (SQLite's `json_extract` hands back an
// integer or a real) makes no judgment. A target whose read erases the
// category is judged; a target no lane has measured is judged too, because
// the ruled alpha contract requires proof and an unmeasured read is not
// proof.
//
// The document read is recognized by its typed operand, never by a
// function's spelling: the transformer hands every reach to the SQL AST as
// a `JsonPathLiteral`/`PublishedJsonPathLiteral` argument, and no other
// operation carries one.
//
// THE ACCOUNT COVERS THE WHOLE SQL AST, not only its expression nodes: a
// consumer may stand in surrounding structure. `SELECT DISTINCT` compares
// every published position; a call's `DISTINCT` compares its arguments
// before the contract applies (`count(x)` consumes nothing, `count(DISTINCT
// x)` compares); a merge join pair is the equality it is written as; a
// deduplicating fixpoint (`UNION`) compares every position of every part;
// an INSERT source column or an UPDATE assignment is compared with a target
// column's type. Each spends the stated demand. The rest of the structure
// consumes no evidence and says so where it is walked: a set operation is
// `UNION ALL` only (the AST has no other operator); a bound (LIMIT/OFFSET)
// and an ordering direction read no value.
//
// A TABLE-VALUED FUNCTION IS JUDGED BY ITS ORIGIN, not by the storage
// variant it shares. `TableExpression::TVF` has two producers: a sequence
// expansion, whose function is the intrinsic iterator and takes a document
// AS the document it iterates; and
// `r_lower_tvf`, which lowers every authored relation functor — a catalog
// TVF or, under permissive resolution, any unknown target table function
// (`generate_series`). The second is a callable the compiler did not spell:
// it has no contract, and handed a document value it refuses, exactly as
// an unknown scalar callable does. `Registry::function_origin` is the one
// fact that tells the two apart; a third producer would have to mint an
// origin and would be judged by it.

use std::collections::HashMap;

use crate::diagnostic::{Constraint, Internal};
use crate::error::{DelightQLError, Result};
use crate::names::{ColId, FnId, FnOrigin, Intrinsic, Registry};
use crate::pipeline::asts::core::LiteralValue;
use crate::pipeline::generator::{DocumentNumberCategory, SqlDialect};
use crate::pipeline::sql_ast::{
    BinaryOperator, Cte, CteBody, DomainExpression, FunctionName, JoinCondition, QueryExpression,
    SelectItem, SelectStatement, SqlFrameBound, SqlStatement, TableExpression, TvfArgument,
    UnaryOperator,
};

use super::compound_affinity::published_slots;

/// Every target but one whose document read STATES a category is judged:
/// an erased read needs proof, and an unmeasured read is not proof.
pub fn needs_judgment(dialect: SqlDialect) -> bool {
    dialect.document_number_category() != DocumentNumberCategory::Stated
}

/// The kind a typed value was established in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Numeric,
    Text,
    Boolean,
    /// A record, tuple or array the compiler constructed or carried.
    Structured,
    /// A value the target types itself in a kind the compiler did not
    /// state: a catalog or TVF column, NULL, the result of a typed
    /// operation over typed operands.
    Target,
}

/// What this pass knows about an expression.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Evidence {
    /// A value read out of a document, or one that merely selects among
    /// such values: no category has been established for it.
    Document,
    /// A category was established, by the target or by the author.
    Typed(Kind),
}

/// What an operation requires of an operand.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Demand {
    /// A numeric category: `sum`, arithmetic, a sign.
    Numeric,
    /// Any established category: a comparison, an ordered reduction, an
    /// ordering or grouping key, a predicate position.
    Stated,
}

fn satisfies(evidence: Evidence, demand: Demand) -> bool {
    match (evidence, demand) {
        (Evidence::Document, _) => false,
        (Evidence::Typed(_), Demand::Stated) => true,
        (Evidence::Typed(kind), Demand::Numeric) => {
            matches!(kind, Kind::Numeric | Kind::Target)
        }
    }
}

/// The kind a cast establishes, from the DQL-canonical type word the SQL
/// AST carries. A word outside the canonical five is the target's own.
fn cast_kind(type_name: &str) -> Kind {
    match type_name.to_ascii_lowercase().as_str() {
        "integer" | "real" | "numeric" => Kind::Numeric,
        "text" => Kind::Text,
        "boolean" => Kind::Boolean,
        _ => Kind::Target,
    }
}

/// The evidence of a value that may be any of several: a document if any
/// is, one kind if all agree, otherwise the target's own.
fn join(evidences: impl IntoIterator<Item = Evidence>) -> Evidence {
    let mut joined: Option<Evidence> = None;
    for evidence in evidences {
        joined = Some(match (joined, evidence) {
            (None, e) => e,
            (Some(Evidence::Document), _) | (_, Evidence::Document) => Evidence::Document,
            (Some(Evidence::Typed(a)), Evidence::Typed(b)) if a == b => Evidence::Typed(a),
            (Some(Evidence::Typed(_)), Evidence::Typed(_)) => Evidence::Typed(Kind::Target),
        });
    }
    joined.unwrap_or(Evidence::Typed(Kind::Target))
}

/// What a named operation does with its arguments' evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Contract {
    /// Requires the demand of every argument; the result is the kind given.
    Requires(Demand, Kind),
    /// Requires a stated category of every argument and answers in the
    /// arguments' joined kind (`min`, `max`).
    Ordered,
    /// Accepts any evidence and answers a count.
    Counts,
    /// Accepts any evidence — a document value is what it is made to hold —
    /// and answers a structured value: the compiler's own constructors.
    Structured,
    /// Hands its arguments' joined evidence through unchanged.
    Preserves,
    /// No contract: the compiler did not spell this callable and cannot
    /// know what it needs. It receives a document value only by refusing;
    /// over typed operands its result is the target's own.
    Unknown,
}

fn user_contract(name: &str) -> Contract {
    match name.to_ascii_lowercase().as_str() {
        "sum" | "avg" | "total" => Contract::Requires(Demand::Numeric, Kind::Numeric),
        "min" | "max" => Contract::Ordered,
        "count" => Contract::Counts,
        // The spellings the transformer itself mints over structured values
        // (`tree_group`, `scalar`): each is a constructor or re-marking of a
        // document, and a document value is exactly what it holds.
        "json_object" | "json_array" | "json_group_object" | "json_group_array" | "json"
        | "json_quote" => Contract::Structured,
        _ => Contract::Unknown,
    }
}

fn intrinsic_contract(intrinsic: Intrinsic) -> Contract {
    match intrinsic {
        // A document read is judged apart (its typed reach operand), so
        // reaching here means a raw read without one; it hands through. The
        // admission and the label hand their value to the constructor that
        // holds it.
        Intrinsic::JsonExtractRaw
        | Intrinsic::Arbitrary
        | Intrinsic::JsonScalar
        | Intrinsic::JsonLabel
        | Intrinsic::Exact => Contract::Preserves,
        Intrinsic::JsonEachDocument
        | Intrinsic::JsonEachArray
        | Intrinsic::JsonEachObject
        | Intrinsic::JsonObject
        | Intrinsic::JsonSplice => Contract::Structured,
        Intrinsic::ScalarMax | Intrinsic::ScalarMin => Contract::Ordered,
        Intrinsic::Round2 => Contract::Requires(Demand::Numeric, Kind::Numeric),
    }
}

/// A document read: an operation carrying a typed reach as an argument.
fn is_document_read(args: &[DomainExpression]) -> bool {
    args.iter().any(|arg| {
        matches!(
            arg,
            DomainExpression::JsonPathLiteral(_) | DomainExpression::PublishedJsonPathLiteral(_)
        )
    })
}

fn is_null_literal(expr: &DomainExpression) -> bool {
    matches!(expr, DomainExpression::Literal(LiteralValue::Null))
}

pub fn refuse_unproved_document_numbers(
    stmt: &SqlStatement,
    dialect: SqlDialect,
    identities: &Registry,
) -> Result<()> {
    let mut pass = Pass {
        dialect,
        identities,
        env: HashMap::new(),
    };
    pass.statement(stmt)
}

struct Pass<'a> {
    dialect: SqlDialect,
    /// The one authority for a table function's origin — intrinsic or user.
    identities: &'a Registry,
    /// Every published position judged so far, in emission order.
    env: HashMap<ColId, Evidence>,
}

impl Pass<'_> {
    /// The authored spelling of a table function, for its refusal.
    fn table_function_name(&self, function: FnId) -> String {
        let mut name = String::new();
        if self
            .identities
            .write_function_name(function, &mut crate::names::sink::Teaching(&mut name))
            .is_err()
            || name.is_empty()
        {
            name = "the table function".to_string();
        }
        name
    }

    fn refuse(&self, consumer: &str, demand: Demand, found: Evidence) -> DelightQLError {
        let target = self.dialect.family_name();
        let read = match self.dialect.document_number_category() {
            DocumentNumberCategory::Erased => "the target's document read publishes text",
            DocumentNumberCategory::Unmeasured | DocumentNumberCategory::Stated => {
                "no lane has measured what the target's document read publishes"
            }
        };
        let message = match (demand, found) {
            (Demand::Numeric, Evidence::Document) => format!(
                "on {target} a number read out of a document carries no numeric category — \
                 {read} — so `{consumer}` cannot take it as written; state the category you \
                 mean with a cast (cast:(x, ::integer) or cast:(x, ::real)) between the read \
                 and the {consumer}"
            ),
            (Demand::Numeric, Evidence::Typed(kind)) => format!(
                "on {target} `{consumer}` needs a numeric category, and the cast in its \
                 operand states {} instead; cast the value to ::integer or ::real",
                match kind {
                    Kind::Text => "text",
                    Kind::Boolean => "boolean",
                    Kind::Structured => "a structured value",
                    Kind::Numeric | Kind::Target => "another kind",
                }
            ),
            (Demand::Stated, _) => format!(
                "on {target} a value read out of a document carries no category — {read} — \
                 so `{consumer}` would compare or order it as text ('10' before '2') or be \
                 refused by the target; state the category you mean with a cast \
                 (cast:(x, ::integer), ::real or ::text) before the {consumer}"
            ),
        };
        DelightQLError::from(Constraint::TargetTyping { message })
    }

    fn refuse_unknown(&self, callable: &str) -> DelightQLError {
        DelightQLError::from(Constraint::TargetTyping {
            message: format!(
                "on {} `{callable}` receives a value read out of a document, and the compiler \
                 has no contract for what `{callable}` needs of it — a document read states \
                 no category there; cast the value to the category you mean (cast:(x, \
                 ::integer), ::real or ::text) before calling `{callable}`",
                self.dialect.family_name()
            ),
        })
    }

    fn statement(&mut self, stmt: &SqlStatement) -> Result<()> {
        match stmt {
            SqlStatement::DropTempTable { .. } => Ok(()),
            SqlStatement::Query { with_clause, query }
            | SqlStatement::CreateTempTable {
                with_clause, query, ..
            } => {
                self.ctes(with_clause.as_deref())?;
                self.query(query).map(drop)
            }
            // An INSERT assigns each source column to a target column of a
            // stated type: an assignment compares with that type.
            SqlStatement::Insert {
                with_clause,
                source: query,
                ..
            } => {
                self.ctes(with_clause.as_deref())?;
                for evidence in self.query(query)? {
                    if !satisfies(evidence, Demand::Stated) {
                        return Err(self.refuse("INSERT", Demand::Stated, evidence));
                    }
                }
                Ok(())
            }
            SqlStatement::Delete {
                with_clause,
                where_clause,
                ..
            } => {
                self.ctes(with_clause.as_deref())?;
                if let Some(predicate) = where_clause {
                    self.demand("WHERE", Demand::Stated, predicate)?;
                }
                Ok(())
            }
            SqlStatement::Update {
                with_clause,
                set_clause,
                where_clause,
                ..
            } => {
                self.ctes(with_clause.as_deref())?;
                for (_, value) in set_clause {
                    self.demand("SET", Demand::Stated, value)?;
                }
                if let Some(predicate) = where_clause {
                    self.demand("WHERE", Demand::Stated, predicate)?;
                }
                Ok(())
            }
        }
    }

    fn ctes(&mut self, ctes: Option<&[Cte]>) -> Result<()> {
        for cte in ctes.into_iter().flatten() {
            self.cte(cte)?;
        }
        Ok(())
    }

    /// A binding publishes what its parts' columns carry, joined across
    /// parts through the binding's slots.
    fn cte(&mut self, cte: &Cte) -> Result<()> {
        let parts = cte.parts();
        let Some((anchor, members)) = parts.split_first() else {
            return Ok(());
        };
        let mut columns = self.query(anchor)?;
        for (position, evidence) in columns.iter().enumerate() {
            for slot in published_slots(anchor, position) {
                self.env.insert(slot, *evidence);
            }
        }
        for member in members {
            let member_columns = self.query(member)?;
            for (position, evidence) in member_columns.into_iter().enumerate() {
                if let Some(column) = columns.get_mut(position) {
                    *column = join([*column, evidence]);
                }
            }
        }
        // A deduplicating fixpoint (`UNION`) compares every position of
        // every part for equality across the accumulation.
        if let CteBody::Fixpoint(fixpoint) = cte.body() {
            if fixpoint.is_deduplicating() {
                for evidence in &columns {
                    if !satisfies(*evidence, Demand::Stated) {
                        return Err(self.refuse("UNION", Demand::Stated, *evidence));
                    }
                }
            }
        }
        if let Some(names) = cte.column_names() {
            for (slot, evidence) in names.iter().zip(columns) {
                self.env.insert(*slot, evidence);
            }
        }
        Ok(())
    }

    /// Judge one query; the result is the evidence of each column it
    /// publishes, in order.
    #[stacksafe::stacksafe]
    fn query(&mut self, query: &QueryExpression) -> Result<Vec<Evidence>> {
        match query {
            QueryExpression::Select(select) => self.select(select),
            QueryExpression::SetOperation { left, right, .. } => {
                let left_columns = self.query(left)?;
                let right_columns = self.query(right)?;
                if left_columns.len() != right_columns.len() {
                    return Err(DelightQLError::from(Internal::invariant(
                        "sql_rewriter::document_numbers",
                        format!(
                            "set operation arms publish {} and {} columns",
                            left_columns.len(),
                            right_columns.len()
                        ),
                    )));
                }
                let columns: Vec<Evidence> = left_columns
                    .into_iter()
                    .zip(right_columns)
                    .map(|(l, r)| join([l, r]))
                    .collect();
                for (position, evidence) in columns.iter().enumerate() {
                    for slot in published_slots(query, position) {
                        self.env.insert(slot, *evidence);
                    }
                }
                Ok(columns)
            }
        }
    }

    fn select(&mut self, select: &SelectStatement) -> Result<Vec<Evidence>> {
        if let Some(from) = select.from() {
            for table in from {
                self.table(table)?;
            }
        }
        if let Some(predicate) = select.where_clause() {
            self.demand("WHERE", Demand::Stated, predicate)?;
        }
        if let Some(keys) = select.group_by() {
            for key in keys {
                // A grouping key compares values for equality: a document
                // number would group as text.
                self.demand("GROUP BY", Demand::Stated, key)?;
            }
        }
        if let Some(predicate) = select.having() {
            self.demand("HAVING", Demand::Stated, predicate)?;
        }
        if let Some(terms) = select.order_by() {
            for term in terms {
                // An ordering key sorts: a document number would sort as
                // text, '10' before '2', with no error to say so.
                self.demand("ORDER BY", Demand::Stated, term.expr())?;
            }
        }
        let mut columns = Vec::new();
        for item in select.select_list() {
            match item {
                SelectItem::Publishing { expr, slot, .. }
                | SelectItem::Scaffolding { expr, slot } => {
                    let evidence = self.expr(expr)?;
                    self.env.insert(*slot, evidence);
                    columns.push(evidence);
                }
                SelectItem::Star { reads, expansion } => {
                    for (read, published) in reads.iter().zip(expansion.iter()) {
                        let evidence = self.column(*read);
                        self.env.insert(*published, evidence);
                        columns.push(evidence);
                    }
                }
            }
        }
        // `SELECT DISTINCT` compares every published position for equality:
        // two document numbers '1' and '1.0' are two rows as text and one
        // as numbers.
        if select.is_distinct() {
            for evidence in &columns {
                if !satisfies(*evidence, Demand::Stated) {
                    return Err(self.refuse("DISTINCT", Demand::Stated, *evidence));
                }
            }
        }
        Ok(columns)
    }

    #[stacksafe::stacksafe]
    fn table(&mut self, table: &TableExpression) -> Result<()> {
        match table {
            TableExpression::Scope(_) | TableExpression::Entity { .. } => Ok(()),
            // A table function is judged by its ORIGIN. The compiler's own
            // iterator takes a document as the document it iterates. Any
            // other table function — a catalog TVF, a permissively admitted
            // target function — is a callable the compiler did not spell:
            // no contract, so a document value in an argument refuses.
            TableExpression::TVF {
                function,
                arguments,
                ..
            } => {
                let (callable, contract) = match self.identities.function_origin(*function) {
                    FnOrigin::Intrinsic(intrinsic) => {
                        (format!("{intrinsic:?}"), intrinsic_contract(intrinsic))
                    }
                    FnOrigin::User(_) => (self.table_function_name(*function), Contract::Unknown),
                };
                let evidences = arguments
                    .iter()
                    .map(|argument| match argument {
                        TvfArgument::Column(column) => self.column(*column),
                    })
                    .collect();
                // The WHOLE contract is spent, as a scalar call spends it: a
                // numeric or ordered intrinsic placed in table position
                // demands what it demands, the iterators admit a document as
                // the document they iterate, and an unknown callable refuses
                // one. What the table publishes is the target's own rows.
                self.apply_evidences(&callable, contract, false, evidences)?;
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
                match join_condition {
                    JoinCondition::On(predicate) => {
                        self.demand("ON", Demand::Stated, predicate)?;
                    }
                    JoinCondition::Cartesian => {}
                }
                Ok(())
            }
        }
    }

    /// A column carries what published it; a column nothing in this
    /// statement published (a catalog column, a TVF's) is the target's own.
    fn column(&self, column: ColId) -> Evidence {
        self.env
            .get(&column)
            .copied()
            .unwrap_or(Evidence::Typed(Kind::Target))
    }

    /// Judge an operand under a consumer's demand.
    fn demand(&mut self, consumer: &str, demand: Demand, operand: &DomainExpression) -> Result<()> {
        let evidence = self.expr(operand)?;
        if !satisfies(evidence, demand) {
            return Err(self.refuse(consumer, demand, evidence));
        }
        Ok(())
    }

    /// Apply a named operation's contract to its arguments. `DISTINCT`
    /// compares the arguments before the contract applies, so it spends the
    /// stated demand whatever the contract is: `count(x)` consumes nothing,
    /// `count(DISTINCT x)` compares.
    fn apply(
        &mut self,
        callable: &str,
        contract: Contract,
        distinct: bool,
        args: &[DomainExpression],
    ) -> Result<Evidence> {
        let mut evidences = Vec::with_capacity(args.len());
        for arg in args {
            evidences.push(self.expr(arg)?);
        }
        self.apply_evidences(callable, contract, distinct, evidences)
    }

    /// The one decision over a callable's contract and its arguments'
    /// evidence, shared by the scalar call, the window call and the table
    /// call: no road may compute a contract and then apply less of it.
    fn apply_evidences(
        &self,
        callable: &str,
        contract: Contract,
        distinct: bool,
        evidences: Vec<Evidence>,
    ) -> Result<Evidence> {
        if distinct {
            for evidence in &evidences {
                if !satisfies(*evidence, Demand::Stated) {
                    return Err(self.refuse(
                        &format!("{callable}(DISTINCT)"),
                        Demand::Stated,
                        *evidence,
                    ));
                }
            }
        }
        Ok(match contract {
            Contract::Requires(demand, kind) => {
                for evidence in &evidences {
                    if !satisfies(*evidence, demand) {
                        return Err(self.refuse(callable, demand, *evidence));
                    }
                }
                Evidence::Typed(kind)
            }
            Contract::Ordered => {
                for evidence in &evidences {
                    if !satisfies(*evidence, Demand::Stated) {
                        return Err(self.refuse(callable, Demand::Stated, *evidence));
                    }
                }
                join(evidences)
            }
            Contract::Counts => Evidence::Typed(Kind::Numeric),
            Contract::Structured => Evidence::Typed(Kind::Structured),
            Contract::Preserves => join(evidences),
            Contract::Unknown => {
                if evidences.contains(&Evidence::Document) {
                    return Err(self.refuse_unknown(callable));
                }
                Evidence::Typed(Kind::Target)
            }
        })
    }

    /// Judge an expression's evidence, judging every consumer it nests.
    #[stacksafe::stacksafe]
    fn expr(&mut self, expr: &DomainExpression) -> Result<Evidence> {
        Ok(match expr {
            DomainExpression::Literal(literal) => Evidence::Typed(match literal {
                LiteralValue::Number(_) => Kind::Numeric,
                LiteralValue::String(_) | LiteralValue::Symbol(_) | LiteralValue::Mention(_) => {
                    Kind::Text
                }
                LiteralValue::Boolean(_) => Kind::Boolean,
                LiteralValue::Null => Kind::Target,
            }),
            DomainExpression::PublishedNameLiteral(_)
            | DomainExpression::PublishedJsonPathLiteral(_)
            | DomainExpression::JsonPathLiteral(_)
            | DomainExpression::ScopeNameLiteral(_) => Evidence::Typed(Kind::Text),
            DomainExpression::Star => Evidence::Typed(Kind::Target),
            DomainExpression::Column(column) => self.column(*column),
            // THE PROOF: an authored cast establishes exactly the kind it
            // names, over whatever it was given.
            DomainExpression::Cast { expr, type_name } => {
                self.expr(expr)?;
                Evidence::Typed(cast_kind(type_name))
            }
            DomainExpression::Parens(inner) => self.expr(inner)?,
            DomainExpression::Unary { op, expr } => match op {
                UnaryOperator::Minus => {
                    self.demand("-", Demand::Numeric, expr)?;
                    Evidence::Typed(Kind::Numeric)
                }
                UnaryOperator::Plus => {
                    self.demand("+", Demand::Numeric, expr)?;
                    Evidence::Typed(Kind::Numeric)
                }
                UnaryOperator::Not => {
                    self.demand("NOT", Demand::Stated, expr)?;
                    Evidence::Typed(Kind::Boolean)
                }
            },
            DomainExpression::Subquery(query) => {
                let columns = self.query(query)?;
                columns
                    .first()
                    .copied()
                    .unwrap_or(Evidence::Typed(Kind::Target))
            }
            DomainExpression::Exists { query, .. } => {
                self.query(query)?;
                Evidence::Typed(Kind::Boolean)
            }
            DomainExpression::Binary { left, op, right } => {
                let spelled = format!("{op:?}");
                match op {
                    BinaryOperator::Add
                    | BinaryOperator::Subtract
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo => {
                        self.demand(&spelled, Demand::Numeric, left)?;
                        self.demand(&spelled, Demand::Numeric, right)?;
                        Evidence::Typed(Kind::Numeric)
                    }
                    // A NULL test asks only whether a value is there: a
                    // document value answers it as well as any.
                    BinaryOperator::Is | BinaryOperator::IsNot
                        if is_null_literal(left) || is_null_literal(right) =>
                    {
                        self.expr(left)?;
                        self.expr(right)?;
                        Evidence::Typed(Kind::Boolean)
                    }
                    BinaryOperator::Concatenate => {
                        self.demand(&spelled, Demand::Stated, left)?;
                        self.demand(&spelled, Demand::Stated, right)?;
                        Evidence::Typed(Kind::Text)
                    }
                    BinaryOperator::Equal
                    | BinaryOperator::NotEqual
                    | BinaryOperator::LessThan
                    | BinaryOperator::LessThanOrEqual
                    | BinaryOperator::GreaterThan
                    | BinaryOperator::GreaterThanOrEqual
                    | BinaryOperator::And
                    | BinaryOperator::Or
                    | BinaryOperator::Like
                    | BinaryOperator::NotLike
                    | BinaryOperator::Is
                    | BinaryOperator::IsNot
                    | BinaryOperator::IsNotDistinctFrom
                    | BinaryOperator::IsDistinctFrom => {
                        self.demand(&spelled, Demand::Stated, left)?;
                        self.demand(&spelled, Demand::Stated, right)?;
                        Evidence::Typed(Kind::Boolean)
                    }
                }
            }
            DomainExpression::Function {
                name,
                args,
                distinct,
            } => {
                if is_document_read(args) {
                    // The read's own source may itself be a document value:
                    // reading into a document is the one lawful use of one.
                    for arg in args {
                        self.expr(arg)?;
                    }
                    Evidence::Document
                } else {
                    match name {
                        FunctionName::User(name) => {
                            self.apply(name, user_contract(name), *distinct, args)?
                        }
                        FunctionName::Intrinsic(intrinsic) => self.apply(
                            &format!("{intrinsic:?}"),
                            intrinsic_contract(*intrinsic),
                            *distinct,
                            args,
                        )?,
                    }
                }
            }
            // A sigma predicate compares its arguments.
            DomainExpression::PredicateRewrite { name, args, .. } => {
                for arg in args {
                    self.demand(name, Demand::Stated, arg)?;
                }
                Evidence::Typed(Kind::Boolean)
            }
            // A CASE selects among its arms and carries what they carry.
            DomainExpression::Case {
                expr,
                when_clauses,
                else_clause,
            } => {
                if let Some(subject) = expr {
                    self.demand("CASE", Demand::Stated, subject)?;
                }
                let mut arms = Vec::with_capacity(when_clauses.len() + 1);
                for clause in when_clauses {
                    if expr.is_some() {
                        self.demand("WHEN", Demand::Stated, clause.when())?;
                    } else {
                        self.expr(clause.when())?;
                    }
                    arms.push(self.expr(clause.then())?);
                }
                if let Some(otherwise) = else_clause {
                    arms.push(self.expr(otherwise)?);
                }
                join(arms)
            }
            DomainExpression::WindowFunction {
                name,
                args,
                distinct,
                partition_by,
                order_by,
                frame,
            } => {
                let evidence = self.apply(name, user_contract(name), *distinct, args)?;
                for key in partition_by {
                    self.demand("PARTITION BY", Demand::Stated, key)?;
                }
                for (key, _) in order_by {
                    self.demand("ORDER BY", Demand::Stated, key)?;
                }
                if let Some(frame) = frame {
                    for bound in [&frame.start, &frame.end] {
                        if let SqlFrameBound::Preceding(e) | SqlFrameBound::Following(e) = bound {
                            self.demand("frame bound", Demand::Numeric, e)?;
                        }
                    }
                }
                evidence
            }
            DomainExpression::Observation { expr, .. } => self.expr(expr)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_target_whose_read_states_a_category_is_unjudged() {
        assert!(!needs_judgment(SqlDialect::SQLite));
        assert!(needs_judgment(SqlDialect::PostgreSQL));
        assert!(needs_judgment(SqlDialect::DuckDB));
        // Unmeasured is not proof: judged, fail closed.
        assert!(needs_judgment(SqlDialect::MySQL));
        assert!(needs_judgment(SqlDialect::SqlServer));
    }

    #[test]
    fn a_document_satisfies_no_demand_and_a_cast_discharges_only_its_own_kind() {
        assert!(!satisfies(Evidence::Document, Demand::Numeric));
        assert!(!satisfies(Evidence::Document, Demand::Stated));
        assert!(satisfies(Evidence::Typed(Kind::Numeric), Demand::Numeric));
        assert!(satisfies(Evidence::Typed(Kind::Target), Demand::Numeric));
        assert!(!satisfies(Evidence::Typed(Kind::Text), Demand::Numeric));
        assert!(!satisfies(Evidence::Typed(Kind::Boolean), Demand::Numeric));
        assert!(!satisfies(
            Evidence::Typed(Kind::Structured),
            Demand::Numeric
        ));
        for kind in [
            Kind::Numeric,
            Kind::Text,
            Kind::Boolean,
            Kind::Structured,
            Kind::Target,
        ] {
            assert!(satisfies(Evidence::Typed(kind), Demand::Stated), "{kind:?}");
        }
        assert_eq!(cast_kind("integer"), Kind::Numeric);
        assert_eq!(cast_kind("REAL"), Kind::Numeric);
        assert_eq!(cast_kind("numeric"), Kind::Numeric);
        assert_eq!(cast_kind("text"), Kind::Text);
        assert_eq!(cast_kind("boolean"), Kind::Boolean);
        assert_eq!(cast_kind("date"), Kind::Target);
    }

    #[test]
    fn every_callable_has_a_contract_and_the_unknown_one_manufactures_no_proof() {
        assert_eq!(
            user_contract("SUM"),
            Contract::Requires(Demand::Numeric, Kind::Numeric)
        );
        assert_eq!(user_contract("min"), Contract::Ordered);
        assert_eq!(user_contract("max"), Contract::Ordered);
        assert_eq!(user_contract("count"), Contract::Counts);
        assert_eq!(user_contract("JSON_OBJECT"), Contract::Structured);
        for unknown in [
            "abs",
            "coalesce",
            "upper",
            "length",
            "group_concat",
            "nosuch",
        ] {
            assert_eq!(user_contract(unknown), Contract::Unknown, "{unknown}");
        }
        assert_eq!(
            intrinsic_contract(Intrinsic::Round2),
            Contract::Requires(Demand::Numeric, Kind::Numeric)
        );
        assert_eq!(intrinsic_contract(Intrinsic::ScalarMax), Contract::Ordered);
        assert_eq!(
            intrinsic_contract(Intrinsic::JsonObject),
            Contract::Structured
        );
        assert_eq!(
            intrinsic_contract(Intrinsic::Arbitrary),
            Contract::Preserves
        );
    }

    #[test]
    fn a_join_is_a_document_if_any_arm_is_and_one_kind_only_if_all_agree() {
        assert_eq!(
            join([Evidence::Typed(Kind::Numeric), Evidence::Document]),
            Evidence::Document
        );
        assert_eq!(
            join([
                Evidence::Typed(Kind::Numeric),
                Evidence::Typed(Kind::Numeric)
            ]),
            Evidence::Typed(Kind::Numeric)
        );
        assert_eq!(
            join([Evidence::Typed(Kind::Numeric), Evidence::Typed(Kind::Text)]),
            Evidence::Typed(Kind::Target)
        );
        assert_eq!(join([]), Evidence::Typed(Kind::Target));
    }

    /// A table call spends its WHOLE contract. Safe Rust can place any
    /// intrinsic in `TableExpression::TVF`, so the fence is the judgment,
    /// not the producer: a numeric or ordered intrinsic in table position
    /// refuses a document argument, the iterator admits one, and a user
    /// callable refuses one.
    #[test]
    fn a_table_call_spends_its_whole_contract_over_a_document_argument() {
        use crate::names::{Addressing, Registry};
        use crate::pipeline::sql_ast::TableExpression;

        let identities = Registry::new(&[]);
        let spelling = identities.intern("t", false);
        let entity = identities.mint_entity(spelling);
        let scope = identities.resolved_access_scope(entity, spelling);
        let document = identities.sql_column(
            scope,
            Some(identities.intern("d", false)),
            Addressing::Published,
        );
        let typed = identities.sql_column(
            scope,
            Some(identities.intern("n", false)),
            Addressing::Published,
        );
        let mut pass = Pass {
            dialect: SqlDialect::PostgreSQL,
            identities: &identities,
            env: HashMap::from([(document, Evidence::Document)]),
        };
        let tvf = |function: FnId, argument: ColId| TableExpression::TVF {
            function,
            arguments: vec![TvfArgument::Column(argument)],
            alias: scope,
        };
        let refuses = |outcome: Result<()>| {
            let error = outcome.expect_err("a document argument must refuse");
            assert_eq!(
                error.error_uri(),
                "delightql-error://semantic/constraint/target_typing"
            );
        };

        // A numeric intrinsic in table position demands a numeric category.
        refuses(pass.table(&tvf(identities.mint_intrinsic(Intrinsic::Round2), document)));
        // An ordered intrinsic demands a stated category.
        refuses(pass.table(&tvf(
            identities.mint_intrinsic(Intrinsic::ScalarMax),
            document,
        )));
        // A user/target table function has no contract.
        let user = identities.mint_function(identities.intern("generate_series", false), vec![]);
        refuses(pass.table(&tvf(user, document)));
        // The compiler's iterators take a document as the document they iterate.
        pass.table(&tvf(
            identities.mint_intrinsic(Intrinsic::JsonEachArray),
            document,
        ))
        .expect("the array iterator admits a document");
        pass.table(&tvf(
            identities.mint_intrinsic(Intrinsic::JsonEachObject),
            document,
        ))
        .expect("the object iterator admits a document");
        // Over a typed argument every contract is satisfied.
        pass.table(&tvf(identities.mint_intrinsic(Intrinsic::Round2), typed))
            .expect("a typed argument satisfies the numeric demand");
        pass.table(&tvf(user, typed))
            .expect("a typed argument reaches an unknown callable");
    }
}
