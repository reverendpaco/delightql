// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `semantic/recursion/…` — the recursion contract.

use super::Semantic;
use crate::diagnostic::{DelightQLError, Taxon};

/// `semantic/recursion/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(DelightQLError::Semantic, Semantic::Recursion))]
pub enum Recursion {
    /// DelightQL defines a row limit inside a recursive rule as a TOTAL-ROW
    /// CAP on the fixpoint — a demand bound on the unfold. SQLite and MySQL
    /// spell it natively (a trailing LIMIT on the recursive member); this
    /// target has no single-statement equivalent, and the near-miss
    /// spellings silently change meaning (a subquery LIMIT becomes
    /// per-iteration — non-terminating). Rewrite the bound as a filter
    /// condition on the recursive rule: a depth counter carried in the
    /// frontier row, or a value predicate.
    #[leaf("limit_bound", class = Syntax, summary = "#<N inside a recursive rule has no spelling on this target.")]
    #[error("Validation error: {message}")]
    LimitBound { message: String },

    /// Renames and constraints on the self-reference ('c(m)' inside c's own
    /// definition) do not bind inside a recursive definition yet — refused
    /// rather than returning wrong results. Use glob binding 'c(*)' and
    /// rename or filter in a pipe stage. RECURSION-CONTRACT.md B2.
    #[leaf("argumentative_binding", class = Syntax, summary = "Argumentative binding on a recursive self-reference.")]
    #[error("Validation error: {message}")]
    ArgumentativeBinding { message: String },

    /// A definition whose name can see itself — a CHOE or a consulted rule,
    /// whose head declares the name before any clause body is read — reached
    /// itself before any base clause established its fixpoint. Names resolve
    /// left to right, so the anchor comes first: every recursive definition
    /// has at least one base clause, and every base clause precedes the
    /// recursive ones. Put the base (non-recursive) clause first.
    /// SEMANTICS/recursion-contract-law.md, THE ANCHOR COMES FIRST.
    #[leaf("anchor_first", class = Syntax, summary = "A self-reference stands before any base clause.")]
    #[error("Validation error: {message}")]
    AnchorFirst { message: String },

    /// Recursion is over relations. A value function that reaches itself —
    /// with the same actual, a changed one, or through other value
    /// functions — has no fixpoint to re-enter. Define the relation and
    /// ground the argument instead: `fib(k, v)` as a recursive relation,
    /// `fib(5, v)` to read the point. SEMANTICS/recursion-contract-law.md,
    /// RELATION-FORM ONLY.
    #[leaf("function-form", class = Syntax, summary = "A value function recurses.")]
    #[error("Validation error: {message}")]
    FunctionForm { message: String },

    /// A truth rule that cites itself — with any actual, directly or through
    /// other truth rules — is an existence test with no fixpoint to re-enter,
    /// whether or not it has a base clause. Write the recursion as a
    /// relational rule and test the relation.
    /// SEMANTICS/recursion-contract-law.md, RELATION-FORM ONLY.
    #[leaf("truth-form", class = Syntax, summary = "A truth rule recurses.")]
    #[error("Validation error: {message}")]
    TruthForm { message: String },

    /// Mutual recursion is not supported. Each recursive definition may
    /// re-enter only its own established frontier; reaching an earlier open
    /// definition through another family closes a cycle in the
    /// definition-instance graph. The refusal reports the complete cycle
    /// from either entry point. Break the cycle or combine the state into
    /// one recursive relation. SEMANTICS/recursion-contract-law.md, NO
    /// MUTUAL RECURSION.
    #[leaf("mutual", class = Syntax, summary = "Two or more definitions form a recursion cycle.")]
    #[error("Validation error: {message}")]
    Mutual { message: String },

    /// Clause accumulation is the fixpoint's own operation and uses the
    /// flavor declared by the recursive target. A union-family operator
    /// cannot stand inside one recursive member. Finish the fixpoint first,
    /// then use its result as an ordinary set-operation arm.
    /// SEMANTICS/recursion-contract-law.md, THE BADGE CHOOSES THE UNION.
    #[leaf("set_operator", class = Syntax, summary = "A union-family operator appears inside a recursive clause.")]
    #[error("Validation error: {message}")]
    SetOperator { message: String },

    /// THE BADGE CHOOSES THE UNION: a fixpoint flavor is one claim about the
    /// TARGET, and the target is the whole definition — so every clause of
    /// it wears the same badge. An unbadged clause beside a `%`-badged one
    /// is two claims about one thing. Badge every clause of the target, or
    /// none of them. RECURSION-CONTRACT.md THE BADGE CHOOSES THE UNION.
    #[leaf("mixed_badge", class = Syntax, summary = "Clauses of one target disagree about the fixpoint badge.")]
    #[error("Validation error: {message}")]
    MixedBadge { message: String },

    /// `%` names the DEDUPLICATING fixpoint, and a fixpoint flavor on a
    /// non-fixpoint is a false statement. The definition has no
    /// self-reference, so there is no unfold for the badge to choose the
    /// union of. Drop the badge; to deduplicate an ordinary definition,
    /// spell the distinct view in the body (`|> %(*)`).
    /// RECURSION-CONTRACT.md THE BADGE CHOOSES THE UNION.
    #[leaf("false_fixpoint", class = Syntax, summary = "A `%` badge on a definition that does not reference itself.")]
    #[error("Validation error: {message}")]
    FalseFixpoint { message: String },

    /// The semantic actuals of a parameterized recursive definition select
    /// ONE fixpoint instance and stay invariant for that instance: a
    /// self-reference with the same actuals re-enters the active fixpoint,
    /// and one with different actuals never opens another specialization.
    /// State that changes between recursive iterations belongs in the
    /// recursive relation's ordinary columns — a definition that needs n to
    /// become n + 1 carries n as a column instead of calling itself under a
    /// new actual (SEMANTICS/recursion-contract-law.md, MONOMORPHIC
    /// PARAMETERS).
    #[leaf("parameter-widening", class = Syntax, summary = "A recursive definition's self-reference changed a parameter actual.")]
    #[error("Validation error: {message}")]
    ParameterWidening { message: String },

    /// The frontier cannot join with itself (or with the accumulated
    /// result) — forward evaluation carries one previous iteration. Carry
    /// the values you need as columns of one frontier row instead: the
    /// tupling transformation. fib is the canonical example — two self-calls
    /// become one two-column state, (a, b) stepping to (b, a+b).
    /// RECURSION-CONTRACT.md N1.
    #[leaf("nonlinear", class = Syntax, summary = "A recursive rule references itself more than once.")]
    #[error("Validation error: {message}")]
    Nonlinear { message: String },

    /// An aggregate over the frontier would need the accumulated set, which
    /// a recursive rule never sees. Aggregate after the fixpoint — strata
    /// are textual, so a later pipe stage aggregates the finished recursion
    /// — or carry a running value as a column of the frontier row when the
    /// aggregation is per-path. RECURSION-CONTRACT.md N3.
    #[leaf("aggregate", class = Syntax, summary = "Aggregation inside a recursive rule.")]
    #[error("Validation error: {message}")]
    Aggregate { message: String },

    /// Semi/anti-joins, IN, scalar subqueries, or derived tables against
    /// the definition itself would need the accumulated set — a recursive
    /// rule sees only the previous iteration's rows, as a direct source.
    /// Track visited state in the frontier row (the visited-string idiom),
    /// or deduplicate/filter after the fixpoint. RECURSION-CONTRACT.md N4.
    #[leaf("self_subquery", class = Syntax, summary = "A recursive rule references itself inside a subquery.")]
    #[error("Validation error: {message}")]
    SelfSubquery { message: String },
}
