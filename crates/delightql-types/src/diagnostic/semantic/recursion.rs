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

    /// While inlining a consulted definition, the resolver re-encountered a
    /// name it was already expanding — the self-reference did not resolve
    /// as the in-progress recursive CTE, so expansion would never terminate.
    /// The common cause: in a consulted rules file, the recursive clause
    /// appears BEFORE the base clause — clause order matters; a
    /// self-reference is only recursive once a prior clause has established
    /// the name. Put the base (non-recursive) clause first. If the cycle
    /// runs through another view (a uses v, v uses a), break the cycle. The
    /// error message shows the expansion chain. RECURSION-CONTRACT.md B5.
    #[leaf("consulted_clause_order", class = Syntax, summary = "Circular consulted-definition expansion (recursive clause before base, or an indirect view cycle).")]
    #[error("Validation error: {message}")]
    ConsultedClauseOrder { message: String },

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
