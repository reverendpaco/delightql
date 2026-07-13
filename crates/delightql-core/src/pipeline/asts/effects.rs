// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The effect AST family (EFFECT-ALGEBRA.md; IMPLEMENTATION-PLAN §2.2).
//!
//! Effect rules are user-defined directives (`name!(*) :- body`). Like the
//! DDL AST (`asts/ddl.rs`), the effect family is EPHEMERAL: built from
//! consulted definition text for validation and registration, then discarded.
//! The database stores text; ASTs are re-parsed on demand.
//!
//! This module owns:
//! - the directive CATEGORY taxonomy (EFFECT-ALGEBRA §3) — the single source
//!   of truth for liminal eligibility (§8) and R9 positional checks;
//! - the typed shapes of the new constructs: effect rule (head, clauses),
//!   effect body (effect-CTE definitions + body expression), directive
//!   invocation (name, category, params, access), liminal directive;
//! - the demand walker used by the R1/R2/R4/R6/R9 validators in `system.rs`
//!   (`validate_effect_rule_discipline` — the RULE 2 precedent's sibling).
//!
//! The signed witness (`+-`) lives with the other postfix operators as
//! `UnaryRelationalOperator::SignedWitness` (asts/core/operators.rs); it is
//! a plain-pipeline citizen (resolver + transformer_v4) as well as a
//! value-position lowering in the effect transformer.

use super::core::{
    DomainExpression, Query, Relation, RelationalExpression, UnaryRelationalOperator, Unresolved,
};
use super::ddl::{DdlBody, DdlDefinition, DdlHead, HoParam, ViewHeadItem};
use crate::error::{DelightQLError, Result};

// ============================================================================
// Directive categories (EFFECT-ALGEBRA §3)
// ============================================================================

/// Session directives — direct the session's namespace tree; the ONLY
/// liminal-eligible category (EFFECT-ALGEBRA §8). `doc!` is included: it is
/// a session directive (annotation only) and is additionally R9-exempt.
pub const SESSION_DIRECTIVES: &[&str] = &[
    "consult",
    "consult_tree",
    "reconsult",
    "unconsult",
    "mount",
    "mount_new",
    "mount_tree",
    "unmount",
    "refresh",
    "ground",
    "enlist",
    "delist",
    "alias",
    "expose",
    "doc",
];

/// DDL directives — create database objects (EFFECT-ALGEBRA §3).
pub const DDL_DIRECTIVES: &[&str] = &[
    "temp_table",
    "table",
    "temp_view",
    "imprint",
    "imprint_replace",
];

/// DML directives — write rows in user tables (EFFECT-ALGEBRA §3).
pub const DML_DIRECTIVES: &[&str] = &["insert", "update", "delete"];

/// Execution directives — start runs (EFFECT-ALGEBRA §9).
pub const EXECUTION_DIRECTIVES: &[&str] = &["run", "run_namespace"];

/// Utility directives — direct the run itself (EFFECT-ALGEBRA §3, §5).
pub const UTILITY_DIRECTIVES: &[&str] = &["exit", "returning", "returning_other", "stdout"];

/// The category of a directive, by what it directs (EFFECT-ALGEBRA §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectiveCategory {
    /// Directs the session's namespace tree; liminal-eligible (§8).
    Session,
    /// Creates database objects.
    Ddl,
    /// Writes rows in user tables.
    Dml,
    /// Starts runs (`run!`, `run_namespace!`).
    Execution,
    /// Directs the run itself (stop, return, sequence, print).
    Utility,
    /// A user directive — defined by an effect rule (or unknown; the
    /// distinction is resolution's, not the category taxonomy's).
    User,
}

/// Classify a directive name (with or without the trailing `!`).
pub fn directive_category(name: &str) -> DirectiveCategory {
    let bare = name.strip_suffix('!').unwrap_or(name);
    if SESSION_DIRECTIVES.contains(&bare) {
        DirectiveCategory::Session
    } else if DDL_DIRECTIVES.contains(&bare) {
        DirectiveCategory::Ddl
    } else if DML_DIRECTIVES.contains(&bare) {
        DirectiveCategory::Dml
    } else if EXECUTION_DIRECTIVES.contains(&bare) {
        DirectiveCategory::Execution
    } else if UTILITY_DIRECTIVES.contains(&bare) {
        DirectiveCategory::Utility
    } else {
        DirectiveCategory::User
    }
}

/// Is this directive name (with or without `!`) liminal-eligible?
/// Exactly the session directives are (EFFECT-ALGEBRA §8).
pub fn is_liminal_eligible(name: &str) -> bool {
    directive_category(name) == DirectiveCategory::Session
}

/// Badge for the liminal-eligibility refusal (REPORT-1.2 proposal 3).
pub const LIMINAL_NOT_ELIGIBLE_BADGE: &str = "directive/liminal/not_eligible";

/// The liminal-eligibility refusal message. Substring pinned red-first by the
/// effects ball (liminal--41_dml_not_eligible, liminal--42_run_not_eligible:
/// "only session directives are liminal-eligible").
pub fn liminal_not_eligible_message(name: &str) -> String {
    let bare = name.strip_suffix('!').unwrap_or(name);
    format!(
        "cannot execute '{bare}!' in the liminal space: only session directives \
         are liminal-eligible — every other directive (DML, DDL, execution, \
         utility, and user effect rules) executes by demand, not at load \
         (EFFECT-ALGEBRA §8). Put it in an effect rule and demand it from main!."
    )
}

// ============================================================================
// Directive invocations (the normalized record the validators walk)
// ============================================================================

/// How a directive invocation appears in the text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectiveAccess {
    /// Expression-position call: `name!(args)` (with optional trailing
    /// access parens desugared upstream) — `Relation::PseudoPredicate`.
    Call,
    /// Pipe-terminal, access parens omitted: `… |> name!(args)` —
    /// `UnaryRelationalOperator::DirectiveTerminal`.
    PipeTerminal,
    /// Pipe-terminal two-paren form with a relational HO argument:
    /// `… |> name!(Rel(*))(spec)` —
    /// `UnaryRelationalOperator::DirectivePipeInvocation`.
    PipeInvocation,
    /// DML terminal: `… |> insert!(t(*))(spec)` —
    /// `UnaryRelationalOperator::DmlTerminal`.
    DmlTerminal,
}

/// A single directive invocation found in an expression: name (with `!`),
/// category (§3), parameters, and the access form it was written in.
#[derive(Debug, Clone)]
pub struct DirectiveInvocation {
    /// Directive name, `!` included (e.g. `"insert!"`, `"route!"`).
    pub name: String,
    /// §3 category of the name (`User` for effect-rule names).
    pub category: DirectiveCategory,
    /// The invocation's parameters, as written (best-effort: the DML
    /// terminal's target is recorded as a single name parameter).
    /// Consumed by the effect transformer (Epic 3); today the validators
    /// read only `name`/`category`.
    #[allow(dead_code)]
    pub params: Vec<DomainExpression<Unresolved>>,
    /// The syntactic access form. Consumed by the effect transformer
    /// (Epic 3).
    #[allow(dead_code)]
    pub access: DirectiveAccess,
}

// ============================================================================
// Liminal directives (EFFECT-ALGEBRA §8)
// ============================================================================

/// A directive statement in a file's liminal space — the top level of a
/// file, outside the rules. Only session directives are liminal-eligible.
#[derive(Debug, Clone)]
pub struct LiminalDirective {
    /// Directive name WITHOUT the `!` (matches the extraction layer's
    /// `EmbeddedDirective` convention).
    pub name: String,
    /// Naive string arguments, quotes stripped.
    pub args: Vec<String>,
}

// ============================================================================
// Effect rules (EFFECT-ALGEBRA §1, §4; R1–R9)
// ============================================================================

/// One clause (arm) of an effect rule (R5: clauses are arms).
#[derive(Debug, Clone)]
pub struct EffectClause {
    /// Higher-order head parameters (empty for the glob head `name!(*)`).
    /// Consumed by the effect transformer (Epic 3: F4 argument plumbing).
    #[allow(dead_code)]
    pub params: Vec<HoParam>,
    /// Output head of the HO form (`None` = glob `(*)`).
    #[allow(dead_code)]
    pub output_head: Option<Vec<ViewHeadItem>>,
    /// The clause body, with effect-CTE definitions separated out.
    pub body: EffectBody,
    /// The clause's source text (head + neck + body). Consumed by the
    /// effect transformer (Epic 3).
    #[allow(dead_code)]
    pub full_source: String,
}

/// An effect rule: a user directive definition, possibly multi-clause (R5).
#[derive(Debug, Clone)]
pub struct EffectRule {
    /// Rule name, `!` included (e.g. `"route!"`, `"main!"`).
    pub name: String,
    pub clauses: Vec<EffectClause>,
}

/// One CTE definition inside an effect body (R3/R4): a pure CTE (`: name`)
/// or an effect CTE (`: name!`).
#[derive(Debug, Clone)]
pub struct EffectCteDef {
    pub name: String,
    /// True when the label carries the `!` effect marker (`: name!`).
    pub effect_marked: bool,
    /// True when the CTE's expression demands a directive (R4's criterion).
    pub demands_directive: bool,
    pub expression: RelationalExpression<Unresolved>,
}

/// An effect-clause body (R3): CTE definitions + the body expression whose
/// value is the clause's value.
#[derive(Debug, Clone)]
pub struct EffectBody {
    pub ctes: Vec<EffectCteDef>,
    pub expression: RelationalExpression<Unresolved>,
}

impl EffectBody {
    /// Build an `EffectBody` view over a parsed body `Query`, reading each
    /// CTE binding's effect marker (the marker the builder used to DROP —
    /// REPORT-2.1 note 1; pinned by `effect_cte_marker_is_read_by_builder`).
    pub fn from_query(query: &Query<Unresolved>) -> Result<EffectBody> {
        match query {
            Query::Relational(expr) => Ok(EffectBody {
                ctes: Vec::new(),
                expression: expr.clone(),
            }),
            Query::WithCtes { ctes, query } => Ok(EffectBody {
                ctes: ctes
                    .iter()
                    .map(|cte| EffectCteDef {
                        name: cte.name.clone(),
                        effect_marked: cte.effect_label,
                        demands_directive: expression_demands_directive(&cte.expression),
                        expression: cte.expression.clone(),
                    })
                    .collect(),
                expression: query.clone(),
            }),
            // R3: a clause body is a single expression in the pure-body
            // grammar. CFEs / REPL wrappers / ER contexts are not effect-body
            // shapes; refuse rather than silently accept a shape the effect
            // transformer will never lower.
            other => Err(DelightQLError::validation_error_categorized(
                "effect/rule/body_grammar",
                format!(
                    "effect rule body has an unsupported top-level shape ({:?}); \
                     a clause body is a single expression with optional CTEs \
                     (EFFECT-ALGEBRA R3)",
                    std::mem::discriminant(other)
                ),
                "unsupported effect body shape",
            )),
        }
    }
}

impl EffectRule {
    /// Assemble an `EffectRule` from the typed DDL definitions of one
    /// name-group (all clauses of one rule, in definition order).
    pub fn from_ddl_definitions(name: &str, defs: &[DdlDefinition]) -> Result<EffectRule> {
        let mut clauses = Vec::new();
        for def in defs {
            let DdlHead::EffectRule {
                ref params,
                ref output_head,
            } = def.head
            else {
                return Err(DelightQLError::validation_error_categorized(
                    "effect/rule/mixed_kind",
                    format!(
                        "definition '{}': an effect rule ('!' head) and a pure \
                         definition share one name — clauses of one name must all \
                         be effect clauses (EFFECT-ALGEBRA R5)",
                        name
                    ),
                    "mixed effect/pure clauses",
                ));
            };
            let DdlBody::Relational(ref query) = def.body else {
                return Err(DelightQLError::validation_error_categorized(
                    "effect/rule/body_grammar",
                    format!(
                        "effect rule '{}': body is not a relational expression \
                         (EFFECT-ALGEBRA R3)",
                        name
                    ),
                    "non-relational effect body",
                ));
            };
            clauses.push(EffectClause {
                params: params.clone(),
                output_head: output_head.clone(),
                body: EffectBody::from_query(query)?,
                full_source: def.full_source.clone(),
            });
        }
        Ok(EffectRule {
            name: name.to_string(),
            clauses,
        })
    }
}

// ============================================================================
// The demand walker
// ============================================================================

/// Collect every directive invocation in an expression, in syntactic order.
pub fn collect_directive_invocations(
    expr: &RelationalExpression<Unresolved>,
) -> Vec<DirectiveInvocation> {
    let mut out = Vec::new();
    walk_relational(expr, &mut out);
    out
}

/// Collect every directive invocation in a full body query (CTEs included).
pub fn collect_directive_invocations_in_query(
    query: &Query<Unresolved>,
) -> Vec<DirectiveInvocation> {
    let mut out = Vec::new();
    match query {
        Query::Relational(expr) => walk_relational(expr, &mut out),
        Query::WithCtes { ctes, query } => {
            for cte in ctes {
                walk_relational(&cte.expression, &mut out);
            }
            walk_relational(query, &mut out);
        }
        Query::WithCfes { cfes: _, query } | Query::WithPrecompiledCfes { query, .. } => {
            out.extend(collect_directive_invocations_in_query(query));
        }
        Query::ReplTempTable { query, .. }
        | Query::ReplTempView { query, .. }
        | Query::WithErContext { query, .. } => {
            out.extend(collect_directive_invocations_in_query(query));
        }
    }
    out
}

/// Does this expression demand a directive, directly or through a nested
/// subquery? (R1's and R4's criterion. Demands through CTE LABELS are seen
/// at the label's reference site — a reference to an effect CTE is written
/// `label!(*)`, which walks as a `!`-named call.)
pub fn expression_demands_directive(expr: &RelationalExpression<Unresolved>) -> bool {
    !collect_directive_invocations(expr).is_empty()
}

/// R2: does the body expression END in a directive? The "end" is the
/// rightmost step of the expression: the last pipe operator, the rightmost
/// conjunct of a join, or every arm of a union. Witness postfixes (`+`,
/// `\+`, `+-`) pass through — the algebra's own ledger tail applies them to
/// receipt arms (EFFECT-ALGEBRA §3, §10). Pinned red-first by the effects
/// ball (rules--26_r2_ending).
#[stacksafe::stacksafe]
pub fn ends_in_directive(expr: &RelationalExpression<Unresolved>) -> bool {
    match expr {
        RelationalExpression::Relation(rel) => matches!(rel, Relation::PseudoPredicate { .. }),
        RelationalExpression::Pipe(pipe) => match &pipe.operator {
            UnaryRelationalOperator::DirectiveTerminal { .. }
            | UnaryRelationalOperator::DmlTerminal { .. }
            | UnaryRelationalOperator::DirectivePipeInvocation { .. } => true,
            // Witness totalizers keep the underlying arm's ending.
            UnaryRelationalOperator::Witness { .. }
            | UnaryRelationalOperator::SignedWitness => ends_in_directive(&pipe.source),
            _ => false,
        },
        // A trailing guard/join ends in its rightmost element.
        RelationalExpression::Join { right, .. } => ends_in_directive(right),
        RelationalExpression::Filter { .. } => false,
        // A union ends in a directive when every arm does (the ledger shape).
        RelationalExpression::SetOperation { operands, .. } => {
            !operands.is_empty() && operands.iter().all(ends_in_directive)
        }
        RelationalExpression::ErJoinChain { .. }
        | RelationalExpression::ErTransitiveJoin { .. } => false,
        RelationalExpression::IntersectCorresponding { .. } => false,
    }
}

/// The names (with `!`) of all directives a clause body demands, EXCLUDING
/// references to the body's own effect-CTE labels (a reference `n!(*)` to a
/// CTE labeled `: n!` demands the CTE, not a rule named `n!` — E2). Used by
/// the R6 recursion check and the R9 positional checks.
pub fn demanded_directive_names(body: &EffectBody) -> Vec<DirectiveInvocation> {
    let mut out = Vec::new();
    for cte in &body.ctes {
        walk_relational(&cte.expression, &mut out);
    }
    walk_relational(&body.expression, &mut out);
    let labels: Vec<String> = body
        .ctes
        .iter()
        .map(|c| format!("{}!", c.name))
        .collect();
    out.retain(|inv| !labels.contains(&inv.name));
    out
}

#[stacksafe::stacksafe]
fn walk_relational(expr: &RelationalExpression<Unresolved>, out: &mut Vec<DirectiveInvocation>) {
    match expr {
        RelationalExpression::Relation(rel) => walk_relation(rel, out),
        RelationalExpression::Join { left, right, .. } => {
            walk_relational(left, out);
            walk_relational(right, out);
        }
        RelationalExpression::Filter { source, .. } => walk_relational(source, out),
        RelationalExpression::Pipe(pipe) => {
            walk_relational(&pipe.source, out);
            match &pipe.operator {
                UnaryRelationalOperator::DirectiveTerminal { name, arguments } => {
                    out.push(DirectiveInvocation {
                        name: name.clone(),
                        category: directive_category(name),
                        params: arguments.clone(),
                        access: DirectiveAccess::PipeTerminal,
                    });
                }
                UnaryRelationalOperator::DirectivePipeInvocation {
                    name, argument, ..
                } => {
                    walk_relational(argument, out);
                    out.push(DirectiveInvocation {
                        name: name.clone(),
                        category: directive_category(name),
                        params: Vec::new(),
                        access: DirectiveAccess::PipeInvocation,
                    });
                }
                UnaryRelationalOperator::DmlTerminal { kind, target, .. } => {
                    let name = match kind {
                        super::core::operators::DmlKind::Update => "update!",
                        super::core::operators::DmlKind::Delete => "delete!",
                        super::core::operators::DmlKind::Insert => "insert!",
                    };
                    out.push(DirectiveInvocation {
                        name: name.to_string(),
                        category: directive_category(name),
                        params: vec![DomainExpression::lvar_builder(target.clone()).build()],
                        access: DirectiveAccess::DmlTerminal,
                    });
                }
                _ => {}
            }
        }
        RelationalExpression::SetOperation { operands, .. } => {
            for operand in operands {
                walk_relational(operand, out);
            }
        }
        RelationalExpression::ErJoinChain { relations } => {
            for rel in relations {
                walk_relation(rel, out);
            }
        }
        RelationalExpression::ErTransitiveJoin { left, right } => {
            walk_relational(left, out);
            walk_relational(right, out);
        }
        RelationalExpression::IntersectCorresponding { .. } => {}
    }
}

fn walk_relation(rel: &Relation<Unresolved>, out: &mut Vec<DirectiveInvocation>) {
    match rel {
        Relation::PseudoPredicate {
            name, arguments, ..
        } => {
            out.push(DirectiveInvocation {
                name: name.clone(),
                category: directive_category(name),
                params: arguments.clone(),
                access: DirectiveAccess::Call,
            });
        }
        Relation::InnerRelation { pattern, .. } => {
            use super::core::expressions::InnerRelationPattern as P;
            match pattern {
                P::Indeterminate { subquery, .. }
                | P::UncorrelatedDerivedTable { subquery, .. }
                | P::CorrelatedScalarJoin { subquery, .. }
                | P::CorrelatedGroupJoin { subquery, .. } => walk_relational(subquery, out),
            }
        }
        Relation::ConsultedView { body, .. } => {
            out.extend(collect_directive_invocations_in_query(body));
        }
        Relation::Ground { .. } | Relation::Anonymous { .. } | Relation::TVF { .. } => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The §3 category taxonomy is the single source of truth for liminal
    /// eligibility (§8) and the R9 positional checks — pin the boundaries.
    #[test]
    fn directive_categories_match_effect_algebra_section_3() {
        assert_eq!(directive_category("consult!"), DirectiveCategory::Session);
        assert_eq!(directive_category("doc"), DirectiveCategory::Session);
        assert_eq!(directive_category("temp_table!"), DirectiveCategory::Ddl);
        assert_eq!(directive_category("insert!"), DirectiveCategory::Dml);
        assert_eq!(directive_category("run!"), DirectiveCategory::Execution);
        assert_eq!(
            directive_category("run_namespace!"),
            DirectiveCategory::Execution
        );
        assert_eq!(directive_category("returning!"), DirectiveCategory::Utility);
        assert_eq!(directive_category("route!"), DirectiveCategory::User);

        // §8: exactly the session directives are liminal-eligible.
        assert!(is_liminal_eligible("mount!"));
        // mount_new! is a session directive (EFFECT-ALGEBRA §6, §8 intro table),
        // liminal-eligible like mount!.
        assert_eq!(directive_category("mount_new!"), DirectiveCategory::Session);
        assert!(is_liminal_eligible("mount_new!"));
        assert!(!is_liminal_eligible("insert!"));
        assert!(!is_liminal_eligible("run!"));
        assert!(!is_liminal_eligible("route!"));
    }
}
