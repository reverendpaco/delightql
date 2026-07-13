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
    BooleanExpression, DomainExpression, DomainSpec, Query, Relation, RelationalExpression,
    SigmaCondition, UnaryRelationalOperator, Unresolved,
};
use super::core::operators::DmlKind;
use super::ddl::{DdlBody, DdlDefinition, DdlHead, HoParam, ViewHeadItem};
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_visit::{
    walk_visit_boolean, walk_visit_domain_spec, walk_visit_operator, walk_visit_query,
    walk_visit_relational, walk_visit_sigma, AstVisit, Descent,
};

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
///
/// Rides the shared whole-tree closure `AstVisit` (INDUCTIVE-TRAVERSAL-PLAN §5
/// W1), so it reaches EVERY query-bearing edge — including the ones the former
/// private `Filter { source, .. }` walker dropped: `Filter.condition`,
/// `join_condition`, pipe-operator argument subqueries, and HO table arguments
/// (review llswlspw::zmxlwkky P2). A directive hidden under an IN/EXISTS/scalar
/// predicate is therefore now a visible demand.
pub fn collect_directive_invocations(
    expr: &RelationalExpression<Unresolved>,
) -> Vec<DirectiveInvocation> {
    let mut c = DirectiveDemandCollector::default();
    // The collector's hooks never fail, so the walk is infallible.
    let _ = walk_visit_relational(&mut c, expr);
    c.out
}

/// Collect every directive invocation in a full body query (CTEs included).
pub fn collect_directive_invocations_in_query(
    query: &Query<Unresolved>,
) -> Vec<DirectiveInvocation> {
    let mut c = DirectiveDemandCollector::default();
    let _ = walk_visit_query(&mut c, query);
    c.out
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
    // Rides Helper B `fold_tail` (the ending/tail spine): Join→right, and a
    // union ends in a directive iff EVERY arm does (`!empty && all`, the ledger
    // shape). The per-node ending test is `ends_in_directive_leaf`; only the
    // Join→right / SetOp→arms RECURSION is shared. Byte-equivalent to the old
    // hand-rolled walk. Pinned by
    // `fold_tail_descends_join_right_and_all_setop_arms`.
    crate::pipeline::spine::fold_tail(
        expr,
        &ends_in_directive_leaf,
        &|arms: Vec<bool>| !arms.is_empty() && arms.iter().all(|b| *b),
    )
}

/// The tail-LEAF half of `ends_in_directive`: does THIS tail node (a Pipe's tail
/// operator, or a leaf relation) end in a directive? Witness totalizers keep the
/// underlying arm's ending (re-rooting the tail fold at `pipe.source`); a
/// trailing Filter / ER chain does not end in a directive.
fn ends_in_directive_leaf(expr: &RelationalExpression<Unresolved>) -> bool {
    match expr {
        RelationalExpression::Relation(rel) => matches!(rel, Relation::PseudoPredicate { .. }),
        RelationalExpression::Pipe(pipe) => match &pipe.operator {
            UnaryRelationalOperator::DirectiveTerminal { .. }
            | UnaryRelationalOperator::DmlTerminal { .. }
            | UnaryRelationalOperator::DirectivePipeInvocation { .. } => true,
            UnaryRelationalOperator::Witness { .. }
            | UnaryRelationalOperator::SignedWitness => ends_in_directive(&pipe.source),
            // Operator-KIND classification: any other tail operator is not a
            // directive terminal, so the pipe does not end in a directive —
            // regardless of subqueries in the operator's own argument domain
            // expressions, which the tail contract DELIBERATELY does not recurse
            // (descending would be the §7 over-recursion bug). A newly-added
            // directive-terminal operator must be added to the arms above.
            _ => false,
        },
        // Tail-leaf STOP, spelled per R-I3 (was a bare `_ => false`): a trailing
        // Filter (source/condition), ER chain, or IntersectCorresponding
        // (operands/correlation) at the tail does NOT end in a directive — their
        // recursive fields are DELIBERATELY not descended (the tail contract). A
        // new relational variant now forces a decision here.
        RelationalExpression::Filter { .. }
        | RelationalExpression::ErJoinChain { .. }
        | RelationalExpression::ErTransitiveJoin { .. }
        | RelationalExpression::IntersectCorresponding { .. }
        // Join/SetOperation never reach the leaf — fold_tail recurses them — but
        // spelling them keeps this match exhaustive without a bare `_`.
        | RelationalExpression::Join { .. }
        | RelationalExpression::SetOperation { .. } => false,
    }
}

/// The names (with `!`) of all directives a clause body demands, EXCLUDING
/// references to the body's own effect-CTE labels (a reference `n!(*)` to a
/// CTE labeled `: n!` demands the CTE, not a rule named `n!` — E2). Used by
/// the R6 recursion check and the R9 positional checks.
pub fn demanded_directive_names(body: &EffectBody) -> Vec<DirectiveInvocation> {
    let mut c = DirectiveDemandCollector::default();
    for cte in &body.ctes {
        let _ = walk_visit_relational(&mut c, &cte.expression);
    }
    let _ = walk_visit_relational(&mut c, &body.expression);
    let labels: Vec<String> = body
        .ctes
        .iter()
        .map(|c| format!("{}!", c.name))
        .collect();
    c.out.retain(|inv| !labels.contains(&inv.name));
    c.out
}

/// Does the predicate `condition` demand a directive anywhere in its
/// boolean/domain subtree (through IN/EXISTS/scalar subqueries)? Used by the
/// effect transformer's lowering walker (W4) to detect an effect-head
/// predicate directive — legal in principle, but not yet lowerable (Q-I1(b)).
pub fn condition_demands_directive(cond: &SigmaCondition<Unresolved>) -> bool {
    let mut c = DirectiveDemandCollector::default();
    let _ = walk_visit_sigma(&mut c, cond);
    !c.out.is_empty()
}

/// Does the boolean expression `b` demand a directive anywhere in its subtree?
/// (A join condition is a bare `BooleanExpression`, not wrapped in a sigma.)
pub fn boolean_demands_directive(b: &BooleanExpression<Unresolved>) -> bool {
    let mut c = DirectiveDemandCollector::default();
    let _ = walk_visit_boolean(&mut c, b);
    !c.out.is_empty()
}

/// Does the pipe operator `op` demand a directive inside one of its argument
/// domain expressions (a scalar subquery hidden in a Transform/MapCover/…)?
/// The directive-bearing operators themselves (DML / directive terminals) are
/// lowered on the spine; this catches directives smuggled into a *pure*
/// operator's arguments.
pub fn operator_demands_directive(op: &UnaryRelationalOperator<Unresolved>) -> bool {
    let mut c = DirectiveDemandCollector::default();
    let _ = walk_visit_operator(&mut c, op);
    !c.out.is_empty()
}

/// Does this access/domain spec demand a directive (a scalar subquery hidden in
/// a positional column expression)? Used by the lowering walker (W4) to close
/// the recursive type: a directive smuggled into a Ground read's access spec or
/// a DML terminal's access spec is OFF the lowered spine, so it must be refused
/// (Q-I1(b)) rather than passed to SQL unprocessed (other-code-review.md [P1]).
pub fn domain_spec_demands_directive(spec: &DomainSpec<Unresolved>) -> bool {
    let mut c = DirectiveDemandCollector::default();
    let _ = walk_visit_domain_spec(&mut c, spec);
    !c.out.is_empty()
}

/// The `AstVisit` tenant that realizes the whole-tree directive-demand closure
/// (INDUCTIVE-TRAVERSAL-PLAN §5 W1, R-I6). The default `AstVisit` walk performs
/// the complete structural descent; this collector only names the demand
/// positions. Demand ORDER (load-bearing for R9's positional reads and for the
/// lowering walker) follows the 2026-07-12 ruling: EVERY directive is recorded
/// on `exit_*`, so a directive nested in another's argument is demanded first
/// (inputs before invocation). For flat cases this is order-identical to the
/// former walker. Pinned by the effects ball's rules--79/80/81 (a directive
/// under a PURE head, now seen through a predicate subquery, so R1 refuses) and
/// by `nested_directive_argument_is_demanded_before_enclosing` (the order).
#[derive(Default)]
struct DirectiveDemandCollector {
    out: Vec<DirectiveInvocation>,
}

impl AstVisit<Unresolved> for DirectiveDemandCollector {
    // RULED 2026-07-12 (inputs before invocation): EVERY directive form is
    // recorded on `exit_*`, AFTER its argument expressions have been descended.
    // A directive is thus demanded AFTER the demands nested in its arguments
    // (inner-before-outer), CONSISTENT across all forms — arguments are inputs,
    // so their demands precede the enclosing invocation. (Previously
    // PseudoPredicate/DirectiveTerminal/DmlTerminal recorded on enter while the
    // two-paren form recorded on exit — an inconsistency other-code-review.md
    // flagged.) For flat, non-nested cases this is order-identical to the former
    // walker (no directive is recorded during a non-directive arg descent).
    // Pinned by `nested_directive_argument_is_demanded_before_enclosing`.
    fn exit_relation(&mut self, r: &Relation<Unresolved>) -> Result<Descent> {
        if let Relation::PseudoPredicate {
            name, arguments, ..
        } = r
        {
            self.out.push(DirectiveInvocation {
                name: name.clone(),
                category: directive_category(name),
                params: arguments.clone(),
                access: DirectiveAccess::Call,
            });
        }
        Ok(Descent::Continue)
    }

    fn exit_operator(&mut self, op: &UnaryRelationalOperator<Unresolved>) -> Result<Descent> {
        match op {
            UnaryRelationalOperator::DirectiveTerminal { name, arguments } => {
                self.out.push(DirectiveInvocation {
                    name: name.clone(),
                    category: directive_category(name),
                    params: arguments.clone(),
                    access: DirectiveAccess::PipeTerminal,
                });
            }
            UnaryRelationalOperator::DmlTerminal { kind, target, .. } => {
                let name = match kind {
                    DmlKind::Update => "update!",
                    DmlKind::Delete => "delete!",
                    DmlKind::Insert => "insert!",
                };
                self.out.push(DirectiveInvocation {
                    name: name.to_string(),
                    category: directive_category(name),
                    params: vec![DomainExpression::lvar_builder(target.clone()).build()],
                    access: DirectiveAccess::DmlTerminal,
                });
            }
            UnaryRelationalOperator::DirectivePipeInvocation { name, .. } => {
                self.out.push(DirectiveInvocation {
                    name: name.clone(),
                    category: directive_category(name),
                    params: Vec::new(),
                    access: DirectiveAccess::PipeInvocation,
                });
            }
            _ => {}
        }
        Ok(Descent::Continue)
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

    // ------------------------------------------------------------------------
    // Whole-tree directive-demand closure (review llswlspw::zmxlwkky P2)
    //
    // The former private walker matched `Filter { source, .. }` and dropped
    // `condition`, so a directive hidden under an IN/EXISTS/scalar predicate was
    // invisible to R1/R4/R6/R9 — all of which read this collector. These pins
    // prove the migrated `AstVisit` closure reaches those positions. R1 is
    // additionally pinned end-to-end by the effects ball's
    // rules--79/80/81_r1_predicate_{in,exists,scalar}.
    // ------------------------------------------------------------------------

    use crate::pipeline::asts::core::expressions::metadata_types::FilterOrigin;
    use crate::pipeline::asts::core::{PhaseBox, QualifiedName};

    fn qn(name: &str) -> QualifiedName {
        QualifiedName {
            namespace_path: crate::pipeline::asts::core::metadata::NamespacePath::empty(),
            name: name.into(),
            grounding: None,
        }
    }

    /// A directive demand sentinel: `route!(*)` as an expression-position call.
    fn directive(name: &str) -> RelationalExpression<Unresolved> {
        RelationalExpression::Relation(Relation::PseudoPredicate {
            name: name.to_string(),
            arguments: vec![],
            alias: None,
            cpr_schema: PhaseBox::phantom(),
        })
    }

    /// A non-directive relation (a bare Ground read) — the collector records
    /// nothing for it, so it is inert scaffolding around the demand sentinels.
    fn plain() -> RelationalExpression<Unresolved> {
        RelationalExpression::Relation(Relation::Ground {
            identifier: qn("rows"),
            canonical_name: PhaseBox::phantom(),
            backend_schema: PhaseBox::phantom(),
            domain_spec: crate::pipeline::asts::core::DomainSpec::Glob,
            alias: None,
            outer: false,
            mutation_target: false,
            passthrough: false,
            cpr_schema: PhaseBox::phantom(),
            hygienic_injections: Vec::new(),
        })
    }

    fn filter_with_predicate(pred: BooleanExpression<Unresolved>) -> RelationalExpression<Unresolved> {
        RelationalExpression::Filter {
            source: Box::new(plain()),
            condition: SigmaCondition::Predicate(pred),
            origin: FilterOrigin::UserWritten,
            cpr_schema: PhaseBox::phantom(),
        }
    }

    fn in_relational(sub: RelationalExpression<Unresolved>) -> BooleanExpression<Unresolved> {
        BooleanExpression::InRelational {
            value: Box::new(DomainExpression::NonUnifiyingUnderscore),
            subquery: Box::new(sub),
            identifier: qn("p"),
            negated: false,
        }
    }

    fn inner_exists(sub: RelationalExpression<Unresolved>) -> BooleanExpression<Unresolved> {
        BooleanExpression::InnerExists {
            exists: true,
            identifier: qn("p"),
            subquery: Box::new(sub),
            alias: None,
            using_columns: vec![],
        }
    }

    fn scalar_cmp(sub: RelationalExpression<Unresolved>) -> BooleanExpression<Unresolved> {
        BooleanExpression::Comparison {
            operator: "=".to_string(),
            left: Box::new(DomainExpression::ScalarSubquery {
                identifier: qn("s"),
                subquery: Box::new(sub),
                alias: None,
            }),
            right: Box::new(DomainExpression::NonUnifiyingUnderscore),
        }
    }

    #[test]
    fn demand_reaches_predicate_subqueries_in_exists_scalar() {
        for build in [
            in_relational as fn(RelationalExpression<Unresolved>) -> BooleanExpression<Unresolved>,
            inner_exists,
            scalar_cmp,
        ] {
            let expr = filter_with_predicate(build(directive("route!")));
            let found = collect_directive_invocations(&expr);
            assert_eq!(
                found.iter().map(|i| i.name.as_str()).collect::<Vec<_>>(),
                vec!["route!"],
                "directive under a predicate subquery must be a visible demand"
            );
            assert!(expression_demands_directive(&expr));
            assert!(condition_demands_directive(&SigmaCondition::Predicate(build(directive(
                "route!"
            )))));
        }
    }

    #[test]
    fn nested_directive_argument_is_demanded_before_enclosing() {
        // RULED 2026-07-12 (inputs before invocation): a directive nested in
        // another directive's ARGUMENT is demanded FIRST (inner-before-outer),
        // because every directive is recorded on `exit_*` — after its arguments
        // are descended. Pre-fix, expression-position calls recorded on `enter_`
        // (outer-before-inner), the inconsistency other-code-review.md flagged.
        let inner_in_arg = DomainExpression::ScalarSubquery {
            identifier: qn("s"),
            subquery: Box::new(directive("inner!")),
            alias: None,
        };
        let outer = RelationalExpression::Relation(Relation::PseudoPredicate {
            name: "outer!".to_string(),
            arguments: vec![inner_in_arg],
            alias: None,
            cpr_schema: PhaseBox::phantom(),
        });
        let invs = collect_directive_invocations(&outer);
        let order: Vec<&str> = invs.iter().map(|i| i.name.as_str()).collect();
        assert_eq!(
            order,
            vec!["inner!", "outer!"],
            "an argument's demands precede the enclosing invocation"
        );
    }

    #[test]
    fn domain_spec_demands_directive_reaches_positional_scalar_subquery() {
        use crate::pipeline::asts::core::DomainSpec;
        // A directive hidden in a scalar subquery in a positional access column
        // (a Ground read's or DML terminal's access spec). The builder currently
        // routes non-column access expressions to WHERE filters, so this shape
        // is not reachable via surface DQL today — but the closure reaches it, so
        // the lowering walker (W4) refuses it as defense-in-depth against any
        // future construction path (other-code-review.md [P1]).
        let spec = DomainSpec::Positional(vec![DomainExpression::ScalarSubquery {
            identifier: qn("s"),
            subquery: Box::new(directive("insert!")),
            alias: None,
        }]);
        assert!(domain_spec_demands_directive(&spec));
        assert!(!domain_spec_demands_directive(&DomainSpec::Glob));
    }

    #[test]
    fn demand_reaches_deeply_nested_boolean_composition() {
        // NOT( plain-ish AND (plain OR EXISTS(route!)) ) — the demand sits
        // under three layers of boolean composition, so only genuine recursion
        // finds it.
        let deep = BooleanExpression::Not {
            expr: Box::new(BooleanExpression::And {
                left: Box::new(BooleanExpression::BooleanLiteral { value: true }),
                right: Box::new(BooleanExpression::Or {
                    left: Box::new(BooleanExpression::BooleanLiteral { value: false }),
                    right: Box::new(inner_exists(directive("route!"))),
                }),
            }),
        };
        let expr = filter_with_predicate(deep);
        let found = collect_directive_invocations(&expr);
        assert_eq!(found.len(), 1, "deeply nested demand must be reached");
        assert_eq!(found[0].name, "route!");
    }

    #[test]
    fn demand_reaches_join_condition_and_operator_arguments() {
        // Join condition (via InnerExists) — missed by the old walker.
        let join = RelationalExpression::Join {
            left: Box::new(plain()),
            right: Box::new(plain()),
            join_condition: Some(inner_exists(directive("route!"))),
            join_type: None,
            cpr_schema: PhaseBox::phantom(),
        };
        assert!(
            boolean_demands_directive(&inner_exists(directive("route!"))),
            "join-condition helper must see the nested demand"
        );
        assert_eq!(collect_directive_invocations(&join).len(), 1);

        // Pipe-OPERATOR argument (a scalar subquery inside a Transform) — the
        // edge no relational-entry walker reached before.
        let op = UnaryRelationalOperator::Transform {
            transformations: vec![(
                DomainExpression::ScalarSubquery {
                    identifier: qn("s"),
                    subquery: Box::new(directive("route!")),
                    alias: None,
                },
                "a".to_string(),
                None,
            )],
            conditioned_on: None,
        };
        assert!(operator_demands_directive(&op));
        let pipe = RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(
            crate::pipeline::asts::core::expressions::PipeExpression {
                source: plain(),
                operator: op,
                cpr_schema: PhaseBox::phantom(),
            },
        )));
        assert_eq!(collect_directive_invocations(&pipe).len(), 1);
    }
}
