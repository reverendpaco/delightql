// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE EDGE SHAPE: an edge body reads exactly its two endpoints, each
//! once, each spelled as its term, and carries only simple conditions —
//! at least one connection (a comparison between the two endpoints, of any
//! kind) standing as a top-level conjunct, and filters. Nothing else: no
//! helper, no alias, no outer mark, no dequalifying access, no aggregate,
//! window, probe, subquery or binding, and no stage. The shape is a fact
//! of the body's text and is judged here, when the edge is declared and
//! again as an invariant when it is used. The text's judgment is the
//! necessary one: what it can know it refuses — a self-pair, a read that
//! is not an endpoint, a probe written in place, a body whose every
//! comparison provably stands over one endpoint. Which endpoint a bare
//! column belongs to, and what a named function's body is, are not facts
//! of the text; every edge consumer judges them on the resolved body
//! (`body`).

use crate::diagnostic::Er;
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_unresolved;
use crate::pipeline::ast_visit::{walk_visit_boolean, AstVisit, Descent};
use crate::pipeline::asts::core::{Reference, Unresolved};

use super::ErTerm;

/// Which of the body's two reads is the LEFT term's: its head, or its one
/// member. The other read is the right term's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::defuse) enum LeftAt {
    Head,
    Member,
}

pub(super) fn refuse(pair: &str, what: String) -> DelightQLError {
    DelightQLError::from(Er::BodyShape {
        message: format!(
            "the edge body for {pair} {what} — an edge body reads exactly its two \
             endpoints, each spelled as its term, and carries only simple \
             conditions: at least one connection (a comparison between the two \
             endpoints, outside any disjunction) and filters over the endpoints' \
             columns. Anything that changes the population on its own belongs in \
             a named rule, which an endpoint term may then name"
        ),
    })
}

/// The refusal of a body with no connection — one teaching, whether the
/// text shows it or the resolved body does.
pub(super) fn no_connection(pair: &str) -> DelightQLError {
    refuse(
        pair,
        "connects its endpoints with no comparison whose two sides read the two \
         endpoints, one each, standing as a top-level conjunct — a comparison over \
         one endpoint alone, or inside a disjunction, is a filter, not a connection"
            .to_string(),
    )
}

/// Judge a declared body against the two terms that select its edge:
/// where the left term's read stands. The body's reads are held to the
/// term law by canonical spelling (`reads`, from the body's own bytes):
/// a read IS an endpoint's read exactly when it spells as the term.
pub(in crate::defuse) fn judge(
    query: &ast_unresolved::Query,
    reads: &crate::term_spec::BodyReads,
    left: &ErTerm,
    right: &ErTerm,
    pair: &str,
) -> Result<LeftAt> {
    if left.name() == right.name() {
        return Err(refuse(
            pair,
            format!(
                "names one relation, {}, at both endpoints — a self-pair cannot be \
                 declared, since its body would read one relation twice; declare a \
                 named rule for one side (boss(*) :- employees(*)) and an edge over \
                 the two distinct terms",
                left.name()
            ),
        ));
    }
    if let Some(name) = binders(left.read())
        .into_iter()
        .find(|name| binders(right.read()).contains(name))
    {
        return Err(refuse(
            pair,
            format!(
                "binds the name {name} in both endpoint terms — the two reads would \
                 correspond by binder, directing the join and publishing one column \
                 for two, which is not the pair schema; bind the positions to distinct \
                 names and spell the equality"
            ),
        ));
    }
    for term in [left, right] {
        if let Some(reused) = reused_name(term.read()) {
            return Err(refuse(
                pair,
                format!(
                    "reads its endpoint {} with a slot that reuses {reused} — an endpoint \
                     term reads alone and publishes its every position; spell the \
                     equality",
                    term.spelling()
                ),
            ));
        }
    }
    if !query.is_bare() {
        return Err(refuse(pair, "declares a query-local binding".to_string()));
    }
    let body = &query.body;
    if !matches!(body.head().form(), ast_unresolved::GroundForm::Reference(_)) {
        return Err(refuse(pair, "reads an anonymous table".to_string()));
    }
    let (head, steps) = body.clone().split_read();
    // A read stands bare — the alias and the mark are written outside the
    // read's bytes — and spells as the term.
    let (left_at, other) = match reads.head.as_deref() {
        Some(spelling) if super::is_bare_read(&head) && spelling == left.spelling() => {
            (LeftAt::Head, right)
        }
        Some(spelling) if super::is_bare_read(&head) && spelling == right.spelling() => {
            (LeftAt::Member, left)
        }
        _ => return Err(refuse(pair, not_an_endpoint_read(&head, left, right))),
    };
    let mut other_read = false;
    let mut conditions = Vec::new();
    let mut member_reads = reads.members.iter();
    for step in steps {
        match step.into_form() {
            ast_unresolved::Continuation::Member {
                rhs,
                correlation,
                join_type,
            } => {
                let spelling = member_reads.next().and_then(|read| read.as_deref());
                if correlation.is_some() || join_type.is_some() {
                    return Err(refuse(pair, marked_read(&rhs, left, right)));
                }
                if super::is_bare_read(&rhs) && spelling == Some(other.spelling()) {
                    if other_read {
                        return Err(refuse(pair, format!("reads {} twice", other.spelling())));
                    }
                    other_read = true;
                } else {
                    return Err(refuse(pair, not_an_endpoint_read(&rhs, left, right)));
                }
            }
            ast_unresolved::Continuation::Restrict { condition, .. } => conditions.push(condition),
            ast_unresolved::Continuation::Access { .. } => {
                return Err(refuse(
                    pair,
                    "carries a further dimension access".to_string(),
                ))
            }
            ast_unresolved::Continuation::Bound { .. } => {
                return Err(refuse(pair, "carries a row bound".to_string()))
            }
            ast_unresolved::Continuation::Correlate { .. } => {
                return Err(refuse(
                    pair,
                    "carries a whole-heading correlation".to_string(),
                ))
            }
            ast_unresolved::Continuation::Correlated(_) => {
                return Err(refuse(pair, "carries a correlation".to_string()))
            }
            ast_unresolved::Continuation::Destructure { .. } => {
                return Err(refuse(pair, "carries a destructure".to_string()))
            }
            ast_unresolved::Continuation::Pipe { .. }
            | ast_unresolved::Continuation::Structural(_) => {
                return Err(refuse(pair, "carries a stage".to_string()))
            }
            ast_unresolved::Continuation::BagOp { .. } => {
                return Err(refuse(pair, "carries a set operation".to_string()))
            }
            ast_unresolved::Continuation::ErJoin(_) => {
                return Err(refuse(pair, "carries a nested edge call".to_string()))
            }
        }
    }
    if !other_read {
        return Err(refuse(
            pair,
            format!("does not read its endpoint {}", other.spelling()),
        ));
    }
    // Every condition is simple; the top-level conjuncts are what a
    // connection may be.
    let mut conjuncts = Vec::new();
    for condition in conditions {
        top_level(condition, &mut conjuncts);
    }
    for conjunct in &conjuncts {
        simple(conjunct, pair)?;
    }
    // A connection is a comparison whose two sides read the two endpoints,
    // one each. The text settles a side whose every reference is qualified
    // by an endpoint; a side with a bare reference is settled where the
    // body resolves. A body whose every comparison is settled and confined
    // to one endpoint has no connection, and refuses here.
    let names = [left.name(), right.name()];
    let mut open = false;
    for conjunct in &conjuncts {
        let ast_unresolved::TruthExpression::Comparison(comparison) = conjunct else {
            continue;
        };
        let (l, r) = (
            references(&comparison.left).side(names),
            references(&comparison.right).side(names),
        );
        match (l, r) {
            (Side::Endpoint(a), Side::Endpoint(b)) if a != b => return Ok(left_at),
            (Side::Literal, _) | (_, Side::Literal) => {}
            (Side::Mixed, _) | (_, Side::Mixed) => {}
            (Side::Endpoint(_), Side::Endpoint(_)) => {}
            (Side::Open, _) | (_, Side::Open) => open = true,
        }
    }
    if open {
        Ok(left_at)
    } else {
        Err(no_connection(pair))
    }
}

/// The names a term's read binds in slot position: `pa(ax, y, av)` binds
/// ax, y and av. A name one read binds twice unifies two of its own
/// positions and is that read's own; a name both reads bind would unify a
/// position of each — a correspondence the text does not spell.
fn binders(read: &ast_unresolved::Chain) -> Vec<delightql_types::SqlIdentifier> {
    let Some(ast_unresolved::Access::Slots(slots)) = read.head_access() else {
        return Vec::new();
    };
    slots
        .iter()
        .filter_map(|slot| match slot {
            ast_unresolved::Slot::Bind(binder) => Some(binder.name.clone()),
            ast_unresolved::Slot::Anon
            | ast_unresolved::Slot::Reuse(_)
            | ast_unresolved::Slot::Constraint(_) => None,
        })
        .collect()
}

/// A qualified name in slot position, `pb(pa.x, …)`: the slot reuses
/// another relation's value and publishes nothing, so the read neither
/// stands alone nor publishes its every position.
fn reused_name(read: &ast_unresolved::Chain) -> Option<String> {
    let ast_unresolved::Access::Slots(slots) = read.head_access()? else {
        return None;
    };
    slots.iter().find_map(|slot| match slot {
        ast_unresolved::Slot::Reuse(reference) => {
            let column = reference.column();
            Some(match &column.qualifier {
                Some(qualifier) => format!("{qualifier}.{}", column.name),
                None => column.name.to_string(),
            })
        }
        ast_unresolved::Slot::Bind(_)
        | ast_unresolved::Slot::Anon
        | ast_unresolved::Slot::Constraint(_) => None,
    })
}

fn not_an_endpoint_read(read: &ast_unresolved::Chain, left: &ErTerm, right: &ErTerm) -> String {
    let named = match read.as_read_relation() {
        Some(ast_unresolved::Relation::Ground {
            mention:
                ast_unresolved::GroundMention::Named {
                    identifier, alias, ..
                },
            outer,
        }) => Some((identifier.name.clone(), alias.is_some(), *outer)),
        Some(ast_unresolved::Relation::InnerRelation {
            pattern: ast_unresolved::InnerRelationPattern::Indeterminate { identifier, .. },
            alias,
            outer,
        }) => Some((identifier.name.clone(), alias.is_some(), *outer)),
        _ => None,
    };
    match named {
        Some((name, aliased, outer)) if name == *left.name() || name == *right.name() => {
            let how = if aliased {
                "under an alias"
            } else if outer {
                "under an outer mark"
            } else {
                "with a different access"
            };
            format!(
                "reads its endpoint {name} {how}; spell the endpoint exactly as its term \
                 (the alias and the mark stand outside a term, and a dequalifying \
                 access is spelled as an equality)"
            )
        }
        Some((name, ..)) => format!("reads {name}, which is not one of its endpoints"),
        None => "reads a relation that is not one of its endpoints".to_string(),
    }
}

/// A member read that carries a join directive — the outer mark, or a
/// member correlation — cannot be an endpoint read.
fn marked_read(read: &ast_unresolved::Chain, left: &ErTerm, right: &ErTerm) -> String {
    let named = match read.as_read_relation() {
        Some(ast_unresolved::Relation::Ground {
            mention: ast_unresolved::GroundMention::Named { identifier, .. },
            ..
        }) => Some(identifier.name.clone()),
        Some(ast_unresolved::Relation::InnerRelation {
            pattern: ast_unresolved::InnerRelationPattern::Indeterminate { identifier, .. },
            ..
        }) => Some(identifier.name.clone()),
        _ => None,
    };
    match named {
        Some(name) if name == *left.name() || name == *right.name() => format!(
            "reads its endpoint {name} under an outer mark or a member correlation; spell \
             the endpoint exactly as its term (the mark stands outside a term, on the \
             call's peer)"
        ),
        _ => "directs a member with a correlation or a join spelling".to_string(),
    }
}

/// The top-level conjuncts of a condition: a conjunction's parts,
/// recursively; anything else is one conjunct.
fn top_level(
    condition: ast_unresolved::TruthExpression,
    out: &mut Vec<ast_unresolved::TruthExpression>,
) {
    match condition {
        ast_unresolved::TruthExpression::Conjunction(parts) => {
            for part in parts.into_vec() {
                top_level(part, out);
            }
        }
        other => out.push(other),
    }
}

/// A SIMPLE condition reaches no relation: no existence or membership
/// probe, no sigma, no window, no subquery, no constructed value, and
/// every reference is a named column. A truth crossed into a value is
/// judged as the truth it is.
fn simple(condition: &ast_unresolved::TruthExpression, pair: &str) -> Result<()> {
    struct Simple<'a> {
        pair: &'a str,
        refusal: Option<DelightQLError>,
    }
    impl Simple<'_> {
        fn stop(&mut self, what: &str) -> Result<Descent> {
            self.refusal = Some(refuse(self.pair, format!("carries {what} in a condition")));
            Ok(Descent::Break)
        }
    }
    impl AstVisit<Unresolved> for Simple<'_> {
        fn enter_boolean(&mut self, e: &ast_unresolved::TruthExpression) -> Result<Descent> {
            match e {
                ast_unresolved::TruthExpression::Existence(_) => self.stop("an existence probe"),
                ast_unresolved::TruthExpression::Membership(_)
                | ast_unresolved::TruthExpression::RelationalMembership(_) => {
                    self.stop("a membership test")
                }
                ast_unresolved::TruthExpression::Sigma(_) => self.stop("a sigma predicate"),
                ast_unresolved::TruthExpression::Comparison(_)
                | ast_unresolved::TruthExpression::Conjunction(_)
                | ast_unresolved::TruthExpression::Disjunction(_)
                | ast_unresolved::TruthExpression::Not { .. } => Ok(Descent::Continue),
            }
        }
        fn enter_domain(&mut self, e: &ast_unresolved::DomainExpression) -> Result<Descent> {
            match e {
                ast_unresolved::DomainExpression::Reference(reference) => match reference {
                    Reference::Named(_) => Ok(Descent::Continue),
                    Reference::Ordinal(_) | Reference::Physical(_) => {
                        self.stop("a positional reference")
                    }
                },
                ast_unresolved::DomainExpression::Application(_) => Ok(Descent::Continue),
            }
        }
        fn enter_function(&mut self, f: &ast_unresolved::FunctionApplication) -> Result<Descent> {
            match f {
                ast_unresolved::FunctionApplication::Standard(application)
                    if application.window.is_some() =>
                {
                    self.stop("a window")
                }
                ast_unresolved::FunctionApplication::Scalarized(_) => self.stop("a subquery"),
                ast_unresolved::FunctionApplication::Enclyph(_) => self.stop("a constructed value"),
                ast_unresolved::FunctionApplication::Crossed(_)
                | ast_unresolved::FunctionApplication::Ground(_)
                | ast_unresolved::FunctionApplication::Open(_)
                | ast_unresolved::FunctionApplication::Standard(_)
                | ast_unresolved::FunctionApplication::Infix(_)
                | ast_unresolved::FunctionApplication::Template(_)
                | ast_unresolved::FunctionApplication::Case(_)
                | ast_unresolved::FunctionApplication::FieldSelect(_)
                | ast_unresolved::FunctionApplication::ClauseSelection(_)
                | ast_unresolved::FunctionApplication::JsonAccess(_) => Ok(Descent::Continue),
            }
        }
    }
    let mut visit = Simple {
        pair,
        refusal: None,
    };
    walk_visit_boolean(&mut visit, condition)?;
    match visit.refusal {
        Some(refusal) => Err(refusal),
        None => Ok(()),
    }
}

/// The named references of one side of a comparison: which qualifiers
/// they carry.
struct SideReferences {
    qualifiers: Vec<Option<delightql_types::SqlIdentifier>>,
}

/// What the text knows of one side of a comparison.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    /// No column: a literal side.
    Literal,
    /// Every reference qualified by the one endpoint.
    Endpoint(usize),
    /// Every reference qualified, by both endpoints.
    Mixed,
    /// Some reference bare, or qualified by a name that is not an
    /// endpoint: the resolved body says whose it is.
    Open,
}

impl SideReferences {
    fn side(&self, names: [&delightql_types::SqlIdentifier; 2]) -> Side {
        if self.qualifiers.is_empty() {
            return Side::Literal;
        }
        let mut which: Option<usize> = None;
        for qualifier in &self.qualifiers {
            let Some(qualifier) = qualifier.as_ref() else {
                return Side::Open;
            };
            let Some(index) = names.iter().position(|name| *name == qualifier) else {
                return Side::Open;
            };
            match which {
                None => which = Some(index),
                Some(seen) if seen == index => {}
                Some(_) => return Side::Mixed,
            }
        }
        which.map_or(Side::Literal, Side::Endpoint)
    }
}

fn references(side: &ast_unresolved::DomainExpression) -> SideReferences {
    struct Collect(SideReferences);
    impl AstVisit<Unresolved> for Collect {
        fn enter_domain(&mut self, e: &ast_unresolved::DomainExpression) -> Result<Descent> {
            if let ast_unresolved::DomainExpression::Reference(Reference::Named(named)) = e {
                self.0.qualifiers.push(named.column().qualifier.clone());
            }
            Ok(Descent::Continue)
        }
    }
    let mut collect = Collect(SideReferences {
        qualifiers: Vec::new(),
    });
    let _ = crate::pipeline::ast_visit::walk_visit_domain(&mut collect, side);
    collect.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(source: &str) -> ast_unresolved::Query {
        let tree = crate::pipeline::parse::query_sequence(source).expect("a body parses");
        let registry = std::rc::Rc::new(crate::names::Registry::new(&[]));
        let normalized =
            crate::pipeline::normalize::query_sequence(&tree, registry).expect("a body normalizes");
        let mut queries = normalized.into_queries();
        assert_eq!(queries.len(), 1);
        queries.remove(0).query
    }

    fn term(spelling: &str) -> ErTerm {
        let canonical = crate::term_spec::canonicalize_term(spelling).expect("a term");
        ErTerm::spelled(&canonical).expect("a declared term")
    }

    fn reads(source: &str) -> crate::term_spec::BodyReads {
        crate::term_spec::query_body_reads(source).expect("one relex")
    }

    fn judge_terms(source: &str, left: &str, right: &str) -> Result<LeftAt> {
        judge(
            &body(source),
            &reads(source),
            &term(left),
            &term(right),
            &format!("{left} & {right}"),
        )
    }

    fn judged(source: &str) -> Result<LeftAt> {
        judge_terms(source, "a(*)", "b(*)")
    }

    #[test]
    fn the_simple_shapes_are_admitted_in_either_read_order() {
        for (source, left) in [
            ("a(*), b(*), a.k = b.k", LeftAt::Head),
            ("b(*), a(*), a.k = b.k", LeftAt::Member),
            (
                "a(*), b(*), a.low <= b.value, b.value < a.high",
                LeftAt::Head,
            ),
            (
                "a(*), b(*), a.x = b.x, a.y = b.y, a.tag = \"t\"",
                LeftAt::Head,
            ),
            (
                "a(*), b(*), lower:(a.email) = lower:(b.email)",
                LeftAt::Head,
            ),
            (
                "a(*), b(*), (a.id + 0) = (b.pid - 0), b.x > 1 or b.x = 1",
                LeftAt::Head,
            ),
            ("a(*), b(*), a.k = b.k, a.low < a.high", LeftAt::Head),
            (
                "a(*), b(*), a.k = b.k or a.vip = 1, a.k = b.k",
                LeftAt::Head,
            ),
        ] {
            let at = judged(source).unwrap_or_else(|e| panic!("{source}: {e}"));
            assert_eq!(at, left, "{source}");
        }
    }

    /// A comparison with a bare side is settled where the body resolves:
    /// the text admits it, beside whatever else it can settle.
    #[test]
    fn a_bare_side_leaves_the_connection_to_the_resolved_body() {
        for source in [
            "a(*), b(*), k = pk",
            "a(*), b(*), a.k = pk",
            "a(*), b(*), a.k = a.k, k = pk",
            "a(*), b(*), zz.k = b.k",
        ] {
            judged(source).unwrap_or_else(|e| panic!("{source}: {e}"));
        }
    }

    #[test]
    fn everything_beyond_simple_refuses_by_name() {
        for (source, what) in [
            (
                "a(*), b(*), c(*), a.k = b.k",
                "reads c, which is not one of its endpoints",
            ),
            ("a(*), b(*) as bb, a.k = bb.k", "under an alias"),
            (
                "a(*), b?(*), a.k = b.k",
                "under an outer mark or a member correlation",
            ),
            ("a(*), b(, x > 0), a.k = b.k", "with a different access"),
            ("a(*), b(*.(k))", "with a different access"),
            ("a(*), b(.*)", "with a different access"),
            ("a(*), a.k = 1", "does not read its endpoint b(*)"),
            ("a(*), b(*), b(*), a.k = b.k", "reads b(*) twice"),
            ("a(*), b(*), a.k = b.k |> (a.*, b.*)", "carries a stage"),
            ("a(*), b(*), a.k = b.k, # < 2", "carries a row bound"),
            (
                "a(*), b(*), a.k = b.k or a.vip = 1",
                "no comparison whose two sides read the two endpoints",
            ),
            (
                "a(*), b(*), a.k = 1",
                "no comparison whose two sides read the two endpoints",
            ),
            // Settled and confined: every comparison stands over one
            // endpoint, or has a side that mixes both.
            (
                "a(*), b(*), a.k = a.k",
                "no comparison whose two sides read the two endpoints",
            ),
            (
                "a(*), b(*), a.k = (a.x + b.k), b.y = 1",
                "no comparison whose two sides read the two endpoints",
            ),
            (
                "a(*), b(*), a.k = a.k, b.k = b.x",
                "no comparison whose two sides read the two endpoints",
            ),
            ("a(*), b(*), a.k = b.k, +c(, x = a.k)", "an existence probe"),
            ("a(*), b(*), a.k = b.k, a.x in (1; 2)", "a membership test"),
            ("a(*), b(*), row_number:(<~ #(a.k)) = b.k", "a window"),
            ("a(*), b(*), a.k = |1|", "a positional reference"),
        ] {
            let refusal = judged(source)
                .err()
                .unwrap_or_else(|| panic!("{source}: admitted"));
            let text = refusal.to_string();
            assert!(
                text.contains("body_shape") || text.contains("edge body"),
                "{source}: {text}"
            );
            assert!(text.contains(what), "{source}: {text}");
        }
    }

    /// A self-pair refuses before its body is read: two endpoints naming
    /// one relation have no simple body.
    #[test]
    fn a_self_pair_cannot_be_declared() {
        for (left, right) in [("a(*)", "a(*)"), ("a(*)", "a(, k > 0)")] {
            let refusal = judge_terms("a(*), a(*), a.k = a.k", left, right)
                .err()
                .unwrap_or_else(|| panic!("{left} & {right}: admitted"));
            let text = refusal.to_string();
            assert!(
                text.contains("names one relation, a, at both endpoints"),
                "{text}"
            );
        }
    }

    /// A name bound by both endpoint terms would unify a position of each
    /// read — a correspondence the text does not spell, and one published
    /// column for two. Distinct binders with the equality spelled are the
    /// simple shape; a name one read binds twice is that read's own.
    #[test]
    fn a_name_bound_by_both_reads_cannot_be_declared() {
        let refusal = judge_terms("a(k, y), b(m, y), k = m", "a(k, y)", "b(m, y)")
            .err()
            .expect("a shared binder is refused");
        let text = refusal.to_string();
        assert!(
            text.contains("binds the name y in both endpoint terms"),
            "{text}"
        );
        judge_terms("a(k, ya), b(m, yb), k = m, ya = yb", "a(k, ya)", "b(m, yb)")
            .expect("distinct binders with the equality spelled");
        judge_terms("a(v, v), b(m, yb), v = m", "a(v, v)", "b(m, yb)")
            .expect("one read binding a name twice is its own restriction");
        let refusal = judge_terms("a(*), b(a.k, yb), a.y = yb", "a(*)", "b(a.k, yb)")
            .err()
            .expect("a slot reusing the other read's column is refused");
        assert!(
            refusal.to_string().contains("with a slot that reuses a.k"),
            "{refusal}"
        );
    }

    /// A read is an endpoint's read exactly when it spells as the term:
    /// the same canonical spelling agrees, whatever it contains and
    /// whichever registry normalized either side; a different spelling
    /// — a different constraint, a different call, an operand reordered
    /// — is a different term and refuses.
    #[test]
    fn a_shaped_term_is_read_as_spelled() {
        judge_terms("p(*), q(, x > 0), p.id = q.pid", "p(*)", "q(, x > 0)").expect("as spelled");
        assert!(judge_terms("p(*), q(, 0 < x), p.id = q.pid", "p(*)", "q(, x > 0)").is_err());
        for (source, left, right) in [
            (
                "p(*), q(abs:(1), y, v), p.y = y",
                "p(*)",
                "q(abs:(1), y, v)",
            ),
            (
                "q(abs:(1), y, v), p(*), p.y = y",
                "p(*)",
                "q(abs:(1), y, v)",
            ),
            (
                "p(*), q(, abs:(x) = 1), p.y = q.y",
                "p(*)",
                "q(, abs:(x) = 1)",
            ),
            (
                "p(*), q((1 + 0), y, v), p.y = y",
                "p(*)",
                "q((1 + 0), y, v)",
            ),
            (
                "p(*), q(lower:(v), y, w), p.y = y",
                "p(*)",
                "q(lower:(v), y, w)",
            ),
        ] {
            judge_terms(source, left, right).unwrap_or_else(|e| panic!("{source}: {e}"));
        }
        for (source, left, right, what) in [
            (
                "p(*), q(abs:(2), y, v), p.y = y",
                "p(*)",
                "q(abs:(1), y, v)",
                "with a different access",
            ),
            (
                "p(*), q(, abs:(x) = 2), p.y = q.y",
                "p(*)",
                "q(, abs:(x) = 1)",
                "with a different access",
            ),
            (
                "p(*), q(1 + 0, y, v), p.y = y",
                "p(*)",
                "q((1 + 0), y, v)",
                "with a different access",
            ),
        ] {
            let text = judge_terms(source, left, right)
                .err()
                .unwrap_or_else(|| panic!("{source}: admitted"))
                .to_string();
            assert!(text.contains(what), "{source}: {text}");
        }
    }
}
